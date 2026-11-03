"""Load configuration from environment / .env."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from mattermost_acp.deployment import config_root, resolve_path

DEFAULT_TOKEN_FILE = Path.home() / ".config" / "mattermost-acp-gateway" / "token"


def _truthy(value: str | None, default: bool = False) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: str | None) -> frozenset[str]:
    if not value:
        return frozenset()
    return frozenset(part.strip() for part in value.split(",") if part.strip())


def _token_file_csv(value: str | None) -> frozenset[str]:
    """Slash tokens from a shared file (one per line or CSV), so every fleet
    gateway accepts the team's commands without copying tokens into each
    env file."""
    if not value or not value.strip():
        return frozenset()
    try:
        raw = Path(value.strip()).expanduser().read_text(encoding="utf-8")
    except OSError:
        return frozenset()
    return _csv(raw.replace("\n", ","))


WILDCARD_CHANNELS = {"all", "*", "any"}


def _env(name: str, default: str = "") -> str:
    """Read an ``AGENT_*`` key, falling back to its pre-rename ``GROK_*`` name."""
    value = os.environ.get(name)
    if value is None:
        legacy = "GROK_" + name.removeprefix("AGENT_")
        value = os.environ.get(legacy, default)
    return value


def _optional_path(value: str | None) -> Path | None:
    raw = (value or "").strip()
    if not raw:
        return None
    selected = resolve_path(raw, config_root())
    if not selected.is_file():
        raise ValueError("Selected instruction file is missing")
    return selected


def _read_token(path: Path) -> str:
    try:
        return path.expanduser().read_text(encoding="utf-8").strip()
    except OSError:
        return ""


@dataclass(frozen=True)
class Config:
    mattermost_url: str
    mattermost_token: str
    channel_ids: frozenset[str]
    exclude_channel_ids: frozenset[str]
    all_channels: bool
    auto_join_channels: bool
    allow_user_ids: frozenset[str]
    team_id: str | None
    peer_channel_id: str | None
    fleet_file: Path | None
    role_file: Path | None
    harness: str

    grok_command: str
    grok_model: str | None
    grok_cwd: Path
    always_approve: bool
    answer_mode: str
    max_parallel_turns: int
    show_thoughts: bool
    text_slash: bool
    auto_rename_threads: bool
    inject_channel_context: bool

    post_max_lines: int
    status_tool_lines: int
    status_done_tool_lines: int
    status_progress_lines: int

    slash_tokens: frozenset[str]
    slash_host: str
    slash_port: int

    db_path: Path
    log_file: Path | None
    attachments_dir: Path
    attachments_max_bytes: int

    @classmethod
    def load(cls, env_file: str | Path | None = None) -> Config:
        if env_file:
            load_dotenv(env_file, override=True)
        else:
            # A second gateway user runs from the same working directory, so a
            # bare load_dotenv() would read the *other* user's `.env` for any
            # key its own EnvironmentFile happens to omit. MATTERMOST_DOTENV
            # pins the file; `none` disables the fallback entirely.
            chosen = os.environ.get("MATTERMOST_DOTENV", "").strip()
            if not chosen:
                load_dotenv()
            elif chosen.lower() != "none":
                load_dotenv(chosen)

        url = os.environ.get("MATTERMOST_URL", "").strip().rstrip("/")
        if not url:
            raise ValueError("MATTERMOST_URL is required (https://your-mm-host)")

        token_file = Path(
            os.environ.get("MATTERMOST_TOKEN_FILE", str(DEFAULT_TOKEN_FILE))
        ).expanduser()
        token = os.environ.get("MATTERMOST_TOKEN", "").strip() or _read_token(token_file)
        if not token:
            raise ValueError(
                "MATTERMOST_TOKEN is required (or MATTERMOST_TOKEN_FILE "
                f"at {token_file})"
            )

        raw_channels = _csv(os.environ.get("MATTERMOST_CHANNEL_ID")) | _csv(
            os.environ.get("MATTERMOST_CHANNEL_IDS")
        )
        # `all` / `*` / `any` — or an empty value — means every channel this
        # account can see, including channels created after startup.
        all_channels = not raw_channels or any(
            c.lower() in WILDCARD_CHANNELS for c in raw_channels
        )
        channel_ids = frozenset(
            c for c in raw_channels if c.lower() not in WILDCARD_CHANNELS
        )
        auto_join = _truthy(
            os.environ.get("MATTERMOST_AUTO_JOIN_CHANNELS"), default=all_channels
        )
        # Channels this account must never join or answer in, even in `all`
        # mode. Used to keep one harness out of another harness's channel.
        exclude_channel_ids = _csv(
            os.environ.get("MATTERMOST_EXCLUDE_CHANNEL_ID")
        ) | _csv(os.environ.get("MATTERMOST_EXCLUDE_CHANNEL_IDS"))

        team_raw = os.environ.get("MATTERMOST_TEAM_ID", "").strip()
        team_id = team_raw or None
        peer_channel_id = os.environ.get("MATTERMOST_PEER_CHANNEL_ID", "").strip() or None

        harness = os.environ.get("HARNESS", "").strip()
        if harness not in {"grok", "claude", "codex", "cursor"}:
            raise ValueError("HARNESS must explicitly select grok, claude, codex or cursor")
        command = _env("AGENT_COMMAND", "").strip()
        if not command:
            raise ValueError("AGENT_COMMAND is required")
        model = _env("AGENT_MODEL", "").strip() or None
        cwd = Path(_env("AGENT_CWD", ".")).expanduser()
        answer_mode = _env("AGENT_ANSWER_MODE", "finalize").strip().lower()
        if answer_mode not in {"finalize", "live"}:
            raise ValueError("AGENT_ANSWER_MODE must be 'finalize' or 'live'")

        max_parallel = int(_env("AGENT_MAX_PARALLEL_TURNS", "4"))
        db = Path(os.environ.get("MATTERMOST_ACP_DB", "./data/threads-mm.db")).expanduser()
        log_raw = os.environ.get("MATTERMOST_ACP_LOG_FILE", "").strip()
        log_file = Path(log_raw).expanduser() if log_raw else None
        attachments_raw = os.environ.get("MATTERMOST_ACP_ATTACHMENTS_DIR", "").strip()
        attachments_dir = (
            Path(attachments_raw).expanduser() if attachments_raw else db.parent / "attachments"
        )
        attachments_max_mb = float(os.environ.get("MATTERMOST_ACP_ATTACHMENT_MAX_MB", "10"))

        slash_port = int(os.environ.get("MATTERMOST_SLASH_PORT", "8766"))

        # Readability budgets: the webapp hides anything past 600px behind
        # "Show more", and a finished status post is scrollback noise.
        post_max_lines = max(4, int(os.environ.get("MATTERMOST_POST_MAX_LINES", "20")))
        status_tool_lines = max(0, int(os.environ.get("MATTERMOST_STATUS_TOOL_LINES", "6")))
        status_done_tool_lines = max(
            0, int(os.environ.get("MATTERMOST_STATUS_DONE_TOOL_LINES", "0"))
        )
        status_progress_lines = max(
            0, int(os.environ.get("MATTERMOST_STATUS_PROGRESS_LINES", "3"))
        )

        return cls(
            mattermost_url=url,
            mattermost_token=token,
            channel_ids=channel_ids,
            exclude_channel_ids=exclude_channel_ids,
            all_channels=all_channels,
            auto_join_channels=auto_join,
            allow_user_ids=_csv(os.environ.get("MATTERMOST_ALLOW_USER_IDS")),
            team_id=team_id,
            peer_channel_id=peer_channel_id,
            fleet_file=_optional_path(os.environ.get("MATTERMOST_FLEET_FILE")),
            role_file=_optional_path(os.environ.get("MATTERMOST_ROLE_FILE")),
            harness=harness,
            grok_command=command,
            grok_model=model,
            grok_cwd=cwd,
            always_approve=_truthy(_env("AGENT_ALWAYS_APPROVE"), default=True),
            answer_mode=answer_mode,
            max_parallel_turns=max(1, max_parallel),
            show_thoughts=_truthy(_env("AGENT_SHOW_THOUGHTS"), default=False),
            text_slash=_truthy(_env("AGENT_TEXT_SLASH"), default=True),
            auto_rename_threads=_truthy(
                _env("AGENT_AUTO_RENAME_THREADS"), default=True
            ),
            inject_channel_context=_truthy(
                _env("AGENT_INJECT_CHANNEL_CONTEXT"), default=True
            ),
            post_max_lines=post_max_lines,
            status_tool_lines=status_tool_lines,
            status_done_tool_lines=status_done_tool_lines,
            status_progress_lines=status_progress_lines,
            slash_tokens=_csv(os.environ.get("MATTERMOST_SLASH_TOKEN"))
            | _csv(os.environ.get("MATTERMOST_SLASH_TOKENS"))
            | _token_file_csv(os.environ.get("MATTERMOST_SLASH_TOKEN_FILE")),
            slash_host=os.environ.get("MATTERMOST_SLASH_HOST", "127.0.0.1").strip()
            or "127.0.0.1",
            slash_port=slash_port,
            db_path=db,
            log_file=log_file,
            attachments_dir=attachments_dir,
            attachments_max_bytes=int(attachments_max_mb * 1024 * 1024),
        )
