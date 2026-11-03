"""Command catalog: gateway operations and harness-specific passthroughs."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

_TEXT_CMD_RE = re.compile(r"^/([a-zA-Z][\w-]*)(?:\s+(.*))?$", re.DOTALL)


@dataclass(frozen=True)
class PassthroughCmd:
    """Grok TUI/shell slash command exposed through Mattermost."""

    name: str
    grok_name: str
    description: str
    arg_name: str | None = None
    arg_description: str | None = None
    text_aliases: tuple[str, ...] = ()


GATEWAY_COMMANDS: tuple[str, ...] = (
    "stop",
    "hint",
    "new",
    "clear",
    "status",
    "help",
    "restart",
    "mcps",
    "mcp",
    "model",
    "models",
    "effort",
    "harness",
)

GROK_PASSTHROUGH: tuple[PassthroughCmd, ...] = (
    PassthroughCmd(
        name="session-info",
        grok_name="session-info",
        description="Grok session details (auth, model, turns, context)",
        text_aliases=("info",),
    ),
    PassthroughCmd(
        name="context",
        grok_name="context",
        description="Context window breakdown for this Grok session",
    ),
    PassthroughCmd(
        name="compact",
        grok_name="compact",
        description="Compress conversation history to free context",
        arg_name="note",
        arg_description="Optional note about what to keep",
    ),
    PassthroughCmd(
        name="doctor",
        grok_name="doctor",
        description="Grok environment / terminal diagnostic report",
    ),
    PassthroughCmd(
        name="workflows",
        grok_name="workflows",
        description="List or inspect Grok workflow runs (TUI dashboard is text-only here)",
    ),
    PassthroughCmd(
        name="workflow",
        grok_name="workflow",
        description="Launch or manage a Grok workflow (pause/resume/stop/save)",
        arg_name="args",
        arg_description="Name, JSON args, or pause|resume|stop|save <run>",
        text_aliases=("wf",),
    ),
)

_PASSTHROUGH_BY_NAME: dict[str, PassthroughCmd] = {}
for _cmd in GROK_PASSTHROUGH:
    _PASSTHROUGH_BY_NAME[_cmd.name] = _cmd
    for _alias in _cmd.text_aliases:
        _PASSTHROUGH_BY_NAME[_alias] = _cmd


def resolve_passthrough_prompt(cmd: str, arg: str = "") -> str | None:
    """Map a text/slash command to a Grok prompt, or None if not a passthrough."""
    key = cmd.strip().lower().lstrip("/")
    spec = _PASSTHROUGH_BY_NAME.get(key)
    if spec is None:
        return None
    arg = (arg or "").strip()
    if arg:
        return f"/{spec.grok_name} {arg}"
    return f"/{spec.grok_name}"


def text_command_names() -> frozenset[str]:
    names = set(GATEWAY_COMMANDS)
    for cmd in GROK_PASSTHROUGH:
        names.add(cmd.name)
        names.update(cmd.text_aliases)
    return frozenset(names)


TEXT_COMMANDS: frozenset[str] = text_command_names()

SLASH_VERBS: tuple[str, ...] = (
    "stop",
    "hint",
    "new",
    "status",
    "help",
    "restart",
    "mcps",
    "model",
    "effort",
    "harness",
) + tuple(c.name for c in GROK_PASSTHROUGH)


def parse_text_command(content: str) -> tuple[str, str] | None:
    """Return ``(name, arg)`` if ``content`` is a known ``/command``."""
    m = _TEXT_CMD_RE.match((content or "").strip())
    if not m:
        return None
    name = m.group(1).lower()
    if name not in TEXT_COMMANDS:
        return None
    return name, (m.group(2) or "").strip()


def help_text(*, home_channel_ids: Iterable[str], harness: str = "") -> str:
    homes = ", ".join(str(i) for i in sorted(home_channel_ids, key=str)) or "all channels"
    lines = [
        "**mattermost-acp-gateway**: agent harness over ACP",
        f"Chat in a thread under a configured channel (`{homes}`).",
        "Each root post maps to one session. The agent can propose a topic with "
        "`MATTERMOST_THREAD_RENAME:`; `/new` enables a new title.",
        "",
        "• `/stop`: cancel active work",
        "• `/hint <text>`: steer active work when the adapter supports it; "
        "idle hints do not start work",
        "• `/new` or `/clear`: start a fresh session in this thread",
        "• `/status`: session, working directory, model and activity",
        "• `/restart`: restart this gateway under systemd",
        "• `/mcps [name]`: connector status through the selected harness CLI",
        "• `/model [id] [effort] [scope]`: list or select a model for thread, channel or harness",
        "• `/effort [level]`: show or select supported reasoning effort",
        "• `/harness [grok|claude|codex|cursor] [model]`: switch using private profiles; "
        "restarts when idle and creates new native sessions",
        "• `/help`: this message",
        "",
        "A requested model switch can also emit `MATTERMOST_MODEL_SET:` in chat.",
        "Connectors, permissions and available models come from the configured native harness.",
    ]
    if harness == "grok":
        lines.extend(["", "**Grok passthroughs**"])
        for cmd in GROK_PASSTHROUGH:
            alias = f" (alias `/{'`, `/'.join(cmd.text_aliases)}`)" if cmd.text_aliases else ""
            lines.append(f"• `/{cmd.name}`: {cmd.description}{alias}")
    elif harness in {"claude", "codex"}:
        lines.append("`/context` and `/compact` use this adapter's supported session commands.")
    elif harness == "cursor":
        lines.append(
            "Cursor session/model features follow its adapter capabilities; "
            "Grok workflow commands are unavailable."
        )
    lines.extend(
        [
            "",
            "Text commands use message prefixes when `AGENT_TEXT_SLASH=1`. "
            "Registered Mattermost slash commands use the same handlers.",
        ]
    )
    return "\n".join(lines)
