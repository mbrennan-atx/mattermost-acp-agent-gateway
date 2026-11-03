"""Switch a fleet agent between harnesses (grok, claude, codex, cursor).

Shared by ``scripts/set-harness`` and the ``/harness`` chat command. Stdlib
only so the script runs without the virtualenv.

A switch rewrites the managed keys in the agent's env file, records the
harness in the selected private roster, and restarts the unit once the agent has no
turn in flight. systemd rereads ``EnvironmentFile`` on every start, and
``EnvironmentFile`` values override the unit's ``Environment=`` lines.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from mattermost_acp import deployment

ROOT = Path(__file__).resolve().parents[2]
PROFILES_PATH = deployment.selected_path("profiles")
ROSTER_PATH = deployment.selected_path("roster")
BASE_PATH = ("~/.local/bin", "/usr/local/bin", "/usr/bin", "/bin")
CORE_KEYS = ("HARNESS", "AGENT_COMMAND", "AGENT_MODEL", "PATH")
# Pre-rename keys; a switch drops them so the env file carries one name.
LEGACY_KEYS = ("GROK_COMMAND", "GROK_MODEL")


class SwitchError(RuntimeError):
    pass


@dataclass(frozen=True)
class Profile:
    name: str
    command: str
    model: str
    path_prefix: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=dict)


def _expand(value: str, home: Path) -> str:
    if value == "~":
        return str(home)
    if value.startswith("~/"):
        return str(home / value[2:])
    return value


def load_profiles(path: Path | None = None, *, home: Path | None = None) -> dict[str, Profile]:
    home = home or Path.home()
    src = deployment.selected_path("profiles", path)
    try:
        data = deployment.load_profiles_document(src, allow_legacy=True)
    except (OSError, ValueError) as exc:
        raise SwitchError(f"cannot read harness profiles {src}: {exc}") from exc
    profiles: dict[str, Profile] = {}
    for name, raw in (data.get("harnesses") or {}).items():
        profiles[name] = Profile(
            name=name,
            command=(
                str(deployment.resolve_path(_expand(raw["command"], home), src.parent))
                if "/" in raw["command"]
                else raw["command"]
            ),
            model=str(raw.get("model") or ""),
            path_prefix=tuple(
                str(deployment.resolve_path(_expand(p, home), src.parent))
                for p in raw.get("path_prefix") or []
            ),
            env={k: _expand(str(v), home) for k, v in (raw.get("env") or {}).items()},
        )
    if not profiles:
        raise SwitchError(f"no harness profiles in {src}")
    return profiles


def managed_keys(profiles: dict[str, Profile]) -> set[str]:
    keys = set(CORE_KEYS) | set(LEGACY_KEYS)
    for profile in profiles.values():
        keys.update(profile.env)
    return keys


def target_env(profile: Profile, *, model: str | None, home: Path | None = None) -> dict[str, str]:
    home = home or Path.home()
    path = [*profile.path_prefix, *(_expand(p, home) for p in BASE_PATH)]
    out = {
        "HARNESS": profile.name,
        "AGENT_COMMAND": profile.command,
        "AGENT_MODEL": model or profile.model,
        "PATH": ":".join(dict.fromkeys(path)),
    }
    out.update(profile.env)
    return out


def rewrite_env_text(text: str, updates: dict[str, str], managed: set[str]) -> str:
    """Replace managed keys in place, drop stale ones, append missing ones."""

    def encode(value: str) -> str:
        if any(c in value for c in "\n\r\x00"):
            raise SwitchError("Harness settings must be single-line text")
        return json.dumps(value) if any(c.isspace() or c in "\"'\\" for c in value) else value

    updates = {key: encode(value) for key, value in updates.items()}
    out: list[str] = []
    written: set[str] = set()
    for line in text.splitlines():
        key = line.split("=", 1)[0].strip() if "=" in line else ""
        if key.startswith("export "):
            key = key[len("export ") :].strip()
        if key and not line.lstrip().startswith("#") and key in managed:
            if key in updates and key not in written:
                out.append(f"{key}={updates[key]}")
                written.add(key)
            continue
        out.append(line)
    missing = [k for k in updates if k not in written]
    if missing:
        if out and out[-1].strip():
            out.append("")
        out.append("# Harness (managed by scripts/set-harness)")
        out.extend(f"{k}={updates[k]}" for k in missing)
    return "\n".join(out) + "\n"


def read_env_value(text: str, key: str) -> str | None:
    for line in text.splitlines():
        if line.lstrip().startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip() == key:
            return v.strip().strip('"').strip("'")
    return None


def load_roster(path: Path | None = None) -> dict:
    try:
        return deployment.load_inventory(path, allow_legacy=True, check_files=False)
    except deployment.DeploymentError as exc:
        raise SwitchError(str(exc)) from None


def find_agent(roster: dict, *, username: str | None = None, user_id: str | None = None) -> dict:
    for agent in roster["agents"]:
        if username and agent.get("username") == username:
            return agent
        if user_id and agent.get("user_id") == user_id:
            return agent
    raise SwitchError(f"agent {username or user_id} is not in the roster")


def _atomic_write(path: Path, text: str) -> None:
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        if path.exists():
            tmp.chmod(path.stat().st_mode & 0o777)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def busy_path(db_path: Path) -> Path:
    """In-flight turn marker written by the gateway next to its thread database."""
    return db_path.with_suffix(".busy.json")


def db_path_for(env_text: str, root: Path = ROOT) -> Path:
    raw = read_env_value(env_text, "MATTERMOST_ACP_DB") or "./data/threads.db"
    path = Path(raw).expanduser()
    return path if path.is_absolute() else (root / path).resolve()


def busy_threads(marker: Path) -> list[str]:
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    pid = int(data.get("pid") or 0)
    if pid <= 0:
        return []
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return []
    except PermissionError:
        pass
    return [str(t) for t in data.get("threads") or []]


@dataclass
class SwitchPlan:
    username: str
    unit: str
    env_path: Path
    backup_path: Path | None
    old_harness: str
    new_harness: str
    model: str
    db_path: Path


def apply_switch(
    username: str,
    harness: str,
    *,
    model: str | None = None,
    root: Path = ROOT,
    roster_path: Path | None = None,
    profiles_path: Path | None = None,
    home: Path | None = None,
    dry_run: bool = False,
) -> SwitchPlan:
    """Rewrite the agent's env file and roster entry. Does not restart."""
    profiles = load_profiles(profiles_path, home=home)
    if harness not in profiles:
        raise SwitchError(f"unknown harness {harness!r}; choose {', '.join(sorted(profiles))}")
    profile = profiles[harness]
    if not Path(profile.command).is_file() or not os.access(profile.command, os.X_OK):
        found = shutil.which(profile.command)
        if not found:
            raise SwitchError(f"{harness} command not found: {profile.command}")

    roster_file = (
        deployment.selected_path("roster", roster_path)
        if roster_path is not None or root == ROOT
        else root / "agents" / "roster.json"
    )
    roster = load_roster(roster_file)
    agent = find_agent(roster, username=username)
    env_path = deployment.resolve_path(
        str(agent.get("env_file") or f".env.{username}"),
        deployment.document_base(roster_file, roster),
    )
    try:
        env_text = env_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SwitchError(f"cannot read {env_path}: {exc}") from exc

    old = read_env_value(env_text, "HARNESS") or str(agent.get("harness") or "grok")
    updates = target_env(profile, model=model, home=home)
    new_text = rewrite_env_text(env_text, updates, managed_keys(profiles))
    plan = SwitchPlan(
        username=username,
        unit=str(agent.get("unit") or f"mattermost-acp-gateway-{username}.service"),
        env_path=env_path,
        backup_path=None,
        old_harness=old,
        new_harness=harness,
        model=updates["AGENT_MODEL"],
        db_path=db_path_for(env_text, root),
    )
    if dry_run:
        return plan

    tag_legacy_rows(plan.db_path, old)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = env_path.parent / ".env-backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / f"{env_path.name}.{stamp}"
    backup.write_text(env_text, encoding="utf-8")
    os.chmod(backup, 0o600)
    plan.backup_path = backup
    _atomic_write(env_path, new_text)

    agent["harness"] = harness
    _atomic_write(roster_file, json.dumps(roster, indent=2) + "\n")
    return plan


def tag_legacy_rows(db_path: Path, harness: str) -> None:
    """Tag rows written before the harness column with the harness that wrote them.

    Without this, a database first opened after a switch would credit the old
    sessions to the new harness.
    """
    if not db_path.is_file():
        return
    con = sqlite3.connect(db_path, timeout=30)
    try:
        for table in ("threads", "model_settings"):
            cols = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
            if not cols:
                continue
            if "harness" not in cols:
                con.execute(f"ALTER TABLE {table} ADD COLUMN harness TEXT")
            con.execute(f"UPDATE {table} SET harness = ? WHERE harness IS NULL", (harness,))
        con.commit()
    finally:
        con.close()


def wait_idle(db_path: Path, *, timeout_s: float, poll_s: float = 5.0) -> list[str]:
    """Wait for the agent to finish in-flight turns. Returns threads still busy."""
    marker = busy_path(db_path)
    deadline = time.monotonic() + timeout_s
    while True:
        busy = busy_threads(marker)
        if not busy or time.monotonic() >= deadline:
            return busy
        time.sleep(poll_s)


def restart_unit(unit: str) -> None:
    subprocess.run(["systemctl", "--user", "restart", unit], check=True)


def unit_active(unit: str) -> bool:
    res = subprocess.run(["systemctl", "--user", "is-active", unit], capture_output=True, text=True)
    return res.stdout.strip() == "active"
