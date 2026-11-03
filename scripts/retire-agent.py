#!/usr/bin/env python3
"""Retire a configured assistant. Preview unless --yes.

Deactivates the account, archives its channel, and removes its unit and roster
entry. Files are retained unless explicitly listed in owned_paths. Shared
resources, other assistants' resources, and the administrator token are retained.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fleet_roster

ROOT = fleet_roster.ROOT
ROSTER = fleet_roster.ROSTER_PATH
SECRETS = Path.home() / ".config/mattermost-acp-gateway/secrets"
UNIT_DIR = Path.home() / ".config" / "systemd" / "user"


@dataclass(frozen=True)
class Step:
    kind: str
    detail: str
    path: str = ""
    api: str = ""


def _die(message: str) -> None:
    raise SystemExit(message)


def _resolve(root: Path, raw: str) -> Path:
    return fleet_roster.deployment.resolve_path(raw, root).absolute()


def _agent(roster: dict, username: str) -> dict:
    for item in fleet_roster.agents(roster):
        if item["username"] == username:
            return item
    _die("No matching roster entry")


def _references(agent: dict, root: Path) -> set[Path]:
    paths = {
        _resolve(root, value)
        for key in ("env_file", "role", "grok_home")
        if (value := agent.get(key))
    }
    paths.update(_resolve(root, raw) for raw in agent.get("owned_paths", []))
    env = agent.get("env_file")
    if env:
        from dotenv import dotenv_values

        values = dotenv_values(_resolve(root, env), interpolate=False)
        for key in (
            "MATTERMOST_ACP_DB",
            "MATTERMOST_ACP_LOG_FILE",
            "MATTERMOST_TOKEN_FILE",
            "MATTERMOST_SHARED_ROLE_FILE",
            "MATTERMOST_ROLE_FILE",
            "GROK_HOME",
            "CODEX_HOME",
            "CLAUDE_CONFIG_DIR",
            "AGENT_CWD",
        ):
            if values.get(key):
                paths.add(_resolve(root, values[key]))
    return paths


def _overlaps(first: Path, second: Path) -> bool:
    # Include both ancestors and descendants. A directory claim cannot consume
    # a shared child, and a child claim cannot consume part of a shared home.
    return first == second or first in second.parents or second in first.parents


def remove_tree_unlink(path: Path) -> None:
    """Delete a home directory. Symlinks are unlinked, not followed."""
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_symlink() or not child.is_dir():
            child.unlink()
        else:
            remove_tree_unlink(child)
    path.rmdir()


def plan(
    agent: dict,
    roster: dict,
    *,
    force: bool,
    root: Path = ROOT,
    secrets: Path = SECRETS,
    unit_dir: Path = UNIT_DIR,
    home_root: Path | None = None,
) -> list[Step]:
    username = str(agent["username"])
    if username in fleet_roster.protected_names(roster) and not force:
        _die(f"refusing {username} without --force")
    home_root = home_root or Path.home()
    steps: list[Step] = []
    unit = fleet_roster.unit_for(agent)
    if Path(unit).name != unit or not unit.endswith(".service"):
        _die("Invalid service unit")
    unit_file = unit_dir / unit
    if any(
        fleet_roster.unit_for(other) == unit
        for other in roster["agents"]
        if other["username"] != username
    ):
        _die("Unit is shared by another assistant")
    steps.extend(
        [
            Step("disable-unit", f"disable and stop {unit}", path=unit),
            Step("delete-unit-file", f"delete {unit_file}", path=str(unit_file)),
            Step("daemon-reload", "reload user units"),
        ]
    )
    for key, endpoint in (("user_id", "users"), ("home_channel_id", "channels")):
        ident = agent.get(key)
        if not isinstance(ident, str) or not ident.isalnum():
            _die(f"Invalid or missing {key}")
        if any(
            other.get(key) == ident for other in roster["agents"] if other["username"] != username
        ):
            _die(f"Shared {key}; refusing retirement")
        steps.append(
            Step(
                "http-delete",
                f"deactivate/archive {endpoint}/{ident}",
                api=f"/api/v4/{endpoint}/{ident}",
            )
        )
    protected = {_resolve(root, raw) for raw in roster.get("shared_paths", [])}
    token = os.environ.get("MATTERMOST_ADMIN_TOKEN_FILE")
    if token:
        protected.add(_resolve(root, token))
    for other in roster["agents"]:
        if other["username"] != username:
            protected.update(_references(other, root))
    for raw in agent.get("owned_paths", []):
        path = _resolve(root, raw)
        # A symlink itself can be unlinked, but never walk a symlinked parent.
        unsafe_parent = any(parent.is_symlink() for parent in path.parents)
        ident = path.resolve() if not path.is_symlink() else path
        reserved = {
            Path("/"),
            home_root.resolve(),
            root.resolve(),
            secrets.resolve(),
            unit_dir.resolve(),
        }
        shared = any(_overlaps(ident, p.resolve()) or _overlaps(path, p) for p in protected)
        if unsafe_parent or ident in reserved or shared:
            steps.append(Step("skip", f"retain protected/shared path {path}"))
        else:
            steps.append(
                Step(
                    "rm-home" if path.is_dir() and not path.is_symlink() else "rm-file",
                    f"remove declared owned path {path}",
                    path=str(path),
                )
            )
    steps.append(Step("roster-remove", f"remove {username} from roster"))
    for step in steps:
        if "permanent=true" in step.api or "permanent=true" in step.detail:
            _die("refusing a plan that requests permanent deletion")
    return steps


def _token() -> str:
    raw = os.environ.get("MATTERMOST_ADMIN_TOKEN_FILE", "").strip()
    if not raw:
        _die("MATTERMOST_ADMIN_TOKEN_FILE is required")
    path = Path(raw).expanduser()
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        _die(f"cannot read admin token {path}: {exc}")
    if not token:
        _die(f"admin token file is empty: {path}")
    return token


def _http_delete(base: str, token: str, api: str) -> None:
    if "permanent" in api:
        _die(f"refusing {api}")
    req = urllib.request.Request(
        base.rstrip("/") + api,
        method="DELETE",
        headers={"Authorization": "Bearer " + token},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:200]
        gone = exc.code == 404 or (
            exc.code == 400
            and any(
                word in detail.lower() for word in ("already", "archived", "deleted", "inactive")
            )
        )
        if gone:
            print(f"already gone {api} -> {exc.code}: {detail}")
            return
        raise SystemExit(f"DELETE {api} -> {exc.code}: {detail}") from exc


def _drop_roster(path: Path, username: str) -> None:
    roster = fleet_roster.load_roster(path)
    roster["agents"] = [item for item in roster["agents"] if item.get("username") != username]
    from mattermost_acp.harness_switch import _atomic_write

    _atomic_write(path, json.dumps(roster, indent=2) + "\n")


def apply_steps(
    steps: list[Step],
    *,
    username: str,
    roster_path: Path,
    base: str,
    token: str,
    run,
) -> None:
    errors: list[str] = []
    for step in steps:
        print(step.detail)
        try:
            if step.kind == "skip":
                continue
            if step.kind == "disable-unit":
                run(["systemctl", "--user", "disable", "--now", step.path])
            elif step.kind == "delete-unit-file":
                unit_path = Path(step.path)
                if unit_path.is_symlink() or unit_path.is_file():
                    unit_path.unlink()
            elif step.kind == "daemon-reload":
                run(["systemctl", "--user", "daemon-reload"])
            elif step.kind == "http-delete":
                _http_delete(base, token, step.api)
            elif step.kind == "rm-home":
                remove_tree_unlink(Path(step.path))
            elif step.kind == "rm-file":
                target = Path(step.path)
                if target.is_symlink() or target.is_file():
                    target.unlink()
                elif target.exists():
                    errors.append(f"not a file: {target}")
            elif step.kind == "roster-remove":
                _drop_roster(roster_path, username)
            else:
                errors.append(f"unknown step {step.kind}")
        except (OSError, SystemExit) as exc:
            message = exc.code if isinstance(exc, SystemExit) and exc.code else str(exc)
            errors.append(f"{step.detail}: {message}")
            _die(f"Retirement stopped; remaining steps were not applied: {message}")
    if errors:
        _die(f"{len(errors)} step(s) failed")


def _run(argv: list[str]) -> None:
    proc = subprocess.run(argv, check=False, text=True, capture_output=True)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise OSError(f"command failed ({proc.returncode}): {' '.join(argv)} {detail}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Retire a Mattermost agent (dry-run by default)")
    parser.add_argument("--username", required=True)
    parser.add_argument("--yes", action="store_true", help="perform the removal")
    parser.add_argument(
        "--force",
        action="store_true",
        help="allow retirement of a protected roster entry",
    )
    parser.add_argument("--roster", type=Path, default=ROSTER)
    args = parser.parse_args()
    username = args.username.strip().lower()
    roster = fleet_roster.load_roster(args.roster)
    agent = _agent(roster, username)
    steps = plan(
        agent,
        roster,
        force=args.force,
        root=fleet_roster.deployment.document_base(args.roster, roster),
    )
    if not args.yes:
        print(f"dry-run: retire {username}")
        for step in steps:
            print(f"dry-run: {step.detail}")
        print("dry-run: pass --yes to act")
        return
    base = os.environ.get("MATTERMOST_URL", "").strip()
    if not base:
        _die("MATTERMOST_URL is required")
    apply_steps(
        steps,
        username=username,
        roster_path=args.roster,
        base=base,
        token=_token(),
        run=_run,
    )
    print(json.dumps({"retired": username}))


if __name__ == "__main__":
    main()
