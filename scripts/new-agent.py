#!/usr/bin/env python3
"""Provision an explicitly configured assistant. Preview by default; --apply writes."""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from mattermost_acp import deployment, harness_switch  # noqa: E402

SECRETS = Path(
    os.environ.get("MATTERMOST_SECRETS_DIR")
    or Path.home() / ".config/mattermost-acp-gateway/secrets"
).expanduser()


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _die(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def _token() -> str:
    raw = _env("MATTERMOST_ADMIN_TOKEN_FILE")
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


def _request(
    base: str,
    token: str,
    method: str,
    path: str,
    body: dict | None = None,
    ok: set[int] | None = None,
) -> tuple[int, dict | list | None]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base.rstrip("/") + path,
        data=data,
        method=method,
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            status = resp.status
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        status = exc.code
        if ok and status in ok:
            try:
                parsed = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                parsed = None
            return status, parsed
        _die(f"Mattermost request failed: HTTP {status}")
    if not raw:
        return status, None
    parsed = json.loads(raw)
    return status, parsed


def _as_dict(payload: dict | list | None, what: str) -> dict:
    if not isinstance(payload, dict):
        _die(f"{what} returned {type(payload).__name__}")
    return payload


def _user_by_username(base: str, token: str, username: str) -> dict | None:
    status, payload = _request(
        base, token, "GET", f"/api/v4/users/username/{urllib.parse.quote(username)}", ok={404}
    )
    if status == 404:
        return None
    return _as_dict(payload, "users/username")


def _ensure_system_admin(base: str, token: str, user: dict) -> None:
    """Grant the explicitly requested administrator role."""
    roles = str(user.get("roles") or "").split()
    if "system_admin" in roles:
        return
    roles.append("system_admin")
    user_id = str(user.get("id") or "")
    _request(
        base,
        token,
        "PUT",
        f"/api/v4/users/{user_id}/roles",
        {"roles": " ".join(roles)},
    )
    print(f"granted system_admin to {user.get('username')}")


def _ensure_user(
    base: str, token: str, username: str, display: str, existing: bool, email: str
) -> dict:
    found = _user_by_username(base, token, username)
    if found:
        return found
    if existing:
        _die(f"no Mattermost user {username}")
    password = secrets.token_urlsafe(24)
    if not email:
        _die("--email is required to create an account")
    _status, payload = _request(
        base,
        token,
        "POST",
        "/api/v4/users",
        {
            "email": email,
            "username": username,
            "password": password,
            "nickname": display,
            "first_name": display,
        },
    )
    print(f"created user {username} (password discarded)")
    user = _as_dict(payload, "create user")
    return user


def _ensure_team(base: str, token: str, team_id: str, user_id: str) -> None:
    status, _ = _request(base, token, "GET", f"/api/v4/teams/{team_id}/members/{user_id}", ok={404})
    if status == 404:
        _request(
            base,
            token,
            "POST",
            f"/api/v4/teams/{team_id}/members",
            {"team_id": team_id, "user_id": user_id},
        )


def _channel_by_name(base: str, token: str, team_id: str, name: str) -> dict | None:
    status, payload = _request(
        base,
        token,
        "GET",
        f"/api/v4/teams/{team_id}/channels/name/{urllib.parse.quote(name)}",
        ok={404},
    )
    if status == 404:
        return None
    return _as_dict(payload, "channel by name")


def _ensure_private_channel(base: str, token: str, team_id: str, name: str, display: str) -> dict:
    found = _channel_by_name(base, token, team_id, name)
    if found:
        if found.get("type") != "P":
            _die("Existing home channel is not private")
        return found
    _status, payload = _request(
        base,
        token,
        "POST",
        "/api/v4/channels",
        {
            "team_id": team_id,
            "name": name,
            "display_name": display,
            "type": "P",
        },
    )
    print(f"created private channel {name}")
    return _as_dict(payload, "create channel")


def _add_member(base: str, token: str, channel_id: str, user_id: str) -> None:
    status, _ = _request(
        base, token, "GET", f"/api/v4/channels/{channel_id}/members/{user_id}", ok={404}
    )
    if status == 404:
        _request(
            base, token, "POST", f"/api/v4/channels/{channel_id}/members", {"user_id": user_id}
        )


def _ensure_token_file(base: str, token: str, user_id: str, username: str) -> Path:
    path = SECRETS / f"mattermost_user_{username}_pat"
    if path.is_file() and path.read_text(encoding="utf-8").strip():
        _, identity = _request(
            base, path.read_text(encoding="utf-8").strip(), "GET", "/api/v4/users/me"
        )
        if not isinstance(identity, dict) or identity.get("id") != user_id:
            _die("Existing token belongs to another account")
        print(f"kept existing token file {path}")
        return path
    _status, payload = _request(
        base,
        token,
        "POST",
        f"/api/v4/users/{user_id}/tokens",
        {"description": "mattermost-acp-gateway"},
    )
    body = _as_dict(payload, "create token")
    value = str(body.get("token") or "")
    if not value:
        _die("token create returned no token")
    SECRETS.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write(value + "\n")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    print(f"wrote token file {path}")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--username", required=True)
    parser.add_argument("--display", required=True)
    parser.add_argument("--email", default="")
    parser.add_argument("--role", type=Path)
    parser.add_argument("--cwd", type=Path, required=True)
    parser.add_argument("--harness", required=True)
    parser.add_argument("--home-name", default="")
    parser.add_argument("--peer-channel-id", default="")
    parser.add_argument("--existing", action="store_true")
    parser.add_argument("--system-admin", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--start", action="store_true")
    parser.add_argument("--no-start", action="store_true")
    args = parser.parse_args()
    username = args.username.strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", username):
        parser.error("invalid username")
    base = _env("MATTERMOST_URL")
    team = _env("MATTERMOST_TEAM_ID")
    if not base or not team:
        parser.error("MATTERMOST_URL and MATTERMOST_TEAM_ID are required")
    role = args.role.expanduser().absolute() if args.role else None
    if role and not role.is_file():
        parser.error("selected role file is missing")
    profile = harness_switch.load_profiles().get(args.harness)
    if profile is None:
        parser.error("selected harness profile is missing")
    roster_path = deployment.selected_path("roster")
    roster = (
        deployment.load_inventory(roster_path)
        if roster_path.exists()
        else {"schema_version": 1, "agents": []}
    )
    previous = next((a for a in roster["agents"] if a["username"] == username), None)
    if previous:
        parser.error("assistant already configured; use set-harness or edit its private settings")
    cwd = args.cwd.expanduser().absolute()
    if not cwd.is_dir():
        parser.error("selected working directory is missing")
    if not args.existing and not args.email:
        parser.error("--email is required unless --existing is selected")
    config = roster_path.parent
    env_path = config / f"{username}.env"
    unit = f"mattermost-acp-gateway-{username}.service"
    unit_dir = Path.home() / ".config/systemd/user"
    unit_path = unit_dir / unit
    if env_path.exists() or env_path.is_symlink() or unit_path.exists() or unit_path.is_symlink():
        parser.error("env or unit already exists; reconcile existing resources first")
    token_path = SECRETS / f"mattermost_user_{username}_pat"
    if token_path.exists() and (not token_path.is_file() or not token_path.read_text().strip()):
        parser.error("existing token file is unusable")
    values = harness_switch.target_env(profile, model=None)
    text_inputs = [
        *values.values(),
        str(cwd),
        str(role or ""),
        base,
        team,
        str(roster_path),
        str(deployment.selected_path("profiles")),
        str(token_path),
        args.display,
        args.email,
        args.home_name,
        args.peer_channel_id,
    ]
    if any("\n" in value or "\r" in value or "\x00" in value for value in text_inputs):
        parser.error("configuration values must be single-line text")
    executable = ROOT / ".venv/bin/mattermost-acp-gateway"
    if not executable.is_file():
        parser.error("install the gateway in this checkout before provisioning")
    print(f"Provision {username} using {args.harness}; configuration {roster_path}")
    print(f"Administrator role requested: {args.system_admin}; start requested: {args.start}")
    if not args.apply or args.dry_run:
        print("Preview only. Use --apply to create/rebind resources.")
        return
    token = _token()
    _, owner = _request(base, token, "GET", "/api/v4/users/me")
    user = _ensure_user(base, token, username, args.display, args.existing, args.email)
    if args.system_admin:
        _ensure_system_admin(base, token, user)
    user_id = user["id"]
    _ensure_team(base, token, team, user_id)
    channel = _ensure_private_channel(base, token, team, args.home_name or username, args.display)
    for member in [owner["id"], user_id]:
        _add_member(base, token, channel["id"], member)
    if args.peer_channel_id:
        _add_member(base, token, args.peer_channel_id, user_id)
    token_file = _ensure_token_file(base, token, user_id, username)
    config.mkdir(parents=True, exist_ok=True, mode=0o700)
    state = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
    state = state / "mattermost-acp-gateway" / username
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    values.update(
        {
            "MATTERMOST_URL": base,
            "MATTERMOST_DOTENV": "none",
            "MATTERMOST_TOKEN_FILE": str(token_file),
            "MATTERMOST_CHANNEL_ID": channel["id"],
            "MATTERMOST_ALLOW_USER_IDS": owner["id"],
            "MATTERMOST_TEAM_ID": team,
            "MATTERMOST_AUTO_JOIN_CHANNELS": "0",
            "MATTERMOST_SLASH_PORT": "0",
            "MATTERMOST_CONFIG_DIR": str(config),
            "MATTERMOST_ROSTER_FILE": str(roster_path),
            "MATTERMOST_HARNESS_PROFILES_FILE": str(deployment.selected_path("profiles")),
            "AGENT_CWD": str(args.cwd.expanduser().absolute()),
            "AGENT_ALWAYS_APPROVE": "0",
            "MATTERMOST_ACP_DB": str(state / "threads.db"),
        }
    )
    if role:
        values["MATTERMOST_ROLE_FILE"] = str(role)
    if args.peer_channel_id:
        values["MATTERMOST_PEER_CHANNEL_ID"] = args.peer_channel_id
    if any("\n" in value or "\r" in value for value in values.values()):
        parser.error("configuration values must not contain newlines")
    fd = os.open(env_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write("".join(f"{k}={json.dumps(v)}\n" for k, v in values.items()))
    unit_dir.mkdir(parents=True, exist_ok=True)
    with unit_path.open("x") as unit_stream:
        unit_stream.write(
            "[Unit]\nDescription=Mattermost ACP assistant\nAfter=network.target\n"
            "[Service]\nType=simple\n"
            f"WorkingDirectory={json.dumps(str(cwd).replace('%', '%%'))}\n"
            f"EnvironmentFile={json.dumps(str(env_path).replace('%', '%%'))}\n"
            f"ExecStart={json.dumps(str(executable).replace('%', '%%'))}\n"
            "Restart=on-failure\nKillMode=mixed\n[Install]\nWantedBy=default.target\n"
        )
    roster["agents"].append(
        {
            "username": username,
            "display": args.display,
            "user_id": user_id,
            "home_channel_id": channel["id"],
            "unit": unit,
            "env_file": str(env_path),
            "harness": args.harness,
            **({"role": str(role)} if role else {}),
        }
    )
    harness_switch._atomic_write(roster_path, json.dumps(roster, indent=2) + "\n")
    if args.start and not args.no_start:
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
        subprocess.run(["systemctl", "--user", "enable", "--now", unit], check=True)
    print(f"Configured {username}; env and state remain private")


if __name__ == "__main__":
    main()
