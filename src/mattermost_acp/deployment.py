"""External deployment configuration. No account or checkout defaults."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class DeploymentError(ValueError):
    """Invalid selected deployment input; messages never include file contents."""


def config_root() -> Path:
    selected = os.environ.get("MATTERMOST_CONFIG_DIR")
    if selected:
        return Path(selected).expanduser().absolute()
    base = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return base.expanduser().absolute() / "mattermost-acp-gateway"


def resolve_path(raw: str | Path, base: Path) -> Path:
    path = Path(raw).expanduser()
    return path if path.is_absolute() else base / path


def selected_path(kind: str, path: Path | None = None) -> Path:
    selectors = {
        "roster": ("MATTERMOST_ROSTER_FILE", "roster.json"),
        "profiles": ("MATTERMOST_HARNESS_PROFILES_FILE", "harnesses.json"),
    }
    key, default = selectors[kind]
    return resolve_path(path or os.environ.get(key) or default, config_root())


def read_document(path: Path, *, allow_legacy: bool = False) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        raise DeploymentError(
            "Selected deployment document is unreadable or invalid JSON"
        ) from None
    if not isinstance(data, dict):
        raise DeploymentError("Deployment document must be an object")
    version = data.get("schema_version")
    if "schema_version" not in data and allow_legacy:
        return data
    if type(version) is not int or version != 1:
        raise DeploymentError("Unsupported deployment schema_version; expected 1")
    return data


def document_base(path: Path, data: dict[str, Any]) -> Path:
    """Legacy callers explicitly retain their old checkout-relative base."""
    return path.parent if "schema_version" in data else path.parent.parent


def env_port(path: Path) -> int | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        raise DeploymentError("Selected agent env_file is unreadable") from None
    for line in lines:
        key, sep, value = line.partition("=")
        if sep and key.strip() == "MATTERMOST_SLASH_PORT":
            try:
                port = int(value.strip().strip('"').strip("'"))
            except ValueError:
                raise DeploymentError("MATTERMOST_SLASH_PORT must be an integer") from None
            if not 0 <= port <= 65535:
                raise DeploymentError("MATTERMOST_SLASH_PORT must be between 0 and 65535")
            return port or None
    return None


def load_inventory(
    path: Path | None = None, *, allow_legacy: bool = False, check_files: bool = True
) -> dict[str, Any]:
    path = selected_path("roster", path)
    data = read_document(path, allow_legacy=allow_legacy)
    agents = data.get("agents")
    if not isinstance(agents, list):
        raise DeploymentError("Deployment agents must be a list")
    seen: dict[str, set[Any]] = {
        key: set() for key in ("username", "user_id", "home_channel_id", "unit", "port")
    }
    base = document_base(path, data)
    shared = data.get("shared_paths", [])
    if not isinstance(shared, list) or not all(isinstance(p, str) and p.strip() for p in shared):
        raise DeploymentError("shared_paths must be a list of nonempty paths")
    for agent in agents:
        if not isinstance(agent, dict):
            raise DeploymentError("Each agent must be an object")
        if not isinstance(agent.get("username"), str) or not agent["username"].strip():
            raise DeploymentError("Each agent requires a nonempty username")
        for flag in ("protected", "nudge_enabled"):
            if flag in agent and type(agent[flag]) is not bool:
                raise DeploymentError(f"Agent {flag} must be boolean")
        if "config_source_priority" in agent and type(agent["config_source_priority"]) is not int:
            raise DeploymentError("Agent config_source_priority must be an integer")
        owned = agent.get("owned_paths", [])
        if not isinstance(owned, list) or not all(isinstance(p, str) and p.strip() for p in owned):
            raise DeploymentError("Agent owned_paths must be a list of nonempty paths")
        for key in ("username", "user_id", "home_channel_id", "unit"):
            value = agent.get(key)
            if value is None or value == "":
                continue
            if not isinstance(value, str):
                raise DeploymentError(f"Agent {key} must be a string")
            value = value.strip().casefold() if key == "username" else value
            if value in seen[key]:
                raise DeploymentError(f"Duplicate agent {key}")
            seen[key].add(value)
        for key in ("env_file", "role"):
            raw = agent.get(key)
            if raw is not None and (not isinstance(raw, str) or not raw.strip()):
                raise DeploymentError(f"Agent {key} must be a nonempty path")
            if raw and check_files:
                selected = resolve_path(raw, base)
                if not selected.is_file():
                    raise DeploymentError(f"Selected agent {key} is missing")
                if key == "env_file":
                    port = env_port(selected)
                    if port is not None:
                        if port in seen["port"]:
                            raise DeploymentError("Duplicate agent slash port")
                        seen["port"].add(port)
    return data


def load_profiles_document(
    path: Path | None = None, *, allow_legacy: bool = False
) -> dict[str, Any]:
    data = read_document(selected_path("profiles", path), allow_legacy=allow_legacy)
    profiles = data.get("harnesses")
    if not isinstance(profiles, dict) or not profiles:
        raise DeploymentError("Harness profiles must be a nonempty object")
    for name, profile in profiles.items():
        if not name.strip() or not isinstance(profile, dict):
            raise DeploymentError("Invalid harness profile")
        command = profile.get("command")
        if not isinstance(command, str) or not command.strip():
            raise DeploymentError("Harness command must be a nonempty executable path")
        if not isinstance(profile.get("model", ""), str):
            raise DeploymentError("Harness model must be a string")
        prefixes = profile.get("path_prefix", [])
        if not isinstance(prefixes, list) or not all(isinstance(x, str) for x in prefixes):
            raise DeploymentError("Harness path_prefix must be a list of strings")
        env = profile.get("env", {})
        if not isinstance(env, dict) or not all(isinstance(v, str) for v in env.values()):
            raise DeploymentError("Harness env must map names to strings")
    return data
