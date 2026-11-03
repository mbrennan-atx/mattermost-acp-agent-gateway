"""Read the operator-selected fleet inventory for optional administration tools."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from mattermost_acp import deployment  # noqa: E402

ROSTER_PATH = deployment.selected_path("roster")


def protected_names(roster: dict) -> frozenset[str]:
    return frozenset(str(a["username"]) for a in agents(roster) if a.get("protected"))


def default_unit(username: str) -> str:
    return f"mattermost-acp-gateway-{username}.service"


def default_env_file(username: str) -> str:
    return f".env.{username}"


def load_roster(path: Path | None = None) -> dict:
    try:
        return deployment.load_inventory(path, allow_legacy=True, check_files=False)
    except deployment.DeploymentError as exc:
        raise SystemExit(str(exc)) from None


def agents(roster: dict) -> list[dict]:
    found: list[dict] = []
    for item in roster["agents"]:
        if isinstance(item, dict) and str(item.get("username") or "").strip():
            found.append(item)
    return found


def unit_for(agent: dict) -> str:
    unit = str(agent.get("unit") or "").strip()
    return unit or default_unit(str(agent["username"]))


def env_file_for(agent: dict) -> str:
    rel = str(agent.get("env_file") or "").strip()
    return rel or default_env_file(str(agent["username"]))


def harness_for(agent: dict) -> str:
    return str(agent.get("harness") or "").strip() or "grok"


def _add_unique(homes: list[Path], seen: set[Path], path: Path) -> None:
    key = path.expanduser()
    try:
        ident = key.resolve() if key.exists() else key
    except OSError:
        ident = key
    if ident in seen:
        return
    seen.add(ident)
    homes.append(key)


def fleet_homes(roster: dict, *, extra: Path | None = None) -> list[Path]:
    """~/.grok plus each roster grok_home, de-duplicated."""
    homes: list[Path] = []
    seen: set[Path] = set()
    _add_unique(homes, seen, extra if extra is not None else Path.home() / ".grok")
    for agent in agents(roster):
        raw = str(agent.get("grok_home") or "").strip()
        if raw:
            _add_unique(homes, seen, Path(raw))
    return homes


def fleet_units(roster: dict) -> list[str]:
    units: list[str] = []
    seen: set[str] = set()
    for agent in agents(roster):
        unit = unit_for(agent)
        if unit not in seen:
            seen.add(unit)
            units.append(unit)
    return units


def nudge_usernames(roster: dict) -> list[str]:
    names = [str(agent["username"]) for agent in agents(roster) if agent.get("nudge_enabled", True)]
    return sorted(names)


def config_source(roster: dict, dest_home: Path) -> Path | None:
    """Copy config from a fleet home using configured source priorities."""
    try:
        dest_resolved = dest_home.expanduser().resolve()
    except OSError:
        dest_resolved = dest_home.expanduser()
    ranked: list[tuple[tuple[int, int], Path]] = []
    for agent in agents(roster):
        raw = str(agent.get("grok_home") or "").strip()
        if not raw:
            continue
        home = Path(raw).expanduser()
        path = home / "config.toml"
        if not path.is_file():
            continue
        try:
            if home.resolve() == dest_resolved:
                continue
        except OSError:
            continue
        rank = (int(agent.get("config_source_priority", 0)), 0)
        ranked.append((rank, path))
    if ranked:
        ranked.sort(key=lambda item: item[0])
        return ranked[0][1]
    fallback = Path.home() / ".grok" / "config.toml"
    try:
        fallback_home = fallback.parent.resolve()
    except OSError:
        fallback_home = fallback.parent
    if fallback.is_file() and fallback_home != dest_resolved:
        return fallback
    return None
