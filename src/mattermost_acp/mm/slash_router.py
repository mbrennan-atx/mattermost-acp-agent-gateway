"""Route team-wide slash commands to the gateway that owns the channel.

Mattermost custom slash commands belong to a team, not a channel, and each
trigger has one URL. Every trigger points at one reachable gateway; that
gateway forwards the POST to whichever fleet gateway's home channel the
command was typed in, using ``agents/roster.json`` and each agent's
``MATTERMOST_SLASH_PORT``. The roster is reread on every request so a new
agent is routable without restarting the router.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from mattermost_acp import deployment

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
ROSTER_PATH = ROOT / "agents" / "roster.json"


def _env_port(env_path: Path) -> int | None:
    try:
        lines = env_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    for line in lines:
        if line.startswith("MATTERMOST_SLASH_PORT="):
            raw = line.split("=", 1)[1].strip()
            return int(raw) if raw.isdigit() and int(raw) > 0 else None
    return None


def load_routes(roster_path: Path | None = None) -> dict[str, int]:
    """Map home channel id -> local slash port for every roster agent."""
    path = deployment.selected_path("roster", roster_path)
    explicit = roster_path is not None or bool(os.environ.get("MATTERMOST_ROSTER_FILE"))
    if not explicit and not path.exists():
        return {}
    roster = deployment.read_document(path, allow_legacy=True)
    modern = "schema_version" in roster
    if modern:
        roster = deployment.load_inventory(path)
    base = deployment.document_base(path, roster)
    routes: dict[str, int] = {}
    for agent in roster.get("agents", []):
        channel = str(agent.get("home_channel_id") or "")
        env_file = agent.get("env_file")
        if not channel or not env_file:
            continue
        env_path = deployment.resolve_path(env_file, base)
        port = deployment.env_port(env_path) if modern else _env_port(env_path)
        if port:
            routes[channel] = port
    return routes


def route_port(channel_id: str, own_port: int, roster_path: Path | None = None) -> int | None:
    """Port to forward to, or None when this gateway should handle it."""
    port = load_routes(roster_path).get(channel_id)
    if port is None or port == own_port:
        return None
    return port
