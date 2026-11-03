"""Read-only deployment validation. Does not connect, launch, or create state."""

from __future__ import annotations

import os
import shutil

from mattermost_acp import deployment
from mattermost_acp.channel_context import load_role_text
from mattermost_acp.config import Config


def check() -> None:
    config = Config.load()
    if not config.grok_cwd.is_dir():
        raise ValueError("AGENT_CWD must select an existing directory")
    if not shutil.which(config.grok_command):
        raise ValueError("AGENT_COMMAND must select an installed executable")
    if not 0 <= config.slash_port <= 65535:
        raise ValueError("MATTERMOST_SLASH_PORT is out of range")
    load_role_text(config.fleet_file, config.role_file)
    for kind, setting in (
        ("roster", "MATTERMOST_ROSTER_FILE"),
        ("profiles", "MATTERMOST_HARNESS_PROFILES_FILE"),
    ):
        path = deployment.selected_path(kind)
        if os.environ.get(setting) or path.exists():
            loader = (
                deployment.load_inventory if kind == "roster" else deployment.load_profiles_document
            )
            loader(path, allow_legacy=True)
