"""Grok Build backend: long-lived ``grok agent`` over ACP.

V1 uses ``mattermost_acp.acp.AcpBroker`` directly. This module exists so
Claude Code / Cursor backends can land next to it without a rename.
"""

from mattermost_acp.acp import AcpBroker

__all__ = ["AcpBroker"]
