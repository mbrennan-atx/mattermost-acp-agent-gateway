"""Mattermost REST + WebSocket adapter."""

from .client import MattermostClient, MattermostError, PostedEvent
from .slash import SlashRequest, parse_slash_payload, slash_authorized

__all__ = [
    "MattermostClient",
    "MattermostError",
    "PostedEvent",
    "SlashRequest",
    "parse_slash_payload",
    "slash_authorized",
]
