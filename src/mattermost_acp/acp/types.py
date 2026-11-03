"""ACP-facing types (subset used by the Discord gateway)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

StopReason = Literal[
    "end_turn",
    "max_tokens",
    "max_turn_requests",
    "refusal",
    "cancelled",
    "unknown",
]


@dataclass
class AcpCapabilities:
    load_session: bool = False
    resume: bool = False
    close: bool = False
    steering: bool = False
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class SessionUpdate:
    """Normalized session/update notification."""

    session_id: str
    kind: str  # agent_message_chunk | agent_thought_chunk | tool_call | …
    raw: dict[str, Any] = field(default_factory=dict)
    text: str | None = None
    message_id: str | None = None
    tool_call_id: str | None = None
    tool_title: str | None = None
    tool_kind: str | None = None
    tool_status: str | None = None
    usage_used: int | None = None
    usage_size: int | None = None
    model_id: str | None = None


@dataclass
class TurnResult:
    session_id: str
    stop_reason: StopReason
    answer_text: str
    thought_text: str
    tool_titles: list[str] = field(default_factory=list)
    usage_used: int | None = None
    usage_size: int | None = None
    model_id: str | None = None
