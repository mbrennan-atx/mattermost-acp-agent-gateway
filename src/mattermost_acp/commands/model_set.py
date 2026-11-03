"""Parse ``MATTERMOST_MODEL_SET`` / ``DISCORD_MODEL_SET`` control lines.

The gateway applies the switch after the turn and strips the line from what
the user sees.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from mattermost_acp.commands.effort import EFFORT_LEVEL_SET, parse_model_and_effort

SCOPE_ALIASES: dict[str, str] = {
    "thread": "thread",
    "channel": "channel",
    "harness": "harness",
    "system": "harness",
}

_LINE_RE = re.compile(
    r"^\s*(?:MATTERMOST_MODEL_SET|DISCORD_MODEL_SET):\s*(.+?)\s*$",
    re.MULTILINE | re.IGNORECASE,
)

_ALT_RE = re.compile(
    r"^\s*(?:DISCORD_MODEL|MODEL_SET)\s*[:：]\s*(.+?)\s*$",
    re.MULTILINE | re.IGNORECASE,
)


@dataclass(frozen=True)
class ModelSetDirective:
    query: str
    effort: str | None = None
    scope: str = "thread"


def parse_model_set_payload(raw: str) -> ModelSetDirective | None:
    """Parse ``<id-or-alias> [effort] [thread|channel|harness|system]``."""
    text = (raw or "").strip().strip("`\"'")
    if not text:
        return None
    parts = text.split()
    scope = "thread"
    if parts:
        last = parts[-1].lower()
        prev = parts[-2].lower() if len(parts) >= 2 else ""
        if last in SCOPE_ALIASES:
            scope = SCOPE_ALIASES[parts.pop().lower()]
        elif last in EFFORT_LEVEL_SET and prev in SCOPE_ALIASES:
            effort_tok = parts.pop()
            scope = SCOPE_ALIASES[parts.pop().lower()]
            parts.append(effort_tok)
    query, effort = parse_model_and_effort(" ".join(parts))
    if not query:
        return None
    return ModelSetDirective(query=query, effort=effort, scope=scope)


def extract_model_set_directive(
    answer: str,
) -> tuple[str, ModelSetDirective | None]:
    """Pull ``MATTERMOST_MODEL_SET:`` / ``DISCORD_MODEL_SET:`` out of an answer.

    Returns ``(cleaned_answer, directive_or_None)``. Last match wins.
    """
    if not answer:
        return answer, None

    matches = list(_LINE_RE.finditer(answer))
    alt = list(_ALT_RE.finditer(answer))
    chosen = matches or alt
    cleaned = _LINE_RE.sub("", answer)
    cleaned = _ALT_RE.sub("", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    if not chosen:
        return cleaned, None
    return cleaned, parse_model_set_payload(chosen[-1].group(1))
