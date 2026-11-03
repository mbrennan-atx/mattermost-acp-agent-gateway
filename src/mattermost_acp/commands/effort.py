"""Reasoning-effort helpers for Mattermost ``/model`` and ``/effort``."""

from __future__ import annotations

# Canonical levels Grok accepts (CLI / headless). Models may only support a
# subset; unknown levels are rejected by the gateway before the ACP call.
EFFORT_LEVELS: tuple[str, ...] = (
    "none",
    "minimal",
    "low",
    "medium",
    "high",
    "xhigh",
    "max",
)

EFFORT_LEVEL_SET: frozenset[str] = frozenset(EFFORT_LEVELS)

# Mattermost slash Choices (keep short; omit rarely used none/minimal/max if crowded
# — still accept them as free text / trailing token).
EFFORT_SLASH_CHOICES: tuple[str, ...] = (
    "low",
    "medium",
    "high",
    "xhigh",
    "max",
    "minimal",
    "none",
)


def normalize_effort(raw: str | None) -> str | None:
    """Return canonical effort level or None if empty/invalid."""
    if raw is None:
        return None
    level = raw.strip().lower()
    if not level:
        return None
    if level not in EFFORT_LEVEL_SET:
        return None
    return level


def parse_model_and_effort(arg: str) -> tuple[str, str | None]:
    """Split ``model_id [effort]`` or ``model_id:effort`` into (model, effort).

    Effort is only recognized when the trailing token (or ``:suffix``) is a
    known level so multi-token model names are not mis-parsed.
    """
    text = (arg or "").strip()
    if not text:
        return "", None

    # model:effort form
    if ":" in text and " " not in text:
        mid, _, eff = text.rpartition(":")
        level = normalize_effort(eff)
        if mid and level:
            return mid, level

    parts = text.split()
    if len(parts) >= 2:
        level = normalize_effort(parts[-1])
        if level:
            return " ".join(parts[:-1]), level
    return text, None
