"""Cursor advertises effort as a parameter inside each model ID."""

from mattermost_acp.acp.broker import AcpError


def model_effort(model: str | None) -> str | None:
    if not model or "[" not in model or not model.endswith("]"):
        return None
    for item in model.split("[", 1)[1][:-1].split(","):
        key, _, value = item.partition("=")
        if key in {"effort", "reasoning", "reasoning_effort"}:
            return value
    return None


def with_effort(model: str, effort: str | None) -> str:
    if not effort:
        return model
    if model_effort(model) is None:
        raise AcpError(
            "This Cursor model does not advertise an effort parameter; "
            "select an exact model from /model."
        )
    base, params = model.split("[", 1)
    parts = []
    for item in params[:-1].split(","):
        key, _, value = item.partition("=")
        parts.append(
            f"{key}={effort}" if key in {"effort", "reasoning", "reasoning_effort"} else item
        )
    return base + "[" + ",".join(parts) + "]"
