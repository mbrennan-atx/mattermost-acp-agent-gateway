"""Reassemble streamed agent text without changing its contents."""

from __future__ import annotations


def join_text_chunks(parts: list[str]) -> str:
    """Concatenate ACP text deltas verbatim.

    Transport boundaries can fall anywhere, including before a Markdown
    closing delimiter or within code and URLs. They are not paragraph breaks.
    """
    return "".join(parts)
