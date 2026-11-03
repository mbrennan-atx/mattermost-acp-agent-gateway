"""Mattermost thread title helpers.

Mattermost has no separate thread-name field. The channel-list / thread-pane
header is the **root post message**. The gateway asks the agent for a short
topic title, prepends it as the first line of that root, and strips the
control line from the visible answer.
"""

from __future__ import annotations

import re

_MAX_TITLE_LENGTH = 80
MAX_RENAME_ATTEMPTS = 3

_RENAME_LINE_RE = re.compile(
    r"^\s*(?:MATTERMOST_THREAD_RENAME|DISCORD_THREAD_RENAME)\s*:\s*(.+?)\s*$",
    re.MULTILINE | re.IGNORECASE,
)

_RENAME_ALT_RE = re.compile(
    r"^\s*(?:THREAD_RENAME|THREAD_TITLE|MATTERMOST_TITLE)\s*[:：]\s*(.+?)\s*$",
    re.MULTILINE | re.IGNORECASE,
)

_PREFIX_RE = re.compile(
    r"^(?:title|here'?s?\s+(?:a\s+)?(?:suggested\s+)?title|suggested title)\s*[:：]\s*",
    re.IGNORECASE,
)

_HEADING_RE = re.compile(r"^\*\*(.+?)\*\*\s*$")

_PROVISIONAL_NAMES = frozenset(
    {
        "",
        "new chat",
        "new session",
        "grok",
        "thread",
        "(unnamed)",
        "(kept)",
    }
)


def sanitize_title(raw: str, *, max_length: int = _MAX_TITLE_LENGTH) -> str:
    """Normalize a free-form title into a one-line header."""
    text = (raw or "").strip()
    text = _PREFIX_RE.sub("", text).strip()
    text = text.strip("`\"'")
    text = re.sub(r"[*_`#\[\]]+", "", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip(" .;:|")
    if not text:
        return ""
    if len(text) > max_length:
        cut = text[:max_length].rstrip()
        if " " in cut:
            cut = cut.rsplit(" ", 1)[0]
        text = cut or text[:max_length].rstrip()
    return text


def is_provisional_title(name: str | None) -> bool:
    """True if this header is still a stub or the user's raw first message."""
    n = (name or "").strip()
    if n.lower() in _PROVISIONAL_NAMES:
        return True
    if n.startswith("↗"):
        return True
    if len(n) > _MAX_TITLE_LENGTH:
        return True
    if n.endswith("…") or n.endswith("..."):
        return True
    if re.match(
        r"^(ok|okay|hey|hi|hello|can you|could you|please|i |my |how do|what |"
        r"why |is there|are there|do we|should we|go ahead|help|fix this|"
        r"look at|check |need |want |try |make |add |create |update |write )",
        n,
        re.I,
    ):
        return True
    if n.endswith("?"):
        return True
    return False


def heading_from_root(message: str) -> str:
    """Best current header from a root-post body (bold first line, else first line)."""
    first = next((ln.strip() for ln in (message or "").splitlines() if ln.strip()), "")
    match = _HEADING_RE.match(first)
    if match:
        return match.group(1).strip()
    return first


def compose_titled_root(original: str, title: str) -> str:
    """Prepend or replace a ``**Title**`` first line on the root post."""
    clean = sanitize_title(title)
    if not clean:
        return original or ""
    heading = f"**{clean}**"
    body = (original or "").strip()
    if not body:
        return heading
    lines = body.splitlines()
    if lines and _HEADING_RE.match(lines[0].strip()):
        rest = "\n".join(lines[1:]).lstrip()
        return f"{heading}\n\n{rest}" if rest else heading
    return f"{heading}\n\n{body}"


def extract_rename_directive(answer: str) -> tuple[str, str | None]:
    """Pull ``MATTERMOST_THREAD_RENAME:`` (or Discord/alt forms) out of an answer.

    Returns ``(cleaned_answer, title_or_None)``. Last match wins.
    """
    if not answer:
        return answer, None

    matches = list(_RENAME_LINE_RE.finditer(answer))
    alt = list(_RENAME_ALT_RE.finditer(answer))
    chosen = matches or alt
    if not chosen:
        return answer, None

    title = sanitize_title(chosen[-1].group(1))
    cleaned = _RENAME_LINE_RE.sub("", answer)
    cleaned = _RENAME_ALT_RE.sub("", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned, title or None


def rename_instruction(current_name: str) -> str:
    """Prefix injected until the gateway successfully titles the root post."""
    name = (current_name or "").strip() or "(unnamed)"
    return (
        "## Mattermost thread title\n"
        "This conversation is in a Mattermost thread. The channel-list / "
        "thread-pane header is the **root post**, currently:\n"
        f'"{name}"\n'
        "On this turn, choose a short logical thread title that captures "
        "the topic (≤80 chars, plain text, no quotes, no markdown).\n"
        "The current header is a placeholder, a slash stub, or the user's "
        "raw first message — it is NOT the final title. Always propose a "
        "better topic summary (not a question, not a sentence fragment of "
        "the user message).\n"
        "Include exactly one line in your final answer:\n"
        "MATTERMOST_THREAD_RENAME: <title>\n"
        "The gateway updates the root post's first line to that title and "
        "strips this line from what the user sees. Do this every turn until "
        "the header has been updated.\n"
        "---\n"
    )


def fallback_title_from_prompt(user_text: str, *, max_length: int = 60) -> str:
    """Last-resort title from the user message if the model never emitted a rename."""
    text = (user_text or "").strip()
    if text.startswith("<channel_context>"):
        parts = text.split("</channel_context>", 1)
        text = parts[1].strip() if len(parts) > 1 else text
    if text.startswith("## Mattermost thread title"):
        if "\n---\n" in text:
            text = text.split("\n---\n", 1)[1].strip()
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and not line.startswith("<"):
            text = line
            break
    text = re.sub(r"^/+\s*", "", text)
    return sanitize_title(text, max_length=max_length)
