"""Fence-aware Mattermost chunker.

Mattermost caps a post at 16383 chars; stay under 15000. Fenced code blocks are
atomic: a fence that fits in one post is never split across posts. Only a fence
larger than the limit is close/reopened, and each piece is a complete fence.
Cuts never land inside inline code, a markdown link, or a URL.

The model may insert a line containing only ``MATTERMOST_CHUNK`` to force a post
boundary. Markers inside a fence stay literal.

Ported from the Discord gateway's ``discord_ui/chunker.py`` minus the GFM
table rewrite, since Mattermost renders pipe tables natively.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

DEFAULT_LIMIT = 15000

# The webapp collapses any post taller than 600px behind "Show more" (hardcoded
# MAX_POST_HEIGHT in show_more.tsx, no server or user setting). At ~22px per
# rendered line that is ~27 lines; 20 leaves room for narrower panes, where a
# long line wraps into more rendered rows than it does in the center channel.
DEFAULT_MAX_LINES = 20
# Assumed rendered width in characters before a line wraps (thread pane, not
# the wide center channel).
WRAP_COLS = 80

# Model-emitted post boundary (optional colon, nothing else on the line).
_CHUNK_MARK_RE = re.compile(r"(?m)^[ \t]*MATTERMOST_CHUNK:?[ \t]*\r?$")

_HEADING_LINE_RE = re.compile(r"(?m)^#{1,3}[ \t]+\S")
_LIST_ITEM_RE = re.compile(r"(?m)^(?:[-*][ \t]|\d+\.[ \t])")
_URL_TAIL_RE = re.compile(r"https?://[^\s]*$")
_UNCLOSED_LINK_LABEL_RE = re.compile(r"\[[^\]]*$")
_UNCLOSED_LINK_DEST_RE = re.compile(r"\[[^\]]*\]\([^)]*$")

# Fence: 3+ backticks, optional lang. Closer must have at least as many.
_FENCE_OPEN_RE = re.compile(r"^(\s*)(`{3,})([^`\n]*)\s*$")
_FENCE_CLOSE_RE = re.compile(r"^\s*(`{3,})\s*$")


@dataclass(frozen=True, slots=True)
class _Block:
    kind: str  # prose | fence | break
    text: str


def normalize_answer(text: str) -> str:
    """Collapse runaway blank lines and trim outer whitespace."""
    text = re.sub(r"\n{3,}", "\n\n", text or "")
    return text.strip()


def estimate_lines(text: str, *, wrap_cols: int = WRAP_COLS) -> int:
    """Rendered rows a post occupies, counting soft wraps at ``wrap_cols``.

    Fenced code scrolls horizontally instead of wrapping, so each fenced line
    costs exactly one row.
    """
    total = 0
    in_fence = False
    fence_n = 3
    for line in (text or "").split("\n"):
        if not in_fence:
            opened = _fence_open(line)
            if opened:
                in_fence = True
                fence_n = opened[0]
                total += 1
                continue
        elif _fence_close(line, fence_n):
            in_fence = False
            total += 1
            continue
        n = len(line.rstrip())
        if in_fence or n <= wrap_cols:
            total += 1
        else:
            total += -(-n // wrap_cols)
    return total


def _chars_for_lines(text: str, max_lines: int, *, wrap_cols: int = WRAP_COLS) -> int:
    """Character offset in ``text`` where the rendered-line budget runs out."""
    if max_lines <= 0:
        return 0
    used = 0
    pos = 0
    for line in text.split("\n"):
        n = len(line.rstrip())
        cost = 1 if n <= wrap_cols else -(-n // wrap_cols)
        if used + cost > max_lines:
            return pos + (max_lines - used) * wrap_cols
        used += cost
        pos += len(line) + 1
    return len(text)


def chunk_message(
    text: str,
    *,
    limit: int = DEFAULT_LIMIT,
    max_lines: int = DEFAULT_MAX_LINES,
) -> list[str]:
    body = (text or "").strip()
    if not body:
        return []
    blocks = _expand_markers(_parse_blocks(body))
    return _pack_blocks(blocks, limit, max_lines)


# ── blocks ──────────────────────────────────────────────────────────


def _fence_open(line: str) -> tuple[int, str] | None:
    m = _FENCE_OPEN_RE.match(line.rstrip("\n\r"))
    if not m:
        return None
    return len(m.group(2)), m.group(3).strip()


def _fence_close(line: str, n: int) -> bool:
    m = _FENCE_CLOSE_RE.match(line.rstrip("\n\r"))
    return bool(m and len(m.group(1)) >= n)


def _parse_blocks(text: str) -> list[_Block]:
    """Split into prose / fence blocks. An unclosed fence runs to EOF."""
    blocks: list[_Block] = []
    lines = text.splitlines(keepends=True)
    i = 0
    prose: list[str] = []

    def flush_prose() -> None:
        if prose:
            blocks.append(_Block("prose", "".join(prose)))
            prose.clear()

    while i < len(lines):
        opened = _fence_open(lines[i])
        if opened:
            flush_prose()
            n, _lang = opened
            fence = [lines[i]]
            i += 1
            while i < len(lines):
                fence.append(lines[i])
                if _fence_close(lines[i], n):
                    i += 1
                    break
                i += 1
            blocks.append(_Block("fence", "".join(fence)))
            continue
        prose.append(lines[i])
        i += 1
    flush_prose()
    return blocks


def _expand_markers(blocks: list[_Block]) -> list[_Block]:
    """Honor MATTERMOST_CHUNK in prose only. Markers inside fences stay literal."""
    out: list[_Block] = []
    for block in blocks:
        if block.kind != "prose" or not _CHUNK_MARK_RE.search(block.text):
            out.append(block)
            continue
        parts = _CHUNK_MARK_RE.split(block.text)
        for i, part in enumerate(parts):
            if i:
                out.append(_Block("break", ""))
            stripped = part.strip("\n")
            if stripped.strip():
                out.append(_Block("prose", stripped))
    return out


def _pack_blocks(
    blocks: list[_Block], max_chars: int, max_lines: int = DEFAULT_MAX_LINES
) -> list[str]:
    chunks: list[str] = []
    current = ""
    # Typical post size once the line budget bites; used for the heuristics
    # that decide whether topping up the current post is worth it.
    soft = min(max_chars, max(200, max_lines * WRAP_COLS))

    def over(text: str) -> bool:
        return len(text) > max_chars or estimate_lines(text) > max_lines

    def budget_for(text: str, *, room_lines: int, room_chars: int) -> int:
        """Chars of ``text`` that fit in the given line and char room."""
        return max(0, min(room_chars, _chars_for_lines(text, room_lines)))

    def emit(text: str) -> None:
        text = text.rstrip()
        if not text:
            return
        text, _lang = _close_open_fence(text)
        chunks.append(text)

    def flush() -> None:
        nonlocal current
        emit(current)
        current = ""

    def join(a: str, b: str) -> str:
        if not a:
            return b
        if not b:
            return a
        if a.endswith("\n") or b.startswith("\n"):
            return a + b
        return a + "\n" + b

    for block in blocks:
        if block.kind == "break":
            flush()
            continue

        text = block.text
        if block.kind == "fence":
            if over(text):
                flush()
                for piece in _split_huge_fence(text, max_chars, max_lines):
                    emit(piece)
                continue
            candidate = join(current, text)
            if current and over(candidate):
                flush()
                current = text
            else:
                current = candidate
            continue

        # prose
        if not over(text):
            candidate = join(current, text)
            if current and over(candidate):
                flush()
                current = text
            else:
                current = candidate
            continue

        remaining = text
        if current:
            sep = "" if current.endswith("\n") else "\n"
            room = budget_for(
                remaining,
                room_lines=max_lines - estimate_lines(current) - len(sep),
                room_chars=max_chars - len(current) - len(sep),
            )
            if room > max(32, soft // 6):
                split_at = _find_split_point(remaining, room)
                piece = remaining[:split_at].rstrip()
                if piece and not _cut_is_inside_inline(remaining, split_at):
                    current = join(current, piece)
                    remaining = remaining[split_at:].lstrip("\n")
            flush()
        for piece in _chunk_prose(remaining, max_chars, max_lines):
            emit(piece)

    flush()
    return chunks


def _split_huge_fence(
    text: str, max_chars: int, max_lines: int = DEFAULT_MAX_LINES
) -> list[str]:
    """Split an oversized fence into complete fences that each fit one post."""
    lines = text.splitlines(keepends=True)
    if not lines:
        return []
    opener = lines[0]
    opened = _fence_open(opener)
    n = opened[0] if opened else 3
    prefix = opener if opener.endswith("\n") else opener + "\n"
    body = lines[1:]
    if body and _fence_close(body[-1], n):
        body = body[:-1]
    close = "`" * n
    budget = max(8, max_chars - len(prefix) - len(close))
    # Opener and closer each take a rendered row.
    line_budget = max(1, max_lines - 2)

    chunks: list[str] = []
    buf: list[str] = []
    buf_len = 0

    def emit_buf() -> None:
        nonlocal buf, buf_len
        if not buf:
            return
        body_text = "".join(buf)
        if not body_text.endswith("\n"):
            body_text += "\n"
        chunks.append(prefix + body_text + close)
        buf = []
        buf_len = 0

    for line in body:
        if len(line) > budget:
            emit_buf()
            for i in range(0, len(line), budget):
                piece = line[i : i + budget]
                if not piece.endswith("\n"):
                    piece += "\n"
                chunks.append(prefix + piece + close)
            continue
        if buf and (buf_len + len(line) > budget or len(buf) >= line_budget):
            emit_buf()
        buf.append(line)
        buf_len += len(line)
    emit_buf()
    if not chunks:
        closed, _lang = _close_open_fence(text)
        return [closed]
    return chunks


def _chunk_prose(
    text: str, max_chars: int, max_lines: int = DEFAULT_MAX_LINES
) -> list[str]:
    if not text:
        return []

    def fits(t: str) -> bool:
        return len(t) <= max_chars and estimate_lines(t) <= max_lines

    if fits(text):
        return [text]
    chunks: list[str] = []
    remaining = text
    while remaining:
        if fits(remaining):
            chunks.append(remaining)
            break
        budget = max(1, min(max_chars, _chars_for_lines(remaining, max_lines)))
        split_at = _find_split_point(remaining, budget)
        chunk = remaining[:split_at].rstrip()
        remaining = remaining[split_at:].lstrip("\n")
        if chunk:
            chunks.append(chunk)
    return chunks


def _find_split_point(text: str, max_chars: int) -> int:
    """Pick a safe cut in prose: heading / paragraph / list / line / space."""
    window = text[:max_chars]
    min_ok = max(1, max_chars // 3)
    best: tuple[int, int] | None = None  # (priority, pos) — lower prio wins

    def consider(priority: int, pos: int) -> None:
        nonlocal best
        if pos < min_ok or pos > max_chars:
            return
        if _cut_is_inside_inline(window, pos):
            return
        if best is None or priority < best[0] or (priority == best[0] and pos > best[1]):
            best = (priority, pos)

    for match in _HEADING_LINE_RE.finditer(window):
        consider(0, match.start())

    idx = window.rfind("\n\n")
    if idx != -1:
        consider(1, idx + 2)

    for match in _LIST_ITEM_RE.finditer(window):
        if match.start() > 0:
            consider(2, match.start())

    idx = window.rfind("\n")
    if idx != -1:
        consider(3, idx + 1)

    idx = _last_safe_space(window, max_chars, min_ok)
    if idx is not None:
        consider(4, idx)

    if best is not None:
        return best[1]
    return max_chars


def _last_safe_space(text: str, max_chars: int, min_ok: int) -> int | None:
    idx = text.rfind(" ", min_ok, max_chars)
    while idx >= min_ok:
        if not _cut_is_inside_inline(text, idx):
            return idx + 1
        idx = text.rfind(" ", min_ok, idx)
    return None


def _cut_is_inside_inline(text: str, idx: int) -> bool:
    """True when ``idx`` sits inside a URL, markdown link, code, or emphasis span."""
    if idx <= 0 or idx > len(text):
        return False
    left = text[:idx]
    if _URL_TAIL_RE.search(left):
        return True
    if _UNCLOSED_LINK_LABEL_RE.search(left) or _UNCLOSED_LINK_DEST_RE.search(left):
        return True
    if _unmatched_inline_backticks(left):
        return True
    if left.count("**") % 2 == 1:
        return True
    return False


def _unmatched_inline_backticks(left: str) -> bool:
    """Odd number of non-fence backticks means we are inside inline code."""
    stripped = left.replace("```", "")
    return stripped.count("`") % 2 == 1


def _close_open_fence(chunk: str) -> tuple[str, str | None]:
    """If chunk ends inside a fence, close it and return (lang or '')."""
    in_fence = False
    lang = ""
    n = 3
    for line in chunk.splitlines():
        if not in_fence:
            opened = _fence_open(line)
            if opened:
                n, lang = opened
                in_fence = True
        elif _fence_close(line, n):
            in_fence = False
            lang = ""
    if in_fence:
        if not chunk.endswith("\n"):
            chunk += "\n"
        chunk += "`" * n
        return chunk, lang
    return chunk, None
