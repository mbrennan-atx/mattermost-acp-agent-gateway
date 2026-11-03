"""In-flight status post for a Mattermost turn.

Do **not** stream answer tokens into this post. Grok emits those as many tiny
``agent_message_chunk`` updates; editing a Mattermost post on each one looks
like a flickering ``…`` and can crash on whitespace-only chunks.

The status post carries a model stamp, a live status line, a per-tool timeline
with wall-clock durations, and the agent's intermediate narration as
blockquotes. Ported from the Discord gateway's ``discord_ui/status.py``.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

from mattermost_acp.acp import SessionUpdate

_FAIL_STATUSES = frozenset({"failed", "error"})
MIN_EDIT_INTERVAL_S = 0.8
# Mattermost caps a post at 16383 chars; keep the status post far below that.
STATUS_BUDGET = 3000

# The webapp collapses any post taller than 600px behind "Show more", and a
# finished status post is pure scrollback noise, so the timeline is windowed
# while the turn runs and dropped entirely when it ends.
LIVE_TOOL_LINES = 6
DONE_TOOL_LINES = 0
LIVE_PROGRESS_LINES = 3
DONE_PROGRESS_LINES = 0

_VERSION_RE = re.compile(r"\d+(\.\d+)*")
_TOKEN_VER_RE = re.compile(r"[a-z]+\d+(\.\d+)*", re.IGNORECASE)
_ROUTER_ALIAS_RE = re.compile(r"claude-router-((?:[0-9a-fA-F]{2})+)")
_MODEL_ID_RE = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]*")
MODEL_STAMP_PREFIX = "> *Model:"
TOOLS_LABEL = "_tools_"


def pretty_model_name(model_id: str | None) -> str:
    """Human label for a harness model id.

    ``grok-4.6`` → ``Grok 4.6``; ``hub-claude-4.5-sonnet`` → ``Claude 4.5 Sonnet``.
    """
    raw = (model_id or "").strip()
    if not raw:
        return "default"
    s = raw[4:] if raw.lower().startswith("hub-") else raw
    alias = _ROUTER_ALIAS_RE.fullmatch(s)
    if alias:
        try:
            decoded = bytes.fromhex(alias[1]).decode("utf-8")
        except UnicodeDecodeError:
            decoded = ""
        if _MODEL_ID_RE.fullmatch(decoded):
            s = decoded
    if s.startswith("glm-5p3["):
        return "GLM 5.3"
    parts: list[str] = []
    for w in re.split(r"[-_\s]+", s):
        if not w:
            continue
        if _VERSION_RE.fullmatch(w):
            parts.append(w)
        elif w.lower() == "gpt":
            parts.append("GPT")
        elif _TOKEN_VER_RE.fullmatch(w):
            parts.append(w[0].upper() + w[1:])
        elif w.isupper() or any(c.isupper() for c in w[1:]):
            parts.append(w)
        else:
            parts.append(w.capitalize())
    if len(parts) >= 2 and parts[0] == "GPT" and _VERSION_RE.fullmatch(parts[1]):
        parts[:2] = [f"GPT-{parts[1]}"]
    return " ".join(parts) if parts else raw


def format_model_stamp(
    model_id: str | None, *, label: str | None = None, effort: str | None = None
) -> str:
    """Mattermost blockquote line: ``> *Model: Grok 4.6 · Effort: high*``."""
    name = label if label is not None else pretty_model_name(model_id)
    return f"{MODEL_STAMP_PREFIX} {name} · Effort: {effort or 'default'}*"


def stamp_answer(answer: str, stamp: str) -> str:
    """Prefix an answer with the model stamp unless it already carries one."""
    text = (answer or "").strip()
    if not stamp:
        return text
    if text.lstrip().startswith(MODEL_STAMP_PREFIX):
        return text
    return f"{stamp}\n\n{text}" if text else stamp


def format_context(used: int | None, size: int | None) -> str:
    """``ctx 84,120/500,000 (17%)`` for the done line, or empty when unknown."""
    if used is None:
        return ""
    if not size:
        return f"ctx {used:,}"
    pct = round(100 * used / size)
    return f"ctx {used:,}/{size:,} ({pct}%)"


# ── Icon + short verb per category ─────────────────────────────────

_CATALOG: dict[str, tuple[str, str]] = {
    "edit": ("✏️", "Editing…"),
    "read": ("📖", "Reading…"),
    "search": ("🔍", "Searching…"),
    "shell": ("💻", "Running…"),
    "web": ("🌐", "Looking up…"),
    "image": ("🖼️", "Imaging…"),
    "browser": ("🧭", "Browsing…"),
    "mcp": ("🔌", "Using tools…"),
    "think": ("🧠", "Thinking…"),
    "other": ("🔧", "Working…"),
    "idle": ("⏳", "Working…"),
    "failed": ("⚠️", "Tool failed…"),
}

# ACP ``kind`` values (when present) → category
_KIND_MAP: dict[str, str] = {
    "edit": "edit",
    "write": "edit",
    "read": "read",
    "search": "search",
    "execute": "shell",
    "shell": "shell",
    "bash": "shell",
    "terminal": "shell",
    "fetch": "web",
    "web": "web",
    "think": "think",
    "delete": "edit",
    "move": "edit",
}

# Substrings in tool title / name (lowercase) → category. First match wins.
_TITLE_RULES: list[tuple[str, str]] = [
    ("search_replace", "edit"),
    ("strreplace", "edit"),
    ("write_file", "edit"),
    ("write", "edit"),
    ("edit", "edit"),
    ("apply_patch", "edit"),
    ("create_file", "edit"),
    ("delete_file", "edit"),
    ("read_file", "read"),
    ("read", "read"),
    ("cat ", "read"),
    ("grep", "search"),
    ("glob", "search"),
    ("find", "search"),
    ("list_dir", "search"),
    ("listdir", "search"),
    ("semantic_search", "search"),
    ("codebase_search", "search"),
    ("web_search", "web"),
    ("web_fetch", "web"),
    ("open_page", "web"),
    ("browse", "browser"),
    ("run_terminal", "shell"),
    ("bash", "shell"),
    ("shell", "shell"),
    ("terminal", "shell"),
    ("command", "shell"),
    ("image_gen", "image"),
    ("image_edit", "image"),
    ("image_to_video", "image"),
    ("video", "image"),
    ("mcp", "mcp"),
    ("use_tool", "mcp"),
]


def categorize_tool(title: str | None, kind: str | None = None) -> str:
    """Map tool title/kind to a status category key."""
    if kind:
        k = kind.strip().lower()
        if k in _KIND_MAP:
            return _KIND_MAP[k]
        if k in _CATALOG:
            return k

    name = (title or "").strip().lower()
    if not name:
        return "other"
    for needle, cat in _TITLE_RULES:
        if needle in name:
            return cat
    return "other"


def format_duration(seconds: float) -> str:
    """Human duration for tool/turn timings (``0.4s``, ``12s``, ``1m 05s``)."""
    if seconds < 0:
        seconds = 0.0
    if seconds < 10:
        return f"{seconds:.1f}s"
    if seconds < 60:
        return f"{seconds:.0f}s"
    total = int(round(seconds))
    m, s = divmod(total, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def short_tool_title(title: str | None, *, max_len: int = 72) -> str:
    """Single-line tool label; collapse whitespace and truncate."""
    t = " ".join((title or "tool").split())
    if not t:
        t = "tool"
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rstrip() + "…"


@dataclass
class ToolLogEntry:
    """One tool invocation with wall-clock timing."""

    tool_call_id: str
    title: str
    kind: str | None
    cat: str
    started: float
    ended: float | None = None
    status: str = "running"  # running | completed | failed | cancelled


@dataclass
class TurnStatus:
    """Live status emoji, tool timeline, and publish bookkeeping for one turn."""

    started: float = field(default_factory=time.monotonic)
    last_line: str = ""
    last_edit: float = 0.0
    post_id: str | None = None
    model_label: str | None = None
    effort: str | None = None

    icon: str = "⏳"
    label: str = "Working…"
    failed: int = 0
    live_tool_lines: int = LIVE_TOOL_LINES
    done_tool_lines: int = DONE_TOOL_LINES
    live_progress_lines: int = LIVE_PROGRESS_LINES
    done_progress_lines: int = DONE_PROGRESS_LINES
    usage_used: int | None = None
    usage_size: int | None = None
    # Sequential intermediate narrations (blockquoted under the status line)
    progress_lines: list[str] = field(default_factory=list)
    _active: dict[str, str] = field(default_factory=dict)
    _trail: list[str] = field(default_factory=list)
    _trail_max: int = 4
    _tools: dict[str, ToolLogEntry] = field(default_factory=dict)
    _tool_order: list[str] = field(default_factory=list)
    _anon_tool_n: int = 0

    # ── turn bookkeeping ────────────────────────────────────────────

    def elapsed_s(self) -> int:
        return max(0, int(time.monotonic() - self.started))

    @property
    def tools(self) -> list[str]:
        """Tool titles in call order (used for turn logging)."""
        return [self._tools[t].title for t in self._tool_order if t in self._tools]

    @property
    def failed_tool(self) -> bool:
        return self.failed > 0

    def observe(self, update: SessionUpdate) -> None:
        if update.usage_used is not None:
            self.usage_used = update.usage_used
        if update.usage_size is not None:
            self.usage_size = update.usage_size
        if update.kind == "tool_call":
            self.on_tool_call(
                tool_call_id=update.tool_call_id,
                title=update.tool_title,
                kind=update.tool_kind,
                status=update.tool_status,
            )
            return
        if update.kind == "tool_call_update":
            self.on_tool_update(
                tool_call_id=update.tool_call_id,
                title=update.tool_title,
                kind=update.tool_kind,
                status=update.tool_status,
            )

    def should_publish(self, line: str, *, force: bool = False) -> bool:
        if line == self.last_line and not force:
            return False
        if force or self.post_id is None:
            return True
        return (time.monotonic() - self.last_edit) >= MIN_EDIT_INTERVAL_S

    # ── tool tracking ───────────────────────────────────────────────

    def on_tool_call(
        self,
        *,
        tool_call_id: str | None,
        title: str | None,
        kind: str | None,
        status: str | None = None,
        now: float | None = None,
    ) -> None:
        cat = categorize_tool(title, kind)
        t = now if now is not None else time.monotonic()
        tid = (tool_call_id or "").strip()
        if not tid:
            self._anon_tool_n += 1
            tid = f"anon-{self._anon_tool_n}"
        if tid not in self._tools:
            self._tools[tid] = ToolLogEntry(
                tool_call_id=tid,
                title=title or "tool",
                kind=kind,
                cat=cat,
                started=t,
            )
            self._tool_order.append(tid)
        else:
            entry = self._tools[tid]
            if title:
                entry.title = title
            if kind:
                entry.kind = kind
            entry.cat = cat
        self._active[tid] = cat
        self._push_trail(cat)
        self._set_from_category(cat)
        if status and status.lower() in _FAIL_STATUSES:
            self._finish_tool(tid, status="failed", now=t)
            self._active.pop(tid, None)
            self.failed += 1
            self._set_from_category("failed")

    def on_tool_update(
        self,
        *,
        tool_call_id: str | None,
        title: str | None,
        kind: str | None,
        status: str | None,
        now: float | None = None,
    ) -> None:
        st = (status or "").lower()
        t = now if now is not None else time.monotonic()
        tid = (tool_call_id or "").strip() or None
        cat = None
        if tid and tid in self._active:
            cat = self._active[tid]
        elif title or kind:
            cat = categorize_tool(title, kind)
            if tid and cat:
                self._active[tid] = cat

        if tid and tid in self._tools:
            entry = self._tools[tid]
            if title:
                entry.title = title
            if kind:
                entry.kind = kind
            if cat:
                entry.cat = cat

        if st in _FAIL_STATUSES:
            self.failed += 1
            if tid:
                self._finish_tool(tid, status="failed", now=t)
                self._active.pop(tid, None)
            self._set_from_category("failed")
            return

        if st in {"completed", "cancelled", "canceled", "success"}:
            done = "cancelled" if st in {"cancelled", "canceled"} else "completed"
            if tid:
                self._finish_tool(tid, status=done, now=t)
                self._active.pop(tid, None)
            # Prefer another in-flight tool's icon, else keep the last category
            if self._active:
                last_cat = next(reversed(self._active.values()))
                self._set_from_category(last_cat)
            elif cat:
                self._set_from_category(cat)
            return

        if st in {"in_progress", "pending", "running"} or cat:
            if cat:
                self._push_trail(cat)
                self._set_from_category(cat)

    def _finish_tool(self, tool_call_id: str, *, status: str, now: float) -> None:
        entry = self._tools.get(tool_call_id)
        if entry is None:
            return
        if entry.ended is None:
            entry.ended = now
        entry.status = status

    def close_open_tools(
        self, *, now: float | None = None, status: str = "completed"
    ) -> None:
        """Mark any still-running tools finished (turn end / cancel)."""
        t = now if now is not None else time.monotonic()
        for tid in list(self._active.keys()):
            self._finish_tool(tid, status=status, now=t)
        self._active.clear()

    def set_progress_lines(self, lines: list[str] | None) -> None:
        """Set the intermediate narration trail shown under the status line."""
        if not lines:
            return
        cleaned = [" ".join((line or "").split()) for line in lines]
        cleaned = [line for line in cleaned if line]
        if cleaned:
            self.progress_lines = cleaned

    def _push_trail(self, cat: str) -> None:
        if cat in {"idle", "failed"}:
            return
        if self._trail and self._trail[-1] == cat:
            return
        self._trail.append(cat)
        if len(self._trail) > self._trail_max:
            self._trail = self._trail[-self._trail_max :]

    def _set_from_category(self, cat: str) -> None:
        icon, label = _CATALOG.get(cat, _CATALOG["other"])
        self.icon = icon
        self.label = label

    # ── rendering ───────────────────────────────────────────────────

    def tool_log_lines(
        self, *, now: float | None = None, max_lines: int | None = None
    ) -> list[str]:
        """Display lines for the most recent tools (with duration when known).

        ``max_lines`` keeps only the newest N calls; older ones collapse into a
        single ``… +N earlier tools`` line. ``0`` drops the timeline.
        """
        if max_lines is not None and max_lines <= 0:
            return []
        lines: list[str] = []
        for tid in self._tool_order:
            entry = self._tools.get(tid)
            if entry is None:
                continue
            icon = _CATALOG.get(entry.cat, _CATALOG["other"])[0]
            title = short_tool_title(entry.title)
            if entry.ended is not None:
                dur = format_duration(entry.ended - entry.started)
                if entry.status in {"failed", "cancelled"}:
                    lines.append(f"{icon} {title} · {dur} · {entry.status}")
                else:
                    lines.append(f"{icon} {title} · {dur}")
            else:
                elapsed = (now if now is not None else time.monotonic()) - entry.started
                lines.append(f"{icon} {title} · {format_duration(elapsed)}…")
        if max_lines is not None and len(lines) > max_lines:
            hidden = len(lines) - max_lines
            lines = [f"… +{hidden} earlier tool{'s' if hidden != 1 else ''}"] + lines[
                -max_lines:
            ]
        return lines

    def render(
        self,
        *,
        phase: str = "running",
        now: float | None = None,
        stop_reason: str | None = None,
        extra: str | None = None,
    ) -> str:
        """Model stamp, status line, tool timeline, intermediate narrations."""
        if phase in {"done", "failed"}:
            return self._render_terminal(
                phase=phase, now=now, stop_reason=stop_reason, extra=extra
            )

        trail = ""
        icons = [_CATALOG[c][0] for c in self._trail if c in _CATALOG]
        if len(icons) > 1:
            trail = " ".join(icons[:-1]) + " · "
        head = f"{trail}{self.icon} {self.label} ({self.elapsed_s()}s)"
        return self._pack(
            self._stamped(head),
            self.tool_log_lines(now=now, max_lines=self.live_tool_lines),
            max_progress=self.live_progress_lines,
        )

    def _render_terminal(
        self,
        *,
        phase: str,
        now: float | None = None,
        stop_reason: str | None = None,
        extra: str | None = None,
    ) -> str:
        t = now if now is not None else time.monotonic()
        ok = phase == "done"
        self.close_open_tools(now=t, status="completed" if ok else "cancelled")

        mark = "✅" if ok else "❌"
        verb = "Done" if ok else "Failed"
        head = f"{mark} {verb}"
        if stop_reason:
            head += f" · {stop_reason}"
        head += f" · {format_duration(t - self.started)}"
        n = len(self._tool_order)
        if n:
            head += f" · {n} tool{'s' if n != 1 else ''}"
        if self.failed:
            head += f" · {self.failed} failed"
        ctx = format_context(self.usage_used, self.usage_size)
        if ctx:
            head += f" · {ctx}"
        if extra:
            head += f" · {extra}"
        return self._pack(
            self._stamped(head),
            self.tool_log_lines(now=t, max_lines=self.done_tool_lines),
            max_progress=self.done_progress_lines,
        )

    def _stamped(self, head: str) -> str:
        if not self.model_label:
            return head
        return f"{format_model_stamp(None, label=self.model_label, effort=self.effort)}\n{head}"

    def _pack(
        self, head: str, tool_lines: list[str], *, max_progress: int | None = None
    ) -> str:
        """Keep the head; drop oldest narration then oldest tool lines to fit."""
        tools = list(tool_lines)
        progress = list(self.progress_lines)
        if max_progress is not None:
            progress = progress[-max_progress:] if max_progress > 0 else []
        omit_tools = False
        omit_progress = len(progress) < len(self.progress_lines) and bool(progress)

        def build() -> str:
            parts = [head]
            if tools:
                parts.append(TOOLS_LABEL)
                if omit_tools:
                    parts.append("> …")
                parts.extend(f"> {line}" for line in tools)
            if progress:
                if omit_progress:
                    parts.append("> …")
                parts.extend(f"> {line}" for line in progress)
            return "\n".join(parts)

        body = build()
        if len(body) <= STATUS_BUDGET:
            return body
        while progress and len(build()) > STATUS_BUDGET:
            progress.pop(0)
            omit_progress = True
        while tools and len(build()) > STATUS_BUDGET:
            tools.pop(0)
            omit_tools = True
        body = build()
        return body if len(body) <= STATUS_BUDGET else head[:STATUS_BUDGET]
