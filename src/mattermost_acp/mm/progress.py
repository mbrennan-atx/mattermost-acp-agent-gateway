"""Separate intermediate agent narration from the final answer.

Grok emits short progress sentences as ``agent_message_chunk`` bursts between
tool calls. Posting them inline with the answer reads as one run-on message, so
text that arrived before a tool call is treated as intermediate progress and
rendered as blockquotes above the answer. Ported from the Discord gateway's
``discord_ui/progress.py``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from mattermost_acp.text import join_text_chunks

INTERMEDIATE_LABEL = "_intermediate_"


def _looks_like_progress_paragraph(para: str) -> bool:
    """Heuristic: short, single-ish status line without markdown structure."""
    s = para.strip()
    if not s or len(s) > 400:
        return False
    if s.startswith("#") or s.startswith("```") or s.startswith("|"):
        return False
    if s.startswith("- ") or s.startswith("* ") or s.startswith("> "):
        return False
    if "\n" in s and len(s) > 120:
        return False
    return True


def format_progress_answer(*, intermediate: list[str], final: str) -> str:
    """Build the user-visible body for one turn (no model stamp)."""
    final = (final or "").strip()
    inter = [p.strip() for p in intermediate if p and p.strip()]

    if not inter and not final:
        return ""
    if not inter:
        return final

    if not final:
        # No formal answer: a single structured or long blob *is* the answer.
        if len(inter) == 1 and (
            not _looks_like_progress_paragraph(inter[0]) or len(inter[0]) > 400
        ):
            return inter[0]
        return f"{INTERMEDIATE_LABEL}\n{_quote_lines(inter)}"

    return f"{INTERMEDIATE_LABEL}\n{_quote_lines(inter)}\n\n{final}"


def _quote_lines(paragraphs: list[str]) -> str:
    lines: list[str] = []
    for para in paragraphs:
        flat = re.sub(r"\s*\n\s*", " ", para.strip())
        if flat:
            lines.append(f"> {flat}")
    return "\n".join(lines)


@dataclass
class TurnProgressBuffer:
    """Tracks intermediate vs final agent text for one turn.

    Text that arrives *before* a tool call is intermediate progress. Text left
    after the last tool is the final answer. Pure-text turns (no tools) treat
    everything as the final answer.
    """

    intermediate: list[str] = field(default_factory=list)
    _current_parts: list[str] = field(default_factory=list)
    _saw_tool: bool = False

    def on_agent_text(self, text: str | None) -> None:
        if not text:
            return
        self._current_parts.append(text)

    def on_tool(self) -> None:
        """Any tool activity marks prior agent text as intermediate progress."""
        self._saw_tool = True
        if self._current_parts:
            self._flush_current()

    def _flush_current(self) -> None:
        joined = join_text_chunks(self._current_parts).strip()
        self._current_parts.clear()
        if not joined:
            return
        for para in re.split(r"\n\s*\n", joined):
            p = para.strip()
            if p:
                self.intermediate.append(p)

    def progress_lines(self) -> list[str]:
        """Full sequential intermediate trail, including in-flight text."""
        lines = list(self.intermediate)
        if self._current_parts:
            current = join_text_chunks(self._current_parts).strip()
            if current:
                for para in re.split(r"\n\s*\n", current):
                    p = para.strip()
                    if p:
                        lines.append(p)
        return lines

    def finalize(self) -> tuple[list[str], str]:
        """Return ``(intermediate_paragraphs, final_answer)``."""
        final = join_text_chunks(self._current_parts).strip()
        self._current_parts.clear()
        if not self._saw_tool:
            return [], final
        return list(self.intermediate), final

    def format_answer(self, *, include_progress: bool = True) -> str:
        inter, final = self.finalize()
        if final and not include_progress:
            return final
        return format_progress_answer(intermediate=inter, final=final)
