"""Bounded thread progress, edited in place independently of tool status."""

from __future__ import annotations

import time
from collections.abc import Callable

from mattermost_acp.acp import SessionUpdate

REPORT_INTERVAL_S = 60.0


def _line(text: object, limit: int = 360) -> str:
    # Prevent a worker's @mention or protocol directive from becoming an action.
    value = " ".join(str(text or "").split()).replace("@", "＠")
    for directive in ("MATTERMOST_THREAD_RENAME:", "MATTERMOST_MODEL_SET:"):
        value = value.replace(directive, "")
    return value[:limit]


class TurnReporter:
    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.last_post = clock()
        self.last_report = clock()
        self.last_activity = clock()
        self.latest = ""
        self.workers: dict[str, dict] = {}
        self.active_tools: set[str] = set()
        self.parts = ""
        self.post_id: str | None = None

    def observe(self, update: SessionUpdate) -> None:
        if update.kind in {
            "agent_message_chunk",
            "tool_call",
            "tool_call_update",
            "subagent_activity",
        }:
            self.last_activity = self.clock()
        if update.kind == "agent_message_chunk":
            self.parts = (self.parts + (update.text or ""))[-4000:]
        elif update.kind in {"tool_call", "tool_call_update"}:
            tool_id = update.tool_call_id
            if tool_id:
                state = (update.tool_status or "").lower()
                if state in {"completed", "success", "failed", "cancelled", "canceled", "error"}:
                    self.active_tools.discard(tool_id)
                elif update.kind == "tool_call" or state in {"pending", "in_progress", "running"}:
                    self.active_tools.add(tool_id)
            # Only publish complete narration that precedes tool use, never
            # partial final-answer chunks or private reasoning.
            if self.parts.strip():
                self.latest = _line(self.parts.strip().split("\n\n")[-1])
                self.last_report = self.clock()
            self.parts = ""
        elif update.kind == "subagent_activity":
            worker_id = update.raw.get("worker_id")
            if worker_id:
                self.workers[str(worker_id)] = update.raw

    def has_live_workers(self) -> bool:
        return any(
            worker.get("state") not in {"completed", "cancelled", "canceled", "failed", "idle"}
            for worker in self.workers.values()
        )

    def has_pending_work(self) -> bool:
        return bool(self.active_tools) or self.has_live_workers()

    def worker_lines(self) -> list[str]:
        lines = []
        for worker in list(self.workers.values())[-4:]:
            age = max(0, int(self.clock() - worker.get("updated_at", self.clock())))
            model = f" ({_line(worker['model'], 60)})" if worker.get("model") else ""
            line = (
                f"{_line(worker.get('name'), 80)}{model}: "
                f"{_line(worker.get('state'), 40)}; last activity {age}s ago"
            )
            if worker.get("latest"):
                line += f". {_line(worker['latest'], 200)}"
            lines.append(line)
        return lines

    def due(self) -> str | None:
        if self.clock() - self.last_post < REPORT_INTERVAL_S:
            return None
        lines = self.worker_lines()
        if self.latest:
            age = max(0, int(self.clock() - self.last_report))
            body = f"Progress: {self.latest}"
            if age >= REPORT_INTERVAL_S:
                body += f" (Last parent update {age}s ago.)"
        else:
            body = "Still working; no new parent progress report yet."
        if lines:
            body += "\n\n" + "\n".join(f"- {line}" for line in lines)
        return body

    def published(self) -> None:
        self.last_post = self.clock()
