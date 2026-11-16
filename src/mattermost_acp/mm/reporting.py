"""Bounded thread progress, edited in place independently of tool status."""

from __future__ import annotations

import time
from collections.abc import Callable

from mattermost_acp.acp import SessionUpdate

REPORT_INTERVAL_S = 60.0
ASYNC_TERMINAL = frozenset({"completed", "failed", "stopped"})


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
        self.observed_tools: set[str] = set()
        # A completed Bash card can represent a command that is still detached.
        # Only its separate native task lifecycle can prove that command ended.
        self.background_requested: set[str] = set()
        self.detached_tools: set[str] = set()
        self.async_tasks: dict[str, dict] = {}
        self.parts = ""
        self.post_id: str | None = None

    def observe(self, update: SessionUpdate) -> None:
        if update.kind in {
            "agent_message_chunk",
            "tool_call",
            "tool_call_update",
            "subagent_activity",
            "async_task_spawned",
            "async_task_progress",
            "async_task_state_update",
        }:
            self.last_activity = self.clock()
        if update.kind == "agent_message_chunk":
            self.parts = (self.parts + (update.text or ""))[-4000:]
        elif update.kind in {"tool_call", "tool_call_update"}:
            tool_id = update.tool_call_id
            if tool_id:
                if update.kind == "tool_call":
                    self.observed_tools.add(tool_id)
                state = (update.tool_status or "").lower()
                meta = update.raw.get("_meta")
                meta = meta if isinstance(meta, dict) else {}
                native = meta.get("claudeCode")
                native = native if isinstance(native, dict) else {}
                inputs = update.raw.get("rawInput")
                if (
                    native.get("toolName") == "Bash"
                    and isinstance(inputs, dict)
                    and inputs.get("run_in_background") is True
                ):
                    self.background_requested.add(tool_id)
                jetbrains = meta.get("jetbrains")
                air = jetbrains.get("air", {}) if isinstance(jetbrains, dict) else {}
                background = air.get("asyncTasks", {}) if isinstance(air, dict) else {}
                if state in {"completed", "success"} and (
                    tool_id in self.background_requested
                    or (isinstance(background, dict) and background.get("backgrounded") is True)
                ):
                    self.detached_tools.add(tool_id)
                    if any(
                        task.get("toolCallId") == tool_id and task.get("state") in ASYNC_TERMINAL
                        for task in self.async_tasks.values()
                    ):
                        self.detached_tools.discard(tool_id)
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
        elif update.kind in {
            "async_task_spawned",
            "async_task_progress",
            "async_task_state_update",
        }:
            task_id = update.raw.get("asyncTaskId")
            if not isinstance(task_id, str) or not task_id:
                return
            task = self.async_tasks.setdefault(task_id, {"state": "running"})
            prior_state = task.get("state")
            task.update(update.raw)
            if not isinstance(task.get("state"), str):
                task["state"] = "unknown"
            if prior_state in ASYNC_TERMINAL and task.get("state") not in ASYNC_TERMINAL:
                task["state"] = prior_state
            tool_id = task.get("toolCallId")
            tool_id = tool_id if isinstance(tool_id, str) else None
            task["owned"] = tool_id in self.observed_tools
            if task.get("state") in ASYNC_TERMINAL:
                self.detached_tools.discard(tool_id)

    def observe_retained(self, update: SessionUpdate) -> None:
        """Update known predecessor work without claiming the next turn's tools."""
        if update.kind in {"tool_call", "tool_call_update"}:
            if update.tool_call_id not in (self.observed_tools | self.active_tools):
                return
        elif update.kind.startswith("async_task_"):
            task_id = update.raw.get("asyncTaskId")
            if not isinstance(task_id, str):
                return
            if task_id not in self.async_tasks and update.tool_call_id not in self.observed_tools:
                return
        self.observe(update)

    def owns_async_task(self, task_id: str) -> bool:
        tasks = []
        if task_id in self.async_tasks:
            tasks.append(self.async_tasks[task_id])
        for worker in self.workers.values():
            child_tasks = worker.get("async_tasks", {})
            if isinstance(child_tasks, dict) and task_id in child_tasks:
                tasks.append(child_tasks[task_id])
        # A task ID shared by distinct protocol sources has ambiguous stop ownership.
        if len(tasks) != 1 or not isinstance(tasks[0], dict):
            return False
        task = tasks[0]
        return (
            task.get("owned") is True
            and task.get("canStop") is True
            and task.get("state") not in ASYNC_TERMINAL
        )

    def stoppable_async_tasks(self) -> list[str]:
        task_ids = set(self.async_tasks)
        for worker in self.workers.values():
            tasks = worker.get("async_tasks", {})
            if isinstance(tasks, dict):
                task_ids.update(tasks)
        return sorted(task_id for task_id in task_ids if self.owns_async_task(task_id))

    def has_live_workers(self) -> bool:
        return any(
            worker.get("pending_async_work")
            or worker.get("state") not in {"completed", "cancelled", "canceled", "failed", "idle"}
            for worker in self.workers.values()
        )

    def has_pending_work(self) -> bool:
        return (
            bool(self.active_tools or self.detached_tools)
            or self.has_live_workers()
            or any(task.get("state") not in ASYNC_TERMINAL for task in self.async_tasks.values())
        )

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
