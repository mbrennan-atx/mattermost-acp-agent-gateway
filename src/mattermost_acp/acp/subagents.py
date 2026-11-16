"""Route native ACP child-session status to its actual parent and ancestors."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .types import SessionUpdate

ASYNC_TERMINAL = frozenset({"completed", "failed", "stopped"})
WORKER_TERMINAL = frozenset({"completed", "cancelled", "canceled", "failed", "idle"})


def _short(value: object, limit: int = 240) -> str:
    return " ".join(str(value or "").split())[:limit]


@dataclass
class Worker:
    session_id: str
    parent_id: str
    name: str
    state: str = "running"
    model: str = ""
    latest: str = ""
    updated_at: float = field(default_factory=time.monotonic)
    parts: str = ""
    async_tasks: dict[str, dict] = field(default_factory=dict)
    pending_async_tasks: set[str] = field(default_factory=set)
    latest_async_task: str = ""
    async_tasks_by_tool: dict[str, set[str]] = field(default_factory=dict)
    terminal_tasks_by_tool: dict[str, set[str]] = field(default_factory=dict)
    observed_tools: set[str] = field(default_factory=set)
    active_tools: set[str] = field(default_factory=set)
    background_requested: set[str] = field(default_factory=set)
    detached_tools: set[str] = field(default_factory=set)

    def pending_execution(self) -> bool:
        return bool(self.active_tools or self.detached_tools or self.pending_async_tasks)


class SubagentTracker:
    """Protocol state only. No prompts, tool inputs/results or hidden thoughts.

    Supports the Codex AIR draft and the Claude/ACP subagents RFD. Replayed
    announcements rebuild associations after session load. Unknown/orphan
    child streams are never guessed onto another conversation.
    """

    def __init__(self) -> None:
        self.workers: dict[str, Worker] = {}
        self._retired_sessions: set[str] = set()
        self._task_sources: dict[str, set[str]] = {}
        self._pending_task_sources: dict[str, set[str]] = {}
        self._root_tasks: dict[str, dict[str, str]] = {}
        self._root_pending_tasks: dict[str, set[str]] = {}

    def _ancestors(self, worker: Worker):
        parent = worker.parent_id
        seen = {worker.session_id}
        while parent and parent not in seen:
            seen.add(parent)
            yield parent
            ancestor = self.workers.get(parent)
            parent = ancestor.parent_id if ancestor else ""

    def _root(self, worker: Worker) -> str:
        root = worker.session_id
        for ancestor in self._ancestors(worker):
            root = ancestor
        return root

    def _related(self, session_id: str) -> set[str]:
        return {
            child for child, worker in self.workers.items() if session_id in self._ancestors(worker)
        }

    def _events(self, worker: Worker) -> list[SessionUpdate]:
        # Keep completed lifecycle records internally, but routine activity only
        # carries unresolved tasks and the most recent lifecycle update.
        task_ids = worker.pending_async_tasks | (
            {worker.latest_async_task} if worker.latest_async_task else set()
        )
        tasks = {}
        root = self._root(worker)
        task_ids.update(
            task_id
            for task_id in self._root_pending_tasks.get(root, ())
            if task_id in worker.async_tasks
        )
        # A terminal sibling copy still fences a live task's cached ownership.
        # Retain just collisions with current pending IDs, never scan history.
        task_ids.update(
            task_id
            for task_id, sources in self._pending_task_sources.items()
            if task_id in worker.async_tasks
            and any(self._root(self.workers[source]) == root for source in sources)
        )
        for task_id in task_ids:
            task = dict(worker.async_tasks[task_id])
            if any(
                source != worker.session_id
                and (self._root(self.workers[source]) if source in self.workers else source) == root
                for source in self._task_sources.get(task_id, ())
            ):
                task["owned"] = False
            tasks[task_id] = task
        events = []
        for parent in self._ancestors(worker):
            events.append(
                SessionUpdate(
                    session_id=parent,
                    kind="subagent_activity",
                    raw={
                        "worker_id": worker.session_id,
                        "name": worker.name,
                        "state": worker.state,
                        "model": worker.model,
                        "latest": worker.latest,
                        "updated_at": worker.updated_at,
                        "async_tasks": {task_id: dict(task) for task_id, task in tasks.items()},
                        "pending_async_work": worker.pending_execution(),
                    },
                )
            )
        return events

    def snapshots(self, parent_id: str) -> list[SessionUpdate]:
        return [
            event
            for worker in self.workers.values()
            for event in self._events(worker)
            if event.session_id == parent_id
        ]

    def forget(self, session_id: str) -> None:
        descendants = self._related(session_id)
        for child in descendants | {session_id}:
            worker = self.workers.pop(child, None)
            root_tasks = self._root_tasks.pop(child, {})
            self._root_pending_tasks.pop(child, None)
            for records in (worker.async_tasks if worker is not None else {}, root_tasks):
                for task_id in records:
                    sources = self._task_sources.get(task_id)
                    if sources is not None:
                        sources.discard(child)
                        if not sources:
                            self._task_sources.pop(task_id)
                    pending = self._pending_task_sources.get(task_id)
                    if pending is not None:
                        pending.discard(child)
                        if not pending:
                            self._pending_task_sources.pop(task_id)
            self._retired_sessions.discard(child)

    def pending(self, session_id: str) -> bool:
        related = self._related(session_id)
        if session_id in self.workers:
            related.add(session_id)
        return any(
            self.workers[child].state not in WORKER_TERMINAL
            or self.workers[child].pending_execution()
            for child in related
        )

    def retire(self, session_id: str) -> None:
        """Retain routing until known child execution supplies terminal evidence."""
        if self.pending(session_id):
            self._retired_sessions.add(session_id)
        else:
            self.forget(session_id)

    def prune_retired(self) -> None:
        # The broker calls this after delivering the final ancestor snapshots.
        for session_id in list(self._retired_sessions):
            if not self.pending(session_id):
                self.forget(session_id)

    def observe(self, update: SessionUpdate) -> list[SessionUpdate]:
        raw = update.raw
        if update.kind in {"subagent_spawned", "subagent_state_update", "subagent_update"}:
            child_id = raw.get("subagentSessionId") or raw.get("sessionId")
            if not isinstance(child_id, str) or not child_id or child_id == update.session_id:
                return []
            worker = self.workers.get(child_id)
            if worker and worker.parent_id != update.session_id:
                return []  # Do not reparent an existing worker across chats.
            parent = update.session_id
            seen = {child_id}
            while parent in self.workers:
                if parent in seen:
                    return []
                seen.add(parent)
                parent = self.workers[parent].parent_id
            if parent in seen:
                return []
            if worker is None:
                worker = Worker(
                    child_id,
                    update.session_id,
                    _short(raw.get("name") or raw.get("title") or "worker", 80),
                )
                self.workers[child_id] = worker
            state = raw.get("state")
            if isinstance(state, dict):
                stop = state.get("stopReason")
                state = "failed" if stop == "error" else state.get("state")
            if isinstance(state, str):
                worker.state = _short(state, 40)
            worker.updated_at = time.monotonic()
            return self._events(worker)

        worker = self.workers.get(update.session_id)
        if worker is None:
            # Parent tasks retain source evidence too. If a live parent task
            # collides with omitted child history, reissue that bounded evidence.
            if update.kind in {
                "async_task_spawned",
                "async_task_progress",
                "async_task_state_update",
            }:
                task_id = raw.get("asyncTaskId")
                if not isinstance(task_id, str) or not task_id:
                    return []
                tasks = self._root_tasks.setdefault(update.session_id, {})
                previous = tasks.get(task_id, "running")
                state = raw.get("state", previous)
                state = state if isinstance(state, str) else "unknown"
                state = (
                    previous
                    if previous in ASYNC_TERMINAL and state not in ASYNC_TERMINAL
                    else state
                )
                tasks[task_id] = state
                pending = self._root_pending_tasks.setdefault(update.session_id, set())
                if state in ASYNC_TERMINAL:
                    pending.discard(task_id)
                else:
                    pending.add(task_id)
                sources = self._task_sources.setdefault(task_id, set())
                sources.add(update.session_id)
                activities = [
                    activity
                    for source in sources
                    if source in self.workers
                    and self._root(self.workers[source]) == update.session_id
                    for activity in self._events(self.workers[source])
                ]
                for activity in activities:
                    activity.raw["ownership_snapshot"] = True
                return activities
            return []
        # Do not count hidden thinking or arbitrary metadata as reported progress.
        if update.kind == "agent_message_chunk" and update.text:
            worker.parts = (worker.parts + update.text)[-2000:]
            worker.latest = _short(worker.parts)
        elif update.kind in {"tool_call", "tool_call_update"}:
            tool_id = update.tool_call_id
            if tool_id:
                if update.kind == "tool_call" and tool_id not in worker.observed_tools:
                    worker.observed_tools.add(tool_id)
                    for task_id in worker.async_tasks_by_tool.get(tool_id, ()):
                        worker.async_tasks[task_id]["owned"] = True
                state = (update.tool_status or "").lower()
                meta = raw.get("_meta")
                meta = meta if isinstance(meta, dict) else {}
                native = meta.get("claudeCode")
                native = native if isinstance(native, dict) else {}
                inputs = raw.get("rawInput")
                if (
                    native.get("toolName") == "Bash"
                    and isinstance(inputs, dict)
                    and inputs.get("run_in_background") is True
                ):
                    worker.background_requested.add(tool_id)
                jetbrains = meta.get("jetbrains")
                air = jetbrains.get("air", {}) if isinstance(jetbrains, dict) else {}
                background = air.get("asyncTasks", {}) if isinstance(air, dict) else {}
                if state in {"completed", "success"} and (
                    tool_id in worker.background_requested
                    or (isinstance(background, dict) and background.get("backgrounded") is True)
                ):
                    worker.detached_tools.add(tool_id)
                    if worker.terminal_tasks_by_tool.get(tool_id):
                        worker.detached_tools.discard(tool_id)
                if state in {"completed", "success", "failed", "cancelled", "canceled", "error"}:
                    worker.active_tools.discard(tool_id)
                elif update.kind == "tool_call" or state in {"pending", "in_progress", "running"}:
                    worker.active_tools.add(tool_id)
            worker.parts = ""
            if not worker.latest:
                worker.latest = "Using tools"
        elif update.kind in {
            "async_task_spawned",
            "async_task_progress",
            "async_task_state_update",
        }:
            task_id = raw.get("asyncTaskId")
            if not isinstance(task_id, str) or not task_id:
                return []
            task = worker.async_tasks.setdefault(task_id, {"state": "running"})
            old_tool = task.get("toolCallId")
            if isinstance(old_tool, str):
                for index in (worker.async_tasks_by_tool, worker.terminal_tasks_by_tool):
                    members = index.get(old_tool)
                    if members is not None:
                        members.discard(task_id)
                        if not members:
                            index.pop(old_tool)
            previous = task.get("state")
            if "toolCallId" in raw:
                value = raw["toolCallId"]
                task["toolCallId"] = value if isinstance(value, str) else None
            if "canStop" in raw:
                task["canStop"] = raw["canStop"] is True
            state = raw.get("state", previous)
            state = state if isinstance(state, str) else "unknown"
            task["state"] = (
                previous if previous in ASYNC_TERMINAL and state not in ASYNC_TERMINAL else state
            )
            tool_id = task.get("toolCallId")
            task["owned"] = isinstance(tool_id, str) and tool_id in worker.observed_tools
            if isinstance(tool_id, str):
                worker.async_tasks_by_tool.setdefault(tool_id, set()).add(task_id)
            if task.get("state") in ASYNC_TERMINAL:
                worker.pending_async_tasks.discard(task_id)
                pending = self._pending_task_sources.get(task_id)
                if pending is not None:
                    pending.discard(worker.session_id)
                    if not pending:
                        self._pending_task_sources.pop(task_id)
                if isinstance(tool_id, str):
                    worker.terminal_tasks_by_tool.setdefault(tool_id, set()).add(task_id)
                worker.detached_tools.discard(tool_id)
            else:
                worker.pending_async_tasks.add(task_id)
                self._pending_task_sources.setdefault(task_id, set()).add(worker.session_id)
            worker.latest_async_task = task_id
            self._task_sources.setdefault(task_id, set()).add(worker.session_id)
        elif update.model_id:
            worker.model = _short(update.model_id, 80)
        else:
            return []
        if update.model_id:
            worker.model = _short(update.model_id, 80)
        worker.updated_at = time.monotonic()
        return self._events(worker)
