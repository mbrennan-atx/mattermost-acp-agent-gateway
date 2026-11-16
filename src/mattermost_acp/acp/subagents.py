"""Route native ACP child-session status to its actual parent and ancestors."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .types import SessionUpdate


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
    observed_tools: set[str] = field(default_factory=set)


class SubagentTracker:
    """Protocol state only. No prompts, tool inputs/results or hidden thoughts.

    Supports the Codex AIR draft and the Claude/ACP subagents RFD. Replayed
    announcements rebuild associations after session load. Unknown/orphan
    child streams are never guessed onto another conversation.
    """

    def __init__(self) -> None:
        self.workers: dict[str, Worker] = {}

    def _events(self, worker: Worker) -> list[SessionUpdate]:
        events = []
        parent = worker.parent_id
        seen = {worker.session_id}
        while parent and parent not in seen:
            seen.add(parent)
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
                        "async_tasks": {
                            task_id: dict(task) for task_id, task in worker.async_tasks.items()
                        },
                        "pending_async_work": any(
                            task.get("state") not in {"completed", "failed", "stopped"}
                            for task in worker.async_tasks.values()
                        ),
                    },
                )
            )
            ancestor = self.workers.get(parent)
            parent = ancestor.parent_id if ancestor else ""
        return events

    def snapshots(self, parent_id: str) -> list[SessionUpdate]:
        return [
            event
            for worker in self.workers.values()
            for event in self._events(worker)
            if event.session_id == parent_id
        ]

    def forget(self, session_id: str) -> None:
        descendants = {event.raw["worker_id"] for event in self.snapshots(session_id)}
        for child in descendants | {session_id}:
            self.workers.pop(child, None)

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
            return []
        # Do not count hidden thinking or arbitrary metadata as reported progress.
        if update.kind == "agent_message_chunk" and update.text:
            worker.parts = (worker.parts + update.text)[-2000:]
            worker.latest = _short(worker.parts)
        elif update.kind in {"tool_call", "tool_call_update"}:
            if update.kind == "tool_call" and update.tool_call_id:
                worker.observed_tools.add(update.tool_call_id)
                for task in worker.async_tasks.values():
                    task["owned"] = task.get("toolCallId") in worker.observed_tools
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
            previous = task.get("state")
            if "toolCallId" in raw:
                value = raw["toolCallId"]
                task["toolCallId"] = value if isinstance(value, str) else None
            if "canStop" in raw:
                task["canStop"] = raw["canStop"] is True
            state = raw.get("state", previous)
            state = state if isinstance(state, str) else "unknown"
            task["state"] = (
                previous
                if previous in {"completed", "failed", "stopped"}
                and state not in {"completed", "failed", "stopped"}
                else state
            )
            tool_id = task.get("toolCallId")
            task["owned"] = isinstance(tool_id, str) and tool_id in worker.observed_tools
        elif update.model_id:
            worker.model = _short(update.model_id, 80)
        else:
            return []
        if update.model_id:
            worker.model = _short(update.model_id, 80)
        worker.updated_at = time.monotonic()
        return self._events(worker)
