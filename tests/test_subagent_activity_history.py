"""Routine child activity scales with live tasks while history preserves ownership."""

import pytest

from mattermost_acp.acp.broker import parse_session_update
from mattermost_acp.acp.subagents import SubagentTracker
from mattermost_acp.mm.reporting import TurnReporter


def event(session, kind, **raw):
    return parse_session_update({"sessionId": session, "update": {"sessionUpdate": kind, **raw}})


def spawn(tracker, parent="root", child="child"):
    return tracker.observe(event(parent, "subagent_spawned", subagentSessionId=child))


@pytest.mark.parametrize("nested", [False, True])
def test_completed_async_history_is_not_repeated_in_routine_ancestor_events(nested):
    tracker = SubagentTracker()
    spawn(tracker)
    child = "child"
    if nested:
        spawn(tracker, "child", "nested")
        child = "nested"
    count, copied = 256, 0
    for index in range(count):
        task_id = str(index)
        for update in [
            event(child, "async_task_spawned", asyncTaskId=task_id),
            event(child, "async_task_state_update", asyncTaskId=task_id, state="completed"),
        ]:
            activities = tracker.observe(update)
            copied += sum(len(activity.raw["async_tasks"]) for activity in activities)
            assert all(task_id in activity.raw["async_tasks"] for activity in activities)
    assert len(tracker.workers[child].async_tasks) == count
    assert copied <= count * (4 if nested else 2)
    assert all(task["state"] == "completed" for task in tracker.workers[child].async_tasks.values())


class NoHistoryIteration(dict):
    def items(self):
        raise AssertionError("routine activity iterated completed task history")

    def values(self):
        raise AssertionError("routine activity scanned completed task history")

    def __iter__(self):
        raise AssertionError("routine activity iterated completed task history")


def test_output_pending_checks_and_tool_updates_do_not_scan_completed_history():
    tracker = SubagentTracker()
    spawn(tracker)
    for index in range(100):
        tracker.observe(
            event("child", "async_task_state_update", asyncTaskId=str(index), state="completed")
        )
    worker = tracker.workers["child"]
    worker.async_tasks = NoHistoryIteration(worker.async_tasks)
    tracker.observe(
        event("child", "agent_message_chunk", content={"type": "text", "text": "progress"})
    )
    tracker.observe(event("child", "tool_call", toolCallId="current", status="pending"))
    tracker.observe(event("child", "tool_call_update", toolCallId="current", status="completed"))
    tracker.observe(
        event("root", "subagent_state_update", subagentSessionId="child", state="completed")
    )
    assert not tracker.pending("root")


@pytest.mark.parametrize("nested", [False, True])
def test_active_task_and_terminal_evidence_survive_history_compaction_and_late_tool(nested):
    tracker, reporter = SubagentTracker(), TurnReporter()

    def observe(update):
        events = tracker.observe(update)
        for activity in events:
            if activity.session_id == "root":
                reporter.observe(activity)
        return events

    observe(event("root", "subagent_spawned", subagentSessionId="child"))
    child, parent = "child", "root"
    if nested:
        observe(event("child", "subagent_spawned", subagentSessionId="nested"))
        child, parent = "nested", "child"
    observe(
        event(
            child, "async_task_spawned", asyncTaskId="active", toolCallId="late-tool", canStop=True
        )
    )
    for index in range(100):
        observe(event(child, "async_task_state_update", asyncTaskId=str(index), state="completed"))
    assert reporter.has_pending_work() and not reporter.owns_async_task("active")
    observe(event(child, "tool_call", toolCallId="late-tool", status="completed"))
    assert reporter.owns_async_task("active")
    observe(event(parent, "subagent_state_update", subagentSessionId=child, state="completed"))
    if nested:
        observe(
            event("root", "subagent_state_update", subagentSessionId="child", state="completed")
        )
    previous = reporter.workers[child]["async_tasks"]["active"]
    observe(event(child, "async_task_state_update", asyncTaskId="active", state="stopped"))
    assert previous["state"] == "running", "queued snapshots must not mutate retroactively"
    assert not reporter.has_pending_work() and not reporter.owns_async_task("active")
    observe(event(child, "async_task_progress", asyncTaskId="active", state="running"))
    assert not reporter.has_pending_work()
    assert tracker.workers[child].async_tasks["active"]["state"] == "stopped"


@pytest.mark.parametrize("other_root", ["root", "unrelated"])
def test_completed_task_id_source_evidence_still_controls_ambiguous_stop(other_root):
    tracker, reporters = SubagentTracker(), {"root": TurnReporter(), "unrelated": TurnReporter()}

    def observe(update):
        for activity in tracker.observe(update):
            if activity.session_id in reporters:
                reporters[activity.session_id].observe(activity)

    observe(event("root", "subagent_spawned", subagentSessionId="first"))
    observe(event("first", "tool_call", toolCallId="first-tool", status="completed"))
    observe(
        event(
            "first",
            "async_task_spawned",
            asyncTaskId="shared",
            toolCallId="first-tool",
            canStop=True,
        )
    )
    observe(event("first", "async_task_state_update", asyncTaskId="shared", state="completed"))
    for index in range(100):
        observe(
            event("first", "async_task_state_update", asyncTaskId=str(index), state="completed")
        )
    observe(event(other_root, "subagent_spawned", subagentSessionId="second"))
    observe(event("second", "tool_call", toolCallId="second-tool", status="completed"))
    observe(
        event(
            "second",
            "async_task_spawned",
            asyncTaskId="shared",
            toolCallId="second-tool",
            canStop=True,
        )
    )
    assert reporters[other_root].owns_async_task("shared") is (other_root == "unrelated")
    assert tracker.workers["first"].async_tasks["shared"]["state"] == "completed"


def test_retired_root_keeps_routing_and_pending_task_until_matching_terminal():
    tracker = SubagentTracker()
    spawn(tracker)
    spawn(tracker, "unrelated", "other")
    tracker.observe(event("child", "async_task_spawned", asyncTaskId="pending"))
    for index in range(100):
        tracker.observe(
            event("child", "async_task_state_update", asyncTaskId=str(index), state="completed")
        )
    tracker.observe(
        event("root", "subagent_state_update", subagentSessionId="child", state="completed")
    )
    tracker.retire("root")
    assert tracker.pending("root") and "child" in tracker.workers
    activities = tracker.observe(
        event("child", "async_task_state_update", asyncTaskId="pending", state="stopped")
    )
    assert activities[0].session_id == "root"
    assert activities[0].raw["async_tasks"]["pending"]["state"] == "stopped"
    assert not activities[0].raw["pending_async_work"]
    tracker.prune_retired()
    assert set(tracker.workers) == {"other"}


def test_live_native_parent_task_cannot_hide_collision_with_completed_child_history():
    tracker, reporter = SubagentTracker(), TurnReporter()

    def observe(update):
        if update.session_id == "root":
            reporter.observe(update)
        for activity in tracker.observe(update):
            if activity.session_id == "root":
                reporter.observe(activity)

    observe(event("root", "subagent_spawned", subagentSessionId="child"))
    observe(event("child", "tool_call", toolCallId="child-tool", status="completed"))
    observe(
        event(
            "child",
            "async_task_spawned",
            asyncTaskId="shared",
            toolCallId="child-tool",
            canStop=True,
        )
    )
    observe(event("child", "async_task_state_update", asyncTaskId="shared", state="completed"))
    for index in range(100):
        observe(
            event("child", "async_task_state_update", asyncTaskId=str(index), state="completed")
        )
    observe(event("root", "tool_call", toolCallId="parent-tool", status="completed"))
    observe(
        event(
            "root",
            "async_task_spawned",
            asyncTaskId="shared",
            toolCallId="parent-tool",
            canStop=True,
        )
    )
    assert not reporter.owns_async_task("shared")
    observe(event("child", "agent_message_chunk", content={"type": "text", "text": "progress"}))
    assert not reporter.owns_async_task("shared")
    assert reporter.workers["child"]["async_tasks"]["shared"]["state"] == "completed"
