"""Native sibling task collisions remain unowned while any copy is pending."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_collaboration_regressions import memory_broker

from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.mm.reporting import TurnReporter
from mattermost_acp.session_service import SessionService


async def collision_sequence(*, terminal="completed", nested=False, unrelated=False):
    broker, writer = memory_broker()
    reporters = {"existing-native": TurnReporter(), "unrelated": TurnReporter()}

    def observe(update):
        if update.session_id in reporters:
            reporters[update.session_id].observe(update)

    broker.update_observer = observe

    async def dispatch(session, kind, **fields):
        await broker._dispatch(
            {
                "method": "session/update",
                "params": {
                    "sessionId": session,
                    "update": {"sessionUpdate": kind, **fields},
                },
            }
        )

    first_parent = "existing-native"
    second_parent = "unrelated" if unrelated else "existing-native"
    if nested:
        await dispatch(first_parent, "subagent_spawned", subagentSessionId="first-branch")
        await dispatch(second_parent, "subagent_spawned", subagentSessionId="second-branch")
        first_parent, second_parent = "first-branch", "second-branch"
    await dispatch(first_parent, "subagent_spawned", subagentSessionId="first")
    await dispatch(second_parent, "subagent_spawned", subagentSessionId="second")
    await dispatch("first", "tool_call", toolCallId="first-tool", status="completed")
    await dispatch(
        "first", "async_task_spawned", asyncTaskId="shared", toolCallId="first-tool", canStop=True
    )
    reporter = reporters["existing-native"]
    assert reporter.owns_async_task("shared")
    cached_first = reporter.workers["first"]
    await dispatch("second", "tool_call", toolCallId="second-tool", status="completed")
    await dispatch(
        "second", "async_task_spawned", asyncTaskId="shared", toolCallId="second-tool", canStop=True
    )
    assert reporter.owns_async_task("shared") is unrelated
    await dispatch("second", "async_task_state_update", asyncTaskId="shared", state=terminal)
    await dispatch("second", "async_task_state_update", asyncTaskId="later", state="completed")
    assert reporter.workers["first"] is cached_first
    assert cached_first["async_tasks"]["shared"]["owned"] is True
    return broker, writer, reporters, dispatch


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("terminal", ["completed", "failed", "stopped"])
async def test_native_sibling_collision_survives_terminal_history_compaction(nested, terminal):
    broker, _writer, reporters, _dispatch = await collision_sequence(
        nested=nested, terminal=terminal
    )
    reporter = reporters["existing-native"]
    assert broker.subagents.workers["second"].async_tasks["shared"]["state"] == terminal
    assert not reporter.owns_async_task("shared")
    assert "shared" not in reporter.stoppable_async_tasks()
    assert reporter.workers["second"]["async_tasks"]["shared"]["state"] == terminal


@pytest.mark.parametrize("unrelated", [False, True])
async def test_project_stop_never_writes_ambiguous_native_task_id(tmp_path, unrelated):
    broker, writer, reporters, _dispatch = await collision_sequence(unrelated=unrelated)
    records = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(session_id="existing-native"))
    )
    sessions = SessionService(broker, records, default_cwd=tmp_path)
    sessions._native_epochs["discussion"] = 7
    consumer = CollaborationConsumer.__new__(CollaborationConsumer)
    consumer.gateway = SimpleNamespace(sessions=sessions)
    await consumer.stop_owned_async_tasks(
        "discussion",
        reporters["existing-native"],
        session="existing-native",
        epoch=7,
        guard=lambda: True,
    )
    expected = [{"sessionId": "existing-native", "asyncTaskId": "shared"}] if unrelated else []
    assert [message["params"] for message in writer.messages] == expected
    assert all(message["method"] == "_session/async_task/stop" for message in writer.messages)


@pytest.mark.parametrize("nested", [False, True])
async def test_unrelated_roots_keep_independent_native_stop_ownership(nested):
    _broker, _writer, reporters, _dispatch = await collision_sequence(nested=nested, unrelated=True)
    assert reporters["existing-native"].owns_async_task("shared")
    assert "shared" not in reporters["unrelated"].stoppable_async_tasks()
    assert "shared" not in reporters["unrelated"].workers["second"]["async_tasks"]


@pytest.mark.parametrize("nested", [False, True])
async def test_settled_collision_evidence_compacts_with_unrelated_terminal_history(nested):
    broker, _writer, reporters, dispatch = await collision_sequence(nested=nested)
    await dispatch("first", "async_task_state_update", asyncTaskId="shared", state="completed")
    await dispatch("first", "async_task_state_update", asyncTaskId="later-first", state="completed")
    for index in range(100):
        await dispatch(
            "second", "async_task_state_update", asyncTaskId=str(index), state="completed"
        )
        assert len(reporters["existing-native"].workers["second"]["async_tasks"]) == 1
    await dispatch("first", "tool_call", toolCallId="new-tool", status="completed")
    await dispatch(
        "first", "async_task_spawned", asyncTaskId="unique", toolCallId="new-tool", canStop=True
    )
    assert reporters["existing-native"].owns_async_task("unique")
    assert not reporters["existing-native"].owns_async_task("shared")
    assert len(broker.subagents.workers["second"].async_tasks) == 102


async def test_forgetting_pending_source_releases_only_its_collision_evidence():
    broker, _writer, reporters, dispatch = await collision_sequence()
    broker.subagents.forget("first")
    await dispatch("second", "async_task_state_update", asyncTaskId="newer", state="completed")
    assert set(reporters["existing-native"].workers["second"]["async_tasks"]) == {"newer"}
    await dispatch("second", "tool_call", toolCallId="unique-tool", status="completed")
    await dispatch(
        "second", "async_task_spawned", asyncTaskId="unique", toolCallId="unique-tool", canStop=True
    )
    assert reporters["existing-native"].owns_async_task("unique")
