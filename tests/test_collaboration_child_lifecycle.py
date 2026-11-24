"""Child execution survives foreground completion and ordinary session replacement."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from test_collaboration import (
    active_gateway,
    api_fixture,
    ask,
    close_api,
    execution_report,
    project,
)
from test_collaboration_continuation_review import retained_parent, terminal
from test_collaboration_dispatch_review import close_reports
from test_collaboration_regressions import memory_broker

from mattermost_acp.acp.broker import AcpError, parse_session_update
from mattermost_acp.acp.subagents import SubagentTracker
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.mm.reporting import TurnReporter
from mattermost_acp.session_service import SessionService

pytest_plugins = ["test_session_service"]


def event(session, kind, **raw):
    return parse_session_update({"sessionId": session, "update": {"sessionUpdate": kind, **raw}})


def background_fields(mechanism):
    if mechanism == "requested":
        return {
            "_meta": {"claudeCode": {"toolName": "Bash"}},
            "rawInput": {"command": "synthetic command", "run_in_background": True},
        }
    return {"_meta": {"jetbrains": {"air": {"asyncTasks": {"backgrounded": True}}}}}


def observed_child(tracker, reporter, *, mechanism="requested", nested=False):
    def observe(update):
        for activity in tracker.observe(update):
            if activity.session_id == "root":
                reporter.observe_retained(activity)

    observe(event("root", "subagent_spawned", subagentSessionId="child"))
    child, parent = "child", "root"
    if nested:
        observe(event("child", "subagent_spawned", subagentSessionId="nested"))
        child, parent = "nested", "child"
    observe(event(child, "tool_call", toolCallId="bash", status="pending"))
    observe(
        event(
            child,
            "tool_call_update",
            toolCallId="bash",
            status="completed",
            **background_fields(mechanism),
        )
    )
    observe(event(parent, "subagent_state_update", subagentSessionId=child, state="completed"))
    if nested:
        observe(
            event("root", "subagent_state_update", subagentSessionId="child", state="completed")
        )
    return observe, child


@pytest.mark.parametrize("mechanism", ["requested", "backgrounded"])
@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("terminal_state", ["completed", "failed", "stopped"])
def test_child_detached_card_retains_debt_until_matching_task_termination(
    mechanism, nested, terminal_state
):
    tracker, reporter = SubagentTracker(), TurnReporter()
    observe, child = observed_child(tracker, reporter, mechanism=mechanism, nested=nested)
    assert reporter.has_pending_work()
    assert reporter.stoppable_async_tasks() == []
    assert tracker.workers[child].detached_tools == {"bash"}
    observe(event(child, "async_task_state_update", asyncTaskId="other", state="stopped"))
    assert reporter.has_pending_work()
    observe(
        event(child, "async_task_spawned", asyncTaskId="command", toolCallId="bash", canStop=True)
    )
    assert reporter.stoppable_async_tasks() == ["command"]
    observe(event(child, "async_task_state_update", asyncTaskId="command", state=terminal_state))
    assert not reporter.has_pending_work()
    observe(event(child, "async_task_progress", asyncTaskId="command", state="running"))
    observe(
        event(
            child,
            "tool_call_update",
            toolCallId="bash",
            status="completed",
            **background_fields(mechanism),
        )
    )
    assert not reporter.has_pending_work() and not reporter.owns_async_task("command")


@pytest.mark.parametrize("mechanism", ["requested", "backgrounded"])
def test_task_termination_before_detached_card_does_not_reopen_execution(mechanism):
    tracker, reporter = SubagentTracker(), TurnReporter()

    def observe(update):
        for activity in tracker.observe(update):
            reporter.observe(activity)

    observe(event("root", "subagent_spawned", subagentSessionId="child"))
    observe(event("child", "tool_call", toolCallId="bash", status="pending"))
    observe(event("child", "async_task_spawned", asyncTaskId="command", toolCallId="bash"))
    observe(event("child", "async_task_state_update", asyncTaskId="command", state="completed"))
    observe(
        event(
            "child",
            "tool_call_update",
            toolCallId="bash",
            status="completed",
            **background_fields(mechanism),
        )
    )
    observe(event("root", "subagent_state_update", subagentSessionId="child", state="completed"))
    assert not reporter.has_pending_work()


def test_child_completion_does_not_clear_unresolved_foreground_tool():
    tracker, reporter = SubagentTracker(), TurnReporter()
    for update in [
        event("root", "subagent_spawned", subagentSessionId="child"),
        event("child", "tool_call", toolCallId="foreground", status="pending"),
        event("root", "subagent_state_update", subagentSessionId="child", state="completed"),
    ]:
        for activity in tracker.observe(update):
            reporter.observe(activity)
    assert reporter.has_pending_work()
    for activity in tracker.observe(
        event("child", "tool_call_update", toolCallId="foreground", status="failed")
    ):
        reporter.observe(activity)
    assert not reporter.has_pending_work()


def test_unobserved_child_tool_result_retains_uncertainty_without_stop_ownership():
    tracker, reporter = SubagentTracker(), TurnReporter()
    for update in [
        event("root", "subagent_spawned", subagentSessionId="child"),
        event(
            "child",
            "tool_call_update",
            toolCallId="unknown",
            status="completed",
            **background_fields("backgrounded"),
        ),
        event("root", "subagent_state_update", subagentSessionId="child", state="completed"),
        event(
            "child", "async_task_spawned", asyncTaskId="command", toolCallId="unknown", canStop=True
        ),
    ]:
        for activity in tracker.observe(update):
            reporter.observe(activity)
    assert reporter.has_pending_work() and reporter.stoppable_async_tasks() == []


def test_child_foreground_output_does_not_invent_detached_execution():
    tracker, reporter = SubagentTracker(), TurnReporter()
    for update in [
        event("root", "subagent_spawned", subagentSessionId="child"),
        event("child", "tool_call", toolCallId="bash", status="pending"),
        event(
            "child",
            "tool_call_update",
            toolCallId="bash",
            status="completed",
            rawOutput="backgroundTaskId command run_in_background true",
        ),
        event("root", "subagent_state_update", subagentSessionId="child", state="completed"),
    ]:
        for activity in tracker.observe(update):
            reporter.observe(activity)
    assert not reporter.has_pending_work()


async def native(broker, session, kind, **raw):
    await broker._dispatch(
        {
            "method": "session/update",
            "params": {"sessionId": session, "update": {"sessionUpdate": kind, **raw}},
        }
    )


@pytest.mark.parametrize("mechanism", ["requested", "backgrounded"])
async def test_correlated_child_result_cannot_settle_unannounced_detached_execution(
    tmp_path, mechanism
):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, report):
        broker, writer = memory_broker()
        gateway.sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        gateway.sessions._native_epochs["discussion"] = 7
        broker.update_observer = gateway._observe_background_update
        report["reporter"] = TurnReporter()
        await native(broker, "existing-native", "subagent_spawned", subagentSessionId="child")
        await native(broker, "child", "tool_call", toolCallId="bash", status="pending")
        await native(
            broker,
            "child",
            "tool_call_update",
            toolCallId="bash",
            status="completed",
            **background_fields(mechanism),
        )
        await native(
            broker,
            "existing-native",
            "subagent_state_update",
            subagentSessionId="child",
            state="completed",
        )
        store.reply(parent["id"], "beta", "Foreground result", claim=parent["claim"])
        assert store.cancel(p["id"], "alpha")["status"] == "cancelling"
        key = parent["id"], parent["claim"]
        state = store.get_message(parent["id"], "beta")
        await consumer.settle_ownership(key, state)
        assert store.get_message(parent["id"], "beta")["execution_pending"]
        assert consumer.ownership[key] is report
        await native(
            broker,
            "child",
            "async_task_spawned",
            asyncTaskId="command",
            toolCallId="bash",
            canStop=True,
        )
        assert report["reporter"].stoppable_async_tasks() == ["command"]
        await native(
            broker, "child", "async_task_state_update", asyncTaskId="unrelated", state="completed"
        )
        assert report["reporter"].has_pending_work()
        await native(
            broker, "child", "async_task_state_update", asyncTaskId="command", state="stopped"
        )
        await consumer.settle_ownership(key, state)
        assert not store.get_message(parent["id"], "beta")["execution_pending"]
        assert store.get_project(p["id"], "alpha")["status"] == "cancelled"
        assert writer.messages == []
        await close_reports(gateway)


@pytest.mark.parametrize("close_support", ["unsupported", "acknowledged", "failed"])
async def test_closed_root_routes_matching_child_settlement_before_cleanup(close_support):
    broker, writer = memory_broker()
    broker.capabilities.close = close_support != "unsupported"
    if close_support == "failed":
        broker.request = AsyncMock(side_effect=AcpError("synthetic unsupported close"))
    updates = []
    broker.update_observer = updates.append
    for root, child in [("existing-native", "child"), ("other-root", "other-child")]:
        await native(broker, root, "subagent_spawned", subagentSessionId=child)
    await native(broker, "child", "tool_call", toolCallId="bash", status="completed")
    await native(
        broker,
        "child",
        "async_task_spawned",
        asyncTaskId="command",
        toolCallId="bash",
        canStop=True,
    )
    await native(
        broker,
        "existing-native",
        "subagent_state_update",
        subagentSessionId="child",
        state="completed",
    )
    await broker.session_close("existing-native")
    assert "child" in broker.subagents.workers
    updates.clear()
    await native(
        broker, "other-child", "async_task_state_update", asyncTaskId="command", state="completed"
    )
    assert "child" in broker.subagents.workers
    await native(
        broker, "child", "async_task_state_update", asyncTaskId="unrelated", state="completed"
    )
    assert "child" in broker.subagents.workers
    await native(broker, "child", "async_task_state_update", asyncTaskId="command", state="stopped")
    assert any(
        u.session_id == "existing-native"
        and u.kind == "subagent_activity"
        and u.raw["pending_async_work"] is False
        for u in updates
    )
    assert set(broker.subagents.workers) == {"other-child"}
    assert not broker.capabilities.close or close_support == "failed" or writer.messages


@pytest.mark.parametrize("close_supported", [False, True])
async def test_ordinary_new_retains_child_route_until_queued_project_work_can_resume(
    service, tmp_path, close_supported
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.sessions = service
    gateway.config.db_path = service.store._path
    gateway.run_prompt = AsyncMock(wraps=MattermostGateway.run_prompt.__get__(gateway))
    broker = service.broker
    broker.update_observer = gateway._observe_background_update
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    try:
        await consumer.reconcile_inherited_claims()
        old_sid = await service.ensure_session("discussion", channel_id="floor")
        broker.capabilities.close = close_supported
        report = execution_report(None, "ordinary-execution")
        gateway._reports[old_sid] = report
        gateway._last_terminal["discussion"] = terminal(
            "ordinary-execution", session_id=old_sid, pending=True
        )
        await native(broker, old_sid, "subagent_spawned", subagentSessionId="child")
        await native(broker, "child", "tool_call", toolCallId="bash", status="completed")
        await native(
            broker,
            "child",
            "async_task_spawned",
            asyncTaskId="command",
            toolCallId="bash",
            canStop=True,
        )
        await native(
            broker, old_sid, "subagent_state_update", subagentSessionId="child", state="completed"
        )
        await gateway.dispatch_command(
            thread_id="discussion",
            channel_id="floor",
            cmd="new",
            arg="",
            user_id="human",
            user_name="synthetic human",
        )
        new_sid = (await service.store.get("discussion")).session_id
        assert new_sid != old_sid and report["suppressed"]
        assert gateway.background_pending("discussion")
        p = project(store)
        requested = ask(store, p["id"])
        await consumer.handle(store.claim_next("beta", consumer.instance))
        assert store.get_message(requested["id"], "beta")["state"] == "queued"
        assert report["reporter"].has_pending_work()
        await native(
            broker, "child", "async_task_state_update", asyncTaskId="other", state="stopped"
        )
        assert gateway.background_pending("discussion")
        await native(
            broker, "child", "async_task_state_update", asyncTaskId="command", state="completed"
        )
        assert not report["reporter"].has_pending_work()
        assert not gateway.background_pending("discussion")
        assert "child" not in broker.subagents.workers
        await asyncio.sleep(2.1)
        delivery = store.claim_next("beta", consumer.instance)
        run_prompt = gateway.run_prompt

        async def explicit_reply(**options):
            receipt = await run_prompt(**options)
            await clients["beta"].request(
                "POST",
                f"/messages/{requested['id']}/reply",
                {"result": receipt["answer"], "claim": delivery["claim"]},
            )
            return receipt

        gateway.run_prompt = explicit_reply
        await asyncio.wait_for(consumer.handle(delivery), 5)
        assert store.get_message(requested["id"], "beta")["state"] == "done"
        assert (await service.store.get("discussion")).session_id == new_sid
    finally:
        await close_reports(gateway)
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("nested", [False, True])
def test_child_initial_background_request_survives_plain_completion_card(nested):
    tracker, reporter = SubagentTracker(), TurnReporter()

    def observe(update):
        for activity in tracker.observe(update):
            if activity.session_id == "root":
                reporter.observe(activity)

    observe(event("root", "subagent_spawned", subagentSessionId="child"))
    child, parent = "child", "root"
    if nested:
        observe(event("child", "subagent_spawned", subagentSessionId="nested"))
        child, parent = "nested", "child"
    observe(
        event(
            child,
            "tool_call",
            toolCallId="bash",
            status="pending",
            **background_fields("requested"),
        )
    )
    observe(event(child, "tool_call_update", toolCallId="bash", status="completed"))
    observe(event(parent, "subagent_state_update", subagentSessionId=child, state="completed"))
    if nested:
        observe(
            event("root", "subagent_state_update", subagentSessionId="child", state="completed")
        )
    assert reporter.has_pending_work()
    observe(
        event(child, "async_task_spawned", asyncTaskId="command", toolCallId="bash", canStop=True)
    )
    observe(event(child, "async_task_state_update", asyncTaskId="command", state="stopped"))
    assert not reporter.has_pending_work()


async def test_close_retains_foreground_child_and_nested_debt_until_last_terminal_snapshot():
    broker, _writer = memory_broker()
    broker.capabilities.close = False
    reporter = TurnReporter()
    broker.update_observer = lambda update: (
        reporter.observe_retained(update) if update.session_id == "existing-native" else None
    )
    await native(broker, "existing-native", "subagent_spawned", subagentSessionId="child")
    await native(broker, "child", "subagent_spawned", subagentSessionId="nested")
    await broker.session_close("existing-native")
    assert broker.subagents.pending("existing-native")
    assert not broker.native_boundary_idle
    await native(
        broker,
        "existing-native",
        "subagent_state_update",
        subagentSessionId="child",
        state="completed",
    )
    assert "nested" in broker.subagents.workers and reporter.has_pending_work()
    await native(
        broker, "child", "subagent_state_update", subagentSessionId="nested", state="completed"
    )
    assert not reporter.has_pending_work() and not broker.subagents.workers
    assert not broker.native_boundary_idle
