"""Session settings and child task stops retain their actual native ownership."""

import asyncio

import pytest
from test_collaboration_continuation_review import retained_parent
from test_collaboration_dispatch_review import close_reports
from test_collaboration_regressions import memory_broker

from mattermost_acp.acp import SessionUpdate
from mattermost_acp.acp.subagents import SubagentTracker
from mattermost_acp.mm.reporting import TurnReporter
from mattermost_acp.session_service import SessionService

pytest_plugins = ["test_session_service"]


@pytest.mark.parametrize("scope", ["thread", "channel", "harness", "effort"])
async def test_settings_commit_serializes_replacement_cancellation(service, scope):
    sid = await service.ensure_session("settings-thread", channel_id="channel")
    epoch = service.native_epoch("settings-thread")
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    save, cancel = service.store.save_switch_if_session, service.broker.cancel

    async def paused_save(*args, **kwargs):
        entered.set()
        await release.wait()
        return await save(*args, **kwargs)

    async def observed_cancel(*args, **kwargs):
        cancelled.set()
        return await cancel(*args, **kwargs)

    service.store.save_switch_if_session = paused_save
    service.broker.cancel = observed_cancel
    switch = asyncio.create_task(
        service.set_effort("settings-thread", "high")
        if scope == "effort"
        else service.switch_model(
            "settings-thread", "hub-claude-5-sonnet", scope=scope, channel_id="channel"
        )
    )
    replacement = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        replacement = asyncio.create_task(
            service.new_session("settings-thread", channel_id="channel")
        )
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(cancelled.wait(), 0.1)
        assert service.native_epoch("settings-thread") == epoch
        assert (await service.store.get("settings-thread")).session_id == sid
        release.set()
        await switch
        new_sid = await replacement
        assert cancelled.is_set() and new_sid != sid
    finally:
        release.set()
        await asyncio.gather(
            switch, *([replacement] if replacement else []), return_exceptions=True
        )


def update(session, kind, **raw):
    return SessionUpdate(session, kind, raw=raw, tool_call_id=raw.get("toolCallId"))


@pytest.mark.parametrize("nested", [False, True])
def test_child_task_capabilities_follow_verified_tools_and_terminal_lifecycle(nested):
    tracker, reporter = SubagentTracker(), TurnReporter()

    def observe(event):
        for activity in tracker.observe(event):
            if activity.session_id == "root":
                reporter.observe(activity)

    observe(update("root", "subagent_spawned", subagentSessionId="child"))
    child = "child"
    if nested:
        observe(update("child", "subagent_spawned", subagentSessionId="grandchild"))
        child = "grandchild"
    observe(update(child, "tool_call", toolCallId="owned-bash"))
    observe(
        update(
            child,
            "async_task_spawned",
            asyncTaskId="command",
            toolCallId="owned-bash",
            canStop=True,
        )
    )
    observe(
        update(
            "child" if nested else "root",
            "subagent_state_update",
            subagentSessionId=child,
            state="completed",
        )
    )
    assert reporter.has_pending_work()
    assert reporter.stoppable_async_tasks() == ["command"]
    observe(update(child, "async_task_state_update", asyncTaskId="command", state="stopped"))
    assert not reporter.owns_async_task("command")
    observe(update(child, "async_task_progress", asyncTaskId="command", state="running"))
    assert not reporter.owns_async_task("command")


def test_child_task_duplicate_ids_do_not_authorize_ambiguous_stops():
    tracker, reporter = SubagentTracker(), TurnReporter()
    for child in ("first", "second"):
        for event in (
            update("root", "subagent_spawned", subagentSessionId=child),
            update(child, "tool_call", toolCallId="bash-" + child),
            update(
                child,
                "async_task_spawned",
                asyncTaskId="shared-id",
                toolCallId="bash-" + child,
                canStop=True,
            ),
        ):
            for activity in tracker.observe(event):
                reporter.observe(activity)
    assert reporter.has_pending_work() and reporter.stoppable_async_tasks() == []


async def test_project_cancellation_stops_owned_child_task_through_root_native_session(tmp_path):
    async with retained_parent(tmp_path) as (
        store,
        _clients,
        gateway,
        consumer,
        project,
        parent,
        report,
    ):
        broker, writer = memory_broker()
        gateway.sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        gateway.sessions._native_epochs["discussion"] = 7
        broker.update_observer = gateway._observe_background_update
        report["reporter"] = TurnReporter()

        async def native(session, kind, **raw):
            await broker._dispatch(
                {
                    "method": "session/update",
                    "params": {"sessionId": session, "update": {"sessionUpdate": kind, **raw}},
                }
            )

        await native("existing-native", "subagent_spawned", subagentSessionId="child")
        await native("child", "tool_call", toolCallId="child-bash")
        await native(
            "child",
            "tool_call_update",
            toolCallId="child-bash",
            status="completed",
            _meta={"jetbrains": {"air": {"asyncTasks": {"backgrounded": True}}}},
        )
        await native(
            "child",
            "async_task_spawned",
            asyncTaskId="child-command",
            toolCallId="child-bash",
            canStop=True,
        )
        await native(
            "existing-native", "subagent_state_update", subagentSessionId="child", state="completed"
        )
        store.reply(parent["id"], "beta", "Foreground result", claim=parent["claim"])
        store.cancel(project["id"], "alpha")
        control = store.claim_next("beta", consumer.instance)
        requested = []
        stop_requested = asyncio.Event()

        async def stop_task(session, task_id, *, guard):
            assert guard()
            requested.append((session, task_id))
            stop_requested.set()
            return True

        broker.stop_async_task = stop_task
        cancellation = asyncio.create_task(consumer.handle_cancel(control))
        await asyncio.wait_for(stop_requested.wait(), 3)
        assert requested == [("existing-native", "child-command")]
        assert store.get_message(parent["id"], "beta")["execution_pending"]
        await native(
            "child", "async_task_state_update", asyncTaskId="child-command", state="stopped"
        )
        await asyncio.wait_for(cancellation, 3)
        state = store.get_message(parent["id"], "beta")
        await consumer.settle_ownership((parent["id"], parent["claim"]), state)
        assert not store.get_message(parent["id"], "beta")["execution_pending"]
        await close_reports(gateway)
