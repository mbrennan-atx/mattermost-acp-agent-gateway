"""Child stop failures cannot consume the independently guarded parent stop."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_collaboration_continuation_review import retained_parent, terminal
from test_collaboration_regressions import memory_broker

from mattermost_acp.acp.broker import AcpError, NativeDispatchChanged
from mattermost_acp.session_service import SessionService


@pytest.fixture
def fast_stop_bounds(monkeypatch):
    timeout, sleep = asyncio.timeout, asyncio.sleep

    def bounded(delay):
        return timeout(0.005 if delay == 3 else delay)

    async def poll(delay):
        await sleep(0 if delay == 1 else delay)

    monkeypatch.setattr(asyncio, "timeout", bounded)
    monkeypatch.setattr(asyncio, "sleep", poll)


def native_stops(gateway, report, tmp_path):
    broker, writer = memory_broker()
    gateway.sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
    gateway.sessions._native_epochs["discussion"] = 7
    report["native_dispatched"] = True
    report["reporter"] = SimpleNamespace(
        has_pending_work=lambda: True,
        stoppable_async_tasks=lambda: ["first-task", "other-task"],
        owns_async_task=lambda task: task in {"first-task", "other-task"},
    )
    broker.cancel = AsyncMock(wraps=broker.cancel)
    return broker, writer


async def child_stop_failure(task_id, failure):
    if task_id == "other-task":
        return True
    if failure == "timeout":
        await asyncio.Event().wait()
    if failure == "task_changed":
        raise NativeDispatchChanged("Only this child task lost its stop capability")
    if failure == "unsupported":
        raise AcpError("Adapter does not support individual async task stops")
    raise OSError("Child stop request failed")


@pytest.mark.parametrize("failure", ["error", "timeout", "task_changed", "unsupported"])
async def test_deadline_child_stop_failure_still_cancels_parent(
    tmp_path, fast_stop_bounds, failure
):
    async with retained_parent(tmp_path) as (
        _store, _clients, gateway, consumer, project, parent, report
    ):
        broker, writer = native_stops(gateway, report, tmp_path)

        async def stop(_session, task_id, *, guard):
            assert guard()
            return await child_stop_failure(task_id, failure)

        broker.stop_async_task = AsyncMock(side_effect=stop)
        consumer.project_deadlines[project["id"]] = -1
        consumer.refresh_budget = AsyncMock()
        await asyncio.wait_for(consumer.enforce_deadline(project["id"]), 0.5)
        assert [message["method"] for message in writer.messages] == ["session/cancel"]
        assert writer.messages[0]["params"] == {"sessionId": "existing-native"}
        broker.cancel.assert_awaited_once()
        assert broker.stop_async_task.await_count == (1 if failure == "timeout" else 2)
        key = (parent["id"], parent["claim"])
        assert consumer.ownership[key] is report and consumer.ownership_pending(key)
        assert key in consumer.cancelled_claims
        await consumer.enforce_deadline(project["id"])
        broker.cancel.assert_awaited_once()


@pytest.mark.parametrize("failure", ["error", "timeout", "task_changed", "unsupported"])
async def test_hub_child_stop_failure_still_cancels_parent_and_preserves_debt(
    tmp_path, fast_stop_bounds, failure
):
    async with retained_parent(tmp_path) as (
        store, _clients, gateway, consumer, project, parent, report
    ):
        broker, writer = native_stops(gateway, report, tmp_path)

        async def stop(_session, task_id, *, guard):
            assert guard()
            return await child_stop_failure(task_id, failure)

        broker.stop_async_task = AsyncMock(side_effect=stop)
        store.cancel(project["id"], "alpha")
        control = store.claim_next("beta", consumer.instance, controls_only=True)
        await asyncio.wait_for(consumer.handle_cancel(control), 1.5)
        assert [message["method"] for message in writer.messages] == ["session/cancel"] * 20
        assert all(
            message["params"] == {"sessionId": "existing-native"} for message in writer.messages
        )
        assert store.get_message(control["id"], "beta")["state"] == "queued"
        state = store.get_message(parent["id"], "beta")
        assert state["state"] == "cancelling" and state["execution_pending"]
        assert consumer.ownership[(parent["id"], parent["claim"])] is report
        assert report["async_stop_requested"] == (
            set() if failure == "timeout" else {"other-task"}
        )
        assert broker.stop_async_task.await_count == (20 if failure == "timeout" else 21)


@pytest.mark.parametrize("path", ["deadline", "hub"])
@pytest.mark.parametrize(
    "changed", ["report", "ownership", "execution", "project", "reserving", "epoch", "session"]
)
async def test_parent_stop_rechecks_ownership_after_awaited_child_stop(
    tmp_path, fast_stop_bounds, path, changed
):
    async with retained_parent(tmp_path) as (
        store, _clients, gateway, consumer, project, parent, report
    ):
        broker, writer = native_stops(gateway, report, tmp_path)
        key = (parent["id"], parent["claim"])

        async def stop(_session, _task, *, guard):
            assert guard()
            await asyncio.sleep(0)
            if changed == "report":
                gateway._reports["existing-native"] = {**report}
            elif changed == "ownership":
                consumer.ownership[key] = {**report}
            elif changed == "execution":
                report["execution_id"] = "replacement-execution"
            elif changed == "project":
                report["collaboration_project"] = "another-project"
            elif changed == "reserving":
                consumer.reserving.add(key)
            elif changed == "epoch":
                gateway.sessions._native_epochs["discussion"] = 8
            else:
                gateway.sessions.store.get.return_value.session_id = "replacement-native"
            return True

        # Only one child request is needed to exercise the awaited boundary.
        report["reporter"].stoppable_async_tasks = lambda: ["first-task"]
        broker.stop_async_task = AsyncMock(side_effect=stop)
        if path == "deadline":
            consumer.project_deadlines[project["id"]] = -1
            consumer.refresh_budget = AsyncMock()
            await consumer.enforce_deadline(project["id"])
        else:
            store.cancel(project["id"], "alpha")
            control = store.claim_next("beta", consumer.instance, controls_only=True)
            await asyncio.wait_for(consumer.handle_cancel(control), 1.5)
            assert store.get_message(control["id"], "beta")["state"] == "blocked"
        assert writer.messages == []
        assert key in consumer.ownership
        assert store.get_message(parent["id"], "beta")["execution_pending"]


async def test_hub_accepted_child_stop_is_once_and_waits_for_matching_lifecycle(
    tmp_path, fast_stop_bounds
):
    async with retained_parent(tmp_path) as (
        store, _clients, gateway, consumer, project, parent, report
    ):
        broker, writer = native_stops(gateway, report, tmp_path)

        async def stop(_session, _task, *, guard):
            assert guard()
            return True

        broker.stop_async_task = AsyncMock(side_effect=stop)
        store.cancel(project["id"], "alpha")
        control = store.claim_next("beta", consumer.instance, controls_only=True)
        await consumer.handle_cancel(control)
        assert broker.stop_async_task.await_count == 2
        assert report["async_stop_requested"] == {"first-task", "other-task"}
        assert len(writer.messages) == 20
        assert store.get_message(parent["id"], "beta")["execution_pending"]
        # Neither accepted task stops nor parent cancellation release ownership.
        report["reporter"].has_pending_work = lambda: False
        gateway._last_terminal["discussion"] = terminal(stop_reason="cancelled")
        next_poll = store.clock() + 3
        store.clock = lambda: next_poll
        prior_elapsed = store.elapsed
        store.elapsed = lambda: prior_elapsed() + 3
        control = store.claim_next("beta", consumer.instance, controls_only=True)
        assert control is not None
        await consumer.handle_cancel(control)
        await consumer.reconcile_ownership()
        assert not store.get_message(parent["id"], "beta")["execution_pending"]
        assert (parent["id"], parent["claim"]) not in consumer.ownership


@pytest.mark.parametrize("path", ["direct", "reconcile"])
async def test_released_ownership_discards_only_its_cancellation_marker(tmp_path, path):
    async with retained_parent(tmp_path) as (
        store, _clients, _gateway, consumer, _project, parent, _report
    ):
        key, other = (parent["id"], parent["claim"]), ("other-request", "other-claim")
        consumer.cancelled_claims.update({key, other})
        if path == "direct":
            consumer.release_ownership(key)
        else:
            store.reply(parent["id"], "beta", "Correlated result", claim=parent["claim"])
            await consumer.reconcile_ownership()
        assert key not in consumer.ownership
        assert consumer.cancelled_claims == {other}


@pytest.mark.parametrize("path", ["deadline", "hub"])
@pytest.mark.parametrize("failure", ["error", "timeout"])
async def test_parent_stop_failure_is_bounded_and_keeps_execution_unresolved(
    tmp_path, fast_stop_bounds, path, failure
):
    async with retained_parent(tmp_path) as (
        store, _clients, gateway, consumer, project, parent, report
    ):
        broker, writer = native_stops(gateway, report, tmp_path)
        broker.stop_async_task = AsyncMock(return_value=True)

        async def cancel(_session, *, guard):
            assert guard()
            if failure == "timeout":
                await asyncio.Event().wait()
            raise OSError("Parent cancellation delivery is uncertain")

        broker.cancel.side_effect = cancel
        if path == "deadline":
            consumer.project_deadlines[project["id"]] = -1
            consumer.refresh_budget = AsyncMock()
            await asyncio.wait_for(consumer.enforce_deadline(project["id"]), 0.5)
            await consumer.enforce_deadline(project["id"])
            assert broker.cancel.await_count == 1
        else:
            store.cancel(project["id"], "alpha")
            control = store.claim_next("beta", consumer.instance, controls_only=True)
            await asyncio.wait_for(consumer.handle_cancel(control), 1.5)
            assert store.get_message(control["id"], "beta")["state"] == "queued"
        assert writer.messages == []
        key = (parent["id"], parent["claim"])
        assert consumer.ownership[key] is report and consumer.ownership_pending(key)
        assert store.get_message(parent["id"], "beta")["execution_pending"]


@pytest.mark.parametrize("path", ["deadline", "hub"])
async def test_external_cancellation_during_child_stop_propagates(tmp_path, path):
    async with retained_parent(tmp_path) as (
        store, _clients, gateway, consumer, project, parent, report
    ):
        broker, writer = native_stops(gateway, report, tmp_path)
        entered = asyncio.Event()

        async def stop(_session, _task, *, guard):
            assert guard()
            entered.set()
            await asyncio.Event().wait()

        broker.stop_async_task = AsyncMock(side_effect=stop)
        if path == "deadline":
            observer = consumer.deadline_tasks.pop(project["id"], None)
            if observer:
                observer.cancel()
                await asyncio.gather(observer, return_exceptions=True)
            consumer.project_deadlines[project["id"]] = -1
            consumer.refresh_budget = AsyncMock()
            operation = consumer.enforce_deadline(project["id"])
        else:
            store.cancel(project["id"], "alpha")
            control = store.claim_next("beta", consumer.instance, controls_only=True)
            operation = consumer.handle_cancel(control)
        task = asyncio.create_task(operation)
        try:
            await asyncio.wait_for(entered.wait(), 0.5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            broker.cancel.assert_not_awaited()
            assert writer.messages == []
            assert consumer.ownership[(parent["id"], parent["claim"])] is report
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
