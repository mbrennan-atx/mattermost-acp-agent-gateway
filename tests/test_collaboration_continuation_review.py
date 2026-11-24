"""Continuation and stop boundaries use synthetic actors and local adapters only."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_regressions import (
    auth,
    maintenance_client,
    maintenance_gateway,
    memory_broker,
)

from mattermost_acp.acp import TurnResult
from mattermost_acp.acp.broker import NativeDispatchChanged
from mattermost_acp.collaboration.client import Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.session_service import SessionService


def report(project_id, execution="parent-execution"):
    return {
        "execution_id": execution,
        "collaboration_project": project_id,
        "thread_id": "discussion",
        "channel_id": "floor",
        "active": False,
        "foreground_complete": True,
        "reporter": SimpleNamespace(has_pending_work=lambda: False),
        "task": None,
    }


def terminal(execution="parent-execution", **changes):
    return {
        "execution_id": execution,
        "session_id": "existing-native",
        "stop_reason": "end_turn",
        "answer": "Verified native evidence",
        "pending": False,
        "native_dispatched": True,
        "completed_at": 2**32,
        **changes,
    }


@asynccontextmanager
async def retained_parent(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False, steering=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.native_epoch = lambda _thread: 7
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    try:
        p = project(store)
        ask(store, p["id"])
        parent = store.claim_next("beta", consumer.instance)
        owned = report(p["id"])
        gateway._reports["existing-native"] = owned
        assert await consumer.reserve(parent, "existing-native")
        gateway._last_terminal["discussion"] = terminal()
        yield store, clients, gateway, consumer, p, parent, owned
    finally:
        for owned_report in gateway.live_reports():
            task = owned_report.get("task")
            if task and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await consumer.close()
        await close_api(store, server, clients)


def child_result(store, p, parent, consumer):
    child = ask(
        store,
        p["id"],
        sender="beta",
        recipient="gamma",
        key="child",
        parent=parent["id"],
    )
    claimed = store.claim_next("gamma", "peer-worker")
    store.reply(child["id"], "gamma", "Peer verified the dependency", claim=claimed["claim"])
    return store.claim_next("beta", consumer.instance)


async def test_idle_parent_continuation_rebinds_native_report_and_queues_unrelated_work(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        delivery = child_result(store, p, parent, consumer)
        assert delivery["continuation_parent"] == parent["id"]
        assert delivery["continuation_claim"] == parent["claim"]
        gateway.run_prompt = AsyncMock(wraps=type(gateway).run_prompt.__get__(gateway))
        gateway.sessions.ensure_session = AsyncMock(return_value="existing-native")
        gateway.sessions.status = AsyncMock(return_value={"session_id": "existing-native"})
        gateway.sessions.native_epoch = lambda _thread: 8
        observed = []

        async def native(thread, _text, **kwargs):
            assert thread == "discussion"
            assert await kwargs["before_prompt"]("existing-native")
            assert kwargs["dispatch_guard"]()
            current = gateway._reports["existing-native"]
            assert current is not old
            assert consumer.ownership[(parent["id"], parent["claim"])] is current
            receipt = store.get_message(parent["id"], "beta")
            assert receipt["evidence"]["native_epoch"] == 8
            assert receipt["evidence"]["execution_id"] == current["execution_id"]
            observed.append(current)
            return TurnResult("existing-native", "end_turn", "Peer result incorporated", "")

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        await consumer.handle(delivery)
        gateway.sessions.hint.assert_not_awaited()
        gateway.sessions.run_turn.assert_awaited_once()
        assert store.get_message(delivery["id"], "beta")["state"] == "done"
        assert store.get_message(parent["id"], "beta")["state"] == "running"
        assert consumer.ownership == {(parent["id"], parent["claim"]): observed[0]}
        assert observed[0]["foreground_complete"] and not observed[0]["active"]
        assert gateway._last_terminal["discussion"]["execution_id"] == observed[0]["execution_id"]

        unrelated = ask(store, p["id"], key="unrelated")
        await consumer.handle(store.claim_next("beta", consumer.instance))
        assert store.get_message(unrelated["id"], "beta")["state"] == "queued"
        gateway.sessions.run_turn.assert_awaited_once()
        assert consumer.owns_thread("discussion")


@pytest.mark.parametrize("invalidate", ["busy", "active", "tools", "cancelled", "session"])
async def test_child_continuation_preserves_unsettled_or_changed_parent(tmp_path, invalidate):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        delivery = child_result(store, p, parent, consumer)
        if invalidate == "busy":
            gateway.sessions.is_busy = lambda _thread: True
        elif invalidate == "active":
            old["active"] = True
        elif invalidate == "tools":
            old["reporter"].has_pending_work = lambda: True
        elif invalidate == "cancelled":
            consumer.cancelled_claims.add((parent["id"], parent["claim"]))
        else:
            gateway._last_terminal["discussion"]["session_id"] = "another-native"
        await consumer.handle(delivery)
        assert store.get_message(delivery["id"], "beta")["state"] == "queued"
        gateway.run_prompt.assert_not_awaited()
        assert consumer.ownership[(parent["id"], parent["claim"])] is old


@pytest.mark.parametrize(
    "mismatch", ["session", "execution", "pending", "not_dispatched", "foreground", "tools"]
)
async def test_late_ambiguous_reply_releases_only_matching_native_terminal(tmp_path, mismatch):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, _p, parent, old):
        await consumer.update(parent, "unconfirmed", "Native injection acceptance was ambiguous")
        store.reply(parent["id"], "beta", "Explicit late result", claim=parent["claim"])
        receipt = gateway._last_terminal["discussion"]
        if mismatch == "session":
            receipt["session_id"] = "another-native"
        elif mismatch == "execution":
            receipt["execution_id"] = "another-execution"
        elif mismatch == "pending":
            receipt["pending"] = True
        elif mismatch == "not_dispatched":
            receipt["native_dispatched"] = False
        elif mismatch == "foreground":
            old["foreground_complete"] = False
        else:
            old["reporter"].has_pending_work = lambda: True
        await consumer.reconcile_ownership()
        assert consumer.ownership[(parent["id"], parent["claim"])] is old

        old["foreground_complete"] = True
        old["reporter"].has_pending_work = lambda: False
        gateway._last_terminal["discussion"] = terminal()
        await consumer.reconcile_ownership()
        assert not consumer.owns_thread("discussion")
        settled = store.get_message(parent["id"], "beta")
        assert settled["result"] == "Explicit late result"
        assert settled["evidence"]["foreground_answer"] == "Verified native evidence"


async def test_false_dispatch_receipt_does_not_prove_older_parent_cancelled(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        gateway._last_terminal["discussion"] = terminal(
            stop_reason="cancelled", native_dispatched=False
        )
        store.cancel(p["id"], "alpha")
        control = store.claim_next("beta", consumer.instance)
        gateway.sessions.cancel.side_effect = NativeDispatchChanged("Native ownership changed")
        await consumer.handle_cancel(control)
        assert store.get_message(parent["id"], "beta")["state"] == "cancelling"
        assert store.get_message(control["id"], "beta")["state"] == "blocked"
        assert consumer.ownership[(parent["id"], parent["claim"])] is old
        gateway.sessions.cancel.assert_awaited_once()


async def test_cancellation_during_continuation_transfer_keeps_parent_unsettled(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        delivery = child_result(store, p, parent, consumer)
        unrelated = project(store)
        untouched = ask(store, unrelated["id"], recipient="gamma", key="unrelated-project")
        broker, writer = memory_broker()
        broker.capabilities.steering = False
        sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        sessions.ensure_session = AsyncMock(return_value="existing-native")
        sessions.status = AsyncMock(return_value={"session_id": "existing-native"})
        sessions.cancel = AsyncMock(side_effect=NativeDispatchChanged("No dispatched native turn"))
        gateway.sessions = sessions
        gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
        reserve_continuation = consumer.reserve_continuation
        accepted = []

        async def reserve(*args):
            result = await reserve_continuation(*args)
            accepted.append(result)
            return result

        consumer.reserve_continuation = AsyncMock(side_effect=reserve)
        entered, release = asyncio.Event(), asyncio.Event()
        original = consumer.update

        async def delayed(item, state, *args, **kwargs):
            receipt = await original(item, state, *args, **kwargs)
            if item["id"] == parent["id"] and state == "dispatch":
                entered.set()
                await release.wait()
            return receipt

        consumer.update = delayed
        handling = asyncio.create_task(consumer.handle(delivery))
        cancellation = None
        try:
            await asyncio.wait_for(entered.wait(), 2)
            moved = gateway._reports["existing-native"]
            assert moved is not old
            assert consumer.ownership[(parent["id"], parent["claim"])] is moved
            evidence = store.get_message(parent["id"], "beta")["evidence"]
            assert evidence["execution_id"] == moved["execution_id"]
            assert evidence["native_epoch"] == sessions.native_epoch("discussion")
            assert (parent["id"], parent["claim"]) in consumer.reserving
            store.cancel(p["id"], "alpha")
            control = store.claim_next("beta", consumer.instance)
            cancellation = asyncio.create_task(consumer.handle_cancel(control))
            async with asyncio.timeout(2):
                while not moved.get("cancel_requested"):
                    await asyncio.sleep(0)
            assert not cancellation.done()
            release.set()
            await asyncio.wait_for(handling, 2)
            assert consumer.reserve_continuation.await_count == 1
            assert accepted == [False]
            cached = gateway._last_terminal["discussion"]
            assert cached["execution_id"] == old["execution_id"]
            assert cached["stop_reason"] == "end_turn" and cached["native_dispatched"]
            restored = store.get_message(parent["id"], "beta")["evidence"]
            assert restored["execution_id"] == old["execution_id"]
            assert restored["native_epoch"] == sessions.native_epoch("discussion")
            await asyncio.wait_for(cancellation, 3)
            assert store.get_message(parent["id"], "beta")["state"] == "cancelling"
            assert consumer.ownership[(parent["id"], parent["claim"])] is old
            assert store.show(unrelated["id"], "alpha")["status"] == "active"
            assert store.get_message(untouched["id"], "gamma")["state"] == "queued"
            assert writer.messages == []
        finally:
            release.set()
            tasks = [handling] + ([cancellation] if cancellation else [])
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_before_prompt_rejection_has_false_dispatch_flag_and_no_native_write(tmp_path):
    broker, writer = memory_broker()
    sessions = SessionService(
        broker,
        SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(session_id="existing-native"))),
        default_cwd=tmp_path,
    )
    sessions.ensure_session = AsyncMock(return_value="existing-native")
    before_prompt = AsyncMock(return_value=False)
    result = await sessions.run_turn(
        "discussion", "Rejected continuation", before_prompt=before_prompt
    )
    before_prompt.assert_awaited_once_with("existing-native")
    assert result.stop_reason == "cancelled" and not result.native_dispatched
    assert writer.messages == [] and broker._pending == {}
    assert not sessions.is_busy("discussion")


@pytest.mark.parametrize("refusal", ["workspace", "background"])
async def test_rejected_preparation_preserves_parent_epoch_before_cancellation(tmp_path, refusal):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        delivery = child_result(store, p, parent, consumer)
        broker, writer = memory_broker()
        broker.capabilities.steering = False
        sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        sessions._native_epochs["discussion"] = 7
        sessions.status = AsyncMock(return_value={"session_id": "existing-native"})
        preparations = 0

        async def prepare(*args, **kwargs):
            nonlocal preparations
            preparations += 1
            if preparations == 2:
                if refusal == "workspace":
                    raise NativeDispatchChanged("Workspace configuration changed before dispatch")
                old["reporter"].has_pending_work = lambda: True
            return "existing-native"

        sessions.ensure_session = AsyncMock(side_effect=prepare)
        gateway.sessions = sessions
        gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
        await consumer.handle(delivery)
        assert preparations == 2 and sessions.native_epoch("discussion") == 7
        assert writer.messages == []
        assert store.get_message(delivery["id"], "beta")["state"] == "queued"
        assert consumer.ownership[(parent["id"], parent["claim"])] is old
        evidence = store.get_message(parent["id"], "beta")["evidence"]
        assert evidence["execution_id"] == old["execution_id"]
        assert evidence["native_epoch"] == sessions.native_epoch("discussion")

        cancel = broker.cancel

        async def confirmed_cancel(*args, **kwargs):
            await cancel(*args, **kwargs)
            old["reporter"].has_pending_work = lambda: False
            gateway._last_terminal["discussion"] = terminal(stop_reason="cancelled")

        broker.cancel = AsyncMock(side_effect=confirmed_cancel)
        store.cancel(p["id"], "alpha")
        control = store.claim_next("beta", consumer.instance)
        await consumer.handle(control)
        broker.cancel.assert_awaited_once()
        assert writer.messages[0]["method"] == "session/cancel"
        assert store.get_message(parent["id"], "beta")["state"] == "cancelled"
        assert store.get_message(control["id"], "beta")["state"] == "done"
        assert not consumer.owns_thread("discussion")


@pytest.mark.parametrize("settle_before", ["reservation", "rollback"])
async def test_terminal_parent_rollback_retains_restored_background_ownership(
    tmp_path, settle_before
):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        delivery = child_result(store, p, parent, consumer)
        info = await consumer.continuation(delivery)
        new = report(p["id"], "rejected-continuation")
        gateway._reports["existing-native"] = new
        if settle_before == "reservation":
            store.reply(parent["id"], "beta", "Explicit result", claim=parent["claim"])
        accepted = await consumer.reserve_continuation(delivery, "existing-native", info)
        assert accepted is (settle_before == "rollback")
        if settle_before == "rollback":
            store.reply(parent["id"], "beta", "Explicit result", claim=parent["claim"])
        old["reporter"].has_pending_work = lambda: True
        gateway._reports["existing-native"] = old
        gateway._last_terminal["discussion"]["pending"] = True
        await consumer.rollback_continuation(info)
        consumer.reserving.difference_update(
            (state["id"], state.get("claim")) for state in info["parents"]
        )
        key = (parent["id"], parent["claim"])
        assert consumer.ownership[key] is old
        evidence = store.get_message(parent["id"], "beta")["evidence"]
        assert evidence["execution_id"] == old["execution_id"]
        await consumer.reconcile_ownership()
        assert consumer.ownership[key] is old
        old["reporter"].has_pending_work = lambda: False
        gateway._last_terminal["discussion"] = terminal()
        await consumer.reconcile_ownership()
        assert key not in consumer.ownership
        assert store.get_message(parent["id"], "beta")["result"] == "Explicit result"


async def test_warmed_native_without_goal_queue_evidence_refuses_maintenance_seal():
    gateway = maintenance_gateway()
    broker, _writer = memory_broker()
    gateway.sessions.broker = broker
    gateway._reports["existing-native"] = report("project")
    client = await maintenance_client(gateway)
    try:
        response = await client.post("/maintenance", json={"action": "drain"}, headers=auth())
        state = await response.json()
        assert state["native_boundary_unknown"]
        assert state["background"] == 1 and not state["idle"]
        assert all(
            state[key] == 0
            for key in [
                "foreground",
                "responses",
                "inbound",
                "commands",
                "native_rpc",
                "collaboration",
            ]
        )
        response = await client.post("/maintenance", json={"action": "seal"}, headers=auth())
        assert response.status == 409 and not gateway.sealed
        assert broker.is_running
    finally:
        await client.close()


async def test_delayed_restart_counts_until_native_stop_and_exit_complete(tmp_path, monkeypatch):
    gateway = maintenance_gateway()
    gateway.config.db_path = tmp_path / "project.db"
    entered, release = asyncio.Event(), asyncio.Event()
    exits = []

    async def stop():
        entered.set()
        await release.wait()

    gateway.sessions.broker.stop = AsyncMock(side_effect=stop)
    monkeypatch.setattr("mattermost_acp.commands.restart.os._exit", exits.append)
    client = await maintenance_client(gateway)
    tasks = []
    try:
        await gateway._request_restart(
            thread_id="discussion", channel_id="floor", user_id="human", reason="test"
        )
        tasks = list(gateway._slash_tasks)
        assert len(tasks) == 1
        assert gateway.maintenance_state()["commands"] == 1
        await asyncio.wait_for(entered.wait(), 2)
        await client.post("/maintenance", json={"action": "drain"}, headers=auth())
        response = await client.post("/maintenance", json={"action": "seal"}, headers=auth())
        assert response.status == 409
        assert not gateway.maintenance_state()["idle"] and exits == []
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 2)
        assert exits == [0] and gateway.maintenance_state()["commands"] == 0
        response = await client.post("/maintenance", json={"action": "seal"}, headers=auth())
        assert response.status == 200 and gateway.sealed
    finally:
        release.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.close()


async def test_saved_stop_cutoff_survives_reconstruction_and_preserves_new_project(
    tmp_path, monkeypatch
):
    store, server, clients = await api_fixture(tmp_path)
    now = [1000.0]
    store.clock = lambda: now[0]
    monkeypatch.setattr("mattermost_acp.collaboration.consumer.time.time", lambda: now[0])
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    restarted = None
    try:
        old = project(store)
        now[0] = 1001.0
        # A saved pre-upgrade timestamp remains readable and retryable.
        consumer.pending_stops["origin"] = 1001.0
        consumer.save_stops()
        consumer.hub.request = AsyncMock(side_effect=TimeoutError("Hub unavailable"))
        with pytest.raises(TimeoutError):
            await consumer.cancel_thread("origin", retry=True)
        assert json.loads(consumer.stops_path.read_text()) == {"origin": 1001.0}
        await consumer.close()
        now[0] = 1002.0
        newer = project(store)
        restarted = CollaborationConsumer(gateway, clients["alpha"].settings)
        restarted.hub.request = AsyncMock(wraps=restarted.hub.request)
        assert restarted.pending_stops == {"origin": 1001.0}
        await restarted.cancel_thread("origin", retry=True)
        restarted.hub.request.assert_awaited_once_with(
            "POST", "/cancel-thread", {"thread_id": "origin", "cutoff": 1001.0}
        )
        assert store.show(old["id"], "alpha")["status"] == "cancelled"
        assert store.show(newer["id"], "alpha")["status"] == "active"
        assert restarted.pending_stops == {} and json.loads(restarted.stops_path.read_text()) == {}
    finally:
        await consumer.close()
        if restarted:
            await restarted.close()
        await close_api(store, server, clients)


async def test_older_stop_retry_cannot_erase_newer_pending_stop(tmp_path, monkeypatch):
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, Settings("http://127.0.0.1:18781", "x" * 32))
    consumer.pending_stops["origin"] = 1000.0
    consumer.save_stops()
    monkeypatch.setattr("mattermost_acp.collaboration.consumer.time.time", lambda: 2000.0)
    entered = {1000.0: asyncio.Event(), 2000.0: asyncio.Event()}
    release = {1000.0: asyncio.Event(), 2000.0: asyncio.Event()}

    async def request(_method, path, body):
        if path == "/stop-boundaries":
            return {"key": body["key"], "thread_id": "origin", "admission_order": 2000}
        cutoff = body["cutoff"] if "cutoff" in body else body["boundary"]["admission_order"]
        entered[cutoff].set()
        await release[cutoff].wait()
        return {"cancelled": []}

    consumer.hub.request = AsyncMock(side_effect=request)
    tasks = []
    try:
        older = asyncio.create_task(consumer.cancel_thread("origin", retry=True))
        tasks.append(older)
        await asyncio.wait_for(entered[1000.0].wait(), 1)
        newer = asyncio.create_task(consumer.cancel_thread("origin"))
        tasks.append(newer)
        await asyncio.wait_for(entered[2000.0].wait(), 1)
        release[1000.0].set()
        await asyncio.wait_for(older, 1)
        assert not newer.done()
        assert consumer.pending_stops["origin"]["phase"] == "captured"
        assert consumer.pending_stops["origin"]["boundary"]["admission_order"] == 2000
        assert json.loads(consumer.stops_path.read_text()) == consumer.pending_stops
        release[2000.0].set()
        await asyncio.wait_for(newer, 1)
        assert consumer.pending_stops == {} and json.loads(consumer.stops_path.read_text()) == {}
    finally:
        for event in release.values():
            event.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.close()
