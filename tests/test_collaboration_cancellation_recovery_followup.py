"""Stop uncertainty, lease supervision and clock-safe native settlement."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_continuation_review import retained_parent, terminal

from mattermost_acp.acp.broker import NativeDispatchChanged, check_native_command_admission
from mattermost_acp.collaboration.client import HubError
from mattermost_acp.collaboration.consumer import CollaborationConsumer


async def test_uncaptured_stop_preserves_unrelated_mailbox_and_ownership_progress(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, _gateway, consumer, _p, parent, _old):
        store.reply(parent["id"], "beta", "Finished original work", claim=parent["claim"])
        unrelated = project(store)
        unrelated["origin_thread_id"] = "another-origin"
        with store.transaction():
            store.save_project(unrelated)
        request = ask(store, unrelated["id"])
        real_request = consumer.hub.request
        calls = []

        async def transport(method, path, body=None):
            calls.append((method, path))
            if path == "/stop-boundaries":
                raise OSError("Stop never reached the hub")
            return await real_request(method, path, body)

        consumer.hub.request = transport
        with pytest.raises(OSError):
            await consumer.cancel_thread("origin")
        stop = dict(consumer.pending_stops["origin"])
        assert stop["phase"] == "uncertain"
        handled = asyncio.Event()

        async def handle(item):
            assert item["id"] == request["id"]
            handled.set()

        consumer.handle = handle
        consumer.start()
        await asyncio.wait_for(handled.wait(), 1)
        assert (parent["id"], parent["claim"]) not in consumer.ownership
        assert consumer.pending_stops == {"origin": stop}
        assert json.loads(consumer.stops_path.read_text()) == {"origin": stop}
        assert calls.count(("POST", "/stop-boundaries")) == 1
        assert ("GET", "/stop-boundaries/" + stop["key"]) in calls


@pytest.mark.parametrize("at_reservation", [False, True])
async def test_unresolved_stop_fences_related_native_admission(tmp_path, at_reservation):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        request = ask(store, p["id"])
        item = store.claim_next("beta", consumer.instance)
        consumer.pending_stops["origin"] = {"key": "a" * 32, "phase": "uncertain"}
        if at_reservation:
            assert not await consumer.reserve(item, "existing-native")
        else:
            await consumer.handle(item)
        assert store.get_message(request["id"], "alpha")["state"] == "queued"
        gateway.sessions.hint.assert_not_awaited()
        gateway.run_prompt.assert_not_awaited()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_unresolved_stop_preserves_unrelated_ordinary_native_admission(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    consumer.recovery_pending = False
    gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
    gateway._run_prompt = AsyncMock()
    consumer.pending_stops["origin"] = {"key": "a" * 32, "phase": "uncertain"}
    try:
        await gateway.run_prompt(thread_id="origin", channel_id="home", prompt="Follow up")
        gateway._run_prompt.assert_not_awaited()
        await gateway.run_prompt(
            thread_id="another-thread", channel_id="home", prompt="Independent work"
        )
        gateway._run_prompt.assert_awaited_once()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_stop_arriving_after_admission_fences_native_write(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    native_writes = []
    item = {"binding": {"thread_id": "discussion"}, "origin_thread_id": "origin"}

    async def operation():
        consumer.pending_stops["origin"] = {"key": "a" * 32, "phase": "uncertain"}
        check_native_command_admission()
        native_writes.append("session/prompt")

    try:
        with pytest.raises(NativeDispatchChanged):
            await consumer.deadline_work(item, operation())
        assert native_writes == []
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("status", [500, 503, 429, None])
async def test_transient_heartbeat_failure_retries_before_lease_expiry(
    tmp_path, monkeypatch, status
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway()
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    now = [store.clock()]
    store.clock = lambda: now[0]
    store.elapsed = lambda: now[0]
    real_sleep = asyncio.sleep
    renewed = asyncio.Event()
    real_update = consumer.update
    attempts = []

    async def sleep(delay):
        if delay in {10, 2}:
            if renewed.is_set():
                await asyncio.Event().wait()
            now[0] += delay
            await real_sleep(0)
        else:
            await real_sleep(delay)

    async def update(item, state):
        attempts.append(state)
        if len(attempts) <= 3:
            raise HubError("Transient lease failure", status=status)
        receipt = await real_update(item, state)
        renewed.set()
        return receipt

    monkeypatch.setattr(asyncio, "sleep", sleep)
    consumer.update = update
    renewal = None
    try:
        p = project(store)
        request = ask(store, p["id"])
        item = store.claim_next("beta", consumer.instance)
        renewal = asyncio.create_task(consumer.heartbeat(item))
        await asyncio.wait_for(renewed.wait(), 1)
        assert attempts == ["heartbeat"] * 4
        now[0] += 20
        store.expire()
        stored = store.get_message(request["id"], "alpha")
        assert stored["state"] == "running"
        assert stored["claim"] == item["claim"] and stored["instance"] == consumer.instance
        assert stored["lease_until"] > now[0]
        assert not renewal.done()
    finally:
        if renewal:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("status", [401, 403, 404, 409])
async def test_heartbeat_stops_on_definitive_rejection(tmp_path, monkeypatch, status):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway()
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    real_sleep = asyncio.sleep

    async def sleep(_delay):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", sleep)
    consumer.update = AsyncMock(side_effect=HubError("Ownership rejected", status=status))
    try:
        await asyncio.wait_for(consumer.heartbeat({"id": "synthetic-request"}), 1)
        consumer.update.assert_awaited_once()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("path", ["reconcile", "observer", "cancel", "cancel_after_reply"])
async def test_linked_backward_clock_terminal_receipt_settles_native_debt(tmp_path, path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        reserved = store.get_message(parent["id"], "beta")["evidence"]
        gateway._last_terminal["discussion"] = terminal(
            completed_at=reserved["dispatch_started"] - 3600,
            stop_reason="cancelled" if path.startswith("cancel") else "end_turn",
        )
        if path.startswith("cancel"):
            if path == "cancel_after_reply":
                store.reply(parent["id"], "beta", "Explicit early result", claim=parent["claim"])
            store.cancel(p["id"], "alpha")
            control = store.claim_next("beta", consumer.instance, controls_only=True)
            assert control["kind"] == "cancel"
            await asyncio.wait_for(consumer.handle_cancel(control), 1)
            assert store.get_message(control["id"], "beta")["state"] == "done"
            gateway.sessions.cancel.assert_not_awaited()
            await consumer.reconcile_ownership()
        else:
            store.reply(parent["id"], "beta", "Explicit correlated result", claim=parent["claim"])
            if path == "observer":
                await asyncio.wait_for(consumer.injected_wait(parent), 2)
            await consumer.reconcile_ownership()
        stored = store.get_message(parent["id"], "beta")
        assert not stored["execution_pending"]
        assert stored["evidence"]["completed_at"] < reserved["dispatch_started"]
        assert stored["evidence"]["reservation_id"] == reserved["reservation_id"]
        assert stored["evidence"]["native_epoch"] == reserved["native_epoch"]
        assert (parent["id"], parent["claim"]) not in consumer.ownership
        assert old["foreground_complete"]


@pytest.mark.parametrize(
    "mismatch", ["reservation_id", "native_epoch", "execution_id", "session_id"]
)
async def test_backward_clock_settlement_requires_matching_reservation(tmp_path, mismatch):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, _p, parent, _old):
        store.reply(parent["id"], "beta", "Explicit correlated result", claim=parent["claim"])
        reserved = store.get_message(parent["id"], "beta")["evidence"]
        receipt = terminal(completed_at=reserved["dispatch_started"] - 3600)
        proof = consumer.foreground(parent, receipt)
        proof[mismatch] = 8 if mismatch == "native_epoch" else "foreign-reservation"
        with pytest.raises(ValueError, match="settlement"):
            store.update(
                parent["id"],
                "beta",
                consumer.instance,
                "execution_settled",
                evidence=proof,
                claim=parent["claim"],
            )
        assert store.get_message(parent["id"], "beta")["execution_pending"]
        gateway._last_terminal["discussion"] = {**receipt, mismatch: proof[mismatch]}
        await consumer.reconcile_ownership()
        assert consumer.owns_thread("discussion")


async def test_same_execution_terminal_before_new_reservation_remains_fenced(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, _p, parent, _old):
        previous = gateway._last_terminal["discussion"]
        assert await consumer.reserve(parent, "existing-native")
        store.reply(parent["id"], "beta", "Explicit late result", claim=parent["claim"])
        await consumer.reconcile_ownership()
        assert consumer.owns_thread("discussion")
        assert store.get_message(parent["id"], "beta")["execution_pending"]
        # A later receipt can have an earlier timestamp after a clock adjustment.
        previous["completed_at"] = (
            store.get_message(parent["id"], "beta")["evidence"]["dispatch_started"] - 7200
        )
        await consumer.reconcile_ownership()
        assert not consumer.owns_thread("discussion")
        assert not store.get_message(parent["id"], "beta")["execution_pending"]
