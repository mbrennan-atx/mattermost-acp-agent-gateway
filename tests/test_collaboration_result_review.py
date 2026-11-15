"""Result delivery and reconciled ownership use synthetic local native sessions."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_continuation_review import child_result, retained_parent
from test_collaboration_dispatch_review import close_reports, native_gateway
from test_collaboration_regressions import (
    maintenance_client,
    maintenance_gateway,
    memory_broker,
    sidecar_project,
)

from mattermost_acp.acp import SessionUpdate, TurnResult
from mattermost_acp.collaboration.client import HubError
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.session_service import SessionService


@asynccontextmanager
async def fresh_result(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    broker.capabilities.steering = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    try:
        p = project(store)
        peer = ask(store, p["id"], sender="beta", recipient="gamma")
        claimed = store.claim_next("gamma", "peer-worker")
        store.reply(peer["id"], "gamma", "Peer verified the dependency", claim=claimed["claim"])
        delivery = store.claim_next("beta", consumer.instance)
        assert delivery["kind"] == "result" and not delivery.get("continuation_parent")
        yield store, gateway, consumer, p, delivery
    finally:
        await consumer.close()
        await close_reports(gateway)
        await close_api(store, server, clients)


async def test_active_participant_result_turn_receives_native_cancellation(tmp_path):
    async with fresh_result(tmp_path) as (store, gateway, consumer, p, delivery):
        entered, stopped = asyncio.Event(), asyncio.Event()
        handling = cancellation = None

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            assert options["dispatch_guard"]()
            entered.set()
            await stopped.wait()
            return TurnResult("existing-native", "cancelled", "Native result work stopped", "")

        async def cancel(_thread, **_options):
            stopped.set()

        gateway.sessions.run_turn = native
        gateway.sessions.cancel = AsyncMock(side_effect=cancel)
        try:
            handling = asyncio.create_task(consumer.handle(delivery))
            await asyncio.wait_for(entered.wait(), 2)
            store.cancel(p["id"], "alpha")
            assert store.get_message(delivery["id"], "beta")["state"] == "cancelling"
            assert store.show(p["id"], "alpha")["status"] == "cancelling"
            control = store.claim_next("beta", consumer.instance)
            assert control["kind"] == "cancel" and control["parent"] == delivery["id"]
            cancellation = asyncio.create_task(consumer.handle(control))
            await asyncio.wait_for(asyncio.gather(handling, cancellation), 3)
            gateway.sessions.cancel.assert_awaited_once()
            call = gateway.sessions.cancel.call_args
            assert call.args == ("discussion",)
            assert call.kwargs["expected_session"] == "existing-native"
            assert store.get_message(delivery["id"], "beta")["state"] == "cancelled"
            assert store.get_message(control["id"], "beta")["state"] == "done"
            assert store.show(p["id"], "alpha")["status"] == "cancelled"
            assert not consumer.owns_thread("discussion")
        finally:
            stopped.set()
            tasks = [task for task in [handling, cancellation] if task]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_legacy_owner_home_result_keeps_native_cancellation_exemption(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        peer = ask(store, p["id"], recipient="gamma")
        store.claim_next("gamma", "peer-worker")
        store.reply(peer["id"], "gamma", "Owner peer result")
        delivery = store.claim_next("alpha", "owner-worker")
        assert delivery["binding"] == {"channel_id": "home", "thread_id": "origin"}
        store.update(
            delivery["id"],
            "alpha",
            "owner-worker",
            "dispatch",
            evidence={"session_id": "owner-native", "execution_id": "owner-execution"},
        )
        store.cancel(p["id"], "alpha")
        assert store.get_message(delivery["id"], "alpha")["state"] == "cancelled"
        assert not [m for m in store.show(p["id"], "alpha")["messages"] if m["kind"] == "cancel"]
        assert store.claim_next("alpha", "owner-worker", controls_only=True) is None
    finally:
        await close_api(store, server, clients)


@pytest.mark.parametrize("reset", ["requeue", "retry"])
async def test_result_provenance_never_becomes_recipient_dispatch_evidence(tmp_path, reset):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        peer = ask(store, p["id"], sender="beta", recipient="gamma")
        original = store.claim_next("gamma", "peer-worker")
        peer_evidence = {
            "session_id": "peer-native",
            "execution_id": "peer-execution",
            "dispatch_started": store.clock(),
            "stop_reason": "end_turn",
            "pending": False,
        }
        store.reply(peer["id"], "gamma", "Peer result", evidence=peer_evidence)
        queued = next(m for m in store.show(p["id"], "beta")["messages"] if m["kind"] == "result")
        assert queued["evidence"] == {} and queued["result_evidence"] == peer_evidence
        store.update(
            peer["id"], "gamma", "peer-worker", "evidence", evidence={"model": "peer-model"}
        )
        assert store.get_message(queued["id"], "beta")["evidence"] == {}
        delivery = store.claim_next("beta", "recipient-worker")
        recipient = store.update(
            delivery["id"],
            "beta",
            "recipient-worker",
            "dispatch",
            evidence={"session_id": "recipient-native", "execution_id": "recipient-execution"},
        )
        recipient_evidence = dict(recipient["evidence"])
        store.update(
            peer["id"],
            "gamma",
            "peer-worker",
            "evidence",
            evidence={"artifacts": ["https://example.com/peer-proof"]},
            claim=original["claim"],
        )
        enriched = store.get_message(delivery["id"], "beta")
        assert enriched["evidence"] == recipient_evidence
        provenance = {
            **peer_evidence,
            "model": "peer-model",
            "artifacts": ["https://example.com/peer-proof"],
        }
        assert enriched["result_evidence"] == provenance
        if reset == "retry":
            store.update(delivery["id"], "beta", "recipient-worker", "unconfirmed", "Receipt lost")
            reset_delivery = store.retry(delivery["id"], "beta", "Recipient execution reconciled")
        else:
            reset_delivery = store.update(delivery["id"], "beta", "recipient-worker", "queued")
        assert reset_delivery["evidence"] == {}
        assert reset_delivery["result_evidence"] == provenance
        assert reset_delivery["delivery_history"][-1] == recipient_evidence
    finally:
        await close_api(store, server, clients)


async def test_lost_result_mailbox_can_drain_without_treating_peer_as_local_native_work(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = maintenance_gateway()
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    consumer.actor = "beta"
    gateway.collaboration = consumer
    headers = {"Authorization": "Bearer " + consumer.hub.settings.token}
    actual_request = consumer.hub.request
    lost = asyncio.Event()

    async def lost_response(method, path, body=None):
        result = await actual_request(method, path, body)
        if path.startswith("/mailbox?") and result and not lost.is_set():
            assert result["kind"] == "result"
            lost.set()
            raise HubError("Result claim committed but response lost")
        return result

    consumer.hub.request = lost_response
    client = await maintenance_client(gateway)
    try:
        p = project(store)
        peer = ask(store, p["id"], sender="beta", recipient="gamma")
        store.claim_next("gamma", "peer-worker")
        peer_evidence = {"session_id": "peer-native", "execution_id": "peer-execution"}
        store.reply(peer["id"], "gamma", "Peer result", evidence=peer_evidence)
        consumer.start()
        await asyncio.wait_for(lost.wait(), 2)
        await asyncio.wait_for(consumer.claim_settled.wait(), 2)
        assert consumer.claim_uncertain and not gateway.maintenance_state()["idle"]
        drained = await client.post("/maintenance", json={"action": "drain"}, headers=headers)
        assert drained.status == 200 and gateway.maintenance_state()["idle"]
        result = next(m for m in store.show(p["id"], "beta")["messages"] if m["kind"] == "result")
        assert result["state"] == "queued" and result["evidence"] == {}
        assert result["result_evidence"] == peer_evidence
        sealed = await client.post("/maintenance", json={"action": "seal"}, headers=headers)
        assert sealed.status == 200 and gateway.sealed
        gateway.run_prompt.assert_not_awaited()
        gateway.sessions.cancel.assert_not_awaited()
    finally:
        await consumer.close()
        await client.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("cancel_during", ["origin_read", "post_write"])
async def test_owner_result_notification_cancellation_never_targets_peer_native(
    tmp_path, cancel_during
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(steering=False, busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.collab_sidecar = True
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    gateway.collaboration = consumer
    entered, release = asyncio.Event(), asyncio.Event()
    handling = None
    published = {}

    async def read(post_id):
        if post_id == "origin":
            if cancel_during == "origin_read":
                entered.set()
                await release.wait()
            return {"id": "origin", "channel_id": "home", "root_id": ""}
        return published

    async def publish(channel, message, *, root_id):
        published.update(id="notification", channel_id=channel, root_id=root_id, message=message)
        if cancel_during == "post_write":
            entered.set()
            await release.wait()
        return published

    gateway.client.get_post = AsyncMock(side_effect=read)
    gateway.client.create_post = AsyncMock(side_effect=publish)
    try:
        p = sidecar_project(store)
        peer = ask(store, p["id"], recipient="beta")
        store.claim_next("beta", "peer-worker")
        provenance = {"session_id": "peer-native", "execution_id": "peer-execution"}
        store.reply(peer["id"], "beta", "Peer result", evidence=provenance)
        delivery = store.claim_next("alpha", consumer.instance)
        handling = asyncio.create_task(consumer.handle(delivery))
        await asyncio.wait_for(entered.wait(), 2)
        store.cancel(p["id"], "alpha")
        assert store.get_message(delivery["id"], "alpha")["state"] == "cancelling"
        if cancel_during == "post_write":
            control = store.claim_next("alpha", consumer.instance)
            await asyncio.wait_for(consumer.handle(control), 2)
            assert store.get_message(control["id"], "alpha")["state"] == "done"
        release.set()
        await asyncio.wait_for(handling, 3)
        if cancel_during == "origin_read":
            gateway.client.create_post.assert_not_awaited()
            control = store.claim_next("alpha", consumer.instance)
            await asyncio.wait_for(consumer.handle(control), 2)
        settled = store.get_message(delivery["id"], "alpha")
        assert settled["state"] == "cancelled" and not settled["evidence"].get("session_id")
        assert settled["result_evidence"] == provenance
        assert store.show(p["id"], "alpha")["status"] == "cancelled"
        gateway.run_prompt.assert_not_awaited()
        gateway.sessions.cancel.assert_not_awaited()
    finally:
        release.set()
        if handling and not handling.done():
            handling.cancel()
            await asyncio.gather(handling, return_exceptions=True)
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("background", ["tool", "worker"])
async def test_result_end_turn_settles_after_background_finishes_without_reply(
    tmp_path, background
):
    async with fresh_result(tmp_path) as (store, gateway, consumer, _p, delivery):
        handling = None

        def update(completed=False):
            if background == "worker":
                return SessionUpdate(
                    "existing-native",
                    "subagent_activity",
                    raw={"worker_id": "child", "state": "completed" if completed else "running"},
                )
            return SessionUpdate(
                "existing-native",
                "tool_call_update" if completed else "tool_call",
                tool_call_id="background-tool",
                tool_status="completed" if completed else "in_progress",
            )

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            await options["on_update"](update())
            return TurnResult("existing-native", "end_turn", "Peer result incorporated", "")

        gateway.sessions.run_turn = native
        try:
            handling = asyncio.create_task(consumer.handle(delivery))
            async with asyncio.timeout(2):
                while not store.get_message(delivery["id"], "beta")["evidence"].get("pending"):
                    await asyncio.sleep(0)
            assert not handling.done()
            assert consumer.owns_thread("discussion")
            assert gateway._last_terminal["discussion"]["pending"]
            gateway._observe_background_update(update(completed=True))
            await asyncio.wait_for(handling, 3)
            settled = store.get_message(delivery["id"], "beta")
            assert settled["state"] == "done" and settled["result"] == "Peer result incorporated"
            assert settled["evidence"]["foreground_answer"] == "Peer result incorporated"
            assert settled["evidence"]["stop_reason"] == "end_turn"
            assert not settled["evidence"]["pending"]
            assert not consumer.owns_thread("discussion")
            assert not [
                m
                for m in store.show(delivery["project"], "beta")["messages"]
                if (m["kind"] == "result" and m.get("parent") == delivery["id"])
            ]
        finally:
            if handling and not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)


async def test_lost_parent_dispatch_response_restores_ownership_before_native_write(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        before = dict(gateway._last_terminal["discussion"])
        old_evidence = dict(store.get_message(parent["id"], "beta")["evidence"])
        broker, writer = memory_broker()
        broker.capabilities.steering = False
        sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        sessions.ensure_session = AsyncMock(return_value="existing-native")
        sessions.status = AsyncMock(return_value={"session_id": "existing-native"})
        gateway.sessions = sessions
        gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
        delivery = child_result(store, p, parent, consumer)
        actual_update = consumer.update
        discarded = []

        async def lost_response(item, state, *args, **options):
            receipt = await actual_update(item, state, *args, **options)
            if item["id"] == parent["id"] and state == "dispatch" and not discarded:
                discarded.append(consumer.ownership[(parent["id"], parent["claim"])])
                assert receipt["evidence"]["execution_id"] == discarded[0]["execution_id"]
                raise HubError("Parent dispatch committed but response lost")
            return receipt

        consumer.update = lost_response
        await asyncio.wait_for(consumer.handle(delivery), 3)
        assert writer.messages == []
        assert discarded and discarded[0] is not old
        assert discarded[0]["task"].done()
        assert consumer.ownership[(parent["id"], parent["claim"])] is old
        restored = store.get_message(parent["id"], "beta")["evidence"]
        assert restored["session_id"] == old_evidence["session_id"]
        assert restored["execution_id"] == old_evidence["execution_id"]
        assert gateway._last_terminal["discussion"] == before
        assert gateway._reports["existing-native"] is old
        assert not consumer.reserving
        clock = store.clock
        store.clock = lambda: clock() + 3
        retried = store.claim_next("beta", consumer.instance)
        assert retried["id"] == delivery["id"]
        assert (await consumer.continuation(retried))["report"] is old

        def prompt_reply(payload):
            message = json.loads(payload)
            writer.messages.append(message)
            broker._pending[message["id"]].set_result({"stopReason": "end_turn"})

        writer.write = prompt_reply
        await asyncio.wait_for(consumer.handle(retried), 3)
        assert len(writer.messages) == 1 and writer.messages[0]["method"] == "session/prompt"
        assert store.get_message(retried["id"], "beta")["state"] == "done"
        assert store.get_message(parent["id"], "beta")["state"] == "running"


@pytest.mark.parametrize(
    "unsettled",
    [
        None,
        "tool",
        "receipt_pending",
        "foreground",
        "active",
        "busy",
        "session",
        "execution",
        "undispatched",
    ],
)
async def test_cancelled_reconciled_retry_uses_history_only_for_settled_native_owner(
    tmp_path, unsettled
):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        before = dict(store.get_message(parent["id"], "beta")["evidence"])
        store.update(parent["id"], "beta", consumer.instance, "unconfirmed", "Native receipt lost")
        queued = store.retry(parent["id"], "beta", "Old native work reconciled before retry")
        assert queued["evidence"] == {} and queued["delivery_history"][-1] == before
        gateway.draining = True
        store.cancel(p["id"], "alpha")
        cancelled = store.get_message(parent["id"], "beta")
        assert cancelled["state"] == "cancelled" and cancelled["claim"] == parent["claim"]
        receipt = gateway._last_terminal["discussion"]
        if unsettled == "tool":
            old["reporter"].has_pending_work = lambda: True
        elif unsettled == "receipt_pending":
            receipt["pending"] = True
        elif unsettled == "foreground":
            old["foreground_complete"] = False
        elif unsettled == "active":
            old["active"] = True
        elif unsettled == "busy":
            gateway.sessions.is_busy = lambda _thread: True
        elif unsettled == "session":
            receipt["session_id"] = "other-native"
        elif unsettled == "execution":
            receipt["execution_id"] = "other-execution"
        elif unsettled == "undispatched":
            receipt["native_dispatched"] = False
        await consumer.reconcile_ownership()
        if unsettled:
            assert consumer.ownership[(parent["id"], parent["claim"])] is old
            assert consumer.owns_thread("discussion")
        else:
            assert not consumer.owns_thread("discussion")
            assert not consumer.ownership and "discussion" not in gateway._last_terminal
            final = store.get_message(parent["id"], "beta")
            assert final["state"] == "cancelled"
            assert final["evidence"] == {} and final["delivery_history"][-1] == before
        gateway.sessions.cancel.assert_not_awaited()
        gateway.run_prompt.assert_not_awaited()
