"""Native dispatch tracking, rebound ownership and acyclic delegation contracts."""

import asyncio
from types import SimpleNamespace
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
from test_collaboration_continuation_review import terminal
from test_collaboration_dispatch_review import close_reports, native_gateway

from mattermost_acp.acp import TurnResult
from mattermost_acp.collaboration.client import HubError
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.store import CollaborationStore


@pytest.mark.parametrize("early_reply", [False, True])
async def test_rebound_owner_native_cancellation_preserves_original_conversation(
    tmp_path, early_reply
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    gateway.collaboration = consumer
    try:
        p = project(store)
        store.bind(p["id"], "alpha", {"thread_id": "discussion", "channel_id": "floor"})
        requested = ask(store, p["id"], sender="beta", recipient="alpha")
        claimed = store.claim_next("alpha", consumer.instance)
        owned = execution_report(p["id"])
        owned.update(active=True, foreground_complete=False)
        original = execution_report(None, "unrelated-original-execution")
        original.update(thread_id="origin", channel_id="home", active=True)
        gateway._reports.update({"existing-native": owned, "original-native": original})
        origin_terminal = terminal("unrelated-original-execution", session_id="original-native")
        gateway._last_terminal["origin"] = origin_terminal
        assert await consumer.reserve(claimed, "existing-native")
        if early_reply:
            store.reply(requested["id"], "alpha", "Verified early result", claim=claimed["claim"])
        store.cancel(p["id"], "alpha")
        current = store.get_message(requested["id"], "alpha")
        assert current["state"] == ("done" if early_reply else "cancelling")
        assert current["execution_pending"]
        assert bool(current.get("execution_cancelling")) is early_reply
        assert store.show(p["id"], "alpha")["status"] == "cancelling"
        control = store.claim_next("alpha", consumer.instance, controls_only=True)
        assert control["kind"] == "cancel" and control["parent"] == requested["id"]

        async def cancel(thread, **options):
            assert thread == "discussion" and options["expected_session"] == "existing-native"
            assert options["guard"]()
            owned.update(active=False, foreground_complete=True)
            gateway._last_terminal[thread] = terminal(
                "tracked-execution", stop_reason="cancelled", answer="Native work stopped"
            )

        gateway.sessions.cancel = AsyncMock(side_effect=cancel)
        await asyncio.wait_for(consumer.handle(control), 3)
        gateway.sessions.cancel.assert_awaited_once()
        settled = store.get_message(requested["id"], "alpha")
        assert settled["state"] == ("done" if early_reply else "cancelled")
        assert not settled["execution_pending"] and not settled.get("execution_cancelling")
        if early_reply:
            assert settled["result"] == "Verified early result"
        assert store.show(p["id"], "alpha")["status"] == "cancelled"
        assert gateway._reports["original-native"] is original and original["active"]
        assert gateway._last_terminal["origin"] is origin_terminal
        assert not consumer.owns_thread("discussion")
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("binding", [None, {"thread_id": "origin", "channel_id": "other"}])
def test_owner_exemption_requires_recorded_original_channel_and_thread(tmp_path, binding):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store)
        requested = ask(store, p["id"], sender="beta", recipient="alpha")
        claimed = store.claim_next("alpha", "owner")
        store.update(
            requested["id"],
            "alpha",
            "owner",
            "dispatch",
            evidence={"session_id": "native", "execution_id": "execution"},
            claim=claimed["claim"],
        )
        with store.transaction():
            current = store.get_message(requested["id"], "alpha")
            if binding is None:
                current.pop("binding")
            else:
                current["binding"] = binding
            store.save_message(current)
        store.cancel(p["id"], "alpha")
        assert store.get_message(requested["id"], "alpha")["state"] == "cancelling"
        assert store.claim_next("alpha", "owner", controls_only=True)["parent"] == requested["id"]
    finally:
        store.close()


@pytest.mark.parametrize("missing_id", ["report", None, ""])
async def test_busy_untracked_session_never_steers_or_reserves_null_ownership(tmp_path, missing_id):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=True)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        requested = ask(store, p["id"])
        if missing_id != "report":
            gateway._reports["existing-native"] = execution_report(p["id"], missing_id)
        claimed = store.claim_next("beta", consumer.instance)
        await consumer.handle(claimed)
        gateway.sessions.hint.assert_not_awaited()
        gateway.run_prompt.assert_not_awaited()
        receipt = store.get_message(requested["id"], "beta")
        assert receipt["state"] == "queued"
        assert not receipt.get("execution_pending") and not receipt["evidence"]
        assert not consumer.ownership and not consumer.ownership_threads
        assert "reservation_evidence" not in claimed
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_idle_untracked_session_prepares_real_execution_and_settles(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway, _broker, _writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native", harness="claude")
    )
    gateway.sessions.store.harness = "claude"
    gateway.sessions.hint = AsyncMock(wraps=gateway.sessions.hint)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    try:
        p = project(store)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        assert gateway._reports == {}

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            assert options["dispatch_guard"]()
            receipt = store.get_message(requested["id"], "beta")
            owned = gateway._reports["existing-native"]
            assert receipt["evidence"]["execution_id"] == owned["execution_id"]
            assert receipt["evidence"]["execution_id"]
            assert consumer.ownership[(requested["id"], claimed["claim"])] is owned
            store.reply(requested["id"], "beta", "Verified real execution", claim=claimed["claim"])
            return TurnResult("existing-native", "end_turn", "Native result", "")

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        await consumer.handle(claimed)
        gateway.sessions.hint.assert_not_awaited()
        gateway.sessions.ensure_session.assert_awaited_once()
        gateway.sessions.run_turn.assert_awaited_once()
        settled = store.get_message(requested["id"], "beta")
        assert settled["state"] == "done" and not settled["execution_pending"]
        assert settled["evidence"]["execution_id"] and not consumer.owns_thread("discussion")
    finally:
        await close_reports(gateway)
        await consumer.close()
        await close_api(store, server, clients)


async def test_execution_report_disappearing_before_reservation_leaves_no_dispatch(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=True)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    original_reserve = consumer.reserve
    try:
        p = project(store)
        requested = ask(store, p["id"])
        gateway._reports["existing-native"] = execution_report(p["id"])

        async def lost_report(item, session):
            gateway._reports.pop(session)
            return await original_reserve(item, session)

        consumer.reserve = lost_report
        claimed = store.claim_next("beta", consumer.instance)
        await consumer.handle(claimed)
        gateway.sessions.hint.assert_not_awaited()
        gateway.run_prompt.assert_not_awaited()
        receipt = store.get_message(requested["id"], "beta")
        assert receipt["state"] == "queued" and not receipt.get("execution_pending")
        assert receipt["evidence"] == {} and "reservation_evidence" not in claimed
        assert not consumer.ownership
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("change", ["removed", "replaced", "execution"])
async def test_steering_rechecks_execution_report_at_native_write(tmp_path, change):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native", harness="claude")
    )
    gateway.sessions.store.harness = "claude"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    handling = None
    try:
        p = project(store)
        requested = ask(store, p["id"])
        owned = execution_report(p["id"])
        gateway._reports["existing-native"] = owned
        async with broker._write_lock:
            claimed = store.claim_next("beta", consumer.instance)
            handling = asyncio.create_task(consumer.handle(claimed))
            async with asyncio.timeout(2):
                while not broker._pending:
                    await asyncio.sleep(0)
            receipt = store.get_message(requested["id"], "beta")
            assert receipt["execution_pending"] and receipt["evidence"]["execution_id"]
            if change == "removed":
                gateway._reports.pop("existing-native")
            elif change == "replaced":
                gateway._reports["existing-native"] = execution_report(p["id"], "new-execution")
            else:
                owned["execution_id"] = "new-execution"
        await asyncio.wait_for(handling, 2)
        assert writer.messages == [] and broker._pending == {}
        receipt = store.get_message(requested["id"], "beta")
        assert receipt["state"] == "queued" and not receipt["execution_pending"]
        assert receipt["evidence"] == {}
        rejected = receipt["delivery_history"][-1]
        assert rejected["not_dispatched"]
        assert rejected["execution_id"] == "tracked-execution"
        assert not consumer.owns_thread("discussion")
    finally:
        if handling and not handling.done():
            handling.cancel()
            await asyncio.gather(handling, return_exceptions=True)
        await close_reports(gateway)
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("state", ["running", "unconfirmed", "interrupted", "cancelling", "done"])
async def test_missing_report_cannot_clear_existing_pending_execution(tmp_path, state):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        owned = execution_report(p["id"])
        gateway._reports["existing-native"] = owned
        assert await consumer.reserve(claimed, "existing-native")
        if state == "done":
            store.reply(requested["id"], "beta", "Early result", claim=claimed["claim"])
        elif state == "cancelling":
            store.cancel(p["id"], "alpha")
        elif state != "running":
            store.update(
                requested["id"],
                "beta",
                consumer.instance,
                state,
                "Prior delivery uncertain",
                claim=claimed["claim"],
            )
        before = store.get_message(requested["id"], "beta")
        gateway._reports.pop("existing-native")
        assert not await consumer.reserve(claimed, "existing-native")
        after = store.get_message(requested["id"], "beta")
        assert after["execution_pending"] and after["evidence"] == before["evidence"]
        assert after["state"] == (
            state if state in {"done", "cancelling", "interrupted"} else "unconfirmed"
        )
        if state == "cancelling":
            assert store.show(p["id"], "alpha")["status"] == "cancelling"
        assert consumer.ownership[(requested["id"], claimed["claim"])] is owned
        assert claimed["reservation_evidence"]["execution_id"] == "tracked-execution"
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_missing_report_with_stale_claim_preserves_new_attempt(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        requested = ask(store, p["id"])
        stale = store.claim_next("beta", consumer.instance)
        store.update(
            requested["id"],
            "beta",
            consumer.instance,
            "unconfirmed",
            "Prior delivery uncertain",
            claim=stale["claim"],
        )
        store.retry(requested["id"], "beta", "Verified no previous execution")
        current = store.claim_next("beta", consumer.instance)
        assert current["claim"] != stale["claim"]
        before = store.get_message(requested["id"], "beta")
        assert not await consumer.reserve(stale, "existing-native")
        assert store.get_message(requested["id"], "beta") == before
        assert "reservation_evidence" not in stale and not consumer.ownership
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize(
    ("sender", "recipient", "parent_name", "error"),
    [
        ("beta", "alpha", "root", "ancestor"),
        ("gamma", "alpha", "child", "ancestor"),
        ("gamma", "beta", "child", "ancestor"),
        ("beta", "beta", "root", "Self-addressed"),
        ("gamma", "gamma", "child", "Self-addressed"),
    ],
)
def test_ancestor_delegation_rejected_before_request_admission(
    tmp_path, sender, recipient, parent_name, error
):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store)
        root = ask(store, p["id"])
        store.claim_next("beta", "parent-worker")
        child = ask(store, p["id"], sender="beta", recipient="gamma", parent=root["id"])
        store.claim_next("gamma", "child-worker")
        before = store.show(p["id"], "alpha")
        parent = {"root": root, "child": child}[parent_name]
        with pytest.raises(ValueError, match=error):
            ask(
                store,
                p["id"],
                sender=sender,
                recipient=recipient,
                parent=parent["id"],
                key="ancestor-callback",
            )
        assert store.show(p["id"], "alpha") == before
        assert not store.db.execute(
            "SELECT 1 FROM messages WHERE key='ancestor-callback'"
        ).fetchone()
        independent = ask(
            store, p["id"], sender="beta", recipient="alpha", key="independent-request"
        )
        assert independent["parent"] is None and independent["depth"] == 0
    finally:
        store.close()


def test_unassigned_child_task_claim_rejects_ancestors_and_accepts_new_participant(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma", "delta"})
    try:
        p = project(store)
        with store.transaction():
            p["members"].append("delta")
            p["bindings"]["delta"] = {"thread_id": "discussion", "channel_id": "floor"}
            store.save_project(p)
        root = ask(store, p["id"])
        child = ask(store, p["id"], sender="beta", recipient="gamma", parent=root["id"])
        opened = ask(
            store,
            p["id"],
            sender="gamma",
            recipient=None,
            parent=child["id"],
            kind="work",
            key="open-task",
        )
        before = store.show(p["id"], "alpha")
        for actor in ("alpha", "beta", "gamma"):
            with pytest.raises(ValueError, match="ancestor|your own"):
                store.claim_open(opened["id"], actor)
            assert store.show(p["id"], "alpha") == before
        claimed = store.claim_open(opened["id"], "delta")
        assert claimed["recipient"] == "delta" and claimed["state"] == "queued"
        assert claimed["depth"] == 2 and claimed["parent"] == child["id"]
    finally:
        store.close()


async def test_blocked_child_result_returns_to_parent_and_allows_corrected_request(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    consumer = None
    try:
        p = project(store)
        parent = ask(store, p["id"])
        store.claim_next("beta", "parent-worker")
        child = ask(store, p["id"], sender="beta", recipient="gamma", parent=parent["id"])
        claimed = store.claim_next("gamma", "child-worker")
        store.reply(
            child["id"],
            "gamma",
            "Need the commit hash from the parent",
            state="blocked",
            claim=claimed["claim"],
        )
        delivery = store.claim_next("beta", "parent-worker")
        assert delivery["kind"] == "result" and delivery["result_state"] == "blocked"
        assert delivery["parent"] == child["id"] and delivery["continuation_parent"] == parent["id"]
        gateway = active_gateway()
        consumer = CollaborationConsumer(gateway, clients["beta"].settings)
        assert "Need the commit hash from the parent" in consumer.prompt(delivery)
        assert "Result status: blocked" in consumer.prompt(delivery)
        corrected = ask(
            store,
            p["id"],
            sender="beta",
            recipient="gamma",
            parent=parent["id"],
            key="corrected-child",
            text="Verify commit abc123",
        )
        assert corrected["id"] != child["id"] and corrected["depth"] == 1
        assert store.claim_next("gamma", "child-worker")["id"] == corrected["id"]
    finally:
        if consumer:
            await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("mode", ["steering", "preparation"])
@pytest.mark.parametrize("state", ["done", "failed", "blocked"])
async def test_reply_before_dispatch_rejection_releases_only_provisional_ownership(
    tmp_path, mode, state
):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native", harness="claude")
    )
    gateway.sessions.store.harness = "claude"
    gateway.sessions.hint = AsyncMock(wraps=gateway.sessions.hint)
    broker.capabilities.steering = mode == "steering"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    original_update = consumer.update
    try:
        p = project(store)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        previous = execution_report(None, "previous-execution")
        previous_terminal = terminal("previous-execution")
        gateway._reports["existing-native"] = previous
        gateway._last_terminal["discussion"] = previous_terminal

        async def reply_before_dispatch(item, update, *args, **kwargs):
            if update == "dispatch":
                store.reply(
                    requested["id"],
                    "beta",
                    "Explicit result before native dispatch",
                    state=state,
                    claim=claimed["claim"],
                )
            return await original_update(item, update, *args, **kwargs)

        consumer.update = reply_before_dispatch
        await consumer.handle(claimed)
        gateway.sessions.hint.assert_not_awaited()
        assert writer.messages == []
        receipt = store.get_message(requested["id"], "beta")
        assert receipt["state"] == state and not receipt.get("execution_pending")
        assert receipt["result"] == "Explicit result before native dispatch"
        assert receipt["evidence"] == {} and "reservation_evidence" not in claimed
        assert not consumer.ownership and not consumer.ownership_threads
        assert not consumer.rejected_reservations and not consumer.active("discussion")
        assert gateway._reports["existing-native"] is previous
        assert gateway._last_terminal["discussion"] is previous_terminal
        followup = ask(store, p["id"], key="followup")
        assert store.claim_next("beta", consumer.instance)["id"] == followup["id"]
    finally:
        await close_reports(gateway)
        await consumer.close()
        await close_api(store, server, clients)


async def test_terminal_dispatch_rejection_after_historical_settlement_creates_no_owner(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    try:
        p = project(store)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        store.update(
            requested["id"],
            "beta",
            consumer.instance,
            "dispatch",
            evidence={"session_id": "existing-native", "execution_id": "historical-execution"},
            claim=claimed["claim"],
        )
        store.reply(requested["id"], "beta", "Historical result", claim=claimed["claim"])
        old_terminal = terminal("historical-execution")
        store.update(
            requested["id"],
            "beta",
            consumer.instance,
            "execution_settled",
            evidence=old_terminal,
            claim=claimed["claim"],
        )
        before = store.get_message(requested["id"], "beta")
        assert not before["execution_pending"] and before["evidence"]["dispatch_started"]
        gateway._last_terminal["discussion"] = old_terminal
        gateway._reports["existing-native"] = execution_report(p["id"], "new-provisional-execution")
        assert not await consumer.reserve(claimed, "existing-native")
        assert store.get_message(requested["id"], "beta") == before
        assert not consumer.ownership and not consumer.ownership_threads
        assert "reservation_evidence" not in claimed and not consumer.rejected_reservations
        assert gateway._last_terminal["discussion"] is old_terminal
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("local_owner", [False, True])
async def test_rejected_new_dispatch_preserves_existing_native_execution_debt(
    tmp_path, legacy, local_owner
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    try:
        p = project(store)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        old = execution_report(p["id"], "existing-execution")
        old.update(active=True, foreground_complete=False)
        gateway._reports["existing-native"] = old
        if local_owner:
            assert await consumer.reserve(claimed, "existing-native")
        else:
            store.update(
                requested["id"],
                "beta",
                consumer.instance,
                "dispatch",
                evidence={"session_id": "existing-native", "execution_id": "existing-execution"},
                claim=claimed["claim"],
            )
        store.reply(requested["id"], "beta", "Early native result", claim=claimed["claim"])
        if legacy:
            with store.transaction():
                current = store.get_message(requested["id"], "beta")
                current.pop("execution_pending")
                store.save_message(current)
        before = store.get_message(requested["id"], "beta")
        old_terminal = terminal("existing-execution")
        gateway._last_terminal["discussion"] = old_terminal
        provisional = execution_report(p["id"], "new-provisional-execution")
        provisional["previous"] = old
        gateway._reports["existing-native"] = provisional
        with pytest.raises(HubError, match="HTTP 409"):
            await consumer.reserve(claimed, "existing-native")
        key = (requested["id"], claimed["claim"])
        assert store.get_message(requested["id"], "beta") == before
        assert gateway._last_terminal["discussion"] is old_terminal
        assert not consumer.rejected_reservations
        if local_owner:
            assert consumer.ownership[key] is old
            assert claimed["reservation_evidence"]["execution_id"] == "existing-execution"
            assert consumer.ownership_pending(key)
            old.update(active=False, foreground_complete=True)
            await consumer.reconcile_ownership()
            assert not store.get_message(requested["id"], "beta")["execution_pending"]
            assert not consumer.owns_thread("discussion")
        else:
            assert not consumer.ownership and not consumer.ownership_threads
            assert "reservation_evidence" not in claimed
            assert before.get("execution_pending", True)
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_conflicting_response_after_accepted_reservation_keeps_matching_native_owner(
    tmp_path,
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    original_update = consumer.update
    try:
        p = project(store)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        owned = execution_report(p["id"])
        gateway._reports["existing-native"] = owned

        async def accepted_then_conflict(item, update, *args, **kwargs):
            receipt = await original_update(item, update, *args, **kwargs)
            if update == "dispatch":
                store.reply(
                    requested["id"], "beta", "Accepted early result", claim=claimed["claim"]
                )
                raise HubError("Response rejected after accepted reservation", status=409)
            return receipt

        consumer.update = accepted_then_conflict
        with pytest.raises(HubError, match="Response rejected"):
            await consumer.reserve(claimed, "existing-native")
        key = (requested["id"], claimed["claim"])
        current = store.get_message(requested["id"], "beta")
        assert current["state"] == "done" and current["execution_pending"]
        assert consumer.ownership[key] is owned and not consumer.rejected_reservations
        assert current["evidence"]["execution_id"] == owned["execution_id"]
        assert claimed["reservation_evidence"]["execution_id"] == owned["execution_id"]
        gateway._last_terminal["discussion"] = terminal(owned["execution_id"])
        await consumer.reconcile_ownership()
        assert (
            not consumer.ownership
            and not store.get_message(requested["id"], "beta")["execution_pending"]
        )
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_rejected_dispatch_with_failed_read_recovers_on_ownership_reconciliation(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    original_update, original_request = consumer.update, consumer.hub.request
    try:
        p = project(store)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        owned = execution_report(p["id"])
        gateway._reports["existing-native"] = owned
        previous_terminal = terminal("previous-execution")
        gateway._last_terminal["discussion"] = previous_terminal

        async def reply_before_dispatch(item, update, *args, **kwargs):
            store.reply(requested["id"], "beta", "Early result", claim=claimed["claim"])
            return await original_update(item, update, *args, **kwargs)

        async def unreadable(method, path, body=None):
            if method == "GET":
                raise TimeoutError("Reservation verification unavailable")
            return await original_request(method, path, body)

        consumer.update, consumer.hub.request = reply_before_dispatch, unreadable
        with pytest.raises(TimeoutError, match="verification unavailable"):
            await consumer.reserve(claimed, "existing-native")
        key = (requested["id"], claimed["claim"])
        assert consumer.ownership[key] is owned and key in consumer.rejected_reservations
        consumer.hub.request, consumer.update = original_request, original_update
        await consumer.reconcile_ownership()
        assert not consumer.ownership and not consumer.ownership_threads
        assert not consumer.rejected_reservations and "reservation_evidence" not in claimed
        assert gateway._last_terminal["discussion"] is previous_terminal
        settled = store.get_message(requested["id"], "beta")
        assert settled["state"] == "done" and settled["evidence"] == {}
    finally:
        await consumer.close()
        await close_api(store, server, clients)
