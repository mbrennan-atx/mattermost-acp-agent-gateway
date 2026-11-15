"""Result delivery and reconciled ownership use synthetic local native sessions."""

import asyncio
import json
import os
import stat
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_continuation_review import child_result, retained_parent, terminal
from test_collaboration_dispatch_review import close_reports, idle_report, native_gateway
from test_collaboration_regressions import (
    maintenance_client,
    maintenance_gateway,
    memory_broker,
    sidecar_project,
)

from mattermost_acp.acp import SessionUpdate, TurnResult
from mattermost_acp.collaboration.client import HubError
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.mm.client import PostedEvent
from mattermost_acp.session_service import SessionService


def test_result_retry_at_full_work_budget_preserves_result_and_requires_reconciliation(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store, max_turns=1)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", "worker")
        store.reply(
            requested["id"],
            "beta",
            "Verified dependency",
            evidence={"artifact": "https://example.com/proof"},
            claim=claimed["claim"],
        )
        delivery = store.claim_next("alpha", "owner")
        store.update(
            delivery["id"],
            "alpha",
            "owner",
            "evidence",
            evidence={"post_id": "uncertain-post"},
            claim=delivery["claim"],
        )
        store.update(
            delivery["id"],
            "alpha",
            "owner",
            "unconfirmed",
            "Result posting was ambiguous",
            claim=delivery["claim"],
        )
        retried = store.retry(delivery["id"], "alpha", "Verified no result post exists")
        assert retried["state"] == "queued"
        assert retried["result_evidence"]["artifact"] == "https://example.com/proof"
        assert retried["delivery_history"][-1]["post_id"] == "uncertain-post"
        assert store.show(p["id"], "alpha")["dispatched"] == 1
        replay = store.claim_next("alpha", "new-owner")
        assert replay["id"] == delivery["id"] and replay["claim"] != delivery["claim"]
        assert replay["text"] == "Verified dependency"
        assert store.get_message(requested["id"], "beta")["state"] == "done"
        with pytest.raises(ValueError, match="dispatch limit"):
            ask(store, p["id"], key="another-request")
    finally:
        store.close()


@pytest.mark.parametrize("delivery", ["foreground", "injected"])
@pytest.mark.parametrize("background", ["tool", "worker"])
async def test_early_reply_keeps_native_owner_until_background_finishes(
    tmp_path, delivery, background
):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    broker.capabilities.steering = delivery == "injected"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    handling = None
    observed_terminal = asyncio.Event()
    actual_request = consumer.hub.request

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

    async def observe(method, path, body=None):
        result = await actual_request(method, path, body)
        if method == "GET" and path == "/messages/" + claimed["id"] and result["state"] == "done":
            observed_terminal.set()
        return result

    consumer.hub.request = observe
    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            await options["on_update"](update())
            store.reply(request["id"], "beta", "Explicit early result", claim=claimed["claim"])
            return TurnResult("existing-native", "end_turn", "Completed authorized work", "")

        async def inject(_thread, _prompt, **_options):
            gateway._observe_background_update(update())
            gateway._last_terminal["discussion"] = {
                "session_id": "existing-native",
                "execution_id": owned["execution_id"],
                "answer": "Completed authorized work",
                "stop_reason": "end_turn",
                "pending": True,
                "native_dispatched": True,
                "completed_at": store.clock(),
            }
            return "injected"

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        if delivery == "injected":
            owned = idle_report()
            owned["collaboration_project"] = p["id"]
            gateway._reports["existing-native"] = owned
            gateway.sessions.hint = AsyncMock(side_effect=inject)
        handling = asyncio.create_task(consumer.handle(claimed))
        if delivery == "injected":
            async with asyncio.timeout(2):
                while (
                    store.get_message(request["id"], "beta")["evidence"].get("delivery")
                    != "injected"
                ):
                    await asyncio.sleep(0)
            store.reply(request["id"], "beta", "Explicit early result", claim=claimed["claim"])
        await asyncio.wait_for(observed_terminal.wait(), 2)
        assert not handling.done() and consumer.owns_thread("discussion")
        assert consumer.ownership[(request["id"], claimed["claim"])]["reporter"].has_pending_work()
        await gateway.run_prompt(
            thread_id="discussion", channel_id="floor", prompt="User follow-up"
        )
        assert gateway.sessions.run_turn.await_count == (1 if delivery == "foreground" else 0)
        gateway._observe_background_update(update(completed=True))
        await asyncio.wait_for(handling, 3)
        assert not consumer.owns_thread("discussion")
        settled = store.get_message(request["id"], "beta")
        assert settled["result"] == "Explicit early result"
        assert settled["evidence"]["foreground_answer"] == "Completed authorized work"
        assert not settled["evidence"]["pending"]
    finally:
        if handling and not handling.done():
            handling.cancel()
            await asyncio.gather(handling, return_exceptions=True)
        await consumer.close()
        await close_reports(gateway)
        await close_api(store, server, clients)


@pytest.mark.parametrize("pending", ["foreground", "tool", "worker"])
async def test_project_cancel_stops_native_execution_after_early_result(tmp_path, pending):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    broker.capabilities.steering = False
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    entered, release = asyncio.Event(), asyncio.Event()
    handling = cancelling = None

    def background_update(completed=False):
        if pending == "worker":
            return SessionUpdate(
                "existing-native",
                "subagent_activity",
                raw={"worker_id": "early-worker", "state": "completed" if completed else "running"},
            )
        return SessionUpdate(
            "existing-native",
            "tool_call_update" if completed else "tool_call",
            tool_call_id="early-tool",
            tool_status="completed" if completed else "in_progress",
        )

    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        unrelated = project(store)
        untouched = ask(store, unrelated["id"], recipient="gamma")

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            if pending != "foreground":
                await options["on_update"](background_update())
            store.reply(
                request["id"],
                "beta",
                "Explicit early result",
                evidence={"artifact": "https://example.com/proof"},
                claim=claimed["claim"],
            )
            entered.set()
            if pending == "foreground":
                await release.wait()
                return TurnResult("existing-native", "cancelled", "Native execution stopped", "")
            return TurnResult("existing-native", "end_turn", "Native foreground completed", "")

        async def cancel(_thread, **options):
            assert options["expected_session"] == "existing-native"
            assert options["guard"]()
            if pending == "foreground":
                release.set()
            else:
                gateway._observe_background_update(background_update(completed=True))

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        gateway.sessions.cancel = AsyncMock(side_effect=cancel)
        handling = asyncio.create_task(consumer.handle(claimed))
        await asyncio.wait_for(entered.wait(), 2)
        if pending != "foreground":
            async with asyncio.timeout(2):
                while "discussion" not in gateway._last_terminal:
                    await asyncio.sleep(0)
        assert store.get_message(request["id"], "beta")["state"] == "done"
        store.cancel(p["id"], "alpha")
        assert store.show(p["id"], "alpha")["status"] == "cancelling"
        control = store.claim_next("beta", consumer.instance)
        assert control and control["kind"] == "cancel"
        assert control["parent_claim"] == claimed["claim"]
        cancelling = asyncio.create_task(consumer.handle(control))
        await asyncio.wait_for(asyncio.gather(handling, cancelling), 4)
        gateway.sessions.cancel.assert_awaited_once()
        settled = store.get_message(request["id"], "beta")
        assert settled["state"] == "done" and settled["result"] == "Explicit early result"
        assert settled["evidence"]["artifact"] == "https://example.com/proof"
        assert not settled["execution_pending"] and not settled["execution_cancelling"]
        assert store.show(p["id"], "alpha")["status"] == "cancelled"
        assert store.show(unrelated["id"], "alpha")["status"] == "active"
        assert store.get_message(untouched["id"], "gamma")["state"] == "queued"
        assert not consumer.owns_thread("discussion")
        assert (
            len([m for m in store.show(p["id"], "alpha")["messages"] if m["kind"] == "result"]) == 1
        )
    finally:
        release.set()
        tasks = [task for task in [handling, cancelling] if task]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.close()
        await close_reports(gateway)
        await close_api(store, server, clients)


@pytest.mark.parametrize(
    "mismatch", ["session", "execution", "pending", "started", "no_dispatch", "nan"]
)
async def test_native_settlement_cannot_clear_early_result_cancellation_with_invalid_receipt(
    tmp_path, mismatch
):
    async with retained_parent(tmp_path) as (store, _clients, _gateway, consumer, p, parent, _old):
        store.reply(parent["id"], "beta", "Explicit early result", claim=parent["claim"])
        store.cancel(p["id"], "alpha")
        evidence = terminal()
        if mismatch == "session":
            evidence["session_id"] = "another-session"
        elif mismatch == "execution":
            evidence["execution_id"] = "another-execution"
        elif mismatch == "pending":
            evidence["pending"] = True
        elif mismatch == "started":
            evidence["completed_at"] = 0
        elif mismatch == "no_dispatch":
            evidence["not_dispatched"] = True
        else:
            evidence["completed_at"] = float("nan")
        with pytest.raises(ValueError, match="settlement"):
            store.update(
                parent["id"],
                "beta",
                consumer.instance,
                "execution_settled",
                evidence=evidence,
                claim=parent["claim"],
            )
        unresolved = store.get_message(parent["id"], "beta")
        assert unresolved["execution_pending"] and unresolved["execution_cancelling"]
        assert unresolved["result"] == "Explicit early result"
        assert store.show(p["id"], "alpha")["status"] == "cancelling"
        store.update(
            parent["id"],
            "beta",
            consumer.instance,
            "execution_settled",
            evidence=terminal(),
            claim=parent["claim"],
        )
        assert store.show(p["id"], "alpha")["status"] == "cancelled"
        assert store.get_message(parent["id"], "beta")["state"] == "done"


async def test_early_result_retains_worker_slot_and_admission_claim_until_native_settlement(
    tmp_path,
):
    async with retained_parent(tmp_path) as (store, _clients, _gateway, consumer, p, parent, _old):
        p["limits"]["max_workers"] = 1
        with store.transaction():
            store.save_project(p)
        store.reply(parent["id"], "beta", "Explicit early result", claim=parent["claim"])
        next_request = ask(store, p["id"], recipient="gamma", key="next-worker")
        assert store.claim_next("gamma", "another-worker") is None
        assert [item["id"] for item in store.active_claims("beta", consumer.instance)] == [
            parent["id"]
        ]
        store.update(
            parent["id"],
            "beta",
            consumer.instance,
            "execution_settled",
            evidence=terminal(),
            claim=parent["claim"],
        )
        assert store.active_claims("beta", consumer.instance) == []
        assert store.claim_next("gamma", "another-worker")["id"] == next_request["id"]


async def test_admission_cannot_forget_unobserved_native_execution_after_early_reply(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, _gateway, consumer, _p, parent, _old):
        store.reply(parent["id"], "beta", "Explicit early result", claim=parent["claim"])
        consumer.ownership.pop((parent["id"], parent["claim"]))
        consumer.claim_uncertain = True
        with pytest.raises(RuntimeError, match="native reconciliation"):
            await consumer.set_admission("sealed")
        assert consumer.claim_uncertain
        assert store.get_message(parent["id"], "beta")["execution_pending"]


async def test_old_worker_cleanup_preserves_new_claim_owner_and_cancellation_fence(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, _p, parent, old):
        entered, release = asyncio.Event(), asyncio.Event()
        actual_request = consumer.hub.request
        delayed = False

        async def delayed_read(method, path, body=None):
            nonlocal delayed
            if method == "GET" and path == "/messages/" + parent["id"] and not delayed:
                delayed = True
                entered.set()
                await release.wait()
            return await actual_request(method, path, body)

        consumer.hub.request = delayed_read
        handling = asyncio.create_task(consumer.handle(dict(parent)))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            store.update(
                parent["id"], "beta", consumer.instance, "unconfirmed", "Old delivery lost"
            )
            store.retry(parent["id"], "beta", "Old execution reconciled")
            fresh = store.claim_next("beta", consumer.instance)
            assert fresh["claim"] != parent["claim"]
            consumer.release_ownership((parent["id"], parent["claim"]))
            current = idle_report()
            current.update(
                execution_id="new-execution",
                collaboration_project=old["collaboration_project"],
                active=True,
                foreground_complete=False,
            )
            current["reporter"].active_tools.add("pending-new-work")
            gateway._reports["existing-native"] = current
            assert await consumer.reserve(fresh, "existing-native")
            store.reply(fresh["id"], "beta", "New early result", claim=fresh["claim"])
            key = (fresh["id"], fresh["claim"])
            consumer.cancelled_claims.add(key)
            release.set()
            await asyncio.wait_for(handling, 3)
            assert consumer.ownership[key] is current
            assert key in consumer.cancelled_claims and consumer.owns_thread("discussion")
            assert current["reporter"].has_pending_work()
            gateway.run_prompt.assert_not_awaited()
        finally:
            release.set()
            if not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)


@pytest.mark.parametrize("reused", [False, True])
async def test_duplicate_floor_ask_after_cancellation_cannot_wake_new_native_work(tmp_path, reused):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.allow_user_ids = frozenset({"alpha-user"})
    gateway.config.all_channels = False
    gateway.config.exclude_channel_ids = frozenset()
    gateway.config.text_slash = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    posted = PostedEvent(
        post_id="post-1",
        channel_id="floor",
        user_id="alpha-user",
        message="@beta ASK: Verify the dependency",
        root_id="discussion",
    )
    body = {
        "post_id": posted.post_id,
        "author_id": posted.user_id,
        "channel_id": posted.channel_id,
        "thread_id": posted.root_id,
        "text": posted.message,
    }
    try:
        p = project(store)
        admitted = await clients["beta"].request("POST", "/mentions", body)
        store.cancel(p["id"], "alpha")
        if reused:
            store.create_project(
                "alpha",
                {
                    "title": "Next project",
                    "objective": "Use the same discussion",
                    "members": ["alpha", "beta", "gamma"],
                    "channel_id": "floor",
                    "thread_id": "discussion",
                    "origin_channel_id": "home",
                    "origin_thread_id": "origin",
                },
            )
        duplicate = await clients["beta"].request("POST", "/mentions", body)
        assert duplicate["id"] == admitted["id"] and duplicate["state"] == "cancelled"
        await gateway.handle_posted(posted)
        gateway.run_prompt.assert_not_awaited()
        all_messages = [
            m
            for item in store.list_projects("alpha")
            for m in store.show(item["id"], "alpha")["messages"]
        ]
        assert len(all_messages) == 1 and all_messages[0]["id"] == admitted["id"]
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("missing", [None, "completed_at", "dispatch_started"])
async def test_late_reply_cannot_reconcile_with_receipt_before_new_injection(tmp_path, missing):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, _p, parent, old):
        initial = store.get_message(parent["id"], "beta")["evidence"]["dispatch_started"]
        receipt = gateway._last_terminal["discussion"]
        receipt["completed_at"] = initial
        receipt["answer"] = "Output before the new injection"
        store.clock = lambda: initial + 1
        assert await consumer.reserve(parent, "existing-native")
        await consumer.update(parent, "unconfirmed", "New injection receipt unavailable")
        settled = store.reply(parent["id"], "beta", "Explicit late result", claim=parent["claim"])
        expected = dict(settled["evidence"])
        if missing == "completed_at":
            receipt.pop("completed_at")
        elif missing == "dispatch_started":
            settled["evidence"].pop("dispatch_started")
            with store.transaction():
                store.save_message(settled)
            expected = dict(settled["evidence"])

        await consumer.reconcile_ownership()
        assert consumer.ownership[(parent["id"], parent["claim"])] is old
        assert consumer.owns_thread("discussion")
        assert store.get_message(parent["id"], "beta")["evidence"] == expected

        if missing == "dispatch_started":
            settled["evidence"]["dispatch_started"] = initial + 1
            with store.transaction():
                store.save_message(settled)
        receipt.update(completed_at=initial + 1, answer="Output after the new injection")
        await consumer.reconcile_ownership()
        assert not consumer.owns_thread("discussion")
        final = store.get_message(parent["id"], "beta")
        assert final["result"] == "Explicit late result"
        assert final["evidence"]["foreground_answer"] == "Output after the new injection"


@pytest.mark.parametrize("limit", ["dispatch", "membership", "sender-membership"])
async def test_rejected_project_ask_cannot_fall_through_to_allowlisted_native_wake(tmp_path, limit):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.allow_user_ids = frozenset({"alpha-user"})
    gateway.config.all_channels = False
    gateway.config.exclude_channel_ids = frozenset()
    gateway.config.text_slash = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    try:
        if "membership" in limit:
            p = store.create_project(
                "gamma" if limit == "sender-membership" else "alpha",
                {
                    "title": "Restricted participants",
                    "objective": "Verify project admission",
                    "members": ["beta", "gamma"]
                    if limit == "sender-membership"
                    else ["alpha", "gamma"],
                    "channel_id": "floor",
                    "thread_id": "discussion",
                    "origin_channel_id": "home",
                    "origin_thread_id": "origin",
                },
            )
        else:
            p = project(store, max_turns=1)
        if limit == "dispatch":
            ask(store, p["id"])
        posted = PostedEvent(
            post_id="post-1",
            channel_id="floor",
            user_id="alpha-user",
            message="@beta ASK: Verify the dependency",
            root_id="discussion",
        )
        with pytest.raises(HubError, match="HTTP 403" if "membership" in limit else "HTTP 409"):
            await clients["beta"].request(
                "POST",
                "/mentions",
                {
                    "post_id": posted.post_id,
                    "author_id": posted.user_id,
                    "channel_id": posted.channel_id,
                    "thread_id": posted.root_id,
                    "text": posted.message,
                },
            )
        before = store.show(p["id"], p["owner"])["messages"]
        await gateway.handle_posted(posted)
        gateway.run_prompt.assert_not_awaited()
        assert store.show(p["id"], p["owner"])["messages"] == before

        unregistered = PostedEvent(
            post_id="post-2",
            channel_id="floor",
            user_id="alpha-user",
            message=posted.message,
            root_id="unregistered",
        )
        await gateway.handle_posted(unregistered)
        gateway.run_prompt.assert_awaited_once()
        assert gateway.run_prompt.call_args.kwargs["thread_id"] == "unregistered"
    finally:
        await consumer.close()
        await close_api(store, server, clients)


def test_message_expiry_index_upgrades_history_and_preserves_stale_lease_expiry(tmp_path):
    now = [1000.0]
    path = tmp_path / "hub.db"
    actors = {"alpha", "beta", "gamma"}
    store = CollaborationStore(path, actors, clock=lambda: now[0])
    try:
        p = project(store)
        completed = ask(store, p["id"])
        store.claim_next("beta", "history-worker")
        store.reply(completed["id"], "beta", "Completed historical task")
        completed = store.get_message(completed["id"], "beta")
        with store.transaction():
            for index in range(256):
                store.save_message(
                    {**completed, "id": f"history-{index}", "key": f"history-{index}"}
                )
            store.db.execute("DROP INDEX IF EXISTS message_expiry")
    finally:
        store.close()

    reopened = CollaborationStore(path, actors, clock=lambda: now[0])
    try:
        plan = reopened.db.execute(
            "EXPLAIN QUERY PLAN SELECT data FROM messages WHERE state='running'"
        ).fetchall()
        assert any("SEARCH messages USING INDEX message_expiry" in row[3] for row in plan)
        request = ask(reopened, p["id"], key="current-request")
        assert reopened.claim_next("beta", "current-worker")["id"] == request["id"]
        now[0] += 46
        reopened.expire()
        assert reopened.get_message(request["id"], "beta")["state"] == "interrupted"
        assert (
            reopened.db.execute("SELECT count(*) FROM messages WHERE state='done'").fetchone()[0]
            == 257
        )
    finally:
        reopened.close()


@pytest.mark.parametrize("directory_failure", [False, True])
async def test_stop_intent_syncs_directory_after_replacement_and_closes_on_failure(
    tmp_path, monkeypatch, directory_failure
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway()
    gateway.config.db_path = tmp_path / "nested" / "project.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    calls = []
    real_fsync = os.fsync

    def sync(descriptor):
        directory = stat.S_ISDIR(os.fstat(descriptor).st_mode)
        calls.append((descriptor, directory))
        if directory:
            assert json.loads(consumer.stops_path.read_text()) == consumer.pending_stops
            if directory_failure:
                raise OSError("Directory synchronization unavailable")
        real_fsync(descriptor)

    try:
        consumer.pending_stops = {"origin": 1000.0}
        with monkeypatch.context() as patch:
            patch.setattr(os, "fsync", sync)
            if directory_failure:
                with pytest.raises(OSError, match="Directory synchronization unavailable"):
                    consumer.save_stops()
            else:
                consumer.save_stops()
        assert [directory for _descriptor, directory in calls] == [False, True]
        with pytest.raises(OSError):
            os.fstat(calls[-1][0])
        assert consumer.pending_stops == {"origin": 1000.0}
        restarted = CollaborationConsumer(gateway, clients["beta"].settings)
        try:
            assert restarted.pending_stops == consumer.pending_stops
        finally:
            await restarted.close()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


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
