"""Proven pre-dispatch failures retry without affecting replacement work."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from test_collaboration import api_fixture, ask, close_api, project
from test_collaboration_continuation_review import retained_parent
from test_collaboration_independent_cancellation import native_stops

from mattermost_acp.acp.broker import NativeDispatchChanged
from mattermost_acp.collaboration.client import HubError
from mattermost_acp.collaboration.store import CollaborationStore

pytest_plugins = ["test_session_service"]


@pytest.fixture
def short_stop_bound(monkeypatch):
    original = asyncio.timeout
    monkeypatch.setattr(asyncio, "timeout", lambda delay: original(0.02 if delay == 3 else delay))


async def expired_native(gateway, consumer, report, project, tmp_path):
    observer = consumer.deadline_tasks.pop(project["id"], None)
    if observer:
        observer.cancel()
        await asyncio.gather(observer, return_exceptions=True)
    broker, writer = native_stops(gateway, report, tmp_path)
    report["reporter"].stoppable_async_tasks = lambda: []
    consumer.project_deadlines[project["id"]] = -1
    consumer.hub.request = AsyncMock(side_effect=HubError("Hub offline", status=503))
    return broker, writer


@pytest.mark.parametrize("accepted_child", [False, True])
async def test_deadline_write_lock_timeout_retries_locally_while_hub_offline(
    tmp_path, short_stop_bound, accepted_child
):
    async with retained_parent(tmp_path) as (
        _store,
        _clients,
        gateway,
        consumer,
        project,
        _parent,
        report,
    ):
        broker, writer = await expired_native(gateway, consumer, report, project, tmp_path)
        if accepted_child:
            report["reporter"].stoppable_async_tasks = lambda: ["first-task"]
            broker.stop_async_task = AsyncMock(return_value=True)
        async with broker._write_lock:
            await consumer.enforce_deadline(project["id"])
        assert writer.messages == []
        assert not consumer.deadline_stops
        await consumer.reconcile_deadlines()
        assert [message["method"] for message in writer.messages] == ["session/cancel"]
        assert len(consumer.deadline_stops) == 1
        await consumer.reconcile_deadlines()
        assert len(writer.messages) == 1
        if accepted_child:
            broker.stop_async_task.assert_awaited_once()


async def test_cancelled_deadline_observer_before_parent_dispatch_releases_attempt(tmp_path):
    async with retained_parent(tmp_path) as (
        _store,
        _clients,
        gateway,
        consumer,
        project,
        _parent,
        report,
    ):
        broker, writer = await expired_native(gateway, consumer, report, project, tmp_path)
        report["reporter"].stoppable_async_tasks = lambda: ["first-task"]
        entered = asyncio.Event()

        async def child(_session, _task, *, guard):
            assert guard()
            entered.set()
            await asyncio.Event().wait()

        broker.stop_async_task = AsyncMock(side_effect=child)
        task = asyncio.create_task(consumer.enforce_deadline(project["id"]))
        consumer.deadline_tasks[project["id"]] = task
        try:
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert writer.messages == [] and not consumer.deadline_stops
            report["reporter"].stoppable_async_tasks = lambda: []
            await consumer.reconcile_deadlines()
            assert [message["method"] for message in writer.messages] == ["session/cancel"]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("failure", ["timeout", "cancelled", "error"])
async def test_physical_deadline_dispatch_is_not_repeated_after_drain_failure(
    tmp_path, short_stop_bound, failure
):
    async with retained_parent(tmp_path) as (
        _store,
        _clients,
        gateway,
        consumer,
        project,
        _parent,
        report,
    ):
        _broker, writer = await expired_native(gateway, consumer, report, project, tmp_path)
        entered = asyncio.Event()

        async def drain():
            entered.set()
            if failure == "error":
                raise OSError("Cancellation bytes written; drain failed")
            await asyncio.Event().wait()

        writer.drain = drain
        task = asyncio.create_task(consumer.enforce_deadline(project["id"]))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            if failure == "cancelled":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                await task
            assert len(writer.messages) == 1 and len(consumer.deadline_stops) == 1
            await consumer.reconcile_deadlines()
            assert len(writer.messages) == 1
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("waiting", ["thread", "global"])
@pytest.mark.parametrize("replacement", ["new", "epoch"])
async def test_effort_queued_before_replacement_preserves_native_and_stored_settings(
    service, waiting, replacement
):
    thread = "effort-thread"
    await service.ensure_session(thread)
    captured = asyncio.Event()
    original_get = service.store.get

    async def get(target):
        record = await original_get(target)
        captured.set()
        return record

    service.store.get = get
    held = []
    if waiting == "thread":
        lock = service._thread_locks.setdefault(thread, asyncio.Lock())
        await lock.acquire()
        held.append(lock)
    else:
        while service._global_sem._value:
            await service._global_sem.acquire()
            held.append(service._global_sem)
    task = asyncio.create_task(service.set_effort(thread, "high"))
    try:
        await asyncio.wait_for(captured.wait(), 1)
        if replacement == "new":
            await service.new_session(thread)
        else:
            service._advance_native(thread)
        before = await original_get(thread)
        service.broker.set_model = AsyncMock(wraps=service.broker.set_model)
        for lock in held:
            lock.release()
        held.clear()
        with pytest.raises(NativeDispatchChanged, match="effort command waited"):
            await asyncio.wait_for(task, 2)
        service.broker.set_model.assert_not_awaited()
        after = await original_get(thread)
        assert (after.session_id, after.model, after.effort) == (
            before.session_id,
            before.model,
            before.effort,
        )
    finally:
        for lock in held:
            lock.release()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_empty_busy_retries_keep_history_and_message_size_constant(tmp_path):
    now = [0.0]
    store = CollaborationStore(
        tmp_path / "hub.db",
        {"alpha", "beta", "gamma"},
        clock=lambda: now[0],
        elapsed=lambda: now[0],
        epoch="synthetic",
    )
    try:
        p = project(store)
        request = ask(store, p["id"])
        sizes = []
        for _ in range(100):
            claimed = store.claim_next("beta", "worker")
            assert claimed["id"] == request["id"]
            receipt = store.update(
                request["id"], "beta", "worker", "queued", claim=claimed["claim"]
            )
            sizes.append(len(json.dumps(receipt)))
            now[0] += 3
        assert store.get_message(request["id"], "beta").get("delivery_history", []) == []
        assert max(sizes) - min(sizes) < 100
        assert store.get_project(p["id"], "alpha")["dispatched"] == 1
    finally:
        store.close()


async def test_retained_execution_history_is_absent_from_routine_payloads(tmp_path, monkeypatch):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        request = ask(store, p["id"])
        history = [
            {"session_id": "old-native", "execution_id": f"old-{index}", "proof": "x" * 256}
            for index in range(500)
        ]
        stored = store.get_message(request["id"], "beta")
        stored["delivery_history"] = history
        with store.transaction():
            store.save_message(stored)
        dumps = json.dumps
        serialized_histories = []

        def bounded_dump(value, *args, **kwargs):
            if isinstance(value, dict) and value.get("id") == request["id"]:
                serialized_histories.append(len(value.get("delivery_history", [])))
            return dumps(value, *args, **kwargs)

        monkeypatch.setattr(json, "dumps", bounded_dump)
        claimed = store.claim_next("beta", "worker")
        assert "delivery_history" not in claimed
        direct = store.update(request["id"], "beta", "worker", "heartbeat", claim=claimed["claim"])
        assert direct["delivery_history"] == history
        queries = []
        store.db.set_trace_callback(queries.append)
        receipt = store.update(
            request["id"],
            "beta",
            "worker",
            "heartbeat",
            claim=claimed["claim"],
            include_history=False,
        )
        assert "delivery_history" not in receipt
        receipt = await clients["beta"].request(
            "POST",
            "/messages/" + request["id"] + "/update",
            {"instance": "worker", "state": "queued", "claim": claimed["claim"]},
        )
        assert "delivery_history" not in receipt
        brief = await clients["beta"].request("GET", "/messages/" + request["id"])
        assert "delivery_history" not in brief
        assert len(dumps(brief)) < 4096
        assert all(count == 0 for count in serialized_histories)
        store.db.set_trace_callback(None)
        assert not any("FROM DELIVERY_HISTORY" in query.upper() for query in queries)
        assert store.get_message(request["id"], "beta")["delivery_history"] == history
        detailed = await clients["beta"].request("GET", "/messages/" + request["id"] + "?history=1")
        assert detailed["delivery_history"] == history
    finally:
        store.db.set_trace_callback(None)
        await close_api(store, server, clients)


def test_legacy_inline_history_migrates_once_without_losing_execution_proof(tmp_path):
    path = tmp_path / "hub.db"
    store = CollaborationStore(path, {"alpha", "beta", "gamma"})
    p = project(store)
    request = ask(store, p["id"])
    history = [{}, {"session_id": "prior-native", "execution_id": "prior-execution"}]
    legacy = {**request, "delivery_history": history}
    with store.transaction():
        store.db.execute(
            "UPDATE messages SET data=? WHERE id=?", (json.dumps(legacy), request["id"])
        )
    store.close()
    for _ in range(2):
        store = CollaborationStore(path, {"alpha", "beta", "gamma"})
        try:
            assert store.get_message(request["id"], "beta")["delivery_history"] == history
            raw = store.db.execute(
                "SELECT data FROM messages WHERE id=?", (request["id"],)
            ).fetchone()[0]
            assert "delivery_history" not in json.loads(raw)
        finally:
            store.close()


def test_real_rejected_reservation_history_remains_available_after_queue_retry(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", "worker")
        reserved = store.update(
            request["id"],
            "beta",
            "worker",
            "dispatch",
            evidence={"session_id": "native", "execution_id": "execution", "native_epoch": 7},
            claim=claimed["claim"],
        )
        proof = dict(reserved["evidence"])
        store.update(request["id"], "beta", "worker", "queued", claim=claimed["claim"])
        assert store.get_message(request["id"], "beta")["delivery_history"] == [proof]
    finally:
        store.close()
