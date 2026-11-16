"""Cancellation uses durable hub order, including ambiguous transport and upgrade."""

import asyncio
import json
import sqlite3
from unittest.mock import AsyncMock

import pytest
from test_collaboration import project
from test_collaboration_regressions import (
    active_gateway,
    api_fixture,
    close_api,
    creation_body,
)

from mattermost_acp.collaboration import store as store_module
from mattermost_acp.collaboration.client import HubError
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.store import CollaborationStore


def status(store, item):
    return store.get_project(item["id"], item["owner"])["status"]


@pytest.mark.parametrize("later_clock", [500.0, 1000.0, 10_000.0])
def test_hub_order_preserves_later_creation_after_clock_change(tmp_path, later_clock):
    now = [1000.0]
    store = CollaborationStore(
        tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: now[0]
    )
    try:
        old = project(store)
        receipt = store.capture_stop_boundary("alpha", "origin", "first-stop")
        now[0] = later_clock
        newer = project(store)
        assert store.cancel_thread("alpha", "origin", boundary=receipt) == []
        assert status(store, old) == "cancelled" and status(store, newer) == "active"
        assert store.capture_stop_boundary("alpha", "origin", "first-stop") == receipt
        assert store.cancel_thread("alpha", "origin", boundary=receipt) == []
    finally:
        store.close()


def test_preboundary_intent_finishes_cancelled_and_later_intent_remains_active(tmp_path):
    now = [1000.0]
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta"}, clock=lambda: now[0])
    try:
        early, _ = store.prepare_creation("alpha", creation_body())
        receipt = store.capture_stop_boundary("alpha", "origin", "stop")
        assert store.cancel_thread("alpha", "origin", boundary=receipt) == []
        now[0] = 100.0
        later, _ = store.prepare_creation("alpha", {**creation_body(), "key": "later"})
        late_project = store.finish_creation("alpha", {**later, "thread_id": "later-root"})
        early_project = store.finish_creation("alpha", {**early, "thread_id": "early-root"})
        assert early_project["status"] == "cancelled"
        assert late_project["status"] == "active"
        assert store.known_discussion("floor", "early-root")
        assert store.known_discussion("floor", "later-root")
        assert early_project["dispatched"] == late_project["dispatched"] == 0
    finally:
        store.close()


def test_delayed_older_stop_cannot_replace_newer_order_after_restart(tmp_path):
    path = tmp_path / "hub.db"
    now = [1000.0]
    store = CollaborationStore(path, {"alpha", "beta", "gamma"}, clock=lambda: now[0])
    first = project(store)
    old_stop = store.capture_stop_boundary("alpha", "origin", "old")
    now[0] = 200.0
    middle = project(store)
    new_stop = store.capture_stop_boundary("alpha", "origin", "new")
    store.cancel_thread("alpha", "origin", boundary=new_stop)
    now[0] = 100.0
    later = project(store)
    store.close()
    store = CollaborationStore(path, {"alpha", "beta", "gamma"}, clock=lambda: now[0])
    try:
        assert store.get_stop_boundary("alpha", "old") == old_stop
        assert store.cancel_thread("alpha", "origin", boundary=old_stop) == []
        assert status(store, first) == status(store, middle) == "cancelled"
        assert status(store, later) == "active"
        assert (
            store.db.execute("SELECT admission_order FROM origin_stops").fetchone()[0]
            == new_stop["admission_order"]
        )
    finally:
        store.close()


async def test_lost_capture_response_reconciles_key_without_widening(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    reopened = None
    real_request = consumer.hub.request
    seen = []

    async def lose_capture(method, path, body=None):
        seen.append((method, path, body))
        result = await real_request(method, path, body)
        if path == "/stop-boundaries":
            raise TimeoutError("Capture receipt lost")
        return result

    try:
        first = project(store)
        consumer.hub.request = lose_capture
        with pytest.raises(TimeoutError):
            await consumer.cancel_thread("origin")
        saved = dict(consumer.pending_stops["origin"])
        assert saved["phase"] == "uncertain"
        assert status(store, first) == "cancelled"
        boundary = store.get_stop_boundary("alpha", saved["key"])
        # Clocks on the gateway and hub have no ordering relationship.
        store.clock = lambda: first["created"] - 100
        later = project(store)
        await consumer.close()
        reopened = CollaborationConsumer(gateway, clients["alpha"].settings)
        reopened.hub.request = AsyncMock(wraps=reopened.hub.request)
        await reopened.cancel_thread("origin", retry=True)
        assert reopened.hub.request.await_args_list[0].args == (
            "GET",
            "/stop-boundaries/" + saved["key"],
        )
        assert reopened.hub.request.await_args_list[1].args == (
            "POST",
            "/cancel-thread",
            {"thread_id": "origin", "boundary": boundary},
        )
        assert status(store, first) == "cancelled" and status(store, later) == "active"
        assert reopened.pending_stops == {}
    finally:
        await consumer.close()
        if reopened:
            await reopened.close()
        await close_api(store, server, clients)


async def test_uncaptured_stop_refuses_fresh_capture_on_retry(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    real_request = consumer.hub.request
    try:
        first = project(store)
        consumer.hub.request = AsyncMock(side_effect=TimeoutError("Never reached hub"))
        with pytest.raises(TimeoutError):
            await consumer.cancel_thread("origin")
        saved = dict(consumer.pending_stops["origin"])
        later = project(store)
        consumer.hub.request = AsyncMock(wraps=real_request)
        with pytest.raises(HubError) as error:
            await consumer.cancel_thread("origin", retry=True)
        assert error.value.status == 404
        consumer.hub.request.assert_awaited_once_with("GET", "/stop-boundaries/" + saved["key"])
        assert consumer.pending_stops == {"origin": saved}
        assert json.loads(consumer.stops_path.read_text()) == consumer.pending_stops
        assert status(store, first) == status(store, later) == "active"
        # A new explicit stop authorizes a new capture covering current work.
        consumer.hub.request = real_request
        await consumer.cancel_thread("origin")
        assert status(store, first) == status(store, later) == "cancelled"
        assert consumer.pending_stops == {}
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_captured_stop_retries_exact_cancel_receipt_after_restart(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    reopened = None
    real_request = consumer.hub.request

    async def fail_cancel(method, path, body=None):
        if path == "/cancel-thread":
            raise TimeoutError("Cancellation request not received")
        return await real_request(method, path, body)

    try:
        first = project(store)
        consumer.hub.request = fail_cancel
        with pytest.raises(TimeoutError):
            await consumer.cancel_thread("origin")
        saved = consumer.pending_stops["origin"]
        assert saved["phase"] == "captured"
        later = project(store)
        await consumer.close()
        reopened = CollaborationConsumer(gateway, clients["alpha"].settings)
        reopened.hub.request = AsyncMock(wraps=reopened.hub.request)
        await reopened.cancel_thread("origin", retry=True)
        reopened.hub.request.assert_awaited_once_with(
            "POST", "/cancel-thread", {"thread_id": "origin", "boundary": saved["boundary"]}
        )
        assert status(store, first) == "cancelled" and status(store, later) == "active"
    finally:
        await consumer.close()
        if reopened:
            await reopened.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("tamper", ["thread", "order", "key", "actor"])
def test_stop_receipts_are_scoped_and_cannot_be_forged(tmp_path, tamper):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        project(store)
        receipt = store.capture_stop_boundary("alpha", "origin", "recorded")
        old = project(store)
        actor = "beta" if tamper == "actor" else "alpha"
        if tamper == "thread":
            receipt["thread_id"] = "unrelated"
        elif tamper == "order":
            receipt["admission_order"] += 1
        elif tamper == "key":
            receipt["key"] = "not-recorded"
        with pytest.raises((KeyError, ValueError)):
            store.cancel_thread(actor, "origin", boundary=receipt)
        assert status(store, old) == "active"
        with pytest.raises(ValueError, match="Conflicting stop"):
            store.capture_stop_boundary("alpha", "unrelated", "recorded")
    finally:
        store.close()


def test_legacy_cutoff_is_frozen_before_clock_rollback_admission(tmp_path):
    now = [1000.0]
    store = CollaborationStore(
        tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: now[0]
    )
    try:
        first = project(store)
        store.cancel_thread("alpha", "origin", cutoff=999999.0)
        now[0] = 100.0
        intent, _ = store.prepare_creation("alpha", creation_body())
        newer = store.finish_creation("alpha", {**intent, "thread_id": "later"})
        assert status(store, first) == "cancelled" and status(store, newer) == "active"
        assert store.cancel_thread("alpha", "origin", cutoff=999999.0) == []
        assert status(store, newer) == "active"
    finally:
        store.close()


def test_legacy_schema_migrates_stop_and_late_creation_without_future_clock_fence(tmp_path):
    path = tmp_path / "hub.db"
    seed = CollaborationStore(path, {"alpha", "beta", "gamma"}, clock=lambda: 1000.0)
    first = project(seed)
    intent, _ = seed.prepare_creation("alpha", creation_body())
    seed.cancel_thread("alpha", "origin", cutoff=999999.0)
    seed.close()
    with sqlite3.connect(path) as db:
        # Rebuild the pre-order schema from retained data without executing old
        # code or depending on a diagnostics-only fixture path.
        for table, columns in (
            ("projects", "id,data,status,deadline,created,owner,origin_thread,channel,thread"),
            ("creations", "actor,key,data,channel,thread,unresolved"),
            ("origin_stops", "actor,thread,cutoff"),
        ):
            db.execute(f"CREATE TABLE legacy_{table} AS SELECT {columns} FROM {table}")
            db.execute(f"DROP TABLE {table}")
            db.execute(f"ALTER TABLE legacy_{table} RENAME TO {table}")
        db.execute("CREATE UNIQUE INDEX legacy_project_id ON projects(id)")
        db.execute("CREATE UNIQUE INDEX legacy_creation_key ON creations(actor,key)")
        db.execute("CREATE UNIQUE INDEX legacy_origin_stop ON origin_stops(actor,thread)")
        db.execute("DROP TABLE admission_sequence")
        db.execute("DROP TABLE stop_boundaries")
    store = CollaborationStore(path, {"alpha", "beta", "gamma"}, clock=lambda: 100.0)
    try:
        assert status(store, first) == "cancelled"
        early = store.finish_creation("alpha", {**intent, "thread_id": "early"})
        assert early["status"] == "cancelled"
        newer, _ = store.prepare_creation("alpha", {**creation_body(), "key": "new-key"})
        later = store.finish_creation("alpha", {**newer, "thread_id": "later"})
        assert later["status"] == "active"
        assert store.cancel_thread("alpha", "origin", cutoff=999999.0) == []
        assert status(store, later) == "active"
    finally:
        store.close()


def test_admission_sequence_is_shared_and_durable_across_store_connections(tmp_path):
    path = tmp_path / "hub.db"
    actors = {"alpha", "beta", "gamma"}
    first = CollaborationStore(path, actors)
    second = CollaborationStore(path, actors)
    try:
        old = project(first)
        receipt = second.capture_stop_boundary("alpha", "origin", "capture")
        newer = project(first)
        assert second.cancel_thread("alpha", "origin", boundary=receipt) == []
        assert status(first, old) == "cancelled" and status(first, newer) == "active"
        orders = [
            r[0]
            for r in first.db.execute(
                "SELECT admission_order FROM projects ORDER BY admission_order"
            )
        ]
        assert orders[0] < orders[1]
    finally:
        first.close()
        second.close()


async def test_restart_before_first_capture_cannot_automatically_widen_stop(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    reopened = None
    try:
        first = project(store)
        recorded = consumer.record_stop("origin")
        await consumer.close()
        later = project(store)
        reopened = CollaborationConsumer(gateway, clients["alpha"].settings)
        reopened.hub.request = AsyncMock(wraps=reopened.hub.request)
        with pytest.raises(HubError) as error:
            await reopened.cancel_thread("origin", retry=True)
        assert error.value.status == 404
        reopened.hub.request.assert_awaited_once_with("GET", "/stop-boundaries/" + recorded["key"])
        assert reopened.pending_stops == {"origin": recorded}
        assert status(store, first) == status(store, later) == "active"
    finally:
        await consumer.close()
        if reopened:
            await reopened.close()
        await close_api(store, server, clients)


async def test_superseded_uncaptured_control_refuses_new_capture(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    try:
        old = consumer.record_stop("origin")
        newer = consumer.record_stop("origin")
        consumer.hub.request = AsyncMock(wraps=consumer.hub.request)
        with pytest.raises(ValueError, match="superseded"):
            await consumer.cancel_thread("origin", retry=True, cutoff=old)
        consumer.hub.request.assert_not_awaited()
        assert consumer.pending_stops == {"origin": newer}
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_duplicate_first_control_reconciles_same_inflight_capture(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    real_request = consumer.hub.request
    captured, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def delayed_receipt(method, path, body=None):
        calls.append((method, path, body))
        result = await real_request(method, path, body)
        if path == "/stop-boundaries":
            captured.set()
            await release.wait()
        return result

    task = None
    try:
        first = project(store)
        recorded = consumer.record_stop("origin")
        consumer.hub.request = delayed_receipt
        task = asyncio.create_task(consumer.cancel_thread("origin", retry=True, cutoff=recorded))
        await asyncio.wait_for(captured.wait(), 2)
        newer = project(store)
        # The second control still holds the original phase='new' object.
        await consumer.cancel_thread("origin", retry=True, cutoff=recorded)
        assert len([c for c in calls if c[:2] == ("POST", "/stop-boundaries")]) == 1
        assert any(c[:2] == ("GET", "/stop-boundaries/" + recorded["key"]) for c in calls)
        release.set()
        await asyncio.wait_for(task, 2)
        assert status(store, first) == "cancelled" and status(store, newer) == "active"
        assert consumer.pending_stops == {}
    finally:
        release.set()
        if task and not task.done():
            task.cancel()
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("invalid", [None, [], True, 1, "receipt", "bool-order", "float-order"])
async def test_invalid_http_boundary_cannot_fall_through_to_current_stop(tmp_path, invalid):
    store, server, clients = await api_fixture(tmp_path)
    try:
        project(store)
        receipt = store.capture_stop_boundary("alpha", "origin", "recorded")
        first = project(store)
        assert receipt["admission_order"] == 1
        if invalid == "bool-order":
            boundary = {**receipt, "admission_order": True}
        elif invalid == "float-order":
            boundary = {**receipt, "admission_order": 1.0}
        else:
            boundary = invalid
        with pytest.raises(HubError) as error:
            await clients["alpha"].request(
                "POST", "/cancel-thread", {"thread_id": "origin", "boundary": boundary}
            )
        assert error.value.status == 409
        assert status(store, first) == "active"
        assert store.db.execute("SELECT admission_order FROM origin_stops").fetchone()[0] == 1
    finally:
        await close_api(store, server, clients)


async def test_captured_older_stop_survives_newer_uncaptured_local_intent(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    reopened = None
    real_request = consumer.hub.request

    async def fail_cancel(method, path, body=None):
        if path == "/cancel-thread":
            raise TimeoutError("Cancellation receipt unavailable")
        return await real_request(method, path, body)

    try:
        first = project(store)
        pending, _ = store.prepare_creation("alpha", creation_body())
        consumer.hub.request = fail_cancel
        with pytest.raises(TimeoutError):
            await consumer.cancel_thread("origin")
        old_stop = dict(consumer.pending_stops["origin"])
        assert old_stop["phase"] == "captured" and status(store, first) == "cancelled"
        later = project(store)
        consumer.hub.request = AsyncMock(side_effect=TimeoutError("Never reached hub"))
        with pytest.raises(TimeoutError):
            await consumer.cancel_thread("origin")
        newer_stop = dict(consumer.pending_stops["origin"])
        assert newer_stop["key"] != old_stop["key"] and newer_stop["phase"] == "uncertain"
        # The older hub intent is already committed, even though its local
        # retry has been superseded and discussion posting finishes later.
        late_finish = store.finish_creation("alpha", {**pending, "thread_id": "late-root"})
        assert late_finish["status"] == "cancelled"
        assert store.known_discussion("floor", "late-root")
        await consumer.close()
        reopened = CollaborationConsumer(gateway, clients["alpha"].settings)
        with pytest.raises(HubError) as error:
            await reopened.cancel_thread("origin", retry=True)
        assert error.value.status == 404
        assert reopened.pending_stops == {"origin": newer_stop}
        assert store.get_stop_boundary("alpha", old_stop["key"]) == old_stop["boundary"]
        assert status(store, later) == "active"
    finally:
        await consumer.close()
        if reopened:
            await reopened.close()
        await close_api(store, server, clients)


def test_stop_capture_and_cancellation_roll_back_together(tmp_path, monkeypatch):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        first = project(store)
        actual_cancel = store.cancel

        def failed_cancel(*args, **kwargs):
            actual_cancel(*args, **kwargs)
            raise RuntimeError("Synthetic failure after cancellation write")

        with monkeypatch.context() as patch:
            patch.setattr(store, "cancel", failed_cancel)
            with pytest.raises(RuntimeError, match="after cancellation write"):
                store.capture_stop_boundary("alpha", "origin", "atomic")
        with pytest.raises(KeyError):
            store.get_stop_boundary("alpha", "atomic")
        assert store.db.execute("SELECT COUNT(*) FROM origin_stops").fetchone()[0] == 0
        assert status(store, first) == "active"
        receipt = store.capture_stop_boundary("alpha", "origin", "atomic")
        assert status(store, first) == "cancelled"
        assert store.capture_stop_boundary("alpha", "origin", "atomic") == receipt
    finally:
        store.close()


def test_legacy_boundary_uses_metadata_without_decoding_project_history(tmp_path, monkeypatch):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: 100.0)
    try:
        first = project(store)
        intent, _ = store.prepare_creation("alpha", creation_body())

        def no_history_decode(_raw):
            raise AssertionError("Boundary selection decoded retained project JSON")

        with monkeypatch.context() as patch:
            patch.setattr(store_module.json, "loads", no_history_decode)
            receipt = store.legacy_stop_boundary("alpha", "origin", 101.0)
            assert store.legacy_stop_boundary("alpha", "origin", 101.0) == receipt
        assert receipt["admission_order"] == intent["admission_order"]
        assert status(store, first) == "active"
    finally:
        store.close()
