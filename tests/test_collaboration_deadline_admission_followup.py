"""Suspension, indexed admission and evidence-based deadline retirement."""

import asyncio
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_elapsed_deadlines import open_store
from test_collaboration_regressions import close_creation, creation_api, creation_body

import mattermost_acp.collaboration.client as client_module
import mattermost_acp.collaboration.consumer as consumer_module
import mattermost_acp.collaboration.store as store_module
from mattermost_acp.acp.broker import NativeDispatchChanged
from mattermost_acp.collaboration.cli import execute, parser
from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.store import CollaborationStore


def clock_fixture(monkeypatch):
    elapsed, monotonic = [100.0], [100.0]
    fake_time = SimpleNamespace(
        CLOCK_BOOTTIME=99,
        clock_gettime=lambda _clock: elapsed[0],
        monotonic=lambda: monotonic[0],
    )
    monkeypatch.setattr(client_module, "time", fake_time)
    monkeypatch.setattr(consumer_module, "time", fake_time, raising=False)
    monkeypatch.setattr(consumer_module, "elapsed_clock", lambda: elapsed[0], raising=False)
    return elapsed, monotonic


async def test_suspension_during_receipt_blocks_native_writes_without_hub(tmp_path, monkeypatch):
    elapsed, monotonic = clock_fixture(monkeypatch)

    async def response(_request):
        elapsed[0] += 1000
        return web.json_response({"project": "project", "project_remaining_seconds": 10})

    app = web.Application()
    app.router.add_get("/receipt", response)
    server = TestServer(app)
    await server.start_server()
    gateway = active_gateway()
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(
        gateway, Settings(str(server.make_url("")).rstrip("/"), "synthetic" * 5)
    )
    writes = []

    async def operation():
        writes.append("native prompt")

    try:
        receipt = await consumer.hub.request("GET", "/receipt")
        assert monotonic[0] == 100 and elapsed[0] == 1100
        consumer.hub.request = AsyncMock(side_effect=HubError("Hub unavailable", status=503))
        with pytest.raises(NativeDispatchChanged):
            await consumer.deadline_work(receipt, operation())
        assert writes == [] and not consumer.before_deadline(receipt)
    finally:
        await consumer.close()
        await server.close()


async def test_suspension_deadline_timer_rechecks_and_stops_only_owned_work(tmp_path, monkeypatch):
    elapsed, monotonic = clock_fixture(monkeypatch)
    real_sleep = asyncio.sleep
    sleeps = []

    async def suspended_sleep(delay):
        sleeps.append(delay)
        assert delay <= 1, "Deadline timer must recheck within one second after resume"
        elapsed[0] += 4000
        await real_sleep(0)

    gateway = active_gateway()
    gateway.config.db_path = tmp_path / "gateway.db"
    gateway.sessions.native_epoch = lambda _thread: 7
    consumer = CollaborationConsumer(gateway, Settings("http://127.0.0.1:1", "synthetic" * 5))
    item = {
        "project": "owned-project",
        "binding": {"thread_id": "discussion"},
        "reservation_evidence": {
            "session_id": "native",
            "execution_id": "execution",
            "native_epoch": 7,
        },
    }
    report = {
        "execution_id": "execution",
        "collaboration_project": "owned-project",
        "native_dispatched": True,
        "reporter": SimpleNamespace(),
    }
    key = ("request", "claim")
    consumer.ownership[key] = report
    consumer.ownership_items[key] = item
    gateway._reports["native"] = report
    unrelated = {"execution_id": "other", "collaboration_project": "unrelated"}
    consumer.ownership[("other", "claim")] = unrelated
    consumer.ownership_items[("other", "claim")] = {"project": "unrelated"}
    consumer.project_deadlines["owned-project"] = elapsed[0] + 3600
    consumer.hub.request = AsyncMock(side_effect=HubError("Hub unavailable", status=503))
    monkeypatch.setattr(asyncio, "sleep", suspended_sleep)
    try:
        await consumer.enforce_deadline("owned-project")
        assert sleeps and monotonic[0] == 100 and elapsed[0] == 4100
        gateway.sessions.cancel.assert_awaited_once()
        call = gateway.sessions.cancel.await_args
        assert call.args == ("discussion",)
        assert call.kwargs["expected_session"] == "native"
        assert call.kwargs["expected_epoch"] == 7 and call.kwargs["guard"]()
        assert report["cancel_requested"] and not unrelated.get("cancel_requested")
        assert consumer.ownership[key] is report
    finally:
        await consumer.close()


def body(thread="discussion", key="new-project", owner="alpha", members=None):
    return {
        "title": "Build investigation",
        "objective": "Find the dependency",
        "members": members or ["alpha", "beta", "gamma"],
        "channel_id": "floor",
        "thread_id": thread,
        "origin_thread_id": owner + "-origin",
        "origin_channel_id": "home",
        "key": key,
    }


@pytest.mark.parametrize("action", ["create", "prepare", "rebind"])
def test_binding_admission_expires_idle_projects_in_its_transaction(tmp_path, action):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        old = store.create_project("alpha", {**body(key="old"), "limits": {"max_seconds": 10}})
        queued = ask(store, old["id"])
        if action == "rebind":
            new = store.create_project("alpha", body("another-discussion", "other"))
        wall[0], elapsed[0] = 80.0, 61.0
        if action == "create":
            new = store.create_project("alpha", body())
            assert new["thread_id"] == "discussion"
        elif action == "prepare":
            intent, fresh = store.prepare_creation("alpha", body())
            assert fresh and intent["thread_id"] == "discussion"
        else:
            binding = store.bind(
                new["id"], "beta", {"thread_id": "discussion", "channel_id": "floor"}
            )
            assert binding["thread_id"] == "discussion"
        assert store.get_project(old["id"], "alpha")["status"] == "cancelled"
        assert store.get_message(queued["id"], "alpha")["state"] == "cancelled"
    finally:
        store.close()


@pytest.mark.parametrize("action", ["create", "rebind"])
def test_expired_native_execution_still_conflicts_with_binding_admission(tmp_path, action):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        old = store.create_project("alpha", {**body(key="old"), "limits": {"max_seconds": 10}})
        ask(store, old["id"])
        claim = store.claim_next("beta", "worker")
        proof = {"session_id": "native", "execution_id": "execution", "native_epoch": 7}
        store.update(
            claim["id"], "beta", "worker", "dispatch", evidence=proof, claim=claim["claim"]
        )
        if action == "rebind":
            new = store.create_project("alpha", body("another-discussion", "other"))
        wall[0], elapsed[0] = 80.0, 61.0
        with pytest.raises(ValueError, match="binding"):
            if action == "create":
                store.create_project("alpha", body())
            else:
                store.bind(new["id"], "beta", {"thread_id": "discussion", "channel_id": "floor"})
        retained = store.get_message(claim["id"], "alpha")
        assert retained["execution_pending"] and retained["evidence"]["execution_id"] == "execution"
        assert store.get_project(old["id"], "alpha")["status"] != "cancelled"
    finally:
        store.close()


def retain_history(store, count=80):
    history = set()
    owner = "gamma" if "gamma" in store.actors else "beta"
    for index in range(count):
        p = store.create_project(
            owner, body("history-" + str(index), "history-" + str(index), owner, [owner])
        )
        store.cancel(p["id"], owner)
        history.add(p["id"])
    return history


def reject_history_decode(monkeypatch, history):
    loads, dumps = json.loads, json.dumps
    decoded = []

    def bounded_loads(raw):
        result = loads(raw)
        if isinstance(result, dict) and result.get("id") in history:
            raise AssertionError("Admission decoded unrelated settled project history")
        decoded.append(result)
        return result

    monkeypatch.setattr(store_module, "json", SimpleNamespace(loads=bounded_loads, dumps=dumps))
    return decoded


@pytest.mark.parametrize("action", ["create", "repeat", "prepare", "bind"])
def test_admission_does_not_decode_unrelated_settled_project_history(tmp_path, monkeypatch, action):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        history = retain_history(store)
        current = store.create_project("alpha", body())
        reject_history_decode(monkeypatch, history)
        if action == "create":
            store.create_project("alpha", body("new-discussion", "second"))
        elif action == "repeat":
            assert store.create_project("alpha", body())["id"] == current["id"]
        elif action == "prepare":
            assert store.prepare_creation("alpha", body("new-discussion", "second"))[1]
        else:
            store.bind(current["id"], "beta", {"thread_id": "worker-root", "channel_id": "floor"})
    finally:
        store.close()


def test_legacy_json_projects_migrate_creator_keys_and_native_bindings(tmp_path):
    wall, elapsed = [100.0], [50.0]
    source = open_store(tmp_path / "source.db", wall, elapsed)
    owned = source.create_project("alpha", body(key="same"))
    member = source.create_project("beta", body("other-discussion", "same", "beta"))
    source.close()
    path = tmp_path / "legacy.db"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE projects(id TEXT PRIMARY KEY,data TEXT NOT NULL)")
    for p in (owned, member):
        db.execute("INSERT INTO projects VALUES(?,?)", (p["id"], json.dumps(p)))
    db.commit()
    db.close()
    store = open_store(path, wall, elapsed)
    try:
        assert store.create_project("alpha", body(key="same"))["id"] == owned["id"]
        assert (
            store.create_project("beta", body("other-discussion", "same", "beta"))["id"]
            == member["id"]
        )
        rows = store.db.execute("SELECT owner,creation_key FROM projects ORDER BY owner").fetchall()
        assert [tuple(row) for row in rows] == [("alpha", "same"), ("beta", "same")]
        assert store.db.execute("SELECT COUNT(*) FROM project_bindings").fetchone()[0] == 6
        plans = [
            store.db.execute(
                "EXPLAIN QUERY PLAN SELECT data FROM projects WHERE owner=? AND creation_key=?",
                ("alpha", "same"),
            ).fetchall(),
            store.db.execute(
                "EXPLAIN QUERY PLAN SELECT project FROM project_bindings "
                "WHERE actor=? AND thread=?",
                ("beta", "discussion"),
            ).fetchall(),
        ]
        assert "project_creation_key" in str([tuple(row) for row in plans[0]])
        assert "native_binding" in str([tuple(row) for row in plans[1]])
        with pytest.raises(ValueError, match="binding"):
            store.bind(member["id"], "beta", {"thread_id": "discussion", "channel_id": "floor"})
    finally:
        store.close()


def cli_args(key="same-project"):
    return parser().parse_args(
        [
            "create",
            "--title",
            "Build investigation",
            "--objective",
            "Verify the dependency",
            "--members",
            "alpha,beta",
            "--origin-thread",
            "origin",
            "--origin-channel",
            "home",
            "--key",
            key,
        ]
    )


async def test_cli_creation_uses_keyed_evidence_and_retains_idempotency(tmp_path, monkeypatch):
    post = AsyncMock(return_value={"id": "created-root", "channel_id": "floor"})
    store, server, client = await creation_api(tmp_path / "hub.db", post)
    monkeypatch.setenv("MATTERMOST_PEER_CHANNEL_ID", "floor")
    try:
        history = retain_history(store)
        reject_history_decode(monkeypatch, history)
        args = cli_args("same/key # encoded")
        calls = []
        request = client.request

        async def tracked(method, path, data=None):
            calls.append((method, path))
            return await request(method, path, data)

        client.request = tracked
        first = await execute(args, client)
        assert (await execute(args, client))["id"] == first["id"]
        assert all(path != "/projects" for _method, path in calls)
        assert ("GET", "/creations/same%2Fkey%20%23%20encoded") in calls
        post.assert_awaited_once()
        args.objective = "Changed objective"
        with pytest.raises(ValueError, match="Conflicting project creation key"):
            await execute(args, client)
    finally:
        await close_creation(store, server, client)


async def test_cli_key_lookup_preserves_legacy_direct_project_creation(tmp_path, monkeypatch):
    post = AsyncMock()
    store, server, client = await creation_api(tmp_path / "hub.db", post)
    monkeypatch.setenv("MATTERMOST_PEER_CHANNEL_ID", "floor")
    try:
        legacy = store.create_project("alpha", {**creation_body(), "thread_id": "legacy-root"})
        args = cli_args("creation-key")
        args.title = legacy["title"]
        assert (await execute(args, client))["id"] == legacy["id"]
        post.assert_not_awaited()
        with pytest.raises(KeyError):
            store.get_creation("alpha", "creation-key")
    finally:
        await close_creation(store, server, client)


async def consumer_fixture(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "gateway.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    consumer.recovery_pending = False
    return store, server, clients, consumer


async def test_settled_closed_project_evicts_all_deadline_state_after_last_owner(tmp_path):
    store, server, clients, consumer = await consumer_fixture(tmp_path)
    key = ("request", "claim")
    pid = "project"
    item = {"project": pid, "project_remaining_seconds": 60}
    consumer.observe_deadline(item)
    consumer.ownership[key] = {}
    consumer.ownership_items[key] = item
    consumer.hub.deadlines[pid] = consumer.project_deadlines[pid]
    timer = consumer.deadline_tasks[pid]
    try:
        consumer.observe_deadline(
            {
                **item,
                "project_admission_closed": True,
                "project_execution_pending": False,
            }
        )
        assert pid in consumer.project_deadlines
        consumer.release_ownership(key)
        assert pid not in consumer.project_deadlines
        assert pid not in consumer.deadline_tasks and pid not in consumer.hub.deadlines
        assert timer.cancelling() or timer.done()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("reason", ["active", "unknown", "debt", "inherited", "worker", "recovery"])
async def test_deadline_eviction_preserves_active_and_uncertain_budgets(tmp_path, reason):
    store, server, clients, consumer = await consumer_fixture(tmp_path)
    pid = "project"
    item = {"project": pid, "project_remaining_seconds": 60}
    consumer.observe_deadline(item)
    consumer.hub.deadlines[pid] = consumer.project_deadlines[pid]
    receipt = {**item, "project_admission_closed": True, "project_execution_pending": False}
    if reason == "active":
        receipt["project_admission_closed"] = False
    elif reason == "unknown":
        receipt.pop("project_execution_pending")
    elif reason == "debt":
        receipt["project_execution_pending"] = True
    elif reason == "inherited":
        consumer.inherited_claims = [{"project": pid}]
    elif reason == "worker":
        if not hasattr(consumer, "handling"):
            consumer.handling = {}
        consumer.handling[("request", "claim")] = pid
    else:
        consumer.recovery_pending = True
    try:
        consumer.observe_deadline(receipt)
        assert pid in consumer.project_deadlines
        assert pid in consumer.deadline_tasks and pid in consumer.hub.deadlines
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_idle_active_budget_keeps_its_expiry_timer_between_requests(tmp_path):
    store, server, clients, consumer = await consumer_fixture(tmp_path)
    item = {"project": "project", "project_remaining_seconds": 60}
    key = ("request", "claim")
    consumer.observe_deadline(item)
    consumer.ownership[key] = {}
    consumer.ownership_items[key] = item
    timer = consumer.deadline_tasks["project"]
    try:
        consumer.release_ownership(key)
        assert "project" in consumer.project_deadlines
        assert not timer.cancelling() and not timer.done()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_expired_idle_budget_is_evicted_after_authoritative_settlement(tmp_path):
    store, server, clients, consumer = await consumer_fixture(tmp_path)
    try:
        p = project(store)
        message = ask(store, p["id"])
        receipt = await consumer.hub.request("GET", "/messages/" + message["id"])
        consumer.observe_deadline(receipt)
        consumer.project_deadlines[p["id"]] = -1
        store.clock = lambda: p["deadline"] + 1
        await consumer.enforce_deadline(p["id"])
        assert p["id"] not in consumer.project_deadlines
        assert p["id"] not in consumer.deadline_tasks and p["id"] not in consumer.hub.deadlines
        assert store.get_project(p["id"], "alpha")["status"] == "cancelled"
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_hub_outage_retains_expired_budget_until_reconciliation(tmp_path):
    store, server, clients, consumer = await consumer_fixture(tmp_path)
    try:
        p = project(store)
        message = ask(store, p["id"])
        receipt = await consumer.hub.request("GET", "/messages/" + message["id"])
        consumer.observe_deadline(receipt)
        timer = consumer.deadline_tasks[p["id"]]
        timer.cancel()
        await asyncio.gather(timer, return_exceptions=True)
        consumer.project_deadlines[p["id"]] = -1
        store.clock = lambda: p["deadline"] + 1
        request = consumer.hub.request
        consumer.hub.request = AsyncMock(side_effect=HubError("Hub unavailable", status=503))
        await consumer.enforce_deadline(p["id"])
        assert p["id"] in consumer.project_deadlines
        consumer.hub.request = request
        await consumer.reconcile_deadlines()
        assert p["id"] not in consumer.project_deadlines
        assert p["id"] not in consumer.hub.deadlines
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_client_evicts_only_authoritatively_settled_project_deadlines():
    receipts = [
        {"project": "project", "project_remaining_seconds": 60},
        {
            "project": "project",
            "project_remaining_seconds": 0,
            "project_admission_closed": True,
            "project_execution_pending": True,
        },
        {
            "project": "project",
            "project_remaining_seconds": 0,
            "project_admission_closed": True,
            "project_execution_pending": False,
        },
    ]

    async def response(_request):
        return web.json_response(receipts.pop(0))

    app = web.Application()
    app.router.add_get("/receipt", response)
    server = TestServer(app)
    await server.start_server()
    client = HubClient(Settings(str(server.make_url("")).rstrip("/"), "synthetic" * 5))
    try:
        await client.request("GET", "/receipt")
        assert "project" in client.deadlines
        await client.request("GET", "/receipt")
        assert "project" in client.deadlines
        closed = await client.request("GET", "/receipt")
        assert "project" not in client.deadlines
        assert closed["project_admission_closed"]
    finally:
        await client.close()
        await server.close()


def test_binding_index_tracks_rebinding_and_same_owner_origin_exception(tmp_path, monkeypatch):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        history = retain_history(store)
        original = store.create_project("alpha", body(members=["alpha", "beta"]))
        reject_history_decode(monkeypatch, history)
        store.bind(original["id"], "beta", {"thread_id": "worker-root", "channel_id": "floor"})
        replacement = store.create_project(
            "alpha", body(key="replacement", members=["alpha", "beta"])
        )
        assert replacement["id"] != original["id"]
        assert replacement["bindings"]["alpha"] == original["bindings"]["alpha"]
        with pytest.raises(ValueError, match="binding"):
            store.bind(
                replacement["id"], "beta", {"thread_id": "worker-root", "channel_id": "floor"}
            )
    finally:
        store.close()


async def test_creation_key_evidence_is_owner_scoped_when_actor_is_only_a_member(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    try:
        member_project = store.create_project(
            "beta", body("other-discussion", "same", "beta", ["alpha", "beta"])
        )
        evidence = await clients["alpha"].request("GET", "/creations/same")
        assert evidence == {"project": None, "creation": None}
        evidence = await clients["beta"].request("GET", "/creations/same")
        assert evidence["project"]["id"] == member_project["id"]
    finally:
        await close_api(store, server, clients)


async def test_project_budget_read_avoids_settled_history_and_preserves_execution_debt(
    tmp_path, monkeypatch
):
    store, server, clients = await api_fixture(tmp_path)
    try:
        history = retain_history(store)
        p = project(store)
        ask(store, p["id"])
        claim = store.claim_next("beta", "worker")
        proof = {"session_id": "native", "execution_id": "execution", "native_epoch": 7}
        store.update(
            claim["id"], "beta", "worker", "dispatch", evidence=proof, claim=claim["claim"]
        )
        store.cancel(p["id"], "alpha")
        reject_history_decode(monkeypatch, history)
        budget = await clients["beta"].request("GET", f"/projects/{p['id']}/budget")
        assert budget["project_admission_closed"] and budget["project_execution_pending"]
        assert store.get_message(claim["id"], "beta")["execution_pending"]
    finally:
        await close_api(store, server, clients)


async def test_deadline_cleanup_is_bounded_and_preserves_uncertain_projects(tmp_path):
    store, server, clients, consumer = await consumer_fixture(tmp_path)
    calls = []

    async def budget(_method, path):
        pid = path.split("/")[2]
        calls.append(pid)
        return {
            "project": pid,
            "project_remaining_seconds": 0,
            "project_admission_closed": True,
            "project_execution_pending": pid in {"project-0", "project-1"},
        }

    consumer.hub.request = budget
    consumer.project_deadlines = {"project-" + str(index): -1 for index in range(8)}
    try:
        await consumer.reconcile_deadlines()
        assert len(calls) == 4
        await consumer.reconcile_deadlines()
        assert len(calls) == 8
        assert set(consumer.project_deadlines) == {"project-0", "project-1"}
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_claim_recovery_tracks_budget_for_eventual_cache_eviction(tmp_path):
    store, server, clients, consumer = await consumer_fixture(tmp_path)
    try:
        p = project(store)
        ask(store, p["id"])
        claim = store.claim_next("beta", "predecessor")
        proof = {"session_id": "native", "execution_id": "execution", "native_epoch": 7}
        store.update(
            claim["id"], "beta", "predecessor", "dispatch", evidence=proof, claim=claim["claim"]
        )
        await consumer.reconcile_inherited_claims()
        assert p["id"] in consumer.project_deadlines
        assert p["id"] in consumer.hub.deadlines and len(consumer.inherited_claims) == 1
        store.reply(claim["id"], "beta", "Verified result", claim=claim["claim"])
        store.update(
            claim["id"],
            "beta",
            "predecessor",
            "execution_settled",
            claim=claim["claim"],
            evidence={
                **proof,
                "native_dispatched": True,
                "pending": False,
                "stop_reason": "end_turn",
                "completed_at": store.clock() + 1,
            },
        )
        store.cancel(p["id"], "alpha")
        await consumer.reconcile_inherited_claims()
        consumer.project_deadlines[p["id"]] = -1
        timer = consumer.deadline_tasks[p["id"]]
        timer.cancel()
        await asyncio.gather(timer, return_exceptions=True)
        await consumer.reconcile_deadlines()
        assert p["id"] not in consumer.project_deadlines
        assert p["id"] not in consumer.hub.deadlines and consumer.inherited_claims == []
    finally:
        await consumer.close()
        await close_api(store, server, clients)
