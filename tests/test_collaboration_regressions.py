"""Race and recovery contracts use synthetic adapters, actors and local HTTP servers."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer, make_mocked_request
from test_collaboration import (
    active_gateway,
    api_fixture,
    ask,
    close_api,
    project,
)

from mattermost_acp.acp import AcpBroker, TurnResult
from mattermost_acp.acp.broker import NativeDispatchChanged
from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.mm.http import build_slash_app
from mattermost_acp.session_service import SessionService


class MemoryAdapter:
    def __init__(self, broker):
        self.broker = broker
        self.messages = []

    def write(self, payload):
        message = json.loads(payload)
        self.messages.append(message)
        if "id" in message:
            self.broker._pending[message["id"]].set_result({"outcome": "injected"})

    async def drain(self):
        return None


def memory_broker():
    broker = AcpBroker(backend="claude")
    writer = MemoryAdapter(broker)
    broker._proc = SimpleNamespace(stdin=writer, returncode=None)
    broker._capabilities.steering = True
    return broker, writer


async def control(broker, kind, *, guard):
    if kind == "cancel":
        return await broker.cancel("native", guard=guard)
    return await broker.steer("native", "Useful guidance", guard=guard)


@pytest.mark.parametrize("kind", ["cancel", "steer"])
async def test_native_control_rechecks_guard_after_waiting_for_write_lock(kind):
    broker, writer = memory_broker()
    allowed = True
    async with broker._write_lock:
        pending = asyncio.create_task(control(broker, kind, guard=lambda: allowed))
        await asyncio.sleep(0)
        assert not pending.done()
        allowed = False
    with pytest.raises(NativeDispatchChanged):
        await pending
    assert writer.messages == []
    assert broker._pending == {}


@pytest.mark.parametrize("kind", ["cancel", "steer"])
async def test_native_control_with_unchanged_guard_reaches_only_expected_session(kind):
    broker, writer = memory_broker()
    await control(broker, kind, guard=lambda: True)
    assert len(writer.messages) == 1
    assert writer.messages[0]["params"]["sessionId"] == "native"
    assert writer.messages[0]["method"] == (
        "session/cancel" if kind == "cancel" else "_session/steering"
    )


@pytest.mark.parametrize("kind", ["cancel", "hint"])
@pytest.mark.parametrize("replacement", ["new_session", "turn"])
async def test_session_control_epoch_rejects_new_work_before_native_write(
    tmp_path, kind, replacement
):
    broker, writer = memory_broker()
    store = SimpleNamespace(
        get=AsyncMock(
            return_value=SimpleNamespace(session_id="native", cwd=str(tmp_path), channel_id="floor")
        ),
        model_preference=AsyncMock(return_value=(None, None, None)),
    )
    sessions = SessionService(broker, store, default_cwd=tmp_path)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def stall(*args, **kwargs):
        entered.set()
        await release.wait()
        return "replacement-native"

    broker.start = stall
    broker.session_close = AsyncMock()
    broker.prompt_turn = AsyncMock(
        return_value=TurnResult(
            session_id="replacement-native",
            stop_reason="end_turn",
            answer_text="done",
            thought_text="",
        )
    )
    sessions.ensure_session = stall
    sessions._create_and_pin = AsyncMock(return_value="replacement-native")
    async with broker._write_lock:
        kwargs = {"expected_session": "native", "expected_epoch": sessions.native_epoch("thread")}
        pending = asyncio.create_task(
            sessions.cancel("thread", **kwargs)
            if kind == "cancel"
            else sessions.hint("thread", "Guidance", **kwargs)
        )
        await asyncio.sleep(0)
        newer = asyncio.create_task(
            sessions.new_session("thread")
            if replacement == "new_session"
            else sessions.run_turn("thread", "New work")
        )
        await asyncio.wait_for(entered.wait(), 1)
    try:
        with pytest.raises(NativeDispatchChanged):
            await pending
        assert writer.messages == []
    finally:
        release.set()
        await newer


@pytest.mark.parametrize("kind", ["cancel", "hint"])
async def test_session_control_rejects_changed_session_association(tmp_path, kind):
    broker, writer = memory_broker()
    sessions = SessionService(
        broker,
        SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(session_id="replacement"))),
        default_cwd=tmp_path,
    )
    kwargs = {"expected_session": "native", "expected_epoch": 0}
    with pytest.raises(NativeDispatchChanged):
        if kind == "cancel":
            await sessions.cancel("thread", **kwargs)
        else:
            await sessions.hint("thread", "Guidance", **kwargs)
    assert writer.messages == []


def maintenance_gateway():
    gateway = active_gateway(busy=False)
    gateway.config.slash_port = 0
    gateway.config.slash_tokens = frozenset({"synthetic-slash-token"})
    gateway.sessions.busy_threads = set()
    gateway.sessions.broker._pending = {}
    gateway.collaboration = SimpleNamespace(
        hub=SimpleNamespace(settings=SimpleNamespace(token="synthetic-maintenance-token")),
        workers={},
    )
    return gateway


async def maintenance_client(gateway):
    client = TestClient(TestServer(build_slash_app(gateway.config, gateway.handle_slash)))
    await client.start_server()
    return client


def auth():
    return {"Authorization": "Bearer synthetic-maintenance-token"}


async def test_maintenance_requires_actor_credential_and_loopback():
    gateway = maintenance_gateway()
    client = await maintenance_client(gateway)
    try:
        response = await client.post("/maintenance", json={"action": "drain"})
        assert response.status == 401
        assert not gateway.draining
        response = await client.post(
            "/maintenance", json={"action": "drain"}, headers={"Authorization": "Bearer wrong"}
        )
        assert response.status == 401
        handler = next(
            route.handler
            for route in client.server.app.router.routes()
            if route.resource.canonical == "/maintenance"
        )
        transport = SimpleNamespace(
            get_extra_info=lambda name, default=None: (
                ("198.51.100.7", 1234) if name == "peername" else default
            )
        )
        request = make_mocked_request("POST", "/maintenance", headers=auth(), transport=transport)
        with pytest.raises(web.HTTPUnauthorized):
            await handler(request)
        assert not gateway.draining
    finally:
        await client.close()


@pytest.mark.parametrize(
    "pending",
    ["foreground", "responses", "inbound", "commands", "native_rpc", "background", "collaboration"],
)
async def test_maintenance_seal_rejects_each_pending_work_class(pending):
    gateway = maintenance_gateway()
    lock = asyncio.Lock()
    if pending == "foreground":
        gateway.sessions.busy_threads.add("native-thread")
    elif pending == "responses":
        await lock.acquire()
        gateway._prompt_locks["native-thread"] = lock
    elif pending == "inbound":
        gateway._inbound_tasks.add("inbound")
    elif pending == "commands":
        gateway._active_controls = 1
    elif pending == "native_rpc":
        gateway.sessions.broker._pending[1] = "pending-rpc"
    elif pending == "background":
        gateway._reports["native"] = {
            "active": False,
            "reporter": SimpleNamespace(has_pending_work=lambda: True),
        }
    else:
        gateway.collaboration.workers["request"] = "worker"
    client = await maintenance_client(gateway)
    try:
        drained = await client.post("/maintenance", json={"action": "drain"}, headers=auth())
        state = await drained.json()
        assert state["draining"] and not state["idle"] and state[pending] > 0
        sealed = await client.post("/maintenance", json={"action": "seal"}, headers=auth())
        assert sealed.status == 409
        assert not gateway.sealed
    finally:
        if lock.locked():
            lock.release()
        await client.close()


async def test_sealed_maintenance_blocks_stop_and_resume_reopens_controls():
    gateway = maintenance_gateway()
    gateway._dispatch_command = AsyncMock()
    client = await maintenance_client(gateway)
    try:
        rejected = await client.post("/maintenance", json={"action": "seal"}, headers=auth())
        assert rejected.status == 409
        await client.post("/maintenance", json={"action": "drain"}, headers=auth())
        sealed = await client.post("/maintenance", json={"action": "seal"}, headers=auth())
        state = await sealed.json()
        assert state["sealed"] and state["draining"] and state["idle"]
        slash = await gateway.handle_slash(SimpleNamespace(command="stop"))
        assert "updating" in slash["text"].lower()
        await gateway.dispatch_command(cmd="stop", channel_id="floor", thread_id="discussion")
        gateway._dispatch_command.assert_not_awaited()
        resumed = await client.post("/maintenance", json={"action": "resume"}, headers=auth())
        state = await resumed.json()
        assert not state["sealed"] and not state["draining"] and state["idle"]
        await gateway.dispatch_command(cmd="stop", channel_id="floor", thread_id="discussion")
        gateway._dispatch_command.assert_awaited_once()
    finally:
        await client.close()


async def test_explicit_reply_enriches_original_and_existing_result_without_duplicate(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(steering=False, busy=False)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)

        async def native_prompt(**kwargs):
            await clients["beta"].request(
                "POST",
                f"/messages/{request['id']}/reply",
                {
                    "result": "Verified result",
                    "claim": claimed["claim"],
                    "evidence": {"artifacts": ["https://example.com/proof"]},
                },
            )
            return {
                "answer": "Verified result",
                "session_id": "native",
                "execution_id": "turn",
                "stop_reason": "end_turn",
                "pending": False,
            }

        gateway.run_prompt = native_prompt
        await consumer.handle(claimed)
        original = store.get_message(request["id"], "alpha")
        assert original["state"] == "done"
        assert original["evidence"]["session_id"] == "native"
        deliveries = [m for m in store.show(p["id"], "alpha")["messages"] if m["kind"] == "result"]
        assert len(deliveries) == 1
        assert deliveries[0]["text"] == "Verified result"
        assert deliveries[0]["result_evidence"]["session_id"] == "native"
        assert deliveries[0]["result_evidence"]["artifacts"] == ["https://example.com/proof"]
    finally:
        await consumer.close()
        await close_api(store, server, clients)


def test_long_result_artifacts_are_separate_per_attempt(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    consumer = object.__new__(CollaborationConsumer)
    first = consumer.foreground({"id": "request", "claim": "first"}, {"answer": "A" * 9000})
    second = consumer.foreground({"id": "request", "claim": "second"}, {"answer": "B" * 9000})
    first_path, second_path = Path(first["result_file"]), Path(second["result_file"])
    assert first_path != second_path
    assert first_path.read_text() == "A" * 9000
    assert second_path.read_text() == "B" * 9000
    assert first_path.stat().st_mode & 0o777 == second_path.stat().st_mode & 0o777 == 0o600


async def test_injected_explicit_reply_waits_for_its_foreground_evidence(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway()
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        owned = {"active": True, "execution_id": "execution"}
        gateway._reports["existing-native"] = owned
        await consumer.reserve(claimed, "existing-native")
        store.reply(request["id"], "beta", "Correlated reply", claim=claimed["claim"])
        calls = 0
        original_request = consumer.hub.request

        async def receipt(*args, **kwargs):
            nonlocal calls
            if args[0] == "GET":
                calls += 1
                if calls == 2:
                    gateway._last_terminal["discussion"] = {
                        "session_id": "existing-native",
                        "execution_id": "execution",
                        "model": "actual-model",
                        "answer": "Foreground proof",
                        "completed_at": store.clock(),
                        "stop_reason": "end_turn",
                        "pending": False,
                    }
                    owned["active"] = False
            return await original_request(*args, **kwargs)

        consumer.hub.request = receipt
        await asyncio.wait_for(consumer.injected_wait(claimed), 4)
        assert calls == 2
        original = store.get_message(request["id"], "alpha")
        assert original["result"] == "Correlated reply"
        assert original["evidence"]["model"] == "actual-model"
        deliveries = [m for m in store.show(p["id"], "alpha")["messages"] if m["kind"] == "result"]
        assert len(deliveries) == 1
        assert deliveries[0]["result_evidence"]["foreground_answer"] == "Foreground proof"
    finally:
        await consumer.close()
        await close_api(store, server, clients)


def test_conflicting_late_reply_preserves_first_result_and_state(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", "worker")
        store.cancel(p["id"], "alpha")
        first = store.reply(request["id"], "beta", "Finished", claim=claimed["claim"])
        assert store.reply(request["id"], "beta", "Finished", claim=claimed["claim"]) == first
        with pytest.raises(ValueError, match="Conflicting"):
            store.reply(request["id"], "beta", "Changed", state="failed", claim=claimed["claim"])
        assert store.get_message(request["id"], "alpha")["late_result"] == "Finished"
        assert store.get_message(request["id"], "alpha")["late_state"] == "done"
    finally:
        store.close()


async def test_reconciled_retry_discards_only_its_old_native_ownership(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(steering=False, busy=False)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        request = ask(store, p["id"])
        first = store.claim_next("beta", consumer.instance)
        store.update(
            request["id"],
            "beta",
            consumer.instance,
            "unconfirmed",
            "Lost receipt",
            claim=first["claim"],
        )
        consumer.ownership[(request["id"], first["claim"])] = {"thread_id": "discussion"}
        other = {"thread_id": "unrelated"}
        consumer.ownership[("other-request", "other-claim")] = other
        store.retry(request["id"], "alpha", "Verified no native operation remains")
        second = store.claim_next("beta", consumer.instance)
        gateway.run_prompt.return_value = {
            "answer": "cancelled",
            "session_id": "new-native",
            "stop_reason": "cancelled",
            "pending": False,
        }
        await consumer.handle(second)
        gateway.run_prompt.assert_awaited_once()
        assert (request["id"], first["claim"]) not in consumer.ownership
        assert consumer.ownership[("other-request", "other-claim")] is other
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("recovery", ["worker_interrupted", "hub_restart"])
def test_interrupted_cancel_is_requeued_and_old_claim_is_fenced(tmp_path, recovery):
    now = [1000.0]
    path = tmp_path / "hub.db"
    store = CollaborationStore(path, {"alpha", "beta", "gamma"}, clock=lambda: now[0])
    try:
        p = project(store)
        request = ask(store, p["id"])
        store.claim_next("beta", "worker")
        store.cancel(p["id"], "alpha")
        old = store.claim_next("beta", "worker")
        assert old["kind"] == "cancel"
        if recovery == "hub_restart":
            store.close()
            store = CollaborationStore(path, {"alpha", "beta", "gamma"}, clock=lambda: now[0])
        else:
            store.update(
                old["id"], "beta", "worker", "interrupted", "Gateway exited", claim=old["claim"]
            )
            now[0] += 3
        assert store.get_message(old["id"], "alpha")["state"] == "queued"
        newer = store.claim_next("beta", "worker")
        assert newer["id"] == old["id"] and newer["claim"] != old["claim"]
        assert store.get_message(request["id"], "alpha")["state"] == "cancelling"
        with pytest.raises(PermissionError):
            store.update(old["id"], "beta", "worker", "done", "stale stop", claim=old["claim"])
    finally:
        store.close()


async def test_injected_native_failure_settles_request_and_routes_failed_result(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway()
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        gateway._reports["existing-native"] = {"execution_id": "owned-turn"}
        assert await consumer.reserve(claimed, "existing-native")
        gateway._last_terminal["discussion"] = {
            "session_id": "existing-native",
            "execution_id": "owned-turn",
            "stop_reason": "error",
            "completed_at": store.clock() + 1,
            "pending": False,
            "answer": "Native execution failed",
        }
        await asyncio.wait_for(consumer.injected_wait(claimed), 3)
        original = store.get_message(request["id"], "alpha")
        assert original["state"] == "failed" and original["evidence"]["stop_reason"] == "error"
        result = store.claim_next("alpha", "owner")
        assert result["kind"] == "result" and result["result_state"] == "failed"
    finally:
        await consumer.close()
        await close_api(store, server, clients)


def test_descendant_slot_loan_breaks_parent_wait_and_serializes_siblings(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store, max_workers=1, max_depth=2)
        parent = ask(store, p["id"])
        store.claim_next("beta", "parent-worker")
        child = ask(
            store, p["id"], sender="beta", recipient="gamma", key="child", parent=parent["id"]
        )
        sibling = ask(
            store, p["id"], sender="beta", recipient="alpha", key="sibling", parent=parent["id"]
        )
        loaned = store.claim_next("gamma", "child-worker")
        assert loaned["id"] == child["id"] and loaned["delegation_slot"]
        assert store.claim_next("alpha", "sibling-worker") is None
        grandchild = ask(
            store, p["id"], sender="gamma", recipient="alpha", key="grandchild", parent=child["id"]
        )
        nested = store.claim_next("alpha", "nested-worker")
        assert nested["id"] == grandchild["id"] and nested["delegation_slot"]
        assert sum(m["state"] == "running" for m in store.show(p["id"], "alpha")["messages"]) == 3
        with pytest.raises(ValueError, match="depth"):
            ask(
                store,
                p["id"],
                sender="alpha",
                recipient="beta",
                key="too-deep",
                parent=grandchild["id"],
            )
        store.reply(child["id"], "gamma", "Delegated finding", claim=loaned["claim"])
        store.reply(grandchild["id"], "alpha", "Nested finding", claim=nested["claim"])
        next_item = store.claim_next("alpha", "sibling-worker")
        assert next_item["id"] == sibling["id"] and next_item["delegation_slot"]
    finally:
        store.close()


def creation_body():
    return {
        "title": "Shared investigation",
        "objective": "Verify the dependency",
        "members": ["alpha", "beta"],
        "channel_id": "floor",
        "origin_channel_id": "home",
        "origin_thread_id": "origin",
        "key": "creation-key",
    }


async def creation_api(path, post_hook, verify_hook=None):
    store = CollaborationStore(path, {"alpha", "beta"})
    actors = {name: {"token": name * 32, "user_id": name + "-user"} for name in store.actors}
    server = TestServer(
        build_app(store, actors, create_post=post_hook, verify_creation=verify_hook)
    )
    await server.start_server()
    client = HubClient(Settings(str(server.make_url("/")).rstrip("/"), actors["alpha"]["token"]))
    return store, server, client


async def close_creation(store, server, client):
    await client.close()
    await server.close()
    store.close()


async def test_concurrent_creation_retries_reuse_one_durable_discussion(tmp_path):
    posted = []

    async def post(actor, body):
        posted.append((actor, body))
        await asyncio.sleep(0.01)
        return {"id": "created-root", "channel_id": "floor"}

    store, server, client = await creation_api(tmp_path / "hub.db", post)
    try:
        responses = await asyncio.gather(
            *(client.request("POST", "/creations", creation_body()) for _ in range(3))
        )
        assert len({response["id"] for response in responses}) == 1
        assert len(posted) == 1
        assert responses[0]["thread_id"] == "created-root"
        assert len(store.list_projects("alpha")) == 1
    finally:
        await close_creation(store, server, client)


async def test_uncertain_creation_survives_restart_requires_explicit_reconciliation(tmp_path):
    posted = []

    async def post(actor, body):
        posted.append("unconfirmed-root")
        raise ValueError("Post may have been accepted before connection loss")

    path = tmp_path / "hub.db"
    store, server, client = await creation_api(path, post)
    try:
        with pytest.raises(HubError, match="409"):
            await client.request("POST", "/creations", creation_body())
        assert store.list_projects("alpha") == []
        reserved, fresh = store.prepare_creation("alpha", creation_body())
        assert not fresh and reserved["status"] == "reserved"
    finally:
        await close_creation(store, server, client)

    async def verify(actor, thread):
        return {"id": thread, "channel_id": "floor", "root_id": ""}

    store, server, client = await creation_api(path, post, verify)
    try:
        for _ in range(2):
            with pytest.raises(HubError, match="409"):
                await client.request("POST", "/creations", creation_body())
        assert posted == ["unconfirmed-root"]
        body = {**creation_body(), "thread_id": "unconfirmed-root"}
        results = await asyncio.gather(
            *(client.request("POST", "/creations", body) for _ in range(2))
        )
        assert results[0]["id"] == results[1]["id"]
        assert posted == ["unconfirmed-root"]
        assert len(store.list_projects("alpha")) == 1
    finally:
        await close_creation(store, server, client)


async def test_creation_conflicting_explicit_root_does_not_replace_first_discussion(tmp_path):
    post = AsyncMock(return_value={"id": "first-root", "channel_id": "floor"})
    store, server, client = await creation_api(tmp_path / "hub.db", post)
    try:
        first = await client.request("POST", "/creations", creation_body())
        with pytest.raises(HubError, match="409"):
            await client.request(
                "POST", "/creations", {**creation_body(), "thread_id": "conflicting-root"}
            )
        assert store.list_projects("alpha")[0]["thread_id"] == "first-root"
        assert store.list_projects("alpha")[0]["id"] == first["id"]
        post.assert_awaited_once()
    finally:
        await close_creation(store, server, client)


async def test_created_discussion_readback_failure_retries_verification_without_reposting(tmp_path):
    post = AsyncMock(return_value={"id": "first-root", "channel_id": "floor"})
    verify = AsyncMock(return_value={"id": "first-root", "channel_id": "wrong-channel"})
    store, server, client = await creation_api(tmp_path / "hub.db", post, verify)
    try:
        with pytest.raises(HubError, match="409"):
            await client.request("POST", "/creations", creation_body())
        assert store.list_projects("alpha") == []
        verify.return_value = {"id": "first-root", "channel_id": "floor", "root_id": ""}
        created = await client.request("POST", "/creations", creation_body())
        assert created["thread_id"] == "first-root"
        post.assert_awaited_once()
        assert verify.await_count == 2
    finally:
        await close_creation(store, server, client)
