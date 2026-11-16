"""Native command admission and independently verified creation regressions."""

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from fake_agent import spawn_fake_agent_process
from test_collaboration import FakeMattermost, api_fixture, ask, close_api, gateway_config, project
from test_collaboration_continuation_review import child_result, report, retained_parent
from test_collaboration_dispatch_review import close_reports, idle_report, native_gateway
from test_collaboration_regressions import (
    close_creation,
    creation_api,
    creation_body,
    maintenance_gateway,
    memory_broker,
)
from test_gateway_turn import _Client

from mattermost_acp.acp.broker import (
    AcpBroker,
    AcpError,
    NativeDispatchChanged,
    native_command_admission,
)
from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.commands.pending_restart import now_pending, pending_path, write_pending
from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.mm.reporting import TurnReporter
from mattermost_acp.session_service import SessionService, ThreadBusy
from mattermost_acp.store import ThreadStore


async def test_project_deadline_cannot_cross_awaited_native_write_lock(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    handling = None
    p = project(store, max_seconds=1)
    request = ask(store, p["id"])
    claimed = store.claim_next("beta", consumer.instance)
    report = idle_report()
    report.update(active=True, foreground_complete=False, collaboration_project=p["id"])
    gateway._reports["existing-native"] = report
    await broker._write_lock.acquire()
    try:
        handling = asyncio.create_task(consumer.handle(claimed))
        async with asyncio.timeout(2):
            while not broker._pending:
                await asyncio.sleep(0)
        reserved = store.get_message(request["id"], "beta")
        assert reserved["execution_pending"] is True
        assert reserved["evidence"]["dispatch_started"] < p["deadline"]
        await asyncio.sleep(max(0, p["deadline"] - store.clock()) + 0.05)
        broker._write_lock.release()
        await asyncio.sleep(0.05)
        assert all(message["method"] == "session/cancel" for message in writer.messages), {
            "native_writes": [message["method"] for message in writer.messages],
            "elapsed_past_deadline": store.clock() - p["deadline"],
            "claim_contains_deadline": "project_deadline" in claimed,
        }
        assert all(
            message["params"]["sessionId"] == "existing-native" for message in writer.messages
        )
    finally:
        if broker._write_lock.locked():
            broker._write_lock.release()
        if handling is not None and not handling.done():
            handling.cancel()
        if handling is not None:
            await asyncio.gather(handling, return_exceptions=True)
        await close_reports(gateway)
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("changed", ["unchanged", "epoch", "session", "report", "shared"])
async def test_project_deadline_cancels_only_owned_native_work_during_hub_outage(tmp_path, changed):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    p = project(store, max_seconds=1)
    requested = ask(store, p["id"])
    claimed = store.claim_next("beta", consumer.instance)
    report = idle_report()
    report.update(
        active=True,
        foreground_complete=False,
        collaboration_project=None if changed == "shared" else p["id"],
    )
    gateway._reports["existing-native"] = report
    actual = consumer.hub.request
    outage = False

    async def request(method, path, body=None):
        if outage:
            raise HubError("Synthetic hub unavailable", status=503)
        return await actual(method, path, body)

    consumer.hub.request = request
    handling = asyncio.create_task(consumer.handle(claimed))
    try:
        async with asyncio.timeout(2):
            while (
                store.get_message(requested["id"], "beta")["evidence"].get("delivery") != "injected"
            ):
                await asyncio.sleep(0)
        outage = True
        if changed == "epoch":
            gateway.sessions._advance_native("discussion")
        elif changed == "session":
            gateway.sessions.store.get = AsyncMock(
                return_value=SimpleNamespace(session_id="replacement-native")
            )
        elif changed == "report":
            gateway._reports["existing-native"] = idle_report()
        async with asyncio.timeout(3):
            import time

            while time.monotonic() < consumer.project_deadlines[p["id"]] + 0.15:
                await asyncio.sleep(0.01)
        controls = [
            message for message in writer.messages if message["method"] != "_session/steering"
        ]
        assert len(controls) == (1 if changed == "unchanged" else 0)
        if controls:
            assert controls[0]["method"] == "session/cancel"
            assert controls[0]["params"] == {"sessionId": "existing-native"}
        assert store.get_message(requested["id"], "beta")["execution_pending"] is True
        assert consumer.ownership[(requested["id"], claimed["claim"])] is report
        assert gateway.maintenance_state()["native_boundary_unknown"] is True
    finally:
        if not handling.done():
            handling.cancel()
        await asyncio.gather(handling, return_exceptions=True)
        await close_reports(gateway)
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("remaining", [2.0, 10.0])
async def test_remaining_budget_uses_request_start_and_retries_cannot_extend_it(
    monkeypatch, remaining
):
    import mattermost_acp.collaboration.client as client_module

    now = [100.0]
    monkeypatch.setattr(client_module, "time", SimpleNamespace(monotonic=lambda: now[0]))

    async def response(request):
        now[0] += 5
        return web.json_response(
            {
                "project": "synthetic",
                "project_deadline": 1000 + remaining,
                "project_remaining_seconds": remaining,
            }
        )

    app = web.Application()
    app.router.add_get("/receipt", response)
    server = TestServer(app)
    await server.start_server()
    client = HubClient(Settings(str(server.make_url("")).rstrip("/"), "synthetic-token" * 3))
    try:
        first = await client.request("GET", "/receipt")
        assert first["local_project_deadline"] == 100 + remaining
        second = await client.request("GET", "/receipt")
        assert second["local_project_deadline"] == first["local_project_deadline"]
        assert second["local_project_deadline"] <= now[0]
    finally:
        await client.close()
        await server.close()


@pytest.mark.parametrize(
    "phase", ["transfer", "stale_parent", "reserving_parent", "stale_epoch", "rollback"]
)
async def test_deadline_cancellation_tracks_continuation_execution_and_rollback(tmp_path, phase):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        broker, writer = memory_broker()
        sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        sessions._native_epochs["discussion"] = 7
        gateway.sessions = sessions
        old["native_dispatched"] = True
        delivery = child_result(store, p, parent, consumer)
        info = await consumer.continuation(delivery)
        moved = report(p["id"], "continuation-execution")
        moved["native_dispatched"] = True
        gateway._reports["existing-native"] = moved
        assert await consumer.reserve_continuation(delivery, "existing-native", info)
        parent_key = (parent["id"], parent["claim"])
        child_key = (delivery["id"], delivery["claim"])
        if phase == "rollback":
            gateway._reports["existing-native"] = old
            old["reporter"].has_pending_work = lambda: True
            await consumer.rollback_continuation(info)
            consumer.release_ownership(child_key)
        elif phase == "stale_parent":
            consumer.ownership_items[parent_key]["reservation_evidence"] = parent["evidence"]
        elif phase == "stale_epoch":
            consumer.ownership_items[parent_key]["reservation_evidence"]["native_epoch"] = 6
        if phase != "reserving_parent":
            consumer.reserving.discard(parent_key)
        consumer.project_deadlines[p["id"]] = time.monotonic() - 1
        await consumer.enforce_deadline(p["id"])
        assert writer.messages == [
            {
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": {"sessionId": "existing-native"},
            }
        ]
        expected = old if phase == "rollback" else moved
        assert consumer.ownership[parent_key] is expected
        assert expected["cancel_requested"] is True
        assert store.get_message(parent["id"], "beta")["execution_pending"] is True


@pytest.mark.parametrize("prior_owner", [False, True])
async def test_rejected_reservation_restores_deadline_ownership_registry(tmp_path, prior_owner):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        key = (parent["id"], parent["claim"])
        original = consumer.ownership_items[key]
        store.reply(parent["id"], "beta", "Explicit early result", claim=parent["claim"])
        if not prior_owner:
            consumer.release_ownership(key)
        provisional = report(p["id"], "rejected-reservation")
        provisional["previous"] = old
        gateway._reports["existing-native"] = provisional
        retry = dict(parent)
        with pytest.raises(HubError, match="409"):
            await consumer.reserve(retry, "existing-native")
        if prior_owner:
            assert consumer.ownership[key] is old
            assert consumer.ownership_items[key] is original
            assert original["reservation_evidence"]["execution_id"] == old["execution_id"]
        else:
            assert key not in consumer.ownership and key not in consumer.ownership_items


@pytest.mark.parametrize("backend", ["claude", "codex", "grok"])
async def test_async_task_capability_negotiation_reaches_only_claude_initialize(backend):
    broker = AcpBroker(backend=backend, process_factory=spawn_fake_agent_process)
    actual = broker._write
    writes = []

    async def capture(message, **kwargs):
        writes.append(message)
        return await actual(message, **kwargs)

    broker._write = capture
    try:
        await broker.start()
        initialize = next(message for message in writes if message["method"] == "initialize")
        caps = initialize["params"]["clientCapabilities"]
        assert caps["subagents"] == {}
        if backend == "claude":
            assert caps["_meta"] == {
                "jetbrains": {"air": {"version": 1, "capabilities": ["asyncTasks"]}}
            }
        else:
            assert caps == {"subagents": {}}
    finally:
        await broker.stop()


@pytest.fixture
async def native(tmp_path):
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(tmp_path / "native.db")
    await store.open()
    sessions = SessionService(broker, store, default_cwd=tmp_path)
    await sessions.ensure_session("discussion")
    writes = []
    original_write = broker._proc.stdin.write

    def write(payload):
        writes.append(json.loads(payload))
        original_write(payload)

    broker._proc.stdin.write = write
    gateway = MattermostGateway(gateway_config(), FakeMattermost("beta"), sessions)
    yield SimpleNamespace(gateway=gateway, sessions=sessions, broker=broker, writes=writes)
    await broker.stop()
    await store.close()


async def command(gateway, cmd, arg=""):
    await gateway.dispatch_command(
        thread_id="discussion",
        channel_id="floor",
        cmd=cmd,
        arg=arg,
        user_id="human-user",
        user_name="human",
    )


def ownership(*, owned=False, inherited=False):
    return SimpleNamespace(
        owns_thread=lambda _thread: owned,
        inherited_thread_pending=lambda _thread: inherited,
        recovery_pending=False,
        record_stop=lambda _thread: 1000,
        cancel_thread=AsyncMock(),
    )


@pytest.mark.parametrize("cmd", ["new", "clear", "restart", "model", "models", "effort", "harness"])
@pytest.mark.parametrize("pending", ["recovery", "inherited", "owned", "background"])
async def test_native_commands_refuse_project_admission_without_native_effects(
    native, cmd, pending
):
    gateway, sessions = native.gateway, native.sessions
    if pending == "recovery":
        gateway.collaboration_recovery_pending = True
    elif pending in {"inherited", "owned"}:
        gateway.collaboration = ownership(
            owned=pending == "owned", inherited=pending == "inherited"
        )
    else:
        gateway._reports["pending-project"] = {
            "thread_id": "discussion",
            "collaboration_project": "project",
            "active": False,
            "reporter": SimpleNamespace(has_pending_work=lambda: True),
        }
    epoch = sessions.native_epoch("discussion")
    record = await sessions.store.get("discussion")
    await command(gateway, cmd, "high" if cmd == "effort" else "grok-4.5")
    assert native.writes == []
    assert sessions.native_epoch("discussion") == epoch
    assert (await sessions.store.get("discussion")).session_id == record.session_id
    assert "Project work is pending" in gateway.client.posts[-1][1]
    assert gateway._active_controls == 0


async def test_project_recovery_preserves_stop_and_active_only_hint(native):
    gateway, sessions = native.gateway, native.sessions
    gateway.collaboration_recovery_pending = True
    gateway.collaboration = ownership(owned=True)
    sessions.hint = AsyncMock(return_value="injected")
    await command(gateway, "hint", "Check the result")
    sessions.hint.assert_awaited_once_with("discussion", "Check the result")
    await command(gateway, "stop")
    assert [item["method"] for item in native.writes] == ["session/cancel"]
    gateway.collaboration.cancel_thread.assert_awaited_once()


@pytest.mark.parametrize(
    ("cmd", "waiting"),
    [
        ("new", "prepare"),
        ("new", "start"),
        ("new", "write"),
        ("effort", "prepare"),
        ("effort", "thread"),
        ("effort", "write"),
    ],
)
async def test_admission_rechecked_after_native_lock_wait(native, cmd, waiting):
    sessions, broker, gateway = native.sessions, native.broker, native.gateway
    lock = {
        "prepare": sessions._preparation_locks.setdefault("discussion", asyncio.Lock()),
        "thread": sessions._thread_locks.setdefault("discussion", asyncio.Lock()),
        "start": broker._lock,
        "write": broker._write_lock,
    }[waiting]
    epoch = sessions.native_epoch("discussion")
    await lock.acquire()
    task = asyncio.create_task(command(gateway, cmd, "high"))
    try:
        async with asyncio.timeout(2):
            while not getattr(lock, "_waiters", None):
                await asyncio.sleep(0)
        gateway.collaboration_recovery_pending = True
        lock.release()
        await asyncio.wait_for(task, 3)
        assert native.writes == []
        assert sessions.native_epoch("discussion") == epoch
        assert "Project work is pending" in gateway.client.posts[-1][1]
    finally:
        if lock.locked():
            lock.release()
        await asyncio.gather(task, return_exceptions=True)


async def test_admission_rechecked_after_workspace_guard(native):
    entered, release = asyncio.Event(), asyncio.Event()

    async def guard(*_args):
        entered.set()
        await release.wait()

    native.sessions.workspace_guard = guard
    epoch = native.sessions.native_epoch("discussion")
    task = asyncio.create_task(command(native.gateway, "new"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        native.gateway.collaboration = ownership(inherited=True)
        release.set()
        await asyncio.wait_for(task, 2)
        assert native.writes == []
        assert native.sessions.native_epoch("discussion") == epoch
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_new_session_epoch_changes_only_after_accepted_write(native):
    await native.sessions.store.delete("discussion")
    epoch = native.sessions.native_epoch("discussion")
    await native.broker._write_lock.acquire()
    task = asyncio.create_task(command(native.gateway, "new"))
    try:
        async with asyncio.timeout(2):
            while not native.broker._write_lock._waiters:
                await asyncio.sleep(0)
        native.gateway.collaboration_recovery_pending = True
        native.broker._write_lock.release()
        await asyncio.wait_for(task, 2)
        assert native.writes == []
        assert native.sessions.native_epoch("discussion") == epoch
        assert await native.sessions.store.get("discussion") is None
    finally:
        if native.broker._write_lock.locked():
            native.broker._write_lock.release()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("close", ["unsupported", "failed"])
async def test_queued_model_switch_cannot_compact_or_restore_replaced_session(native, close):
    svc = native.sessions
    await svc.switch_model("discussion", "hub-claude-5-sonnet")
    if close == "unsupported":
        native.broker.capabilities.close = False
    else:
        request = native.broker.request

        async def rpc(method, *args, **kwargs):
            if method == "session/close":
                raise AcpError("Unsupported synthetic close")
            return await request(method, *args, **kwargs)

        native.broker.request = rpc
    lock = svc._thread_locks["discussion"]
    await lock.acquire()
    task = asyncio.create_task(svc.switch_model("discussion", "grok-4.5"))
    try:
        async with asyncio.timeout(2):
            while not lock._waiters:
                await asyncio.sleep(0)
        replacement = await svc.new_session("discussion")
        snapshot = await svc.store.get("discussion")
        native.writes.clear()
        lock.release()
        with pytest.raises(NativeDispatchChanged, match="while model switch waited"):
            await asyncio.wait_for(task, 2)
        assert native.writes == []
        assert (await svc.store.get("discussion")) == snapshot
        assert snapshot.session_id == replacement
    finally:
        if lock.locked():
            lock.release()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("window", ["rpc", "persistence"])
@pytest.mark.parametrize("scope", ["thread", "channel", "harness"])
async def test_replacement_during_model_switch_cannot_restore_session_or_overrides(
    native, window, scope
):
    svc = native.sessions
    await svc.switch_model("discussion", "grok-4.5")
    await svc.store.set_thread_override("discussion", "floor", None)
    entered, release = asyncio.Event(), asyncio.Event()
    if window == "rpc":
        original = native.broker.set_model

        async def set_model(session, model, **kwargs):
            result = await original(session, model, **kwargs)
            if model == "hub-claude-5-sonnet":
                entered.set()
                await release.wait()
            return result

        native.broker.set_model = set_model
    else:
        original = svc.store.save_switch_if_session

        async def save(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        svc.store.save_switch_if_session = save
    task = asyncio.create_task(
        svc.switch_model(
            "discussion",
            "hub-claude-5-sonnet",
            scope=scope,
            channel_id="floor",
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        replacement = await svc.new_session("discussion")
        await svc.store.set_thread_override("discussion", "floor", "grok-4.5", "high")
        snapshot = await svc.store.get("discussion")
        release.set()
        with pytest.raises(NativeDispatchChanged, match="before model settings were saved"):
            await asyncio.wait_for(task, 2)
        assert (await svc.store.get("discussion")) == snapshot
        assert snapshot.session_id == replacement
        assert await svc.store.get_model_setting("channel:floor") is None
        assert await svc.store.get_model_setting("harness") is None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_compaction_write_guard_preserves_epoch_and_nested_admission(native):
    svc = native.sessions
    session = (await svc.store.get("discussion")).session_id
    epoch = svc.native_epoch("discussion")
    admitted = True
    await native.broker._write_lock.acquire()

    async def compact():
        with native_command_admission(lambda: admitted):
            with native_command_admission(lambda: True):
                return await svc._compact_history("discussion", session)

    task = asyncio.create_task(compact())
    try:
        async with asyncio.timeout(2):
            while not native.broker._write_lock._waiters:
                await asyncio.sleep(0)
        admitted = False
        native.broker._write_lock.release()
        with pytest.raises(NativeDispatchChanged):
            await asyncio.wait_for(task, 3)
        assert native.writes == []
        assert svc.native_epoch("discussion") == epoch
        assert not svc.is_busy("discussion")
        result = await native.broker.prompt_turn(session, "Independent later work")
        assert "Independent later work" in result.answer_text
    finally:
        if native.broker._write_lock.locked():
            native.broker._write_lock.release()
        await asyncio.gather(task, return_exceptions=True)


async def test_persistent_reader_does_not_retain_starting_command_guard(tmp_path):
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    admitted = True
    writes = []
    try:
        with native_command_admission(lambda: admitted):
            await broker.start()
            original_write = broker._proc.stdin.write

            def write(payload):
                writes.append(json.loads(payload))
                original_write(payload)

            broker._proc.stdin.write = write
            admitted = False
            broker._proc.stdout.feed_data(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": "server-check",
                        "method": "unsupported/server/request",
                        "params": {},
                    }
                ).encode()
                + b"\n"
            )
            async with asyncio.timeout(2):
                while not writes:
                    await asyncio.sleep(0)
            assert writes[0]["id"] == "server-check"
            assert writes[0]["error"]["code"] == -32601
        session = await broker.session_new(tmp_path)
        result = await broker.prompt_turn(session, "Unrelated later work")
        assert "Unrelated later work" in result.answer_text
    finally:
        await broker.stop()


@pytest.fixture
async def rejected_api(tmp_path):
    state = SimpleNamespace(status=401, attempts=[], roots={})

    async def post(request):
        body = await request.json()
        state.attempts.append(body)
        if state.status != 200:
            return web.json_response({"error": "Synthetic rejection"}, status=state.status)
        root = {
            "id": "created-root",
            "channel_id": body["channel_id"],
            "root_id": "",
            "user_id": "alpha-user",
            "props": body["props"],
        }
        state.roots[root["id"]] = root
        return web.json_response(root)

    async def read(request):
        return web.json_response(state.roots[request.match_info["post"]])

    mm = web.Application()
    mm.router.add_post("/api/v4/posts", post)
    mm.router.add_get("/api/v4/posts/{post}", read)
    mm_server = TestServer(mm)
    await mm_server.start_server()
    credential = tmp_path / "mm.token"
    credential.write_text("synthetic-mattermost-credential")
    actors = {
        "alpha": {
            "token": "alpha" * 32,
            "user_id": "alpha-user",
            "mattermost_url": str(mm_server.make_url("/")).rstrip("/"),
            "mattermost_token_file": str(credential),
        },
        "beta": {"token": "beta" * 32, "user_id": "beta-user"},
    }
    store = CollaborationStore(tmp_path / "hub.db", set(actors))
    server = TestServer(build_app(store, actors))
    await server.start_server()
    clients = {
        actor: HubClient(Settings(str(server.make_url("/")).rstrip("/"), data["token"]))
        for actor, data in actors.items()
    }
    yield SimpleNamespace(state=state, store=store, clients=clients)
    await asyncio.gather(*(client.close() for client in clients.values()))
    await server.close()
    store.close()
    await mm_server.close()


@pytest.mark.parametrize("status", [401, 403])
async def test_confirmed_post_rejection_releases_fallback_and_allows_same_key_retry(
    rejected_api, status
):
    runtime = rejected_api
    runtime.state.status = status
    with pytest.raises(HubError, match="409"):
        await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    intent = runtime.store.get_creation("alpha", "creation-key")
    assert intent["status"] == "rejected"
    assert not runtime.state.roots
    assert runtime.store.unresolved_creations("floor") == []
    assert (
        await runtime.clients["beta"].request(
            "POST",
            "/mentions",
            {
                "post_id": "unrelated-reply",
                "author_id": "human-user",
                "channel_id": "floor",
                "thread_id": "ordinary-root",
                "text": "@beta ASK: Check the result",
            },
        )
        is None
    )
    runtime.state.status = 200
    result = await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    assert result["thread_id"] == "created-root"
    assert len(runtime.state.attempts) == 2 and len(runtime.state.roots) == 1
    retry = runtime.store.get_creation("alpha", "creation-key")
    assert retry["creation_id"] != intent["creation_id"]
    assert retry["created"] == intent["created"]
    repeated = await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    assert repeated["id"] == result["id"] and len(runtime.state.attempts) == 2


async def test_post_service_failure_retains_uncertainty_without_replay(rejected_api):
    runtime = rejected_api
    runtime.state.status = 503
    for _ in range(2):
        with pytest.raises(HubError, match="409"):
            await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    assert len(runtime.state.attempts) == 1
    assert len(runtime.store.unresolved_creations("floor")) == 1
    with pytest.raises(HubError, match="403"):
        await runtime.clients["beta"].request(
            "POST",
            "/mentions",
            {
                "post_id": "unrelated-reply",
                "author_id": "human-user",
                "channel_id": "floor",
                "thread_id": "ordinary-root",
                "text": "@beta ASK: Check the result",
            },
        )


def test_confirmed_rejection_never_erases_independently_bound_root(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta"})
    try:
        intent, _ = store.prepare_creation("alpha", creation_body())
        store.record_creation_root(
            "alpha", "creation-key", "retained-root", creation_id=intent["creation_id"]
        )
        store.reject_creation("alpha", "creation-key", intent["creation_id"])
        retained, fresh = store.prepare_creation("alpha", creation_body())
        assert retained["thread_id"] == "retained-root" and retained["status"] == "posted"
        assert not fresh
    finally:
        store.close()


@pytest.mark.parametrize("mismatch", ["creator", "nonce", "actor", "props", "correct"])
async def test_automatic_post_success_requires_independent_creator_and_correlation(
    tmp_path, mismatch
):
    post = AsyncMock(return_value={"id": "created-root", "channel_id": "floor"})

    async def verify(_actor, _thread):
        if mismatch == "creator":
            return {"user_id": "wrong-user"}
        if mismatch == "props":
            return {"props": {}}
        if mismatch in {"nonce", "actor"}:
            props = dict(post.call_args.args[1]["props"])
            props[
                "mattermost_acp_creation_id"
                if mismatch == "nonce"
                else "mattermost_acp_creation_actor"
            ] = "wrong"
            return {"props": props}
        return {}

    store, server, client = await creation_api(tmp_path / "hub.db", post, verify)
    try:
        if mismatch == "correct":
            result = await client.request("POST", "/creations", creation_body())
            assert result["thread_id"] == "created-root"
        else:
            for _ in range(2):
                with pytest.raises(HubError, match="403"):
                    await client.request("POST", "/creations", creation_body())
            assert store.list_projects("alpha") == []
            assert store.get_creation("alpha", "creation-key")["thread_id"] == "created-root"
        post.assert_awaited_once()
    finally:
        await close_creation(store, server, client)


async def test_fresh_explicit_unmarked_discussion_keeps_existing_creation_path(tmp_path):
    post = AsyncMock()
    verify = AsyncMock(return_value={"id": "explicit-root", "channel_id": "floor", "root_id": ""})
    store, server, client = await creation_api(tmp_path / "hub.db", post, verify)
    try:
        result = await client.request(
            "POST", "/creations", {**creation_body(), "thread_id": "explicit-root"}
        )
        assert result["thread_id"] == "explicit-root"
        assert store.get_creation("alpha", "creation-key")["creation_id"] is None
        post.assert_not_awaited()
    finally:
        await close_creation(store, server, client)


@pytest.mark.parametrize(
    "origin",
    ["http://127.0.0.1:abc", "http://127.0.0.1:0", "http://localhost:65536", "http://[::1]:-1"],
)
def test_collaboration_settings_reject_invalid_tcp_ports(tmp_path, monkeypatch, origin):
    credential = tmp_path / "actor.token"
    credential.write_text("alpha" * 32)
    monkeypatch.setenv("MATTERMOST_COLLAB_URL", origin)
    monkeypatch.setenv("MATTERMOST_COLLAB_TOKEN_FILE", str(credential))
    with pytest.raises(ValueError, match="Collaboration URL"):
        Settings.load()


@pytest.mark.parametrize(
    "origin", ["http://127.0.0.1", "http://localhost:18780", "http://[::1]:65535"]
)
def test_collaboration_settings_accept_valid_loopback_tcp_ports(tmp_path, monkeypatch, origin):
    credential = tmp_path / "actor.token"
    credential.write_text("alpha" * 32)
    monkeypatch.setenv("MATTERMOST_COLLAB_URL", origin)
    monkeypatch.setenv("MATTERMOST_COLLAB_TOKEN_FILE", str(credential))
    assert Settings.load().url == origin


@pytest.mark.parametrize(
    "pending", ["unknown", "foreground", "background", "ownership", "native_rpc"]
)
async def test_explicit_restart_preserves_other_thread_native_work(pending):
    gateway = maintenance_gateway()
    gateway.collaboration.owns_thread = lambda _thread: False
    gateway._request_restart = AsyncMock(return_value="Synthetic restart scheduled")
    if pending == "unknown":
        del gateway.sessions.broker.native_boundary_idle
    elif pending == "foreground":
        gateway.sessions.busy_threads.add("other-project-thread")
    elif pending == "background":
        gateway._reports["other-session"] = {
            "thread_id": "other-project-thread",
            "active": False,
            "reporter": SimpleNamespace(has_pending_work=lambda: True),
        }
    elif pending == "ownership":
        gateway.collaboration.ownership = {("other-request", "claim"): None}
    else:
        gateway.sessions.broker._pending[1] = object()
    await command(gateway, "restart")
    tasks = list(gateway._slash_tasks)
    assert tasks
    await asyncio.wait_for(asyncio.gather(*tasks), 2)
    gateway._request_restart.assert_not_awaited()
    assert not gateway.draining and not gateway.sealed
    assert any("Restart deferred" in text for _, text, _ in gateway.client.posts)


async def test_explicit_restart_uses_verified_idle_maintenance_transition():
    gateway = maintenance_gateway()
    gateway.collaboration.owns_thread = lambda _thread: False
    gateway._request_restart = AsyncMock(return_value="Synthetic restart scheduled")
    await command(gateway, "restart")
    await asyncio.wait_for(asyncio.gather(*list(gateway._slash_tasks)), 2)
    gateway._request_restart.assert_awaited_once_with(
        thread_id="discussion",
        channel_id="floor",
        user_id="human-user",
        reason="slash_restart",
    )
    assert gateway.draining and gateway.sealed
    assert [call.args[0] for call in gateway.collaboration.set_admission.await_args_list] == [
        "controls",
        "sealed",
    ]


async def test_explicit_restart_waits_for_originating_reply_to_settle():
    gateway = maintenance_gateway()
    gateway.collaboration.owns_thread = lambda _thread: False
    gateway._request_restart = AsyncMock(return_value="Synthetic restart scheduled")
    entered, release = asyncio.Event(), asyncio.Event()
    original = gateway.client.create_post

    async def reply(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    gateway.client.create_post = reply
    task = asyncio.create_task(command(gateway, "restart"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert gateway._active_controls == 1 and not gateway._slash_tasks
        gateway._request_restart.assert_not_awaited()
        release.set()
        await asyncio.wait_for(task, 2)
        await asyncio.wait_for(asyncio.gather(*list(gateway._slash_tasks)), 2)
        gateway._request_restart.assert_awaited_once()
        assert gateway.draining and gateway.sealed
    finally:
        release.set()
        await asyncio.gather(task, *list(gateway._slash_tasks), return_exceptions=True)


@pytest.mark.parametrize("preparation", [1, 2])
async def test_ordinary_native_preparation_carries_admission_through_workspace_wait(
    native, preparation
):
    gateway, svc = native.gateway, native.sessions
    gateway.collaboration = ownership()
    calls = 0
    entered, release = asyncio.Event(), asyncio.Event()

    async def guard(*_args):
        nonlocal calls
        calls += 1
        if calls == preparation:
            entered.set()
            await release.wait()

    svc.workspace_guard = guard
    task = asyncio.create_task(
        gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="Ordinary follow-up",
            author_id="human-user",
            author_name="human",
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        writes = list(native.writes)
        gateway.collaboration_recovery_pending = True
        release.set()
        assert await asyncio.wait_for(task, 3) is None
        assert native.writes == writes
        assert not svc.is_busy("discussion")
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await close_reports(gateway)


async def test_refused_ordinary_prompt_restores_predecessor_cancellation_epoch(native):
    gateway, svc = native.gateway, native.sessions
    gateway.collaboration = ownership()
    record = await svc.store.get("discussion")
    predecessor = {
        "execution_id": "prior-execution",
        "collaboration_project": "prior-project",
        "thread_id": "discussion",
        "channel_id": "floor",
        "active": False,
        "foreground_complete": True,
        "native_dispatched": True,
        "reporter": SimpleNamespace(has_pending_work=lambda: False),
        "task": None,
    }
    gateway._reports[record.session_id] = predecessor
    epoch = svc.native_epoch("discussion")
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def guard(*_args):
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            await release.wait()

    svc.workspace_guard = guard
    task = asyncio.create_task(
        gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="Ordinary follow-up",
            author_id="human-user",
            author_name="human",
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert gateway._reports[record.session_id] is not predecessor
        gateway.collaboration = ownership(owned=True)
        release.set()
        assert await asyncio.wait_for(task, 3) is None
        assert gateway._reports[record.session_id] is predecessor
        assert svc.native_epoch("discussion") == epoch
        native.writes.clear()
        assert await svc.cancel(
            "discussion", expected_session=record.session_id, expected_epoch=epoch
        )
        assert [item["method"] for item in native.writes] == ["session/cancel"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await close_reports(gateway)


async def test_native_start_rechecks_admission_after_awaited_start_marker(tmp_path):
    admitted = True
    entered, release = asyncio.Event(), asyncio.Event()

    async def mark():
        entered.set()
        await release.wait()

    factory = AsyncMock(side_effect=spawn_fake_agent_process)
    broker = AcpBroker(process_factory=factory, on_native_start=mark)

    async def start():
        with native_command_admission(lambda: admitted):
            return await broker.start()

    task = asyncio.create_task(start())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        admitted = False
        release.set()
        with pytest.raises(NativeDispatchChanged):
            await asyncio.wait_for(task, 2)
        factory.assert_not_awaited()
        assert broker._proc is None and broker._reader_task is None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await broker.stop()


@pytest.mark.parametrize("revoke", ["spawn", "initialize-write", "authentication"])
async def test_refused_native_initialization_retains_one_process_for_admitted_retry(
    revoke, tmp_path
):
    admitted = True
    processes = []
    writes = []
    broker = None

    async def factory(argv):
        nonlocal admitted
        process = await spawn_fake_agent_process(argv)
        processes.append(process)
        original = process.stdin.write

        def write(payload):
            writes.append(json.loads(payload))
            original(payload)

        process.stdin.write = write
        if revoke == "spawn":
            admitted = False
        return process

    broker = AcpBroker(
        process_factory=factory, backend="cursor" if revoke == "authentication" else "grok"
    )
    request = broker.request
    entered = asyncio.Event()

    async def rpc(method, *args, **kwargs):
        nonlocal admitted
        if method == "initialize":
            entered.set()
        result = await request(method, *args, **kwargs)
        if method == "initialize" and revoke == "authentication":
            admitted = False
        return result

    broker.request = rpc

    async def start():
        with native_command_admission(lambda: admitted):
            return await broker.start()

    if revoke == "initialize-write":
        await broker._write_lock.acquire()
    task = asyncio.create_task(start())
    try:
        if revoke == "initialize-write":
            await asyncio.wait_for(entered.wait(), 2)
            async with asyncio.timeout(2):
                while not broker._write_lock._waiters:
                    await asyncio.sleep(0)
            admitted = False
            broker._write_lock.release()
        with pytest.raises(NativeDispatchChanged):
            await asyncio.wait_for(task, 2)
        assert len(processes) == 1 and broker._proc is processes[0]
        assert broker.is_running and not broker._started
        assert (
            writes == []
            if revoke != "authentication"
            else [item["method"] for item in writes] == ["initialize"]
        )
        reader = broker._reader_task
        admitted = True
        await broker.start()
        assert len(processes) == 1 and broker._proc is processes[0]
        assert broker._reader_task is reader and broker._started
        assert [item["method"] for item in writes].count("initialize") == 1
    finally:
        if broker._write_lock.locked():
            broker._write_lock.release()
        await asyncio.gather(task, return_exceptions=True)
        await broker.stop()
        assert all(process.returncode is not None for process in processes)


async def test_uncertain_accepted_initialization_cannot_relaunch_or_replay(tmp_path):
    factory = AsyncMock(side_effect=spawn_fake_agent_process)
    broker = AcpBroker(process_factory=factory)
    request = broker.request

    async def lost_result(method, *args, **kwargs):
        await request(method, *args, **kwargs)
        raise AcpError("Synthetic initialization result loss")

    broker.request = lost_result
    try:
        with pytest.raises(AcpError, match="initialization result loss"):
            await broker.start()
        process, reader = broker._proc, broker._reader_task
        broker.request = request
        with pytest.raises(AcpError, match="initialization is unconfirmed"):
            await broker.start()
        assert factory.await_count == 1
        assert broker._proc is process and broker._reader_task is reader
        assert broker.is_running and not broker._started
    finally:
        await broker.stop()


async def test_ordinary_preparation_cannot_swallow_refused_model_pin(native):
    svc, gateway = native.sessions, native.gateway
    await svc.switch_model("discussion", "grok-4.5")
    gateway.collaboration = ownership()
    native.writes.clear()
    entered, release = asyncio.Event(), asyncio.Event()
    original = native.broker.set_model

    async def set_model(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    native.broker.set_model = set_model
    task = asyncio.create_task(
        gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="Ordinary follow-up",
            author_id="human-user",
            author_name="human",
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        gateway.collaboration_recovery_pending = True
        release.set()
        assert await asyncio.wait_for(task, 3) is None
        assert [item["method"] for item in native.writes] == ["session/resume"]
        assert not gateway._reports
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await close_reports(gateway)


@pytest.mark.parametrize("pending", ["missing", None, 0, "false", True])
def test_execution_settlement_requires_explicit_false_pending_proof(tmp_path, pending):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store)
        request = ask(store, p["id"])
        claim = store.claim_next("beta", "worker")
        reserved = store.update(
            request["id"],
            "beta",
            "worker",
            "dispatch",
            evidence={
                "session_id": "native",
                "execution_id": "execution",
            },
            claim=claim["claim"],
        )
        store.reply(request["id"], "beta", "Early correlated reply", claim=claim["claim"])
        receipt = {
            "session_id": "native",
            "execution_id": "execution",
            "native_dispatched": True,
            "stop_reason": "end_turn",
            "completed_at": reserved["evidence"]["dispatch_started"] + 1,
        }
        if pending != "missing":
            receipt["pending"] = pending
        with pytest.raises(ValueError, match="settlement must match"):
            store.update(
                request["id"],
                "beta",
                "worker",
                "execution_settled",
                evidence=receipt,
                claim=claim["claim"],
                expected_execution_id="execution",
            )
        assert store.get_message(request["id"], "beta")["execution_pending"] is True
        settled = store.update(
            request["id"],
            "beta",
            "worker",
            "execution_settled",
            evidence={**receipt, "pending": False},
            claim=claim["claim"],
            expected_execution_id="execution",
        )
        assert settled["execution_pending"] is False
    finally:
        store.close()


def restart_marker(native):
    gateway = native.gateway
    gateway.config.db_path = native.sessions.default_cwd / "native.db"
    gateway.config.grok_model = "synthetic-pinned-model"
    path = pending_path(gateway.config.db_path)
    write_pending(
        path,
        now_pending(
            thread_id="discussion",
            session_id="prior-native",
            user_id="human-user",
            parent_channel_id="floor",
        ),
    )
    return path


@pytest.mark.parametrize("blocked", ["recovery", "inherited", "ownership", "draining"])
async def test_restart_warming_defers_without_native_changes_until_thread_is_admitted(
    native, blocked
):
    gateway, svc = native.gateway, native.sessions
    path = restart_marker(native)
    if blocked == "recovery":
        gateway.collaboration_recovery_pending = True
    elif blocked == "draining":
        gateway.draining = True
    else:
        gateway.collaboration = ownership(
            owned=blocked == "ownership", inherited=blocked == "inherited"
        )
    original = svc.ensure_session
    svc.ensure_session = AsyncMock(wraps=original)
    epoch = svc.native_epoch("discussion")
    record = await svc.store.get("discussion")
    await gateway._handle_pending_restart_bump()
    svc.ensure_session.assert_not_awaited()
    assert native.writes == [] and svc.native_epoch("discussion") == epoch
    assert await svc.store.get("discussion") == record
    assert "session preparation deferred" in gateway.client.posts[-1][1]
    assert not path.exists()


async def test_restart_warming_carries_admission_through_workspace_wait(native):
    gateway, svc = native.gateway, native.sessions
    restart_marker(native)
    entered, release = asyncio.Event(), asyncio.Event()

    async def guard(*_args):
        entered.set()
        await release.wait()

    svc.workspace_guard = guard
    epoch = svc.native_epoch("discussion")
    task = asyncio.create_task(gateway._handle_pending_restart_bump())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        gateway.collaboration_recovery_pending = True
        release.set()
        await asyncio.wait_for(task, 2)
        assert native.writes == [] and svc.native_epoch("discussion") == epoch
        assert "session preparation deferred" in gateway.client.posts[-1][1]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_admitted_restart_warming_preserves_existing_session_and_no_prompt(native):
    gateway, svc = native.gateway, native.sessions
    restart_marker(native)
    gateway.collaboration = ownership()
    record = await svc.store.get("discussion")
    await gateway._handle_pending_restart_bump()
    assert [item["method"] for item in native.writes] == ["session/resume"]
    assert (await svc.store.get("discussion")).session_id == record.session_id
    assert f"session `{record.session_id}` resumed" in gateway.client.posts[-1][1]


@pytest.mark.parametrize("refusal", ["cancelled", "admission", "busy"])
async def test_refused_dispatch_finalizes_its_status_without_cancelling_predecessor(
    native, refusal
):
    gateway, svc = native.gateway, native.sessions
    client = _Client()
    gateway.client = client
    record = await svc.store.get("discussion")
    previous = {
        "execution_id": "prior-execution",
        "collaboration_project": "prior-project",
        "thread_id": "discussion",
        "channel_id": "floor",
        "active": False,
        "foreground_complete": True,
        "native_dispatched": True,
        "reporter": TurnReporter(),
        "task": None,
    }
    gateway._reports[record.session_id] = previous

    async def before_native(_session):
        if refusal == "admission":
            previous["reporter"].active_tools.add("retained-background-tool")
            raise NativeDispatchChanged("Synthetic native admission change")
        return False

    if refusal == "busy":
        svc.run_turn = AsyncMock(side_effect=ThreadBusy("Synthetic preparation race"))
    try:
        result = await gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="Check the result",
            author_id="human-user",
            author_name="human",
            before_native=before_native,
            collaboration_project="new-project",
        )
        assert result["queued"] is True
        assert "Working" in client.created[0][1]
        assert client.updated[-1][0] == "post1"
        final_status = client.updated[-1][1]
        assert ("Cancelled" if refusal == "cancelled" else "Queued") in final_status
        assert "Working" not in final_status
        assert gateway._reports[record.session_id] is previous
        assert not previous.get("cancel_requested")
        assert not any(
            item["method"] in {"session/cancel", "session/prompt"} for item in native.writes
        )
        if refusal == "admission":
            assert previous["reporter"].active_tools == {"retained-background-tool"}
    finally:
        await close_reports(gateway)


@pytest.mark.parametrize("pending", ["ownership", "recovery", "background"])
async def test_model_directive_refuses_catalog_preparation_before_native_effects(native, pending):
    gateway = native.gateway
    gateway.config.harness = "claude"
    if pending == "ownership":
        gateway.collaboration = ownership(owned=True)
    elif pending == "recovery":
        gateway.collaboration_recovery_pending = True
    else:
        gateway._reports["native"] = {
            "thread_id": "discussion",
            "collaboration_project": "project",
            "active": False,
            "reporter": SimpleNamespace(has_pending_work=lambda: True),
        }
    native.sessions.ensure_session = AsyncMock(wraps=native.sessions.ensure_session)
    with pytest.raises(NativeDispatchChanged):
        await gateway._apply_model_directive("discussion", "floor", "model", "high", "thread")
    native.sessions.ensure_session.assert_not_awaited()
    assert native.writes == []


async def test_model_directive_catalog_wait_rechecks_project_admission(native):
    gateway, svc = native.gateway, native.sessions
    gateway.config.harness = "claude"
    entered, release = asyncio.Event(), asyncio.Event()

    async def guard(*_args):
        entered.set()
        await release.wait()

    svc.workspace_guard = guard
    epoch = svc.native_epoch("discussion")
    task = asyncio.create_task(
        gateway._apply_model_directive("discussion", "floor", "model", "high", "thread")
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        gateway.collaboration = ownership(owned=True)
        release.set()
        with pytest.raises(NativeDispatchChanged):
            await asyncio.wait_for(task, 2)
        assert native.writes == [] and svc.native_epoch("discussion") == epoch
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_respawn_refused_initialization_cannot_reuse_previous_started_capabilities(tmp_path):
    admitted = True
    processes, writes = [], []

    async def factory(argv):
        nonlocal admitted
        process = await spawn_fake_agent_process(argv)
        processes.append(process)
        original = process.stdin.write

        def write(payload):
            writes.append((process.pid, json.loads(payload)))
            original(payload)

        process.stdin.write = write
        if len(processes) == 2:
            admitted = False
        return process

    broker = AcpBroker(process_factory=factory)
    try:
        await broker.start()
        assert broker._started
        old_reader = broker._reader_task
        processes[0].terminate()
        await asyncio.wait_for(processes[0].wait(), 2)
        await asyncio.wait_for(old_reader, 2)
        with native_command_admission(lambda: admitted):
            with pytest.raises(NativeDispatchChanged):
                await broker.start()
        assert broker.is_running and not broker._started and not broker._initialized
        replacement = broker._proc
        assert not any(pid == replacement.pid for pid, _msg in writes)
        admitted = True
        await broker.start()
        assert broker._proc is replacement and broker._started and len(processes) == 2
        assert [msg["method"] for pid, msg in writes if pid == replacement.pid] == ["initialize"]
        session = await broker.session_new(tmp_path)
        assert session
    finally:
        await broker.stop()
