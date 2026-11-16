"""Native command admission and independently verified creation regressions."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from fake_agent import spawn_fake_agent_process
from test_collaboration import FakeMattermost, gateway_config
from test_collaboration_regressions import close_creation, creation_api, creation_body

from mattermost_acp.acp.broker import (
    AcpBroker,
    AcpError,
    NativeDispatchChanged,
    native_command_admission,
)
from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.session_service import SessionService
from mattermost_acp.store import ThreadStore


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
