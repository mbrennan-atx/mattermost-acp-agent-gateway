"""Collaboration contracts: durable state, fenced delivery and project isolation."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestServer
from fake_agent import spawn_fake_agent_process

from mattermost_acp.acp import AcpBroker, TurnResult
from mattermost_acp.collaboration.client import HubClient, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.session_service import SessionService
from mattermost_acp.store import ThreadStore


def project(store, owner="alpha", **limits):
    suffix = str(len(store.list_projects(owner)))
    return store.create_project(
        owner,
        {
            "title": "Investigate a build",
            "objective": "Find the failing dependency",
            "members": ["alpha", "beta", "gamma"],
            "channel_id": "floor",
            "thread_id": "discussion" if suffix == "0" else "discussion-" + suffix,
            "origin_channel_id": "home",
            "origin_thread_id": "origin",
            "limits": limits,
        },
    )


def ask(store, pid, sender="alpha", recipient="beta", key="request-1", **extra):
    return store.send(
        pid,
        sender,
        {
            "recipient": recipient,
            "kind": "ask",
            "text": "Check the dependency",
            "key": key,
            **extra,
        },
    )


def test_durable_request_and_idempotent_result(tmp_path):
    path = tmp_path / "hub.db"
    store = CollaborationStore(path, {"alpha", "beta", "gamma"})
    p = project(store)
    m = ask(store, p["id"])
    assert ask(store, p["id"])["id"] == m["id"]
    with pytest.raises(ValueError, match="idempotency"):
        ask(store, p["id"], text="A different operation")
    claimed = store.claim_next("beta", "instance-1")
    assert claimed["id"] == m["id"]
    store.reply(m["id"], "beta", "Found the missing dependency", evidence={"session": "native"})
    store.reply(m["id"], "beta", "Found the missing dependency", evidence={"session": "native"})
    store.close()

    reopened = CollaborationStore(path, {"alpha", "beta", "gamma"})
    state = reopened.show(p["id"], "alpha")
    assert len([item for item in state["messages"] if item["kind"] == "result"]) == 1
    result = reopened.claim_next("alpha", "instance-2")
    assert result["kind"] == "result"
    assert result["text"] == "Found the missing dependency"
    assert result["evidence"]["session"] == "native"
    reopened.close()


async def api_fixture(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    actors = {name: {"token": name * 32, "user_id": name + "-user"} for name in store.actors}

    async def verify_post(post_id, _actor):
        return {
            "user_id": "alpha-user",
            "channel_id": "floor",
            "root_id": "unregistered" if post_id == "post-2" else "discussion",
            "message": "@beta ASK: Verify the dependency",
        }

    async def verify_recipient(actor):
        return {"id": actor + "-user", "username": actor}

    server = TestServer(
        build_app(store, actors, verify_post=verify_post, verify_recipient=verify_recipient)
    )
    await server.start_server()
    clients = {
        name: HubClient(Settings(str(server.make_url("/")).rstrip("/"), data["token"]))
        for name, data in actors.items()
    }
    return store, server, clients


async def close_api(store, server, clients):
    await asyncio.gather(*(client.close() for client in clients.values()))
    await server.close()
    store.close()


async def test_http_actor_auth_mentions_and_quiet_notes(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        before = len(store.show(p["id"], "alpha")["messages"])
        await clients["beta"].request(
            "POST", f"/projects/{p['id']}/notes", {"text": "Useful finding", "artifacts": []}
        )
        assert len(store.show(p["id"], "alpha")["messages"]) == before
        body = {
            "author_id": "alpha-user",
            "channel_id": "floor",
            "thread_id": "discussion",
            "post_id": "post-1",
            "text": "@beta ASK: Verify the dependency",
        }
        first = await clients["beta"].request("POST", "/mentions", body)
        repeated = await clients["beta"].request("POST", "/mentions", body)
        assert first["id"] == repeated["id"]
        body["thread_id"] = "unregistered"
        body["post_id"] = "post-2"
        assert await clients["beta"].request("POST", "/mentions", body) is None
        body["author_id"] = "unknown"
        assert await clients["beta"].request("POST", "/mentions", body) is None
        async with clients["alpha"].session.get(
            str(server.make_url("/projects")), headers={"Authorization": "Bearer invalid"}
        ) as r:
            assert r.status == 401
    finally:
        await close_api(store, server, clients)


class FakeMattermost:
    def __init__(self, actor):
        self.username = actor
        self.user_id = actor + "-user"
        self.posts = []

    async def get_post(self, thread):
        return {
            "id": thread,
            "channel_id": "home" if thread == "origin" else "floor",
            "message": "Project discussion",
        }

    async def create_post(self, channel, message, root_id=None):
        self.posts.append((channel, message, root_id))
        return {"id": "post-" + str(len(self.posts))}

    async def update_post(self, post, message):
        return {"id": post, "message": message}


def gateway_config():
    return SimpleNamespace(
        auto_rename_threads=False,
        inject_channel_context=False,
        show_thoughts=False,
        allow_user_ids=frozenset(),
        channel_ids=frozenset(),
        team_id="team",
        harness="grok",
        peer_channel_id="floor",
        post_max_lines=40,
        status_tool_lines=6,
        status_done_tool_lines=0,
        status_progress_lines=3,
    )


async def test_two_gateways_exchange_result_with_real_native_sessions(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateways, consumers, native_stores = [], [], []
    try:
        p = project(store)
        requested = ask(store, p["id"])
        for actor in ("alpha", "beta"):
            broker = AcpBroker(process_factory=spawn_fake_agent_process, backend="grok")
            native = ThreadStore(tmp_path / actor / "threads.db", harness="grok")
            await native.open()
            sessions = SessionService(
                broker,
                native,
                default_cwd=tmp_path,
                grok_home=tmp_path / "native",
                inject_channel_context=False,
            )
            gateway = MattermostGateway(gateway_config(), FakeMattermost(actor), sessions)
            consumer = CollaborationConsumer(gateway, clients[actor].settings)
            gateways.append(gateway)
            consumers.append(consumer)
            native_stores.append(native)
        beta_item = store.claim_next("beta", consumers[1].instance)
        await asyncio.wait_for(consumers[1].handle(beta_item), 15)
        original = store.get_message(requested["id"], "alpha")
        assert original["state"] == "done"
        assert original["evidence"]["session_id"]
        result = store.claim_next("alpha", consumers[0].instance)
        assert result["kind"] == "result"
        await asyncio.wait_for(consumers[0].handle(result), 15)
        assert store.get_message(result["id"], "alpha")["state"] == "done"
        assert await native_stores[0].get("origin") is not None
        assert await native_stores[1].get("discussion") is not None
        assert gateways[0].client.posts[-1][2] == "origin"
        assert len(store.show(p["id"], "alpha")["messages"]) == 2
    finally:
        await asyncio.gather(*(c.close() for c in consumers))
        await asyncio.gather(*(g.sessions.broker.stop() for g in gateways))
        await asyncio.gather(*(s.close() for s in native_stores))
        await close_api(store, server, clients)


def active_gateway(outcome="injected", *, steering=True, busy=True):
    record = SimpleNamespace(session_id="existing-native")
    broker = SimpleNamespace(
        capabilities=SimpleNamespace(steering=steering, raw={}), is_running=True, backend="claude"
    )
    sessions = SimpleNamespace(
        broker=broker,
        store=SimpleNamespace(get=AsyncMock(return_value=record)),
        is_busy=lambda _thread: busy,
        hint=AsyncMock(return_value=outcome),
        cancel=AsyncMock(),
    )
    gateway = MattermostGateway(gateway_config(), FakeMattermost("beta"), sessions)
    gateway.run_prompt = AsyncMock()
    return gateway


@pytest.mark.parametrize("case", ["unsupported", "timeout", "unknown"])
async def test_busy_delivery_preserves_unsupported_and_ambiguous_requests(tmp_path, case):
    store, server, clients = await api_fixture(tmp_path)
    consumer = None
    try:
        p = project(store)
        m = ask(store, p["id"])
        gateway = active_gateway(
            outcome="unknown" if case == "unknown" else "injected", steering=case != "unsupported"
        )
        if case == "timeout":
            gateway.sessions.hint.side_effect = TimeoutError("possibly accepted")
        consumer = CollaborationConsumer(gateway, clients["beta"].settings)
        await consumer.handle(store.claim_next("beta", consumer.instance))
        expected = "queued" if case == "unsupported" else "unconfirmed"
        assert store.get_message(m["id"], "alpha")["state"] == expected
        gateway.run_prompt.assert_not_awaited()
        assert gateway.sessions.hint.await_count == (0 if case == "unsupported" else 1)
    finally:
        if consumer:
            await consumer.close()
        await close_api(store, server, clients)


async def test_active_injection_uses_explicit_correlated_reply(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    consumer = None
    handling = None
    try:
        p = project(store)
        m = ask(store, p["id"])
        gateway = active_gateway(busy=False)  # Background native work, no foreground RPC.
        consumer = CollaborationConsumer(gateway, clients["beta"].settings)
        handling = asyncio.create_task(consumer.handle(store.claim_next("beta", consumer.instance)))
        async with asyncio.timeout(2):
            while not gateway.sessions.hint.await_count:
                await asyncio.sleep(0.01)
        await clients["beta"].request(
            "POST",
            f"/messages/{m['id']}/reply",
            {"result": "Verified dependency", "evidence": {"artifact": "https://example.com/fix"}},
        )
        await asyncio.wait_for(handling, 3)
        gateway.run_prompt.assert_not_awaited()
        assert store.get_message(m["id"], "alpha")["state"] == "done"
        assert store.claim_next("alpha", "owner")["text"] == "Verified dependency"
    finally:
        if handling and not handling.done():
            handling.cancel()
        if consumer:
            await consumer.close()
        await close_api(store, server, clients)


async def test_saturated_global_slot_serializes_thread_preparation(tmp_path):
    active = 0
    maximum = 0
    events = []

    async def ensure(thread, **_kwargs):
        assert active == 0, "Native preparation overlapped an active same-thread turn"
        events.append("ensure")
        return "native"

    async def prompt(_session, _text, **_kwargs):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        events.append("end")
        return TurnResult(
            session_id="native", stop_reason="end_turn", answer_text="done", thought_text=""
        )

    broker = SimpleNamespace(prompt_turn=prompt)
    sessions = SessionService(broker, SimpleNamespace(), default_cwd=tmp_path, max_parallel_turns=1)
    sessions.ensure_session = ensure
    await sessions._global_sem.acquire()
    tasks = [asyncio.create_task(sessions.run_turn("same-thread", str(i))) for i in range(2)]
    await asyncio.sleep(0.02)
    assert not sessions.is_busy("same-thread")
    sessions._global_sem.release()
    await asyncio.gather(*tasks)
    assert maximum == 1 and events == ["ensure", "end", "ensure", "end"]


def test_open_task_is_claimed_once_and_project_keys_bind_payload(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    p = project(store)
    task = ask(store, p["id"], recipient=None, kind="work")
    assert store.claim_open(task["id"], "beta")["recipient"] == "beta"
    with pytest.raises(ValueError, match="already claimed"):
        store.claim_open(task["id"], "gamma")
    body = {
        "title": "Stable",
        "objective": "Test",
        "members": ["alpha"],
        "channel_id": "floor",
        "thread_id": "stable-root",
        "key": "stable-key",
    }
    first = store.create_project("alpha", body)
    assert store.create_project("alpha", body)["id"] == first["id"]
    with pytest.raises(ValueError, match="idempotency"):
        store.create_project("alpha", {**body, "objective": "Changed"})
    store.close()


def test_expired_project_queues_cancel_control_and_stale_lease_is_fenced(tmp_path):
    now = [1000.0]
    store = CollaborationStore(
        tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: now[0]
    )
    p = project(store, max_seconds=100)
    m = ask(store, p["id"])
    store.claim_next("beta", "old")
    now[0] += 46
    store.expire()
    assert store.get_message(m["id"], "alpha")["state"] == "interrupted"
    with pytest.raises(PermissionError):
        store.update(m["id"], "beta", "old", "done", "stale")
    store.retry(m["id"], "alpha", "Verified no changes were applied")
    store.claim_next("beta", "new")
    now[0] += 55
    store.expire()
    assert store.show(p["id"], "alpha")["status"] == "cancelling"
    assert store.claim_next("beta", "new")["kind"] == "cancel"
    store.close()


def test_restart_preserves_queue_and_interrupts_active(tmp_path):
    path = tmp_path / "hub.db"
    store = CollaborationStore(path, {"alpha", "beta", "gamma"})
    p = project(store)
    active = ask(store, p["id"])
    queued = ask(store, p["id"], recipient="gamma", key="request-2")
    store.claim_next("beta", "old-instance")
    store.close()
    store = CollaborationStore(path, {"alpha", "beta", "gamma"})
    state = {m["id"]: m for m in store.show(p["id"], "alpha")["messages"]}
    assert state[active["id"]]["state"] == "interrupted"
    assert state[queued["id"]]["state"] == "queued"
    assert store.claim_next("beta", "new-instance") is None
    store.retry(active["id"], "alpha", "Verified that no change was applied")
    fresh = store.claim_next("beta", "new-instance")
    assert fresh["id"] == active["id"]
    with pytest.raises(PermissionError):
        store.update(active["id"], "beta", "old-instance", "done", "stale result")
    store.close()


def test_limits_dependencies_atomic_claim_and_notes(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    p = project(store, max_workers=1, max_turns=3, max_depth=1)
    first = ask(store, p["id"])
    second = ask(store, p["id"], recipient="gamma", key="second", dependencies=[first["id"]])
    assert store.claim_next("gamma", "gamma-instance") is None
    store.claim_next("beta", "beta-instance")
    assert store.claim_next("gamma", "gamma-instance") is None
    child = ask(store, p["id"], sender="beta", recipient="gamma", key="child", parent=first["id"])
    with pytest.raises(ValueError, match="depth"):
        ask(store, p["id"], sender="gamma", recipient="alpha", key="grandchild", parent=child["id"])
    store.reply(first["id"], "beta", "Done")
    assert store.claim_next("gamma", "gamma-instance")["id"] == second["id"]
    store.update(second["id"], "gamma", "gamma-instance", "done", "Done")
    store.claim_next("gamma", "gamma-instance")
    store.reply(child["id"], "gamma", "Done")
    with pytest.raises(ValueError, match="dispatch"):
        ask(store, p["id"], key="beyond-budget")
    store.note(
        p["id"], "alpha", "Decision: use the pinned dependency", ["https://example.com/change"]
    )
    assert len(store.show(p["id"], "alpha")["notes"]) == 1
    store.close()


def test_project_cancellation_requires_terminal_evidence(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    p = project(store)
    active = ask(store, p["id"])
    queued = ask(store, p["id"], recipient="gamma", key="queued")
    other = project(store)
    unrelated = ask(store, other["id"], key="unrelated")
    store.claim_next("beta", "worker")
    with pytest.raises(PermissionError):
        store.cancel(p["id"], "gamma")
    store.cancel(p["id"], "alpha")
    state = {m["id"]: m for m in store.show(p["id"], "alpha")["messages"]}
    assert state[active["id"]]["state"] == "cancelling"
    assert state[queued["id"]]["state"] == "cancelled"
    control = store.claim_next("beta", "worker")
    assert control["kind"] == "cancel"
    store.update(active["id"], "beta", "worker", "cancelled", "Native stop=cancelled")
    assert store.show(p["id"], "alpha")["status"] == "cancelled"
    assert store.claim_next("beta", "worker")["id"] == unrelated["id"]
    store.close()


async def test_cancel_between_claim_and_native_dispatch_starts_no_work(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    consumer = None
    try:
        p = project(store)
        m = ask(store, p["id"])
        gateway = active_gateway()
        consumer = CollaborationConsumer(gateway, clients["beta"].settings)
        claimed = store.claim_next("beta", consumer.instance)
        store.cancel(p["id"], "alpha")
        await consumer.handle(claimed)
        assert store.get_message(m["id"], "alpha")["state"] == "cancelled"
        gateway.run_prompt.assert_not_awaited()
        gateway.sessions.hint.assert_not_awaited()
        gateway.sessions.cancel.assert_not_awaited()
    finally:
        if consumer:
            await consumer.close()
        await close_api(store, server, clients)


async def test_completion_cancel_race_settles_without_claiming_effects_stopped(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    consumer = None
    try:
        p = project(store)
        m = ask(store, p["id"])
        gateway = active_gateway()
        consumer = CollaborationConsumer(gateway, clients["beta"].settings)
        claimed = store.claim_next("beta", consumer.instance)
        store.update(
            m["id"],
            "beta",
            consumer.instance,
            "dispatch",
            evidence={"session_id": "existing-native"},
            claim=claimed["claim"],
        )
        gateway._last_terminal["discussion"] = {
            "session_id": "existing-native",
            "stop_reason": "end_turn",
            "pending": False,
            "completed_at": store.clock() + 1,
            "answer": "Completed before cancellation",
        }
        store.cancel(p["id"], "alpha")
        control = store.claim_next("beta", consumer.instance)
        await asyncio.wait_for(consumer.handle(control), 3)
        final = store.get_message(m["id"], "alpha")
        assert final["state"] == "done" and final["completion_during_cancellation"]
        assert store.show(p["id"], "alpha")["status"] == "cancelled"
        gateway.sessions.cancel.assert_not_awaited()
    finally:
        if consumer:
            await consumer.close()
        await close_api(store, server, clients)


async def test_cancellation_preserves_a_replaced_native_session(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    consumer = None
    try:
        p = project(store)
        m = ask(store, p["id"])
        gateway = active_gateway()
        consumer = CollaborationConsumer(gateway, clients["beta"].settings)
        claimed = store.claim_next("beta", consumer.instance)
        store.update(
            m["id"],
            "beta",
            consumer.instance,
            "dispatch",
            evidence={"session_id": "prior-project-native"},
            claim=claimed["claim"],
        )
        store.cancel(p["id"], "alpha")
        control = store.claim_next("beta", consumer.instance)
        await consumer.handle(control)
        assert store.get_message(control["id"], "alpha")["state"] == "blocked"
        assert store.get_message(m["id"], "alpha")["state"] == "cancelling"
        gateway.sessions.cancel.assert_not_awaited()
    finally:
        if consumer:
            await consumer.close()
        await close_api(store, server, clients)


def test_attempt_nonce_fences_old_reply_from_same_gateway(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    p = project(store)
    m = ask(store, p["id"])
    first = store.claim_next("beta", "same-process")
    store.update(
        m["id"],
        "beta",
        "same-process",
        "unconfirmed",
        "Injection response lost",
        claim=first["claim"],
    )
    store.retry(m["id"], "alpha", "Verified the original request did not apply a change")
    second = store.claim_next("beta", "same-process")
    assert second["claim"] != first["claim"]
    with pytest.raises(PermissionError):
        store.reply(m["id"], "beta", "Old response", claim=first["claim"])
    with pytest.raises(PermissionError):
        store.update(m["id"], "beta", "same-process", "done", "Old response", claim=first["claim"])
    store.reply(m["id"], "beta", "Reconciled response", claim=second["claim"])
    assert store.get_message(m["id"], "alpha")["result"] == "Reconciled response"
    store.close()


async def test_forged_mention_does_not_create_work(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        from mattermost_acp.collaboration.client import HubError

        with pytest.raises(HubError, match="403"):
            await clients["beta"].request(
                "POST",
                "/mentions",
                {
                    "author_id": "gamma-user",
                    "channel_id": "floor",
                    "thread_id": "discussion",
                    "post_id": "post-1",
                    "text": "@beta ASK: A forged request",
                },
            )
        assert store.show(p["id"], "alpha")["messages"] == []
    finally:
        await close_api(store, server, clients)


def test_failed_result_prompt_preserves_status_parent_and_evidence(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    p = project(store)
    m = ask(store, p["id"])
    store.claim_next("beta", "worker")
    store.reply(
        m["id"],
        "beta",
        "Dependency is unavailable",
        state="blocked",
        evidence={"artifact": "https://example.com/failure"},
    )
    result = store.claim_next("alpha", "owner")
    consumer = object.__new__(CollaborationConsumer)
    prompt = consumer.prompt(result)
    assert "Result status: blocked" in prompt
    assert m["id"] in prompt and "https://example.com/failure" in prompt
    store.close()


async def test_final_native_dispatch_callback_can_cancel_without_prompt(tmp_path):
    broker = SimpleNamespace(prompt_turn=AsyncMock())
    sessions = SessionService(broker, SimpleNamespace(), default_cwd=tmp_path)
    sessions.ensure_session = AsyncMock(return_value="reserved-native")
    callback = AsyncMock(return_value=False)
    result = await sessions.run_turn("project", "do work", before_prompt=callback)
    assert result.stop_reason == "cancelled" and "before native dispatch" in result.answer_text
    callback.assert_awaited_once_with("reserved-native")
    broker.prompt_turn.assert_not_awaited()


def test_worker_bindings_are_exclusive_but_owner_origins_can_be_shared(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    first = project(store)
    second = project(store)
    assert first["bindings"]["alpha"] == second["bindings"]["alpha"]
    with pytest.raises(ValueError, match="another project"):
        store.bind(second["id"], "beta", {"thread_id": "discussion", "channel_id": "floor"})
    store.cancel(first["id"], "alpha")
    store.bind(second["id"], "beta", {"thread_id": "discussion", "channel_id": "floor"})
    store.close()


def test_cancelled_result_and_owner_request_do_not_stop_owner_native_work(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    p = project(store)
    work = ask(store, p["id"])
    store.claim_next("beta", "worker")
    store.reply(work["id"], "beta", "Verified")
    delivery = store.claim_next("alpha", "owner")
    question = ask(store, p["id"], sender="beta", recipient="alpha", key="owner-question")
    store.claim_next("alpha", "owner")
    store.cancel(p["id"], "alpha")
    state = store.show(p["id"], "alpha")
    assert store.get_message(delivery["id"], "alpha")["state"] == "cancelled"
    assert store.get_message(question["id"], "alpha")["state"] == "cancelled"
    assert not [m for m in state["messages"] if m["kind"] == "cancel"]
    assert state["status"] == "cancelled"
    store.close()


def test_cancellation_preserves_uncertainty_on_shutdown_and_terminal_failure(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    p = project(store)
    m = ask(store, p["id"])
    store.claim_next("beta", "worker")
    store.cancel(p["id"], "alpha")
    store.update(m["id"], "beta", "worker", "interrupted", "Gateway shutting down")
    assert store.show(p["id"], "alpha")["status"] == "cancelling"
    assert store.get_message(m["id"], "alpha")["state"] == "cancelling"
    store.update(
        m["id"],
        "beta",
        "worker",
        "failed",
        "Native limit reached",
        {"stop_reason": "max_tokens", "pending": False},
    )
    assert store.get_message(m["id"], "alpha")["state"] == "failed"
    assert store.show(p["id"], "alpha")["status"] == "cancelled"
    store.close()


def test_attempt_evidence_notes_reserved_keys_and_whitespace(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    p = project(store)
    m = ask(store, p["id"])
    store.claim_next("beta", "worker")
    store.update(m["id"], "beta", "worker", "dispatch", evidence={"session_id": "old"})
    store.update(m["id"], "beta", "worker", "queued")
    assert not store.get_message(m["id"], "alpha")["evidence"]
    now = store.clock()
    store.clock = lambda: now + 3
    store.claim_next("beta", "worker")
    claim = store.get_message(m["id"], "alpha")["claim"]
    store.update(m["id"], "beta", "worker", "done", "Verified.\n", claim=claim)
    store.update(m["id"], "beta", "worker", "done", "Verified.\n", claim=claim)
    with pytest.raises(ValueError, match="Reserved"):
        ask(store, p["id"], sender="beta", key="result:" + m["id"])
    first = store.note(p["id"], "alpha", "Decision", key="note-1")
    assert store.note(p["id"], "alpha", "Decision", key="note-1")["id"] == first["id"]
    with pytest.raises(ValueError, match="note idempotency"):
        store.note(p["id"], "alpha", "Other decision", key="note-1")
    store.close()


async def test_cancel_without_native_reservation_settles_and_reused_turn_is_preserved(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    consumer = CollaborationConsumer(active_gateway(), clients["beta"].settings)
    try:
        p = project(store)
        m = ask(store, p["id"])
        store.claim_next("beta", consumer.instance)
        store.cancel(p["id"], "alpha")
        await consumer.handle(store.claim_next("beta", consumer.instance))
        assert store.get_message(m["id"], "alpha")["state"] == "cancelled"
        consumer.gateway.sessions.cancel.assert_not_awaited()
        other = project(store)
        m = ask(store, other["id"])
        claim = store.claim_next("beta", consumer.instance)
        consumer.gateway._reports["existing-native"] = {"execution_id": "first"}
        await consumer.reserve(claim, "existing-native")
        consumer.gateway._reports["existing-native"] = {"execution_id": "later"}
        consumer.gateway._last_terminal["discussion-1"] = {
            "session_id": "existing-native",
            "execution_id": "later",
            "completed_at": store.clock() + 1,
            "pending": False,
            "stop_reason": "end_turn",
            "answer": "Unrelated later answer",
        }
        store.cancel(other["id"], "alpha")
        await consumer.handle(store.claim_next("beta", consumer.instance))
        assert store.get_message(m["id"], "alpha")["state"] == "cancelling"
        consumer.gateway.sessions.cancel.assert_not_awaited()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_cli_creation_scopes_keys_to_owner(tmp_path, monkeypatch):
    from mattermost_acp.collaboration.cli import execute, parser

    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        p["key"] = "shared-key"
        store.save_project(p)
        monkeypatch.setenv("MATTERMOST_PEER_CHANNEL_ID", "floor")
        args = parser().parse_args(
            [
                "create",
                "--title",
                "Peer-owned project",
                "--objective",
                "Verify the outcome",
                "--members",
                "alpha,beta",
                "--origin-thread",
                "beta-origin",
                "--origin-channel",
                "floor",
                "--discussion-thread",
                "other-root",
                "--key",
                "shared-key",
            ]
        )
        created = await execute(args, clients["beta"])
        assert created["owner"] == "beta" and created["id"] != p["id"]
    finally:
        await close_api(store, server, clients)


async def test_auto_completion_preserves_full_long_answer(tmp_path, monkeypatch):
    from pathlib import Path

    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(steering=False, busy=False)
    answer = "Long result.\n" * 2000
    gateway.run_prompt.return_value = {
        "answer": answer,
        "session_id": "native",
        "model": "model",
        "stop_reason": "end_turn",
        "pending": False,
    }
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        m = ask(store, p["id"])
        await consumer.handle(store.claim_next("beta", consumer.instance))
        saved = store.get_message(m["id"], "alpha")
        assert saved["state"] == "done"
        assert Path(saved["evidence"]["result_file"]).read_text() == answer
        assert len(saved["result"]) < 16000
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_ordinary_prompt_is_not_queued_behind_active_thread():
    gateway = active_gateway()
    gateway.run_prompt = MattermostGateway.run_prompt.__get__(gateway)
    lock = asyncio.Lock()
    gateway._prompt_locks["discussion"] = lock
    gateway._run_prompt = AsyncMock()
    async with lock:
        await gateway.run_prompt(thread_id="discussion", channel_id="floor", prompt="more work")
    gateway._run_prompt.assert_not_awaited()
    assert "Already working" in gateway.client.posts[-1][1]


async def test_hub_rejects_real_post_without_addressed_request_intent(tmp_path):
    from mattermost_acp.collaboration.client import HubError

    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    project(store)
    actors = {name: {"token": name * 32, "user_id": name + "-user"} for name in store.actors}
    body = {
        "author_id": "alpha-user",
        "channel_id": "floor",
        "thread_id": "discussion",
        "post_id": "real-post",
        "text": "@gamma ASK: Verify it",
    }

    async def verify(_post, _actor):
        return {
            "user_id": body["author_id"],
            "channel_id": body["channel_id"],
            "root_id": body["thread_id"],
            "message": body["text"],
        }

    async def verify_recipient(actor):
        return {"id": actor + "-user", "username": "actual-" + actor}

    server = TestServer(
        build_app(store, actors, verify_post=verify, verify_recipient=verify_recipient)
    )
    await server.start_server()
    client = HubClient(Settings(str(server.make_url("/")).rstrip("/"), actors["beta"]["token"]))
    try:
        with pytest.raises(HubError, match="403"):
            await client.request("POST", "/mentions", body)
        assert not store.list_projects("alpha")[0].get("messages")
        body["text"] = "@beta ASK: Actor label is not the recipient username"
        with pytest.raises(HubError, match="403"):
            await client.request("POST", "/mentions", body)
        body["text"] = "@actual-beta ASK: Verify the dependency"
        accepted = await client.request("POST", "/mentions", body)
        assert accepted["recipient"] == "beta"
    finally:
        await client.close()
        await server.close()
        store.close()


async def test_workspace_race_rejects_session_created_while_consumer_waited(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(steering=False, busy=False)
    gateway.run_prompt = MattermostGateway.run_prompt.__get__(gateway)
    workspace = tmp_path / "isolated"
    workspace.mkdir()
    p = project(store)
    store.bind(
        p["id"],
        "beta",
        {"thread_id": "discussion", "channel_id": "floor", "workspace": str(workspace)},
    )
    m = ask(store, p["id"])
    gateway.sessions.store.get.side_effect = [None, SimpleNamespace(cwd=str(tmp_path))]
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        await consumer.handle(store.claim_next("beta", consumer.instance))
        assert store.get_message(m["id"], "alpha")["state"] == "interrupted"
        gateway.sessions.hint.assert_not_awaited()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_cancelled_foreground_receipt_waits_for_pending_workers(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(steering=False, busy=False)
    gateway.run_prompt.return_value = {
        "answer": "",
        "session_id": "native",
        "stop_reason": "cancelled",
        "pending": True,
    }
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    consumer.injected_wait = AsyncMock()
    try:
        p = project(store)
        m = ask(store, p["id"])
        await consumer.handle(store.claim_next("beta", consumer.instance))
        assert store.get_message(m["id"], "alpha")["state"] == "running"
        consumer.injected_wait.assert_awaited_once()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_injected_request_does_not_own_ordinary_user_turn(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway()
    gateway._reports["existing-native"] = {
        "execution_id": "ordinary",
        "collaboration_project": None,
    }
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    try:
        p = project(store)
        m = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        await consumer.reserve(claimed, "existing-native")
        store.cancel(p["id"], "alpha")
        await consumer.handle(store.claim_next("beta", consumer.instance))
        assert store.get_message(m["id"], "alpha")["state"] == "cancelling"
        gateway.sessions.cancel.assert_not_awaited()
    finally:
        await consumer.close()
        await close_api(store, server, clients)
