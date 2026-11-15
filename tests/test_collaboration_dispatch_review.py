"""Dispatch and command boundaries use local hubs and synthetic native adapters."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestServer
from fake_agent import spawn_fake_agent_process
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_regressions import (
    auth,
    maintenance_client,
    maintenance_gateway,
    memory_broker,
)

from mattermost_acp import harness_switch
from mattermost_acp.acp import AcpBroker, SessionUpdate
from mattermost_acp.collaboration.cli import execute, parser
from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.mm.client import PostedEvent
from mattermost_acp.mm.reporting import TurnReporter
from mattermost_acp.session_service import SessionService


@pytest.mark.parametrize("failure", [False, True])
async def test_resume_keeps_local_native_admission_closed_until_hub_accepts(failure):
    gateway = maintenance_gateway()
    gateway.draining = gateway.sealed = True
    gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
    gateway._run_prompt = AsyncMock()
    entered, release = asyncio.Event(), asyncio.Event()

    async def admission(mode):
        assert mode == "normal"
        entered.set()
        await release.wait()
        if failure:
            raise TimeoutError("Lost admission response")

    gateway.collaboration.set_admission = admission
    client = await maintenance_client(gateway)
    task = asyncio.create_task(
        client.post("/maintenance", json={"action": "resume"}, headers=auth())
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await gateway.run_prompt(thread_id="discussion", channel_id="floor", prompt="work")
        gateway._run_prompt.assert_not_awaited()
        assert gateway.draining and gateway.sealed
        release.set()
        response = await task
        assert response.status == (409 if failure else 200)
        assert gateway.draining is failure and gateway.sealed is failure
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await client.close()


async def test_sealed_native_admission_is_closed_even_without_draining_flag():
    gateway = maintenance_gateway()
    gateway.sealed = True
    gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
    gateway._run_prompt = AsyncMock()
    await gateway.run_prompt(thread_id="discussion", channel_id="floor", prompt="work")
    gateway._run_prompt.assert_not_awaited()


async def test_ordinary_followup_preserves_unresolved_project_report_and_terminal_receipt():
    gateway = active_gateway(busy=False)
    gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
    gateway._run_prompt = AsyncMock()
    owned = idle_report()
    receipt = {"execution_id": "project-execution", "completed_at": 10}
    gateway._reports["project-native"] = owned
    gateway._last_terminal["discussion"] = receipt
    gateway.collaboration = SimpleNamespace(owns_thread=lambda thread: thread == "discussion")
    result = await gateway.run_prompt(
        thread_id="discussion", channel_id="floor", prompt="An ordinary follow-up"
    )
    assert result is None
    gateway._run_prompt.assert_not_awaited()
    assert gateway._reports["project-native"] is owned
    assert gateway._last_terminal["discussion"] is receipt
    assert "still pending" in gateway.client.posts[-1][1]


async def test_repeated_harness_switches_share_one_idle_restart_waiter(monkeypatch):
    gateway = maintenance_gateway()
    gateway.config.harness = "codex"
    gateway.client.user_id = "synthetic-owner"
    plan = SimpleNamespace(old_harness="codex", new_harness="claude", model="opus")
    monkeypatch.setattr(harness_switch, "load_profiles", lambda: {})
    monkeypatch.setattr(harness_switch, "load_roster", lambda: {})
    monkeypatch.setattr(harness_switch, "find_agent", lambda *a, **kw: {"username": "alpha"})
    monkeypatch.setattr(harness_switch, "apply_switch", lambda *a, **kw: plan)
    gate = asyncio.Event()

    async def waiter(*args):
        await gate.wait()

    gateway._restart_when_idle = AsyncMock(side_effect=waiter)
    reply = AsyncMock()
    try:
        await gateway._cmd_harness("discussion", "floor", "human", "claude", reply)
        first = gateway._harness_restart_task
        await gateway._cmd_harness("discussion", "floor", "human", "claude", reply)
        assert gateway._harness_restart_task is first
        gate.set()
        await asyncio.wait_for(first, 2)
        gateway._restart_when_idle.assert_awaited_once()
    finally:
        gate.set()
        await asyncio.gather(*gateway._slash_tasks, return_exceptions=True)


def native_gateway(tmp_path):
    gateway = active_gateway(busy=False)
    broker, writer = memory_broker()
    sessions = SessionService(broker, SimpleNamespace(), default_cwd=tmp_path)
    sessions.ensure_session = AsyncMock(return_value="existing-native")
    sessions.status = AsyncMock(return_value={"session_id": "existing-native"})
    gateway.sessions = sessions
    broker.update_observer = gateway._observe_background_update
    gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
    return gateway, broker, writer


def idle_report():
    return {
        "execution_id": "prior-execution",
        "collaboration_project": "project",
        "thread_id": "discussion",
        "channel_id": "floor",
        "active": False,
        "foreground_complete": True,
        "reporter": TurnReporter(),
        "task": None,
    }


async def native_tool(broker, *, state="in_progress"):
    await broker._dispatch(
        {
            "method": "session/update",
            "params": {
                "sessionId": "existing-native",
                "update": {
                    "sessionUpdate": "tool_call" if state == "in_progress" else "tool_call_update",
                    "toolCallId": "background-tool",
                    "title": "Existing native work",
                    "status": state,
                },
            },
        }
    )


async def close_reports(gateway):
    tasks = [report["task"] for report in gateway.live_reports() if report.get("task")]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def test_background_notification_before_guarded_write_is_tracked_synchronously(tmp_path):
    gateway, broker, writer = native_gateway(tmp_path)
    callback_release, guard_checked = asyncio.Event(), asyncio.Event()
    actual_turn = gateway.sessions.run_turn

    async def delayed_updates(thread, text, **options):
        on_update, guard = options["on_update"], options["dispatch_guard"]

        async def delayed(update):
            await callback_release.wait()
            await on_update(update)

        def checked():
            guard_checked.set()
            return guard()

        options.update(on_update=delayed, dispatch_guard=checked)
        return await actual_turn(thread, text, **options)

    gateway.sessions.run_turn = delayed_updates
    turn = None
    try:
        async with broker._write_lock:
            turn = asyncio.create_task(
                gateway.run_prompt(
                    thread_id="discussion",
                    channel_id="floor",
                    prompt="New project work",
                    author_id="actor",
                    author_name="actor",
                    before_native=AsyncMock(return_value=True),
                )
            )
            async with asyncio.timeout(2):
                while "existing-native" not in broker.prompt_subscriptions or not broker._pending:
                    await asyncio.sleep(0)
            await native_tool(broker)
        await asyncio.wait_for(guard_checked.wait(), 2)
        assert writer.messages == [], "Background work must block the actual session/prompt write"
        assert gateway.background_pending("discussion")
        callback_release.set()
        assert await asyncio.wait_for(turn, 2) == {"queued": True}
        assert gateway.background_pending("discussion")
    finally:
        callback_release.set()
        if turn and not turn.done():
            turn.cancel()
        if turn:
            await asyncio.gather(turn, return_exceptions=True)
        await close_reports(gateway)


async def test_rejected_preparation_preserves_background_tool_in_prior_idle_report(tmp_path):
    gateway, broker, writer = native_gateway(tmp_path)
    old = idle_report()
    gateway._reports["existing-native"] = old
    original_post = gateway.client.create_post
    injected = False

    async def post_with_background(*args, **kwargs):
        nonlocal injected
        if not injected:
            injected = True
            await native_tool(broker)
        return await original_post(*args, **kwargs)

    gateway.client.create_post = post_with_background
    reserve = AsyncMock(return_value=True)
    try:
        result = await gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="New project work",
            author_id="actor",
            author_name="actor",
            before_native=reserve,
        )
        assert result == {"queued": True} and writer.messages == []
        reserve.assert_not_awaited()
        assert gateway.background_pending("discussion")
        assert await gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="A competing attempt",
            author_id="actor",
            author_name="actor",
            before_native=reserve,
        ) == {"queued": True}
        assert writer.messages == []
        await native_tool(broker, state="completed")
        assert not gateway.background_pending("discussion")
    finally:
        await close_reports(gateway)


async def test_stop_persists_boundary_and_cancels_projects_before_stalled_native_stop(
    tmp_path, monkeypatch
):
    store, server, clients = await api_fixture(tmp_path)
    now = [1000.0]
    store.clock = lambda: now[0]
    monkeypatch.setattr("mattermost_acp.collaboration.consumer.time.time", lambda: now[0])
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    gateway.collaboration = consumer
    native_entered, native_release, hub_finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    recorded = []
    actual_request = consumer.hub.request

    async def hub_request(method, path, body=None):
        result = await actual_request(method, path, body)
        if path == "/cancel-thread":
            assert body["cutoff"] == 1001.0
            hub_finished.set()
        return result

    async def stalled_cancel(_thread):
        recorded.append(json.loads(consumer.stops_path.read_text()))
        now[0] = 1002.0
        native_entered.set()
        await native_release.wait()
        return True

    consumer.hub.request = hub_request
    gateway.sessions.cancel = stalled_cancel
    command = None
    try:
        old = project(store)
        now[0] = 1001.0
        command = asyncio.create_task(
            gateway.dispatch_command(
                thread_id="origin",
                channel_id="home",
                cmd="stop",
                arg="",
                user_id="human",
                user_name="human",
            )
        )
        await asyncio.wait_for(native_entered.wait(), 2)
        newer = project(store)
        await asyncio.wait_for(hub_finished.wait(), 2)
        assert recorded == [{"origin": 1001.0}]
        assert not command.done()
        assert store.show(old["id"], "alpha")["status"] == "cancelled"
        assert store.show(newer["id"], "alpha")["status"] == "active"
        native_release.set()
        await asyncio.wait_for(command, 2)
    finally:
        native_release.set()
        if command and not command.done():
            command.cancel()
        if command:
            await asyncio.gather(command, return_exceptions=True)
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("boundary", ["unknown", "tool", "owned", "foreground", "inbound"])
async def test_automatic_harness_restart_preserves_unfinished_or_unknown_work(boundary):
    gateway = maintenance_gateway()
    gateway._request_restart = AsyncMock(return_value="Restart scheduled")
    if boundary == "unknown":
        del gateway.sessions.broker.native_boundary_idle
    elif boundary == "tool":
        owned = idle_report()
        owned["reporter"].active_tools.add("background-tool")
        gateway._reports["existing-native"] = owned
    elif boundary == "owned":
        gateway.collaboration.ownership = {("request", "claim"): None}
    elif boundary == "foreground":
        gateway.sessions.busy_threads.add("discussion")
    else:
        gateway._inbound_tasks.add(object())
    task = gateway._track_control(
        gateway._restart_when_idle("discussion", "floor", "human", timeout_s=0),
        "automatic-harness-restart",
    )
    await asyncio.wait_for(task, 2)
    gateway._request_restart.assert_not_awaited()
    assert gateway.client.posts


async def test_automatic_harness_restart_excludes_its_own_control_at_known_idle_boundary():
    gateway = maintenance_gateway()
    gateway._request_restart = AsyncMock(return_value="Restart scheduled")
    task = gateway._track_control(
        gateway._restart_when_idle("discussion", "floor", "human", timeout_s=0),
        "automatic-harness-restart",
    )
    await asyncio.wait_for(task, 2)
    gateway._request_restart.assert_awaited_once_with(
        thread_id="discussion", channel_id="floor", user_id="human", reason="harness_switch"
    )


@pytest.mark.parametrize("sidecar", [False, True])
@pytest.mark.parametrize("message", [" @beta ASK: Verify this", "@beta ASK: Check @gamma's change"])
async def test_addressed_floor_prefix_matches_hub_with_extra_question_mentions(
    tmp_path, sidecar, message
):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    actors = {name: {"token": name * 32, "user_id": name + "-user"} for name in store.actors}

    async def verify_post(_post_id, _actor):
        return {
            "user_id": "alpha-user",
            "channel_id": "floor",
            "root_id": "discussion",
            "message": message,
        }

    async def verify_recipient(actor):
        return {"id": actor + "-user", "username": actor}

    server = TestServer(
        build_app(store, actors, verify_post=verify_post, verify_recipient=verify_recipient)
    )
    await server.start_server()
    client = HubClient(Settings(str(server.make_url("/")).rstrip("/"), actors["beta"]["token"]))
    gateway = active_gateway(busy=False)
    gateway.config.collab_sidecar = sidecar
    gateway.config.allow_user_ids = frozenset({"human"})
    consumer = CollaborationConsumer(gateway, client.settings)
    gateway.collaboration = consumer
    posted = PostedEvent("addressed-post", "floor", "alpha-user", message, "discussion")
    try:
        p = project(store)
        await gateway.handle_posted(posted)
        await gateway.handle_posted(posted)
        messages = store.show(p["id"], "alpha")["messages"]
        assert len(messages) == 1
        assert messages[0]["recipient"] == "beta" and messages[0]["sender"] == "alpha"
        assert messages[0]["text"] == message.strip() and messages[0]["state"] == "queued"
        gateway.run_prompt.assert_not_awaited()
    finally:
        await consumer.close()
        await client.close()
        await server.close()
        store.close()


@pytest.mark.parametrize("state", ["queued", "running", "done"])
async def test_task_claim_retry_returns_current_task_for_same_actor(tmp_path, state):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        task = ask(store, p["id"], recipient=None, kind="work")
        args = parser().parse_args(["claim", task["id"]])
        accepted = await execute(args, clients["beta"])
        assert accepted["recipient"] == "beta" and accepted["state"] == "queued"
        if state in {"running", "done"}:
            claimed = store.claim_next("beta", "native-worker")
            assert claimed["id"] == task["id"]
            if state == "done":
                store.reply(task["id"], "beta", "Verified result", claim=claimed["claim"])
        current = store.get_message(task["id"], "beta")
        assert await execute(args, clients["beta"]) == current
        assert current["state"] == state
        with pytest.raises(HubError):
            await execute(args, clients["gamma"])
    finally:
        await close_api(store, server, clients)


async def test_older_queued_worker_snapshot_cannot_undo_newer_terminal_activity(tmp_path):
    gateway, broker, writer = native_gateway(tmp_path)

    def prompt_reply(payload):
        message = json.loads(payload)
        writer.messages.append(message)
        broker._pending[message["id"]].set_result({"stopReason": "end_turn"})

    writer.write = prompt_reply
    broker.subagents.observe(
        SessionUpdate(
            session_id="existing-native",
            kind="subagent_spawned",
            raw={"subagentSessionId": "native-child", "name": "Synthetic worker"},
        )
    )
    entered, release = asyncio.Event(), asyncio.Event()
    actual_turn = gateway.sessions.run_turn
    observed = []

    async def delayed_snapshot(thread, text, **options):
        on_update = options["on_update"]

        async def delayed(update):
            if update.kind == "subagent_activity" and update.raw["state"] == "running":
                entered.set()
                await release.wait()
                await on_update(update)
                current = gateway._reports["existing-native"]["reporter"]
                observed.append(current.workers["native-child"]["state"])
                return
            await on_update(update)

        options["on_update"] = delayed
        return await actual_turn(thread, text, **options)

    gateway.sessions.run_turn = delayed_snapshot
    turn = None
    try:
        async with broker._write_lock:
            turn = asyncio.create_task(
                gateway.run_prompt(
                    thread_id="discussion",
                    channel_id="floor",
                    prompt="New project work",
                    author_id="actor",
                    author_name="actor",
                    before_native=AsyncMock(return_value=True),
                )
            )
            await asyncio.wait_for(entered.wait(), 2)
            reporter = gateway._reports["existing-native"]["reporter"]
            assert reporter.has_pending_work()
            await broker._dispatch(
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": "existing-native",
                        "update": {
                            "sessionUpdate": "subagent_update",
                            "sessionId": "native-child",
                            "state": {"state": "completed"},
                        },
                    },
                }
            )
            assert not reporter.has_pending_work()
            release.set()
            async with asyncio.timeout(2):
                while not observed:
                    await asyncio.sleep(0)
            assert observed == ["completed"]
        receipt = await asyncio.wait_for(turn, 2)
        assert receipt["stop_reason"] == "end_turn"
        assert len(writer.messages) == 1 and writer.messages[0]["method"] == "session/prompt"
        assert not gateway.background_pending("discussion")
    finally:
        release.set()
        if turn and not turn.done():
            turn.cancel()
        if turn:
            await asyncio.gather(turn, return_exceptions=True)
        await close_reports(gateway)


async def test_native_spawn_latches_unknown_boundary_after_acp_process_exits():
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    assert broker.native_boundary_idle and not broker.is_running
    try:
        await broker.start()
        assert broker.is_running and not broker.native_boundary_idle
    finally:
        await broker.stop()
    assert not broker.is_running
    assert not broker.native_boundary_idle, (
        "Process exit does not prove descendant native goals idle"
    )


async def test_creation_retry_rejects_changed_configured_floor(tmp_path, monkeypatch):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        p["key"] = "stable-project"
        store.save_project(p)
        args = parser().parse_args(
            [
                "create",
                "--title",
                p["title"],
                "--objective",
                p["objective"],
                "--members",
                ",".join(p["members"]),
                "--origin-thread",
                p["origin_thread_id"],
                "--origin-channel",
                p["origin_channel_id"],
                "--discussion-thread",
                p["thread_id"],
                "--key",
                p["key"],
            ]
        )
        monkeypatch.setenv("MATTERMOST_PEER_CHANNEL_ID", "floor")
        assert (await execute(args, clients["alpha"]))["id"] == p["id"]
        monkeypatch.setenv("MATTERMOST_PEER_CHANNEL_ID", "another-floor")
        with pytest.raises(ValueError, match="channel|floor|creation key"):
            await execute(args, clients["alpha"])
        assert store.show(p["id"], "alpha")["channel_id"] == "floor"
        assert len(store.list_projects("alpha")) == 1
    finally:
        await close_api(store, server, clients)
