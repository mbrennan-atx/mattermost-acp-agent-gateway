"""Gateway startup and native evidence survive delivery preparation failures."""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import ClientPayloadError
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_dispatch_review import close_reports, native_gateway

from mattermost_acp.acp import TurnResult
from mattermost_acp.collaboration import consumer as consumer_module
from mattermost_acp.collaboration.client import HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.mm.client import MattermostError, PostedEvent


@pytest.mark.parametrize("failure", [None, "mkdir", "write_text", "chmod"])
async def test_long_foreground_reply_keeps_terminal_evidence_when_artifact_fails(
    tmp_path, monkeypatch, failure
):
    monkeypatch.setenv("HOME", str(tmp_path / "synthetic-home"))
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    broker.capabilities.steering = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    answer = "Long native evidence.\n" * 600 + "FINAL_NATIVE_EVIDENCE"
    receipt, failures = {}, []
    folder = Path.home() / ".local/state/mattermost-acp-collaboration/results"
    if failure is not None:
        operation = getattr(Path, failure)

        def fail_artifact(path, *args, **kwargs):
            if path == folder or folder in path.parents:
                failures.append(dict(gateway._last_terminal["discussion"]))
                raise OSError("synthetic artifact storage failure")
            return operation(path, *args, **kwargs)

        monkeypatch.setattr(Path, failure, fail_artifact)
    actual_prompt = gateway.run_prompt

    async def capture(**options):
        result = await actual_prompt(**options)
        receipt.update(result)
        return result

    gateway.run_prompt = capture
    try:
        p = project(store)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            store.reply(
                requested["id"], "beta", "Explicit correlated result", claim=claimed["claim"]
            )
            return TurnResult("existing-native", "end_turn", answer, "")

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        await asyncio.wait_for(consumer.handle(claimed), 3)
        settled = store.get_message(requested["id"], "beta")
        evidence = settled["evidence"]
        assert settled["state"] == "done"
        assert settled["result"] == "Explicit correlated result"
        assert settled["execution_pending"] is False
        assert evidence["execution_id"] == receipt["execution_id"]
        assert evidence["session_id"] == "existing-native"
        assert evidence["native_dispatched"] is True
        assert evidence["stop_reason"] == "end_turn"
        assert evidence["response_delivered"] is True
        assert evidence["foreground_answer"] == answer[:8000]
        assert "FINAL_NATIVE_EVIDENCE" in "\n".join(
            message for _channel, message, _root in gateway.client.posts
        )
        assert not consumer.owns_thread("discussion")
        assert not consumer.ownership
        if failure is None:
            assert Path(evidence["result_file"]).read_text() == answer
            assert "result_file_error" not in evidence
        else:
            assert len(failures) == 1
            assert failures[0]["session_id"] == "existing-native"
            assert failures[0]["native_dispatched"] is True
            assert failures[0]["stop_reason"] == "end_turn"
            assert failures[0]["execution_id"] == receipt["execution_id"]
            assert "synthetic artifact storage failure" in evidence["result_file_error"]
            assert "result_file" not in evidence
            monkeypatch.setattr(Path, failure, operation)
        await consumer.reconcile_ownership()
        gateway.sessions.run_turn.assert_awaited_once()
    finally:
        await consumer.close()
        await close_reports(gateway)
        await close_api(store, server, clients)


async def test_queued_mailbox_work_waits_for_floor_join_before_binding_lookup(
    tmp_path, monkeypatch
):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.all_channels = False
    gateway.config.auto_join_channels = False
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    gateway.client.get_me = AsyncMock()
    gateway._handle_pending_restart_bump = AsyncMock()
    broker.capabilities.steering = False
    join_entered, release_join, consumer_started, handled = (asyncio.Event() for _ in range(4))
    joined = False
    binding_reads = []
    actual_get_post = gateway.client.get_post

    async def join(channel):
        nonlocal joined
        assert channel == "floor"
        join_entered.set()
        await release_join.wait()
        joined = True

    async def get_post(thread):
        binding_reads.append(thread)
        if not joined:
            raise MattermostError("synthetic floor membership denied", status=403)
        return await actual_get_post(thread)

    async def events(*, stop):
        await stop.wait()
        if False:
            yield {}

    class ObservedConsumer(CollaborationConsumer):
        def start(self):
            consumer_started.set()
            super().start()

        async def handle(self, item):
            await super().handle(item)
            handled.set()

    gateway.client.join_channel = join
    gateway.client.get_post = get_post
    gateway.client.events = events
    monkeypatch.setattr(consumer_module, "CollaborationConsumer", ObservedConsumer)
    monkeypatch.setattr(Settings, "load", lambda: clients["beta"].settings)
    running = None
    try:
        p = project(store)
        requested = ask(store, p["id"])

        async def native(_thread, _prompt, **options):
            assert joined
            assert await options["before_prompt"]("existing-native")
            claimed = store.get_message(requested["id"], "beta")
            store.reply(requested["id"], "beta", "Joined floor before work", claim=claimed["claim"])
            return TurnResult("existing-native", "end_turn", "Verified joined-floor work", "")

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        running = asyncio.create_task(gateway.run())
        await asyncio.wait_for(join_entered.wait(), 2)
        assert not consumer_started.is_set()
        assert gateway.collaboration is None
        assert binding_reads == []
        assert store.get_message(requested["id"], "beta")["state"] == "queued"
        release_join.set()
        await asyncio.wait_for(handled.wait(), 3)
        settled = store.get_message(requested["id"], "beta")
        assert consumer_started.is_set()
        assert binding_reads == ["discussion"]
        assert settled["state"] == "done"
        assert settled["execution_pending"] is False
        assert settled["evidence"]["response_delivered"] is True
        assert not gateway.collaboration.owns_thread("discussion")
        gateway.sessions.run_turn.assert_awaited_once()
    finally:
        release_join.set()
        await gateway.close()
        if running:
            await asyncio.wait_for(running, 2)
        await close_reports(gateway)
        await close_api(store, server, clients)


def ordinary_floor_gateway(settings):
    gateway = active_gateway(busy=False)
    gateway.config.collab_sidecar = False
    gateway.config.all_channels = False
    gateway.config.channel_ids = frozenset({"home"})
    gateway.config.exclude_channel_ids = frozenset()
    gateway.config.allow_user_ids = frozenset({"alpha-user", "outside-user"})
    gateway.config.text_slash = True
    original_get_post = gateway.client.get_post

    async def root(thread):
        return {**await original_get_post(thread), "user_id": "beta-user"}

    gateway.client.get_post = root
    gateway.collaboration = CollaborationConsumer(gateway, settings)
    gateway.dispatch_command = AsyncMock()
    return gateway


@pytest.mark.parametrize("state", ["active", "budget", "expired", "cancelled"])
@pytest.mark.parametrize("author", ["alpha-user", "outside-user"])
@pytest.mark.parametrize("message", ["@beta Thanks", "Thanks"])
async def test_registered_discussion_acknowledgements_never_start_ordinary_work(
    tmp_path, state, author, message
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = ordinary_floor_gateway(clients["beta"].settings)
    try:
        p = project(store, max_turns=1)
        if state == "budget":
            ask(store, p["id"])
        elif state == "expired":
            store.clock = lambda: p["deadline"] + 1
        elif state == "cancelled":
            store.cancel(p["id"], "alpha")
        before = store.show(p["id"], "alpha")
        assert not gateway.collaboration.owns_thread("discussion")
        await gateway.handle_posted(
            PostedEvent(
                post_id="ordinary-ack",
                channel_id="floor",
                root_id="discussion",
                user_id=author,
                message=message,
            )
        )
        gateway.run_prompt.assert_not_awaited()
        gateway.dispatch_command.assert_not_awaited()
        assert gateway.client.posts == []
        assert store.show(p["id"], "alpha") == before
        assert gateway._inbound_tasks == set()
    finally:
        await gateway.collaboration.close()
        await close_api(store, server, clients)


async def test_registered_discussion_outside_recipient_membership_stays_quiet(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = ordinary_floor_gateway(clients["beta"].settings)
    try:
        p = store.create_project(
            "alpha",
            {
                "title": "Another project",
                "objective": "Keep separate project membership",
                "members": ["alpha", "gamma"],
                "channel_id": "floor",
                "thread_id": "discussion",
                "origin_channel_id": "home",
                "origin_thread_id": "origin",
                "limits": {},
            },
        )
        assert store.list_projects("beta") == []
        before = store.show(p["id"], "alpha")
        await gateway.handle_posted(
            PostedEvent(
                post_id="excluded-recipient-ack",
                channel_id="floor",
                root_id="discussion",
                user_id="alpha-user",
                message="@beta Thanks",
            )
        )
        gateway.run_prompt.assert_not_awaited()
        assert gateway.client.posts == []
        assert store.show(p["id"], "alpha") == before
    finally:
        await gateway.collaboration.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("message", ["/hint new work", "/model another-model", "/stop"])
async def test_registered_discussion_does_not_reach_ordinary_text_command_fallback(
    tmp_path, message
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = ordinary_floor_gateway(clients["beta"].settings)
    try:
        p = project(store)
        before = store.show(p["id"], "alpha")
        await gateway.handle_posted(
            PostedEvent(
                post_id="ordinary-control",
                channel_id="floor",
                root_id="discussion",
                user_id="alpha-user",
                message=message,
            )
        )
        gateway.dispatch_command.assert_not_awaited()
        gateway.run_prompt.assert_not_awaited()
        assert gateway.client.posts == []
        assert store.show(p["id"], "alpha") == before
    finally:
        await gateway.collaboration.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("message", ["@beta Thanks", "Thanks", "@beta ASK: Verify the dependency"])
async def test_unregistered_floor_reply_keeps_ordinary_native_routing(tmp_path, message):
    store, server, clients = await api_fixture(tmp_path)
    gateway = ordinary_floor_gateway(clients["beta"].settings)
    calls = []
    request = gateway.collaboration.hub.request

    async def observe(method, path, body=None):
        calls.append((method, path))
        return await request(method, path, body)

    gateway.collaboration.hub.request = observe
    try:
        p = project(store)
        before = store.show(p["id"], "alpha")
        await gateway.handle_posted(
            PostedEvent(
                post_id="unregistered-reply",
                channel_id="floor",
                root_id="unregistered",
                user_id="outside-user",
                message=message,
            )
        )
        gateway.run_prompt.assert_awaited_once()
        assert gateway.run_prompt.call_args.kwargs["thread_id"] == "unregistered"
        assert calls == [("POST", "/mentions")] * (2 if "ASK:" in message else 1)
        assert store.show(p["id"], "alpha") == before
        assert gateway.client.posts == []
    finally:
        await gateway.collaboration.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize(
    "channel,root,message",
    [
        ("home", "discussion", "Ordinary home question"),
        ("floor", "", "@beta Start ordinary assigned work"),
    ],
)
async def test_home_and_new_floor_roots_keep_routing_without_registration_probe(
    tmp_path, channel, root, message
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = ordinary_floor_gateway(clients["beta"].settings)
    gateway.collaboration.hub.request = AsyncMock(side_effect=AssertionError("must not query hub"))
    try:
        await gateway.handle_posted(
            PostedEvent(
                post_id="ordinary-root",
                channel_id=channel,
                root_id=root,
                user_id="alpha-user",
                message=message,
            )
        )
        gateway.run_prompt.assert_awaited_once()
        gateway.collaboration.hub.request.assert_not_awaited()
    finally:
        await gateway.collaboration.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize(
    "failure",
    [
        HubError("Registration rejected", status=403),
        HubError("Hub unavailable", status=500),
        TimeoutError("Hub did not respond"),
        ClientPayloadError("Response incomplete"),
        ValueError("Response is not JSON"),
    ],
)
async def test_unavailable_registration_check_cannot_wake_ordinary_project_reply(tmp_path, failure):
    store, server, clients = await api_fixture(tmp_path)
    gateway = ordinary_floor_gateway(clients["beta"].settings)
    gateway.collaboration.hub.request = AsyncMock(side_effect=failure)
    try:
        await gateway.handle_posted(
            PostedEvent(
                post_id="uncertain-ack",
                channel_id="floor",
                root_id="discussion",
                user_id="alpha-user",
                message="@beta Thanks",
            )
        )
        gateway.run_prompt.assert_not_awaited()
        gateway.dispatch_command.assert_not_awaited()
        assert gateway.client.posts == []
        assert gateway._inbound_tasks == set()
    finally:
        await gateway.collaboration.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("response", [{}, [], False, {"id": "unexpected-work"}])
@pytest.mark.parametrize("message", ["@beta Thanks", "@beta ASK: Check the dependency"])
async def test_registration_probe_requires_exact_unregistered_response(tmp_path, response, message):
    store, server, clients = await api_fixture(tmp_path)
    gateway = ordinary_floor_gateway(clients["beta"].settings)
    gateway.collaboration.hub.request = AsyncMock(return_value=response)
    try:
        await gateway.handle_posted(
            PostedEvent(
                post_id="malformed-ack",
                channel_id="floor",
                root_id="discussion",
                user_id="alpha-user",
                message=message,
            )
        )
        gateway.run_prompt.assert_not_awaited()
        assert gateway.client.posts == []
    finally:
        await gateway.collaboration.close()
        await close_api(store, server, clients)
