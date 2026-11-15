"""Creation admission and delegation preserve distinct native callers."""

import asyncio
import re
from unittest.mock import AsyncMock

import pytest
from aiohttp import ClientPayloadError, web
from aiohttp.test_utils import TestServer
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_regressions import (
    close_creation,
    creation_api,
    creation_body,
    sidecar_project,
)

from mattermost_acp.collaboration.cli import execute, parser
from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.mm.client import MattermostError, PostedEvent


@pytest.mark.parametrize("field", ["title", "objective"])
async def test_cli_creation_retry_normalizes_surrounding_field_whitespace(
    tmp_path, monkeypatch, field
):
    post = AsyncMock(return_value={"id": "created-root", "channel_id": "floor"})
    store, server, client = await creation_api(tmp_path / "hub.db", post)
    monkeypatch.setenv("MATTERMOST_PEER_CHANNEL_ID", "floor")
    values = {"title": "Build investigation", "objective": "Verify the dependency"}
    values[field] = " " + values[field] + " "
    args = parser().parse_args(
        [
            "create",
            "--title",
            values["title"],
            "--objective",
            values["objective"],
            "--members",
            "alpha,beta",
            "--origin-thread",
            "origin",
            "--origin-channel",
            "home",
            "--key",
            "same-project",
        ]
    )
    try:
        first = await execute(args, client)
        repeated = await execute(args, client)
        assert repeated["id"] == first["id"]
        assert repeated[field] == values[field].strip()
        post.assert_awaited_once()
        setattr(args, field, "Changed substantive content")
        with pytest.raises(ValueError, match="Conflicting project creation key"):
            await execute(args, client)
        assert len(store.list_projects("alpha")) == 1
    finally:
        await close_creation(store, server, client)


async def test_original_owner_ask_does_not_borrow_its_sidecars_native_parent(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(steering=False, busy=True)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.collab_sidecar = True
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    gateway.collaboration = consumer
    published = {}

    async def read(post_id):
        if post_id == "origin":
            return {"id": "origin", "channel_id": "home", "root_id": ""}
        return published

    async def publish(channel, message, *, root_id):
        published.update(id="notice", channel_id=channel, root_id=root_id, message=message)
        return published

    gateway.client.get_post = AsyncMock(side_effect=read)
    gateway.client.create_post = AsyncMock(side_effect=publish)
    try:
        p = sidecar_project(store)
        inbound = ask(store, p["id"], sender="gamma", recipient="alpha", key="sidecar-work")
        store.claim_next("alpha", consumer.instance)
        original = ask(store, p["id"], sender="alpha", recipient="beta", key="original-home-ask")
        assert original["parent"] is None and original["depth"] == 0
        peer = store.claim_next("beta", "peer-worker")
        store.reply(original["id"], "beta", "Verified separate work", claim=peer["claim"])
        delivery = store.claim_next("alpha", consumer.instance)
        await consumer.handle(delivery)
        gateway.client.create_post.assert_awaited_once()
        assert published["channel_id"] == "home" and published["root_id"] == "origin"
        assert store.get_message(delivery["id"], "alpha")["state"] == "done"
        assert store.get_message(inbound["id"], "alpha")["state"] == "running"
        gateway.run_prompt.assert_not_awaited()
        delegated = ask(
            store,
            p["id"],
            sender="alpha",
            recipient="beta",
            key="explicit-project-child",
            parent=inbound["id"],
        )
        assert delegated["parent"] == inbound["id"] and delegated["depth"] == 1
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_addressed_floor_request_is_independent_of_running_delegation(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = sidecar_project(store)
        p["limits"]["max_depth"] = 1
        with store.transaction():
            store.save_project(p)
        first = ask(store, p["id"], recipient="gamma", key="parent")
        store.claim_next("gamma", "peer")
        inbound = ask(
            store, p["id"], sender="gamma", recipient="alpha", parent=first["id"], key="child"
        )
        store.claim_next("alpha", "sidecar")
        posted = await clients["beta"].request(
            "POST",
            "/mentions",
            {
                "post_id": "post-1",
                "author_id": "alpha-user",
                "channel_id": "floor",
                "thread_id": "discussion",
                "text": "@beta ASK: Verify the dependency",
            },
        )
        assert posted["parent"] is None and posted["depth"] == 0
        assert posted["state"] == "queued"
        with pytest.raises(ValueError, match="depth limit"):
            ask(
                store,
                p["id"],
                recipient="beta",
                parent=inbound["id"],
                key="explicit-too-deep",
            )
        assert store.get_message(inbound["id"], "alpha")["state"] == "running"
    finally:
        await close_api(store, server, clients)


@pytest.mark.parametrize("phase", ["post", "read-back"])
async def test_origin_stop_cancels_creation_admitted_before_posting_finishes(tmp_path, phase):
    entered, release = asyncio.Event(), asyncio.Event()
    now = [1000.0]

    async def post(actor, body):
        if phase == "post":
            entered.set()
            await release.wait()
        return {"id": "discussion", "channel_id": "floor"}

    async def verify(actor, thread):
        if phase == "read-back" and not release.is_set():
            entered.set()
            await release.wait()
        return {"id": thread, "channel_id": "floor", "root_id": ""}

    store, server, client = await creation_api(tmp_path / "hub.db", post, verify)
    store.clock = lambda: now[0]
    pending = asyncio.create_task(client.request("POST", "/creations", creation_body()))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert store.list_projects("alpha") == []
        now[0] = 1001.0
        assert (
            await client.request(
                "POST", "/cancel-thread", {"thread_id": "origin", "cutoff": now[0]}
            )
            == []
        )
        now[0] = 1002.0
        release.set()
        completed = await asyncio.wait_for(pending, 2)
        assert completed["status"] == "cancelled"
        assert completed["created"] == 1000.0 and completed["dispatched"] == 0
        with pytest.raises(ValueError, match="inactive or expired"):
            ask(store, completed["id"])
        now[0] = 1003.0
        later = await client.request(
            "POST",
            "/creations",
            {**creation_body(), "key": "newer-work", "thread_id": "new-discussion"},
        )
        assert later["status"] == "active" and later["created"] == 1003.0
        store.cancel_thread("alpha", "origin", cutoff=1001.0)
        assert store.get_project(later["id"], "alpha")["status"] == "active"
    finally:
        release.set()
        if not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await close_creation(store, server, client)


def test_pending_creation_stop_boundary_survives_hub_restart_and_older_retry(tmp_path):
    path = tmp_path / "hub.db"
    now = [1000.0]
    store = CollaborationStore(path, {"alpha", "beta"}, clock=lambda: now[0])
    intent, _ = store.prepare_creation("alpha", creation_body())
    store.cancel_thread("alpha", "origin", cutoff=1001.0)
    store.cancel_thread("alpha", "origin", cutoff=1002.0)
    store.cancel_thread("alpha", "origin", cutoff=1001.0)
    store.close()
    now[0] = 1003.0
    store = CollaborationStore(path, {"alpha", "beta"}, clock=lambda: now[0])
    try:
        completed = store.finish_creation("alpha", {**intent, "thread_id": "discussion"})
        assert completed["status"] == "cancelled" and completed["created"] == 1000.0
        assert store.db.execute("SELECT cutoff FROM origin_stops").fetchone()[0] == 1002.0
        later, _ = store.prepare_creation("alpha", {**creation_body(), "key": "later"})
        active = store.finish_creation("alpha", {**later, "thread_id": "later-discussion"})
        assert active["status"] == "active"
    finally:
        store.close()


@pytest.mark.parametrize("failure", ["lost-receipt", "server-error", "timeout", "payload"])
async def test_uncertain_floor_admission_reports_uncertainty_without_repeating_work(
    tmp_path, failure
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.allow_user_ids = frozenset({"alpha-user"})
    gateway.config.all_channels = False
    gateway.config.exclude_channel_ids = frozenset()
    gateway.config.text_slash = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    original = consumer.hub.request
    calls = []

    async def lost(method, path, body=None):
        calls.append(path)
        await original(method, path, body)
        if failure == "lost-receipt":
            raise HubError("Response lost after admission")
        if failure == "server-error":
            raise HubError("Collaboration HTTP 500: @gamma ASK: unrelated text", status=500)
        if failure == "timeout":
            raise TimeoutError("Response did not arrive")
        raise ClientPayloadError("Incomplete response body")

    consumer.hub.request = lost
    posted = PostedEvent(
        post_id="post-1",
        channel_id="floor",
        user_id="alpha-user",
        message="@beta ASK: Verify the dependency",
        root_id="discussion",
    )
    try:
        p = project(store)
        await gateway.handle_posted(posted)
        gateway.run_prompt.assert_not_awaited()
        assert calls == ["/mentions"] and len(gateway.client.posts) == 1
        channel, notice, root = gateway.client.posts[0]
        assert (channel, root) == ("floor", "discussion")
        assert "unconfirmed" in notice and "rejected" not in notice
        assert "@" not in notice and "ASK:" not in notice
        saved = store.show(p["id"], "alpha")
        assert saved["dispatched"] == 1 and len(saved["messages"]) == 1
        assert saved["messages"][0]["state"] == "queued"
        assert saved["messages"][0]["attempt"] == 0
        await gateway.handle_posted(
            PostedEvent(
                post_id="quiet-note",
                channel_id="floor",
                user_id="gamma-user",
                message="Progress is recorded in the project.",
                root_id="discussion",
            )
        )
        assert calls == ["/mentions"] and len(gateway.client.posts) == 1
        gateway.run_prompt.assert_not_awaited()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_failed_rejection_notice_cannot_authorize_ordinary_native_fallback(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.allow_user_ids = frozenset({"alpha-user"})
    gateway.config.all_channels = False
    gateway.config.exclude_channel_ids = frozenset()
    gateway.config.text_slash = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    gateway.client.create_post = AsyncMock(side_effect=MattermostError("Notice unavailable"))
    posted = PostedEvent(
        post_id="post-1",
        channel_id="floor",
        user_id="alpha-user",
        message="@beta ASK: Verify the dependency",
        root_id="discussion",
    )
    try:
        p = project(store, max_turns=1)
        ask(store, p["id"])
        before = store.show(p["id"], "alpha")
        await gateway.handle_posted(posted)
        gateway.run_prompt.assert_not_awaited()
        gateway.client.create_post.assert_awaited_once()
        assert store.show(p["id"], "alpha") == before
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("content", ["plain", "many-mentions", "only-mentions"])
async def test_creation_bounds_rendered_post_and_preserves_full_objective(tmp_path, content):
    posts = []

    async def create(request):
        body = await request.json()
        if len(body["message"]) > 16383:
            return web.json_response({"error": "Message exceeds Mattermost limit"}, status=400)
        post = {**body, "id": "created-root", "root_id": ""}
        posts.append(post)
        return web.json_response(post)

    async def read(request):
        assert request.match_info["post"] == "created-root"
        return web.json_response(posts[0])

    mattermost = web.Application()
    mattermost.router.add_post("/api/v4/posts", create)
    mattermost.router.add_get("/api/v4/posts/{post}", read)
    mm_server = TestServer(mattermost)
    await mm_server.start_server()
    token_file = tmp_path / "mattermost-token"
    token_file.write_text("synthetic-mattermost-token")
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta"})
    actors = {
        name: {
            "token": name * 32,
            "user_id": name + "-user",
            "mattermost_url": str(mm_server.make_url("/")).rstrip("/"),
            "mattermost_token_file": str(token_file),
        }
        for name in store.actors
    }
    server = TestServer(build_app(store, actors))
    await server.start_server()
    client = HubClient(Settings(str(server.make_url("/")).rstrip("/"), actors["alpha"]["token"]))
    objective = {
        "plain": "x" * 16000,
        "many-mentions": ("x" * 39 + "@") * 400,
        "only-mentions": "@" * 16000,
    }[content]
    body = {**creation_body(), "title": "@gamma " * 28, "objective": objective}
    try:
        created = await client.request("POST", "/creations", body)
        repeated = await client.request("POST", "/creations", body)
        assert repeated["id"] == created["id"] and len(posts) == 1
        assert created["objective"] == objective and created["dispatched"] == 0
        assert store.get_project(created["id"], "alpha")["objective"] == objective
        message = posts[0]["message"]
        assert len(message) <= 15000
        assert message.startswith("Project collaboration: @\u200bgamma")
        assert message.endswith("Full objective is saved in the project record.")
        assert not re.search(r"@(?!\u200b)", message)
    finally:
        await close_creation(store, server, client)
        await mm_server.close()
