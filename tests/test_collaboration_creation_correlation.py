"""Uncertain real discussion POSTs retain independently authenticated root admission."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from test_collaboration import active_gateway
from test_collaboration_regressions import creation_body

from mattermost_acp.collaboration.cli import execute, parser
from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.mm.client import PostedEvent


@pytest.fixture
async def correlation_api(tmp_path):
    state = SimpleNamespace(
        now=1000.0,
        roots={},
        posts=[],
        entered=asyncio.Event(),
        release=asyncio.Event(),
        delay=False,
        fail=False,
        disconnect=False,
    )

    async def post(request):
        payload = await request.json()
        owner = request.headers["Authorization"].removeprefix("Bearer mm-")
        root = {
            "id": "automatic-root",
            "user_id": owner + "-user",
            "channel_id": payload["channel_id"],
            "root_id": "",
            "create_at": int(state.now * 1000),
            "message": payload["message"],
            "props": payload["props"],
        }
        state.posts.append(payload)
        state.roots[root["id"]] = root
        state.entered.set()
        if state.delay:
            await state.release.wait()
        if state.disconnect:
            request.transport.close()
            return web.Response()
        if state.fail:
            return web.json_response({"error": "Synthetic unconfirmed response"}, status=503)
        return web.json_response(root)

    async def read(request):
        return web.json_response(state.roots[request.match_info["post"]])

    async def user(request):
        identity = request.match_info["user"]
        return web.json_response({"id": identity, "username": identity.removesuffix("-user")})

    mattermost = web.Application()
    mattermost.router.add_post("/api/v4/posts", post)
    mattermost.router.add_get("/api/v4/posts/{post}", read)
    mattermost.router.add_get("/api/v4/users/{user}", user)
    mm_server = TestServer(mattermost)
    await mm_server.start_server()
    actors = {}
    for name in ("alpha", "beta"):
        credential = tmp_path / (name + ".token")
        credential.write_text("mm-" + name)
        actors[name] = {
            "token": name * 32,
            "user_id": name + "-user",
            "mattermost_url": str(mm_server.make_url("/")).rstrip("/"),
            "mattermost_token_file": str(credential),
        }
    runtime = SimpleNamespace(state=state, actors=actors, store=None, hub=None, clients={})

    async def restart():
        await asyncio.gather(*(client.close() for client in runtime.clients.values()))
        if runtime.hub:
            await runtime.hub.close()
            runtime.store.close()
        runtime.store = CollaborationStore(
            tmp_path / "hub.db", set(actors), clock=lambda: state.now
        )
        runtime.hub = TestServer(build_app(runtime.store, actors))
        await runtime.hub.start_server()
        runtime.clients = {
            name: HubClient(Settings(str(runtime.hub.make_url("/")).rstrip("/"), actor["token"]))
            for name, actor in actors.items()
        }

    runtime.restart = restart
    await restart()
    try:
        yield runtime
    finally:
        state.release.set()
        await asyncio.gather(*(client.close() for client in runtime.clients.values()))
        await runtime.hub.close()
        runtime.store.close()
        await mm_server.close()


def mention_body(*, root="automatic-root", author="human-user"):
    return {
        "post_id": "addressed-reply",
        "author_id": author,
        "channel_id": "floor",
        "thread_id": root,
        "text": "@beta ASK: Check the dependency",
    }


@pytest.mark.parametrize("delayed", [False, True])
@pytest.mark.parametrize("disconnected", [False, True])
async def test_unconfirmed_post_root_cannot_start_ordinary_gateway_work(
    correlation_api, delayed, disconnected
):
    runtime = correlation_api
    state, store, clients = runtime.state, runtime.store, runtime.clients
    state.delay = delayed
    state.fail = True
    state.disconnect = disconnected
    pending = asyncio.create_task(clients["alpha"].request("POST", "/creations", creation_body()))
    consumer = None
    try:
        await asyncio.wait_for(state.entered.wait(), 2)
        intent = store.get_creation("alpha", "creation-key")
        assert intent["thread_id"] is None and intent["creation_id"]
        props = state.roots["automatic-root"]["props"]
        assert props == {
            "mattermost_acp_creation_id": intent["creation_id"],
            "mattermost_acp_creation_actor": "alpha",
        }
        gateway = active_gateway(busy=False)
        gateway.config.db_path = runtime.store.db.execute("PRAGMA database_list").fetchone()[2]
        gateway.config.collab_sidecar = False
        gateway.config.allow_user_ids = frozenset({"human-user"})
        gateway.config.all_channels = False
        gateway.config.exclude_channel_ids = frozenset()
        gateway.config.text_slash = False
        gateway.client.get_post = AsyncMock(return_value=state.roots["automatic-root"])
        consumer = CollaborationConsumer(gateway, clients["beta"].settings)
        gateway.collaboration = consumer
        await consumer.reconcile_inherited_claims()
        posted = PostedEvent(
            "addressed-reply", "floor", "human-user", mention_body()["text"], "automatic-root"
        )
        await gateway.handle_posted(posted)
        gateway.run_prompt.assert_not_awaited()
        assert not await gateway._ordinary_floor_reply_allowed(posted)
        assert store.get_creation("alpha", "creation-key")["thread_id"] == "automatic-root"
        assert store.list_projects("alpha") == []
        assert store.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
        state.release.set()
        with pytest.raises(HubError, match="409"):
            await asyncio.wait_for(pending, 2)
        completed = await clients["alpha"].request("POST", "/creations", creation_body())
        assert completed["thread_id"] == "automatic-root" and len(state.posts) == 1
    finally:
        state.release.set()
        await asyncio.gather(pending, return_exceptions=True)
        if consumer:
            await consumer.close()


@pytest.mark.parametrize(
    "mismatch", ["nonce", "actor", "author", "channel", "not-root", "missing-actor", "props"]
)
async def test_forged_root_metadata_cannot_bind_pending_creation(correlation_api, mismatch):
    runtime = correlation_api
    runtime.state.fail = True
    with pytest.raises(HubError, match="409"):
        await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    intent = runtime.store.get_creation("alpha", "creation-key")
    root = runtime.state.roots["automatic-root"]
    if mismatch == "nonce":
        root["props"]["mattermost_acp_creation_id"] = "forged"
    elif mismatch == "actor":
        root["props"]["mattermost_acp_creation_actor"] = "beta"
    elif mismatch == "missing-actor":
        root["props"].pop("mattermost_acp_creation_actor")
    elif mismatch == "author":
        root["user_id"] = "beta-user"
    elif mismatch == "channel":
        root["channel_id"] = "another-floor"
    elif mismatch == "not-root":
        root["root_id"] = "different-root"
    else:
        root["props"] = "unverified metadata"
    # Caller-supplied props cannot substitute for the independently read root.
    body = {
        **mention_body(),
        "props": {
            "mattermost_acp_creation_id": intent["creation_id"],
            "mattermost_acp_creation_actor": "alpha",
        },
    }
    with pytest.raises(HubError, match="403"):
        await runtime.clients["beta"].request("POST", "/mentions", body)
    assert runtime.store.get_creation("alpha", "creation-key") == intent
    assert runtime.store.list_projects("alpha") == []
    assert runtime.store.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0


async def test_unrelated_verified_root_retains_ordinary_fallback(correlation_api):
    runtime = correlation_api
    runtime.state.fail = True
    with pytest.raises(HubError, match="409"):
        await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    intent = runtime.store.get_creation("alpha", "creation-key")
    runtime.state.roots["ordinary-root"] = {
        "id": "ordinary-root",
        "channel_id": "floor",
        "root_id": "",
        "user_id": "human-user",
        "props": {},
    }
    body = mention_body(root="ordinary-root")
    body["props"] = runtime.state.roots["automatic-root"]["props"]
    assert await runtime.clients["beta"].request("POST", "/mentions", body) is None
    assert runtime.store.get_creation("alpha", "creation-key") == intent


async def test_pending_creation_keeps_existing_project_ask_available(correlation_api):
    runtime = correlation_api
    project = await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    pending, _ = runtime.store.prepare_creation(
        "alpha", {**creation_body(), "key": "another-creation"}
    )
    body = mention_body(author="alpha-user")
    runtime.state.roots[body["post_id"]] = {
        "id": body["post_id"],
        "channel_id": body["channel_id"],
        "root_id": project["thread_id"],
        "user_id": body["author_id"],
        "message": body["text"],
        "create_at": 1000001,
    }
    result = await runtime.clients["beta"].request("POST", "/mentions", body)
    assert result["project"] == project["id"]
    assert result["sender"] == "alpha" and result["recipient"] == "beta"
    assert result["state"] == "queued"
    assert runtime.store.get_creation("alpha", "another-creation") == pending


async def test_creation_correlation_and_origin_stop_survive_hub_restart(correlation_api):
    runtime = correlation_api
    runtime.state.fail = True
    with pytest.raises(HubError, match="409"):
        await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    before = runtime.store.get_creation("alpha", "creation-key")
    runtime.state.now = 1001.0
    await runtime.clients["alpha"].request(
        "POST", "/cancel-thread", {"thread_id": "origin", "cutoff": 1001.0}
    )
    await runtime.restart()
    assert runtime.store.get_creation("alpha", "creation-key") == before
    with pytest.raises(HubError, match="409"):
        await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    with pytest.raises(HubError, match="403"):
        await runtime.clients["beta"].request("POST", "/mentions", mention_body())
    completed = await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    assert completed["status"] == "cancelled" and completed["created"] == 1000.0
    assert completed["dispatched"] == 0 and len(runtime.state.posts) == 1
    assert runtime.store.show(completed["id"], "alpha")["messages"] == []


async def test_explicit_uncertain_root_requires_matching_creation_proof(correlation_api):
    runtime = correlation_api
    runtime.state.fail = True
    with pytest.raises(HubError, match="409"):
        await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    runtime.state.roots["unrelated-root"] = {
        "id": "unrelated-root",
        "user_id": "alpha-user",
        "channel_id": "floor",
        "root_id": "",
    }
    with pytest.raises(HubError, match="403"):
        await runtime.clients["alpha"].request(
            "POST", "/creations", {**creation_body(), "thread_id": "unrelated-root"}
        )
    assert runtime.store.get_creation("alpha", "creation-key")["thread_id"] is None
    completed = await runtime.clients["alpha"].request(
        "POST", "/creations", {**creation_body(), "thread_id": "automatic-root"}
    )
    repeated = await runtime.clients["alpha"].request("POST", "/creations", creation_body())
    assert repeated["id"] == completed["id"] and len(runtime.state.posts) == 1


async def test_delayed_post_response_cannot_replace_independently_correlated_root(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta"})
    entered, release = asyncio.Event(), asyncio.Event()
    root = {}

    async def post(actor, body):
        root.update(
            id="visible-root", channel_id="floor", user_id="alpha-user", props=body["props"]
        )
        entered.set()
        await release.wait()
        return {"id": "different-response-root", "channel_id": "floor"}

    async def read(_actor, _thread):
        return dict(root)

    actors = {name: {"token": name * 32, "user_id": name + "-user"} for name in store.actors}
    server = TestServer(build_app(store, actors, create_post=post, verify_creation=read))
    await server.start_server()
    clients = {
        name: HubClient(Settings(str(server.make_url("/")).rstrip("/"), data["token"]))
        for name, data in actors.items()
    }
    pending = asyncio.create_task(clients["alpha"].request("POST", "/creations", creation_body()))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(HubError, match="403"):
            await clients["beta"].request("POST", "/mentions", mention_body(root="visible-root"))
        release.set()
        with pytest.raises(HubError, match="409"):
            await asyncio.wait_for(pending, 2)
        assert store.get_creation("alpha", "creation-key")["thread_id"] == "visible-root"
        completed = await clients["alpha"].request("POST", "/creations", creation_body())
        assert completed["thread_id"] == "visible-root"
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        await asyncio.gather(*(client.close() for client in clients.values()))
        await server.close()
        store.close()


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("padding", ["", " \tfloor\n"])
async def test_cli_floor_normalization_matches_gateway_configuration(
    correlation_api, monkeypatch, explicit, padding
):
    runtime = correlation_api
    runtime.state.roots["existing-root"] = {
        "id": "existing-root",
        "channel_id": "floor",
        "root_id": "",
        "user_id": "human-user",
    }
    monkeypatch.setenv("MATTERMOST_PEER_CHANNEL_ID", padding or "floor")
    args = parser().parse_args(
        [
            "create",
            "--title",
            "Shared investigation",
            "--objective",
            "Verify the dependency",
            "--members",
            "alpha,beta",
            "--origin-thread",
            "origin",
            "--origin-channel",
            "home",
            "--key",
            "cli-key",
            *(["--discussion-thread", "existing-root"] if explicit else []),
        ]
    )
    completed = await execute(args, runtime.clients["alpha"])
    assert completed["channel_id"] == "floor"
    assert (await execute(args, runtime.clients["alpha"]))["id"] == completed["id"]
    assert len(runtime.state.posts) == (0 if explicit else 1)
