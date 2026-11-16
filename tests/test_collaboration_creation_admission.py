"""Retained discussion roots and real post timestamps fence project admission."""

import asyncio
import json

import pytest
from aiohttp.test_utils import TestServer
from test_collaboration_regressions import creation_body

from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore


@pytest.fixture
async def admission_api(tmp_path):
    state = {"now": 1000.0, "posts": {}, "roots": {}, "readback": None}
    store = CollaborationStore(
        tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: state["now"]
    )
    actors = {name: {"token": name * 32, "user_id": name + "-user"} for name in store.actors}

    async def post(actor, body):
        root = {
            "id": "discussion",
            "channel_id": body["channel_id"],
            "user_id": actors[actor]["user_id"],
            "props": dict(body["props"]),
            "create_at": int(state.get("mm_now", state["now"]) * 1000),
        }
        state["roots"]["discussion"] = root
        return root

    async def readback(actor, thread):
        if state["readback"] is not None:
            return {**state["roots"].get(thread, {}), **await state["readback"](actor, thread)}
        return {
            "id": thread,
            "channel_id": "floor",
            "root_id": "",
            "create_at": 1,
            **state["roots"].get(thread, {}),
        }

    async def verify_post(post_id, _actor):
        return dict(state["posts"][post_id])

    async def recipient(actor):
        return {"id": actor + "-user", "username": actor}

    server = TestServer(
        build_app(
            store,
            actors,
            create_post=post,
            verify_creation=readback,
            verify_post=verify_post,
            verify_recipient=recipient,
        )
    )
    await server.start_server()
    clients = {
        name: HubClient(Settings(str(server.make_url("/")).rstrip("/"), settings["token"]))
        for name, settings in actors.items()
    }
    try:
        yield store, clients, state
    finally:
        await asyncio.gather(*(client.close() for client in clients.values()))
        await server.close()
        store.close()


def source_post(state, post_id, timestamp, *, thread="discussion", project_id=None):
    scope = " [" + project_id + "]" if project_id else ""
    text = "@beta ASK" + scope + ": Verify the generation"
    state["posts"][post_id] = {
        "id": post_id,
        "user_id": "alpha-user",
        "channel_id": "floor",
        "root_id": thread,
        "message": text,
        "create_at": timestamp,
    }
    return {
        "post_id": post_id,
        "author_id": "alpha-user",
        "channel_id": "floor",
        "thread_id": thread,
        "text": text,
    }


@pytest.mark.parametrize("author", ["human-user", "alpha-user"])
async def test_visible_root_is_registered_during_creation_readback(admission_api, author):
    store, clients, state = admission_api
    entered, release = asyncio.Event(), asyncio.Event()

    async def readback(_actor, thread):
        entered.set()
        await release.wait()
        return {"id": thread, "channel_id": "floor", "root_id": ""}

    state["readback"] = readback
    pending = asyncio.create_task(clients["alpha"].request("POST", "/creations", creation_body()))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        intent = json.loads(store.db.execute("SELECT data FROM creations").fetchone()[0])
        assert intent["thread_id"] == "discussion" and intent["status"] == "posted"
        body = source_post(state, "during-readback", 1000001)
        body["author_id"] = author
        with pytest.raises(HubError, match="403"):
            await clients["beta"].request("POST", "/mentions", body)
        assert store.list_projects("alpha") == []
        assert store.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
        # Unregistered discussions retain their ordinary fallback.
        unrelated = dict(body, thread_id="ordinary-root", author_id="human-user")
        assert await clients["beta"].request("POST", "/mentions", unrelated) is None
    finally:
        release.set()
        await asyncio.wait_for(pending, 2)


async def test_failed_readback_retains_registration_and_origin_stop(admission_api):
    store, clients, state = admission_api

    async def failed(_actor, _thread):
        raise ValueError("Unconfirmed read-back")

    state["readback"] = failed
    with pytest.raises(HubError, match="409"):
        await clients["alpha"].request("POST", "/creations", creation_body())
    state["now"] = 1001.0
    assert (
        await clients["alpha"].request(
            "POST", "/cancel-thread", {"thread_id": "origin", "cutoff": 1001.0}
        )
        == []
    )
    body = source_post(state, "after-failed-readback", 1001001)
    body["author_id"] = "human-user"
    with pytest.raises(HubError, match="403"):
        await clients["beta"].request("POST", "/mentions", body)
    state.update(now=1003.0, readback=None)
    completed = await clients["alpha"].request("POST", "/creations", creation_body())
    assert completed["created"] == 1000.0 and completed["status"] == "cancelled"
    assert completed["dispatched"] == 0
    assert store.show(completed["id"], "alpha")["messages"] == []


@pytest.mark.parametrize("status", ["reserved", "posted"])
def test_retained_root_registration_and_stop_boundary_survive_restart(tmp_path, status):
    path = tmp_path / "hub.db"
    now = [1000.0]
    store = CollaborationStore(path, {"alpha", "beta"}, clock=lambda: now[0])
    intent, _ = store.prepare_creation("alpha", {**creation_body(), "thread_id": "discussion"})
    intent["status"] = status
    store.save_creation("alpha", intent["body"]["key"], intent)
    store.cancel_thread("alpha", "origin", cutoff=1001.0)
    store.close()
    now[0] = 1003.0
    reopened = CollaborationStore(path, {"alpha", "beta"}, clock=lambda: now[0])
    try:
        with pytest.raises(PermissionError, match="creation requires reconciliation"):
            reopened.mention(None, "floor", "discussion", "beta", "ordinary reply", "post:delayed")
        assert (
            reopened.mention(None, "different-floor", "discussion", "beta", "reply", "post:other")
            is None
        )
        saved = json.loads(reopened.db.execute("SELECT data FROM creations").fetchone()[0])
        completed = reopened.finish_creation("alpha", saved)
        assert completed["created"] == 1000.0 and completed["status"] == "cancelled"
        assert reopened.show(completed["id"], "alpha")["messages"] == []
    finally:
        reopened.close()


async def test_old_never_admitted_post_cannot_enter_replacement_generation(admission_api):
    store, clients, state = admission_api
    first = await clients["alpha"].request("POST", "/creations", creation_body())
    old_body = source_post(state, "old-never-admitted", 1001000)
    store.cancel(first["id"], "alpha")
    state["now"] = 1002.0
    replacement = await clients["alpha"].request(
        "POST", "/creations", {**creation_body(), "key": "replacement", "thread_id": "discussion"}
    )
    with pytest.raises(HubError, match="discussion generation"):
        await clients["beta"].request("POST", "/mentions", {**old_body, "create_at": 9999999999999})
    assert replacement["created"] == 1002.0 and replacement["dispatched"] == 0
    assert store.show(replacement["id"], "alpha")["messages"] == []
    assert store.show(first["id"], "alpha")["messages"] == []
    fresh = source_post(state, "new-generation", 1002001, project_id=replacement["id"])
    admitted = await clients["beta"].request("POST", "/mentions", fresh)
    assert admitted["project"] == replacement["id"] and admitted["state"] == "queued"


async def test_post_during_creation_delay_uses_intent_admission_not_finalization(admission_api):
    store, clients, state = admission_api
    entered, release = asyncio.Event(), asyncio.Event()

    async def readback(_actor, thread):
        entered.set()
        await release.wait()
        return {"id": thread, "channel_id": "floor", "root_id": "", "create_at": 1}

    state["readback"] = readback
    # Binding an existing root must not use the root's historical creation time.
    pending = asyncio.create_task(
        clients["alpha"].request(
            "POST", "/creations", {**creation_body(), "thread_id": "existing-root"}
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        state["now"] = 1001.0
        body = source_post(state, "during-create", 1001000, thread="existing-root")
        state["now"] = 1005.0
        release.set()
        created = await asyncio.wait_for(pending, 2)
        admitted = await clients["beta"].request("POST", "/mentions", body)
        assert created["created"] == 1000.0 and admitted["project"] == created["id"]
        assert store.get_project(created["id"], "alpha")["dispatched"] == 1
    finally:
        release.set()
        if not pending.done():
            await asyncio.wait_for(pending, 2)


async def test_already_admitted_post_keeps_original_receipt_after_root_reuse(admission_api):
    store, clients, state = admission_api
    first = await clients["alpha"].request("POST", "/creations", creation_body())
    body = source_post(state, "already-admitted", 1000001)
    admitted = await clients["beta"].request("POST", "/mentions", body)
    store.cancel(first["id"], "alpha")
    state["now"] = 1002.0
    second = await clients["alpha"].request(
        "POST", "/creations", {**creation_body(), "key": "next", "thread_id": "discussion"}
    )
    # A retry returns its durable receipt before fresh-post timestamp admission.
    state["posts"]["already-admitted"]["create_at"] = None
    repeated = await clients["beta"].request("POST", "/mentions", body)
    assert repeated["id"] == admitted["id"] and repeated["state"] == "cancelled"
    assert store.show(second["id"], "alpha")["messages"] == []
    assert store.get_project(second["id"], "alpha")["dispatched"] == 0


@pytest.mark.parametrize("timestamp", [None, True, 0, -1, "1000001", 1000001.0])
async def test_unverified_post_creation_time_refuses_fresh_admission(admission_api, timestamp):
    store, clients, state = admission_api
    created = await clients["alpha"].request("POST", "/creations", creation_body())
    body = source_post(state, "invalid-time", timestamp)
    with pytest.raises(HubError, match="creation time could not be verified"):
        await clients["beta"].request("POST", "/mentions", body)
    assert store.show(created["id"], "alpha")["messages"] == []
    assert store.get_project(created["id"], "alpha")["dispatched"] == 0


async def test_post_admission_matches_mattermost_millisecond_precision(admission_api):
    store, clients, state = admission_api
    state["now"] = 1000.0009
    created = await clients["alpha"].request("POST", "/creations", creation_body())
    old = source_post(state, "previous-ms", 999999)
    with pytest.raises(HubError, match="predates"):
        await clients["beta"].request("POST", "/mentions", old)
    same_ms = source_post(state, "same-ms", 1000000)
    admitted = await clients["beta"].request("POST", "/mentions", same_ms)
    assert admitted["project"] == created["id"] and admitted["state"] == "queued"


@pytest.mark.parametrize("hub_now", [10.0, 1000000.0])
async def test_reused_discussion_scope_is_independent_of_hub_clock(admission_api, hub_now):
    store, clients, state = admission_api
    state["mm_now"] = 5000.0
    first = await clients["alpha"].request("POST", "/creations", creation_body())
    old = source_post(state, "old-generation", 5000001, project_id=first["id"])
    store.cancel(first["id"], "alpha")
    state["now"] = hub_now
    replacement = await clients["alpha"].request(
        "POST", "/creations", {**creation_body(), "key": "new-scope", "thread_id": "discussion"}
    )
    with pytest.raises(HubError):
        await clients["beta"].request("POST", "/mentions", old)
    unscoped = source_post(state, "unscoped-generation", 5000002)
    with pytest.raises(HubError, match="discussion generation"):
        await clients["beta"].request("POST", "/mentions", unscoped)
    fresh = source_post(state, "fresh-generation", 5000003, project_id=replacement["id"])
    # A supplied JSON scope cannot substitute for the scope in the real post.
    admitted = await clients["beta"].request("POST", "/mentions", {**fresh, "project": first["id"]})
    assert admitted["project"] == replacement["id"]
    assert len(store.show(replacement["id"], "alpha")["messages"]) == 1
