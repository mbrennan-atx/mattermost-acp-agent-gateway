"""Creation admission and delegation preserve distinct native callers."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from test_collaboration import active_gateway, api_fixture, ask, close_api
from test_collaboration_regressions import (
    close_creation,
    creation_api,
    creation_body,
    sidecar_project,
)

from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.store import CollaborationStore


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
