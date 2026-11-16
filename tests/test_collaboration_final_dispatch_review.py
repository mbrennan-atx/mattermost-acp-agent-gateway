"""Creation prerequisites, overlapping members and maintenance restart ordering."""

import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestServer
from test_collaboration import ask, project
from test_collaboration_cold_restart import assert_no_native, cold_runtime, restart_control
from test_collaboration_regressions import creation_body, maintenance_client

from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore

pytest_plugins = ["test_collaboration_creation_admission"]


@pytest.mark.parametrize(
    "missing", ["token-only", "unreadable-token", "empty-token", "invalid-url", "user-id"]
)
async def test_no_post_configuration_failure_leaves_ordinary_floor_and_key_usable(
    tmp_path, missing
):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta"})
    settings = {"token": "alpha" * 32}
    if missing != "token-only":
        credential = tmp_path / "mattermost.token"
        if missing != "unreadable-token":
            credential.write_text("" if missing == "empty-token" else "synthetic-mattermost-token")
        settings.update(
            mattermost_url="not-an-http-url" if missing == "invalid-url" else "http://127.0.0.1:9",
            mattermost_token_file=str(credential),
            user_id="" if missing == "user-id" else "alpha-user",
        )
    actors = {"alpha": settings, "beta": {"token": "beta" * 32}}
    server = TestServer(build_app(store, actors))
    await server.start_server()
    clients = {
        name: HubClient(Settings(str(server.make_url("/")).rstrip("/"), actor["token"]))
        for name, actor in actors.items()
    }
    try:
        with pytest.raises(HubError, match="409"):
            await clients["alpha"].request("POST", "/creations", creation_body())
        assert store.db.execute("SELECT COUNT(*) FROM creations").fetchone()[0] == 0
        assert store.unresolved_creations("floor") == []
        assert store.list_projects("alpha") == []
        ordinary = {
            "post_id": "unrelated-reply",
            "author_id": "human-user",
            "channel_id": "floor",
            "thread_id": "ordinary-root",
            "text": "ordinary reply",
        }
        assert await clients["beta"].request("POST", "/mentions", ordinary) is None
        # This documented token-only profile can reuse the same key with an
        # explicit root. There was no automatic POST requiring correlation.
        settings.clear()
        settings["token"] = "alpha" * 32
        completed = await clients["alpha"].request(
            "POST", "/creations", {**creation_body(), "thread_id": "existing-root"}
        )
        repeated = await clients["alpha"].request("POST", "/creations", creation_body())
        assert completed["id"] == repeated["id"]
        assert completed["thread_id"] == "existing-root"
        assert await clients["beta"].request("POST", "/mentions", ordinary) is None
    finally:
        await asyncio.gather(*(client.close() for client in clients.values()))
        await server.close()
        store.close()


@pytest.mark.parametrize("first_peer", ["beta", "gamma"])
async def test_shared_root_requests_select_a_project_with_both_members(admission_api, first_peer):
    store, clients, state = admission_api
    peers = [first_peer, "gamma" if first_peer == "beta" else "beta"]
    projects = {}
    for peer in peers:
        projects[peer] = await clients["alpha"].request(
            "POST",
            "/creations",
            {
                **creation_body(),
                "members": ["alpha", peer],
                "key": "shared-root-" + peer,
                "thread_id": "discussion",
            },
        )
    for peer in reversed(peers):
        text = "@" + peer + " ASK: Verify this project's dependency"
        post_id = "shared-request-" + peer
        state["posts"][post_id] = {
            "id": post_id,
            "user_id": "alpha-user",
            "channel_id": "floor",
            "root_id": "discussion",
            "message": text,
            "create_at": 1000001,
        }
        body = {
            "post_id": post_id,
            "author_id": "alpha-user",
            "channel_id": "floor",
            "thread_id": "discussion",
            "text": text,
        }
        message = await clients[peer].request("POST", "/mentions", body)
        repeated = await clients[peer].request("POST", "/mentions", body)
        assert message["id"] == repeated["id"]
        assert message["project"] == projects[peer]["id"]
        assert message["sender"] == "alpha" and message["recipient"] == peer
    assert all(len(store.show(item["id"], "alpha")["messages"]) == 1 for item in projects.values())
    # Both actors appear on this root, but no individual project contains them
    # together. Their unrelated memberships cannot authorize a request.
    text = "@beta ASK: Cross-project request"
    state["posts"]["cross-membership"] = {
        "id": "cross-membership",
        "user_id": "gamma-user",
        "channel_id": "floor",
        "root_id": "discussion",
        "message": text,
        "create_at": 1000002,
    }
    with pytest.raises(HubError, match="403"):
        await clients["beta"].request(
            "POST",
            "/mentions",
            {
                "post_id": "cross-membership",
                "author_id": "gamma-user",
                "channel_id": "floor",
                "thread_id": "discussion",
                "text": text,
            },
        )
    assert store.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 2


async def test_resume_cannot_reopen_mailbox_during_real_delayed_restart(tmp_path, monkeypatch):
    async with cold_runtime(tmp_path, monkeypatch) as (store, clients, gateway, consumer, _debt):
        restart_module = importlib.import_module("mattermost_acp.commands.restart")
        sleeping, release = asyncio.Event(), asyncio.Event()
        real_sleep = asyncio.sleep
        exits = []

        async def delayed_sleep(seconds):
            if seconds == 0.8:
                sleeping.set()
                await release.wait()
            else:
                await real_sleep(seconds)

        monkeypatch.setattr(
            restart_module,
            "asyncio",
            SimpleNamespace(sleep=delayed_sleep, create_task=asyncio.create_task),
        )
        monkeypatch.setattr(restart_module.os, "_exit", exits.append)
        gateway._request_restart = type(gateway)._request_restart.__get__(gateway)
        gateway.sessions.broker.stop = AsyncMock()
        client = await maintenance_client(gateway)
        headers = {"Authorization": "Bearer " + consumer.hub.settings.token}
        shutdowns = []
        try:
            await asyncio.wait_for(restart_control(gateway), 2)
            await asyncio.wait_for(sleeping.wait(), 2)
            shutdowns = list(gateway._slash_tasks)
            assert len(shutdowns) == 1 and gateway.maintenance_state()["restart_pending"]
            response = await client.post("/maintenance", json={"action": "resume"}, headers=headers)
            assert response.status == 409
            assert gateway.draining and gateway.sealed
            item = project(store)
            work = ask(store, item["id"])
            queued = store.get_message(work["id"], "beta")
            assert (
                await clients["beta"].request(
                    "GET", f"/mailbox?instance={consumer.instance}&wait=0"
                )
                is None
            )
            await gateway.run_prompt(thread_id="discussion", channel_id="floor", prompt="Late work")
            assert_no_native(gateway, consumer)
            assert store.get_message(work["id"], "beta") == queued
            release.set()
            await asyncio.wait_for(asyncio.gather(*shutdowns), 2)
            gateway.sessions.broker.stop.assert_awaited_once()
            assert exits == [0]
            assert not gateway.maintenance_state()["restart_pending"]
            assert store.get_message(work["id"], "beta") == queued
        finally:
            release.set()
            for task in shutdowns:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*shutdowns, return_exceptions=True)
            await client.close()


@pytest.mark.parametrize("reason", ["unknown-native", "pending-inbound"])
@pytest.mark.parametrize("action", ["drain", "resume"])
async def test_deferred_restart_notice_preserves_newer_operator_transition(
    tmp_path, monkeypatch, reason, action
):
    async with cold_runtime(tmp_path, monkeypatch) as (store, clients, gateway, consumer, _debt):
        entered, release = asyncio.Event(), asyncio.Event()
        modes = []
        actual = consumer.hub.request

        async def observe(method, path, body=None):
            value = await actual(method, path, body)
            if path == "/mailbox/admission":
                modes.append(body["mode"])
            return value

        async def notice(*args, **kwargs):
            entered.set()
            await release.wait()
            return {}

        consumer.hub.request = observe
        gateway._post = AsyncMock(side_effect=notice)
        if reason == "unknown-native":
            gateway.sessions.broker.native_boundary_idle = False
        else:
            gateway._inbound_tasks.add("retained-inbound")
        client = await maintenance_client(gateway)
        headers = {"Authorization": "Bearer " + consumer.hub.settings.token}
        task = restart_control(gateway)
        try:
            await asyncio.wait_for(entered.wait(), 2)
            response = await client.post("/maintenance", json={"action": action}, headers=headers)
            assert response.status == 200
            expected = (action == "drain", False)
            assert (gateway.draining, gateway.sealed) == expected
            operator_modes = list(modes)
            assert operator_modes[-1] == ("controls" if action == "drain" else "normal")
            release.set()
            await asyncio.wait_for(task, 2)
            assert (gateway.draining, gateway.sealed) == expected
            assert modes == operator_modes
            gateway._request_restart.assert_not_awaited()
            if action == "drain":
                item = project(store)
                work = ask(store, item["id"])
                queued = store.get_message(work["id"], "beta")
                assert (
                    await clients["beta"].request(
                        "GET", f"/mailbox?instance={consumer.instance}&wait=0"
                    )
                    is None
                )
                await gateway.run_prompt(
                    thread_id="discussion", channel_id="floor", prompt="Late work"
                )
                assert store.get_message(work["id"], "beta") == queued
            assert_no_native(gateway, consumer)
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await client.close()
