"""Project isolation, durable cancellation boundaries and bounded polling."""

import asyncio
import json
import sqlite3
import sys
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from test_collaboration import ask, project

from mattermost_acp.collaboration import store as store_module
from mattermost_acp.collaboration.cli import execute
from mattermost_acp.collaboration.client import HubClient, Settings
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.validate import check


@pytest.mark.parametrize("owner_mode", ["discussion", "sidecar"])
def test_native_owner_discussion_cannot_be_shared_between_projects(tmp_path, owner_mode):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    if owner_mode == "sidecar":
        store.sidecar_actors = {"alpha"}
    body = {
        "title": "First",
        "objective": "Work",
        "channel_id": "floor",
        "thread_id": "shared",
        "origin_channel_id": "floor" if owner_mode == "sidecar" else "home",
        "origin_thread_id": "shared" if owner_mode == "sidecar" else "origin",
        "members": ["alpha", "beta"],
        "owner_in_discussion": owner_mode == "discussion",
    }
    try:
        first = store.create_project("alpha", body)
        with pytest.raises(ValueError, match="another project"):
            store.create_project(
                "alpha", {**body, "title": "Second", "members": ["alpha", "gamma"]}
            )
        assert store.get_project(first["id"], "alpha")["status"] == "active"
    finally:
        store.close()


def test_shared_owner_exception_requires_both_actual_origin_bindings(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        first = project(store)
        store.bind(first["id"], "alpha", {"thread_id": "other-native", "channel_id": "floor"})
        with pytest.raises(ValueError, match="another project"):
            store.create_project(
                "alpha",
                {
                    "title": "Second",
                    "objective": "Work",
                    "members": ["alpha", "beta", "gamma"],
                    "channel_id": "floor",
                    "thread_id": "second-discussion",
                    "origin_channel_id": "floor",
                    "origin_thread_id": "other-native",
                },
            )
    finally:
        store.close()


async def test_http_cancellation_retry_cannot_extend_to_new_projects(tmp_path):
    now = [100.0]
    store = CollaborationStore(
        tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: now[0]
    )
    old = project(store)
    now[0] = 101.0
    boundary = now[0]
    now[0] = 102.0
    new = project(store)
    actors = {name: {"token": name * 32} for name in store.actors}
    server = TestServer(build_app(store, actors))
    await server.start_server()
    client = HubClient(Settings(str(server.make_url("/")).rstrip("/"), actors["alpha"]["token"]))
    try:
        request = {"thread_id": "origin", "cutoff": boundary}
        cancelled = await client.request("POST", "/cancel-thread", request)
        assert [item["id"] for item in cancelled] == [old["id"]]
        assert await client.request("POST", "/cancel-thread", request) == []
        assert store.get_project(new["id"], "alpha")["status"] == "active"
        current = await client.request("POST", "/cancel-thread", {"thread_id": "origin"})
        assert [item["id"] for item in current] == [new["id"]]
    finally:
        await client.close()
        await server.close()
        store.close()


@pytest.mark.parametrize("cutoff", ["yesterday", True, float("inf"), float("nan"), 10**1000])
def test_store_rejects_invalid_cancellation_boundaries(tmp_path, cutoff):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        item = project(store)
        with pytest.raises(ValueError, match="finite timestamp"):
            store.cancel_thread("alpha", "origin", cutoff=cutoff)
        assert store.get_project(item["id"], "alpha")["status"] == "active"
    finally:
        store.close()


def test_creation_at_the_stop_boundary_is_included(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: 100.0)
    try:
        item = project(store)
        assert [
            result["id"] for result in store.cancel_thread("alpha", "origin", cutoff=100.0)
        ] == [item["id"]]
    finally:
        store.close()


def test_json_only_project_schema_migrates_without_losing_state(tmp_path):
    seed = CollaborationStore(tmp_path / "seed.db", {"alpha", "beta", "gamma"}, clock=lambda: 100.0)
    item = project(seed)
    seed.close()
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE projects(id TEXT PRIMARY KEY,data TEXT NOT NULL)")
        db.execute("INSERT INTO projects VALUES(?,?)", (item["id"], json.dumps(item)))
    store = CollaborationStore(path, {"alpha", "beta", "gamma"}, clock=lambda: 100.0)
    try:
        assert store.get_project(item["id"], "alpha") == item
        row = store.db.execute(
            "SELECT status,deadline,created,owner,origin_thread FROM projects"
        ).fetchone()
        assert tuple(row) == ("active", item["deadline"], 100.0, "alpha", "origin")
    finally:
        store.close()
    reopened = CollaborationStore(path, {"alpha", "beta", "gamma"}, clock=lambda: 100.0)
    try:
        assert reopened.get_project(item["id"], "alpha") == item
    finally:
        reopened.close()


def test_expiry_uses_active_deadline_index_without_decoding_closed_history(tmp_path, monkeypatch):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: 100.0)
    try:
        expired = project(store, max_seconds=1)
        future = project(store)
        history = []
        with store.transaction():
            for number in range(100):
                item = {**expired, "id": f"closed-{number}", "status": "cancelled"}
                store.save_project(item)
                history.append(json.dumps(item))
        plan = store.db.execute(
            "EXPLAIN QUERY PLAN SELECT data FROM projects WHERE status='active' AND deadline<=?",
            (102.0,),
        ).fetchall()
        assert any("SEARCH" in row[3] and "project_expiry" in row[3] for row in plan)
        original_loads = json.loads

        def loads(raw, *args, **kwargs):
            assert raw not in history, "Mailbox expiry decoded cancelled history"
            return original_loads(raw, *args, **kwargs)

        monkeypatch.setattr(store_module.json, "loads", loads)
        store.clock = lambda: 102.0
        store.expire()
        store.expire()
        assert store.get_project(expired["id"], "alpha")["status"] == "cancelled"
        assert store.get_project(future["id"], "alpha")["status"] == "active"
        assert (
            store.db.execute("SELECT status FROM projects WHERE id=?", (expired["id"],)).fetchone()[
                0
            ]
            == "cancelled"
        )
    finally:
        store.close()


async def test_wait_caps_actual_http_request_by_remaining_deadline():
    release = asyncio.Event()

    async def slow(_request):
        await release.wait()
        return web.json_response({"state": "queued"})

    app = web.Application()
    app.router.add_get("/messages/request", slow)
    server = TestServer(app)
    await server.start_server()
    client = HubClient(Settings(str(server.make_url("/")).rstrip("/"), "synthetic" * 8))
    started = asyncio.get_running_loop().time()
    try:
        with pytest.raises(TimeoutError):
            await execute(SimpleNamespace(command="wait", message="request", seconds=0.05), client)
        assert asyncio.get_running_loop().time() - started < 0.5
    finally:
        release.set()
        await client.close()
        await server.close()


async def test_wait_caps_sleep_and_retains_last_known_receipt():
    class ReadyHub:
        async def request(self, *_args):
            return {"state": "running", "id": "request"}

    started = asyncio.get_running_loop().time()
    result = await execute(
        SimpleNamespace(command="wait", message="request", seconds=0.05), ReadyHub()
    )
    assert result == {"state": "running", "id": "request"}
    assert asyncio.get_running_loop().time() - started < 0.5


async def test_wait_later_http_timeout_returns_last_receipt():
    class LaterSlowHub:
        calls = 0

        async def request(self, *_args):
            self.calls += 1
            if self.calls > 1:
                await asyncio.sleep(2)
            return {"state": "running", "id": "request"}

    hub = LaterSlowHub()
    started = asyncio.get_running_loop().time()
    result = await execute(SimpleNamespace(command="wait", message="request", seconds=1.05), hub)
    assert result["state"] == "running" and hub.calls == 2
    assert asyncio.get_running_loop().time() - started < 1.5


def test_sidecar_offline_validation_requires_configured_hub(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for key in list(__import__("os").environ):
        if key.startswith(("MATTERMOST_", "AGENT_", "GROK_", "HARNESS")):
            monkeypatch.delenv(key)
    for key, value in {
        "MATTERMOST_DOTENV": "none",
        "MATTERMOST_URL": "https://chat.invalid",
        "MATTERMOST_TOKEN": "synthetic",
        "HARNESS": "codex",
        "AGENT_COMMAND": sys.executable,
        "AGENT_CWD": str(tmp_path),
        "MATTERMOST_COLLAB_SIDECAR": "1",
        "MATTERMOST_COLLAB_ORIGIN_ACTOR": "alpha",
    }.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match="sidecar requires"):
        check()
    assert list(tmp_path.iterdir()) == []


def test_nested_result_payload_exposes_current_parent_claim_and_instance(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        item = project(store)
        parent = ask(store, item["id"])
        original = store.claim_next("beta", "first-native-owner")
        child = ask(store, item["id"], sender="beta", recipient="gamma", key="child")
        claim = store.claim_next("gamma", "child-worker")
        store.reply(child["id"], "gamma", "Verified child result", claim=claim["claim"])
        with store.transaction():
            refreshed = store.get_message(parent["id"], "beta")
            refreshed["claim"] = "current-parent-claim"
            refreshed["instance"] = "current-native-owner"
            store.save_message(refreshed)
        result = store.claim_next("beta", "result-worker")
        assert result["continuation_parent"] == original["id"]
        assert result["continuation_claim"] == "current-parent-claim"
        assert result["continuation_instance"] == "current-native-owner"
    finally:
        store.close()
