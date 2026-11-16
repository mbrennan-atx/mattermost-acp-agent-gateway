"""Retained history must not enter routine admission or claim recovery scans."""

import asyncio
import json
import sqlite3
from unittest.mock import AsyncMock

import pytest
from test_collaboration import ask, project
from test_collaboration_regressions import maintenance_gateway

from mattermost_acp.collaboration.store import CollaborationStore


def reject_history(monkeypatch, history):
    original = json.loads

    def loads(raw, *args, **kwargs):
        assert raw not in history, "Routine query decoded unrelated settled history"
        return original(raw, *args, **kwargs)

    monkeypatch.setattr(json, "loads", loads)


def test_claim_queries_use_indexed_debt_and_preserve_legacy_rule(tmp_path, monkeypatch):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store)
        original = ask(store, p["id"])
        history = set()
        with store.transaction():
            for index in range(150):
                item = {
                    **original,
                    "id": f"history-{index}",
                    "key": f"history-{index}",
                    "state": "done",
                    "execution_pending": False,
                    "evidence": {"session_id": "old", "dispatch_started": 1},
                }
                store.save_message(item)
                history.add(json.dumps(item))
            legacy = {
                **original,
                "id": "legacy",
                "key": "legacy",
                "state": "done",
                "instance": "owner",
                "evidence": {"session_id": "native", "dispatch_started": 1},
            }
            explicit = {**legacy, "id": "explicit", "key": "explicit", "execution_pending": True}
            running = {
                **original,
                "id": "running",
                "key": "running",
                "state": "running",
                "instance": "other",
                "execution_pending": False,
            }
            for item in (legacy, explicit, running):
                store.save_message(item)
        reject_history(monkeypatch, history)
        assert {item["id"] for item in store.active_claims("beta")} == {
            "legacy",
            "explicit",
            "running",
        }
        assert {item["id"] for item in store.active_claims("beta", "owner")} == {
            "legacy",
            "explicit",
        }
        plan = store.db.execute(
            "EXPLAIN QUERY PLAN SELECT data FROM messages WHERE recipient=? AND "
            "state IN ('running','cancelling','unconfirmed','interrupted') "
            "UNION ALL SELECT data FROM messages WHERE recipient=? AND execution_pending=1 "
            "AND state NOT IN ('running','cancelling','unconfirmed','interrupted')",
            ("beta", "beta"),
        ).fetchall()
        assert any("SEARCH" in row[3] and "mailbox" in row[3] for row in plan)
        assert any("SEARCH" in row[3] and "execution_debt" in row[3] for row in plan)
        assert not any("SCAN messages" in row[3] for row in plan)
    finally:
        store.close()


def test_floor_queries_match_roots_and_pending_intents_without_history(tmp_path, monkeypatch):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store)
        intent = {
            "body": {"channel_id": "floor"},
            "thread_id": "reserved-root",
            "status": "posted",
            "creation_id": "post-correlation",
        }
        pending = {**intent, "thread_id": None, "status": "reserved"}
        history = set()
        with store.transaction():
            store.save_creation("alpha", "posted", intent)
            store.save_creation("alpha", "pending", pending)
            for index in range(150):
                old = {
                    **p,
                    "id": f"old-{index}",
                    "thread_id": f"old-root-{index}",
                    "status": "cancelled",
                }
                old_intent = {**intent, "thread_id": f"old-intent-{index}", "status": "complete"}
                store.save_project(old)
                store.save_creation("alpha", f"old-{index}", old_intent)
                history.update((json.dumps(old), json.dumps(old_intent)))
        reject_history(monkeypatch, history)
        assert not store.known_discussion("floor", "unrelated")
        assert store.mention("alpha", "floor", "unrelated", "beta", "Ordinary", "ordinary") is None
        assert store.known_discussion("floor", "old-root-0")
        assert store.known_discussion("floor", "old-intent-0")
        assert not store.known_discussion("other", "discussion")
        assert store.unresolved_creations("floor") == [("alpha", "pending", pending)]
        with pytest.raises(PermissionError, match="reconciliation"):
            store.mention("alpha", "floor", "reserved-root", "beta", "Work", "work")
        for table, query, parameters, index in (
            ("projects", "channel=? AND thread=?", ("floor", "unrelated"), "project_discussion"),
            ("creations", "channel=? AND thread=?", ("floor", "unrelated"), "creation_discussion"),
            ("creations", "channel=? AND unresolved=1", ("floor",), "unresolved_creation"),
        ):
            plan = store.db.execute(
                f"EXPLAIN QUERY PLAN SELECT data FROM {table} WHERE {query}", parameters
            ).fetchall()
            assert any("SEARCH" in row[3] and index in row[3] for row in plan)
    finally:
        store.close()


def test_metadata_migration_preserves_legacy_receipts_and_reopens(tmp_path):
    seed = CollaborationStore(tmp_path / "seed.db", {"alpha", "beta", "gamma"})
    p = project(seed)
    original = ask(seed, p["id"])
    seed.close()
    legacy = {
        **original,
        "state": "done",
        "instance": "worker",
        "evidence": {"session_id": "native", "dispatch_started": 10},
    }
    settled = {**legacy, "id": "settled", "key": "settled", "execution_pending": False}
    intent = {"body": {"channel_id": "floor"}, "thread_id": "pending-root", "status": "posted"}
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE projects(id TEXT PRIMARY KEY,data TEXT NOT NULL)")
        db.execute("INSERT INTO projects VALUES(?,?)", (p["id"], json.dumps(p)))
        db.execute(
            "CREATE TABLE messages(id TEXT PRIMARY KEY,project TEXT,sender TEXT,recipient TEXT,"
            "kind TEXT,state TEXT,created REAL,key TEXT,data TEXT)"
        )
        for item in (legacy, settled):
            db.execute(
                "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?)",
                tuple(
                    item[k]
                    for k in (
                        "id",
                        "project",
                        "sender",
                        "recipient",
                        "kind",
                        "state",
                        "created",
                        "key",
                    )
                )
                + (json.dumps(item),),
            )
        db.execute("CREATE TABLE creations(actor TEXT,key TEXT,data TEXT,PRIMARY KEY(actor,key))")
        db.execute("INSERT INTO creations VALUES(?,?,?)", ("alpha", "pending", json.dumps(intent)))
    for _ in range(2):
        store = CollaborationStore(path, {"alpha", "beta", "gamma"})
        try:
            assert store.active_claims("beta", "worker") == [legacy]
            assert store.get_project(p["id"], "alpha") == p
            assert store.get_creation("alpha", "pending") == intent
            assert store.known_discussion("floor", "pending-root")
            assert tuple(
                store.db.execute("SELECT channel,thread,unresolved FROM creations").fetchone()
            ) == ("floor", "pending-root", 0)
        finally:
            store.close()


async def test_real_completed_slash_launcher_does_not_defer_cold_restart():
    gateway = maintenance_gateway()
    gateway.collaboration = None
    gateway._request_restart = AsyncMock(return_value="Restart scheduled")
    launch = gateway._track_control(
        gateway._slash_dispatch(
            thread_id="root",
            channel_id="home",
            cmd="restart",
            arg="",
            user_id="human",
            user_name="Human",
        ),
        "slash-restart",
    )
    await launch
    await asyncio.gather(*gateway._slash_tasks)
    gateway._request_restart.assert_awaited_once()
    assert not any("deferred" in post[1] for post in gateway.client.posts)


async def test_pending_slash_command_still_blocks_restart():
    gateway = maintenance_gateway()
    gateway.collaboration = None
    gateway._request_restart = AsyncMock()
    blocker = gateway._track_control(asyncio.Event().wait(), "pending-command")
    try:
        await gateway._restart_when_idle("root", "home", "human", timeout_s=0)
        gateway._request_restart.assert_not_awaited()
        assert gateway.maintenance_state()["commands"] == 1
    finally:
        blocker.cancel()
        await asyncio.gather(blocker, return_exceptions=True)
