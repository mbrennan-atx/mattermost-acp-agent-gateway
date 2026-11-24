"""Elapsed project budgets survive clock rollback and same-boot hub recovery."""

import json

import pytest
from aiohttp.test_utils import TestServer
from test_collaboration import ask, project
from test_collaboration_stop_order import creation_body

from mattermost_acp.collaboration.client import HubClient, Settings
from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore


def open_store(path, wall, elapsed, epoch="boot-one"):
    return CollaborationStore(
        path,
        {"alpha", "beta", "gamma"},
        clock=lambda: wall[0],
        elapsed=lambda: elapsed[0],
        epoch=epoch,
    )


@pytest.mark.parametrize("rollback", [False, True])
def test_expired_open_task_claim_preserves_unassigned_work(tmp_path, rollback):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        p = project(store, max_seconds=10)
        task = store.send(p["id"], "alpha", {"kind": "work", "text": "Review", "key": "open"})
        wall[0], elapsed[0] = (80.0 if rollback else 111.0), 61.0
        with pytest.raises(ValueError, match="inactive"):
            store.claim_open(task["id"], "beta")
        saved = store.get_message(task["id"], "beta")
        assert saved["recipient"] is None and saved["state"] == "open"
    finally:
        store.close()


async def test_list_refreshes_elapsed_expiry_without_consumers(tmp_path):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    p = project(store, max_seconds=10)
    task = store.send(p["id"], "alpha", {"kind": "work", "text": "Review", "key": "open"})
    actors = {name: {"token": name * 32} for name in ("alpha", "beta", "gamma")}
    server = TestServer(build_app(store, actors))
    await server.start_server()
    client = HubClient(Settings(str(server.make_url("/")).rstrip("/"), actors["alpha"]["token"]))
    try:
        wall[0], elapsed[0] = 80.0, 61.0
        result = await client.request("GET", "/projects")
        assert result[0]["status"] == "cancelled"
        assert store.get_message(task["id"], "alpha")["state"] == "cancelled"
    finally:
        await client.close()
        await server.close()
        store.close()


def test_elapsed_expiry_precedes_first_recipient_deadline_receipt(tmp_path):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        p = project(store, max_seconds=10)
        request = ask(store, p["id"])
        wall[0], elapsed[0] = 80.0, 61.0
        assert store.claim_next("beta", "worker") is None
        assert store.get_message(request["id"], "beta")["state"] == "cancelled"
        assert store.get_project(p["id"], "alpha")["status"] == "cancelled"
    finally:
        store.close()


def test_first_remaining_budget_cannot_gain_time_after_wall_clock_rollback(tmp_path):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        p = project(store, max_seconds=10)
        request = ask(store, p["id"])
        wall[0], elapsed[0] = 80.0, 52.0
        claimed = store.claim_next("beta", "worker")
        assert claimed["project_remaining_seconds"] == 8
        assert store.deadline_receipt(request, "beta")["project_remaining_seconds"] == 8
    finally:
        store.close()


@pytest.mark.parametrize("advanced", [5.0, 11.0])
def test_same_boot_reopen_preserves_original_elapsed_deadline(tmp_path, advanced):
    path = tmp_path / "hub.db"
    wall, elapsed = [100.0], [50.0]
    store = open_store(path, wall, elapsed)
    p = project(store, max_seconds=10)
    request = ask(store, p["id"])
    store.close()
    wall[0], elapsed[0] = 80.0, 50.0 + advanced
    store = open_store(path, wall, elapsed)
    try:
        claimed = store.claim_next("beta", "worker")
        if advanced < 10:
            assert claimed["id"] == request["id"]
            assert claimed["project_remaining_seconds"] == 5
        else:
            assert claimed is None
            assert store.get_project(p["id"], "alpha")["status"] == "cancelled"
    finally:
        store.close()


def test_new_boot_refuses_old_elapsed_work_without_repeating_it(tmp_path):
    path = tmp_path / "hub.db"
    wall, elapsed = [100.0], [50.0]
    store = open_store(path, wall, elapsed)
    p = project(store, max_seconds=10)
    request = ask(store, p["id"])
    store.close()
    elapsed[0] = 2.0
    store = open_store(path, wall, elapsed, epoch="boot-two")
    try:
        assert store.claim_next("beta", "worker") is None
        assert store.get_message(request["id"], "beta")["state"] == "cancelled"
        assert (
            store.db.execute(
                "SELECT COUNT(*) FROM messages WHERE project=?", (p["id"],)
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()


def test_missing_elapsed_recovery_evidence_preserves_native_execution_debt(tmp_path):
    path = tmp_path / "hub.db"
    wall, elapsed = [100.0], [50.0]
    store = open_store(path, wall, elapsed)
    p = project(store, max_seconds=10)
    ask(store, p["id"])
    request = store.claim_next("beta", "worker")
    proof = {"session_id": "native", "execution_id": "execution", "native_epoch": 7}
    store.update(
        request["id"], "beta", "worker", "dispatch", evidence=proof, claim=request["claim"]
    )
    p.pop("elapsed_epoch")
    p.pop("elapsed_deadline")
    store.save_project(p)
    store.close()
    store = open_store(path, wall, elapsed)
    try:
        store.expire()
        assert store.get_project(p["id"], "alpha")["status"] == "cancelling"
        retained = store.get_message(request["id"], "beta")
        assert retained["execution_pending"] and retained["evidence"]["execution_id"] == "execution"
    finally:
        store.close()


def test_creation_readback_recovery_does_not_restart_elapsed_budget(tmp_path):
    path = tmp_path / "hub.db"
    wall, elapsed = [100.0], [50.0]
    store = open_store(path, wall, elapsed)
    body = {**creation_body(), "limits": {"max_seconds": 10}}
    intent, _ = store.prepare_creation("alpha", body)
    store.record_creation_root(
        "alpha", body["key"], "discussion", creation_id=intent["creation_id"]
    )
    store.close()
    wall[0], elapsed[0] = 80.0, 61.0
    store = open_store(path, wall, elapsed)
    try:
        saved = store.get_creation("alpha", body["key"])
        p = store.finish_creation("alpha", saved)
        assert p["status"] == "cancelled"
        assert p["elapsed_deadline"] == 60.0
        assert store.remaining_seconds(p) == 0.0
    finally:
        store.close()


def test_elapsed_metadata_migrates_from_json_and_preserves_same_boot_budget(tmp_path):
    path = tmp_path / "hub.db"
    wall, elapsed = [100.0], [50.0]
    store = open_store(path, wall, elapsed)
    p = project(store, max_seconds=10)
    ask(store, p["id"])
    store.db.execute("UPDATE projects SET elapsed_epoch=NULL,elapsed_deadline=NULL")
    store.db.commit()
    store.close()
    elapsed[0] = 52.0
    store = open_store(path, wall, elapsed)
    try:
        row = store.db.execute(
            "SELECT elapsed_epoch,elapsed_deadline,data FROM projects"
        ).fetchone()
        assert tuple(row[:2]) == ("boot-one", 60.0)
        assert json.loads(row[2])["id"] == p["id"]
        assert store.claim_next("beta", "worker")["project_remaining_seconds"] == 8.0
    finally:
        store.close()
