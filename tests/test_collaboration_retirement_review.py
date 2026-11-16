"""Retirement, rollback and packaging boundaries use synthetic local state."""

import asyncio
import json
import os
import stat
import subprocess
import sys
import tarfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_continuation_review import child_result, retained_parent
from test_collaboration_dispatch_review import (
    close_reports,
    idle_report,
    native_gateway,
    native_tool,
)
from test_collaboration_regressions import (
    maintenance_client,
    maintenance_gateway,
    memory_broker,
)

from mattermost_acp.acp import SessionUpdate, TurnResult
from mattermost_acp.collaboration import hub
from mattermost_acp.collaboration.client import HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.mm.client import PostedEvent
from mattermost_acp.mm.http import build_slash_app
from mattermost_acp.mm.reporting import TurnReporter
from mattermost_acp.session_service import SessionService


def hub_arguments(config, db):
    return ["collaboration-hub", "--config", str(config), "--db", str(db)]


def hub_config(tmp_path):
    token = tmp_path / "actor-token"
    token.write_text("synthetic-actor-credential-" * 2)
    config = tmp_path / "actors.json"
    config.write_text(json.dumps({"actors": {"alpha": {"token_file": str(token)}}}))
    return config


@pytest.mark.parametrize("existing", [False, True])
def test_hub_database_is_private_before_inode_lock_or_sqlite(tmp_path, monkeypatch, existing):
    config = hub_config(tmp_path)
    db = tmp_path / "hub.db"
    if existing:
        db.write_bytes(b"preserved existing bytes")
        db.chmod(0o640)
    before_inode = db.stat().st_ino if existing else None
    actual_flock = hub.fcntl.flock
    observed = []

    def checked_flock(file, operation):
        info = os.fstat(file.fileno())
        if db.exists() and info.st_ino == db.stat().st_ino:
            observed.append(stat.S_IMODE(info.st_mode))
            assert observed[-1] == (0o640 if existing else 0o600)
            if existing:
                assert info.st_ino == before_inode
                assert db.read_bytes() == b"preserved existing bytes"
            raise RuntimeError("Reached inode lock before SQLite")
        return actual_flock(file, operation)

    monkeypatch.setattr(hub.fcntl, "flock", checked_flock)
    monkeypatch.setattr(
        hub, "CollaborationStore", lambda *args: pytest.fail("Unexpected SQLite open")
    )
    monkeypatch.setattr(sys, "argv", hub_arguments(config, db))
    old_umask = os.umask(0)
    try:
        with pytest.raises(RuntimeError, match="Reached inode lock"):
            hub.main()
    finally:
        os.umask(old_umask)
    assert observed == [0o640 if existing else 0o600]


@pytest.mark.parametrize("outcome", ["posted", "failed", "cancelled"])
async def test_accepted_attachment_error_delivery_blocks_cold_gateway_seal(tmp_path, outcome):
    gateway = maintenance_gateway()
    gateway.config.all_channels = True
    gateway.config.exclude_channel_ids = frozenset()
    gateway.config.text_slash = False
    gateway.config.attachments_dir = tmp_path / "blocked-directory"
    gateway.config.attachments_dir.write_text("prevents attachment directory creation")
    gateway.config.attachments_max_bytes = 1000
    gateway.client.get_file_info = AsyncMock(return_value={"name": "notes.txt", "size": 5})
    gateway.client.get_file = AsyncMock(return_value=b"notes")
    entered, release = asyncio.Event(), asyncio.Event()

    async def post_error(channel, message, root_id=None):
        assert "gateway error" in message and channel == "home" and root_id == "origin"
        entered.set()
        await release.wait()
        if outcome == "failed":
            raise RuntimeError("Error notice unavailable")
        return {"id": "error-notice"}

    gateway.client.create_post = post_error
    posted = PostedEvent(
        "accepted-post", "home", "alpha-user", "Read attachment", "origin", file_ids=("attachment",)
    )
    task = asyncio.create_task(gateway._handle_posted_safe(posted))
    client = await maintenance_client(gateway)
    try:
        await asyncio.wait_for(entered.wait(), 2)
        state = gateway.maintenance_state()
        assert state["inbound"] == 1 and state["native_boundary_unknown"] is False
        response = await client.post(
            "/maintenance",
            json={"action": "drain"},
            headers={"Authorization": "Bearer synthetic-maintenance-token"},
        )
        assert response.status == 200
        response = await client.post(
            "/maintenance",
            json={"action": "seal"},
            headers={"Authorization": "Bearer synthetic-maintenance-token"},
        )
        assert response.status == 409 and not gateway.sealed
        if outcome == "cancelled":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            release.set()
            await asyncio.wait_for(task, 2)
        assert gateway.maintenance_state()["inbound"] == 0
        response = await client.post(
            "/maintenance",
            json={"action": "seal"},
            headers={"Authorization": "Bearer synthetic-maintenance-token"},
        )
        assert response.status == 200 and gateway.sealed
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await client.close()


@pytest.mark.parametrize("alias", ["file", "directory", "hard-link"])
def test_hub_alias_lock_precedes_store_recovery(tmp_path, monkeypatch, alias):
    config = hub_config(tmp_path)
    state = tmp_path / "state"
    state.mkdir()
    db = state / "hub.db"
    actual_store = hub.CollaborationStore
    stores = []

    def tracked_store(path, actors):
        store = actual_store(path, actors | {"beta", "gamma"})
        stores.append(store)
        return store

    def running_hub(_app, **_options):
        if len(stores) != 1:
            return
        store = stores[0]
        p = project(store)
        work = ask(store, p["id"])
        store.claim_next("beta", "live-worker")
        if alias == "file":
            aliased = tmp_path / "alias.db"
            aliased.symlink_to(db)
        elif alias == "hard-link":
            aliased = tmp_path / "alias.db"
            os.link(db, aliased)
        else:
            directory = tmp_path / "alias-directory"
            directory.symlink_to(state, target_is_directory=True)
            aliased = directory / db.name
        with monkeypatch.context() as nested:
            nested.setattr(sys, "argv", hub_arguments(config, aliased))
            with pytest.raises(BlockingIOError):
                hub.main()
        assert len(stores) == 1, "Alias startup must not initialize/recover the live database"
        assert store.get_message(work["id"], "alpha")["state"] == "running"

    monkeypatch.setattr(hub, "CollaborationStore", tracked_store)
    monkeypatch.setattr(hub.web, "run_app", running_hub)
    monkeypatch.setattr(sys, "argv", hub_arguments(config, db))
    hub.main()
    assert db.with_name(db.name + ".lock").exists()


def test_hard_linked_database_refuses_recovery_without_an_existing_hub(tmp_path, monkeypatch):
    config = hub_config(tmp_path)
    db = tmp_path / "hub.db"
    db.write_bytes(b"")
    alias = tmp_path / "alias.db"
    os.link(db, alias)
    stores = []
    monkeypatch.setattr(hub, "CollaborationStore", lambda *args: stores.append(args))
    monkeypatch.setattr(sys, "argv", hub_arguments(config, alias))
    with pytest.raises(ValueError, match="hard-link aliases"):
        hub.main()
    assert stores == [], "A hard-link alias must not initialize a separate SQLite WAL"
    assert not alias.with_name(alias.name + "-wal").exists()


def test_distinct_database_extensions_use_independent_hub_locks(tmp_path, monkeypatch):
    config = hub_config(tmp_path)
    first, second = tmp_path / "hub.db", tmp_path / "hub.sqlite"
    actual_store = hub.CollaborationStore
    stores = []

    def tracked_store(path, actors):
        store = actual_store(path, actors | {"beta", "gamma"})
        stores.append(store)
        return store

    def running_hub(_app, **_options):
        if len(stores) != 1:
            return
        store = stores[0]
        p = project(store)
        work = ask(store, p["id"])
        store.claim_next("beta", "live-worker")
        with monkeypatch.context() as nested:
            nested.setattr(sys, "argv", hub_arguments(config, second))
            hub.main()
        assert len(stores) == 2
        assert store.get_message(work["id"], "alpha")["state"] == "running"

    monkeypatch.setattr(hub, "CollaborationStore", tracked_store)
    monkeypatch.setattr(hub.web, "run_app", running_hub)
    monkeypatch.setattr(sys, "argv", hub_arguments(config, first))
    hub.main()
    assert first.with_name(first.name + ".lock").exists()
    assert second.with_name(second.name + ".lock").exists()


def test_source_archive_contains_all_manifest_markdown_including_constitution(tmp_path):
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        ["uv", "build", "--sdist", "--out-dir", str(tmp_path)],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr
    archives = list(tmp_path.glob("*.tar.gz"))
    assert len(archives) == 1
    selected = (root / "release/public-files.txt").read_text().splitlines()
    with tarfile.open(archives[0]) as archive:
        prefix = archive.getmembers()[0].name.split("/", 1)[0]
        for name in selected:
            if name.endswith(".md"):
                member = archive.extractfile(prefix + "/" + name)
                assert member is not None, name
                assert member.read() == (root / name).read_bytes(), name
        assert archive.getmember(prefix + "/.specify/memory/constitution.md").isfile()


async def test_distinct_gateway_database_extensions_preserve_independent_stop_intents(tmp_path):
    consumers = []
    try:
        for filename, thread, cutoff in [
            ("worker.db", "first-origin", 1000.0),
            ("worker.sqlite", "second-origin", 2000.0),
        ]:
            gateway = active_gateway()
            gateway.config.db_path = tmp_path / filename
            consumer = CollaborationConsumer(gateway, Settings("http://127.0.0.1:18781", "x" * 32))
            consumers.append(consumer)
            consumer.pending_stops[thread] = cutoff
            consumer.save_stops()
        assert consumers[0].stops_path != consumers[1].stops_path
        assert json.loads(consumers[0].stops_path.read_text()) == {"first-origin": 1000.0}
        assert json.loads(consumers[1].stops_path.read_text()) == {"second-origin": 2000.0}
        for consumer in consumers[:]:
            recovered = CollaborationConsumer(consumer.gateway, consumer.hub.settings)
            consumers.append(recovered)
            assert recovered.pending_stops == consumer.pending_stops
    finally:
        await asyncio.gather(*(consumer.close() for consumer in consumers))


async def test_rejected_continuation_restores_parent_and_cancels_discarded_progress(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        old["reporter"] = TurnReporter()
        previous_receipt = dict(gateway._last_terminal["discussion"])
        broker, writer = memory_broker()
        broker.capabilities.steering = False
        sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        sessions.ensure_session = AsyncMock(return_value="existing-native")
        sessions.status = AsyncMock(return_value={"session_id": "existing-native"})
        sessions._native_epochs["discussion"] = 7
        gateway.sessions = sessions
        broker.update_observer = gateway._observe_background_update
        gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
        delivery = child_result(store, p, parent, consumer)
        rejected = None
        handling = None
        try:
            async with broker._write_lock:
                handling = asyncio.create_task(consumer.handle(delivery))
                async with asyncio.timeout(2):
                    while not broker._pending:
                        await asyncio.sleep(0)
                rejected = gateway._reports["existing-native"]
                assert rejected is not old
                assert consumer.ownership[(parent["id"], parent["claim"])] is rejected
                assert (
                    store.get_message(parent["id"], "beta")["evidence"]["execution_id"]
                    == (rejected["execution_id"])
                )
                await native_tool(broker)
            await asyncio.wait_for(handling, 3)
            assert writer.messages == []
            assert store.get_message(delivery["id"], "beta")["state"] == "queued"
            assert consumer.ownership[(parent["id"], parent["claim"])] is old
            restored = store.get_message(parent["id"], "beta")["evidence"]
            assert restored["execution_id"] == old["execution_id"]
            assert restored["session_id"] == previous_receipt["session_id"]
            assert gateway._last_terminal["discussion"]["execution_id"] == old["execution_id"]
            assert rejected["task"].done(), "Discarded progress must stop receiving/publishing work"
            assert gateway.background_pending("discussion")
            await native_tool(broker, state="completed")
            assert not gateway.background_pending("discussion")

            clock = store.clock
            store.clock = lambda: clock() + 3
            reclaimed = store.claim_next("beta", consumer.instance)
            assert reclaimed["id"] == delivery["id"]
            assert (await consumer.continuation(reclaimed))["report"] is old

            def prompt_reply(payload):
                message = json.loads(payload)
                writer.messages.append(message)
                broker._pending[message["id"]].set_result({"stopReason": "end_turn"})

            writer.write = prompt_reply
            await asyncio.wait_for(consumer.handle(reclaimed), 3)
            assert len(writer.messages) == 1 and writer.messages[0]["method"] == "session/prompt"
            assert store.get_message(reclaimed["id"], "beta")["state"] == "done"
            assert store.get_message(parent["id"], "beta")["state"] == "running"
        finally:
            if handling and not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)
            if rejected and rejected.get("task") and not rejected["task"].done():
                rejected["task"].cancel()
                await asyncio.gather(rejected["task"], return_exceptions=True)


async def test_cold_drain_settles_mailbox_claim_before_sealing(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = maintenance_gateway()
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    consumer.actor = "beta"
    gateway.collaboration = consumer
    headers = {"Authorization": "Bearer " + consumer.hub.settings.token}
    claimed, release = asyncio.Event(), asyncio.Event()
    actual_request = consumer.hub.request
    delayed = False

    async def delayed_mailbox(method, path, body=None):
        nonlocal delayed
        result = await actual_request(method, path, body)
        if path.startswith("/mailbox") and result and not delayed:
            delayed = True
            claimed.set()
            await release.wait()
        return result

    consumer.hub.request = delayed_mailbox
    client = await maintenance_client(gateway)
    draining = sealing = None
    try:
        p = project(store)
        request = ask(store, p["id"])
        consumer.start()
        await asyncio.wait_for(claimed.wait(), 2)
        assert store.get_message(request["id"], "alpha")["state"] == "running"
        state = gateway.maintenance_state()
        assert state["collaboration"] and not state["idle"]
        draining = asyncio.create_task(
            client.post("/maintenance", json={"action": "drain"}, headers=headers)
        )
        async with asyncio.timeout(2):
            while not gateway.draining:
                await asyncio.sleep(0)
        sealing = asyncio.create_task(
            client.post("/maintenance", json={"action": "seal"}, headers=headers)
        )
        await asyncio.sleep(0.05)
        assert not sealing.done() and not gateway.sealed
        release.set()
        drained = await asyncio.wait_for(draining, 3)
        assert drained.status == 200
        first_seal = await asyncio.wait_for(sealing, 3)
        assert first_seal.status in {200, 409}
        if first_seal.status == 200:
            assert gateway.sealed and gateway.maintenance_state()["idle"]
        async with asyncio.timeout(3):
            while (
                store.get_message(request["id"], "alpha")["state"] != "queued"
                or not gateway.maintenance_state()["idle"]
            ):
                await asyncio.sleep(0)
        gateway.run_prompt.assert_not_awaited()
        assert not store.get_message(request["id"], "alpha")["evidence"].get("session_id")
        sealed = await client.post("/maintenance", json={"action": "seal"}, headers=headers)
        assert sealed.status == 200 and gateway.sealed
    finally:
        release.set()
        tasks = [task for task in [draining, sealing] if task]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.close()
        await client.close()
        await close_api(store, server, clients)


async def test_drain_reconciles_committed_claim_after_lost_mailbox_response(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = maintenance_gateway()
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    consumer.actor = "beta"
    gateway.collaboration = consumer
    headers = {"Authorization": "Bearer " + consumer.hub.settings.token}
    lost = asyncio.Event()
    actual_request = consumer.hub.request

    async def lost_mailbox(method, path, body=None):
        result = await actual_request(method, path, body)
        if path.startswith("/mailbox?") and result and not lost.is_set():
            lost.set()
            raise HubError("Committed mailbox response lost")
        return result

    consumer.hub.request = lost_mailbox
    client = await maintenance_client(gateway)
    try:
        p = project(store)
        request = ask(store, p["id"])
        consumer.start()
        await asyncio.wait_for(lost.wait(), 2)
        await asyncio.wait_for(consumer.claim_settled.wait(), 2)
        assert consumer.claim_uncertain and not consumer.workers
        assert store.get_message(request["id"], "alpha")["state"] == "running"
        assert not gateway.maintenance_state()["idle"]
        drained = await client.post("/maintenance", json={"action": "drain"}, headers=headers)
        assert drained.status == 200
        assert not consumer.claim_uncertain and gateway.maintenance_state()["idle"]
        retained = store.get_message(request["id"], "alpha")
        assert retained["state"] == "queued" and not retained["evidence"].get("session_id")
        gateway.run_prompt.assert_not_awaited()
        sealed = await client.post("/maintenance", json={"action": "seal"}, headers=headers)
        assert sealed.status == 200 and gateway.sealed
    finally:
        await consumer.close()
        await client.close()
        await close_api(store, server, clients)


async def test_failed_admission_keeps_uncertainty_until_successful_drain(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = maintenance_gateway()
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    consumer.actor = "beta"
    gateway.collaboration = consumer
    headers = {"Authorization": "Bearer " + consumer.hub.settings.token}
    polling = asyncio.Event()
    actual_request = consumer.hub.request
    fail_admission = True

    async def failed_admission(method, path, body=None):
        if path.startswith("/mailbox?"):
            polling.set()
        if path == "/mailbox/admission" and fail_admission:
            raise HubError("Admission response unavailable")
        return await actual_request(method, path, body)

    consumer.hub.request = failed_admission
    client = await maintenance_client(gateway)
    try:
        consumer.start()
        await asyncio.wait_for(polling.wait(), 2)
        failed = await client.post("/maintenance", json={"action": "drain"}, headers=headers)
        assert failed.status == 409
        assert gateway.draining and consumer.claim_uncertain
        assert gateway.maintenance_state()["collaboration"]
        assert not gateway.maintenance_state()["idle"]
        sealed = await client.post("/maintenance", json={"action": "seal"}, headers=headers)
        assert sealed.status == 409 and not gateway.sealed
        p = project(store)
        request = ask(store, p["id"])
        fail_admission = False
        drained = await client.post("/maintenance", json={"action": "drain"}, headers=headers)
        assert drained.status == 200
        assert not consumer.claim_uncertain and gateway.maintenance_state()["idle"]
        assert store.get_message(request["id"], "alpha")["state"] == "queued"
        gateway.run_prompt.assert_not_awaited()
        sealed = await client.post("/maintenance", json={"action": "seal"}, headers=headers)
        assert sealed.status == 200 and gateway.sealed
    finally:
        await consumer.close()
        await client.close()
        await close_api(store, server, clients)


async def test_concurrent_resume_and_drain_keep_local_and_hub_admission_ordered(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = maintenance_gateway()
    gateway.config.db_path = tmp_path / "project.db"
    gateway.draining = gateway.sealed = True
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    consumer.actor = "beta"
    gateway.collaboration = consumer
    gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
    gateway._run_prompt = AsyncMock(return_value={"stop_reason": "end_turn"})
    headers = {"Authorization": "Bearer " + consumer.hub.settings.token}
    resume_entered, drain_received, drain_entered = (asyncio.Event() for _ in range(3))
    resume_release, drain_release = asyncio.Event(), asyncio.Event()
    actual_request = consumer.hub.request
    modes = []

    async def delayed_admission(method, path, body=None):
        result = await actual_request(method, path, body)
        if path == "/mailbox/admission":
            modes.append(body["mode"])
            if body["mode"] == "normal":
                resume_entered.set()
                await resume_release.wait()
            elif body["mode"] == "controls":
                drain_entered.set()
                await drain_release.wait()
        return result

    @web.middleware
    async def observe_request(request, handler):
        if request.path == "/maintenance" and (await request.json()).get("action") == "drain":
            drain_received.set()
        return await handler(request)

    consumer.hub.request = delayed_admission
    app = build_slash_app(gateway.config, gateway.handle_slash)
    app.middlewares.append(observe_request)
    client = TestClient(TestServer(app))
    await client.start_server()
    consumer.start()
    resume = drain = None
    try:
        resume = asyncio.create_task(
            client.post("/maintenance", json={"action": "resume"}, headers=headers)
        )
        await asyncio.wait_for(resume_entered.wait(), 2)
        drain = asyncio.create_task(
            client.post("/maintenance", json={"action": "drain"}, headers=headers)
        )
        await asyncio.wait_for(drain_received.wait(), 2)
        resume_release.set()
        await asyncio.wait_for(drain_entered.wait(), 2)
        assert (await asyncio.wait_for(resume, 2)).status == 200
        assert gateway.draining
        await gateway.run_prompt(
            thread_id="origin", channel_id="home", prompt="User work during drain"
        )
        gateway._run_prompt.assert_not_awaited()
        drain_release.set()
        assert (await asyncio.wait_for(drain, 2)).status == 200
        assert modes == ["normal", "controls"]
        assert gateway.draining and not gateway.sealed
        p = project(store)
        request = ask(store, p["id"])
        assert (
            await clients["beta"].request(
                "GET", "/mailbox?instance=" + consumer.instance + "&wait=0"
            )
            is None
        )
        assert store.get_message(request["id"], "beta")["state"] == "queued"
    finally:
        resume_release.set()
        drain_release.set()
        tasks = [task for task in [resume, drain] if task]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.close()
        await client.close()
        await close_api(store, server, clients)


async def test_admission_snapshot_preserves_owned_native_claim(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, _p, parent, old):
        before = store.get_message(parent["id"], "beta")
        assert before["evidence"]["session_id"] == "existing-native"
        consumer.claim_uncertain = True
        gateway.draining = True
        await consumer.set_admission("controls")
        after = store.get_message(parent["id"], "beta")
        assert after["state"] == "running" and after["claim"] == before["claim"]
        assert after["evidence"] == before["evidence"]
        assert consumer.ownership[(parent["id"], parent["claim"])] is old
        assert consumer.owns_thread("discussion") and not consumer.claim_uncertain
        gateway.run_prompt.assert_not_awaited()


@pytest.mark.parametrize("mode", ["controls", "sealed"])
async def test_admission_fence_wakes_old_normal_long_poll_without_claim(
    tmp_path, monkeypatch, mode
):
    store, server, clients = await api_fixture(tmp_path)
    polling = asyncio.Event()
    actual_claim = store.claim_next
    waiting = None

    def observed_claim(actor, instance, **options):
        if actor == "beta" and instance == "waiting-worker":
            polling.set()
        return actual_claim(actor, instance, **options)

    monkeypatch.setattr(store, "claim_next", observed_claim)
    try:
        waiting = asyncio.create_task(
            clients["beta"].request("GET", "/mailbox?instance=waiting-worker&wait=20")
        )
        await asyncio.wait_for(polling.wait(), 2)
        claims = await clients["beta"].request(
            "POST", "/mailbox/admission", {"instance": "waiting-worker", "mode": mode}
        )
        assert claims == []
        assert await asyncio.wait_for(waiting, 1) is None
        p = project(store)
        request = ask(store, p["id"])
        assert (
            await clients["beta"].request("GET", "/mailbox?instance=waiting-worker&wait=0") is None
        )
        assert store.get_message(request["id"], "alpha")["state"] == "queued"
    finally:
        if waiting and not waiting.done():
            waiting.cancel()
        if waiting:
            await asyncio.gather(waiting, return_exceptions=True)
        await close_api(store, server, clients)


async def test_admission_fences_are_isolated_by_authenticated_actor_and_instance(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store, max_workers=4)
        beta_work = ask(store, p["id"])
        gamma_work = ask(store, p["id"], recipient="gamma", key="gamma-work")
        await clients["beta"].request(
            "POST", "/mailbox/admission", {"instance": "shared", "mode": "sealed"}
        )
        assert await clients["beta"].request("GET", "/mailbox?instance=shared&wait=0") is None
        gamma_claim = await clients["gamma"].request("GET", "/mailbox?instance=shared&wait=0")
        beta_claim = await clients["beta"].request("GET", "/mailbox?instance=other&wait=0")
        assert gamma_claim["id"] == gamma_work["id"]
        assert beta_claim["id"] == beta_work["id"]
        resumed = ask(store, p["id"], key="resumed-work")
        await clients["beta"].request(
            "POST", "/mailbox/admission", {"instance": "shared", "mode": "normal"}
        )
        assert (await clients["beta"].request("GET", "/mailbox?instance=shared&wait=0"))["id"] == (
            resumed["id"]
        )
    finally:
        await close_api(store, server, clients)


async def test_controls_admission_delivers_cancel_but_sealed_admission_delivers_nothing(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        active = ask(store, p["id"])
        store.claim_next("beta", "worker")
        other = project(store)
        queued = ask(store, other["id"])
        await clients["beta"].request(
            "POST", "/mailbox/admission", {"instance": "worker", "mode": "controls"}
        )
        assert (
            await asyncio.wait_for(
                clients["beta"].request("GET", "/mailbox?instance=worker&wait=20"), 1
            )
            is None
        )
        await clients["alpha"].request("POST", f"/projects/{p['id']}/cancel", {})
        await clients["beta"].request(
            "POST", "/mailbox/admission", {"instance": "worker", "mode": "sealed"}
        )
        assert (
            await asyncio.wait_for(
                clients["beta"].request("GET", "/mailbox?instance=worker&wait=20&controls_only=1"),
                1,
            )
            is None
        )
        await clients["beta"].request(
            "POST", "/mailbox/admission", {"instance": "worker", "mode": "controls"}
        )
        control = await clients["beta"].request("GET", "/mailbox?instance=worker&wait=20")
        assert control["kind"] == "cancel" and control["parent"] == active["id"]
        assert store.get_message(queued["id"], "alpha")["state"] == "queued"
    finally:
        await close_api(store, server, clients)


async def test_admission_snapshot_preserves_all_current_claims_and_native_evidence(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    expected = {}
    try:
        p = project(store)
        settled = ask(store, p["id"])
        store.claim_next("beta", "snapshot-worker")
        store.reply(settled["id"], "beta", "Already completed")
        for actor, instance in [("beta", "other-worker"), ("gamma", "snapshot-worker")]:
            p = project(store)
            foreign = ask(store, p["id"], recipient=actor)
            assert store.claim_next(actor, instance)["id"] == foreign["id"]
        for state in ["running", "unconfirmed", "interrupted", "cancelling"]:
            p = project(store)
            request = ask(store, p["id"])
            claimed = store.claim_next("beta", "snapshot-worker")
            store.update(
                request["id"],
                "beta",
                "snapshot-worker",
                "evidence",
                evidence={"session_id": "native-" + state, "execution_id": "execution-" + state},
            )
            if state in {"unconfirmed", "interrupted"}:
                store.update(request["id"], "beta", "snapshot-worker", state, "Delivery unknown")
            elif state == "cancelling":
                store.cancel(p["id"], "alpha")
            expected[request["id"]] = (state, claimed["claim"])
        p = project(store)
        ask(store, p["id"])
        snapshot = await clients["beta"].request(
            "POST", "/mailbox/admission", {"instance": "snapshot-worker", "mode": "sealed"}
        )
        assert {item["id"] for item in snapshot} == set(expected)
        for item in snapshot:
            state, claim = expected[item["id"]]
            assert item["state"] == state and item["claim"] == claim
            assert item["evidence"] == {
                "session_id": "native-" + state,
                "execution_id": "execution-" + state,
            }
    finally:
        await close_api(store, server, clients)


async def test_repeated_settled_guarded_turns_do_not_accumulate_previous_reports(tmp_path):
    gateway, _broker, _writer = native_gateway(tmp_path)
    reports = []

    async def native(thread, _text, **options):
        assert await options["before_prompt"]("existing-native")
        assert options["dispatch_guard"]()
        reports.append(gateway._reports["existing-native"])
        return TurnResult("existing-native", "end_turn", "Verified result", "")

    gateway.sessions.run_turn = native
    try:
        for _ in range(4):
            receipt = await gateway.run_prompt(
                thread_id="discussion",
                channel_id="floor",
                prompt="Incorporate a settled peer result",
                author_id="actor",
                author_name="actor",
                before_native=AsyncMock(return_value=True),
            )
            assert receipt["stop_reason"] == "end_turn"
            assert list(gateway.live_reports()) == [reports[-1]]
        assert all(report["task"].done() for report in reports[:-1])
    finally:
        await close_reports(gateway)


async def test_pending_or_owned_previous_reports_survive_until_their_work_settles():
    gateway = active_gateway(busy=False)
    consumer = CollaborationConsumer(gateway, Settings("http://127.0.0.1:18781", "x" * 32))
    gateway.collaboration = consumer
    current, owned, pending = idle_report(), idle_report(), idle_report()
    current["execution_id"], owned["execution_id"], pending["execution_id"] = "now", "owned", "tool"
    current["previous"] = owned
    owned["previous"] = pending
    pending["reporter"].active_tools.add("background-tool")
    gateway._reports["existing-native"] = current
    consumer.ownership[("request", "claim")] = owned
    consumer.ownership_threads[("request", "claim")] = "discussion"
    try:
        gateway._observe_background_update(
            SessionUpdate("existing-native", "agent_message_chunk", text="Current progress")
        )
        assert owned in list(gateway.live_reports()) and pending in list(gateway.live_reports())
        gateway._observe_background_update(
            SessionUpdate(
                "existing-native",
                "tool_call_update",
                tool_call_id="background-tool",
                tool_status="completed",
            )
        )
        assert pending not in list(gateway.live_reports())
        assert owned in list(gateway.live_reports())
        consumer.release_ownership(("request", "claim"))
        assert list(gateway.live_reports()) == [current]
    finally:
        await close_reports(gateway)
        await consumer.close()
