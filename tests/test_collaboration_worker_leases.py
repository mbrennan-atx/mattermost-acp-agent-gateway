"""Worker ownership uses elapsed leases and renews with transport retry margin."""

import asyncio
from types import SimpleNamespace

import pytest
from test_collaboration import ask, project

from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.store import CollaborationStore


def open_store(path, wall, elapsed, epoch="boot-one"):
    return CollaborationStore(
        path,
        {"alpha", "beta", "gamma"},
        clock=lambda: wall[0],
        elapsed=lambda: elapsed[0],
        epoch=epoch,
    )


def claimed_work(store):
    p = project(store, max_seconds=3600)
    ask(store, p["id"])
    return p, store.claim_next("beta", "worker")


@pytest.mark.parametrize("wall_step", [-1000.0, 300.0])
def test_wall_steps_do_not_change_worker_lease_lifetime(tmp_path, wall_step):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        _, item = claimed_work(store)
        wall[0] += wall_step
        elapsed[0] = 94.9
        store.expire()
        assert store.get_message(item["id"], "beta")["state"] == "running"
        elapsed[0] = 95.0
        store.expire()
        expired = store.get_message(item["id"], "beta")
        assert expired["state"] == "interrupted"
        assert expired["claim"] == item["claim"]
        assert store.claim_next("beta", "replacement") is None
    finally:
        store.close()


@pytest.mark.parametrize("renewal", ["dispatch", "post_dispatch", "heartbeat"])
def test_each_renewal_starts_a_fresh_elapsed_lease(tmp_path, renewal):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        _, item = claimed_work(store)
        evidence = {"session_id": "native", "execution_id": "execution", "native_epoch": 1}
        if renewal == "post_dispatch":
            store.reply(item["id"], "beta", "Verified work", claim=item["claim"])
            item = store.claim_next("alpha", "worker")
            evidence = {}
        actor = item["recipient"]
        wall[0], elapsed[0] = -1000.0, 80.0
        saved = store.update(
            item["id"],
            actor,
            "worker",
            renewal,
            evidence=evidence,
            claim=item["claim"],
        )
        assert saved["state"] == "running"
        wall[0], elapsed[0] = 400.0, 124.9
        store.expire()
        assert store.get_message(item["id"], actor)["state"] == "running"
        elapsed[0] = 125.0
        store.expire()
        expired = store.get_message(item["id"], actor)
        assert expired["state"] == "interrupted"
        if renewal == "dispatch":
            assert expired["execution_pending"] is True
            assert expired["evidence"]["execution_id"] == "execution"
    finally:
        store.close()


@pytest.mark.parametrize("advanced", [44.0, 45.0])
def test_same_boot_restart_preserves_execution_debt_without_repeating_work(tmp_path, advanced):
    wall, elapsed = [100.0], [50.0]
    path = tmp_path / "hub.db"
    store = open_store(path, wall, elapsed)
    _, item = claimed_work(store)
    evidence = {"session_id": "native", "execution_id": "execution", "native_epoch": 1}
    saved = store.update(item["id"], "beta", "worker", "dispatch", evidence=evidence)
    store.close()
    wall[0], elapsed[0] = -1000.0, 50.0 + advanced
    store = open_store(path, wall, elapsed)
    try:
        recovered = store.get_message(item["id"], "beta")
        assert recovered["state"] == "interrupted"
        assert recovered["claim"] == item["claim"]
        assert recovered["execution_pending"] is True
        assert recovered["evidence"] == saved["evidence"]
        assert recovered.get("worker_lease") == saved.get("worker_lease")
        assert store.claim_next("beta", "replacement") is None
    finally:
        store.close()


@pytest.mark.parametrize(
    "lease",
    [
        None,
        {},
        {"epoch": "boot-two", "started": 50.0, "deadline": 95.0},
        {"epoch": "boot-one", "started": "50", "deadline": 95.0},
        {"epoch": "boot-one", "started": True, "deadline": 95.0},
        {"epoch": "boot-one", "started": 50.0, "deadline": float("inf")},
        {"epoch": "boot-one", "started": 50.0, "deadline": float("nan")},
        {"epoch": "boot-one", "started": 60.0, "deadline": 105.0},
        {"epoch": "boot-one", "started": 50.0, "deadline": 500.0},
        {"epoch": "boot-one", "started": 50.0, "deadline": 49.0},
    ],
)
def test_unverifiable_lease_fences_work_without_losing_native_debt(tmp_path, lease):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        _, item = claimed_work(store)
        evidence = {"session_id": "native", "execution_id": "execution", "native_epoch": 1}
        saved = store.update(item["id"], "beta", "worker", "dispatch", evidence=evidence)
        if lease is None:
            saved.pop("worker_lease", None)
        else:
            saved["worker_lease"] = lease
        # Legacy wall presentation must never authorize unverified ownership.
        saved["lease_until"] = 10000.0
        with store.transaction():
            store.save_message(saved)
        wall[0] = -1000.0
        store.expire()
        expired = store.get_message(item["id"], "beta")
        assert expired["state"] == "interrupted"
        assert expired["execution_pending"] is True
        assert expired["evidence"] == saved["evidence"]
        assert expired["claim"] == item["claim"]
        assert store.claim_next("beta", "replacement") is None
    finally:
        store.close()


def test_elapsed_clock_reset_does_not_reuse_previous_worker_lease(tmp_path):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        _, item = claimed_work(store)
        # Even if boot metadata is retained, a regressed elapsed clock is invalid.
        elapsed[0] = 1.0
        store.expire()
        assert store.get_message(item["id"], "beta")["state"] == "interrupted"
    finally:
        store.close()


def test_late_heartbeat_cannot_resurrect_expired_ownership(tmp_path):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        _, item = claimed_work(store)
        elapsed[0] = 95.0
        saved = store.update(item["id"], "beta", "worker", "heartbeat", claim=item["claim"])
        assert saved["state"] == "interrupted"
        assert store.claim_next("beta", "replacement") is None
    finally:
        store.close()


def test_expired_cancel_control_requeues_without_repeating_native_work(tmp_path):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        p, item = claimed_work(store)
        store.update(
            item["id"],
            "beta",
            "worker",
            "dispatch",
            evidence={"session_id": "native", "execution_id": "execution", "native_epoch": 1},
        )
        store.cancel(p["id"], "alpha")
        control = store.claim_next("beta", "worker", controls_only=True)
        wall[0], elapsed[0] = 80.0, 95.0
        replacement = store.claim_next("beta", "replacement", controls_only=True)
        assert replacement["id"] == control["id"]
        assert replacement["claim"] != control["claim"]
        with pytest.raises(PermissionError, match="foreign worker"):
            store.update(control["id"], "beta", "worker", "heartbeat", claim=control["claim"])
        saved = store.get_message(item["id"], "beta")
        assert saved["state"] == "cancelling" and saved["execution_pending"] is True
    finally:
        store.close()


async def test_three_hung_renewals_retry_with_margin_before_lease_expiry(tmp_path, monkeypatch):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    consumer = CollaborationConsumer.__new__(CollaborationConsumer)
    consumer.instance = "worker"
    consumer.observe_deadline = lambda _item: None
    real_sleep, real_timeout = asyncio.sleep, asyncio.timeout
    renewed = asyncio.Event()
    attempts, timeout_limits = [], []

    async def sleep(delay):
        if renewed.is_set():
            await asyncio.Event().wait()
        elapsed[0] += delay
        await real_sleep(0)

    def timeout(delay):
        timeout_limits.append(delay)
        assert 0 < delay <= 5, "Heartbeat requests must leave bounded retry margin"
        elapsed[0] += delay
        return real_timeout(0.001)

    async def request(_method, _path, body):
        attempts.append(elapsed[0])
        if len(attempts) <= 3:
            await asyncio.Event().wait()
        receipt = store.update(
            item["id"], "beta", body["instance"], body["state"], claim=body["claim"]
        )
        assert receipt["state"] == "running"
        renewed.set()
        return receipt

    consumer.hub = SimpleNamespace(request=request)
    task = None
    try:
        _, item = claimed_work(store)
        monkeypatch.setattr(asyncio, "sleep", sleep)
        monkeypatch.setattr(asyncio, "timeout", timeout)
        task = asyncio.create_task(consumer.heartbeat(item))
        await asyncio.wait_for(renewed.wait(), 1)
        assert len(attempts) == 4 and len(timeout_limits) == 4
        assert elapsed[0] < 90.0, "A fourth renewal needs margin before the 95-second deadline"
        elapsed[0] += 40.0
        store.expire()
        assert store.get_message(item["id"], "beta")["state"] == "running"
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        store.close()


async def test_heartbeat_observer_stops_on_fenced_receipt(monkeypatch):
    consumer = CollaborationConsumer.__new__(CollaborationConsumer)
    real_sleep = asyncio.sleep
    calls = []

    async def sleep(_delay):
        await real_sleep(0)

    async def update(_item, _state):
        calls.append(True)
        if len(calls) > 1:
            raise asyncio.CancelledError
        return {"state": "interrupted"}

    monkeypatch.setattr(asyncio, "sleep", sleep)
    consumer.update = update
    await consumer.heartbeat({"id": "synthetic"})
    assert len(calls) == 1
