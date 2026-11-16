"""Reservation identity and elapsed retries remain valid across wall-clock changes."""

from contextlib import contextmanager

import pytest
from test_collaboration import ask, project

from mattermost_acp.collaboration.store import CollaborationStore


def open_store(path, wall, elapsed, epoch="boot-one"):
    return CollaborationStore(
        path,
        {"alpha", "beta", "gamma"},
        clock=lambda: wall[0],
        elapsed=lambda: elapsed[0],
        epoch=epoch,
    )


@contextmanager
def reservation(tmp_path, *, token=True):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", "worker")
        wall[0] = 75.0
        proof = {"session_id": "same-session", "execution_id": "same-execution", "native_epoch": 3}
        if token:
            proof["reservation_id"] = "new-reservation"
        store.update(request["id"], "beta", "worker", "dispatch", evidence=proof)
        store.reply(request["id"], "beta", "Early reply", claim=claimed["claim"])
        receipt = {
            **proof,
            "native_dispatched": True,
            "stop_reason": "end_turn",
            "pending": False,
            # An older receipt can look newer after the wall-clock rollback.
            "completed_at": 100.0,
        }
        yield store, request, claimed, receipt
    finally:
        store.close()


@pytest.mark.parametrize(
    "missing", [("reservation_id",), ("native_epoch",), ("reservation_id", "native_epoch")]
)
def test_new_reservation_refuses_unlinked_recovery_settlement(tmp_path, missing):
    with reservation(tmp_path) as (store, request, claimed, receipt):
        for field in missing:
            receipt.pop(field)
        before = store.get_message(request["id"], "beta")
        with pytest.raises(ValueError, match="settlement must match"):
            store.update(
                request["id"],
                "beta",
                "worker",
                "execution_settled",
                evidence=receipt,
                claim=claimed["claim"],
            )
        assert store.get_message(request["id"], "beta") == before
        assert before["execution_pending"] is True


@pytest.mark.parametrize("field", ["reservation_id", "native_epoch"])
def test_new_reservation_refuses_wrong_recovery_identity(tmp_path, field):
    with reservation(tmp_path) as (store, request, claimed, receipt):
        receipt[field] = "wrong-reservation" if field == "reservation_id" else 2
        with pytest.raises(ValueError, match="settlement must match"):
            store.update(request["id"], "beta", "worker", "execution_settled", evidence=receipt)
        assert store.get_message(request["id"], "beta")["execution_pending"] is True


def test_linked_reservation_settles_after_wall_rollback(tmp_path):
    with reservation(tmp_path) as (store, request, claimed, receipt):
        receipt["completed_at"] = 50.0
        saved = store.update(
            request["id"],
            "beta",
            "worker",
            "execution_settled",
            evidence=receipt,
            claim=claimed["claim"],
        )
        assert saved["execution_pending"] is False and saved["result"] == "Early reply"


def test_legacy_reservation_keeps_timestamp_settlement(tmp_path):
    with reservation(tmp_path, token=False) as (store, request, claimed, receipt):
        receipt.pop("native_epoch")
        saved = store.update(
            request["id"],
            "beta",
            "worker",
            "execution_settled",
            evidence=receipt,
            claim=claimed["claim"],
        )
        assert saved["execution_pending"] is False


def retry_request(store, *, cancel):
    p = project(store, max_seconds=3600)
    request = ask(store, p["id"])
    work = store.claim_next("beta", "worker")
    if cancel:
        store.update(
            request["id"],
            "beta",
            "worker",
            "dispatch",
            evidence={
                "session_id": "native",
                "execution_id": "execution",
                "native_epoch": 1,
                "reservation_id": "reserved",
            },
        )
        store.cancel(p["id"], "alpha")
        work = store.claim_next("beta", "worker", controls_only=True)
    deferred = store.update(
        work["id"],
        "beta",
        "worker",
        "unconfirmed" if cancel else "queued",
        result="Stop transport failed" if cancel else "Busy before native dispatch",
        claim=work["claim"],
    )
    assert deferred["state"] == "queued"
    return request, deferred


@pytest.mark.parametrize("cancel", [False, True])
@pytest.mark.parametrize("wall_step", [-1000.0, 300.0])
def test_retry_delay_uses_elapsed_time_after_wall_step(tmp_path, cancel, wall_step):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        request, deferred = retry_request(store, cancel=cancel)
        wall[0] += wall_step
        elapsed[0] = 51.9
        assert store.claim_next("beta", "next-worker", controls_only=cancel) is None
        elapsed[0] = 52.0
        claimed = store.claim_next("beta", "next-worker", controls_only=cancel)
        assert claimed["id"] == deferred["id"] and claimed["attempt"] == 2
        if cancel:
            assert store.get_message(request["id"], "beta")["execution_pending"] is True
    finally:
        store.close()


@pytest.mark.parametrize("cancel", [False, True])
def test_same_boot_restart_retains_retry_deadline(tmp_path, cancel):
    wall, elapsed = [100.0], [50.0]
    path = tmp_path / "hub.db"
    store = open_store(path, wall, elapsed)
    request, deferred = retry_request(store, cancel=cancel)
    store.close()
    wall[0], elapsed[0] = -1000.0, 51.0
    store = open_store(path, wall, elapsed)
    try:
        assert store.claim_next("beta", "next-worker", controls_only=cancel) is None
        elapsed[0] = 52.0
        claimed = store.claim_next("beta", "next-worker", controls_only=cancel)
        assert claimed["id"] == deferred["id"]
        if cancel:
            assert store.get_message(request["id"], "beta")["execution_pending"] is True
    finally:
        store.close()


@pytest.mark.parametrize("evidence", ["new-epoch", "missing", "malformed"])
def test_cancel_retry_recovery_has_one_bounded_backoff(tmp_path, evidence):
    wall, elapsed = [100.0], [50.0]
    path = tmp_path / "hub.db"
    store = open_store(path, wall, elapsed)
    request, deferred = retry_request(store, cancel=True)
    if evidence != "new-epoch":
        if evidence == "missing":
            deferred.pop("retry_delay", None)
        else:
            deferred["retry_delay"] = {"epoch": "boot-one", "started": "invalid"}
        store.save_message(deferred)
        store.db.commit()
    store.close()
    wall[0], elapsed[0] = -1000.0, 1.0 if evidence == "new-epoch" else 51.0
    store = open_store(path, wall, elapsed, "boot-two" if evidence == "new-epoch" else "boot-one")
    try:
        assert store.claim_next("beta", "next-worker", controls_only=True) is None
        saved = store.get_message(deferred["id"], "beta")["retry_delay"]
        elapsed[0] += 1.0
        assert store.claim_next("beta", "next-worker", controls_only=True) is None
        assert store.get_message(deferred["id"], "beta")["retry_delay"] == saved
        elapsed[0] += 1.0
        assert store.claim_next("beta", "next-worker", controls_only=True)["id"] == deferred["id"]
        assert store.get_message(request["id"], "beta")["execution_pending"] is True
    finally:
        store.close()


def test_legacy_future_wall_retry_recovers_with_bounded_elapsed_delay(tmp_path):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        p = project(store)
        request = ask(store, p["id"])
        request["not_before"] = 10000.0
        store.save_message(request)
        store.db.commit()
        assert store.claim_next("beta", "worker") is None
        wall[0], elapsed[0] = -1000.0, 52.0
        assert store.claim_next("beta", "worker")["id"] == request["id"]
    finally:
        store.close()


@pytest.mark.parametrize("explicit", [False, True])
def test_verified_floor_request_handles_server_clock_rollback(tmp_path, explicit):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        p = project(store)
        p["discussion_created_at"] = 100000
        store.save_project(p)
        kwargs = {"post_created_at": 75000, "project_id": p["id"] if explicit else None}
        if explicit:
            accepted = store.mention(
                "alpha", "floor", p["thread_id"], "beta", "Read source", "post:verified", **kwargs
            )
            assert accepted["project"] == p["id"] and accepted["state"] == "queued"
        else:
            with pytest.raises(PermissionError, match="predates"):
                store.mention(
                    "alpha",
                    "floor",
                    p["thread_id"],
                    "beta",
                    "Read source",
                    "post:verified",
                    **kwargs,
                )
    finally:
        store.close()


@pytest.mark.parametrize("invalid", [None, 0, -1, "75000"])
def test_explicit_floor_request_still_requires_verified_post_time(tmp_path, invalid):
    wall, elapsed = [100.0], [50.0]
    store = open_store(tmp_path / "hub.db", wall, elapsed)
    try:
        p = project(store)
        with pytest.raises(PermissionError, match="could not be verified"):
            store.mention(
                "alpha",
                "floor",
                p["thread_id"],
                "beta",
                "Read source",
                "post:invalid",
                post_created_at=invalid,
                project_id=p["id"],
            )
    finally:
        store.close()
