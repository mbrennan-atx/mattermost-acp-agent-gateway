"""Regression tests ensuring idle hub mailbox polls do not spin at 1 Hz."""

from __future__ import annotations

import asyncio

import pytest
from aiohttp.test_utils import TestClient, TestServer

from mattermost_acp.collaboration.hub import build_app
from mattermost_acp.collaboration.store import CollaborationStore

ACTORS = {
    "alpha": {"token": "a" * 32, "user_id": "u-alpha", "sidecar": True},
    "beta": {"token": "b" * 32, "user_id": "u-beta", "sidecar": True},
}


@pytest.mark.asyncio
async def test_idle_mailbox_waits_full_window_without_1s_polling(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", set(ACTORS))
    calls = 0
    real_has_timed = store.has_timed_work

    def counted_has_timed():
        nonlocal calls
        calls += 1
        return real_has_timed()

    store.has_timed_work = counted_has_timed
    app = build_app(store, ACTORS)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/mailbox?instance=inst1&wait=2.2",
            headers={"Authorization": "Bearer " + ACTORS["alpha"]["token"]},
        )
        assert resp.status == 200
        assert await resp.json() is None
    # One check on entry, one check when the 2.2s timeout expires (not 1 Hz polling).
    assert calls == 2
    store.close()


@pytest.mark.asyncio
async def test_idle_mailbox_wakes_immediately_on_post(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", set(ACTORS))
    app = build_app(store, ACTORS)
    async with TestClient(TestServer(app)) as client:
        poll = asyncio.create_task(
            client.get(
                "/mailbox?instance=inst1&wait=15",
                headers={"Authorization": "Bearer " + ACTORS["beta"]["token"]},
            )
        )
        await asyncio.sleep(0.1)
        created = await client.post(
            "/projects",
            headers={"Authorization": "Bearer " + ACTORS["alpha"]["token"]},
            json={
                "title": "Wake test",
                "objective": "Verify immediate wakeup",
                "members": ["alpha", "beta"],
                "channel_id": "ch1",
                "thread_id": "th1",
                "origin_channel_id": "och1",
                "origin_thread_id": "oth1",
                "key": "wake-test",
            },
        )
        assert created.status == 200
        pid = (await created.json())["id"]
        sent = await client.post(
            f"/projects/{pid}/messages",
            headers={"Authorization": "Bearer " + ACTORS["alpha"]["token"]},
            json={"recipient": "beta", "text": "ping", "key": "msg-1"},
        )
        assert sent.status == 200
        resp = await asyncio.wait_for(poll, timeout=3.0)
        assert resp.status == 200
        item = await resp.json()
        assert item is not None
        assert item["text"] == "ping"
    store.close()


def test_claim_next_skips_write_transaction_when_idle(tmp_path, monkeypatch):
    store = CollaborationStore(tmp_path / "hub.db", set(ACTORS))
    tx_entered = False
    real_tx = store.transaction

    def tracked_tx():
        nonlocal tx_entered
        tx_entered = True
        return real_tx()

    monkeypatch.setattr(store, "transaction", tracked_tx)
    assert store.claim_next("alpha", "inst1") is None
    assert not tx_entered
    store.close()
