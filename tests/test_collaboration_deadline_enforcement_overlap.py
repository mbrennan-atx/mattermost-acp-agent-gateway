"""Concurrent deadline observers retain one bounded native stop attempt."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from test_collaboration import active_gateway

from mattermost_acp.collaboration.client import HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer


async def test_overlapping_expiry_observers_stop_a_native_execution_once(tmp_path):
    gateway = active_gateway()
    gateway.config.db_path = tmp_path / "gateway.db"
    gateway.sessions.native_epoch = lambda _thread: 7
    consumer = CollaborationConsumer(gateway, Settings("http://127.0.0.1:1", "synthetic" * 5))
    key = ("request", "claim")
    report = {
        "execution_id": "execution",
        "collaboration_project": "project",
        "native_dispatched": True,
        "reporter": SimpleNamespace(),
    }
    consumer.project_deadlines["project"] = -1
    consumer.ownership[key] = report
    consumer.ownership_items[key] = {
        "project": "project",
        "binding": {"thread_id": "discussion"},
        "reservation_evidence": {
            "session_id": "native",
            "execution_id": "execution",
            "native_epoch": 7,
        },
    }
    gateway._reports["native"] = report
    consumer.hub.request = AsyncMock(side_effect=HubError("Hub unavailable", status=503))
    entered, release = asyncio.Event(), asyncio.Event()

    async def cancel(_thread, **_options):
        entered.set()
        await release.wait()

    gateway.sessions.cancel = AsyncMock(side_effect=cancel)
    tasks = []
    try:
        tasks.append(asyncio.create_task(consumer.enforce_deadline("project")))
        await asyncio.wait_for(entered.wait(), 1)
        tasks.append(asyncio.create_task(consumer.enforce_deadline("project")))
        await asyncio.sleep(0)
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 1)
        gateway.sessions.cancel.assert_awaited_once()
        assert consumer.ownership[key] is report and "project" in consumer.project_deadlines
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.close()
