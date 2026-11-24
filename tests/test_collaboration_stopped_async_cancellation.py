"""Native stopped-task evidence settles cancellation after foreground completion."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.mm.reporting import TurnReporter


def example():
    c = object.__new__(CollaborationConsumer)
    reporter = TurnReporter()
    reporter.observed_tools.add("tool")
    reporter.async_tasks["shell"] = {"state": "stopped", "owned": True, "toolCallId": "tool"}
    report = {
        "thread_id": "thread",
        "execution_id": "execution",
        "reporter": reporter,
        "cancel_requested": True,
        "active": False,
        "foreground_complete": True,
        "collaboration_project": "project",
        "reservation_terminals": {},
    }
    c.ownership = {("request", "claim"): report}
    c.ownership_threads = {("request", "claim"): "thread"}
    c.workers = {}
    c.reserving = set()
    c.cancelled_claims = set()
    terminal = {
        "session_id": "native",
        "execution_id": "execution",
        "completed_at": 3,
        "pending": False,
        "stop_reason": "end_turn",
        "native_dispatched": True,
        "answer": "Foreground completed while shell was still running",
    }
    c.gateway = SimpleNamespace(
        _reports={"native": report},
        _last_terminal={"thread": terminal},
        sessions=SimpleNamespace(is_busy=lambda _thread: False),
    )
    reserved = {
        "session_id": "native",
        "execution_id": "execution",
        "reservation_id": "token",
        "native_epoch": 1,
        "dispatch_started": 2,
    }
    return c, reserved, report


@pytest.mark.parametrize(
    "change",
    ["execution", "report", "pending", "active", "unowned", "unknown_tool", "not_cancelled"],
)
def test_stopped_tasks_cannot_settle_unverified_work(change):
    c, reserved, report = example()
    if change == "execution":
        reserved["execution_id"] = "different"
    elif change == "report":
        c.gateway._reports["native"] = {}
    elif change == "pending":
        report["reporter"].active_tools.add("another")
    elif change == "active":
        report["active"] = True
    elif change == "unowned":
        report["reporter"].async_tasks["shell"]["owned"] = False
    elif change == "unknown_tool":
        report["reporter"].observed_tools.clear()
    else:
        report["cancel_requested"] = False
    assert c.stopped_owned_async_work(("request", "claim"), reserved) == []


async def test_stopped_shell_settles_shared_execution_and_preserves_foreground_reason():
    c, reserved, _report = example()
    target = {
        "id": "request",
        "claim": "claim",
        "instance": "instance",
        "state": "cancelling",
        "project": "project",
        "binding": {"thread_id": "thread"},
        "evidence": reserved,
    }
    posted = []

    async def request(method, _path, body=None):
        if method == "GET":
            return deepcopy(target)
        posted.append(body)
        target["state"] = body["state"]

    c.hub = SimpleNamespace(request=request)
    c.update = AsyncMock()
    await c.handle_cancel({"parent": "request", "binding": target["binding"]})
    assert posted[0]["state"] == "cancelled"
    evidence = posted[0]["evidence"]
    assert evidence["stop_reason"] == "cancelled"
    assert evidence["foreground_stop_reason"] == "end_turn"
    assert evidence["async_tasks_stopped"] == ["shell"]
    assert evidence["reservation_id"] == "token"
    assert not evidence["pending"]
    assert c.gateway._last_terminal["thread"]["stop_reason"] == "end_turn"
