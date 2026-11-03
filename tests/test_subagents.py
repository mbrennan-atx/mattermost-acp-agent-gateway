import asyncio

import pytest

from mattermost_acp.acp import SessionUpdate
from mattermost_acp.acp.broker import AcpBroker
from mattermost_acp.acp.subagents import SubagentTracker


def announcement(kind="subagent_spawned", parent="root", child="child"):
    raw = {"subagentSessionId": child, "name": "Astra"}
    if kind == "subagent_update":
        raw = {"sessionId": child, "title": "Astra", "state": {"state": "running"}}
    return SessionUpdate(session_id=parent, kind=kind, raw=raw)


@pytest.mark.parametrize("backend", ["codex", "claude", "cursor"])
async def test_broker_routes_child_status_without_answer_or_thought_leak(backend):
    broker = AcpBroker(backend=backend)
    root, other = asyncio.Queue(), asyncio.Queue()
    broker._session_queues = {"root": [root], "unrelated": [other]}

    async def dispatch(session, raw):
        await broker._dispatch(
            {
                "method": "session/update",
                "params": {
                    "sessionId": session,
                    "update": raw,
                },
            }
        )

    await dispatch(
        "root", {"sessionUpdate": "subagent_spawned", "subagentSessionId": "child", "name": "Astra"}
    )
    while not root.empty():
        root.get_nowait()
    await dispatch(
        "child",
        {"sessionUpdate": "agent_thought_chunk", "content": {"type": "text", "text": "private"}},
    )
    assert root.empty()
    await dispatch(
        "child",
        {
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": "Tests passed"},
        },
    )
    activity = root.get_nowait()
    assert activity.kind == "subagent_activity"
    assert activity.raw["latest"] == "Tests passed"
    assert activity.text is None
    assert other.empty()
    assert root.empty()


@pytest.mark.parametrize("kind", ["subagent_spawned", "subagent_update"])
def test_native_forms_nested_workers_and_terminal_states(kind):
    tracker = SubagentTracker()
    assert tracker.observe(announcement(kind))[0].raw["name"] == "Astra"
    nested = tracker.observe(announcement(kind, parent="child", child="grandchild"))
    assert [event.session_id for event in nested] == ["child", "root"]
    states = tracker.observe(
        SessionUpdate(
            session_id="child",
            kind="subagent_update",
            raw={"sessionId": "grandchild", "state": {"state": "idle", "stopReason": "error"}},
        )
    )
    assert all(event.raw["state"] == "failed" for event in states)
    assert len(tracker.snapshots("root")) == 2
    assert tracker.snapshots("unrelated") == []


def test_orphan_reparent_and_cycle_are_rejected():
    tracker = SubagentTracker()
    assert (
        tracker.observe(
            SessionUpdate(
                session_id="orphan", kind="agent_message_chunk", text="never guess a parent"
            )
        )
        == []
    )
    tracker.observe(announcement())
    assert tracker.observe(announcement(parent="unrelated")) == []
    assert tracker.observe(announcement(parent="child", child="root")) == []
    assert tracker.observe(announcement(parent="root", child="root")) == []


async def test_initialize_requests_subagents_without_fs_or_terminal():
    from fake_agent import spawn_fake_agent_process

    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    original = broker.request
    seen = []

    async def request(method, params=None, **kwargs):
        if method == "initialize":
            seen.append(params["clientCapabilities"])
        return await original(method, params, **kwargs)

    broker.request = request
    try:
        await broker.start()
        assert seen == [{"subagents": {}}]
    finally:
        await broker.stop()


def test_closed_root_forgets_descendants_only():
    tracker = SubagentTracker()
    tracker.observe(announcement())
    tracker.observe(announcement(parent="child", child="nested"))
    tracker.observe(announcement(parent="other", child="other-child"))
    tracker.forget("root")
    assert set(tracker.workers) == {"other-child"}


async def test_broker_observer_receives_child_status_without_inflight_prompt():
    broker = AcpBroker()
    updates = []
    broker.update_observer = updates.append
    for session_id, raw in [
        (
            "root",
            {"sessionUpdate": "subagent_spawned", "subagentSessionId": "child", "name": "Astra"},
        ),
        (
            "child",
            {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "CI started"},
            },
        ),
    ]:
        await broker._dispatch(
            {
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": raw,
                },
            }
        )
    assert not broker._session_queues
    assert any(
        update.session_id == "root"
        and update.kind == "subagent_activity"
        and update.raw.get("latest") == "CI started"
        for update in updates
    )
