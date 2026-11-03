import asyncio

import pytest
from test_gateway_turn import _Cfg, _Client, _Sessions

from mattermost_acp.acp import SessionUpdate, TurnResult
from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.mm.reporting import TurnReporter


def test_complete_parent_narration_periodic_age_and_sanitizing():
    now = [0.0]
    reporter = TurnReporter(clock=lambda: now[0])
    reporter.observe(
        SessionUpdate(
            session_id="root", kind="agent_message_chunk", text="Build finished. @someone"
        )
    )
    assert reporter.latest == ""  # Do not post partial answers.
    reporter.observe(SessionUpdate(session_id="root", kind="tool_call"))
    now[0] = 59
    assert reporter.due() is None
    now[0] = 60
    assert "Build finished. ＠someone" in reporter.due()
    reporter.published()
    assert reporter.due() is None
    now[0] = 120
    assert "Last parent update 120s ago" in reporter.due()


def test_child_activity_age_never_looks_like_fresh_progress():
    now = [100.0]
    reporter = TurnReporter(clock=lambda: now[0])
    reporter.observe(
        SessionUpdate(
            session_id="root",
            kind="subagent_activity",
            raw={
                "worker_id": "worker",
                "name": "Astra",
                "state": "requires_action",
                "model": "gpt-6-astra",
                "latest": "Waiting for CI",
                "updated_at": 80.0,
            },
        )
    )
    now[0] = 160
    assert "last activity 80s ago" in reporter.due()
    assert "requires_action" in reporter.due()
    assert "no new parent progress report" in reporter.due()


@pytest.mark.parametrize("fail", [False, True])
async def test_live_thread_post_and_reporter_cleanup(monkeypatch, fail):
    import mattermost_acp.mm.reporting as reporting

    monkeypatch.setattr(reporting, "REPORT_INTERVAL_S", 0)
    posted = asyncio.Event()

    class Client(_Client):
        async def create_post(self, channel_id, message, root_id=None):
            result = await super().create_post(channel_id, message, root_id)
            if message.startswith("Progress:"):
                posted.set()
            return result

    class Sessions(_Sessions):
        async def run_turn(self, thread_id, text, **kwargs):
            update = kwargs["on_update"]
            await update(
                SessionUpdate(
                    session_id="root",
                    kind="agent_message_chunk",
                    text="Astra fixed the regression.",
                )
            )
            await update(SessionUpdate(session_id="root", kind="tool_call"))
            await asyncio.wait_for(posted.wait(), timeout=3)
            if fail:
                raise RuntimeError("turn failed")
            return TurnResult(
                session_id="root",
                stop_reason="end_turn",
                answer_text="Final result",
                thought_text="",
            )

    client = Client()
    gateway = MattermostGateway(_Cfg(), client, Sessions([], None))
    await gateway.run_prompt(
        thread_id="root", channel_id="channel", prompt="work", author_id="user", author_name="owner"
    )
    progress = [row for row in client.created if row[1].startswith("Progress:")]
    assert progress and progress[0][2] == "root"
    assert not [
        task
        for task in asyncio.all_tasks()
        if task.get_name() == "progress-root" and not task.done()
    ]


async def test_child_progress_survives_parent_rpc_return_and_quiet_goal_continues(monkeypatch):
    from types import SimpleNamespace

    import mattermost_acp.mm.reporting as reporting

    monkeypatch.setattr(reporting, "REPORT_INTERVAL_S", 0)
    delivered = asyncio.Event()

    class Client(_Client):
        async def create_post(self, channel_id, message, root_id=None):
            result = await super().create_post(channel_id, message, root_id)
            if message.startswith("Progress:"):
                delivered.set()
            return result

    class Sessions(_Sessions):
        broker = SimpleNamespace(update_observer=None)

        async def status(self, thread_id):
            return {"session_id": "root-session", "model": "gpt-6-astra", "effort": "high"}

    result = TurnResult(
        session_id="root-session",
        stop_reason="end_turn",
        answer_text="Worker is still running.",
        thought_text="",
    )
    sessions = Sessions(
        [
            SessionUpdate(
                session_id="root-session",
                kind="subagent_activity",
                raw={
                    "worker_id": "child",
                    "name": "Astra",
                    "state": "running",
                },
            )
        ],
        result,
    )
    client = Client()
    gateway = MattermostGateway(_Cfg(), client, sessions)
    try:
        await gateway.run_prompt(
            thread_id="root",
            channel_id="channel",
            prompt="work",
            author_id="user",
            author_name="owner",
        )
        assert not gateway._reports["root-session"]["task"].done()
        # No run_turn/on_update subscriber exists now, as with a goal continuation.
        for kind, text in [
            ("agent_thought_chunk", "private reasoning"),
            ("agent_message_chunk", "Final CI is running."),
            ("tool_call", None),
        ]:
            sessions.broker.update_observer(
                SessionUpdate(session_id="root-session", kind=kind, text=text)
            )
        await asyncio.wait_for(delivered.wait(), timeout=3)
        messages = [row[1] for row in client.created]
        assert any("Final CI is running." in message for message in messages)
        assert all("private reasoning" not in message for message in messages)
        gateway._stop_reports("root")
        sessions.broker.update_observer(SessionUpdate(session_id="root-session", kind="tool_call"))
        assert "root-session" not in gateway._reports
    finally:
        await gateway.close()
        await asyncio.sleep(0)


async def test_out_of_band_activity_restarts_reporter_after_normal_parent_return(monkeypatch):
    from types import SimpleNamespace

    import mattermost_acp.mm.reporting as reporting

    monkeypatch.setattr(reporting, "REPORT_INTERVAL_S", 0)

    class Sessions(_Sessions):
        broker = SimpleNamespace(update_observer=None)

        async def status(self, thread_id):
            return {"session_id": "session"}

    result = TurnResult(
        session_id="session", stop_reason="end_turn", answer_text="Status reply.", thought_text=""
    )
    sessions = Sessions([], result)
    gateway = MattermostGateway(_Cfg(), _Client(), sessions)
    try:
        await gateway.run_prompt(
            thread_id="thread",
            channel_id="channel",
            prompt="status",
            author_id="user",
            author_name="owner",
        )
        old_task = gateway._reports["session"]["task"]
        assert old_task.done()
        sessions.broker.update_observer(
            SessionUpdate(session_id="session", kind="agent_message_chunk", text="Goal continues.")
        )
        sessions.broker.update_observer(SessionUpdate(session_id="session", kind="tool_call"))
        report = gateway._reports["session"]
        assert report["task"] is not old_task and not report["task"].done()
        assert report["reporter"].latest == "Goal continues."
    finally:
        await gateway.close()
        await asyncio.sleep(0)


def test_background_tool_wait_remains_pending_until_actual_completion():
    reporter = TurnReporter()
    reporter.observe(SessionUpdate(session_id="root", kind="tool_call", tool_call_id="ci-wait"))
    assert reporter.has_pending_work()
    reporter.observe(SessionUpdate(session_id="root", kind="heartbeat"))
    assert reporter.has_pending_work()
    reporter.observe(
        SessionUpdate(
            session_id="root",
            kind="tool_call_update",
            tool_call_id="ci-wait",
            tool_status="completed",
        )
    )
    assert not reporter.has_pending_work()
    # A late content-only patch must not reopen a finished wait.
    reporter.observe(
        SessionUpdate(session_id="root", kind="tool_call_update", tool_call_id="ci-wait")
    )
    assert not reporter.has_pending_work()


@pytest.mark.parametrize("fail_edit_once", [False, True])
async def test_periodic_progress_edits_one_post_and_retries_without_duplicates(
    monkeypatch, fail_edit_once
):
    import mattermost_acp.mm.reporting as reporting

    monkeypatch.setattr(reporting, "REPORT_INTERVAL_S", 0)
    created = asyncio.Event()
    edited = asyncio.Event()

    class Client(_Client):
        attempts = 0

        async def create_post(self, channel_id, message, root_id=None):
            post = await super().create_post(channel_id, message, root_id)
            created.set()
            return post

        async def update_post(self, post_id, message):
            self.attempts += 1
            if fail_edit_once and self.attempts == 1:
                raise RuntimeError("temporary Mattermost failure")
            result = await super().update_post(post_id, message)
            edited.set()
            return result

    client = Client()
    gateway = MattermostGateway(_Cfg(), client, _Sessions([], None))
    reporter = TurnReporter()
    reporter.latest = "Waiting for CI."
    report = {"reporter": reporter, "channel_id": "channel", "thread_id": "thread", "active": True}
    task = asyncio.create_task(gateway._publish_progress(report))
    try:
        await asyncio.wait_for(created.wait(), timeout=3)
        reporter.latest = "CI passed."
        await asyncio.wait_for(edited.wait(), timeout=4)
        assert len(client.created) == 1
        assert client.created[0][0] == "channel" and client.created[0][2] == "thread"
        assert client.created[0][1].startswith("Progress: Waiting for CI.")
        assert len(client.updated) == 1
        assert client.updated[0][0] == "post1"
        assert client.updated[0][1].startswith("Progress: CI passed.")
        assert client.attempts == (2 if fail_edit_once else 1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


def test_progress_post_identity_is_per_run():
    first, second = TurnReporter(), TurnReporter()
    first.post_id = "first-run"
    assert second.post_id is None
