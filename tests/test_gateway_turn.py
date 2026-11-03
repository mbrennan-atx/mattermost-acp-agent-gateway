"""End-to-end wiring of one turn: status post, model stamp, answer assembly."""

import pytest
from fake_agent import spawn_fake_agent_process

from mattermost_acp.acp import SessionUpdate, TurnResult
from mattermost_acp.acp.broker import AcpBroker
from mattermost_acp.mm.client import PostedEvent
from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.session_service import SessionService
from mattermost_acp.store import ThreadStore


class _Cfg:
    auto_rename_threads = False
    inject_channel_context = False
    show_thoughts = False
    allow_user_ids = frozenset()
    channel_ids = frozenset()
    team_id = "team"
    harness = "grok"
    post_max_lines = 20
    status_tool_lines = 6
    status_done_tool_lines = 0
    status_progress_lines = 3


class _Client:
    def __init__(self) -> None:
        self.created: list[tuple[str, str, str | None]] = []
        self.updated: list[tuple[str, str]] = []
        self._n = 0

    async def create_post(self, channel_id, message, root_id=None):
        self._n += 1
        self.created.append((channel_id, message, root_id))
        return {"id": f"post{self._n}"}

    async def update_post(self, post_id, message):
        self.updated.append((post_id, message))
        return {"id": post_id}


class _Sessions:
    def __init__(self, updates, result) -> None:
        self._updates = updates
        self._result = result

    def is_busy(self, thread_id: str) -> bool:
        return False

    async def ensure_session(self, thread_id, *, channel_id=None, route=None):
        return "sess"

    async def status(self, thread_id):
        return {"model": "hub-grok-4.6", "effort": "high"}

    async def run_turn(self, thread_id, text, *, channel_id=None, on_update=None, route=None):
        for update in self._updates:
            await on_update(update)
        return self._result


def _updates() -> list[SessionUpdate]:
    return [
        SessionUpdate(
            session_id="s", kind="agent_message_chunk", text="Checking the config."
        ),
        SessionUpdate(
            session_id="s",
            kind="tool_call",
            tool_call_id="t1",
            tool_title="read_file config.py",
            tool_kind="read",
        ),
        SessionUpdate(
            session_id="s",
            kind="tool_call_update",
            tool_call_id="t1",
            tool_status="completed",
        ),
        SessionUpdate(
            session_id="s",
            kind="agent_message_chunk",
            text="## Answer\n\nThe timeout is 30s.",
        ),
    ]


async def _run(result: TurnResult) -> _Client:
    client = _Client()
    gw = MattermostGateway(_Cfg(), client, _Sessions(_updates(), result))
    await gw.run_prompt(
        thread_id="root1",
        channel_id="chan1",
        prompt="what is the timeout?",
        author_id="u1",
        author_name="example-user",
    )
    return client


async def test_turn_posts_stamped_answer_and_rich_status() -> None:
    result = TurnResult(
        session_id="s",
        stop_reason="end_turn",
        answer_text="ignored when the progress buffer has text",
        thought_text="",
        usage_used=84120,
        usage_size=500000,
    )
    client = await _run(result)

    # First created post is the live status post; the rest are the answer.
    answers = [msg for _c, msg, _r in client.created[1:]]
    assert len(answers) == 1
    answer = answers[0]
    assert answer.startswith("> *Model: Grok 4.6 · Effort: high*")
    assert "Checking the config." not in answer  # already shown in live progress
    assert "## Answer" in answer  # final answer left alone
    assert "The timeout is 30s." in answer

    done = client.updated[-1][1]
    assert done.startswith("> *Model: Grok 4.6 · Effort: high*")
    assert "✅ Done · end_turn" in done
    assert "ctx 84,120/500,000 (17%)" in done
    # The finished status post collapses to the summary line: no tool timeline,
    # no narration left in the thread.
    assert "> 📖 read_file config.py ·" not in done
    assert done.count("\n") == 1


async def test_turn_falls_back_to_broker_answer_text() -> None:
    client = _Client()
    result = TurnResult(
        session_id="s",
        stop_reason="end_turn",
        answer_text="fallback body",
        thought_text="",
    )
    gw = MattermostGateway(_Cfg(), client, _Sessions([], result))
    await gw.run_prompt(
        thread_id="root1",
        channel_id="chan1",
        prompt="hi",
        author_id="u1",
        author_name="example-user",
    )
    answers = [msg for _c, msg, _r in client.created[1:]]
    assert answers == ["> *Model: Grok 4.6 · Effort: high*\n\nfallback body"]


@pytest.mark.parametrize("harness", ["grok", "claude", "codex", "cursor"])
async def test_single_assistant_root_reply_and_new_root_without_persona(tmp_path, harness):
    """Exercise posted-message routing through real session storage and ACP pipes."""
    cfg = _Cfg()
    cfg.harness = harness
    cfg.all_channels = False
    cfg.channel_ids = frozenset({"example-channel"})
    cfg.exclude_channel_ids = frozenset()
    cfg.allow_user_ids = frozenset({"example-owner"})
    cfg.peer_channel_id = None
    cfg.text_slash = True
    cfg.fleet_file = None
    cfg.role_file = None
    client = _Client()
    client.user_id = "example-assistant"
    client.username = "arbitrary-helper"
    broker = AcpBroker(process_factory=spawn_fake_agent_process, backend=harness)
    store = ThreadStore(tmp_path / "external state" / "threads.db", harness=harness)
    await store.open()
    sessions = SessionService(
        broker, store, default_cwd=tmp_path, grok_home=tmp_path / "native-home",
        inject_channel_context=False
    )
    gateway = MattermostGateway(cfg, client, sessions)
    try:
        async def post(post_id, root_id, message):
            await gateway.handle_posted(PostedEvent(
                post_id=post_id, root_id=root_id, channel_id="example-channel",
                user_id="example-owner", message=message,
            ))

        await post("first-root", "", "first question")
        first = await store.get("first-root")
        assert first is not None
        await post("reply", "first-root", "follow-up question")
        reply = await store.get("first-root")
        assert reply.session_id == first.session_id
        assert await store.get("reply") is None
        await post("second-root", "", "independent question")
        second = await store.get("second-root")
        assert second is not None and second.session_id != first.session_id
        assert first.harness == second.harness == harness
        answers = [(text, root) for _, text, root in client.created if "Hello " in text]
        assert len(answers) == 3
        assert [root for _, root in answers] == ["first-root", "first-root", "second-root"]
        assert all(word in answers[i][0] for i, word in enumerate(
            ["first question", "follow-up question", "independent question"]
        ))
    finally:
        await broker.stop()
        await store.close()
