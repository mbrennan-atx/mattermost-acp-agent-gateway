"""Steering stays on the active session and never queues a competing prompt."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from mattermost_acp.acp.broker import AcpBroker, AcpError
from mattermost_acp.commands.defs import SLASH_VERBS, help_text, parse_text_command
from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.mm.slash import parse_slash_payload
from mattermost_acp.session_service import SessionService


def broker_for(backend="codex", *, patched=True):
    broker = AcpBroker(backend=backend)
    steering = {"supported": True}
    if patched:
        steering["idleBehavior"] = "promptRequired"
    broker._capabilities = broker._parse_capabilities({"_meta": {"steering": steering}})
    broker.request = AsyncMock(return_value={"outcome": "injected"})
    return broker


@pytest.mark.parametrize("backend", ["claude", "codex"])
async def test_hint_bypasses_turn_lock_and_uses_active_only_extension(backend):
    broker = broker_for(backend)
    async with broker._turn_lock("session"):
        assert await asyncio.wait_for(broker.steer("session", "  keep existing tests  "), 1)
    broker.request.assert_awaited_once_with(
        "_session/steering",
        {
            "sessionId": "session",
            "prompt": [{"type": "text", "text": "keep existing tests"}],
            "_meta": {"steering": {"idleBehavior": "promptRequired"}},
        },
    )


async def test_hint_idle_race_stays_host_owned():
    broker = broker_for()
    broker.request.return_value = {"outcome": "promptRequired", "reason": "noRunningTurn"}
    assert await broker.steer("session", "check reconnect") == "promptRequired"
    assert broker.request.await_count == 1


@pytest.mark.parametrize("response", [{"outcome": "failed"}, {}, None])
async def test_hint_failure_is_never_claimed_delivered(response):
    broker = broker_for()
    broker.request.return_value = response
    with pytest.raises(AcpError, match="not confirmed"):
        await broker.steer("session", "check reconnect")


@pytest.mark.parametrize("backend", ["cursor", "grok"])
async def test_unsupported_adapter_receives_no_hint_rpc(backend):
    broker = AcpBroker(backend=backend)
    broker.request = AsyncMock()
    with pytest.raises(AcpError, match="not supported"):
        await broker.steer("session", "hello")
    broker.request.assert_not_awaited()


async def test_unpatched_codex_is_rejected_before_rpc():
    broker = broker_for(patched=False)
    with pytest.raises(AcpError, match="patch and a reload"):
        await broker.steer("session", "hello")
    broker.request.assert_not_awaited()


async def test_empty_hint_is_rejected_before_rpc():
    broker = broker_for()
    with pytest.raises(AcpError, match="Usage"):
        await broker.steer("session", "  ")
    broker.request.assert_not_awaited()


async def test_service_does_not_resume_or_repin_active_session(tmp_path):
    broker = broker_for()
    broker._proc = SimpleNamespace(returncode=None)
    store = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(session_id="existing")))
    svc = SessionService(broker, store, default_cwd=tmp_path)
    svc.ensure_session = AsyncMock(side_effect=AssertionError("must not resume"))
    # Native goal turns may be active after the foreground gateway RPC returns.
    assert not svc.is_busy("thread")
    assert await svc.hint("thread", "check reconnect") == "injected"
    assert broker.request.call_args.args[1]["sessionId"] == "existing"
    svc.ensure_session.assert_not_awaited()


async def test_hint_without_session_does_not_start_work(tmp_path):
    broker = broker_for()
    store = SimpleNamespace(get=AsyncMock(return_value=None))
    svc = SessionService(broker, store, default_cwd=tmp_path)
    with pytest.raises(AcpError, match="No active"):
        await svc.hint("missing", "check reconnect")
    broker.request.assert_not_awaited()


def gateway(outcome="injected"):
    config = SimpleNamespace(allow_user_ids=frozenset({"owner"}), channel_ids=frozenset())
    client = SimpleNamespace(create_post=AsyncMock(return_value={"id": "post"}))
    sessions = SimpleNamespace(hint=AsyncMock(return_value=outcome))
    return MattermostGateway(config, client, sessions)


@pytest.mark.parametrize(
    "outcome, message",
    [("injected", "Hint delivered"), ("promptRequired", "no active turn")],
)
async def test_hint_command_reports_actual_outcome(outcome, message):
    gw = gateway(outcome)
    await gw.dispatch_command(
        thread_id="thread", channel_id="channel", cmd="hint", arg="keep scope small",
        user_id="owner", user_name="",
    )
    gw.sessions.hint.assert_awaited_once_with("thread", "keep scope small")
    assert message in gw.client.create_post.call_args.args[1]
    assert gw.client.create_post.call_args.kwargs["root_id"] == "thread"


async def test_hint_command_surfaces_error_without_retry():
    gw = gateway()
    gw.sessions.hint.side_effect = AcpError("RPC timeout")
    await gw.dispatch_command(
        thread_id="thread", channel_id="channel", cmd="hint", arg="keep scope small",
        user_id="owner", user_name="",
    )
    assert "not confirmed" in gw.client.create_post.call_args.args[1]
    assert gw.sessions.hint.await_count == 1


@pytest.mark.parametrize("arg", ["", "  "])
async def test_empty_command_only_shows_usage(arg):
    gw = gateway()
    await gw.dispatch_command(
        thread_id="thread", channel_id="channel", cmd="hint", arg=arg,
        user_id="owner", user_name="",
    )
    gw.sessions.hint.assert_not_awaited()
    assert "Usage" in gw.client.create_post.call_args.args[1]


async def test_hint_slash_without_thread_creates_no_starter_post():
    gw = gateway()
    req = parse_slash_payload({"command": "/hint", "text": "hello", "user_id": "owner"})
    result = await gw.handle_slash(req)
    assert "inside the thread" in result["text"]
    gw.client.create_post.assert_not_awaited()
    gw.sessions.hint.assert_not_awaited()


async def test_hint_keeps_owner_allowlist():
    gw = gateway()
    req = parse_slash_payload({"command": "/hint", "root_id": "thread", "user_id": "other"})
    assert (await gw.handle_slash(req))["text"] == "Not allowed."
    gw.sessions.hint.assert_not_awaited()


def test_hint_text_slash_and_catchall_keep_multiline_guidance():
    text = "Check reconnect.\nKeep existing tests."
    assert parse_text_command("/hint " + text) == ("hint", text)
    req = parse_slash_payload({"command": "/agent", "text": "hint " + text})
    assert (req.command, req.text) == ("hint", text)
    assert "hint" in SLASH_VERBS
    assert "/hint <text>" in help_text(home_channel_ids=[])
