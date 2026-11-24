"""Inactivity stops dispatched ordinary work without admitting new work."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_collaboration import active_gateway
from test_collaboration_dispatch_review import close_reports

from mattermost_acp.acp.broker import AcpBroker, NativeDispatchChanged, native_command_admission
from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.session_service import SessionService


class Transport:
    def __init__(self):
        self.messages = []
        self.dispatched = asyncio.Event()

    def write(self, payload):
        message = json.loads(payload)
        self.messages.append(message)
        if message.get("method") == "session/prompt":
            self.dispatched.set()

    async def drain(self):
        pass


def ordinary_gateway(tmp_path):
    broker = AcpBroker(prompt_idle_timeout_s=0.025)
    transport = Transport()
    broker._proc = SimpleNamespace(stdin=transport, returncode=None)
    record = SimpleNamespace(session_id="existing-native", cwd=str(tmp_path))
    store = SimpleNamespace(get=AsyncMock(return_value=record), save_model_if_session=AsyncMock())
    sessions = SessionService(broker, store, default_cwd=tmp_path)
    sessions.ensure_session = AsyncMock(return_value=record.session_id)
    sessions.status = AsyncMock(return_value={"session_id": record.session_id})
    gateway = active_gateway(busy=False)
    gateway.sessions = sessions
    gateway.collaboration = SimpleNamespace(owns_thread=lambda _thread: False)
    gateway.run_prompt = MattermostGateway.run_prompt.__get__(gateway)
    return gateway, broker, transport, record


async def run_ordinary(gateway):
    return await gateway.run_prompt(
        thread_id="discussion",
        channel_id="floor",
        prompt="stalled ordinary work",
        author_id="human",
        author_name="Synthetic human",
        passthrough=True,
    )


async def nonprogress(broker):
    await asyncio.sleep(0.035)
    await broker._dispatch(
        {
            "method": "session/update",
            "params": {
                "sessionId": "existing-native",
                "update": {
                    "sessionUpdate": "usage_update",
                    "used": 1,
                    "size": 10,
                },
            },
        }
    )


@pytest.mark.parametrize("closed", ["drain", "recovery", "sealed", "unchanged"])
async def test_ordinary_idle_cancels_dispatched_native_work_after_admission_closes(
    tmp_path, closed
):
    gateway, broker, transport, _ = ordinary_gateway(tmp_path)
    process = broker._proc
    turn = asyncio.create_task(run_ordinary(gateway))
    try:
        await asyncio.wait_for(transport.dispatched.wait(), 2)
        if closed == "recovery":
            gateway.collaboration.recovery_pending = True
        elif closed != "unchanged":
            setattr(gateway, "draining" if closed == "drain" else "sealed", True)
        await nonprogress(broker)
        await asyncio.wait_for(turn, 2)
        cancellations = [m for m in transport.messages if m["method"] == "session/cancel"]
        assert [m["params"] for m in cancellations] == [{"sessionId": "existing-native"}]
        assert any("RPC idle timeout" in post[1] for post in gateway.client.posts)
        assert broker._proc is process and not broker._pending
        assert not gateway.sessions.is_busy("discussion")
        assert "existing-native" not in broker.prompt_subscriptions
    finally:
        turn.cancel()
        await asyncio.gather(turn, return_exceptions=True)
        await close_reports(gateway)


@pytest.mark.parametrize("changed", ["epoch", "report", "replacement", "process"])
async def test_ordinary_idle_cancellation_rechecks_owner_after_transport_wait(tmp_path, changed):
    gateway, broker, transport, record = ordinary_gateway(tmp_path)
    entered = asyncio.Event()
    original = broker.cancel

    async def observed_cancel(*args, **kwargs):
        entered.set()
        return await original(*args, **kwargs)

    broker.cancel = observed_cancel
    turn = asyncio.create_task(run_ordinary(gateway))
    try:
        await asyncio.wait_for(transport.dispatched.wait(), 2)
        gateway.draining = True
        async with broker._write_lock:
            notifications = asyncio.create_task(nonprogress(broker))
            await asyncio.wait_for(entered.wait(), 2)
            if changed == "epoch":
                gateway.sessions._advance_native("discussion")
            elif changed == "report":
                gateway._reports["existing-native"] = dict(
                    gateway._reports["existing-native"], execution_id="replacement"
                )
            elif changed == "replacement":
                gateway.sessions._replacement_pending.add("discussion")
            else:
                broker._proc = SimpleNamespace(stdin=transport, returncode=None)
        await notifications
        await asyncio.wait_for(turn, 2)
        assert [m["method"] for m in transport.messages] == ["session/prompt"]
        assert not broker._pending
    finally:
        gateway._reports.pop("existing-native", None)
        turn.cancel()
        await asyncio.gather(turn, return_exceptions=True)
        await close_reports(gateway)


async def test_ordinary_idle_cancellation_refuses_replaced_session(tmp_path):
    gateway, broker, transport, record = ordinary_gateway(tmp_path)
    turn = asyncio.create_task(run_ordinary(gateway))
    try:
        await asyncio.wait_for(transport.dispatched.wait(), 2)
        record.session_id = "replacement-native"
        gateway.draining = True
        await nonprogress(broker)
        await asyncio.wait_for(turn, 2)
        assert [m["method"] for m in transport.messages] == ["session/prompt"]
    finally:
        turn.cancel()
        await asyncio.gather(turn, return_exceptions=True)
        await close_reports(gateway)


async def test_new_native_commands_still_require_work_admission(tmp_path):
    _gateway, broker, transport, _record = ordinary_gateway(tmp_path)
    admitted = True
    with native_command_admission(lambda: admitted):
        admitted = False
        with pytest.raises(NativeDispatchChanged):
            await broker.request("session/new", {"cwd": str(tmp_path)})
    assert transport.messages == []


async def test_idle_before_prompt_dispatch_never_sends_native_cancellation(tmp_path):
    gateway, broker, transport, _record = ordinary_gateway(tmp_path)
    turn = None
    try:
        async with broker._write_lock:
            turn = asyncio.create_task(run_ordinary(gateway))
            async with asyncio.timeout(2):
                while not broker._pending:
                    await asyncio.sleep(0)
            await nonprogress(broker)
            await asyncio.wait_for(asyncio.shield(turn), 0.5)
            assert not gateway.sessions.is_busy("discussion")
        assert transport.messages == []
        assert not broker._pending
    finally:
        if turn is not None:
            turn.cancel()
            await asyncio.gather(turn, return_exceptions=True)
        await close_reports(gateway)
