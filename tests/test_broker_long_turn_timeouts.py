"""Active prompts, optional ceilings and inactivity use separate deadlines."""

import asyncio
import inspect
from types import SimpleNamespace

import pytest

import mattermost_acp.acp.broker as broker_module
from mattermost_acp.acp.broker import AcpBroker, AcpError


@pytest.fixture(autouse=True)
def clean_timeout_environment(monkeypatch):
    for name in (
        "ACP_PROMPT_HARD_MAX_SEC", "GROK_ACP_PROMPT_HARD_MAX_SEC",
        "ACP_PROMPT_IDLE_TIMEOUT_SEC", "GROK_ACP_PROMPT_IDLE_TIMEOUT_SEC",
        "ACP_RPC_TIMEOUT_SEC", "GROK_ACP_RPC_TIMEOUT_SEC",
    ):
        monkeypatch.delenv(name, raising=False)


class MemoryAdapter:
    """Exercise real broker requests/dispatch using a synthetic native transport."""

    def __init__(self, *, kind="agent_message_chunk", count=10, interval=0.025, **settings):
        self.broker = AcpBroker(**settings)
        self.broker._proc = SimpleNamespace(stdin=object(), returncode=None)
        self.broker._write = self.write
        self.kind, self.count, self.interval = kind, count, interval
        self.writes = []
        self.tasks = []

    async def write(self, message, *, guard=None, on_write=None):
        if guard is not None:
            assert guard()
        self.writes.append(message)
        if on_write is not None:
            on_write()
        if message["method"] == "session/prompt" and self.kind is not None:
            self.tasks.append(asyncio.create_task(self.stream(message)))

    async def stream(self, message):
        for index in range(self.count):
            await asyncio.sleep(self.interval)
            update = {"sessionUpdate": self.kind}
            if self.kind in {"agent_message_chunk", "agent_thought_chunk"}:
                update["content"] = {"type": "text", "text": "progress "}
            elif self.kind == "tool_call_update":
                update.update(toolCallId="tool-" + str(index), status="completed")
            elif self.kind == "usage_update":
                update.update(used=index, size=1000)
            await self.broker._dispatch({
                "jsonrpc": "2.0", "method": "session/update",
                "params": {"sessionId": "session", "update": update},
            })
        await self.broker._dispatch({
            "jsonrpc": "2.0", "id": message["id"], "result": {"stopReason": "end_turn"},
        })

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for future in self.broker._pending.values():
            future.cancel()
        self.broker._pending.clear()
        self.broker._proc = None


class AsyncioView:
    """Accelerate only the historical ceiling or quiet heartbeat clock."""

    def __init__(self, *, virtual_quiet=False):
        self.now = 0.0
        self.virtual_quiet = virtual_quiet
        self.real_loop = asyncio.get_running_loop()

    def __getattr__(self, name):
        return getattr(asyncio, name)

    def get_running_loop(self):
        if not self.virtual_quiet:
            return self.real_loop
        return SimpleNamespace(time=lambda: self.now, create_future=self.real_loop.create_future)

    async def wait_for(self, awaitable, timeout=None):
        if (
            self.virtual_quiet
            and inspect.iscoroutine(awaitable)
            and awaitable.cr_code.co_qualname == "Queue.get"
        ):
            awaitable.close()
            self.now += 16
            await asyncio.sleep(0)
            raise TimeoutError
        # Preserve actual progress cadence and inactivity. Only the original
        # two-hour absolute RPC deadline is shortened for this regression.
        if isinstance(awaitable, asyncio.Future) and timeout == 7200:
            timeout = 0.075
        return await asyncio.wait_for(awaitable, timeout=timeout)


def test_default_active_prompt_has_no_absolute_ceiling():
    broker = AcpBroker()
    assert broker.prompt_hard_max_s in {None, 0}
    assert broker.rpc_timeout_s == 120 and broker.prompt_idle_timeout_s == 900


@pytest.mark.parametrize(
    "kind", ["agent_message_chunk", "agent_thought_chunk", "tool_call_update"]
)
async def test_default_active_prompt_outlives_control_deadline_and_historical_ceiling(
    monkeypatch, kind
):
    monkeypatch.setattr(broker_module, "asyncio", AsyncioView())
    adapter = MemoryAdapter(kind=kind, rpc_timeout_s=0.04, prompt_idle_timeout_s=0.1)
    updates = []

    async def observed(update):
        updates.append(update.kind)

    try:
        result = await asyncio.wait_for(
            adapter.broker.prompt_turn("session", "long active task", on_update=observed), 2
        )
        assert result.stop_reason == "end_turn" and updates == [kind] * 10
        assert not adapter.broker._pending
        assert "session" not in adapter.broker.prompt_subscriptions
        assert [item["method"] for item in adapter.writes] == ["session/prompt"]
    finally:
        await adapter.close()


@pytest.mark.parametrize("setting", ["argument", "environment", "legacy_environment"])
async def test_zero_disables_prompt_ceiling_without_using_control_timeout(monkeypatch, setting):
    options = {"prompt_hard_max_s": 0} if setting == "argument" else {}
    if setting != "argument":
        name = (
            "ACP_PROMPT_HARD_MAX_SEC"
            if setting == "environment" else "GROK_ACP_PROMPT_HARD_MAX_SEC"
        )
        monkeypatch.setenv(name, "0")
    adapter = MemoryAdapter(rpc_timeout_s=0.04, prompt_idle_timeout_s=0.1, **options)
    try:
        assert adapter.broker.prompt_hard_max_s in {None, 0}
        result = await asyncio.wait_for(adapter.broker.prompt_turn("session", "active task"), 2)
        assert result.stop_reason == "end_turn"
        assert [item["method"] for item in adapter.writes] == ["session/prompt"]
    finally:
        await adapter.close()


@pytest.mark.parametrize("setting", ["argument", "environment", "legacy_environment"])
async def test_explicit_positive_prompt_ceiling_expires_despite_real_progress(monkeypatch, setting):
    options = {"prompt_hard_max_s": 0.075} if setting == "argument" else {}
    if setting != "argument":
        name = (
            "ACP_PROMPT_HARD_MAX_SEC"
            if setting == "environment" else "GROK_ACP_PROMPT_HARD_MAX_SEC"
        )
        monkeypatch.setenv(name, "0.075")
    adapter = MemoryAdapter(rpc_timeout_s=0.04, prompt_idle_timeout_s=1, **options)
    try:
        with pytest.raises(AcpError, match="RPC timeout: session/prompt"):
            await asyncio.wait_for(adapter.broker.prompt_turn("session", "bounded task"), 2)
        assert not adapter.broker._pending
        assert "session" not in adapter.broker.prompt_subscriptions
    finally:
        await adapter.close()


@pytest.mark.parametrize("setting", ["argument", "environment", "legacy_environment"])
async def test_positive_prompt_ceiling_does_not_inherit_shorter_control_deadline(
    monkeypatch, setting
):
    options = {"prompt_hard_max_s": 0.6} if setting == "argument" else {}
    if setting != "argument":
        name = (
            "ACP_PROMPT_HARD_MAX_SEC"
            if setting == "environment" else "GROK_ACP_PROMPT_HARD_MAX_SEC"
        )
        monkeypatch.setenv(name, "0.6")
    adapter = MemoryAdapter(rpc_timeout_s=0.04, prompt_idle_timeout_s=0.1, **options)
    try:
        result = await asyncio.wait_for(adapter.broker.prompt_turn("session", "active task"), 2)
        assert result.stop_reason == "end_turn"
        assert [item["method"] for item in adapter.writes] == ["session/prompt"]
    finally:
        await adapter.close()


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), float("-inf")])
def test_configured_prompt_ceiling_must_be_finite_and_nonnegative(value):
    with pytest.raises(ValueError):
        AcpBroker(prompt_hard_max_s=value)


@pytest.mark.parametrize("value", ["-1", "nan", "inf", "-inf", "invalid"])
@pytest.mark.parametrize("name", ["ACP_PROMPT_HARD_MAX_SEC", "GROK_ACP_PROMPT_HARD_MAX_SEC"])
def test_environment_prompt_ceiling_rejects_invalid_values(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        AcpBroker()


async def test_quiet_inactivity_cancels_native_prompt():
    adapter = MemoryAdapter(kind=None, prompt_idle_timeout_s=0.1, prompt_hard_max_s=10)
    try:
        with pytest.raises(AcpError, match="RPC idle timeout: session/prompt"):
            await asyncio.wait_for(adapter.broker.prompt_turn("session", "stalled task"), 2)
        cancels = [item for item in adapter.writes if item["method"] == "session/cancel"]
        assert len(cancels) == 1 and cancels[0]["params"] == {"sessionId": "session"}
        assert not adapter.broker._pending
    finally:
        await adapter.close()


@pytest.mark.parametrize("kind", ["heartbeat", "usage_update"])
async def test_nonprogress_traffic_does_not_defeat_inactivity_cancellation(kind):
    adapter = MemoryAdapter(
        kind=kind, count=100, interval=0.025, prompt_idle_timeout_s=0.15,
        prompt_hard_max_s=10,
    )
    updates = []

    async def observed(update):
        updates.append(update.kind)

    try:
        with pytest.raises(AcpError, match="RPC idle timeout: session/prompt"):
            await asyncio.wait_for(
                adapter.broker.prompt_turn("session", "stalled task", on_update=observed), 1
            )
        assert updates and set(updates) == {kind}
        assert [item["method"] for item in adapter.writes] == ["session/prompt", "session/cancel"]
        assert not adapter.broker._pending
    finally:
        await adapter.close()


async def test_locally_generated_heartbeat_does_not_reset_inactivity(monkeypatch):
    clock = AsyncioView(virtual_quiet=True)
    monkeypatch.setattr(broker_module, "asyncio", clock)
    adapter = MemoryAdapter(kind=None, prompt_idle_timeout_s=30, prompt_hard_max_s=3600)
    updates = []

    async def observed(update):
        updates.append(update.kind)

    try:
        with pytest.raises(AcpError, match="RPC idle timeout: session/prompt"):
            await adapter.broker.prompt_turn("session", "quiet task", on_update=observed)
        assert updates == ["heartbeat"] and clock.now == 32
        assert [item["method"] for item in adapter.writes] == ["session/prompt", "session/cancel"]
        assert not adapter.broker._pending
    finally:
        await adapter.close()


@pytest.mark.parametrize("prompt_ceiling", [0, 10])
async def test_control_plane_rpc_retains_its_own_timeout(prompt_ceiling):
    adapter = MemoryAdapter(kind=None, rpc_timeout_s=0.04, prompt_hard_max_s=prompt_ceiling)
    try:
        with pytest.raises(AcpError, match="RPC timeout: session/new"):
            await asyncio.wait_for(adapter.broker.request("session/new", {"cwd": "/tmp"}), 1)
        assert [item["method"] for item in adapter.writes] == ["session/new"]
        assert not adapter.broker._pending
    finally:
        await adapter.close()
