"""AcpBroker against the fake agent process."""

from __future__ import annotations

import asyncio

import pytest
from fake_agent import spawn_fake_agent_process

from mattermost_acp.acp.broker import (
    AcpBroker,
    AcpError,
    _format_rpc_error,
    parse_session_update,
)


@pytest.fixture
async def broker():
    b = AcpBroker(process_factory=spawn_fake_agent_process)
    await b.start()
    yield b
    await b.stop()


@pytest.mark.asyncio
async def test_initialize_capabilities(broker: AcpBroker) -> None:
    assert broker.capabilities.load_session is True
    assert broker.capabilities.resume is True
    assert broker.capabilities.close is True


@pytest.mark.asyncio
async def test_session_new_and_prompt(broker: AcpBroker) -> None:
    sid = await broker.session_new("/tmp")
    assert sid.startswith("sess-test-")

    updates: list[str] = []

    async def on_update(u) -> None:
        updates.append(u.kind)

    result = await broker.prompt_turn(sid, "ping", on_update=on_update)
    assert result.stop_reason == "end_turn"
    assert "Hello " in result.answer_text
    assert "ping" in result.answer_text
    assert "read_file vault" in result.tool_titles
    assert result.usage_used == 1200
    assert "tool_call" in updates
    assert "agent_message_chunk" in updates


@pytest.mark.asyncio
async def test_two_sessions_isolated(broker: AcpBroker) -> None:
    a = await broker.session_new("/tmp/a")
    b = await broker.session_new("/tmp/b")
    ra = await broker.prompt_turn(a, "alpha")
    rb = await broker.prompt_turn(b, "beta")
    assert "alpha" in ra.answer_text
    assert "beta" in rb.answer_text
    assert "beta" not in ra.answer_text


def test_parse_session_update_model_id() -> None:
    upd = parse_session_update(
        {
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "user_message_chunk",
                "content": {"type": "text", "text": "hi"},
                "_meta": {"modelId": "gpt-5.6-sol"},
            },
        }
    )
    assert upd.model_id == "gpt-5.6-sol"
    assert upd.kind == "user_message_chunk"


@pytest.mark.asyncio
async def test_set_model_success(broker: AcpBroker) -> None:
    sid = await broker.session_new("/tmp")
    resolved = await broker.set_model(sid, "hub-claude-5-sonnet")
    assert resolved == "hub-claude-5-sonnet"


@pytest.mark.asyncio
async def test_set_model_unknown_raises(broker: AcpBroker) -> None:
    sid = await broker.session_new("/tmp")
    with pytest.raises(AcpError):
        await broker.set_model(sid, "bogus-model")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend, model, effort_key",
    [("claude", "opus", "effort"), ("codex", "gpt-6-astra", "reasoning_effort")],
)
async def test_set_model_uses_config_option(backend, model, effort_key) -> None:
    b = AcpBroker(command="adapter", backend=backend)
    seen: list[tuple[str, dict]] = []

    async def fake_request(method, params=None, *, timeout=None):
        seen.append((method, params or {}))
        if params.get("configId") == "model":
            return {
                "configOptions": [
                    {"id": "model", "currentValue": model},
                    {"id": "effort", "currentValue": "default"},
                ]
            }
        return {"configOptions": [{"id": effort_key, "currentValue": "high"}]}

    b.request = fake_request  # type: ignore[method-assign]
    resolved = await b.set_model("sess-1", model, effort="high")
    assert resolved == model
    assert b.session_config_options["sess-1"] == [{"id": effort_key, "currentValue": "high"}]
    assert seen[0] == (
        "session/set_config_option",
        {"sessionId": "sess-1", "configId": "model", "value": model},
    )
    assert seen[1] == (
        "session/set_config_option",
        {"sessionId": "sess-1", "configId": effort_key, "value": "high"},
    )


def test_build_argv_grok_backend() -> None:
    b = AcpBroker(command="grok", model="grok-4.6", always_approve=True)
    assert b._build_argv() == [
        "grok",
        "agent",
        "--always-approve",
        "--no-leader",
        "-m",
        "grok-4.6",
        "stdio",
    ]


def test_build_argv_claude_backend_has_no_grok_flags() -> None:
    b = AcpBroker(command="claude-agent-acp", backend="claude", model="claude-opus-5-5")
    assert b._build_argv() == ["claude-agent-acp"]


def test_build_argv_codex_backend_has_no_grok_flags() -> None:
    b = AcpBroker(command="codex-acp", backend="codex", model="gpt-6-astra")
    assert b._build_argv() == ["codex-acp"]


def test_pick_allow_prefers_always() -> None:
    from mattermost_acp.acp.broker import _pick_allow_option

    oid = _pick_allow_option(
        {
            "options": [
                {"optionId": "reject-once", "kind": "reject_once"},
                {"optionId": "allow-once", "kind": "allow_once"},
                {"optionId": "allow-always", "kind": "allow_always"},
            ]
        }
    )
    assert oid == "allow-always"


def test_is_tool_complete_update() -> None:
    from mattermost_acp.acp.broker import is_tool_complete_update
    from mattermost_acp.acp.types import SessionUpdate

    done = SessionUpdate(session_id="s", kind="tool_call_update", tool_status="completed")
    pending = SessionUpdate(session_id="s", kind="tool_call_update", tool_status="in_progress")
    call = SessionUpdate(session_id="s", kind="tool_call", tool_status="pending")
    assert is_tool_complete_update(done) is True
    assert is_tool_complete_update(pending) is False
    assert is_tool_complete_update(call) is False


async def _spawn_stall_agent(_argv: list[str]) -> asyncio.subprocess.Process:
    """Agent that accepts session/prompt but never finishes it."""
    driver = r"""
import sys, json
sessions = {}
n = 0
def sid():
    global n
    n += 1
    return f"sess-stall-{n}"
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    req_id = msg.get("id")
    params = msg.get("params") or {}
    def reply(result=None, error=None):
        if req_id is None:
            return
        out = {"jsonrpc": "2.0", "id": req_id}
        if error is not None:
            out["error"] = error
        else:
            out["result"] = result
        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()
    def notify(method, params):
        sys.stdout.write(json.dumps({"jsonrpc":"2.0","method":method,"params":params}) + "\n")
        sys.stdout.flush()
    if method == "initialize":
        reply({"protocolVersion":1,"agentCapabilities":{"loadSession":True,"sessionCapabilities":{"resume":{},"close":{}}}})
    elif method == "session/new":
        s = sid(); sessions[s]=[]; reply({"sessionId": s})
    elif method == "session/cancel":
        pass
    elif method == "session/prompt":
        s = str(params.get("sessionId"))
        notify("session/update", {"sessionId": s, "update": {
            "sessionUpdate":"tool_call","toolCallId":"t1","title":"hang",
            "kind":"other","status":"pending"}})
        # never complete tool, never reply — idle timeout must fire
    else:
        if req_id is not None:
            reply({})
"""
    return await asyncio.create_subprocess_exec(
        "python3",
        "-c",
        driver,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=16 * 1024 * 1024,
    )


async def _spawn_slow_tools_agent(_argv: list[str]) -> asyncio.subprocess.Process:
    """Agent with two tools that complete after short sleeps; total > idle."""
    driver = r"""
import sys, json, time
sessions = {}
n = 0
def sid():
    global n
    n += 1
    return f"sess-slow-{n}"
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    req_id = msg.get("id")
    params = msg.get("params") or {}
    def reply(result=None, error=None):
        if req_id is None:
            return
        out = {"jsonrpc": "2.0", "id": req_id}
        if error is not None:
            out["error"] = error
        else:
            out["result"] = result
        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()
    def notify(method, params):
        sys.stdout.write(json.dumps({"jsonrpc":"2.0","method":method,"params":params}) + "\n")
        sys.stdout.flush()
    if method == "initialize":
        reply({"protocolVersion":1,"agentCapabilities":{"loadSession":True,"sessionCapabilities":{"resume":{},"close":{}}}})
    elif method == "session/new":
        s = sid(); sessions[s]=[]; reply({"sessionId": s})
    elif method == "session/prompt":
        s = str(params.get("sessionId"))
        for i in range(2):
            tid = f"t{i}"
            notify("session/update", {"sessionId": s, "update": {
                "sessionUpdate":"tool_call","toolCallId":tid,
                "title":f"tool-{i}","kind":"other","status":"pending"}})
            time.sleep(0.45)
            notify("session/update", {"sessionId": s, "update": {
                "sessionUpdate":"tool_call_update","toolCallId":tid,
                "status":"completed"}})
        notify("session/update", {"sessionId": s, "update": {
            "sessionUpdate":"agent_message_chunk","messageId":"m1",
            "content":{"type":"text","text":"done after tools"}}})
        reply({"stopReason":"end_turn"})
    else:
        if req_id is not None:
            reply({})
"""
    return await asyncio.create_subprocess_exec(
        "python3",
        "-c",
        driver,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=16 * 1024 * 1024,
    )


@pytest.mark.asyncio
async def test_prompt_idle_timeout_when_tool_stalls() -> None:
    b = AcpBroker(
        process_factory=_spawn_stall_agent,
        prompt_idle_timeout_s=0.8,
        prompt_hard_max_s=30.0,
        rpc_timeout_s=5.0,
    )
    await b.start()
    try:
        sid = await b.session_new("/tmp")
        with pytest.raises(AcpError, match="RPC idle timeout: session/prompt"):
            await b.prompt_turn(sid, "hang please")
    finally:
        await b.stop()


@pytest.mark.asyncio
async def test_prompt_idle_resets_on_tool_complete() -> None:
    """Two ~0.45s tools with idle=0.7s: total > idle but completes reset it."""
    b = AcpBroker(
        process_factory=_spawn_slow_tools_agent,
        prompt_idle_timeout_s=0.7,
        prompt_hard_max_s=30.0,
        rpc_timeout_s=5.0,
    )
    await b.start()
    try:
        sid = await b.session_new("/tmp")
        result = await b.prompt_turn(sid, "slow tools")
        assert result.stop_reason == "end_turn"
        assert "done after tools" in result.answer_text
        assert result.tool_titles == ["tool-0", "tool-1"]
    finally:
        await b.stop()


def test_rpc_error_keeps_agent_data_detail() -> None:
    """Bare `Invalid params` hid the agent's actual explanation."""
    msg = _format_rpc_error(
        {
            "code": -32602,
            "message": "Invalid params",
            "data": "This session's conversation history is incompatible with "
            "the current model. Please start a new session.",
        }
    )
    assert msg.startswith("Invalid params: ")
    assert "incompatible with the current model" in msg


def test_rpc_error_without_data_is_unchanged() -> None:
    assert _format_rpc_error({"code": -1, "message": "Rate limited"}) == "Rate limited"
    assert _format_rpc_error("boom") == "boom"


@pytest.mark.asyncio
async def test_permission_policy_respects_explicit_denial():
    broker = AcpBroker(command="unused", always_approve=False)
    writes = []

    async def capture(message):
        writes.append(message)

    broker._write = capture
    request = {"options": [{"optionId": "allow", "kind": "allow_once"}]}
    await broker._handle_permission(1, request)
    assert writes[-1]["result"]["outcome"]["outcome"] == "cancelled"
    broker.always_approve = True
    await broker._handle_permission(2, request)
    assert writes[-1]["result"]["outcome"]["optionId"] == "allow"
