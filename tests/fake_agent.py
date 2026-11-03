"""Minimal ACP agent for unit tests (stdio-compatible via pipes)."""

from __future__ import annotations

import asyncio
from typing import Any


class FakeAcpAgent:
    """Responds to initialize / session/* enough for broker tests."""

    def __init__(self) -> None:
        self.sessions: dict[str, list[str]] = {}
        self._n = 0
        self.cancelled: list[str] = []

    def _sid(self) -> str:
        self._n += 1
        return f"sess-test-{self._n}"

    async def handle(self, msg: dict[str, Any]) -> list[dict[str, Any]]:
        """Return zero or more messages to write back (responses + notifications)."""
        if "method" not in msg:
            return []

        method = msg["method"]
        req_id = msg.get("id")
        params = msg.get("params") or {}
        out: list[dict[str, Any]] = []

        def result(payload: Any) -> None:
            if req_id is not None:
                out.append({"jsonrpc": "2.0", "id": req_id, "result": payload})

        if method == "initialize":
            result(
                {
                    "protocolVersion": 1,
                    "agentCapabilities": {
                        "loadSession": True,
                        "sessionCapabilities": {
                            "resume": {},
                            "close": {},
                        },
                    },
                }
            )
            return out

        if method == "authenticate":
            result({})
            return out

        if method == "session/new":
            sid = self._sid()
            self.sessions[sid] = []
            result({"sessionId": sid})
            return out

        if method == "session/resume":
            sid = params.get("sessionId")
            if sid not in self.sessions:
                self.sessions[str(sid)] = []
            result({})
            return out

        if method == "session/load":
            sid = str(params.get("sessionId"))
            # Replay one user+agent turn then result
            out.append(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": sid,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": "replayed"},
                        },
                    },
                }
            )
            result(None)
            return out

        if method == "session/close":
            sid = str(params.get("sessionId"))
            self.sessions.pop(sid, None)
            result({})
            return out

        if method == "session/cancel":
            self.cancelled.append(str(params.get("sessionId")))
            return out

        if method == "session/set_model":
            model_id = str(params.get("modelId") or "")
            # effort / reasoningEffort accepted (ignored by fake; real agent applies)
            _ = params.get("effort") or params.get("reasoningEffort")
            if model_id == "bogus-model":
                out.append(
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {
                            "code": -32602,
                            "message": "Invalid params",
                            "data": "unknown model id",
                        },
                    }
                )
            else:
                result({"_meta": {"model": {"Ok": model_id}}})
            return out

        if method == "session/prompt":
            sid = str(params.get("sessionId"))
            prompt = params.get("prompt") or []
            text = ""
            if prompt and isinstance(prompt[0], dict):
                text = str(prompt[0].get("text") or "")
            self.sessions.setdefault(sid, []).append(text)

            # tool + answer
            out.append(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": sid,
                        "update": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": "t1",
                            "title": "read_file vault",
                            "kind": "read",
                            "status": "pending",
                        },
                    },
                }
            )
            out.append(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": sid,
                        "update": {
                            "sessionUpdate": "tool_call_update",
                            "toolCallId": "t1",
                            "status": "completed",
                        },
                    },
                }
            )
            out.append(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": sid,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "messageId": "m1",
                            "content": {"type": "text", "text": "Hello "},
                        },
                    },
                }
            )
            out.append(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": sid,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "messageId": "m1",
                            "content": {"type": "text", "text": f"from fake agent.\n\nYou said: {text}"},
                        },
                    },
                }
            )
            out.append(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": sid,
                        "update": {
                            "sessionUpdate": "usage_update",
                            "used": 1200,
                            "size": 200000,
                        },
                    },
                }
            )
            result({"stopReason": "end_turn"})
            return out

        if req_id is not None:
            out.append(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Method not found: {method}"},
                }
            )
        return out


async def spawn_fake_agent_process(
    _argv: list[str],
) -> asyncio.subprocess.Process:
    """Return a Process-like pair of pipes driven by FakeAcpAgent.

    We use a real subprocess running a tiny Python driver so AcpBroker's
    StreamReader/Writer APIs work unchanged.
    """
    driver = r"""
import sys, json, asyncio
# Inline minimal agent loop
sessions = {}
n = 0

def sid():
    global n
    n += 1
    return f"sess-test-{n}"

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
    elif method == "authenticate":
        reply({})
    elif method == "session/new":
        s = sid(); sessions[s]=[]; reply({"sessionId": s})
    elif method == "session/resume":
        reply({})
    elif method == "session/load":
        s = str(params.get("sessionId"))
        notify("session/update", {"sessionId": s, "update": {"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"replayed"}}})
        reply(None)
    elif method == "session/close":
        sessions.pop(str(params.get("sessionId")), None); reply({})
    elif method == "session/cancel":
        pass
    elif method == "session/set_model":
        model_id = params.get("modelId") or ""
        # effort / reasoningEffort accepted (CLI parity; fake ignores payload)
        if model_id == "bogus-model":
            reply(error={"code": -32602, "message": "Invalid params", "data": "unknown model id"})
        else:
            reply({"_meta": {"model": {"Ok": model_id}}})
    elif method == "session/prompt":
        s = str(params.get("sessionId"))
        prompt = params.get("prompt") or []
        text = prompt[0].get("text","") if prompt else ""
        notify("session/update", {"sessionId": s, "update": {"sessionUpdate":"tool_call","toolCallId":"t1","title":"read_file vault","kind":"read","status":"pending"}})
        notify("session/update", {"sessionId": s, "update": {"sessionUpdate":"agent_message_chunk","messageId":"m1","content":{"type":"text","text":"Hello "}}})
        notify("session/update", {"sessionId": s, "update": {"sessionUpdate":"agent_message_chunk","messageId":"m1","content":{"type":"text","text":"from fake agent.\n\nYou said: "+text}}})
        notify("session/update", {"sessionId": s, "update": {"sessionUpdate":"usage_update","used":1200,"size":200000}})
        reply({"stopReason":"end_turn"})
    else:
        reply(error={"code":-32601,"message":f"unknown {method}"})
"""
    proc = await asyncio.create_subprocess_exec(
        "python3",
        "-c",
        driver,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=16 * 1024 * 1024,
    )
    return proc
