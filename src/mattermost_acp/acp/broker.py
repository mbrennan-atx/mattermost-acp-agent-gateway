"""JSON-RPC ACP client over a long-lived ``grok agent stdio`` process.

One broker multiplexes many ACP sessions (one chat thread each).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from mattermost_acp.text import join_text_chunks

from .subagents import SubagentTracker
from .types import AcpCapabilities, SessionUpdate, StopReason, TurnResult

logger = logging.getLogger(__name__)

PermissionHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

# Control-plane RPCs (initialize, session/new, set_model, …): hard wall.
# Override with ACP_RPC_TIMEOUT_SEC.
_DEFAULT_RPC_TIMEOUT_S = 120.0

# session/prompt: idle budget that resets on tool completion (and agent text).
# Stuck mid-tool dies after this many quiet seconds. Override with
# ACP_PROMPT_IDLE_TIMEOUT_SEC. Long CI/build waits can legitimately
# produce no ACP events for several minutes.
_DEFAULT_PROMPT_IDLE_TIMEOUT_S = 900.0

# Absolute ceiling for one prompt turn regardless of activity.
# Override with ACP_PROMPT_HARD_MAX_SEC.
_DEFAULT_PROMPT_HARD_MAX_S = 7200.0

_TOOL_DONE_STATUSES = frozenset(
    {"completed", "cancelled", "canceled", "success", "failed", "error"}
)


def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or os.environ.get("GROK_" + name, "")).strip()
    if not raw:
        return default
    try:
        val = float(raw)
    except ValueError:
        return default
    return val if val > 0 else default


def is_tool_complete_update(update: SessionUpdate) -> bool:
    """True when a session/update reports a tool call finishing."""
    if update.kind != "tool_call_update":
        return False
    status = (update.tool_status or "").strip().lower()
    return status in _TOOL_DONE_STATUSES


class AcpError(RuntimeError):
    def __init__(self, message: str, *, data: Any = None) -> None:
        super().__init__(message)
        self.data = data


def _format_rpc_error(err: Any) -> str:
    """Human-readable text for a JSON-RPC error object.

    The agent puts the useful part in ``data`` (e.g. ``Invalid params`` +
    "This session's conversation history is incompatible with the current
    model"), so keep it instead of surfacing the bare ``message``.
    """
    if not isinstance(err, dict):
        return str(err)
    message = str(err.get("message", err))
    data = err.get("data")
    if isinstance(data, str) and data.strip() and data.strip() != message:
        return f"{message}: {data.strip()}"
    return message


def _pick_allow_option(params: dict[str, Any]) -> str:
    """Choose an allow optionId from a permission request (prefer always)."""
    options = params.get("options") if isinstance(params.get("options"), list) else []
    preferred = ("allow_always", "allow-always", "allow_once", "allow-once")
    by_kind: dict[str, str] = {}
    by_id: dict[str, str] = {}
    for opt in options:
        if not isinstance(opt, dict):
            continue
        oid = str(opt.get("optionId") or "")
        kind = str(opt.get("kind") or "")
        if oid:
            by_id[oid] = oid
            if kind:
                by_kind[kind] = oid
    for key in preferred:
        if key in by_kind:
            return by_kind[key]
        if key in by_id:
            return by_id[key]
    # First option, or hard-coded fallback
    if options and isinstance(options[0], dict) and options[0].get("optionId"):
        return str(options[0]["optionId"])
    return "allow-once"


def _normalize_stop(reason: str | None) -> StopReason:
    if not reason:
        return "unknown"
    key = reason.strip().lower().replace("-", "_")
    # Headless sometimes uses EndTurn; ACP uses end_turn
    aliases = {
        "endturn": "end_turn",
        "end_turn": "end_turn",
        "cancelled": "cancelled",
        "canceled": "cancelled",
        "max_tokens": "max_tokens",
        "max_turn_requests": "max_turn_requests",
        "refusal": "refusal",
    }
    return aliases.get(key, "unknown")  # type: ignore[return-value]


def parse_session_update(params: dict[str, Any]) -> SessionUpdate:
    session_id = str(params.get("sessionId") or "")
    update = params.get("update") or {}
    if not isinstance(update, dict):
        update = {}
    kind = str(update.get("sessionUpdate") or "unknown")

    text: str | None = None
    content = update.get("content")
    if isinstance(content, dict) and content.get("type") == "text":
        text = content.get("text")
    elif isinstance(content, str):
        text = content

    usage_used = usage_size = None
    if kind == "usage_update":
        usage_used = update.get("used")
        usage_size = update.get("size")
        if usage_used is not None:
            usage_used = int(usage_used)
        if usage_size is not None:
            usage_size = int(usage_size)

    model_id = _model_id_from_payload(update) or _model_id_from_payload(params)

    return SessionUpdate(
        session_id=session_id,
        kind=kind,
        raw=update,
        text=text if isinstance(text, str) else None,
        message_id=update.get("messageId"),
        tool_call_id=update.get("toolCallId"),
        tool_title=update.get("title"),
        tool_kind=update.get("kind"),
        tool_status=update.get("status"),
        usage_used=usage_used,
        usage_size=usage_size,
        model_id=model_id,
    )


def _model_id_from_payload(payload: dict[str, Any]) -> str | None:
    meta = payload.get("_meta")
    if not isinstance(meta, dict):
        return None
    mid = meta.get("modelId") or meta.get("model_id")
    if isinstance(mid, str) and mid.strip():
        return mid.strip()
    return None


class AcpBroker:
    """Owns an agent subprocess and ACP JSON-RPC routing."""

    def __init__(
        self,
        command: str = "grok",
        *,
        backend: str = "grok",
        model: str | None = None,
        always_approve: bool = True,
        extra_args: list[str] | None = None,
        env: dict[str, str] | None = None,
        # Injectable process factory for tests (fake agent)
        process_factory: Callable[..., Awaitable[asyncio.subprocess.Process]] | None = None,
        rpc_timeout_s: float | None = None,
        prompt_idle_timeout_s: float | None = None,
        prompt_hard_max_s: float | None = None,
    ) -> None:
        self.command = command
        # "grok" spawns `grok agent ... stdio`, a CLI-specific subcommand
        # shape. "claude" and "codex" spawn an ACP-native adapter binary
        # directly on stdio, no subcommand/flags of its own — model and
        # approval are negotiated entirely over the ACP RPCs below.
        self.backend = backend
        self.model = model
        self.always_approve = always_approve
        self.extra_args = list(extra_args or [])
        self.env = env
        self._process_factory = process_factory
        self.rpc_timeout_s = (
            rpc_timeout_s
            if rpc_timeout_s is not None
            else _env_float("ACP_RPC_TIMEOUT_SEC", _DEFAULT_RPC_TIMEOUT_S)
        )
        self.prompt_idle_timeout_s = (
            prompt_idle_timeout_s
            if prompt_idle_timeout_s is not None
            else _env_float(
                "ACP_PROMPT_IDLE_TIMEOUT_SEC", _DEFAULT_PROMPT_IDLE_TIMEOUT_S
            )
        )
        self.prompt_hard_max_s = (
            prompt_hard_max_s
            if prompt_hard_max_s is not None
            else _env_float(
                "ACP_PROMPT_HARD_MAX_SEC", _DEFAULT_PROMPT_HARD_MAX_S
            )
        )

        self._proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._session_queues: dict[str, list[asyncio.Queue[SessionUpdate | None]]] = {}
        self._capabilities = AcpCapabilities()
        self.subagents = SubagentTracker()
        self.update_observer: Callable[[SessionUpdate], None] | None = None
        self._started = False
        self._lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        # session_id → in-flight turn lock
        self._turn_locks: dict[str, asyncio.Lock] = {}
        self._permission_handler: PermissionHandler | None = None
        self.session_config_options: dict[str, list[dict[str, Any]]] = {}
        self._cursor_live_sessions: set[str] = set()

    @property
    def capabilities(self) -> AcpCapabilities:
        return self._capabilities

    @property
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    def set_permission_handler(self, handler: PermissionHandler | None) -> None:
        self._permission_handler = handler

    def _turn_lock(self, session_id: str) -> asyncio.Lock:
        lock = self._turn_locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._turn_locks[session_id] = lock
        return lock

    def _build_argv(self) -> list[str]:
        if self.backend == "cursor":
            return [self.command, *self.extra_args, "acp"]
        if self.backend in {"claude", "codex"}:
            # Adapters speak ACP directly on stdio. Model/effort use session
            # config options; adapter environment controls the initial mode.
            return [self.command, *self.extra_args]
        args = [self.command, "agent"]
        if self.always_approve:
            args.append("--always-approve")
        args.append("--no-leader")
        if self.model:
            args.extend(["-m", self.model])
        args.extend(self.extra_args)
        args.append("stdio")
        return args

    async def start(self) -> AcpCapabilities:
        async with self._lock:
            if self.is_running and self._started:
                return self._capabilities

            self._cursor_live_sessions.clear()
            argv = self._build_argv()
            logger.info("Starting ACP agent: %s", " ".join(argv[:8]))

            if self._process_factory is not None:
                self._proc = await self._process_factory(argv)
            else:
                env = {**os.environ, **(self.env or {})}
                self._proc = await asyncio.create_subprocess_exec(
                    *argv,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                    limit=16 * 1024 * 1024,
                )

            assert self._proc.stdout is not None
            self._reader_task = asyncio.create_task(
                self._read_loop(), name="acp-broker-reader"
            )
            # Drain stderr in background so the pipe cannot fill
            if self._proc.stderr is not None:
                asyncio.create_task(self._drain_stderr(self._proc.stderr), name="acp-stderr")

            # Do NOT advertise fs/terminal client capabilities. If we claim them,
            # Grok issues fs/* and terminal/* RPCs to us and blocks the turn until
            # we answer — this gateway is not an IDE, so those hang forever after the
            # first tool_call UI event. Empty caps force agent-side tools.
            result = await self.request(
                "initialize",
                {
                    "protocolVersion": 1,
                    "clientCapabilities": {"subagents": {}},
                    "clientInfo": {
                        "name": "mattermost-acp-gateway",
                        "title": "Mattermost ACP gateway",
                        "version": "0.1.0",
                    },
                },
            )
            caps_raw = result if isinstance(result, dict) else {}
            self._capabilities = self._parse_capabilities(caps_raw)
            if self.backend == "cursor":
                await self.request("authenticate", {"methodId": "cursor_login"})
            self._started = True
            logger.info(
                "ACP initialized loadSession=%s resume=%s close=%s",
                self._capabilities.load_session,
                self._capabilities.resume,
                self._capabilities.close,
            )
            return self._capabilities

    async def stop(self) -> None:
        async with self._lock:
            self._started = False
            self._cursor_live_sessions.clear()
            self.subagents = SubagentTracker()
            proc = self._proc
            self._proc = None
            if self._reader_task is not None:
                self._reader_task.cancel()
                try:
                    await self._reader_task
                except asyncio.CancelledError:
                    pass
                self._reader_task = None
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(AcpError("broker stopped"))
            self._pending.clear()
            for queues in self._session_queues.values():
                for q in queues:
                    await q.put(None)
            self._session_queues.clear()
            if proc is not None and proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        if self._proc is None or self._proc.stdin is None:
            raise AcpError("agent process is not running")

        req_id = self._next_id
        self._next_id += 1
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        self._pending[req_id] = fut

        msg = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or {}}
        await self._write(msg)

        # None → control-plane default. session/prompt passes a hard max; idle
        # kill for prompts is owned by prompt_turn (resets on tool completion).
        wait_s = self.rpc_timeout_s if timeout is None else timeout
        try:
            return await asyncio.wait_for(fut, timeout=wait_s)
        except TimeoutError as exc:
            self._pending.pop(req_id, None)
            raise AcpError(f"RPC timeout: {method}") from exc
        except asyncio.CancelledError:
            self._pending.pop(req_id, None)
            raise

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        msg = {"jsonrpc": "2.0", "method": method, "params": params or {}}
        await self._write(msg)

    async def steer(self, session_id: str, text: str) -> str:
        """Inject into a live turn without taking its lock or starting a turn.

        The idle opt-in keeps a turn-completion race from launching an
        untracked detached prompt. Codex needs the compatibility patch in
        scripts/patch-codex-steering.py; Claude implements this opt-in natively.
        """
        if not text.strip():
            raise AcpError("Usage: /hint <guidance for the active work>")
        if not self.capabilities.steering:
            raise AcpError(f"Live hints are not supported by the {self.backend} adapter.")
        meta = self.capabilities.raw.get("_meta") or {}
        steering = meta.get("steering") or {}
        if self.backend == "codex" and steering.get("idleBehavior") != "promptRequired":
            raise AcpError("The Codex adapter needs the active-only steering patch and a reload.")
        result = await self.request(
            "_session/steering",
            {
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": text.strip()}],
                "_meta": {"steering": {"idleBehavior": "promptRequired"}},
            },
        )
        outcome = result.get("outcome") if isinstance(result, dict) else None
        if outcome not in {"injected", "promptRequired"}:
            raise AcpError(f"Hint was not confirmed by the adapter (outcome: {outcome!r}).")
        return outcome

    async def session_new(self, cwd: str | Path, *, meta: dict[str, Any] | None = None) -> str:
        params: dict[str, Any] = {
            "cwd": str(cwd),
            "mcpServers": [],
        }
        if meta:
            params["_meta"] = meta
        result = await self.request("session/new", params)
        if not isinstance(result, dict) or "sessionId" not in result:
            raise AcpError("session/new missing sessionId", data=result)
        self._remember_config(str(result["sessionId"]), result)
        if self.backend == "cursor":
            self._cursor_live_sessions.add(str(result["sessionId"]))
        return str(result["sessionId"])

    def _remember_config(self, session_id: str, result: Any) -> None:
        if isinstance(result, dict) and isinstance(result.get("configOptions"), list):
            self.session_config_options[session_id] = result["configOptions"]

    async def session_resume(self, session_id: str, cwd: str | Path) -> None:
        result = await self.request(
            "session/resume",
            {"sessionId": session_id, "cwd": str(cwd), "mcpServers": []},
        )
        self._remember_config(session_id, result)

    async def session_load(self, session_id: str, cwd: str | Path) -> None:
        """Load session; discard replayed history updates for Discord."""
        # Cursor cannot load a new, unpersisted chat already live in this process.
        if self.backend == "cursor" and session_id in self._cursor_live_sessions:
            return
        # Subscribe a throwaway queue so replay does not leak into other waiters
        q: asyncio.Queue[SessionUpdate | None] = asyncio.Queue()
        self._session_queues.setdefault(session_id, []).append(q)
        try:
            result = await self.request(
                "session/load",
                {"sessionId": session_id, "cwd": str(cwd), "mcpServers": []},
            )
            self._remember_config(session_id, result)
            if self.backend == "cursor":
                self._cursor_live_sessions.add(session_id)
        finally:
            queues = self._session_queues.get(session_id, [])
            if q in queues:
                queues.remove(q)
            # Drain residual replay
            while not q.empty():
                q.get_nowait()

    async def set_model(
        self,
        session_id: str,
        model_id: str,
        *,
        effort: str | None = None,
    ) -> str:
        """Switch the live session's model via ACP ``session/set_model``.

        Optional ``effort`` is passed as ``reasoningEffort`` (and ``effort``)
        so reasoning models pick up the CLI-equivalent level without a chat
        passthrough. Returns the resolved model id on success.
        """
        if self.backend == "cursor":
            from mattermost_acp.commands.cursor_models import with_effort
            model_id = with_effort(model_id, effort)
            result = await self.request("session/set_config_option", {
                "sessionId": session_id, "configId": "model", "value": model_id,
            })
            self._remember_config(session_id, result)
            for option in self.session_config_options.get(session_id, []):
                if option.get("id") == "model":
                    return str(option.get("currentValue") or model_id)
            return model_id
        if self.backend in {"claude", "codex"}:
            return await self._set_model_config(session_id, model_id, effort=effort)
        params: dict[str, Any] = {"sessionId": session_id, "modelId": model_id}
        if effort:
            # Live agent accepts both names; send both for forward-compat.
            params["reasoningEffort"] = effort
            params["effort"] = effort
        result = await self.request("session/set_model", params)
        # Observed shape: {"_meta": {"model": {"Ok": "<resolved-id>"}}} or
        # {"_meta": {"model": {"Err": "..."}}}. Tolerate a bare/empty result
        # too (spec says response is otherwise empty on success).
        meta = result.get("_meta") if isinstance(result, dict) else None
        model_field = meta.get("model") if isinstance(meta, dict) else None
        if isinstance(model_field, dict):
            if "Err" in model_field:
                raise AcpError(str(model_field["Err"]))
            ok = model_field.get("Ok")
            if isinstance(ok, str) and ok:
                return ok
        return model_id

    async def _set_model_config(
        self,
        session_id: str,
        model_id: str,
        *,
        effort: str | None = None,
    ) -> str:
        """Claude and Codex expose model and effort through configOptions.

        Codex names its effort option ``reasoning_effort``; Claude uses
        ``effort``. The adapter resolves model values against its own catalog.
        """
        result = await self.request(
            "session/set_config_option",
            {"sessionId": session_id, "configId": "model", "value": model_id},
        )
        self._remember_config(session_id, result)
        resolved = model_id
        options = result.get("configOptions") if isinstance(result, dict) else None
        if isinstance(options, list):
            for opt in options:
                if isinstance(opt, dict) and opt.get("id") == "model":
                    current = opt.get("currentValue")
                    if isinstance(current, str) and current:
                        resolved = current
                    break
        if effort:
            result = await self.request(
                "session/set_config_option",
                {
                    "sessionId": session_id,
                    "configId": "reasoning_effort" if self.backend == "codex" else "effort",
                    "value": effort,
                },
            )
            self._remember_config(session_id, result)
        return resolved

    async def session_close(self, session_id: str) -> None:
        self.subagents.forget(session_id)
        if not self._capabilities.close:
            return
        try:
            await self.request("session/close", {"sessionId": session_id})
        except AcpError as exc:
            logger.warning("session/close failed for %s: %s", session_id, exc)

    async def cancel(self, session_id: str) -> None:
        await self.notify("session/cancel", {"sessionId": session_id})

    async def prompt_turn(
        self,
        session_id: str,
        text: str,
        *,
        on_update: Callable[[SessionUpdate], Awaitable[None]] | None = None,
    ) -> TurnResult:
        """Run one prompt turn; buffer answer text; invoke on_update for status.

        ``session/prompt`` uses an **idle** timeout (default 15m), not a hard
        wall for the whole turn. The idle clock resets whenever a tool call
        completes (and when the agent streams message/thought chunks so a
        long final answer does not get killed). Synthetic heartbeats do not
        reset it. A separate hard max (default 2h) still bounds pathological
        turns.
        """
        async with self._turn_lock(session_id):
            q: asyncio.Queue[SessionUpdate | None] = asyncio.Queue()
            self._session_queues.setdefault(session_id, []).append(q)
            for snapshot in self.subagents.snapshots(session_id):
                q.put_nowait(snapshot)

            answer_parts: list[str] = []
            thought_parts: list[str] = []
            tool_titles: list[str] = []
            usage_used: int | None = None
            usage_size: int | None = None
            model_id: str | None = None

            loop = asyncio.get_running_loop()
            turn_started = loop.time()
            # Idle budget starts at turn open; tool completion resets it.
            last_progress = turn_started
            idle_s = self.prompt_idle_timeout_s
            hard_max_s = self.prompt_hard_max_s

            prompt_task = asyncio.create_task(
                self.request(
                    "session/prompt",
                    {
                        "sessionId": session_id,
                        "prompt": [{"type": "text", "text": text}],
                    },
                    timeout=hard_max_s,
                )
            )

            last_heartbeat = turn_started
            result: Any = None
            try:
                while True:
                    if prompt_task.done() and q.empty():
                        break
                    try:
                        item = await asyncio.wait_for(q.get(), timeout=1.0)
                    except TimeoutError:
                        if prompt_task.done() and q.empty():
                            break
                        now = loop.time()
                        # Idle kill: no tool completion / agent output for idle_s.
                        if now - last_progress >= idle_s:
                            logger.warning(
                                "session/prompt idle timeout session=%s idle=%.0fs",
                                session_id,
                                idle_s,
                            )
                            try:
                                await self.cancel(session_id)
                            except Exception:
                                logger.debug(
                                    "session/cancel after idle timeout failed",
                                    exc_info=True,
                                )
                            if not prompt_task.done():
                                prompt_task.cancel()
                                try:
                                    await prompt_task
                                except (asyncio.CancelledError, AcpError):
                                    pass
                            raise AcpError(
                                f"RPC idle timeout: session/prompt "
                                f"(no tool completion or agent output for "
                                f"{idle_s:.0f}s)"
                            ) from None
                        # Periodic heartbeat so chat status doesn't look frozen
                        # during long tool/model phases with no session/update traffic.
                        # Heartbeats intentionally do NOT reset last_progress.
                        if on_update is not None and now - last_heartbeat >= 15.0:
                            last_heartbeat = now
                            await on_update(
                                SessionUpdate(
                                    session_id=session_id,
                                    kind="heartbeat",
                                    raw={"sessionUpdate": "heartbeat"},
                                )
                            )
                        # Surface prompt failures early
                        if prompt_task.done() and prompt_task.exception():
                            break
                        continue
                    if item is None:
                        break

                    if item.kind == "agent_message_chunk" and item.text:
                        answer_parts.append(item.text)
                        last_progress = loop.time()
                    elif item.kind == "agent_thought_chunk" and item.text:
                        thought_parts.append(item.text)
                        last_progress = loop.time()
                    elif item.kind == "tool_call":
                        if item.tool_title:
                            tool_titles.append(item.tool_title)
                    elif item.kind == "tool_call_update":
                        if is_tool_complete_update(item):
                            last_progress = loop.time()
                    elif item.kind == "subagent_activity":
                        # Real worker output is progress; synthetic heartbeats are not.
                        last_progress = loop.time()
                    elif item.kind == "usage_update":
                        usage_used = item.usage_used
                        usage_size = item.usage_size

                    if item.model_id:
                        model_id = item.model_id

                    if on_update is not None:
                        await on_update(item)
                        last_heartbeat = loop.time()

                result = await prompt_task
            finally:
                queues = self._session_queues.get(session_id, [])
                if q in queues:
                    queues.remove(q)
                if not prompt_task.done():
                    prompt_task.cancel()
                    try:
                        await prompt_task
                    except (asyncio.CancelledError, AcpError):
                        pass

            stop: StopReason = "end_turn"
            if isinstance(result, dict):
                stop = _normalize_stop(result.get("stopReason"))

            # Stream boundaries carry no formatting semantics.
            return TurnResult(
                session_id=session_id,
                stop_reason=stop,
                answer_text=join_text_chunks(answer_parts),
                thought_text="".join(thought_parts),
                tool_titles=tool_titles,
                usage_used=usage_used,
                usage_size=usage_size,
                model_id=model_id,
            )

    async def _write(self, msg: dict[str, Any]) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise AcpError("agent process is not running")
        line = json.dumps(msg, separators=(",", ":")) + "\n"
        async with self._write_lock:
            self._proc.stdin.write(line.encode("utf-8"))
            await self._proc.stdin.drain()

    async def _read_loop(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        try:
            while True:
                line = await self._proc.stdout.readline()
                if not line:
                    break
                decoded = line.decode("utf-8", errors="replace").strip()
                if not decoded:
                    continue
                try:
                    msg = json.loads(decoded)
                except json.JSONDecodeError:
                    logger.warning("ACP non-JSON line: %s", decoded[:200])
                    continue
                await self._dispatch(msg)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ACP reader crashed")
        finally:
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_exception(AcpError("agent stdout closed"))
            self._pending.clear()

    async def _dispatch(self, msg: dict[str, Any]) -> None:
        # Response to one of our requests
        if "id" in msg and ("result" in msg or "error" in msg) and "method" not in msg:
            req_id = msg["id"]
            fut = self._pending.pop(req_id, None)
            if fut is None or fut.done():
                return
            if "error" in msg:
                err = msg["error"]
                message = _format_rpc_error(err)
                fut.set_exception(AcpError(message, data=err))
            else:
                fut.set_result(msg.get("result"))
            return

        method = msg.get("method")
        params = msg.get("params") if isinstance(msg.get("params"), dict) else {}
        req_id = msg.get("id")

        # Server → client request (must always get a response or the agent blocks)
        if method and req_id is not None and "result" not in msg and "error" not in msg:
            if method == "session/request_permission":
                await self._handle_permission(req_id, params)
                return
            if self.backend == "cursor" and method in {"cursor/ask_question", "cursor/create_plan"}:
                # Mattermost answers arrive on a later turn. Never invent a user
                # selection or silently approve a plan requiring explicit approval.
                outcome = "skipped" if method == "cursor/ask_question" else "rejected"
                await self._write({"jsonrpc": "2.0", "id": req_id, "result": {
                    "outcome": {"outcome": outcome, "reason":
                        "Present this question or plan in your chat response "
                        "and wait for the user's next message."}
                }})
                return
            logger.warning(
                "Rejecting unsupported ACP server request method=%s id=%s",
                method,
                req_id,
            )
            await self._write(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not supported by mattermost-acp-gateway: {method}",
                    },
                }
            )
            return

        # Notifications
        if method in {"session/update", "_x.ai/session/update", "x.ai/session/update"}:
            update = parse_session_update(params)
            # Also accept nested sessionUpdate on x.ai envelopes
            for q in list(self._session_queues.get(update.session_id, [])):
                await q.put(update)
            # Native adapters send child streams under their own session IDs.
            # Route a separate status event to ancestors, never child text into
            # the parent's answer/thought/usage buffers.
            activities = self.subagents.observe(update)
            for activity in activities:
                for q in list(self._session_queues.get(activity.session_id, [])):
                    await q.put(activity)
            if self.update_observer is not None:
                for event in [update, *activities]:
                    try:
                        self.update_observer(event)
                    except Exception:
                        logger.exception("background update observer failed")
            return

        if method:
            logger.debug("Unhandled ACP notification method=%s", method)

    async def _handle_permission(self, req_id: int | str, params: dict[str, Any]) -> None:
        if self._permission_handler is not None:
            result = await self._permission_handler(params)
        elif not self.always_approve:
            result = {"outcome": {"outcome": "cancelled"}}
        else:
            option_id = _pick_allow_option(params)
            result = {
                "outcome": {
                    "outcome": "selected",
                    "optionId": option_id,
                }
            }
        await self._write({"jsonrpc": "2.0", "id": req_id, "result": result})

    async def _drain_stderr(self, stream: asyncio.StreamReader) -> None:
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    logger.debug("grok agent stderr: %s", text)
        except Exception:
            logger.debug("stderr drain ended", exc_info=True)

    @staticmethod
    def _parse_capabilities(result: dict[str, Any]) -> AcpCapabilities:
        agent = result.get("agentCapabilities") or result.get("capabilities") or {}
        if not isinstance(agent, dict):
            agent = {}
        session_caps = agent.get("sessionCapabilities") or {}
        if not isinstance(session_caps, dict):
            session_caps = {}
        return AcpCapabilities(
            load_session=bool(agent.get("loadSession")),
            resume="resume" in session_caps or bool(session_caps.get("resume")),
            close="close" in session_caps or bool(session_caps.get("close")),
            steering=(result.get("_meta") or {}).get("steering", {}).get("supported") is True,
            raw=result,
        )
