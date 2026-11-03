"""Thread-scoped Grok sessions on top of AcpBroker + ThreadStore."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tomllib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from mattermost_acp.acp import AcpBroker, AcpError, SessionUpdate, TurnResult
from mattermost_acp.channel_context import session_meta_rules
from mattermost_acp.commands.models_catalog import fetch_catalog
from mattermost_acp.model_router import ModelRouter, RouteRequest
from mattermost_acp.store import ThreadStore

logger = logging.getLogger(__name__)

UpdateCallback = Callable[[SessionUpdate], Awaitable[None]]

_FALLBACK_DEFAULT_MODEL = "grok-4.6"

COMPACT_PROMPT = "/compact"

CompactionOutcome = Literal["not_needed", "compacted", "failed"]


@dataclass(frozen=True)
class ModelSwitch:
    """Result of ``SessionService.switch_model``."""

    model_id: str
    compaction: CompactionOutcome = "not_needed"
    active_model: str | None = None


def model_family(model_id: str | None) -> str | None:
    """Provider family for a model id, ignoring the ``hub-`` gateway prefix.

    ``hub-claude-opus-5`` and ``claude-opus-5`` (the id the agent resolves to,
    and what sqlite stores) both map to ``claude``.
    """
    mid = (model_id or "").strip().lower()
    if not mid:
        return None
    if mid.startswith("hub-"):
        mid = mid[4:]
    head = mid.split("-", 1)[0]
    return head or None


def needs_history_compaction(previous: str | None, target: str) -> bool:
    """True when switching this session to Grok needs a compaction first.

    Only non-Grok → Grok breaks: the xAI API rejects replayed history that
    carries another provider's signed reasoning items. Grok → Anthropic is
    fine, and so is any same-family switch.
    """
    prev_family = model_family(previous)
    if prev_family is None:
        return False
    return model_family(target) == "grok" and prev_family != "grok"


def grok_home_dir(override: Path | None = None) -> Path:
    if override is not None:
        return override
    raw = os.environ.get("GROK_HOME", "").strip()
    return Path(raw).expanduser() if raw else Path.home() / ".grok"


def discover_session_model(
    session_id: str,
    cwd: Path,
    *,
    grok_home: Path | None = None,
    field: str = "current_model_id",
) -> str | None:
    """Read ``current_model_id`` from the on-disk Grok session summary.

    Discord's sqlite store only learns a model after ``/model``. The live ACP
    session can be on something else (process default, prior switch the store
    never recorded). ``summary.json`` is the agent's own record.
    """
    home = grok_home_dir(grok_home)
    seen: set[str] = set()
    candidates: list[Path] = [cwd]
    try:
        resolved = cwd.expanduser().resolve()
        candidates.append(resolved)
    except OSError:
        pass
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        path = home / "sessions" / quote(key, safe="") / session_id / "summary.json"
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.debug("could not read session summary %s", path, exc_info=True)
            continue
        mid = data.get(field)
        if isinstance(mid, str) and mid.strip():
            return mid.strip()
    return None


class SessionService:
    def __init__(
        self,
        broker: AcpBroker,
        store: ThreadStore,
        *,
        default_cwd: Path,
        default_model: str | None = None,
        grok_command: str | None = None,
        grok_home: Path | None = None,
        max_parallel_turns: int = 4,
        inject_channel_context: bool = True,
        busy_marker: Path | None = None,
        router: ModelRouter | None = None,
    ) -> None:
        self.broker = broker
        self.router = router
        self.busy_marker = busy_marker
        self.store = store
        self.default_cwd = default_cwd
        self.default_model = default_model
        self.grok_command = grok_command
        self.grok_home = grok_home
        self.inject_channel_context = inject_channel_context
        self._global_sem = asyncio.Semaphore(max_parallel_turns)
        self._busy: set[str] = set()
        # Threads whose live ACP model has been synced this agent process.
        self._pinned: set[str] = set()

    def is_busy(self, thread_id: str) -> bool:
        return thread_id in self._busy

    @property
    def busy_threads(self) -> frozenset[str]:
        return frozenset(self._busy)

    def _write_busy_marker(self) -> None:
        """Expose in-flight turns so ``scripts/set-harness`` can restart when idle."""
        if self.busy_marker is None:
            return
        try:
            if self._busy:
                tmp = self.busy_marker.with_name(f".{self.busy_marker.name}.tmp")
                tmp.write_text(
                    json.dumps({"pid": os.getpid(), "threads": sorted(self._busy)}),
                    encoding="utf-8",
                )
                tmp.replace(self.busy_marker)
            else:
                self.busy_marker.unlink(missing_ok=True)
        except OSError:
            logger.warning("could not update busy marker %s", self.busy_marker, exc_info=True)

    def _new_session_meta(self) -> dict | None:
        if not self.inject_channel_context:
            return None
        return session_meta_rules()

    async def ensure_session(
        self,
        thread_id: str,
        *,
        cwd: Path | None = None,
        channel_id: str | None = None,
        route: RouteRequest | None = None,
    ) -> str:
        await self.broker.start()
        record = await self.store.get(thread_id)
        work_cwd = Path(record.cwd) if record else (cwd or self.default_cwd)
        work_cwd = work_cwd.expanduser().resolve()

        if record is not None and record.harness not in {None, self.store.harness}:
            logger.info(
                "Thread %s session %s belongs to harness %s; starting a %s session",
                thread_id,
                record.session_id,
                record.harness,
                self.store.harness,
            )
            self._pinned.discard(thread_id)
            await self.store.reset_for_harness(thread_id)
            record = None

        if record is None:
            model, effort, source = await self.store.model_preference(
                thread_id, channel_id
            )
            if source == "configuration" and route is not None:
                model, effort = await self._route(thread_id, route, model, effort)
            return await self._create_and_pin(
                thread_id,
                work_cwd,
                model=model,
                effort=effort,
                channel_id=channel_id,
            )

        # Resume/load can replace live model settings with persisted defaults.
        # Invalidate the pin so the requested model and effort are reapplied.
        # Prefer resume; fall back to load; last resort new session
        caps = self.broker.capabilities
        try:
            if caps.resume:
                await self.broker.session_resume(record.session_id, work_cwd)
                self._pinned.discard(thread_id)
            elif caps.load_session:
                await self.broker.session_load(record.session_id, work_cwd)
                self._pinned.discard(thread_id)
            else:
                # Agent still has in-memory session if process never died
                pass
        except AcpError as exc:
            logger.warning(
                "Resume/load failed for thread %s session %s: %s — creating new",
                thread_id,
                record.session_id,
                exc,
            )
            self._pinned.discard(thread_id)
            desired_model, desired_effort, source = await self.store.model_preference(
                thread_id, channel_id
            )
            if source == "configuration" and record.model:
                # Keep the model this thread already ran on (routed or default).
                desired_model, desired_effort = record.model, record.effort
            return await self._create_and_pin(
                thread_id,
                work_cwd,
                model=desired_model,
                effort=desired_effort,
                channel_id=channel_id or record.channel_id,
            )

        if channel_id is not None and record.channel_id != channel_id:
            await self.store.save(
                thread_id, record.session_id, str(work_cwd), channel_id=channel_id
            )
            record = await self.store.get(thread_id)
            assert record is not None
        desired_model, desired_effort, _ = await self.store.model_preference(
            thread_id, channel_id
        )
        await self._sync_live_model(
            thread_id,
            record.session_id,
            work_cwd,
            stored_model=desired_model,
            stored_effort=desired_effort,
            current_model=record.model,
            current_effort=record.effort,
        )
        return record.session_id

    async def run_turn(
        self,
        thread_id: str,
        prompt: str,
        *,
        channel_id: str | None = None,
        on_update: UpdateCallback | None = None,
        route: RouteRequest | None = None,
    ) -> TurnResult:
        if thread_id in self._busy:
            raise RuntimeError("This thread already has a turn in progress. Use /stop.")

        async with self._global_sem:
            self._busy.add(thread_id)
            self._write_busy_marker()
            try:
                session_id = await self.ensure_session(
                    thread_id, channel_id=channel_id, route=route
                )
                result = await self.broker.prompt_turn(
                    session_id, prompt, on_update=on_update
                )
                if result.model_id:
                    record = await self.store.get(thread_id)
                    cwd = record.cwd if record else str(self.default_cwd)
                    if record is None or record.model != result.model_id:
                        await self.store.save(
                            thread_id, session_id, cwd, model=result.model_id
                        )
                return result
            finally:
                self._busy.discard(thread_id)
                self._write_busy_marker()

    async def hint(self, thread_id: str, text: str) -> str:
        """Use the already loaded session; never resume or reconfigure active work."""
        record = await self.store.get(thread_id)
        if record is None or not self.broker.is_running:
            raise AcpError("No active agent session here. Send a normal message to start work.")
        return await self.broker.steer(record.session_id, text)

    async def switch_model(
        self,
        thread_id: str,
        model_id: str,
        *,
        effort: str | None = None,
        scope: str = "thread",
        channel_id: str | None = None,
    ) -> ModelSwitch:
        """Switch this thread's live ACP session to ``model_id``.

        Optional ``effort`` is applied in the same ``session/set_model`` RPC
        (CLI-equivalent of ``/model <id> <effort>``). Persists model (and
        effort when set) on the thread store.

        Switching a session that already holds non-Grok history to a Grok
        model compacts the history first — see ``_compact_history``.
        """
        if scope not in {"thread", "channel", "harness"}:
            raise ValueError(f"unknown model scope: {scope}")
        if scope == "channel" and channel_id is None:
            raise ValueError("channel scope requires a channel id")

        session_id = await self.ensure_session(thread_id, channel_id=channel_id)
        record = await self.store.get(thread_id)
        active_model = record.model if record else None
        model_to_apply = model_id
        effort_to_apply = effort
        if scope != "thread" and record and record.model_override:
            model_to_apply = record.model_override
            effort_to_apply = record.effort_override

        compaction: CompactionOutcome = "not_needed"
        if model_to_apply != active_model:
            if needs_history_compaction(active_model, model_to_apply):
                compaction = await self._compact_history(session_id)
            active_model = await self.broker.set_model(
                session_id, model_to_apply, effort=effort_to_apply
            )

        record = await self.store.get(thread_id)
        cwd = record.cwd if record else str(self.default_cwd)
        await self.store.save(
            thread_id,
            session_id,
            cwd,
            model=active_model,
            effort=effort_to_apply,
            channel_id=channel_id,
        )
        if scope == "thread":
            resolved_channel = channel_id or (record.channel_id if record else None)
            await self.store.set_thread_override(
                thread_id, resolved_channel, model_id, effort
            )
        elif scope == "channel":
            assert channel_id is not None
            await self.store.set_model_setting(
                f"channel:{channel_id}", model_id, effort
            )
        else:
            await self.store.set_model_setting("harness", model_id, effort)
        self._pinned.add(thread_id)
        return ModelSwitch(
            model_id=model_id,
            compaction=compaction,
            active_model=active_model,
        )

    async def _compact_history(self, session_id: str) -> CompactionOutcome:
        """Compact the session before a cross-provider switch to a Grok model.

        The agent replays the whole conversation to the new provider on the
        next prompt. xAI rejects Anthropic-signed ``reasoning`` items with
        ``-32602 Invalid params`` ("conversation history is incompatible with
        the current model"), so the switch looks fine and the *next* turn
        dies. Compaction rewrites the history without those items.

        Editing the on-disk ``chat_history.jsonl`` is not an option: a running
        agent serves its in-memory copy, ``session/resume`` does not reload,
        and ``session/close`` on a live session does not answer.
        """
        try:
            await self.broker.prompt_turn(session_id, COMPACT_PROMPT)
        except AcpError as exc:
            logger.warning(
                "pre-switch compaction failed session=%s: %s", session_id, exc
            )
            return "failed"
        return "compacted"

    async def set_effort(self, thread_id: str, effort: str) -> tuple[str, str]:
        """Set reasoning effort on the current model for this thread.

        Re-issues ``session/set_model`` with the stored (or default) model id
        plus ``reasoningEffort`` — same wire path as TUI ``/effort``.
        Returns ``(model_id, effort)``.
        """
        session_id = await self.ensure_session(thread_id)
        record = await self.store.get(thread_id)
        model_id = (
            (record.model if record else None)
            or self.default_model
            or "grok-4.5"
        )
        resolved = await self.broker.set_model(
            session_id, model_id, effort=effort
        )
        cwd = record.cwd if record else str(self.default_cwd)
        await self.store.save(
            thread_id, session_id, cwd, model=resolved, effort=effort
        )
        self._pinned.add(thread_id)
        return resolved, effort

    async def cancel(self, thread_id: str) -> bool:
        record = await self.store.get(thread_id)
        if record is None:
            return False
        await self.broker.cancel(record.session_id)
        return True

    async def new_session(
        self,
        thread_id: str,
        *,
        cwd: Path | None = None,
        channel_id: str | None = None,
    ) -> str:
        """Close current ACP session (if any) and start fresh."""
        await self.broker.start()
        record = await self.store.get(thread_id)
        work_cwd = Path(record.cwd) if record else (cwd or self.default_cwd)
        work_cwd = work_cwd.expanduser().resolve()

        if record is not None:
            try:
                await self.broker.cancel(record.session_id)
            except AcpError:
                pass
            await self.broker.session_close(record.session_id)

        desired_model, desired_effort, _ = await self.store.model_preference(
            thread_id, channel_id
        )
        self._pinned.discard(thread_id)
        return await self._create_and_pin(
            thread_id,
            work_cwd,
            model=desired_model,
            effort=desired_effort,
            channel_id=channel_id or (record.channel_id if record else None),
        )

    async def status(self, thread_id: str) -> dict[str, object]:
        record = await self.store.get(thread_id)
        model = (record.model if record else None) or self.default_model
        if record is not None and not record.model:
            live = discover_session_model(
                record.session_id, Path(record.cwd), grok_home=self.grok_home
            )
            if live:
                model = live
        effort = record.effort if record else None
        backend = getattr(self.broker, "backend", "grok")
        if record and backend in {"claude", "codex"}:
            for option in self.broker.session_config_options.get(record.session_id, []):
                if option.get("id") in {"effort", "reasoning_effort"}:
                    value = option.get("currentValue")
                    if isinstance(value, str) and value:
                        effort = value
                        break
        if backend == "cursor":
            from mattermost_acp.commands.cursor_models import model_effort
            effort = model_effort(model) or effort
        if backend == "grok":
            if record:
                effort = discover_session_model(
                    record.session_id, Path(record.cwd), grok_home=self.grok_home,
                    field="reasoning_effort",
                ) or effort
            if not effort:
                try:
                    config = tomllib.loads(
                        (grok_home_dir(self.grok_home) / "config.toml").read_text()
                    )
                    effort = config.get("model", {}).get(model, {}).get("reasoning_effort")
                    effort = effort or config.get("models", {}).get("default_reasoning_effort")
                except (OSError, ValueError):
                    pass
        return {
            "thread_id": thread_id,
            "session_id": record.session_id if record else None,
            "cwd": record.cwd if record else str(self.default_cwd),
            "model": model,
            "effort": effort,
            "subagents": [u.raw for u in self.broker.subagents.snapshots(record.session_id)]
            if record and hasattr(self.broker, "subagents") else [],
            "busy": thread_id in self._busy,
            "agent_running": self.broker.is_running,
        }

    async def _create_and_pin(
        self,
        thread_id: str,
        work_cwd: Path,
        *,
        model: str | None,
        effort: str | None,
        channel_id: str | None,
    ) -> str:
        session_id = await self.broker.session_new(
            work_cwd, meta=self._new_session_meta()
        )
        pinned = await self._pin_or_discover(
            session_id,
            work_cwd,
            stored_model=model,
            stored_effort=effort,
            is_new=True,
        )
        await self.store.save(
            thread_id,
            session_id,
            str(work_cwd),
            model=pinned,
            effort=effort,
            channel_id=channel_id,
        )
        self._pinned.add(thread_id)
        return session_id

    async def _sync_live_model(
        self,
        thread_id: str,
        session_id: str,
        work_cwd: Path,
        *,
        stored_model: str | None,
        stored_effort: str | None,
        current_model: str | None,
        current_effort: str | None,
    ) -> None:
        if (
            thread_id in self._pinned
            and stored_model == current_model
            and stored_effort == current_effort
        ):
            return
        pinned = await self._pin_or_discover(
            session_id,
            work_cwd,
            stored_model=stored_model,
            stored_effort=stored_effort,
            is_new=False,
        )
        if pinned and pinned != stored_model:
            await self.store.save(
                thread_id, session_id, str(work_cwd), model=pinned
            )
        self._pinned.add(thread_id)

    async def _pin_or_discover(
        self,
        session_id: str,
        work_cwd: Path,
        *,
        stored_model: str | None,
        stored_effort: str | None,
        is_new: bool,
    ) -> str | None:
        """Make sqlite match the live ACP model.

        New sessions: ``session/set_model`` to the stored id, else the configured
        / catalog default. Resume with a stored id: re-apply it (agent restart
        otherwise keeps whatever the process default is). Resume with no stored
        id: read the agent's ``summary.json`` so the chat stamp is honest.
        """
        if stored_model:
            try:
                return await self.broker.set_model(
                    session_id, stored_model, effort=stored_effort
                )
            except AcpError as exc:
                logger.warning(
                    "set_model(%s) failed session=%s: %s",
                    stored_model,
                    session_id,
                    exc,
                )

        if not is_new:
            live = discover_session_model(
                session_id, work_cwd, grok_home=self.grok_home
            )
            if live:
                return live
            return stored_model

        desired = await self._resolve_default_model()
        if desired:
            try:
                return await self.broker.set_model(
                    session_id, desired, effort=stored_effort
                )
            except AcpError as exc:
                logger.warning(
                    "pin default model %s failed session=%s: %s",
                    desired,
                    session_id,
                    exc,
                )
        return stored_model

    async def _route(
        self,
        thread_id: str,
        route: RouteRequest,
        model: str | None,
        effort: str | None,
    ) -> tuple[str | None, str | None]:
        """Model for a new thread with no explicit setting; unchanged on no match."""
        if self.router is None or not self.router.applies_to(self.store.harness):
            return model, effort
        try:
            decision = await self.router.route(route)
        except Exception:
            logger.exception("model router failed thread=%s", thread_id)
            return model, effort
        if decision is None:
            logger.info("model route thread=%s default", thread_id)
            return model, effort
        logger.info(
            "model route thread=%s model=%s effort=%s reason=%s",
            thread_id,
            decision.model,
            decision.effort,
            decision.reason,
        )
        return decision.model, decision.effort

    async def _resolve_default_model(self) -> str | None:
        if self.default_model:
            return self.default_model
        if not self.grok_command:
            return None
        try:
            catalog = await fetch_catalog(self.grok_command)
        except Exception:
            logger.debug("catalog lookup for default model failed", exc_info=True)
            return _FALLBACK_DEFAULT_MODEL
        return catalog.default_id or _FALLBACK_DEFAULT_MODEL
