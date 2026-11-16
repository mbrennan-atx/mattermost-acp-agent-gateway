"""Mattermost user loop: thread → harness session, slash verbs, status posts."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp

from mattermost_acp import harness_switch
from mattermost_acp.acp import AcpError, SessionUpdate
from mattermost_acp.acp.broker import NativeDispatchChanged
from mattermost_acp.attachments import (
    RemoteFile,
    format_attachment_note,
    save_files,
)
from mattermost_acp.channel_context import load_role_text, wrap_user_prompt
from mattermost_acp.collaboration.client import HubError
from mattermost_acp.commands.defs import (
    TEXT_COMMANDS,
    help_text,
    parse_text_command,
    resolve_passthrough_prompt,
)
from mattermost_acp.commands.effort import normalize_effort, parse_model_and_effort
from mattermost_acp.commands.mcp_status import mcp_status_report
from mattermost_acp.commands.model_set import extract_model_set_directive
from mattermost_acp.commands.models_catalog import (
    ModelCatalog,
    ModelInfo,
    fetch_catalog,
    format_catalog,
    resolve_model_id,
)
from mattermost_acp.commands.pending_restart import (
    consume_pending,
    now_pending,
    pending_path,
)
from mattermost_acp.commands.restart import restart_gateway
from mattermost_acp.mm.chunk import chunk_message, normalize_answer
from mattermost_acp.mm.client import (
    MattermostClient,
    MattermostError,
    PostedEvent,
    parse_posted,
)
from mattermost_acp.mm.progress import TurnProgressBuffer
from mattermost_acp.mm.reporting import TurnReporter
from mattermost_acp.mm.slash import SlashRequest, ephemeral
from mattermost_acp.mm.status import (
    TurnStatus,
    format_model_stamp,
    pretty_model_name,
    stamp_answer,
)
from mattermost_acp.mm.wake import classify_wake
from mattermost_acp.model_router import RouteRequest
from mattermost_acp.session_service import ThreadBusy
from mattermost_acp.thread_title import (
    MAX_RENAME_ATTEMPTS,
    compose_titled_root,
    extract_rename_directive,
    fallback_title_from_prompt,
    heading_from_root,
    is_provisional_title,
    rename_instruction,
    sanitize_title,
)

if TYPE_CHECKING:
    from mattermost_acp.config import Config
    from mattermost_acp.session_service import SessionService

logger = logging.getLogger(__name__)

_MODEL_SCOPES = ("thread", "channel", "harness")


class MattermostGateway:
    def __init__(
        self,
        config: Config,
        client: MattermostClient,
        sessions: SessionService,
    ) -> None:
        self.config = config
        self.client = client
        self.sessions = sessions
        self._stop = asyncio.Event()
        self._status_lock = asyncio.Lock()
        self._rename_attempts: dict[str, int] = {}
        self._reports: dict[str, dict] = {}
        self._prompt_locks: dict[str, asyncio.Lock] = {}
        self._last_terminal: dict[str, dict] = {}
        self.collaboration = None
        # The maintenance listener starts before run() constructs its consumer.
        # A configured project gateway cannot declare idleness in that interval.
        self.collaboration_recovery_pending = bool(
            os.environ.get("MATTERMOST_COLLAB_URL", "").strip()
            or getattr(config, "collab_sidecar", False)
        )
        self.draining = False
        self.sealed = False
        self._inbound_tasks = set()
        self._slash_tasks = set()
        self._active_controls = 0
        self._harness_restart_task: asyncio.Task | None = None
        broker = getattr(self.sessions, "broker", None)
        if broker is not None:
            broker.update_observer = self._observe_background_update

    def allowed(self, user_id: str) -> bool:
        if not self.config.allow_user_ids:
            return True
        return user_id in self.config.allow_user_ids

    def in_home(self, channel_id: str) -> bool:
        if channel_id in getattr(self.config, "exclude_channel_ids", frozenset()):
            return False
        if getattr(self.config, "all_channels", False) or not self.config.channel_ids:
            return True
        return channel_id in self.config.channel_ids

    async def run(self) -> None:
        await self.client.get_me()
        from mattermost_acp.collaboration.client import Settings
        from mattermost_acp.collaboration.consumer import CollaborationConsumer

        settings = Settings.load()
        if settings is not None:
            self.collaboration_recovery_pending = True
        if getattr(self.config, "collab_sidecar", False) and settings is None:
            raise ValueError("A collaboration sidecar requires a configured hub")
        await self._ensure_peer_membership()
        if settings is not None:
            self.collaboration = CollaborationConsumer(self, settings)
            self.collaboration.start()
        if not getattr(self.config, "collab_sidecar", False):
            self._track_control(self._handle_pending_restart_bump(), "post-restart-bump")
        logger.info(
            "Gateway online as %s (%s) harness=%s home=%s peer=%s",
            self.client.username,
            self.client.user_id,
            self.config.harness,
            "ALL" if self.config.all_channels else sorted(self.config.channel_ids),
            self.config.peer_channel_id or "-",
        )
        if self.config.auto_join_channels and not getattr(self.config, "collab_sidecar", False):
            asyncio.create_task(self._join_all_public_channels(), name="auto-join")
        async for payload in self.client.events(stop=self._stop):
            event = payload.get("event")
            if event == "channel_created":
                if self.config.auto_join_channels and not getattr(
                    self.config, "collab_sidecar", False
                ):
                    asyncio.create_task(self._join_new_channel(payload), name="auto-join-new")
                continue
            if event != "posted":
                continue
            posted = None
            try:
                posted = parse_posted(payload)
            except Exception:
                logger.exception("parse posted failed")
                continue
            if posted is None:
                continue
            # Never await the turn on the WS reader. A long ACP turn otherwise
            # stalls posted-event handling; slash HTTP still ACKs, chat dies.
            asyncio.create_task(
                self._handle_posted_safe(posted),
                name=f"post-{posted.post_id}",
            )

    async def _handle_posted_safe(self, posted: PostedEvent) -> None:
        try:
            await self.handle_posted(posted)
        except Exception:
            logger.exception("handle_posted failed post=%s", posted.post_id)
            try:
                await self.client.create_post(
                    posted.channel_id,
                    "⚠️ gateway error on that turn. Check "
                    "`logs/mattermost-acp.log` on the Linux desktop.",
                    root_id=posted.thread_id,
                )
            except Exception:
                logger.exception("failed to post error notice")

    async def _join_all_public_channels(self) -> None:
        """Join every public channel on the team. WS `posted` events only
        arrive for channels this account is a member of."""
        team_id = self.config.team_id
        if not team_id:
            logger.warning("auto-join skipped: MATTERMOST_TEAM_ID is not set")
            return
        try:
            public = await self.client.get_team_public_channels(team_id)
            mine = await self.client.get_my_channel_ids(team_id)
        except Exception:
            logger.exception("auto-join: failed to list channels")
            return
        joined = 0
        for channel in public:
            cid = str(channel.get("id") or "")
            if not cid or cid in mine:
                continue
            if not self.in_home(cid):
                continue
            try:
                await self.client.join_channel(cid)
                joined += 1
                logger.info("auto-joined channel %s (%s)", channel.get("name"), cid)
            except Exception:
                logger.exception("auto-join failed for %s", cid)
        logger.info("auto-join complete: %d new, %d public total", joined, len(public))

    async def _join_new_channel(self, payload: dict[str, Any]) -> None:
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        cid = str(data.get("channel_id") or "")
        if not cid or not self.in_home(cid):
            return
        try:
            await self.client.join_channel(cid)
            logger.info("auto-joined new channel %s", cid)
        except Exception:
            logger.exception("auto-join failed for new channel %s", cid)

    async def close(self) -> None:
        self._stop.set()
        if self.collaboration is not None:
            await self.collaboration.close()
        for report in self._reports.values():
            if report.get("task"):
                report["task"].cancel()
        self._reports.clear()

    def _observe_background_update(self, update: SessionUpdate) -> None:
        report = self._reports.get(update.session_id)
        if report is None:
            return
        if update.kind not in {
            "agent_message_chunk",
            "tool_call",
            "tool_call_update",
            "subagent_activity",
        }:
            return
        previous = report.get("previous")
        while previous is not None:
            previous["reporter"].observe(update)
            previous = previous.get("previous")
        # Observe native work before a queued prompt callback or final write
        # guard can run. The callback consumes the same event without applying
        # its pending-work state again after newer notifications.
        report["reporter"].observe(update)
        update.report_observed = True
        self.prune_reports()
        if report["active"]:
            return
        receipt = self._last_terminal.get(report["thread_id"])
        if (
            receipt
            and receipt.get("session_id") == update.session_id
            and receipt.get("execution_id") == report.get("execution_id")
        ):
            was_pending = receipt.get("pending")
            receipt["pending"] = report["reporter"].has_pending_work()
            if was_pending and not receipt["pending"]:
                receipt["completed_at"] = time.time()
                if self.collaboration is None or not self.collaboration.owns_thread(
                    report["thread_id"]
                ):
                    self._last_terminal.pop(report["thread_id"], None)
        if report.get("suppressed"):
            return
        if report.get("task") is None or report["task"].done():
            report["task"] = asyncio.create_task(
                self._publish_progress(report), name=f"progress-{report['thread_id']}"
            )

    async def _publish_progress(self, report: dict) -> None:
        reporter = report["reporter"]
        while not self._stop.is_set():
            await asyncio.sleep(1)
            if (
                not report["active"]
                and not reporter.has_pending_work()
                and time.monotonic() - reporter.last_activity >= 60
            ):
                return
            body = reporter.due()
            if body is None:
                continue
            try:
                if reporter.post_id:
                    await self.client.update_post(reporter.post_id, body)
                else:
                    post = await self._post(report["channel_id"], body, root_id=report["thread_id"])
                    reporter.post_id = post["id"]
            except Exception:
                logger.warning("progress post failed thread=%s", report["thread_id"], exc_info=True)
            finally:
                reporter.published()

    def _stop_reports(self, thread_id: str) -> None:
        for report in self.live_reports():
            if report["thread_id"] == thread_id:
                report["suppressed"] = True
                if report.get("task"):
                    report["task"].cancel()
        if not self.background_pending(thread_id) and not (
            self.collaboration is not None and self.collaboration.owns_thread(thread_id)
        ):
            for session_id, report in list(self._reports.items()):
                if report["thread_id"] == thread_id:
                    self._reports.pop(session_id, None)

    async def _ensure_peer_membership(self) -> None:
        peer = self.config.peer_channel_id
        if not peer:
            return
        try:
            await self.client.join_channel(peer)
            logger.info("peer channel joined %s", peer)
        except Exception as exc:
            status = getattr(exc, "status", None)
            # Already a member, or a private channel this user was not added to.
            logger.info("peer channel %s join status=%s", peer, status)

    async def _floor_root_author(self, posted: PostedEvent) -> str:
        """Thread starter for an allowlisted human's floor reply, else ''."""
        peer = self.config.peer_channel_id
        if not (peer and posted.channel_id == peer and posted.root_id):
            return ""
        if not self.config.allow_user_ids or posted.user_id not in self.config.allow_user_ids:
            return ""
        try:
            root = await self.client.get_post(posted.root_id)
        except Exception:
            logger.exception("floor root lookup failed for %s", posted.root_id)
            return ""
        return str(root.get("user_id") or "")

    async def handle_posted(self, posted: PostedEvent) -> None:
        task = asyncio.current_task()
        self._inbound_tasks.add(task)
        try:
            await self._handle_posted(posted)
        finally:
            self._inbound_tasks.discard(task)

    async def _handle_posted(self, posted: PostedEvent) -> None:
        if (
            self.collaboration is not None
            and posted.root_id
            and posted.channel_id == self.config.peer_channel_id
            and re.match(
                r"^\s*@" + re.escape(self.client.username) + r"\s+ASK:\s+\S",
                posted.message,
                re.IGNORECASE,
            )
            and posted.user_id != self.client.user_id
        ):
            if await self.collaboration.mention(posted):
                return
        if getattr(self.config, "collab_sidecar", False):
            return
        _kind, reason = classify_wake(
            channel_id=posted.channel_id,
            author_id=posted.user_id,
            self_id=self.client.user_id or "",
            message=posted.message or "",
            root_id=posted.root_id,
            username=self.client.username,
            home_channel_ids=self.config.channel_ids,
            all_channels=self.config.all_channels,
            exclude_channel_ids=self.config.exclude_channel_ids,
            allow_user_ids=self.config.allow_user_ids,
            peer_channel_id=self.config.peer_channel_id,
            channel_type=getattr(posted, "channel_type", ""),
            root_author_id=await self._floor_root_author(posted),
        )
        if reason == "self" or _kind is None:
            if reason != "self":
                logger.info(
                    "ignored post=%s channel=%s author=%s reason=%s",
                    posted.post_id,
                    posted.channel_id,
                    posted.user_id,
                    reason,
                )
            return
        if (
            self.collaboration is not None
            and posted.root_id
            and posted.channel_id == self.config.peer_channel_id
            and not await self._ordinary_floor_reply_allowed(posted)
        ):
            return
        content = (posted.message or "").strip()
        if not content and not posted.file_ids:
            return

        thread_id = posted.thread_id
        if self.config.text_slash:
            parsed = parse_text_command(content)
            if parsed:
                cmd, arg = parsed
                await self.dispatch_command(
                    thread_id=thread_id,
                    channel_id=posted.channel_id,
                    cmd=cmd,
                    arg=arg,
                    user_id=posted.user_id,
                    user_name="",
                )
                return

        prompt = content
        if posted.file_ids:
            prompt = await self._attach(posted, prompt)

        await self.run_prompt(
            thread_id=thread_id,
            channel_id=posted.channel_id,
            prompt=prompt,
            author_id=posted.user_id,
            author_name=str(posted.raw.get("user_id") or posted.user_id),
            has_attachments=bool(posted.file_ids),
        )

    async def _ordinary_floor_reply_allowed(self, posted: PostedEvent) -> bool:
        """Allow ordinary wake only after confirming an unregistered discussion."""
        try:
            result = await self.collaboration.hub.request(
                "POST",
                "/mentions",
                {
                    # An unknown author selects the hub's read-only fallback.
                    # Registered discussions reject before any work is created.
                    "author_id": None,
                    "channel_id": posted.channel_id,
                    "thread_id": posted.root_id,
                    "post_id": posted.post_id,
                    "text": posted.message,
                },
            )
        except (HubError, aiohttp.ClientError, TimeoutError, ValueError):
            logger.info("ordinary floor wake suppressed post=%s", posted.post_id)
            return False
        return result is None

    async def handle_slash(self, req: SlashRequest) -> dict[str, str]:
        self._active_controls += 1
        try:
            return await self._handle_slash(req)
        finally:
            self._active_controls -= 1

    async def _handle_slash(self, req: SlashRequest) -> dict[str, str]:
        if getattr(self.config, "collab_sidecar", False):
            return ephemeral("Use mm-work for shared project work.")
        if self.sealed or (self.draining and req.command != "stop"):
            return ephemeral("Gateway updating. Retry the command shortly.")
        if not self.allowed(req.user_id):
            return ephemeral("Not allowed.")
        if not self.in_home(req.channel_id):
            return ephemeral("This command is only wired in a home channel.")
        cmd = req.command
        if cmd not in TEXT_COMMANDS:
            return ephemeral(f"Unknown command `/{cmd}`. Try `/help`.")
        thread_id = req.root_id
        if cmd == "hint" and not thread_id:
            return ephemeral("Use `/hint <text>` inside the thread where the agent is working.")
        if not thread_id:
            # Slash payload often omits root_id. Start a thread from a new post.
            starter = await self.client.create_post(
                req.channel_id,
                f"↗ `/{cmd}`{' ' + req.text if req.text else ''}".strip(),
            )
            thread_id = str(starter.get("id") or "")
        # MM requires a JSON response in a few seconds. Do the real work after ACK.
        task = asyncio.create_task(
            self._slash_dispatch(
                thread_id=thread_id,
                channel_id=req.channel_id,
                cmd=cmd,
                arg=req.text,
                user_id=req.user_id,
                user_name=req.user_name,
            ),
            name=f"slash-{cmd}",
        )
        self._slash_tasks.add(task)
        task.add_done_callback(self._slash_tasks.discard)
        if req.root_id:
            ack = f"Working in this thread (`/{cmd}`)."
        else:
            ack = (
                f"This agent will post `↗ /{cmd}` in this channel and reply in "
                "that thread. This grey BOT line is only visible to you."
            )
        return ephemeral(ack)

    async def _slash_dispatch(
        self,
        *,
        thread_id: str,
        channel_id: str,
        cmd: str,
        arg: str,
        user_id: str,
        user_name: str,
    ) -> None:
        try:
            await self.dispatch_command(
                thread_id=thread_id,
                channel_id=channel_id,
                cmd=cmd,
                arg=arg,
                user_id=user_id,
                user_name=user_name,
            )
        except Exception:
            logger.exception("slash dispatch failed cmd=%s thread=%s", cmd, thread_id)
            try:
                await self._post(
                    channel_id,
                    f"❌ `/{cmd}` failed. Check gateway logs.",
                    root_id=thread_id,
                )
            except Exception:
                logger.exception("slash error notice failed")

    async def dispatch_command(self, **kwargs) -> None:
        if self.sealed or (self.draining and kwargs["cmd"] != "stop"):
            await self._post(
                kwargs["channel_id"],
                "Gateway updating. Retry the command shortly.",
                root_id=kwargs["thread_id"],
            )
            return
        self._active_controls += 1
        try:
            await self._dispatch_command(**kwargs)
        finally:
            self._active_controls -= 1

    def _track_control(self, operation, name):
        task = asyncio.create_task(operation, name=name)
        self._slash_tasks.add(task)
        task.add_done_callback(self._slash_tasks.discard)
        return task

    async def _dispatch_command(
        self,
        *,
        thread_id: str,
        channel_id: str,
        cmd: str,
        arg: str,
        user_id: str,
        user_name: str,
    ) -> None:
        async def reply(text: str) -> None:
            await self._post(channel_id, text, root_id=thread_id)

        if cmd == "hint":
            if not arg.strip():
                await reply("Usage: `/hint <guidance for the active work>`")
                return
            try:
                outcome = await self.sessions.hint(thread_id, arg)
            except AcpError as exc:
                await reply(f"Hint not confirmed: {exc}")
                return
            if outcome == "injected":
                await reply("Hint delivered to the active work.")
            else:
                await reply("There is no active turn now. Send your hint as a normal message.")
            return
        if cmd == "stop":
            self._stop_reports(thread_id)
            project_stop = None
            if self.collaboration is not None:
                cutoff = self.collaboration.record_stop(thread_id)
                project_stop = self._track_control(
                    self.collaboration.cancel_thread(thread_id, retry=True, cutoff=cutoff),
                    "project-stop-" + thread_id,
                )
            ok = False
            native_error = None
            try:
                ok = await self.sessions.cancel(thread_id)
            except Exception as exc:
                native_error = type(exc).__name__
            project_error = False
            if project_stop is not None:
                try:
                    await project_stop
                except Exception:
                    project_error = True
                    logger.warning("Project cancellation unconfirmed; retry intent retained")
            answer = "⏹ Cancelled." if ok else "Nothing to cancel."
            if native_error:
                answer = f"Native stop was not confirmed ({native_error})."
            if project_error:
                answer += " Project cancellation is unconfirmed; retry remains queued."
            await reply(answer)
            return
        if cmd in {"new", "clear"}:
            self._stop_reports(thread_id)
            sid = await self.sessions.new_session(thread_id, channel_id=channel_id)
            await self.sessions.store.clear_meta(thread_id)
            self._rename_attempts.pop(thread_id, None)
            await reply(f"🔄 New session `{sid}`.")
            return
        if cmd == "status":
            st = await self.sessions.status(thread_id)
            await reply(self._format_status(st))
            return
        if cmd == "help":
            await reply(
                help_text(home_channel_ids=self.config.channel_ids, harness=self.config.harness)
            )
            return
        if cmd == "restart":
            msg = await self._request_restart(
                thread_id=thread_id,
                channel_id=channel_id,
                user_id=user_id,
                reason="slash_restart",
            )
            await reply(msg)
            return
        if cmd in {"mcps", "mcp"}:
            report = await mcp_status_report(
                self.config.harness, self.config.grok_command, server=arg or None
            )
            await reply(report.text)
            return
        if cmd in {"model", "models"}:
            await self._cmd_model(thread_id, channel_id, arg, reply)
            return
        if cmd == "effort":
            await self._cmd_effort(thread_id, arg, reply)
            return
        if cmd == "harness":
            await self._cmd_harness(thread_id, channel_id, user_id, arg, reply)
            return

        prompt = resolve_passthrough_prompt(cmd, arg)
        if prompt is not None:
            if self.config.harness != "grok" and cmd not in {"context", "compact"}:
                await reply(f"`/{cmd}` is a Grok-specific command; unavailable for this harness.")
                return
            if self.config.harness == "cursor":
                await reply(f"`/{cmd}` passthrough is unavailable for Cursor.")
                return
            await reply(f"↗ `{prompt}`")
            await self.run_prompt(
                thread_id=thread_id,
                channel_id=channel_id,
                prompt=prompt,
                author_id=user_id,
                author_name=user_name,
                passthrough=True,
            )
            return
        await reply(f"Unknown command `/{cmd}`. Try `/help`.")

    def collaboration_recovering(self):
        return self.collaboration_recovery_pending or bool(
            getattr(self.collaboration, "recovery_pending", False)
        )

    def ordinary_admitted(self, thread_id, ownership_guard=None):
        if self.collaboration_recovering():
            return False
        if self.collaboration is None:
            return True
        inherited = getattr(self.collaboration, "inherited_thread_pending", lambda _thread: False)
        if inherited(thread_id):
            return False
        return not self.collaboration.owns_thread(thread_id) or bool(
            ownership_guard and ownership_guard()
        )

    async def run_prompt(self, *, thread_id: str, **kwargs) -> dict | None:
        if self.draining or self.sealed:
            if kwargs.get("before_native") is not None:
                return {"queued": True}
            await self._post(
                kwargs["channel_id"],
                "Gateway updating. Retry this message shortly.",
                root_id=thread_id,
            )
            return None
        if kwargs.get("before_native") is None and self.collaboration_recovering():
            await self._post(
                kwargs["channel_id"],
                "Project recovery is pending. Retry this message shortly.",
                root_id=thread_id,
            )
            return None
        lock = self._prompt_locks.setdefault(thread_id, asyncio.Lock())
        if lock.locked() and kwargs.get("before_native") is None:
            await self._post(
                kwargs["channel_id"],
                "Already working in this thread. Use /hint or /stop.",
                root_id=thread_id,
            )
            return None
        async with lock:
            if self.draining or self.sealed:
                if kwargs.get("before_native") is not None:
                    return {"queued": True}
                await self._post(
                    kwargs["channel_id"],
                    "Gateway updating. Retry this message shortly.",
                    root_id=thread_id,
                )
                return None
            if kwargs.get("before_native") is None and self.collaboration_recovering():
                await self._post(
                    kwargs["channel_id"],
                    "Project recovery is pending. Retry this message shortly.",
                    root_id=thread_id,
                )
                return None
            if (
                self.collaboration is not None
                and self.collaboration.owns_thread(thread_id)
                and not (kwargs.get("ownership_guard") and kwargs["ownership_guard"]())
            ):
                if kwargs.get("before_native") is None:
                    await self._post(
                        kwargs["channel_id"],
                        "Project work is still pending in this thread. Use /hint or /stop.",
                        root_id=thread_id,
                    )
                    return None
                return {"queued": True}
            try:
                return await self._run_prompt(thread_id=thread_id, **kwargs)
            finally:
                for report in self.live_reports():
                    if report["thread_id"] == thread_id:
                        report["preparing"] = False
                self.prune_reports()
                for report in self.live_reports():
                    if report["thread_id"] == thread_id and not report.get("active"):
                        report["foreground_complete"] = True

    def live_reports(self):
        seen = set()
        for report in self._reports.values():
            while report is not None and id(report) not in seen:
                seen.add(id(report))
                yield report
                report = report.get("previous")

    def prune_reports(self):
        """Keep predecessors only while preparation, work or ownership needs them."""
        owned = {id(report) for report in getattr(self.collaboration, "ownership", {}).values()}
        for head in self._reports.values():
            if head.get("preparing"):
                continue
            current = head
            seen = {id(head)}
            while (previous := current.get("previous")) is not None:
                if id(previous) in seen:
                    current["previous"] = None
                    break
                seen.add(id(previous))
                if (
                    previous.get("active")
                    or previous["reporter"].has_pending_work()
                    or id(previous) in owned
                ):
                    current = previous
                    continue
                current["previous"] = previous.get("previous")
                previous["previous"] = None
                if previous.get("task"):
                    previous["task"].cancel()

    def background_pending(self, thread_id, *, exclude=None):
        return any(
            report is not exclude
            and report["thread_id"] == thread_id
            and (report.get("active") or report["reporter"].has_pending_work())
            for report in self.live_reports()
        )

    def maintenance_state(self):
        native_unknown = not getattr(
            self.sessions.broker,
            "native_boundary_idle",
            not getattr(self.sessions.broker, "is_running", False),
        )
        pending = sum(
            bool(report.get("active") or report["reporter"].has_pending_work())
            for report in self.live_reports()
        )
        state = {
            "draining": self.draining,
            "sealed": self.sealed,
            "foreground": len(getattr(self.sessions, "busy_threads", ())),
            "responses": sum(lock.locked() for lock in self._prompt_locks.values()),
            "inbound": len(self._inbound_tasks),
            "commands": self._active_controls + len(self._slash_tasks),
            "native_rpc": len(getattr(self.sessions.broker, "_pending", {})),
            "background": pending + int(native_unknown),
            "native_boundary_unknown": native_unknown,
            "collaboration": (
                len(self.collaboration.workers)
                + len(getattr(self.collaboration, "pending_stops", ()))
                + len(getattr(self.collaboration, "ownership", ()))
                + len(getattr(self.collaboration, "reserving", ()))
                + int(getattr(self.collaboration, "claiming", False))
                + int(getattr(self.collaboration, "claim_uncertain", False))
                + getattr(self.collaboration, "admission_pending", 0)
                + int(getattr(self.collaboration, "recovery_pending", False))
                + len(getattr(self.collaboration, "inherited_claims", ()))
            )
            if self.collaboration
            else int(self.collaboration_recovery_pending),
            "sidecar": getattr(self.config, "collab_sidecar", False),
            "actor": getattr(self.collaboration, "actor", ""),
            "harness": self.config.harness,
            "db_path": str(getattr(self.config, "db_path", "")),
        }
        state["idle"] = not any(
            state[key]
            for key in {
                "foreground",
                "responses",
                "inbound",
                "commands",
                "native_rpc",
                "background",
                "collaboration",
            }
        )
        return state

    async def _run_prompt(
        self,
        *,
        thread_id: str,
        channel_id: str,
        prompt: str,
        author_id: str,
        author_name: str,
        passthrough: bool = False,
        has_attachments: bool = False,
        workspace: str | None = None,
        collaboration_project: str | None = None,
        before_native=None,
        ownership_guard=None,
    ) -> dict | None:
        ordinary_collaboration = before_native is None and (
            self.collaboration is not None or self.collaboration_recovery_pending
        )
        if before_native is not None and self.background_pending(thread_id):
            return {"queued": True}
        if self.sessions.is_busy(thread_id):
            if before_native is not None:
                return {"queued": True, "native_dispatched": False}
            await self._post(
                channel_id,
                "⏳ Already working in this thread. `/hint <text>` to add guidance; "
                "`/stop` to cancel.",
                root_id=thread_id,
            )
            return

        user_text_for_title = prompt if not passthrough else ""
        root_message = ""
        current_name = ""
        if self.config.auto_rename_threads or self.config.inject_channel_context:
            root_message, current_name = await self._root_preview(thread_id)

        text = prompt
        if not passthrough and self.config.inject_channel_context:
            text = wrap_user_prompt(
                prompt,
                thread_id=thread_id,
                thread_name=current_name,
                author_name=author_name,
                author_id=author_id,
                parent_channel_id=channel_id,
                team_id=self.config.team_id,
                harness=self.config.harness,
                role_text=load_role_text(self.config.fleet_file, self.config.role_file),
            )

        need_title = False
        if not passthrough and self.config.auto_rename_threads:
            need_title = await self._need_title(thread_id, current_name)
            if need_title:
                text = rename_instruction(current_name) + text

        # Sync sqlite to the live ACP model before stamping, otherwise the
        # stored default is printed while the session runs something else.
        route = (
            None
            if passthrough
            else RouteRequest(prompt, channel_id=channel_id, has_attachments=has_attachments)
        )
        if ordinary_collaboration and not self.ordinary_admitted(thread_id, ownership_guard):
            await self._post(
                channel_id,
                "Project work is pending. Retry when the thread is ready.",
                root_id=thread_id,
            )
            return None
        try:
            options = {"channel_id": channel_id, "route": route}
            if workspace:
                record = await self.sessions.store.get(thread_id)
                if record is not None and Path(record.cwd).resolve() != Path(workspace).resolve():
                    raise ValueError("Existing native session uses a different workspace")
                options["cwd"] = Path(workspace)
            await self.sessions.ensure_session(thread_id, **options)
        except NativeDispatchChanged:
            logger.warning("Workspace verification refused native preparation")
            if before_native is not None:
                return {"queued": True, "native_dispatched": False}
            await self._post(
                channel_id,
                "Workspace verification failed. Resolve the settings before retrying.",
                root_id=thread_id,
            )
            return None
        except Exception:
            if workspace:
                logger.warning("Project workspace preparation failed; no native work started")
                return None
            logger.debug("ensure_session before model stamp failed", exc_info=True)
        session_status = await self.sessions.status(thread_id)
        model_label = pretty_model_name(str(session_status.get("model") or "") or None)
        effort = str(session_status.get("effort") or "") or None
        model_stamp = format_model_stamp(None, label=model_label, effort=effort)

        status = TurnStatus(
            model_label=model_label,
            effort=effort,
            live_tool_lines=self.config.status_tool_lines,
            done_tool_lines=self.config.status_done_tool_lines,
            live_progress_lines=self.config.status_progress_lines,
            done_progress_lines=0,
        )
        progress = TurnProgressBuffer()
        reporter = TurnReporter()
        logger.info(
            "turn start thread=%s channel=%s author=%s prompt_chars=%s",
            thread_id,
            channel_id,
            author_id,
            len(prompt or ""),
        )

        async def _publish_status(line: str, *, force: bool = False) -> None:
            if not status.should_publish(line, force=force):
                return
            async with self._status_lock:
                try:
                    if status.post_id is None:
                        post = await self.client.create_post(channel_id, line, root_id=thread_id)
                        status.post_id = str(post.get("id") or "") or None
                    else:
                        await self.client.update_post(status.post_id, line)
                    status.last_line = line
                    status.last_edit = time.monotonic()
                except MattermostError:
                    logger.warning("status post failed thread=%s", thread_id, exc_info=True)

        async def on_update(update: SessionUpdate) -> None:
            # Never let status UX abort the ACP turn.
            try:
                status.observe(update)
                if not update.report_observed:
                    reporter.observe(update)
                    prior = report.get("previous")
                    while prior is not None:
                        prior["reporter"].observe(update)
                        prior = prior.get("previous")
                if update.kind == "agent_message_chunk":
                    if not update.text:
                        return
                    progress.on_agent_text(update.text)
                elif update.kind == "tool_call":
                    progress.on_tool()
                elif update.kind == "tool_call_update":
                    # Also counts as tool activity so pre-tool narration does
                    # not sit in the current buffer when only updates arrive.
                    if (update.tool_status or "").lower() in {
                        "in_progress",
                        "pending",
                        "running",
                    }:
                        progress.on_tool()
                elif update.kind == "subagent_activity":
                    pass
                elif update.kind != "heartbeat":
                    return  # thoughts / other — don't thrash the status post
                status.set_progress_lines(progress.progress_lines() + reporter.worker_lines())
                await _publish_status(status.render())
            except Exception:
                logger.exception("on_update failed kind=%s thread=%s", update.kind, thread_id)

        session_id = str(session_status.get("session_id") or "")
        previous = self._reports.get(session_id)
        previous_terminal = self._last_terminal.get(thread_id)
        if before_native is not None and self.background_pending(thread_id):
            return {"queued": True}
        if previous and previous.get("task"):
            previous["task"].cancel()
        report = {
            "execution_id": uuid.uuid4().hex,
            "collaboration_project": collaboration_project,
            "reporter": reporter,
            "thread_id": thread_id,
            "channel_id": channel_id,
            "active": True,
            "foreground_complete": False,
            "preparing": before_native is not None or ordinary_collaboration,
            "previous": previous
            if previous
            and (
                before_native is not None
                or ordinary_collaboration
                or self.background_pending(thread_id)
            )
            else None,
            "task": None,
        }
        if session_id:
            self._reports[session_id] = report
        progress_task = asyncio.create_task(
            self._publish_progress(report), name=f"progress-{thread_id}"
        )
        report["task"] = progress_task

        async def restore_undispatched():
            actual_session = str(session_status.get("session_id") or "")
            if previous is not None and self._reports.get(actual_session) is report:
                progress_task.cancel()
                await asyncio.gather(progress_task, return_exceptions=True)
                self._reports.pop(actual_session, None)
                self._reports[session_id] = previous
                if previous_terminal is not None:
                    previous_terminal["pending"] = previous["reporter"].has_pending_work()
                    self._last_terminal[thread_id] = previous_terminal
                if previous["reporter"].has_pending_work() and (
                    previous.get("task") is None or previous["task"].done()
                ):
                    previous["task"] = asyncio.create_task(
                        self._publish_progress(previous), name=f"progress-{thread_id}"
                    )

        try:
            await _publish_status(status.render(), force=True)
            try:
                turn_options = {"channel_id": channel_id, "on_update": on_update, "route": route}
                if workspace:
                    turn_options["cwd"] = Path(workspace)
                if before_native is not None:

                    async def bind_native(actual_session):
                        if (
                            self.background_pending(thread_id, exclude=report)
                            or reporter.has_pending_work()
                        ):
                            raise NativeDispatchChanged("Background work resumed before dispatch")
                        old_session = str(session_status.get("session_id") or "")
                        if (
                            self._reports.get(old_session) is report
                            and old_session != actual_session
                        ):
                            self._reports.pop(old_session, None)
                        self._reports[actual_session] = report
                        session_status["session_id"] = actual_session
                        try:
                            return await before_native(actual_session)
                        except Exception as exc:
                            # Preparation callbacks run before session/prompt can
                            # write anything. A lost hub response is recoverable
                            # without claiming native termination or repeating an
                            # operation that could have reached the native client.
                            raise NativeDispatchChanged(
                                "Native reservation did not finish"
                            ) from exc

                    turn_options["before_prompt"] = bind_native
                    turn_options["dispatch_guard"] = lambda: (
                        not self.background_pending(thread_id, exclude=report)
                        and not reporter.has_pending_work()
                        and not report.get("cancel_requested")
                    )
                elif ordinary_collaboration:

                    async def bind_ordinary(actual_session):
                        if (
                            not self.ordinary_admitted(thread_id, ownership_guard)
                            or self.draining
                            or self.sealed
                        ):
                            raise NativeDispatchChanged("Project recovery changed native admission")
                        old_session = str(session_status.get("session_id") or "")
                        if (
                            self._reports.get(old_session) is report
                            and old_session != actual_session
                        ):
                            self._reports.pop(old_session, None)
                        self._reports[actual_session] = report
                        session_status["session_id"] = actual_session
                        return True

                    turn_options["before_prompt"] = bind_ordinary
                    turn_options["dispatch_guard"] = lambda: (
                        self.ordinary_admitted(thread_id, ownership_guard)
                        and not self.draining
                        and not self.sealed
                        and not report.get("cancel_requested")
                    )
                result = await self.sessions.run_turn(thread_id, text, **turn_options)
            finally:
                report["active"] = False
                if not reporter.has_pending_work():
                    progress_task.cancel()
                    try:
                        await progress_task
                    except asyncio.CancelledError:
                        pass
        except NativeDispatchChanged:
            await restore_undispatched()
            if ordinary_collaboration:
                await self._post(
                    channel_id,
                    "This message did not start. Retry when the thread is ready.",
                    root_id=thread_id,
                )
                return None
            return {"queued": True}
        except AcpError as exc:
            logger.warning("ACP error thread=%s: %s", thread_id, exc)
            await _publish_status(status.render(phase="failed"), force=True)
            await self._post(channel_id, f"❌ ACP error: {exc}", root_id=thread_id)
            return
        except ThreadBusy as exc:
            if before_native is not None:
                await restore_undispatched()
                return {"queued": True, "native_dispatched": False}
            logger.warning("turn busy thread=%s: %s", thread_id, exc)
            await self._post(channel_id, f"⏳ {exc}", root_id=thread_id)
            return
        except RuntimeError as exc:
            logger.warning("turn busy/runtime thread=%s: %s", thread_id, exc)
            await self._post(channel_id, f"⏳ {exc}", root_id=thread_id)
            return
        except Exception as exc:
            logger.exception("turn crashed thread=%s", thread_id)
            await _publish_status(status.render(phase="failed"), force=True)
            await self._post(
                channel_id,
                f"❌ gateway crash: `{type(exc).__name__}: {exc}`",
                root_id=thread_id,
            )
            return

        if before_native is not None and not result.native_dispatched:
            await restore_undispatched()
            return {"queued": True, "native_dispatched": False}

        # Progress already has its own live post. Keep it out of the answer.
        # Preserve the narration fallback when an adapter supplies no final text.
        answer = normalize_answer(
            progress.format_answer(include_progress=False) or result.answer_text or ""
        )
        answer, new_title = extract_rename_directive(answer)
        answer, directive = extract_model_set_directive(answer)
        if directive is not None:
            try:
                await self._apply_model_directive(
                    thread_id, channel_id, directive.query, directive.effort, directive.scope
                )
            except Exception:
                logger.exception("model set from agent failed")

        public_answer = answer
        receipt = {
            "execution_id": report["execution_id"],
            "answer": public_answer,
            "stop_reason": result.stop_reason,
            "native_dispatched": result.native_dispatched,
            "session_id": result.session_id,
            "model": result.model_id or str(session_status.get("model") or ""),
            "pending": reporter.has_pending_work(),
            "completed_at": time.time(),
        }
        if (
            reporter.has_pending_work()
            or before_native is not None
            or (self.collaboration is not None and self.collaboration.owns_thread(thread_id))
        ):
            self._last_terminal[thread_id] = receipt
            # Keep a bounded preview and durable full output while ownership is unresolved.
            if len(public_answer) > 8000:
                from mattermost_acp.collaboration.consumer import CollaborationConsumer

                receipt["answer"] = public_answer[:8000]
                try:
                    evidence = CollaborationConsumer.foreground(
                        {"id": report["execution_id"], "claim": "terminal"},
                        {**receipt, "answer": public_answer},
                    )
                except OSError as exc:
                    receipt["result_file_error"] = f"{type(exc).__name__}: {exc}"
                    logger.exception("foreground artifact failed thread=%s", thread_id)
                else:
                    receipt["result_file"] = evidence["result_file"]

        if self.config.show_thoughts and result.thought_text:
            answer = f"_{result.thought_text.strip()}_\n\n{answer}".strip()

        if not answer.strip():
            answer = "(empty reply)"
        answer = stamp_answer(normalize_answer(answer), model_stamp)
        parts = chunk_message(answer, max_lines=self.config.post_max_lines)
        logger.info(
            "turn done thread=%s stop=%s answer_chars=%s parts=%s tools=%s elapsed=%ss",
            thread_id,
            result.stop_reason,
            len(answer),
            len(parts),
            len(status.tools),
            status.elapsed_s(),
        )
        try:
            for part in parts:
                await self._post(channel_id, part, root_id=thread_id)
        except MattermostError as exc:
            logger.exception("final post failed thread=%s", thread_id)
            await self._post(
                channel_id,
                f"❌ failed to post the full answer ({exc}). "
                f"Buffered {len(answer)} chars, {len(parts)} parts.",
                root_id=thread_id,
            )
            return receipt

        if result.usage_used is not None:
            status.usage_used = result.usage_used
        if result.usage_size is not None:
            status.usage_size = result.usage_size

        applied_title: str | None = None
        if self.config.auto_rename_threads and not passthrough:
            applied_title = await self._finish_rename(
                thread_id,
                root_message,
                user_text_for_title,
                new_title,
                need_title,
            )
        extra = f"titled → {applied_title}" if applied_title else None
        await _publish_status(
            status.render(phase="done", stop_reason=result.stop_reason, extra=extra),
            force=True,
        )
        receipt["response_delivered"] = True
        logger.info("response complete thread=%s session=%s", thread_id, result.session_id)
        return receipt

    async def _root_preview(self, thread_id: str) -> tuple[str, str]:
        rec = await self.sessions.store.get(thread_id)
        stored = ((rec.title if rec else None) or "").strip()
        message = ""
        try:
            post = await self.client.get_post(thread_id)
            message = str(post.get("message") or "")
        except MattermostError:
            logger.warning("get_post failed thread=%s", thread_id, exc_info=True)
        name = stored or heading_from_root(message)
        return message, name

    async def _need_title(self, thread_id: str, current_name: str) -> bool:
        if self._rename_attempts.get(thread_id, 0) >= MAX_RENAME_ATTEMPTS:
            return False
        rec = await self.sessions.store.get(thread_id)
        stored = ((rec.title if rec else None) or "").strip()
        live = (current_name or "").strip()
        if live and not is_provisional_title(live):
            if not stored:
                await self.sessions.store.set_title(thread_id, sanitize_title(live))
            return False
        if stored and not is_provisional_title(stored):
            return False
        return True

    async def _apply_root_rename(
        self, thread_id: str, original_message: str, title: str
    ) -> str | None:
        clean = sanitize_title(title)
        if not clean or is_provisional_title(clean):
            return None
        new_body = compose_titled_root(original_message, clean)
        if new_body == (original_message or "").strip():
            await self.sessions.store.set_title(thread_id, clean)
            return clean
        try:
            await self.client.patch_post(thread_id, new_body)
        except MattermostError:
            logger.warning("Failed to title root post %s → %r", thread_id, clean, exc_info=True)
            return None
        await self.sessions.store.set_title(thread_id, clean)
        logger.info("Titled thread %s → %r", thread_id, clean)
        return clean

    async def _finish_rename(
        self,
        thread_id: str,
        root_message: str,
        user_text: str,
        new_title: str | None,
        need_title: bool,
    ) -> str | None:
        if not need_title:
            return None
        applied = None
        if new_title:
            applied = await self._apply_root_rename(thread_id, root_message, new_title)
        if applied and not is_provisional_title(applied):
            self._rename_attempts.pop(thread_id, None)
            return applied
        attempts = self._rename_attempts.get(thread_id, 0) + 1
        self._rename_attempts[thread_id] = attempts
        if attempts < MAX_RENAME_ATTEMPTS:
            return applied
        fallback = fallback_title_from_prompt(user_text)
        if not fallback or is_provisional_title(fallback):
            fallback = "Untitled topic"
        applied = await self._apply_root_rename(thread_id, root_message, fallback)
        if applied:
            logger.info(
                "Thread %s title fallback after %s attempts → %r",
                thread_id,
                attempts,
                applied,
            )
        return applied

    async def _attach(self, posted: PostedEvent, prompt: str) -> str:
        files: list[RemoteFile] = []
        for fid in posted.file_ids:
            try:
                info = await self.client.get_file_info(fid)
            except MattermostError:
                logger.warning("file info failed id=%s", fid, exc_info=True)
                continue
            files.append(
                RemoteFile(
                    file_id=fid,
                    filename=str(info.get("name") or fid),
                    size=int(info.get("size") or 0),
                    content_type=info.get("mime_type"),
                )
            )
        saved, skipped = await save_files(
            files,
            base_dir=self.config.attachments_dir,
            thread_id=posted.thread_id,
            message_id=posted.post_id,
            max_bytes=self.config.attachments_max_bytes,
            fetch=self.client.get_file,
        )
        note = format_attachment_note(saved, skipped)
        if not note:
            return prompt
        return f"{prompt}\n\n{note}".strip()

    async def _cmd_model(self, thread_id: str, channel_id: str, arg: str, reply) -> None:
        catalog = await self._model_catalog(thread_id, channel_id)
        raw = (arg or "").strip()
        if not raw:
            await reply(format_catalog(catalog))
            return
        parts = raw.split()
        scope = "thread"
        if parts and parts[-1].lower() in _MODEL_SCOPES:
            scope = parts.pop().lower()
        query, effort = parse_model_and_effort(" ".join(parts))
        mid, candidates = resolve_model_id(catalog, query)
        if mid is None:
            if candidates:
                listed = ", ".join(f"`{c}`" for c in candidates[:8])
                await reply(f"❌ `{query}` matches more than one model: {listed}")
            else:
                await reply(f"❌ Unknown model `{query}`. `/model` lists catalog ids.")
            return
        switch = await self.sessions.switch_model(
            thread_id, mid, effort=effort, scope=scope, channel_id=channel_id
        )
        extra = ""
        if switch.compaction == "compacted":
            extra = " Compacted history first."
        elif switch.compaction == "failed":
            extra = " ⚠️ Compaction failed; switch still applied."
        effort_bit = f" effort `{effort}`" if effort else ""
        await reply(f"✅ Model `{switch.active_model or mid}`{effort_bit} ({scope}).{extra}")

    async def _cmd_effort(self, thread_id: str, arg: str, reply) -> None:
        level = normalize_effort(arg)
        if not arg.strip():
            st = await self.sessions.status(thread_id)
            await reply(f"Effort: `{st.get('effort') or '—'}` on `{st.get('model') or '—'}`")
            return
        if level is None:
            await reply(f"❌ Unknown effort `{arg}`. Use none/minimal/low/medium/high/xhigh/max.")
            return
        model, effort = await self.sessions.set_effort(thread_id, level)
        await reply(f"✅ Effort `{effort}` on `{model}`.")

    async def _apply_model_directive(
        self,
        thread_id: str,
        channel_id: str,
        query: str,
        effort: str | None,
        scope: str,
    ) -> None:
        catalog = await self._model_catalog(thread_id, channel_id)
        mid, _ = resolve_model_id(catalog, query)
        if not mid:
            logger.warning("MATTERMOST_MODEL_SET unknown query=%s", query)
            return
        await self.sessions.switch_model(
            thread_id, mid, effort=effort, scope=scope, channel_id=channel_id
        )

    async def _model_catalog(self, thread_id: str, channel_id: str) -> ModelCatalog:
        if self.config.harness == "grok":
            return await fetch_catalog(self.config.grok_command)
        sid = await self.sessions.ensure_session(thread_id, channel_id=channel_id)
        options = self.sessions.broker.session_config_options.get(sid, [])
        model = next((opt for opt in options if opt.get("id") == "model"), {})
        models = [
            ModelInfo(id=opt["value"], is_default=opt["value"] == self.config.grok_model)
            for opt in model.get("options", [])
            if isinstance(opt, dict) and isinstance(opt.get("value"), str)
        ]
        if not models:
            raise AcpError(f"{self.config.harness} adapter did not supply a model catalog")
        return ModelCatalog(self.config.grok_model, models, "ACP configOptions", time.monotonic())

    async def _cmd_harness(
        self,
        thread_id: str,
        channel_id: str,
        user_id: str,
        arg: str,
        reply: Any,
    ) -> None:
        parts = arg.split()
        try:
            profiles = harness_switch.load_profiles()
        except harness_switch.SwitchError as exc:
            await reply(f"❌ {exc}")
            return
        if not parts:
            names = ", ".join(f"`{p.name}` ({p.model})" for p in profiles.values())
            await reply(
                f"Harness: `{self.config.harness}` model `{self.config.grok_model or '—'}`\n"
                f"Available: {names}\n"
                "Switch: `/harness <name> [model]`. Restarts this agent once it is idle; "
                "each thread's next message starts a fresh session on the new harness."
            )
            return
        target, model = parts[0].lower(), (parts[1] if len(parts) > 1 else None)
        if target == self.config.harness and model is None:
            await reply(f"Already on `{target}`.")
            return
        try:
            roster = harness_switch.load_roster()
            agent = harness_switch.find_agent(roster, user_id=self.client.user_id)
            plan = await asyncio.to_thread(
                harness_switch.apply_switch, str(agent["username"]), target, model=model
            )
        except harness_switch.SwitchError as exc:
            await reply(f"❌ {exc}")
            return
        await reply(
            f"🔀 `{plan.old_harness}` → `{plan.new_harness}` (model `{plan.model}`). "
            "Saved. Automatic restart requires verified idle status."
        )
        pending_restart = getattr(self, "_harness_restart_task", None)
        if pending_restart is None or pending_restart.done():
            self._harness_restart_task = self._track_control(
                self._restart_when_idle(thread_id, channel_id, user_id),
                "harness-switch-restart",
            )

    async def _restart_when_idle(
        self, thread_id: str, channel_id: str, user_id: str, *, timeout_s: float = 900
    ) -> None:
        deadline = time.monotonic() + timeout_s
        while True:
            state = self.maintenance_state()
            # This helper is the control deciding whether it may restart.
            state["commands"] -= int(asyncio.current_task() in self._slash_tasks)
            if state["native_boundary_unknown"]:
                await self._post(
                    channel_id,
                    "The new harness is saved. Native goal/queue idle status is unknown; "
                    "automatic restart is deferred.",
                    root_id=thread_id,
                )
                return
            if not any(
                state[key]
                for key in (
                    "foreground",
                    "responses",
                    "inbound",
                    "commands",
                    "native_rpc",
                    "background",
                    "collaboration",
                )
            ):
                break
            if time.monotonic() >= deadline:
                await self._post(
                    channel_id,
                    "The new harness is saved. Pending work prevents automatic restart.",
                    root_id=thread_id,
                )
                return
            await asyncio.sleep(min(5, max(0, deadline - time.monotonic())))
        # No await separates the idle check from closing native admission.
        previous_flags = self.draining, self.sealed
        self.draining = self.sealed = True
        try:
            msg = await self._request_restart(
                thread_id=thread_id,
                channel_id=channel_id,
                user_id=user_id,
                reason="harness_switch",
            )
        except BaseException:
            self.draining, self.sealed = previous_flags
            raise
        logger.info("harness switch restart: %s", msg)

    def _format_status(self, st: dict[str, object]) -> str:
        reporter = TurnReporter()
        for worker in st.get("subagents") or []:
            reporter.observe(
                SessionUpdate(
                    session_id=str(st.get("session_id") or ""),
                    kind="subagent_activity",
                    raw=worker,
                )
            )
        workers = reporter.worker_lines()
        worker_text = (
            "\n\nWorkers:\n" + "\n".join(f"- {line}" for line in workers) if workers else ""
        )
        return (
            f"**status** harness=`{self.config.harness}`\n"
            f"• thread `{st.get('thread_id')}`\n"
            f"• session `{st.get('session_id') or '—'}`\n"
            f"• cwd `{st.get('cwd')}`\n"
            f"• model `{st.get('model') or '—'}` effort `{st.get('effort') or '—'}`\n"
            f"• busy `{st.get('busy')}` agent `{st.get('agent_running')}`"
        ) + worker_text

    async def _post(self, channel_id: str, message: str, *, root_id: str | None) -> dict[str, Any]:
        return await self.client.create_post(channel_id, message, root_id=root_id)

    async def _request_restart(
        self,
        *,
        thread_id: str,
        channel_id: str,
        user_id: str,
        reason: str,
    ) -> str:
        rec = await self.sessions.store.get(thread_id)
        pending = now_pending(
            thread_id=thread_id,
            session_id=rec.session_id if rec else None,
            user_id=user_id,
            reason=reason,
            team_id=self.config.team_id,
            parent_channel_id=channel_id,
        )

        async def _cancel() -> None:
            if self.sessions.is_busy(thread_id):
                try:
                    await self.sessions.cancel(thread_id)
                except Exception:
                    logger.exception("cancel before restart failed")

        return await restart_gateway(
            pending=pending,
            db_path=self.config.db_path,
            stop_coro=self.sessions.broker.stop,
            cancel_coro=_cancel,
            unit="mattermost-acp-gateway.service",
            track=self._track_control,
        )

    async def _handle_pending_restart_bump(self) -> None:
        pending = consume_pending(pending_path(self.config.db_path))
        if pending is None or pending.thread_id is None:
            return
        sid = pending.session_id
        resume_note = "no prior session map"
        try:
            sid = await self.sessions.ensure_session(pending.thread_id)
            resume_note = f"session `{sid}` resumed"
        except Exception as exc:
            logger.exception("Post-restart ensure_session failed")
            resume_note = f"session warm failed: {exc}"
        body = (
            f"✅ **mattermost-acp-gateway** back online\n"
            f"• trigger: `{pending.reason}`\n"
            f"• harness `{self.config.harness}` model `{self.config.grok_model or '—'}`\n"
            f"• {resume_note}\n"
            f"• downtime ≈ {pending.age_s:.0f}s"
        )
        channel_id = pending.parent_channel_id
        if not channel_id:
            logger.warning("pending restart missing channel; skip bump")
            return
        try:
            await self.client.create_post(channel_id, body, root_id=pending.thread_id)
        except MattermostError:
            logger.exception("Failed to post restart bump")
