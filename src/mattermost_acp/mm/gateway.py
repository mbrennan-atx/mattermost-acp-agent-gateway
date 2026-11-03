"""Mattermost user loop: thread → harness session, slash verbs, status posts."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from mattermost_acp import harness_switch
from mattermost_acp.acp import AcpError, SessionUpdate
from mattermost_acp.attachments import (
    RemoteFile,
    format_attachment_note,
    save_files,
)
from mattermost_acp.channel_context import load_role_text, wrap_user_prompt
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
        asyncio.create_task(self._handle_pending_restart_bump(), name="post-restart-bump")
        logger.info(
            "Gateway online as %s (%s) harness=%s home=%s peer=%s",
            self.client.username,
            self.client.user_id,
            self.config.harness,
            "ALL" if self.config.all_channels else sorted(self.config.channel_ids),
            self.config.peer_channel_id or "-",
        )
        await self._ensure_peer_membership()
        if self.config.auto_join_channels:
            asyncio.create_task(self._join_all_public_channels(), name="auto-join")
        async for payload in self.client.events(stop=self._stop):
            event = payload.get("event")
            if event == "channel_created":
                if self.config.auto_join_channels:
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
        for report in self._reports.values():
            if report.get("task"):
                report["task"].cancel()
        self._reports.clear()

    def _observe_background_update(self, update: SessionUpdate) -> None:
        report = self._reports.get(update.session_id)
        if report is None or report["active"] or report.get("suppressed"):
            return
        if update.kind not in {
            "agent_message_chunk",
            "tool_call",
            "tool_call_update",
            "subagent_activity",
        }:
            return
        report["reporter"].observe(update)
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
        for session_id, report in list(self._reports.items()):
            if report["thread_id"] == thread_id:
                report["suppressed"] = True
                if report.get("task"):
                    report["task"].cancel()
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

    async def handle_slash(self, req: SlashRequest) -> dict[str, str]:
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
        asyncio.create_task(
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

    async def dispatch_command(
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
            ok = await self.sessions.cancel(thread_id)
            await reply("⏹ Cancelled." if ok else "Nothing to cancel.")
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

    async def run_prompt(
        self,
        *,
        thread_id: str,
        channel_id: str,
        prompt: str,
        author_id: str,
        author_name: str,
        passthrough: bool = False,
        has_attachments: bool = False,
    ) -> None:
        if self.sessions.is_busy(thread_id):
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
        try:
            await self.sessions.ensure_session(thread_id, channel_id=channel_id, route=route)
        except Exception:
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
                reporter.observe(update)
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
        if previous and previous.get("task"):
            previous["task"].cancel()
        report = {
            "reporter": reporter,
            "thread_id": thread_id,
            "channel_id": channel_id,
            "active": True,
            "task": None,
        }
        if session_id:
            self._reports[session_id] = report
        progress_task = asyncio.create_task(
            self._publish_progress(report), name=f"progress-{thread_id}"
        )
        report["task"] = progress_task
        try:
            await _publish_status(status.render(), force=True)
            try:
                result = await self.sessions.run_turn(
                    thread_id, text, channel_id=channel_id, on_update=on_update, route=route
                )
            finally:
                report["active"] = False
                if not reporter.has_pending_work():
                    progress_task.cancel()
                    try:
                        await progress_task
                    except asyncio.CancelledError:
                        pass
        except AcpError as exc:
            logger.warning("ACP error thread=%s: %s", thread_id, exc)
            await _publish_status(status.render(phase="failed"), force=True)
            await self._post(channel_id, f"❌ ACP error: {exc}", root_id=thread_id)
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
            return

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
        busy = self.sessions.busy_threads
        wait_note = f" after {len(busy)} running turn(s) finish" if busy else ""
        await reply(
            f"🔀 `{plan.old_harness}` → `{plan.new_harness}` (model `{plan.model}`). "
            f"Restarting{wait_note}."
        )
        asyncio.create_task(
            self._restart_when_idle(thread_id, channel_id, user_id),
            name="harness-switch-restart",
        )

    async def _restart_when_idle(
        self, thread_id: str, channel_id: str, user_id: str, *, timeout_s: float = 900
    ) -> None:
        deadline = time.monotonic() + timeout_s
        while self.sessions.busy_threads and time.monotonic() < deadline:
            await asyncio.sleep(5)
        if self.sessions.busy_threads:
            await self._post(
                channel_id,
                "⏳ Still busy after 15 minutes. The new harness is saved; `/restart` when ready.",
                root_id=thread_id,
            )
            return
        msg = await self._request_restart(
            thread_id=thread_id,
            channel_id=channel_id,
            user_id=user_id,
            reason="harness_switch",
        )
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
