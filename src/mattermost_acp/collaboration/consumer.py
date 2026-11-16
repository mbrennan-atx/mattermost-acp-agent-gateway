"""Route durable project events into one independent gateway's native sessions."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import shlex
import sys
import time
import uuid
from pathlib import Path

import aiohttp

from mattermost_acp.acp.broker import NativeDispatchChanged, native_command_admission
from mattermost_acp.collaboration.client import HubClient, HubError, Settings
from mattermost_acp.collaboration.client import elapsed_clock as receipt_elapsed_clock
from mattermost_acp.mm.chunk import DEFAULT_LIMIT
from mattermost_acp.mm.client import MattermostError

logger = logging.getLogger(__name__)
DEADLINE_RECHECK_SECONDS = 1.0


def elapsed_clock():
    return receipt_elapsed_clock(time)


class CollaborationConsumer:
    def __init__(self, gateway, settings: Settings):
        self.gateway = gateway
        self.instance = uuid.uuid4().hex
        self.task = None
        self.workers: dict[tuple[str, str | None], asyncio.Task] = {}
        self.worker_projects = {}
        self.handling = {}
        self.closing = False
        self.ownership = {}
        self.ownership_threads = {}
        self.ownership_items = {}
        self.project_deadlines = {}
        self.deadline_tasks = {}
        self.deadline_stops = set()
        self.settled_projects = set()
        self.deadline_cleanup_cursor = 0
        self.rejected_reservations = {}
        self.pending_execution_restores = {}
        self.reserving = set()
        self.cancelled_claims = set()
        self.claiming = False
        self.claim_settled = asyncio.Event()
        self.claim_settled.set()
        self.claim_uncertain = False
        self.recovery_pending = True
        self.inherited_claims = []
        self.recovery_lock = asyncio.Lock()
        self.admission_pending = 0
        self.admission_lock = asyncio.Lock()
        self.poll_paused = False
        self.actor = ""
        self.sidecar = getattr(gateway.config, "collab_sidecar", False)
        self.stops_path = Path(
            getattr(
                gateway.config,
                "db_path",
                Path.home()
                / ".local/state/mattermost-acp-collaboration"
                / hashlib.sha256(settings.token.encode()).hexdigest()[:16],
            )
        )
        self.stops_path = self.stops_path.with_name(self.stops_path.name + ".stops.json")
        self.pending_stops = (
            json.loads(self.stops_path.read_text()) if self.stops_path.exists() else {}
        )
        if self.pending_stops == []:
            self.pending_stops = {}
        if not isinstance(self.pending_stops, dict) or any(
            not isinstance(thread, str) or not self.valid_stop_intent(stop)
            for thread, stop in self.pending_stops.items()
        ):
            raise ValueError("Pending project stops require explicit recorded boundaries")
        self.hub = HubClient(settings)

    def start(self):
        self.task = asyncio.create_task(self.run(), name="collaboration-mailbox")

    async def close(self):
        if self.closing:
            return
        self.closing = True
        tasks = [
            task
            for task in [self.task, *self.workers.values(), *self.deadline_tasks.values()]
            if task is not None
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.hub.close()

    async def run(self):
        while not self.closing:
            try:
                await self.reconcile_inherited_claims()
                for thread in list(self.pending_stops):
                    try:
                        await self.cancel_thread(thread, retry=True)
                    except Exception:
                        # An unresolved capture remains fenced, but must not
                        # block unrelated ownership and mailbox processing.
                        logger.warning("Project stop unresolved; recorded intent retained")
                await self.reconcile_ownership()
                await self.reconcile_deadlines()
                if self.gateway.sealed or self.poll_paused:
                    await asyncio.sleep(0.25)
                    continue
                controls = "&controls_only=1" if self.gateway.draining else ""
                self.claiming = True
                self.claim_settled.clear()
                try:
                    wait = 0 if self.gateway.draining else 20
                    item = await self.hub.request(
                        "GET", f"/mailbox?instance={self.instance}&wait={wait}" + controls
                    )
                except BaseException:
                    self.claim_uncertain = True
                    raise
                if item:
                    task = asyncio.create_task(
                        self.handle(item), name="collaboration-" + item["id"]
                    )
                    key = (item["id"], item.get("claim"))
                    self.workers[key] = task
                    self.worker_projects[key] = item["project"]
                    task.add_done_callback(
                        lambda completed, key=key: self.finish_worker(key, completed)
                    )
                self.claiming = False
                self.claim_settled.set()
                if self.gateway.draining:
                    await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                self.claiming = False
                self.claim_settled.set()
                raise
            except Exception:
                self.claiming = False
                self.claim_settled.set()
                logger.warning("Collaboration mailbox unavailable; durable requests retained")
                await asyncio.sleep(2)

    async def reconcile_inherited_claims(self):
        """Read older execution debt without taking over or replaying it."""
        async with self.recovery_lock:
            await self._reconcile_inherited_claims()

    async def _reconcile_inherited_claims(self):
        self.recovery_pending = True
        if not self.actor:
            identity = await self.hub.request("GET", "/identity")
            if self.sidecar and (
                identity["actor"] != getattr(self.gateway.config, "collab_origin_actor", "")
                or identity.get("user_id") != self.gateway.client.user_id
            ):
                raise RuntimeError("Collaboration actor does not match the sidecar identity")
            if bool(identity.get("sidecar")) != self.sidecar:
                raise RuntimeError("Hub and consumer delivery modes do not match")
            self.actor = identity["actor"]
        claims = await self.hub.request("GET", "/claims")
        if not isinstance(claims, list) or any(
            not isinstance(item, dict)
            or not isinstance(item.get("id"), str)
            or item.get("recipient") != self.actor
            or not isinstance(item.get("evidence", {}), dict)
            for item in claims
        ):
            raise RuntimeError("Actor execution recovery evidence is unavailable")
        inherited = [item for item in claims if item.get("instance") != self.instance]
        if any(
            item.get("evidence", {}).get("session_id")
            and item.get("evidence", {}).get("dispatch_started") is not None
            for item in inherited
        ):
            mark_unknown = getattr(
                self.gateway.sessions.broker, "mark_native_boundary_unknown", None
            )
            if mark_unknown is not None:
                await mark_unknown()
        self.inherited_claims = inherited
        for report in self.gateway.live_reports():
            if report.get("preparing") and self.inherited_thread_pending(report.get("thread_id")):
                # Native dispatch rechecks cancel_requested at its write fence.
                # Only refuse this provisional turn; never cancel its predecessor.
                report["cancel_requested"] = True
        self.recovery_pending = False
        self.gateway.collaboration_recovery_pending = False
        for item in claims:
            self.observe_deadline(item)

    async def set_admission(self, mode):
        """Fence hub claims before settling local mailbox uncertainty."""
        self.admission_pending += 1
        try:
            async with self.admission_lock:
                self.poll_paused = True
                async with asyncio.timeout(3):
                    await self.reconcile_inherited_claims()
                # An unstarted consumer has never submitted a mailbox request.
                if self.task is None and not self.claim_uncertain:
                    return
                async with asyncio.timeout(3):
                    claims = await self.hub.request(
                        "POST", "/mailbox/admission", {"instance": self.instance, "mode": mode}
                    )
                    if mode != "normal":
                        await self.claim_settled.wait()
                        for item in claims:
                            key = (item["id"], item.get("claim"))
                            if (
                                key in self.workers
                                or key in self.ownership
                                or key in self.reserving
                            ):
                                continue
                            current = await self.hub.request("GET", "/messages/" + item["id"])
                            if current.get("claim") != key[1] or (
                                current["state"]
                                not in {
                                    "running",
                                    "cancelling",
                                    "unconfirmed",
                                    "interrupted",
                                }
                                and not current.get("execution_pending")
                            ):
                                continue
                            if current["state"] not in {"running", "cancelling"} or current.get(
                                "evidence", {}
                            ).get("session_id"):
                                raise RuntimeError("Mailbox claim requires native reconciliation")
                            if any(
                                field in current.get("evidence", {})
                                for field in ("post_started", "result_post_id")
                            ):
                                raise RuntimeError(
                                    "Mailbox claim requires external-post reconciliation"
                                )
                            await self.update(current, "queued")
                        self.claim_uncertain = False
        except BaseException:
            self.claim_uncertain = True
            raise
        finally:
            self.poll_paused = False
            self.admission_pending -= 1

    def finish_worker(self, key, completed):
        if self.workers.get(key) is completed:
            self.workers.pop(key)
            pid = self.worker_projects.pop(key, None)
            if pid:
                self.evict_deadline(pid)

    async def update(self, item, state, result="", evidence=None, *, expected_execution_id=None):
        if expected_execution_id is None and state not in {"dispatch", "post_dispatch"}:
            if state != "heartbeat" or evidence:
                known = item.get("reservation_evidence", {}).get("execution_id") or item.get(
                    "evidence", {}
                ).get("execution_id")
                if known is not None:
                    expected_execution_id = (evidence or {}).get("execution_id") or known
        body = {
            "instance": self.instance,
            "state": state,
            "result": result,
            "evidence": evidence or {},
            "claim": item.get("claim"),
        }
        if expected_execution_id is not None:
            body["expected_execution_id"] = expected_execution_id
        receipt = await self.hub.request(
            "POST",
            f"/messages/{item['id']}/update",
            body,
        )
        self.observe_deadline(receipt)
        return receipt

    def observe_deadline(self, item):
        pid = item.get("project")
        if not isinstance(pid, str):
            return
        if (
            item.get("project_admission_closed") is True
            and item.get("project_execution_pending") is False
        ):
            self.settled_projects.add(pid)
            if self.evict_deadline(pid):
                return
        deadline = item.get("local_project_deadline")
        remaining = item.get("project_remaining_seconds")
        if deadline is None and type(remaining) in {int, float} and math.isfinite(remaining):
            deadline = elapsed_clock() + max(0, remaining)
        if (
            not isinstance(pid, str)
            or type(deadline) not in {int, float}
            or not math.isfinite(deadline)
        ):
            return
        prior = self.project_deadlines.get(pid, float("inf"))
        self.project_deadlines[pid] = min(prior, deadline)
        task = self.deadline_tasks.get(pid)
        if deadline < prior and task is not None and task is not asyncio.current_task():
            task.cancel()
        if task is None or task.done() or task.cancelling():
            # Cancellation after the deadline must not inherit a work admission guard.
            with native_command_admission(None):
                self.deadline_tasks[pid] = asyncio.create_task(
                    self.enforce_deadline(pid), name="project-deadline-" + pid
                )

    def before_deadline(self, item):
        self.observe_deadline(item)
        return item.get("project_admission_closed") is not True and elapsed_clock() < (
            self.project_deadlines.get(item.get("project"), float("inf"))
        )

    def evict_deadline(self, pid):
        if (
            pid not in self.settled_projects
            or self.recovery_pending
            or pid in self.handling.values()
            or pid in self.worker_projects.values()
            or any(item.get("project") == pid for item in self.ownership_items.values())
            or any(item.get("project") == pid for item in self.inherited_claims)
        ):
            return False
        task = self.deadline_tasks.pop(pid, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        self.project_deadlines.pop(pid, None)
        self.hub.deadlines.pop(pid, None)
        self.deadline_stops = {stop for stop in self.deadline_stops if stop[0] != pid}
        self.settled_projects.discard(pid)
        return True

    async def refresh_budget(self, pid):
        try:
            async with asyncio.timeout(3):
                receipt = await self.hub.request("GET", f"/projects/{pid}/budget")
                self.observe_deadline(receipt)
        except Exception:
            logger.warning("Project deadline reconciliation unavailable; budget retained")

    async def reconcile_deadlines(self):
        for pid in list(self.settled_projects):
            self.evict_deadline(pid)
        pending = [
            pid
            for pid, deadline in self.project_deadlines.items()
            if deadline <= elapsed_clock()
            and (pid not in self.deadline_tasks or self.deadline_tasks[pid].done())
        ]
        if pending:
            start = self.deadline_cleanup_cursor % len(pending)
            selected = (pending[start:] + pending[:start])[:4]
            self.deadline_cleanup_cursor = start + len(selected)
            for pid in selected:
                await self.refresh_budget(pid)

    async def deadline_work(self, item, operation):
        try:
            with native_command_admission(
                lambda: self.before_deadline(item) and not self.stop_pending(item)
            ):
                return await operation
        finally:
            if (not self.before_deadline(item) or self.stop_pending(item)) and hasattr(
                operation, "close"
            ):
                operation.close()

    async def enforce_deadline(self, pid):
        while pid in self.project_deadlines:
            remaining = self.project_deadlines[pid] - elapsed_clock()
            if remaining <= 0:
                break
            # asyncio timers exclude suspension. Recheck BOOTTIME within one
            # second of event-loop progress after the host resumes.
            await asyncio.sleep(min(DEADLINE_RECHECK_SECONDS, remaining))
        if pid not in self.project_deadlines:
            return
        handled = set()
        for key, owned in list(self.ownership.items()):
            item = self.ownership_items.get(key, {})
            if item.get("project") != pid:
                continue
            self.cancelled_claims.add(key)
            if owned.get("collaboration_project") != pid:
                continue
            owned["cancel_requested"] = True
            if id(owned) in handled or not owned.get("native_dispatched"):
                continue
            binding = item.get("binding", {})
            if (
                item.get("recipient") == item.get("project_owner")
                and not item.get("project_owner_in_discussion")
                and binding.get("thread_id") == item.get("origin_thread_id")
                and binding.get("channel_id") == item.get("origin_channel_id")
            ):
                continue
            proof = item.get("reservation_evidence", item.get("evidence", {}))
            session, execution, epoch = (
                proof.get(name) for name in ("session_id", "execution_id", "native_epoch")
            )
            if (
                not session
                or not execution
                or epoch is None
                or owned.get("execution_id") != execution
            ):
                continue

            def still_owned(
                key=key,
                owned=owned,
                session=session,
                execution=execution,
                epoch=epoch,
                thread=binding["thread_id"],
            ):
                return (
                    self.ownership.get(key) is owned
                    and self.gateway._reports.get(session) is owned
                    and owned.get("execution_id") == execution
                    and owned.get("collaboration_project") == pid
                    and key not in self.reserving
                    and getattr(self.gateway.sessions, "native_epoch", lambda _thread: 0)(thread)
                    == epoch
                )

            if not still_owned():
                continue
            stop = (pid, session, execution, epoch)
            if stop in self.deadline_stops:
                continue
            # Timer replacement and other expiry observers can overlap. Keep
            # the original bounded stop attempt uncertain rather than repeat it.
            self.deadline_stops.add(stop)
            handled.add(id(owned))
            await self.stop_owned_async_tasks(
                binding["thread_id"],
                owned["reporter"],
                session=session,
                epoch=epoch,
                guard=still_owned,
            )
            try:
                async with asyncio.timeout(3):
                    await self.gateway.sessions.cancel(
                        binding["thread_id"],
                        expected_session=session,
                        expected_epoch=epoch,
                        guard=still_owned,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                # A refused/ambiguous stop cannot prove that native work ended.
                logger.warning("Project deadline reached; native cancellation remains unresolved")
        await self.refresh_budget(pid)

    async def stop_owned_async_tasks(
        self, thread, reporter, *, session, epoch, guard, stopped=None
    ):
        # Child stop failures must leave time for an independent guarded parent
        # cancellation. Acknowledgements do not establish native termination.
        try:
            async with asyncio.timeout(3):
                if not hasattr(reporter, "stoppable_async_tasks"):
                    return
                for task_id in reporter.stoppable_async_tasks():
                    if stopped is not None and task_id in stopped:
                        continue

                    def task_owned(task_id=task_id):
                        return guard() and reporter.owns_async_task(task_id)

                    try:
                        accepted = await self.gateway.sessions.stop_async_task(
                            thread,
                            task_id,
                            expected_session=session,
                            expected_epoch=epoch,
                            guard=task_owned,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.warning("Native background task cancellation remains unresolved")
                    else:
                        if accepted and stopped is not None:
                            stopped.add(task_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Native background task cancellation remains unresolved")

    def active(self, thread):
        if self.gateway.sessions.is_busy(thread):
            return True
        if self.owns_thread(thread):
            return True
        return any(
            report["thread_id"] == thread
            and (report.get("active") or report["reporter"].has_pending_work())
            for report in self.gateway.live_reports()
        )

    def owns_thread(self, thread):
        return (
            self.inherited_thread_pending(thread)
            or thread in self.pending_stops
            or thread in self.ownership_threads.values()
            or any(
                report and report.get("thread_id") == thread for report in self.ownership.values()
            )
        )

    def inherited_thread_pending(self, thread):
        return any(
            item.get("binding", {}).get("thread_id") in {None, thread}
            for item in self.inherited_claims
        )

    def stop_pending(self, item):
        return any(
            thread in self.pending_stops
            for thread in (
                item.get("binding", {}).get("thread_id"),
                item.get("origin_thread_id"),
            )
        )

    def terminal_matches(self, key, reserved, terminal):
        if (
            not terminal
            or not reserved.get("session_id")
            or (reserved.get("reservation_id") and not reserved.get("execution_id"))
            or terminal.get("session_id") != reserved["session_id"]
            or terminal.get("execution_id") != reserved.get("execution_id")
            or type(terminal.get("completed_at")) not in {int, float}
            or not math.isfinite(terminal["completed_at"])
            or type(reserved.get("dispatch_started")) not in {int, float}
            or not math.isfinite(reserved["dispatch_started"])
        ):
            return False
        token = reserved.get("reservation_id")
        if not token:
            # Legacy reservations have no independent ordering evidence.
            return terminal["completed_at"] >= reserved["dispatch_started"]
        if terminal.get("reservation_id", token) != token or terminal.get(
            "native_epoch", reserved.get("native_epoch")
        ) != reserved.get("native_epoch"):
            return False
        report = self.ownership.get(key) or {}
        before = report.get("reservation_terminals", {}).get(token)
        if before is not None:
            previous, snapshot = before
            if terminal is previous and (
                terminal.get("completed_at") == snapshot.get("completed_at")
                and not (snapshot.get("pending") and not terminal.get("pending"))
            ):
                return False
        return True

    def release_ownership(self, key):
        self.rejected_reservations.pop(key, None)
        self.pending_execution_restores.pop(key, None)
        self.cancelled_claims.discard(key)
        report = self.ownership.pop(key, None)
        item = self.ownership_items.pop(key, {})
        pid = item.get("project")
        if pid:
            self.evict_deadline(pid)
        thread = self.ownership_threads.pop(key, None) or (report or {}).get("thread_id")
        self.gateway.prune_reports()
        if thread and not self.owns_thread(thread) and not self.gateway.background_pending(thread):
            self.gateway._last_terminal.pop(thread, None)

    async def reconcile_ownership(self):
        for key in list(self.ownership):
            if key in self.workers or key in self.reserving:
                continue
            state = await self.hub.request("GET", "/messages/" + key[0])
            if state.get("claim") != key[1]:
                continue
            if state["state"] not in {"done", "failed", "blocked", "cancelled"}:
                continue
            await self.settle_ownership(key, state)

    def ownership_pending(self, key, terminal=None):
        report = self.ownership.get(key)
        thread = self.ownership_threads.get(key) or (report or {}).get("thread_id")
        reporter = (report or {}).get("reporter")
        return bool(
            (terminal or {}).get("pending")
            or (
                report is not None
                and (
                    report.get("active")
                    or not report.get("foreground_complete", not report.get("active"))
                    or (reporter is not None and reporter.has_pending_work())
                    or self.gateway.sessions.is_busy(thread)
                )
            )
        )

    async def settle_ownership(self, key, state):
        if state.get("claim") != key[1] or key in self.reserving:
            return
        if self.settle_rejected_reservation(key, state):
            return
        report = self.ownership.get(key)
        if key in self.pending_execution_restores:
            proof = self.pending_execution_restores[key]
            self.reserving.add(key)
            try:
                await self.update(state, "execution_resumed", evidence=proof)
                if self.pending_execution_restores.get(key) is proof:
                    self.pending_execution_restores.pop(key, None)
            finally:
                self.reserving.discard(key)
        thread = self.ownership_threads.get(key) or (report or {}).get("thread_id")
        terminal = self.gateway._last_terminal.get(thread)
        evidence = state.get("evidence", {})
        cancelled_retry = (
            state["state"] == "cancelled"
            and state.get("instance") is None
            and bool(state.get("reconciliation"))
        )
        if cancelled_retry and report is not None and terminal:
            evidence = next(
                (
                    prior
                    for prior in reversed(state.get("delivery_history", []))
                    if prior.get("execution_id") == report.get("execution_id")
                    and prior.get("session_id") == terminal.get("session_id")
                ),
                evidence,
            )
        if (
            report is not None
            and report.get("foreground_complete")
            and not self.ownership_pending(key, terminal)
            and terminal
            and self.terminal_matches(key, evidence, terminal)
            and terminal.get("native_dispatched", True)
        ):
            if not cancelled_retry:
                self.reserving.add(key)
                try:
                    try:
                        await self.update(
                            state, "execution_settled", evidence=self.foreground(state, terminal)
                        )
                    finally:
                        if (
                            self.ownership.get(key) is report
                            and report.get("execution_id") == evidence.get("execution_id")
                            and self.ownership_pending(key, self.gateway._last_terminal.get(thread))
                        ):
                            proof = {
                                "session_id": evidence["session_id"],
                                "execution_id": evidence["execution_id"],
                                "native_dispatched": True,
                                "pending": True,
                            }
                            self.pending_execution_restores[key] = proof
                            await self.update(state, "execution_resumed", evidence=proof)
                            if self.pending_execution_restores.get(key) is proof:
                                self.pending_execution_restores.pop(key, None)
                finally:
                    self.reserving.discard(key)
            if (
                self.ownership.get(key) is report
                and key not in self.reserving
                and not self.ownership_pending(key, self.gateway._last_terminal.get(thread))
            ):
                self.release_ownership(key)

    def settle_rejected_reservation(self, key, state):
        attempt = self.rejected_reservations.get(key)
        if (
            attempt is None
            or state.get("claim") != key[1]
            or state.get("instance") != self.instance
            or state["state"] not in {"done", "failed", "blocked", "cancelled"}
        ):
            return False
        reserved = state.get("evidence", {})
        has_execution = bool(
            state.get("execution_pending", reserved.get("dispatch_started") is not None)
        )
        prior = attempt["prior_owned"]
        prior_matches = (
            prior is not None
            and reserved.get("session_id") == attempt["session"]
            and reserved.get("execution_id") == prior.get("execution_id")
        )
        attempt_matches = reserved.get("session_id") == attempt["session"] and reserved.get(
            "execution_id"
        ) == attempt["report"].get("execution_id")
        if (not has_execution or prior_matches or not attempt_matches) and self.ownership.get(
            key
        ) is attempt["report"]:
            # Roll back the provisional mapping without discarding an earlier
            # execution owner or its terminal receipts.
            if prior is None:
                self.ownership.pop(key, None)
                self.ownership_items.pop(key, None)
            else:
                self.ownership[key] = prior
                if attempt["prior_item"] is None:
                    self.ownership_items.pop(key, None)
                else:
                    self.ownership_items[key] = attempt["prior_item"]
            if attempt["prior_thread"] is None:
                self.ownership_threads.pop(key, None)
            else:
                self.ownership_threads[key] = attempt["prior_thread"]
            if attempt["prior_reservation"] is None:
                attempt["item"].pop("reservation_evidence", None)
            else:
                attempt["item"]["reservation_evidence"] = attempt["prior_reservation"]
            self.gateway.prune_reports()
        self.rejected_reservations.pop(key, None)
        return not has_execution

    async def continuation(self, item):
        parent_id = item.get("continuation_parent")
        if item["kind"] != "result" or not parent_id:
            return None
        parent = await self.hub.request("GET", "/messages/" + parent_id)
        key = (parent_id, item.get("continuation_claim"))
        report = self.ownership.get(key)
        thread = item["binding"]["thread_id"]
        terminal = self.gateway._last_terminal.get(thread)
        evidence = parent.get("evidence", {})
        if (
            report is None
            or parent.get("claim") != key[1]
            or parent.get("instance") != self.instance
            or item.get("continuation_instance") != self.instance
            or parent["state"] != "running"
            or report.get("collaboration_project") != item["project"]
            or report.get("thread_id") != thread
            or not terminal
            or terminal.get("execution_id") != evidence.get("execution_id")
            or terminal.get("session_id") != evidence.get("session_id")
            or terminal.get("stop_reason") != "end_turn"
            or terminal.get("pending")
        ):
            return None
        related = []
        for owned_key, owned_report in list(self.ownership.items()):
            if self.ownership_threads.get(owned_key) != thread:
                continue
            if owned_report is not report:
                return None
            state = await self.hub.request("GET", "/messages/" + owned_key[0])
            if state["project"] != item["project"] or state.get("claim") != owned_key[1]:
                return None
            related.append(state)
        info = {
            "report": report,
            "parents": related,
            "session": evidence["session_id"],
            "thread": thread,
        }
        return info if self.continuation_idle(info) else None

    async def unresolved_parent(self, item):
        parent_id = item.get("continuation_parent")
        if item["kind"] != "result" or not parent_id:
            return False
        parent = await self.hub.request("GET", "/messages/" + parent_id)
        evidence = parent.get("evidence", {})
        return parent["state"] not in {"done", "failed", "blocked", "cancelled"} or bool(
            parent.get(
                "execution_pending",
                evidence.get("session_id") and evidence.get("dispatch_started") is not None,
            )
        )

    async def reserve_independent(self, item, session):
        if await self.unresolved_parent(item):
            return False
        return await self.reserve(item, session)

    def continuation_idle(self, info, *, reserved=False):
        report = info["report"]
        return (
            not self.inherited_thread_pending(info["thread"])
            and not self.gateway.sessions.is_busy(info["thread"])
            and not report.get("active")
            and report.get("foreground_complete")
            and not report["reporter"].has_pending_work()
            and all(
                self.ownership.get((parent["id"], parent.get("claim"))) is report
                and (reserved or (parent["id"], parent.get("claim")) not in self.reserving)
                and (parent["id"], parent.get("claim")) not in self.pending_execution_restores
                and (parent["id"], parent.get("claim")) not in self.cancelled_claims
                for parent in info["parents"]
            )
        )

    async def reserve_continuation(self, item, session, info):
        keys = {(parent["id"], parent.get("claim")) for parent in info["parents"]}
        self.reserving.update(keys)
        if session != info["session"]:
            raise NativeDispatchChanged("Continuation native session changed")
        if not await self.reserve(item, session):
            return False
        report = self.ownership[(item["id"], item.get("claim"))]
        epoch = item["native_epoch"]
        for parent in info["parents"]:
            key = (parent["id"], parent.get("claim"))
            if self.ownership.get(key) is not info["report"]:
                raise NativeDispatchChanged("Continuation ownership changed")
            self.ownership[key] = report
            self.ownership_items[key] = {
                **self.ownership_items.get(key, parent),
                "reservation_evidence": {
                    "session_id": session,
                    "execution_id": report["execution_id"],
                    "native_epoch": epoch,
                    "reservation_id": item["reservation_evidence"]["reservation_id"],
                },
            }
            evidence = {
                "session_id": session,
                "execution_id": report["execution_id"],
                "native_epoch": epoch,
                "reservation_id": item["reservation_evidence"]["reservation_id"],
            }
            try:
                receipt = await self.update(parent, "dispatch", evidence=evidence)
            except HubError:
                settled = await self.hub.request("GET", "/messages/" + parent["id"])
                if settled.get("claim") == key[1] and settled["state"] in {
                    "done",
                    "failed",
                    "blocked",
                    "cancelled",
                }:
                    report["cancel_requested"] = True
                    continue
                raise
            if receipt["state"] == "cancelling":
                await self.update(
                    parent,
                    "heartbeat",
                    evidence=evidence,
                    expected_execution_id=receipt.get("evidence", {}).get("execution_id"),
                )
            if receipt["state"] != "running" or key in self.cancelled_claims:
                report["cancel_requested"] = True
        return not report.get("cancel_requested")

    async def rollback_continuation(self, info):
        old = info["report"]
        for parent in info["parents"]:
            key = (parent["id"], parent.get("claim"))
            owned = self.ownership.get(key)
            if owned is None:
                continue
            current = await self.hub.request("GET", "/messages/" + parent["id"])
            if current.get("claim") != key[1] or current.get("instance") != self.instance:
                continue
            if current.get("evidence", {}).get("execution_id") not in {
                owned.get("execution_id"),
                parent.get("evidence", {}).get("execution_id"),
            }:
                raise NativeDispatchChanged("Continuation rollback ownership changed")
            evidence = dict(parent.get("evidence", {}))
            evidence["native_epoch"] = getattr(self.gateway.sessions, "native_epoch", lambda _t: 0)(
                info["thread"]
            )
            # Preparation can advance the session epoch before any ownership
            # transfer. Restore its cancellation fence even in that case, and
            # retain terminal parents until their restored native work settles.
            state = (
                "evidence"
                if current["state"] in {"done", "failed", "blocked", "cancelled"}
                else "heartbeat"
            )
            await self.update(
                current,
                state,
                evidence=evidence,
                expected_execution_id=current.get("evidence", {}).get("execution_id"),
            )
            if self.ownership.get(key) is owned:
                self.ownership[key] = old
                self.ownership_items[key] = {
                    **self.ownership_items.get(key, parent),
                    "reservation_evidence": evidence,
                }
        self.gateway.prune_reports()

    async def heartbeat(self, item):
        while True:
            await asyncio.sleep(10)
            try:
                await self.update(item, "heartbeat")
            except HubError as exc:
                if exc.status in {400, 401, 403, 404, 409, 410, 422}:
                    return
                logger.warning("Collaboration lease renewal unavailable")
            except Exception:
                logger.warning("Collaboration lease renewal unavailable")

    async def injected_wait(self, item):
        while True:
            await asyncio.sleep(1)
            state = await self.hub.request("GET", f"/messages/{item['id']}")
            key = (item["id"], item.get("claim"))
            if state.get("claim") != key[1]:
                return
            terminal = self.gateway._last_terminal.get(item["binding"]["thread_id"])
            receipt = state.get("evidence", {})
            if state["state"] not in {"running", "cancelling"}:
                if self.ownership_pending(key, terminal):
                    continue
                if (
                    state["state"] in {"done", "failed", "blocked", "cancelled"}
                    and terminal
                    and self.terminal_matches(key, receipt, terminal)
                ):
                    await self.update(item, "evidence", evidence=self.foreground(item, terminal))
                else:
                    owned = self.ownership.get((item["id"], item.get("claim")))
                    if (
                        owned
                        and not owned.get("foreground_complete", not owned.get("active"))
                        and self.gateway._reports.get(receipt.get("session_id")) is owned
                    ):
                        # A correlated reply can precede the foreground RPC.
                        # Retain the observer until its actual receipt arrives.
                        continue
                return
            if (
                receipt.get("session_id")
                and terminal
                and self.terminal_matches(key, receipt, terminal)
                and (terminal["stop_reason"] != "end_turn" or item["kind"] == "result")
                and not terminal.get("pending")
                and terminal.get("native_dispatched", True)
            ):
                state = {"cancelled": "cancelled", "end_turn": "done"}.get(
                    terminal["stop_reason"], "failed"
                )
                evidence = self.foreground(item, terminal)
                await self.update(
                    item,
                    state,
                    evidence["foreground_answer"]
                    if state == "done"
                    else "Native stop=" + terminal["stop_reason"],
                    evidence,
                )
                return

    def prompt(self, item):
        instructions = (
            "This is authorized collaboration within the project's objective. "
            "Use your existing harness, model and connectors. Your persona role is a specialty; "
            "you may ask project peers for useful help. Preserve the owner's objective and "
            "unrelated work. Do not send acknowledgements or wake peers for status. "
            "Read shared state with `mm-work show " + item["project"] + "`. "
        )
        if item["kind"] in {"ask", "work"}:
            instructions += (
                "Return the substantive result and verification. You MUST submit its result "
                "with `mm-work reply "
                + item["id"]
                + " --claim "
                + item.get("claim", "")
                + " --text '<result and evidence>'`; a normal final answer alone cannot identify "
                "which injected request it answers. "
                "If you launch background work, submit that reply when all required work finishes. "
                "Use --state blocked or failed when appropriate. "
                "Further delegation uses `mm-work ask "
                + item["project"]
                + " <recipient> --parent "
                + item["id"]
                + " --text '<specific request>'`. "
                "Do not delegate back to anyone on the explicit parent chain. "
                "If you need information from an ancestor, return a blocked result with the "
                "missing information so the parent can supply it in a new request. "
            )
        else:
            instructions += (
                "This is a peer result for your existing task. Reconcile it with the objective, "
                "then continue authorized work or report the verified outcome. This delivery "
                "requires no acknowledgement or reply to the sender. "
            )
        selected = getattr(self.gateway.config, "collab_cli_command", None)
        command = (
            shlex.quote(selected)
            if selected
            else shlex.quote(sys.executable) + " -m mattermost_acp.collaboration.cli"
        )
        instructions = instructions.replace("mm-work ", command + " ")
        return (
            f"Project collaboration: {item['project_title']}\n"
            f"Project ID: {item['project']}\nRequest ID: {item['id']}\n"
            f"From: {item['sender']}\nKind: {item['kind']}\n"
            f"Claim: {item.get('claim', '')}\nParent request: {item.get('parent') or ''}\n"
            f"Result status: {item.get('result_state', 'pending')}\n"
            f"Evidence: {json.dumps(item.get('result_evidence', item.get('evidence', {})))}\n"
            f"Objective: {item['objective']}\n{instructions}\n\n{item['text']}"
        )

    async def handle(self, item):
        handling_key = (item["id"], item.get("claim"))
        self.handling[handling_key] = item["project"]
        self.observe_deadline(item)
        renewal = asyncio.create_task(self.heartbeat(item))
        try:
            if self.gateway.sealed or (self.gateway.draining and item["kind"] != "cancel"):
                await self.update(item, "queued")
                return
            if item["kind"] == "cancel":
                await self.handle_cancel(item)
                return
            current = await self.hub.request("GET", f"/messages/{item['id']}")
            self.observe_deadline(current)
            if current["state"] == "cancelling":
                await self.update(item, "queued")
                return
            if current["state"] != "running" or current.get("claim") != item.get("claim"):
                return
            if (
                self.sidecar
                and item["kind"] == "result"
                and item["recipient"] == item.get("project_owner")
                and not item.get("depth")
            ):
                await self.post_result(item)
                return
            # A new claim is explicit reconciliation. Old uncertain reservations
            # must not occupy the thread indefinitely after that reconciliation.
            for key in list(self.ownership):
                if key[0] == item["id"] and key[1] != item.get("claim"):
                    self.release_ownership(key)
            binding = item["binding"]
            if self.sidecar and (
                binding["thread_id"] != item.get("project_thread_id")
                or binding["channel_id"] != item.get("project_channel_id")
            ):
                await self.update(
                    item, "failed", "Sidecar work must stay in its project discussion"
                )
                return
            thread = binding["thread_id"]
            if self.stop_pending(item):
                await self.update(item, "queued")
                return
            if self.inherited_thread_pending(thread):
                await self.update(item, "queued")
                return
            # Verify the declared discussion/binding before creating native work.
            post = await self.gateway.client.get_post(thread)
            if post.get("channel_id") != binding["channel_id"] or post.get("root_id"):
                await self.update(
                    item, "failed", "Project binding does not match the thread channel"
                )
                return
            record = await self.gateway.sessions.store.get(thread)
            if (
                binding.get("workspace")
                and record is not None
                and getattr(record, "cwd", binding["workspace"]) != binding["workspace"]
            ):
                await self.update(
                    item, "failed", "Existing native session uses a different workspace"
                )
                return
            broker = self.gateway.sessions.broker
            prompt = self.prompt(item)
            if await self.unresolved_parent(item) and await self.continuation(item) is None:
                await self.update(item, "queued")
                return
            outcome = None
            caps = broker.capabilities
            report = self.gateway._reports.get(record.session_id) if record is not None else None
            is_loaded = getattr(broker, "is_session_loaded", lambda _session: False)
            can_hint = (
                record is not None
                and getattr(record, "harness", None)
                in {
                    None,
                    getattr(self.gateway.sessions.store, "harness", self.gateway.config.harness),
                }
                and broker.is_running
                and is_loaded(record.session_id)
                and caps.steering
                and self.steerable_report(report)
            )
            if can_hint and broker.backend == "codex":
                can_hint = ((caps.raw.get("_meta") or {}).get("steering") or {}).get(
                    "idleBehavior"
                ) == "promptRequired"
            if can_hint:
                if not await self.reserve(item, record.session_id, steering=True):
                    return
                try:
                    outcome = await self.deadline_work(
                        item,
                        self.gateway.sessions.hint(
                            thread,
                            prompt,
                            expected_session=record.session_id,
                            expected_epoch=item["native_epoch"],
                            guard=lambda: (
                                not self.inherited_thread_pending(thread)
                                and is_loaded(record.session_id)
                                and self.steerable_report(report)
                                and self.gateway._reports.get(record.session_id) is report
                                and self.ownership.get((item["id"], item.get("claim"))) is report
                                and report.get("execution_id")
                                == item["reservation_evidence"]["execution_id"]
                                and (item["id"], item.get("claim")) not in self.cancelled_claims
                            ),
                        ),
                    )
                except NativeDispatchChanged:
                    await self.undispatched(item, requeue=True)
                    self.release_ownership((item["id"], item.get("claim")))
                    return
                except Exception:
                    # The adapter may have accepted the injection before a timeout/error.
                    await self.update(item, "unconfirmed", "Native injection was not confirmed")
                    return
            if outcome == "injected":
                await self.update(
                    item,
                    "heartbeat",
                    evidence={"delivery": "injected", "session_id": record.session_id},
                )
                if item["kind"] == "result":
                    await self.update(
                        item,
                        "done",
                        item["text"],
                        {"delivery": "injected", "session_id": record.session_id},
                    )
                else:
                    await self.injected_wait(item)
                return
            if outcome not in {None, "promptRequired"}:
                await self.update(item, "unconfirmed", "Unknown native delivery outcome")
                return
            if outcome == "promptRequired":
                await self.undispatched(item)
            self.release_ownership((item["id"], item.get("claim")))
            continuation = await self.continuation(item)
            if continuation is None and await self.unresolved_parent(item):
                await self.update(item, "queued")
                return
            if self.active(thread) and continuation is None:
                await self.update(item, "queued")
                return
            transfer_keys = {
                (parent["id"], parent.get("claim"))
                for parent in (continuation or {}).get("parents", [])
            }
            self.reserving.update(transfer_keys)
            try:
                result = await self.deadline_work(
                    item,
                    self.gateway.run_prompt(
                        thread_id=thread,
                        channel_id=binding["channel_id"],
                        prompt=prompt,
                        author_id="project:" + item["project"],
                        author_name=item["sender"],
                        workspace=binding.get("workspace"),
                        collaboration_project=item["project"],
                        before_native=(
                            (lambda session: self.reserve_continuation(item, session, continuation))
                            if continuation is not None
                            else (lambda session: self.reserve_independent(item, session))
                        ),
                        ownership_guard=(
                            lambda: self.continuation_idle(continuation, reserved=True)
                        )
                        if continuation is not None
                        else None,
                    ),
                )
                if (
                    continuation is not None
                    and result
                    and (result.get("queued") or result.get("native_dispatched") is False)
                ):
                    await self.rollback_continuation(continuation)
            finally:
                self.reserving.difference_update(transfer_keys)
            # Explicit mm-work reply may already have settled the request.
            current = await self.hub.request("GET", f"/messages/{item['id']}")
            if (
                result
                and (result.get("queued") or result.get("native_dispatched") is False)
                and item.get("reservation_evidence")
                and current.get("claim") == item.get("claim")
                and current.get("execution_pending")
            ):
                current = await self.undispatched(item)
            if current["state"] in {"done", "failed", "blocked", "cancelled"}:
                if result and not result.get("queued"):
                    await self.update(item, "evidence", evidence=self.foreground(item, result))
                    if self.ownership_pending((item["id"], item.get("claim")), result):
                        await self.injected_wait(item)
                return
            if result is None:
                if current.get("execution_pending") is False and current.get("evidence", {}).get(
                    "not_dispatched"
                ):
                    await self.update(item, "queued")
                else:
                    await self.update(item, "interrupted", "No confirmed native terminal receipt")
            elif result.get("queued"):
                await self.update(item, "queued")
            elif result.get("pending"):
                await self.update(item, "heartbeat", evidence=self.foreground(item, result))
                await self.injected_wait(item)
            elif result["stop_reason"] == "cancelled":
                await self.update(item, "cancelled", "Native stop=cancelled", result)
            else:
                state = "done" if result["stop_reason"] == "end_turn" else "failed"
                evidence = self.foreground(item, result)
                if item["kind"] in {"ask", "work"} and state == "done":
                    await self.update(item, "heartbeat", evidence=evidence)
                    await self.injected_wait(item)
                else:
                    await self.update(item, state, evidence["foreground_answer"], evidence)
        except asyncio.CancelledError:
            try:
                await self.update(
                    item, "interrupted", "Gateway stopped; reconcile native execution"
                )
            except Exception:
                pass
            raise
        except HubError:
            logger.warning("Collaboration receipt rejected; shared state remains authoritative")
            await self.wait_for_continuation(item)
        except Exception:
            logger.exception("Collaboration delivery interrupted request=%s", item["id"])
            try:
                await self.update(item, "interrupted", "Delivery failed; reconcile before retry")
            except Exception:
                pass
            await self.wait_for_continuation(item)
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
            cleanup = [(item["id"], item.get("claim"))]
            if item["kind"] == "cancel" and "parent_claim" in item:
                cleanup.append((item["parent"], item["parent_claim"]))
            for mid, claim in cleanup:
                if mid:
                    try:
                        settled = await self.hub.request("GET", f"/messages/{mid}")
                        self.observe_deadline(settled)
                        if settled.get("claim") != claim:
                            continue
                        key = (mid, claim)
                        if settled["state"] == "queued":
                            self.release_ownership(key)
                        elif settled["state"] in {"done", "failed", "blocked", "cancelled"}:
                            await self.settle_ownership(key, settled)
                        if key not in self.ownership:
                            self.cancelled_claims.discard(key)
                    except Exception:
                        pass
            self.handling.pop(handling_key, None)
            self.evict_deadline(item["project"])

    async def wait_for_continuation(self, item):
        if item["kind"] not in {"ask", "work"}:
            return
        key = (item["id"], item.get("claim"))
        original = item.get("reservation_evidence", {}).get("execution_id")
        owned = self.ownership.get(key)
        if (
            original is None
            or owned is None
            or owned.get("execution_id") == original
            or owned.get("collaboration_project") != item["project"]
        ):
            return
        current = await self.hub.request("GET", "/messages/" + item["id"])
        if (
            current.get("claim") == key[1]
            and current.get("instance") == self.instance
            and current.get("evidence", {}).get("execution_id") == owned.get("execution_id")
            and current.get("execution_pending")
        ):
            # This handler still supervises the parent claim and renews its
            # lease while a child-result worker owns the continued execution.
            await self.injected_wait(item)

    @staticmethod
    def foreground(item, result):
        evidence = {key: value for key, value in result.items() if key != "answer"}
        reserved = item.get("reservation_evidence", item.get("evidence", {}))
        if (
            reserved.get("reservation_id")
            and result.get("session_id") == reserved.get("session_id")
            and result.get("execution_id") == reserved.get("execution_id")
        ):
            evidence.setdefault("reservation_id", reserved["reservation_id"])
            evidence.setdefault("native_epoch", reserved.get("native_epoch"))
        answer = result.get("answer") or "No substantive answer"
        if len(answer) > 8000:
            folder = Path.home() / ".local/state/mattermost-acp-collaboration/results"
            folder.mkdir(parents=True, exist_ok=True, mode=0o700)
            execution = hashlib.sha256(str(result.get("execution_id") or "").encode()).hexdigest()
            path = folder / (
                item["id"]
                + "-"
                + str(item.get("claim") or item.get("attempt", 0))
                + "-"
                + execution
                + ".txt"
            )
            path.write_text(answer)
            path.chmod(0o600)
            evidence["result_file"] = str(path)
            answer = answer[:8000] + "\nFull foreground output saved at " + str(path)
        evidence["foreground_answer"] = answer
        return evidence

    async def undispatched(self, item, evidence=None, *, requeue=False):
        receipt = await self.update(
            item,
            "undispatched",
            evidence={
                **(evidence if evidence is not None else item["reservation_evidence"]),
                "native_dispatched": False,
                "not_dispatched": True,
            },
        )
        self.release_ownership((item["id"], item.get("claim")))
        if requeue and receipt["state"] == "running":
            return await self.update(item, "queued")
        return receipt

    @staticmethod
    def steerable_report(report):
        return bool(
            report is not None
            and report.get("execution_id")
            and report.get("native_dispatched") is True
            and not report.get("preparing")
        )

    async def reserve(self, item, session, *, steering=False):
        self.observe_deadline(item)
        if self.stop_pending(item) or self.inherited_thread_pending(item["binding"]["thread_id"]):
            await self.update(item, "queued")
            return False
        report = self.gateway._reports.get(session)
        if (
            report is None
            or not report.get("execution_id")
            or (steering and not self.steerable_report(report))
        ):
            current = await self.hub.request("GET", "/messages/" + item["id"])
            if current.get("claim") == item.get("claim"):
                if current.get("execution_pending") and current["state"] in {
                    "running",
                    "unconfirmed",
                    "cancelling",
                }:
                    await self.update(
                        item,
                        "unconfirmed",
                        "Native execution report is unavailable; reconcile delivery",
                    )
                elif current["state"] in {"running", "cancelling"}:
                    await self.update(item, "queued")
            return False
        epoch = getattr(self.gateway.sessions, "native_epoch", lambda _thread: 0)(
            item["binding"]["thread_id"]
        )
        key = (item["id"], item.get("claim"))
        prior_owned = self.ownership.get(key)
        prior_item = self.ownership_items.get(key)
        prior_thread = self.ownership_threads.get(key)
        prior_reservation = item.get("reservation_evidence")
        self.reserving.add(key)
        self.ownership[key] = report
        self.ownership_items[key] = item
        self.ownership_threads[key] = item["binding"]["thread_id"]
        evidence = {
            "session_id": session,
            "execution_id": report["execution_id"],
            "native_epoch": epoch,
            "reservation_id": uuid.uuid4().hex,
        }
        terminal = self.gateway._last_terminal.get(item["binding"]["thread_id"])
        report.setdefault("reservation_terminals", {})[evidence["reservation_id"]] = (
            (terminal, dict(terminal)) if terminal is not None else None
        )
        item["reservation_evidence"] = evidence
        try:
            try:
                receipt = await self.update(
                    item,
                    "dispatch",
                    evidence=evidence,
                )
            except HubError as exc:
                if exc.status != 409:
                    raise
                self.rejected_reservations[key] = {
                    "report": report,
                    "session": session,
                    "item": item,
                    "prior_owned": prior_owned,
                    "prior_item": prior_item,
                    "prior_thread": prior_thread,
                    "prior_reservation": prior_reservation,
                }
                current = await self.hub.request("GET", "/messages/" + item["id"])
                if self.settle_rejected_reservation(key, current):
                    return False
                raise
            accepted = (
                receipt["state"] == "running"
                and receipt.get("claim") == item.get("claim")
                and key not in self.cancelled_claims
                and not self.inherited_thread_pending(item["binding"]["thread_id"])
                and self.before_deadline(item)
                and not self.stop_pending(item)
            )
            if accepted:
                item["native_epoch"] = epoch
            else:
                if receipt.get("claim") == item.get("claim") and receipt.get("execution_pending"):
                    receipt = await self.undispatched(item, evidence)
                if (
                    receipt.get("claim") == item.get("claim")
                    and not receipt.get("execution_pending")
                    and receipt.get("evidence", {}).get("execution_id") != evidence["execution_id"]
                ):
                    if prior_reservation is None:
                        item.pop("reservation_evidence", None)
                    else:
                        item["reservation_evidence"] = prior_reservation
                if receipt["state"] == "running":
                    await self.update(item, "queued")
                self.release_ownership(key)
            return accepted
        finally:
            self.reserving.discard(key)

    async def handle_cancel(self, item):
        for _ in range(20):
            target = await self.hub.request("GET", f"/messages/{item['parent']}")
            item.setdefault("parent_claim", target.get("claim"))
            if target.get("claim") != item["parent_claim"]:
                await self.update(item, "done", "Cancellation target claim changed")
                return
            execution_only = bool(target.get("execution_cancelling"))
            if target["state"] != "cancelling" and not execution_only:
                await self.update(item, "done", "Project request has settled")
                return
            key = (target["id"], target.get("claim"))
            self.cancelled_claims.add(key)
            owned = self.ownership.get(key)
            if owned and owned.get("collaboration_project") == target["project"]:
                owned["cancel_requested"] = True
            if (target["id"], target.get("claim")) in self.reserving:
                await asyncio.sleep(1)
                continue
            binding = target.get("binding", item["binding"])
            reserved = target.get("evidence", {})
            expected = reserved.get("session_id")
            if (
                expected
                and reserved.get("execution_id")
                and reserved.get("native_dispatched") is False
                and reserved.get("not_dispatched") is True
                and target.get("execution_pending") is False
            ):
                # Revalidate the stored proof against this claim's current
                # reservation, including execution and epoch, before settling it.
                await self.hub.request(
                    "POST",
                    f"/messages/{target['id']}/update",
                    {
                        "instance": target.get("instance"),
                        "claim": target.get("claim"),
                        "state": "undispatched",
                        "expected_execution_id": reserved["execution_id"],
                        "evidence": reserved,
                    },
                )
                continue
            if not expected:
                await self.hub.request(
                    "POST",
                    f"/messages/{target['id']}/update",
                    {
                        "instance": target.get("instance"),
                        "claim": target.get("claim"),
                        "state": "execution_settled" if execution_only else "cancelled",
                        "result": "No native dispatch was reserved",
                        "evidence": {"not_dispatched": True},
                    },
                )
                continue
            terminal = self.gateway._last_terminal.get(binding["thread_id"])
            if (
                expected
                and terminal
                and self.terminal_matches(key, target.get("evidence", {}), terminal)
                and not terminal.get("pending")
                and not self.ownership_pending(key, terminal)
                and terminal.get("native_dispatched", True)
                and (
                    terminal["stop_reason"] != "end_turn"
                    or target.get("late_result")
                    or execution_only
                )
            ):
                state = {"cancelled": "cancelled", "end_turn": "done"}.get(
                    terminal["stop_reason"], "failed"
                )
                if terminal["stop_reason"] == "end_turn":
                    state = target.get("late_state", state)
                evidence = self.foreground(target, terminal)
                await self.hub.request(
                    "POST",
                    f"/messages/{target['id']}/update",
                    {
                        "instance": target.get("instance"),
                        "claim": target.get("claim"),
                        "state": "execution_settled" if execution_only else state,
                        "result": target.get("late_result") or evidence["foreground_answer"],
                        "evidence": evidence,
                    },
                )
                continue
            record = await self.gateway.sessions.store.get(binding["thread_id"])
            if expected and record is not None and record.session_id == expected:
                owned = self.ownership.get((target["id"], target.get("claim")))
                current = self.gateway._reports.get(expected)
                if owned is None or current is not owned:
                    await self.update(item, "blocked", "Native turn ownership changed or unknown")
                    return
                if current.get("collaboration_project") != target["project"]:
                    await self.update(
                        item, "blocked", "Shared native turn; preserve unrelated work"
                    )
                    return
                execution = reserved.get("execution_id")

                def may_cancel(
                    owned=owned,
                    expected=expected,
                    key=key,
                    execution=execution,
                    project=target["project"],
                ):
                    return (
                        self.gateway._reports.get(expected) is owned
                        and self.ownership.get(key) is owned
                        and owned.get("execution_id") == execution
                        and owned.get("collaboration_project") == project
                        and key not in self.reserving
                    )

                await self.stop_owned_async_tasks(
                    binding["thread_id"],
                    current["reporter"],
                    session=expected,
                    epoch=reserved.get("native_epoch"),
                    guard=may_cancel,
                    stopped=current.setdefault("async_stop_requested", set()),
                )
                # Repeat an idempotent cancellation until actual termination is observed.
                try:
                    async with asyncio.timeout(3):
                        await self.gateway.sessions.cancel(
                            binding["thread_id"],
                            expected_session=expected,
                            expected_epoch=reserved.get("native_epoch"),
                            guard=may_cancel,
                        )
                except NativeDispatchChanged:
                    await self.update(
                        item, "blocked", "Native ownership changed before cancellation"
                    )
                    return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("Native cancellation remains unresolved")
            elif expected and record is not None and record.session_id != expected:
                await self.update(item, "blocked", "Native session changed; preserve other work")
                return
            await asyncio.sleep(1)
        # Cancellation remains visible and receives another deterministic delivery attempt.
        await self.update(item, "queued")

    @staticmethod
    def valid_stop_intent(stop):
        if type(stop) in {int, float}:
            try:
                return math.isfinite(stop)
            except OverflowError:
                return False
        return (
            isinstance(stop, dict)
            and set(stop) <= {"key", "phase", "boundary"}
            and isinstance(stop.get("key"), str)
            and re.fullmatch(r"[a-f0-9]{32}", stop["key"]) is not None
            and stop.get("phase") in {"new", "uncertain", "captured"}
            and (
                stop["phase"] != "captured"
                or (
                    isinstance(stop.get("boundary"), dict)
                    and stop["boundary"].get("key") == stop["key"]
                    and isinstance(stop["boundary"].get("thread_id"), str)
                    and type(stop["boundary"].get("admission_order")) is int
                    and stop["boundary"]["admission_order"] >= 0
                )
            )
        )

    def record_stop(self, thread):
        stop = {"key": uuid.uuid4().hex, "phase": "new"}
        self.pending_stops[thread] = stop
        try:
            self.save_stops()
        except OSError:
            logger.warning("Project stop intent retained in memory; persistence unavailable")
        return stop

    def retain_stop(self, thread, previous, updated):
        if self.pending_stops.get(thread) == previous:
            self.pending_stops[thread] = updated
            try:
                self.save_stops()
            except OSError:
                logger.warning("Project stop boundary retained in memory; persistence unavailable")

    async def cancel_thread(self, thread, *, retry=False, cutoff=None):
        # Retain the argument name for callers with saved legacy timestamps.
        # New intents carry a key and a hub-issued admission-order receipt.
        fresh_capture = not retry or cutoff is not None
        if not retry:
            cutoff = self.record_stop(thread)
        elif cutoff is None:
            cutoff = self.pending_stops[thread]
        if type(cutoff) in {int, float}:
            result = await self.hub.request(
                "POST", "/cancel-thread", {"thread_id": thread, "cutoff": cutoff}
            )
            confirmed = cutoff
        else:
            if not self.valid_stop_intent(cutoff):
                raise ValueError("Invalid pending project stop")
            stop = dict(cutoff)
            current = self.pending_stops.get(thread)
            if isinstance(current, dict) and current.get("key") == stop["key"]:
                stop = dict(current)
            elif stop["phase"] == "new":
                raise ValueError("Uncaptured project stop was superseded")
            if stop["phase"] == "new" and fresh_capture:
                uncertain = {**stop, "phase": "uncertain"}
                # Persist uncertainty before transport. A lost response must
                # never cause a fresh capture to include newer admissions.
                self.retain_stop(thread, stop, uncertain)
                stop = uncertain
                boundary = await self.hub.request(
                    "POST", "/stop-boundaries", {"thread_id": thread, "key": stop["key"]}
                )
            elif stop["phase"] in {"new", "uncertain"}:
                # 404 remains visible. Another process may have admitted newer
                # work during the outage, so POST cannot be retried safely.
                boundary = await self.hub.request("GET", "/stop-boundaries/" + stop["key"])
            else:
                boundary = stop["boundary"]
            captured = {"key": stop["key"], "phase": "captured", "boundary": boundary}
            if not self.valid_stop_intent(captured) or boundary["thread_id"] != thread:
                raise ValueError("Hub stop boundary did not match its intent")
            self.retain_stop(thread, stop, captured)
            confirmed = captured
            result = await self.hub.request(
                "POST", "/cancel-thread", {"thread_id": thread, "boundary": boundary}
            )
        if self.pending_stops.get(thread) == confirmed:
            self.pending_stops.pop(thread)
        try:
            self.save_stops()
        except OSError:
            logger.warning("Confirmed project stop could not clear its durable retry")
        return result

    def save_stops(self):
        self.stops_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self.stops_path.with_name(self.stops_path.name + "." + self.instance)
        with temporary.open("w") as stream:
            temporary.chmod(0o600)
            json.dump(self.pending_stops, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.stops_path)
        directory = os.open(self.stops_path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    async def post_result(self, item):
        thread, channel = item["origin_thread_id"], item["origin_channel_id"]
        root = await self.gateway.client.get_post(thread)
        if root.get("channel_id") != channel or root.get("root_id"):
            await self.update(item, "failed", "Origin is not a root in its recorded channel")
            return
        header = (
            f"Project {item['project_title']}: {item['sender']} "
            f"returned {item.get('result_state', 'done')}.\n\n"
        ).replace("@", "@\u200b")
        footer = (
            f"\n\nRequest: `{item['parent']}`. "
            f"Full result and evidence: `mm-work show {item['project']}`."
        ).replace("@", "@\u200b")
        header = header[: DEFAULT_LIMIT - len(footer)].removesuffix("@")
        preview = item["text"][:8000].replace("@", "@\u200b")
        preview = preview[: DEFAULT_LIMIT - len(header) - len(footer)].removesuffix("@")
        body = header + preview + footer
        receipt = await self.update(
            item, "post_dispatch", evidence={"delivery": "posted", "origin_thread_id": thread}
        )
        if receipt["state"] != "running" or receipt.get("claim") != item.get("claim"):
            if receipt["state"] == "cancelling" and receipt.get("claim") == item.get("claim"):
                await self.update(item, "queued")
            return
        post = await self.gateway.client.create_post(channel, body, root_id=thread)
        await self.update(item, "heartbeat", evidence={"result_post_id": post["id"]})
        verified = await self.gateway.client.get_post(post["id"])
        if (
            verified.get("root_id") != thread
            or verified.get("channel_id") != channel
            or verified.get("message") != body
        ):
            await self.update(item, "unconfirmed", "Result notification read-back failed")
            return
        await self.update(
            item, "done", item["text"], {"delivery": "posted", "result_post_id": post["id"]}
        )

    async def mention(self, posted):
        try:
            return bool(
                await self.hub.request(
                    "POST",
                    "/mentions",
                    {
                        "author_id": posted.user_id,
                        "channel_id": posted.channel_id,
                        "thread_id": posted.root_id,
                        "post_id": posted.post_id,
                        "text": posted.message,
                    },
                )
            )
        except (HubError, aiohttp.ClientError, TimeoutError, ValueError) as exc:
            # Only a successful unregistered-discussion response permits normal
            # wake fallback. Rejection or uncertainty cannot authorize new work.
            logger.warning("Project mention could not be admitted; ordinary wake suppressed")
            rejected = (
                isinstance(exc, HubError) and exc.status is not None and 400 <= exc.status < 500
            )
            notice = (
                "Project request rejected by collaboration admission. "
                "Check the project status, membership and request limits."
                if rejected
                else "Project request admission is unconfirmed. "
                "Check the project state before retrying."
            )
            try:
                await self.gateway.client.create_post(
                    posted.channel_id, notice, root_id=posted.root_id
                )
            except (MattermostError, aiohttp.ClientError, TimeoutError):
                logger.warning("Project request admission notice could not be posted")
            return True
