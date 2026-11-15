"""Route durable project events into one independent gateway's native sessions."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shlex
import sys
import uuid
from pathlib import Path

from mattermost_acp.acp.broker import NativeDispatchChanged
from mattermost_acp.collaboration.client import HubClient, HubError, Settings

logger = logging.getLogger(__name__)


class CollaborationConsumer:
    def __init__(self, gateway, settings: Settings):
        self.gateway = gateway
        self.hub = HubClient(settings)
        self.instance = uuid.uuid4().hex
        self.task = None
        self.workers: dict[str, asyncio.Task] = {}
        self.closing = False
        self.ownership = {}
        self.reserving = set()
        self.cancelled_claims = set()
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
        ).with_suffix(".stops.json")
        self.pending_stops = (
            set(json.loads(self.stops_path.read_text())) if self.stops_path.exists() else set()
        )

    def start(self):
        self.task = asyncio.create_task(self.run(), name="collaboration-mailbox")

    async def close(self):
        if self.closing:
            return
        self.closing = True
        tasks = [task for task in [self.task, *self.workers.values()] if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.hub.close()

    async def run(self):
        while not self.closing:
            try:
                if not self.actor:
                    identity = await self.hub.request("GET", "/identity")
                    if self.sidecar and (
                        identity["actor"] != getattr(self.gateway.config, "collab_origin_actor", "")
                        or identity.get("user_id") != self.gateway.client.user_id
                    ):
                        raise ValueError("Collaboration actor does not match the sidecar identity")
                    self.actor = identity["actor"]
                    if bool(identity.get("sidecar")) != self.sidecar:
                        self.actor = ""
                        raise ValueError("Hub and consumer delivery modes do not match")
                for thread in list(self.pending_stops):
                    await self.cancel_thread(thread)
                if self.gateway.sealed:
                    await asyncio.sleep(0.25)
                    continue
                controls = "&controls_only=1" if self.gateway.draining else ""
                item = await self.hub.request(
                    "GET", f"/mailbox?instance={self.instance}&wait=20" + controls
                )
                if item:
                    task = asyncio.create_task(
                        self.handle(item), name="collaboration-" + item["id"]
                    )
                    key = (item["id"], item.get("claim"))
                    self.workers[key] = task
                    task.add_done_callback(
                        lambda completed, key=key: self.finish_worker(key, completed)
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Collaboration mailbox unavailable; durable requests retained")
                await asyncio.sleep(2)

    def finish_worker(self, key, completed):
        if self.workers.get(key) is completed:
            self.workers.pop(key)

    async def update(self, item, state, result="", evidence=None):
        return await self.hub.request(
            "POST",
            f"/messages/{item['id']}/update",
            {
                "instance": self.instance,
                "state": state,
                "result": result,
                "evidence": evidence or {},
                "claim": item.get("claim"),
            },
        )

    def active(self, thread):
        if self.gateway.sessions.is_busy(thread):
            return True
        if any(report and report.get("thread_id") == thread for report in self.ownership.values()):
            return True
        return any(
            report["thread_id"] == thread
            and (report.get("active") or report["reporter"].has_pending_work())
            for report in self.gateway.live_reports()
        )

    async def heartbeat(self, item):
        while True:
            await asyncio.sleep(10)
            try:
                await self.update(item, "heartbeat")
            except HubError:
                return
            except Exception:
                logger.warning("Collaboration lease renewal unavailable")

    async def injected_wait(self, item):
        while True:
            await asyncio.sleep(1)
            state = await self.hub.request("GET", f"/messages/{item['id']}")
            terminal = self.gateway._last_terminal.get(item["binding"]["thread_id"])
            receipt = state.get("evidence", {})
            if state["state"] not in {"running", "cancelling"}:
                if (
                    state["state"] in {"done", "failed", "blocked", "cancelled"}
                    and terminal
                    and terminal.get("session_id") == receipt.get("session_id")
                    and terminal.get("execution_id") == receipt.get("execution_id")
                    and terminal["completed_at"] >= receipt.get("dispatch_started", float("inf"))
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
                and terminal.get("session_id") == receipt["session_id"]
                and terminal.get("execution_id") == receipt.get("execution_id")
                and terminal["stop_reason"] != "end_turn"
                and terminal["completed_at"] >= receipt.get("dispatch_started", float("inf"))
                and not terminal.get("pending")
            ):
                state = "cancelled" if terminal["stop_reason"] == "cancelled" else "failed"
                await self.update(
                    item,
                    state,
                    "Native stop=" + terminal["stop_reason"],
                    self.foreground(item, terminal),
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
            )
        else:
            instructions += (
                "This is a peer result for your existing task. Reconcile it with the objective, "
                "then continue authorized work or report the verified outcome. This delivery "
                "requires no acknowledgement or reply to the sender. "
            )
        command = shlex.quote(str(Path(sys.executable).parent / "mm-work"))
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
        renewal = asyncio.create_task(self.heartbeat(item))
        try:
            if self.gateway.sealed or (self.gateway.draining and item["kind"] != "cancel"):
                await self.update(item, "queued")
                return
            if item["kind"] == "cancel":
                await self.handle_cancel(item)
                return
            current = await self.hub.request("GET", f"/messages/{item['id']}")
            if current["state"] == "cancelling":
                await self.update(item, "cancelled", "Cancelled before native dispatch")
                return
            if current["state"] != "running" or current.get("claim") != item.get("claim"):
                return
            if (
                self.sidecar
                and item["kind"] == "result"
                and item["recipient"] == item.get("project_owner")
            ):
                await self.post_result(item)
                return
            # A new claim is explicit reconciliation. Old uncertain reservations
            # must not occupy the thread indefinitely after that reconciliation.
            for key in list(self.ownership):
                if key[0] == item["id"] and key[1] != item.get("claim"):
                    self.ownership.pop(key)
            binding = item["binding"]
            thread = binding["thread_id"]
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
            outcome = None
            caps = broker.capabilities
            can_hint = record is not None and broker.is_running and caps.steering
            if can_hint and broker.backend == "codex":
                can_hint = ((caps.raw.get("_meta") or {}).get("steering") or {}).get(
                    "idleBehavior"
                ) == "promptRequired"
            if can_hint:
                if not await self.reserve(item, record.session_id):
                    await self.update(item, "cancelled", "Cancelled before native injection")
                    return
                try:
                    outcome = await self.gateway.sessions.hint(
                        thread,
                        prompt,
                        expected_session=record.session_id,
                        expected_epoch=item["native_epoch"],
                        guard=lambda: (
                            self.gateway._reports.get(record.session_id)
                            is self.ownership.get((item["id"], item.get("claim")))
                            and (item["id"], item.get("claim")) not in self.cancelled_claims
                        ),
                    )
                except NativeDispatchChanged:
                    self.ownership.pop((item["id"], item.get("claim")), None)
                    await self.update(item, "queued")
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
            self.ownership.pop((item["id"], item.get("claim")), None)
            if self.active(thread):
                await self.update(item, "queued")
                return
            result = await self.gateway.run_prompt(
                thread_id=thread,
                channel_id=binding["channel_id"],
                prompt=prompt,
                author_id="project:" + item["project"],
                author_name=item["sender"],
                workspace=binding.get("workspace"),
                collaboration_project=item["project"],
                before_native=lambda session: self.reserve(item, session),
            )
            # Explicit mm-work reply may already have settled the request.
            current = await self.hub.request("GET", f"/messages/{item['id']}")
            if current["state"] in {"done", "failed", "blocked", "cancelled"}:
                if result and not result.get("queued"):
                    await self.update(item, "evidence", evidence=self.foreground(item, result))
                return
            if result is None:
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
        except Exception:
            logger.exception("Collaboration delivery interrupted request=%s", item["id"])
            try:
                await self.update(item, "interrupted", "Delivery failed; reconcile before retry")
            except Exception:
                pass
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
            for mid in [item["id"], item.get("parent") if item["kind"] == "cancel" else None]:
                if mid:
                    try:
                        settled = await self.hub.request("GET", f"/messages/{mid}")
                        if settled["state"] in {"done", "failed", "blocked", "cancelled", "queued"}:
                            self.ownership.pop((mid, settled.get("claim")), None)
                    except Exception:
                        pass

    def foreground(self, item, result):
        evidence = {key: value for key, value in result.items() if key != "answer"}
        answer = result.get("answer") or "No substantive answer"
        if len(answer) > 8000:
            folder = Path.home() / ".local/state/mattermost-acp-collaboration/results"
            folder.mkdir(parents=True, exist_ok=True, mode=0o700)
            path = folder / (
                item["id"] + "-" + str(item.get("claim") or item.get("attempt", 0)) + ".txt"
            )
            path.write_text(answer)
            path.chmod(0o600)
            evidence["result_file"] = str(path)
            answer = answer[:8000] + "\nFull foreground output saved at " + str(path)
        evidence["foreground_answer"] = answer
        return evidence

    async def reserve(self, item, session):
        report = self.gateway._reports.get(session)
        epoch = getattr(self.gateway.sessions, "native_epoch", lambda _thread: 0)(
            item["binding"]["thread_id"]
        )
        key = (item["id"], item.get("claim"))
        self.reserving.add(key)
        self.ownership[key] = report
        try:
            receipt = await self.update(
                item,
                "dispatch",
                evidence={
                    "session_id": session,
                    "execution_id": (report or {}).get("execution_id"),
                    "native_epoch": epoch,
                },
            )
            accepted = (
                receipt["state"] == "running"
                and receipt.get("claim") == item.get("claim")
                and key not in self.cancelled_claims
            )
            if accepted:
                item["native_epoch"] = epoch
            else:
                self.ownership.pop(key, None)
                if receipt["state"] == "cancelling" or key in self.cancelled_claims:
                    await self.update(item, "cancelled", "Cancelled before native dispatch")
            return accepted
        finally:
            self.reserving.discard(key)

    async def handle_cancel(self, item):
        for _ in range(20):
            target = await self.hub.request("GET", f"/messages/{item['parent']}")
            if target["state"] != "cancelling":
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
            expected = target.get("evidence", {}).get("session_id")
            if not expected:
                await self.hub.request(
                    "POST",
                    f"/messages/{target['id']}/update",
                    {
                        "instance": target.get("instance"),
                        "claim": target.get("claim"),
                        "state": "cancelled",
                        "result": "No native dispatch was reserved",
                    },
                )
                continue
            started = target.get("evidence", {}).get("dispatch_started", target["created"])
            terminal = self.gateway._last_terminal.get(binding["thread_id"])
            if (
                expected
                and terminal
                and terminal.get("session_id") == expected
                and terminal.get("execution_id") == target.get("evidence", {}).get("execution_id")
                and terminal["completed_at"] >= started
                and not terminal.get("pending")
                and (terminal["stop_reason"] != "end_turn" or target.get("late_result"))
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
                        "state": state,
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
                # Repeat an idempotent cancellation until actual termination is observed.
                try:
                    await self.gateway.sessions.cancel(
                        binding["thread_id"],
                        expected_session=expected,
                        expected_epoch=target.get("evidence", {}).get("native_epoch"),
                        guard=lambda expected=expected, owned=owned: (
                            self.gateway._reports.get(expected) is owned
                        ),
                    )
                except NativeDispatchChanged:
                    await self.update(
                        item, "blocked", "Native ownership changed before cancellation"
                    )
                    return
            elif expected and record is not None and record.session_id != expected:
                await self.update(item, "blocked", "Native session changed; preserve other work")
                return
            await asyncio.sleep(1)
        # Cancellation remains visible and receives another deterministic delivery attempt.
        await self.update(item, "queued")

    async def cancel_thread(self, thread):
        self.pending_stops.add(thread)
        try:
            self.save_stops()
        except OSError:
            logger.warning("Project stop intent retained in memory; persistence unavailable")
        result = await self.hub.request("POST", "/cancel-thread", {"thread_id": thread})
        self.pending_stops.discard(thread)
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
            json.dump(sorted(self.pending_stops), stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.stops_path)

    async def post_result(self, item):
        thread, channel = item["origin_thread_id"], item["origin_channel_id"]
        root = await self.gateway.client.get_post(thread)
        if root.get("channel_id") != channel or root.get("root_id"):
            await self.update(item, "failed", "Origin is not a root in its recorded channel")
            return
        body = (
            f"Project {item['project_title']}: {item['sender']} "
            f"returned {item.get('result_state', 'done')}.\n\n"
            + item["text"][:8000]
            + f"\n\nRequest: `{item['parent']}`. "
            + f"Full result and evidence: `mm-work show {item['project']}`."
        ).replace("@", "@\u200b")
        await self.update(
            item, "dispatch", evidence={"delivery": "posted", "origin_thread_id": thread}
        )
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
        except HubError:
            return False
