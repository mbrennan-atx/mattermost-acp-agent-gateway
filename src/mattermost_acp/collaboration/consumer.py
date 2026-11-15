"""Route durable project events into one independent gateway's native sessions."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from pathlib import Path

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
                item = await self.hub.request("GET", f"/mailbox?instance={self.instance}&wait=20")
                if item:
                    task = asyncio.create_task(
                        self.handle(item), name="collaboration-" + item["id"]
                    )
                    self.workers[item["id"]] = task
                    task.add_done_callback(
                        lambda _task, mid=item["id"]: self.workers.pop(mid, None)
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Collaboration mailbox unavailable; durable requests retained")
                await asyncio.sleep(2)

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
        return any(
            report["thread_id"] == thread
            and (report.get("active") or report["reporter"].has_pending_work())
            for report in self.gateway._reports.values()
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
            if state["state"] not in {"running", "cancelling"}:
                return
            terminal = self.gateway._last_terminal.get(item["binding"]["thread_id"])
            receipt = state.get("evidence", {})
            if (
                state["state"] == "cancelling"
                and receipt.get("session_id")
                and terminal
                and terminal.get("session_id") == receipt["session_id"]
                and terminal.get("execution_id") == receipt.get("execution_id")
                and terminal["stop_reason"] == "cancelled"
                and terminal["completed_at"] >= receipt.get("dispatch_started", float("inf"))
                and not terminal.get("pending")
            ):
                await self.update(item, "cancelled", "Native stop=cancelled", terminal)
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
        return (
            f"Project collaboration: {item['project_title']}\n"
            f"Project ID: {item['project']}\nRequest ID: {item['id']}\n"
            f"From: {item['sender']}\nKind: {item['kind']}\n"
            f"Claim: {item.get('claim', '')}\nParent request: {item.get('parent') or ''}\n"
            f"Result status: {item.get('result_state', 'pending')}\n"
            f"Evidence: {json.dumps(item.get('evidence', {}))}\n"
            f"Objective: {item['objective']}\n{instructions}\n\n{item['text']}"
        )

    async def handle(self, item):
        renewal = asyncio.create_task(self.heartbeat(item))
        try:
            if item["kind"] == "cancel":
                await self.handle_cancel(item)
                return
            current = await self.hub.request("GET", f"/messages/{item['id']}")
            if current["state"] == "cancelling":
                await self.update(item, "cancelled", "Cancelled before native dispatch")
                return
            if current["state"] != "running" or current.get("claim") != item.get("claim"):
                return
            binding = item["binding"]
            thread = binding["thread_id"]
            # Verify the declared discussion/binding before creating native work.
            post = await self.gateway.client.get_post(thread)
            if post.get("channel_id") != binding["channel_id"]:
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
                    outcome = await self.gateway.sessions.hint(thread, prompt)
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
                return
            if result is None:
                await self.update(item, "interrupted", "No confirmed native terminal receipt")
            elif result.get("pending"):
                await self.update(item, "heartbeat", evidence=result)
                await self.injected_wait(item)
            elif result["stop_reason"] == "cancelled":
                await self.update(item, "cancelled", "Native stop=cancelled", result)
            else:
                state = "done" if result["stop_reason"] == "end_turn" else "failed"
                answer = result["answer"] or "No substantive answer"
                evidence = {
                    "session_id": result["session_id"],
                    "model": result["model"],
                    "stop_reason": result["stop_reason"],
                    "delivery": "continuation",
                }
                if len(answer) > 8000:
                    folder = Path.home() / ".local/state/mattermost-acp-collaboration/results"
                    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
                    path = folder / (item["id"] + ".txt")
                    path.write_text(answer)
                    path.chmod(0o600)
                    evidence["result_file"] = str(path)
                    answer = answer[:8000] + "\nFull result saved at " + str(path)
                await self.update(
                    item,
                    state,
                    answer,
                    evidence,
                )
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

    async def reserve(self, item, session):
        report = self.gateway._reports.get(session)
        receipt = await self.update(
            item,
            "dispatch",
            evidence={"session_id": session, "execution_id": (report or {}).get("execution_id")},
        )
        accepted = receipt["state"] == "running" and receipt.get("claim") == item.get("claim")
        if accepted:
            self.ownership[(item["id"], item.get("claim"))] = report
        return accepted

    async def handle_cancel(self, item):
        for _ in range(20):
            target = await self.hub.request("GET", f"/messages/{item['parent']}")
            if target["state"] != "cancelling":
                await self.update(item, "done", "Project request has settled")
                return
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
            ):
                state = {"cancelled": "cancelled", "end_turn": "done"}.get(
                    terminal["stop_reason"], "failed"
                )
                await self.hub.request(
                    "POST",
                    f"/messages/{target['id']}/update",
                    {
                        "instance": target.get("instance"),
                        "claim": target.get("claim"),
                        "state": state,
                        "result": terminal["answer"] or "Native turn ended",
                        "evidence": terminal,
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
                await self.gateway.sessions.cancel(binding["thread_id"])
            elif expected and record is not None and record.session_id != expected:
                await self.update(item, "blocked", "Native session changed; preserve other work")
                return
            await asyncio.sleep(1)
        # Cancellation remains visible and receives another deterministic delivery attempt.
        await self.update(item, "queued")

    async def cancel_thread(self, thread):
        return await self.hub.request("POST", "/cancel-thread", {"thread_id": thread})

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
