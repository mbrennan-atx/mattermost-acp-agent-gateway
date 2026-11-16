"""Result delivery and reconciled ownership use synthetic local native sessions."""

import asyncio
import json
import os
import re
import stat
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from test_collaboration import active_gateway, api_fixture, ask, close_api, project
from test_collaboration_continuation_review import child_result, retained_parent, terminal
from test_collaboration_dispatch_review import close_reports, idle_report, native_gateway
from test_collaboration_regressions import (
    maintenance_client,
    maintenance_gateway,
    memory_broker,
    sidecar_project,
)

from mattermost_acp.acp import AcpBroker, SessionUpdate, TurnResult
from mattermost_acp.acp.broker import NativeDispatchChanged
from mattermost_acp.collaboration.client import HubError, Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.collaboration.store import CollaborationStore
from mattermost_acp.mm.client import MattermostClient, PostedEvent
from mattermost_acp.mm.reporting import TurnReporter
from mattermost_acp.session_service import SessionService


@pytest.mark.parametrize("termination", ["verified", "ack-only", "missing-lifecycle", "foreign-id"])
async def test_detached_claude_command_requires_owned_task_stop_and_terminal_lifecycle(
    tmp_path, termination
):
    marker = tmp_path / "command-running"
    command = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import pathlib,sys,time; pathlib.Path(sys.argv[1]).write_text('running'); time.sleep(300)",
        str(marker),
        start_new_session=True,
    )
    try:
        async with asyncio.timeout(3):
            while not marker.exists():
                await asyncio.sleep(0.01)
        async with retained_parent(tmp_path) as (
            store,
            _clients,
            gateway,
            consumer,
            p,
            parent,
            report,
        ):
            broker, writer = memory_broker()
            sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
            sessions._native_epochs["discussion"] = 7
            gateway.sessions = sessions
            broker.update_observer = gateway._observe_background_update
            report["reporter"] = TurnReporter()

            async def native(update):
                await broker._dispatch(
                    {
                        "method": "session/update",
                        "params": {
                            "sessionId": "existing-native",
                            "update": update,
                        },
                    }
                )

            await native(
                {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "project-bash",
                    "status": "in_progress",
                    "_meta": {"claudeCode": {"toolName": "Bash"}},
                    "rawInput": {
                        "command": "synthetic detached command",
                        "run_in_background": True,
                    },
                }
            )
            if termination != "missing-lifecycle":
                await native(
                    {
                        "sessionUpdate": "async_task_spawned",
                        "asyncTaskId": "command-task",
                        "taskType": "local_bash",
                        "canStop": True,
                        "toolCallId": "foreign-bash"
                        if termination == "foreign-id"
                        else "project-bash",
                    }
                )
            await native(
                {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "project-bash",
                    "status": "completed",
                    "_meta": {"claudeCode": {"toolName": "Bash"}},
                }
            )
            assert report["reporter"].has_pending_work()
            store.reply(parent["id"], "beta", "Early result", claim=parent["claim"])
            assert store.cancel(p["id"], "alpha")["status"] == "cancelling"
            control = store.claim_next("beta", consumer.instance, controls_only=True)
            native_cancelled = asyncio.Event()
            replies = []

            async def stop_reply(message):
                if termination == "verified":
                    command.terminate()
                    await command.wait()
                    await native(
                        {
                            "sessionUpdate": "async_task_state_update",
                            "asyncTaskId": "command-task",
                            "state": "stopped",
                        }
                    )
                await broker._dispatch({"id": message["id"], "result": {"stopped": True}})

            def write(payload):
                message = json.loads(payload)
                writer.messages.append(message)
                if message["method"] == "_session/async_task/stop":
                    replies.append(asyncio.create_task(stop_reply(message)))
                elif message["method"] == "session/cancel":
                    native_cancelled.set()

            writer.write = write
            task = asyncio.create_task(consumer.handle_cancel(control))
            try:
                await asyncio.wait_for(native_cancelled.wait(), 3)
                stops = [
                    msg for msg in writer.messages if msg["method"] == "_session/async_task/stop"
                ]
                assert len(stops) == (1 if termination in {"verified", "ack-only"} else 0)
                if stops:
                    assert stops[0]["params"] == {
                        "sessionId": "existing-native",
                        "asyncTaskId": "command-task",
                    }
                if termination == "verified":
                    await asyncio.wait_for(task, 3)
                    assert command.returncode is not None
                    assert store.get_project(p["id"], "alpha")["status"] == "cancelled"
                    assert store.get_message(parent["id"], "beta")["execution_pending"] is False
                    assert gateway.maintenance_state()["native_boundary_unknown"] is True
                else:
                    assert command.returncode is None and marker.read_text() == "running"
                    assert store.get_project(p["id"], "alpha")["status"] == "cancelling"
                    assert store.get_message(parent["id"], "beta")["execution_pending"] is True
                    assert consumer.ownership[(parent["id"], parent["claim"])] is report
                    assert gateway.background_pending("discussion")
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, *replies, return_exceptions=True)
                await close_reports(gateway)
    finally:
        if command.returncode is None:
            command.terminate()
        await command.wait()


@pytest.mark.parametrize("changed", ["epoch", "ownership"])
async def test_native_async_task_stop_rechecks_ownership_at_actual_write(tmp_path, changed):
    broker, writer = memory_broker()
    sessions = SessionService(
        broker,
        SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(session_id="native"))),
        default_cwd=tmp_path,
    )
    owned = True
    await broker._write_lock.acquire()
    task = asyncio.create_task(
        sessions.stop_async_task(
            "discussion",
            "project-task",
            expected_session="native",
            expected_epoch=0,
            guard=lambda: owned,
        )
    )
    try:
        async with asyncio.timeout(2):
            while not broker._pending:
                await asyncio.sleep(0)
        if changed == "epoch":
            sessions._advance_native("discussion")
        else:
            owned = False
        broker._write_lock.release()
        with pytest.raises(NativeDispatchChanged):
            await task
        assert writer.messages == [] and not broker._pending
    finally:
        if broker._write_lock.locked():
            broker._write_lock.release()
        await asyncio.gather(task, return_exceptions=True)


async def test_native_child_foreground_completion_does_not_settle_its_detached_command(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, report):
        broker, writer = memory_broker()
        gateway.sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        gateway.sessions._native_epochs["discussion"] = 7
        broker.update_observer = gateway._observe_background_update
        report["reporter"] = TurnReporter()

        async def native(session, update):
            await broker._dispatch(
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": session,
                        "update": update,
                    },
                }
            )

        await native(
            "existing-native",
            {
                "sessionUpdate": "subagent_spawned",
                "subagentSessionId": "native-child",
                "name": "child",
            },
        )
        await native(
            "native-child",
            {
                "sessionUpdate": "async_task_spawned",
                "asyncTaskId": "child-command",
                "toolCallId": "child-bash",
                "canStop": True,
            },
        )
        await native(
            "existing-native",
            {
                "sessionUpdate": "subagent_state_update",
                "subagentSessionId": "native-child",
                "state": "completed",
            },
        )
        assert report["reporter"].has_pending_work()
        assert report["reporter"].stoppable_async_tasks() == []
        store.reply(parent["id"], "beta", "Early result", claim=parent["claim"])
        assert store.cancel(p["id"], "alpha")["status"] == "cancelling"
        state = store.get_message(parent["id"], "beta")
        await consumer.settle_ownership((parent["id"], parent["claim"]), state)
        assert store.get_message(parent["id"], "beta")["execution_pending"] is True
        assert writer.messages == []
        await native(
            "native-child",
            {
                "sessionUpdate": "async_task_state_update",
                "asyncTaskId": "child-command",
                "state": "stopped",
            },
        )
        assert not report["reporter"].has_pending_work()
        await consumer.settle_ownership((parent["id"], parent["claim"]), state)
        assert store.get_message(parent["id"], "beta")["execution_pending"] is False
        assert store.get_project(p["id"], "alpha")["status"] == "cancelled"
        await close_reports(gateway)


def test_result_retry_at_full_work_budget_preserves_result_and_requires_reconciliation(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store, max_turns=1)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", "worker")
        store.reply(
            requested["id"],
            "beta",
            "Verified dependency",
            evidence={"artifact": "https://example.com/proof"},
            claim=claimed["claim"],
        )
        delivery = store.claim_next("alpha", "owner")
        store.update(
            delivery["id"],
            "alpha",
            "owner",
            "evidence",
            evidence={"post_id": "uncertain-post"},
            claim=delivery["claim"],
        )
        store.update(
            delivery["id"],
            "alpha",
            "owner",
            "unconfirmed",
            "Result posting was ambiguous",
            claim=delivery["claim"],
        )
        retried = store.retry(delivery["id"], "alpha", "Verified no result post exists")
        assert retried["state"] == "queued"
        assert retried["result_evidence"]["artifact"] == "https://example.com/proof"
        assert retried["delivery_history"][-1]["post_id"] == "uncertain-post"
        assert store.show(p["id"], "alpha")["dispatched"] == 1
        replay = store.claim_next("alpha", "new-owner")
        assert replay["id"] == delivery["id"] and replay["claim"] != delivery["claim"]
        assert replay["text"] == "Verified dependency"
        assert store.get_message(requested["id"], "beta")["state"] == "done"
        with pytest.raises(ValueError, match="dispatch limit"):
            ask(store, p["id"], key="another-request")
    finally:
        store.close()


@pytest.mark.parametrize("scenario", ["tool", "worker", "cancel-await", "repair-outage"])
async def test_late_native_activity_during_settlement_restores_debt_and_cancellation(
    tmp_path, scenario
):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, report):
        broker, writer = memory_broker()
        sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        sessions._native_epochs["discussion"] = 7
        gateway.sessions = sessions
        report["reporter"] = TurnReporter()
        store.reply(parent["id"], "beta", "Early result", claim=parent["claim"])
        state = store.get_message(parent["id"], "beta")
        accepted, release = asyncio.Event(), asyncio.Event()
        actual = consumer.hub.request
        repair_fails = scenario == "repair-outage"

        async def delayed(method, path, body=None):
            if body and body.get("state") == "execution_resumed" and repair_fails:
                raise HubError("Synthetic debt repair outage", status=503)
            result = await actual(method, path, body)
            if body and body.get("state") == "execution_settled" and not accepted.is_set():
                accepted.set()
                await release.wait()
            return result

        consumer.hub.request = delayed
        key = parent["id"], parent["claim"]
        task = asyncio.create_task(consumer.settle_ownership(key, state))

        def activity(completed=False):
            if scenario == "worker":
                return SessionUpdate(
                    "existing-native",
                    "subagent_activity",
                    raw={
                        "worker_id": "late-child",
                        "state": "completed" if completed else "running",
                    },
                )
            return SessionUpdate(
                "existing-native",
                "tool_call_update" if completed else "tool_call",
                tool_call_id="late-tool",
                tool_status="completed" if completed else "in_progress",
            )

        try:
            await asyncio.wait_for(accepted.wait(), 2)
            assert store.get_message(parent["id"], "beta")["execution_pending"] is False
            gateway._observe_background_update(activity())
            if scenario != "tool":
                assert store.cancel(p["id"], "alpha")["status"] == "cancelled"
            if scenario == "cancel-await":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                release.set()
                if repair_fails:
                    with pytest.raises(HubError, match="repair outage"):
                        await asyncio.wait_for(task, 2)
                    assert key in consumer.pending_execution_restores
                    assert consumer.ownership[key] is report
                    repair_fails = False
                    await consumer.reconcile_ownership()
                else:
                    await asyncio.wait_for(task, 2)
            restored = store.get_message(parent["id"], "beta")
            assert restored["state"] == "done" and restored["execution_pending"] is True
            assert restored["evidence"]["pending"] is True
            assert restored["evidence"]["session_id"] == "existing-native"
            assert restored["evidence"]["execution_id"] == report["execution_id"]
            assert consumer.ownership[key] is report and not consumer.pending_execution_restores
            assert key not in consumer.reserving
            if scenario == "tool":
                store.cancel(p["id"], "alpha")
            assert store.get_project(p["id"], "alpha")["status"] == "cancelling"
            assert store.get_message(parent["id"], "beta")["execution_cancelling"] is True
            control = store.claim_next("beta", consumer.instance, controls_only=True)
            assert control and control["parent"] == parent["id"]
            original_write = writer.write

            def cancelled(payload):
                original_write(payload)
                if json.loads(payload).get("method") == "session/cancel":
                    gateway._observe_background_update(activity(completed=True))
                    gateway._last_terminal["discussion"].update(
                        stop_reason="cancelled",
                        pending=False,
                        completed_at=store.clock() + 1,
                    )

            writer.write = cancelled
            await asyncio.wait_for(consumer.handle(control), 3)
            assert [msg["method"] for msg in writer.messages] == ["session/cancel"]
            assert writer.messages[0]["params"]["sessionId"] == "existing-native"
            assert store.get_project(p["id"], "alpha")["status"] == "cancelled"
            assert store.get_message(parent["id"], "beta")["execution_pending"] is False
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            await close_reports(gateway)


@pytest.mark.parametrize(
    "change",
    [
        "session_id",
        "execution_id",
        "native_epoch",
        "pending",
        "native_dispatched",
        "reserved_undispatched",
        "reserved_dispatch_false",
    ],
)
def test_resumed_execution_proof_cannot_replace_another_reservation(tmp_path, change):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store)
        work = ask(store, p["id"])
        claimed = store.claim_next("beta", "worker")
        store.update(
            work["id"],
            "beta",
            "worker",
            "dispatch",
            evidence={
                "session_id": "native",
                "execution_id": "execution",
                "native_epoch": 7,
            },
            claim=claimed["claim"],
        )
        store.reply(work["id"], "beta", "Early result", claim=claimed["claim"])
        before = store.get_message(work["id"], "beta")
        if change.startswith("reserved_"):
            field = "not_dispatched" if change == "reserved_undispatched" else "native_dispatched"
            before["evidence"][field] = field == "not_dispatched"
            store.save_message(before)
        proof = {
            "session_id": "native",
            "execution_id": "execution",
            "native_epoch": 7,
            "pending": True,
            "native_dispatched": True,
        }
        if not change.startswith("reserved_"):
            proof[change] = False if change in {"pending", "native_dispatched"} else "different"
        with pytest.raises(ValueError, match="reserved execution"):
            store.update(
                work["id"],
                "beta",
                "worker",
                "execution_resumed",
                evidence=proof,
                claim=claimed["claim"],
                expected_execution_id="execution",
            )
        assert store.get_message(work["id"], "beta") == before
    finally:
        store.close()


def test_resumed_execution_requeues_cancel_without_overwriting_control_history(tmp_path):
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"})
    try:
        p = project(store)
        work = ask(store, p["id"])
        claimed = store.claim_next("beta", "worker")
        store.update(
            work["id"],
            "beta",
            "worker",
            "dispatch",
            evidence={"session_id": "native", "execution_id": "execution"},
            claim=claimed["claim"],
        )
        store.reply(work["id"], "beta", "Early result", claim=claimed["claim"])
        store.cancel(p["id"], "alpha")
        control = store.claim_next("beta", "worker", controls_only=True)
        settled = store.update(
            work["id"],
            "beta",
            "worker",
            "execution_settled",
            evidence={
                "session_id": "native",
                "execution_id": "execution",
                "native_dispatched": True,
                "stop_reason": "cancelled",
                "pending": False,
                "completed_at": store.clock(),
            },
            claim=claimed["claim"],
        )
        history = store.update(
            control["id"], "beta", "worker", "done", "Cancelled", claim=control["claim"]
        )
        assert store.get_project(p["id"], "alpha")["status"] == "cancelled"
        for _ in range(2):
            store.update(
                work["id"],
                "beta",
                "worker",
                "execution_resumed",
                evidence={
                    "session_id": "native",
                    "execution_id": "execution",
                    "native_dispatched": True,
                    "pending": True,
                },
                claim=claimed["claim"],
            )
        assert store.get_message(control["id"], "beta") == history
        assert store.get_project(p["id"], "alpha")["status"] == "cancelling"
        replacement = store.claim_next("beta", "worker", controls_only=True)
        assert replacement["id"] != control["id"]
        assert replacement["parent_claim"] == claimed["claim"]
        assert replacement["key"].startswith(control["key"] + ":")
        assert store.claim_next("beta", "worker", controls_only=True) is None
        assert settled["execution_pending"] is False
    finally:
        store.close()


async def test_idle_execution_repair_keeps_continuation_and_cancellation_fenced(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, report):
        store.reply(parent["id"], "beta", "Early result", claim=parent["claim"])
        state = store.get_message(parent["id"], "beta")
        state = await consumer.update(
            state, "execution_settled", evidence=consumer.foreground(state, terminal())
        )
        key = parent["id"], parent["claim"]
        consumer.pending_execution_restores[key] = {
            "session_id": "existing-native",
            "execution_id": report["execution_id"],
            "native_dispatched": True,
            "pending": True,
        }
        assert not consumer.continuation_idle(
            {"report": report, "thread": "discussion", "parents": [state]}
        )
        accepted, release = asyncio.Event(), asyncio.Event()
        actual = consumer.hub.request

        async def delayed(method, path, body=None):
            if body and body.get("state") == "execution_resumed":
                accepted.set()
                await release.wait()
            return await actual(method, path, body)

        consumer.hub.request = delayed
        task = asyncio.create_task(consumer.settle_ownership(key, state))
        try:
            await asyncio.wait_for(accepted.wait(), 2)
            assert key in consumer.reserving and consumer.ownership[key] is report
            await consumer.settle_ownership(key, state)
            assert key in consumer.pending_execution_restores
            release.set()
            await asyncio.wait_for(task, 2)
            assert not consumer.pending_execution_restores and key not in consumer.ownership
            assert not store.get_message(parent["id"], "beta")["execution_pending"]
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)


class MailboxReadLedger:
    def __init__(self, database):
        self.database = database
        self.payload_reads = 0
        self.largest_payload_batch = 0

    def __getattr__(self, name):
        return getattr(self.database, name)

    def execute(self, *args, **kwargs):
        cursor = self.database.execute(*args, **kwargs)
        ledger = self

        class Cursor:
            def record(self, rows):
                count = sum(
                    any(isinstance(value, str) and "queued-payload-proof" in value for value in row)
                    for row in rows
                )
                ledger.payload_reads += count
                ledger.largest_payload_batch = max(ledger.largest_payload_batch, count)
                return rows

            def fetchall(self):
                return self.record(cursor.fetchall())

            def fetchone(self):
                row = cursor.fetchone()
                self.record([row] if row else [])
                return row

            def __iter__(self):
                for row in cursor:
                    self.record([row])
                    yield row

            def __getattr__(self, name):
                return getattr(cursor, name)

        return Cursor()


@pytest.mark.parametrize("blocked", [False, True])
def test_mailbox_claim_bounds_payload_reads_and_crosses_ineligible_batches(tmp_path, blocked):
    now = [1000.0]
    store = CollaborationStore(
        tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: now[0]
    )
    try:
        p = project(store, max_turns=200, max_workers=1)
        work = []
        for i in range(130):
            now[0] += 1
            item = ask(
                store, p["id"], key="queued-" + str(i), text="queued-payload-proof" + "x" * 4000
            )
            if blocked and i < 129:
                item["not_before"] = now[0] + 1000
                store.save_message(item)
            work.append(item)
        ledger = MailboxReadLedger(store.db)
        store.db = ledger
        claimed = store.claim_next("beta", "worker")
        assert claimed["id"] == work[-1 if blocked else 0]["id"]
        assert ledger.largest_payload_batch <= 32
        if not blocked:
            assert ledger.payload_reads <= 2
        else:
            assert ledger.payload_reads == len(work)
            store.reply(claimed["id"], "beta", "Completed eligible request", claim=claimed["claim"])
            now[0] += 2000
            prior_elapsed = store.elapsed
            store.elapsed = lambda: prior_elapsed() + 2000
            assert store.claim_next("beta", "worker")["id"] == work[0]["id"]
    finally:
        store.close()


def test_mailbox_batches_preserve_priority_creation_and_id_ties(tmp_path):
    now = 1000.0
    store = CollaborationStore(tmp_path / "hub.db", {"alpha", "beta", "gamma"}, clock=lambda: now)
    try:
        p = project(store, max_turns=200)
        template = ask(store, p["id"])
        template["not_before"] = now + 100
        store.save_message(template)
        expected = []
        for kind in ("ask", "result", "cancel"):
            for i in reversed(range(42)):
                item = {
                    **template,
                    "id": f"{kind}-{i:03}",
                    "key": f"{kind}-{i:03}",
                    "kind": kind,
                    "created": now - 2 if i == 40 else now - 1,
                    "not_before": now + 100 if i < 40 else now - 1,
                }
                store.save_message(item)
            expected[0:0] = [f"{kind}-040", f"{kind}-041"]
        observed = []
        for _ in expected:
            item = store.claim_next("beta", "worker")
            observed.append(item["id"])
            store.update(item["id"], "beta", "worker", "done", "Handled", claim=item["claim"])
        assert observed == expected
        assert store.claim_next("beta", "worker") is None
        now += 200
        prior_elapsed = store.elapsed
        store.elapsed = lambda: prior_elapsed() + 200
        tied = store.claim_next("beta", "worker", controls_only=True)
        assert tied["id"] == "cancel-000"
    finally:
        store.close()


@pytest.mark.parametrize("delivery", ["foreground", "injected"])
@pytest.mark.parametrize("background", ["tool", "worker"])
async def test_early_reply_keeps_native_owner_until_background_finishes(
    tmp_path, delivery, background
):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    broker.capabilities.steering = delivery == "injected"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    handling = None
    observed_terminal = asyncio.Event()
    actual_request = consumer.hub.request

    def update(completed=False):
        if background == "worker":
            return SessionUpdate(
                "existing-native",
                "subagent_activity",
                raw={"worker_id": "child", "state": "completed" if completed else "running"},
            )
        return SessionUpdate(
            "existing-native",
            "tool_call_update" if completed else "tool_call",
            tool_call_id="background-tool",
            tool_status="completed" if completed else "in_progress",
        )

    async def observe(method, path, body=None):
        result = await actual_request(method, path, body)
        if method == "GET" and path == "/messages/" + claimed["id"] and result["state"] == "done":
            observed_terminal.set()
        return result

    consumer.hub.request = observe
    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            await options["on_update"](update())
            store.reply(request["id"], "beta", "Explicit early result", claim=claimed["claim"])
            return TurnResult("existing-native", "end_turn", "Completed authorized work", "")

        async def inject(_thread, _prompt, **_options):
            gateway._observe_background_update(update())
            gateway._last_terminal["discussion"] = {
                "session_id": "existing-native",
                "execution_id": owned["execution_id"],
                "answer": "Completed authorized work",
                "stop_reason": "end_turn",
                "pending": True,
                "native_dispatched": True,
                "completed_at": store.clock(),
            }
            return "injected"

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        if delivery == "injected":
            owned = idle_report()
            owned["collaboration_project"] = p["id"]
            gateway._reports["existing-native"] = owned
            gateway.sessions.hint = AsyncMock(side_effect=inject)
        handling = asyncio.create_task(consumer.handle(claimed))
        if delivery == "injected":
            async with asyncio.timeout(2):
                while (
                    store.get_message(request["id"], "beta")["evidence"].get("delivery")
                    != "injected"
                ):
                    await asyncio.sleep(0)
            store.reply(request["id"], "beta", "Explicit early result", claim=claimed["claim"])
        await asyncio.wait_for(observed_terminal.wait(), 2)
        assert not handling.done() and consumer.owns_thread("discussion")
        assert consumer.ownership[(request["id"], claimed["claim"])]["reporter"].has_pending_work()
        await gateway.run_prompt(
            thread_id="discussion", channel_id="floor", prompt="User follow-up"
        )
        assert gateway.sessions.run_turn.await_count == (1 if delivery == "foreground" else 0)
        gateway._observe_background_update(update(completed=True))
        await asyncio.wait_for(handling, 3)
        assert not consumer.owns_thread("discussion")
        settled = store.get_message(request["id"], "beta")
        assert settled["result"] == "Explicit early result"
        assert settled["evidence"]["foreground_answer"] == "Completed authorized work"
        assert not settled["evidence"]["pending"]
    finally:
        if handling and not handling.done():
            handling.cancel()
            await asyncio.gather(handling, return_exceptions=True)
        await consumer.close()
        await close_reports(gateway)
        await close_api(store, server, clients)


@pytest.mark.parametrize("pending", ["foreground", "tool", "worker"])
async def test_project_cancel_stops_native_execution_after_early_result(tmp_path, pending):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    broker.capabilities.steering = False
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    entered, release = asyncio.Event(), asyncio.Event()
    handling = cancelling = None

    def background_update(completed=False):
        if pending == "worker":
            return SessionUpdate(
                "existing-native",
                "subagent_activity",
                raw={"worker_id": "early-worker", "state": "completed" if completed else "running"},
            )
        return SessionUpdate(
            "existing-native",
            "tool_call_update" if completed else "tool_call",
            tool_call_id="early-tool",
            tool_status="completed" if completed else "in_progress",
        )

    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        unrelated = project(store)
        untouched = ask(store, unrelated["id"], recipient="gamma")

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            if pending != "foreground":
                await options["on_update"](background_update())
            store.reply(
                request["id"],
                "beta",
                "Explicit early result",
                evidence={"artifact": "https://example.com/proof"},
                claim=claimed["claim"],
            )
            entered.set()
            if pending == "foreground":
                await release.wait()
                return TurnResult("existing-native", "cancelled", "Native execution stopped", "")
            return TurnResult("existing-native", "end_turn", "Native foreground completed", "")

        async def cancel(_thread, **options):
            assert options["expected_session"] == "existing-native"
            assert options["guard"]()
            if pending == "foreground":
                release.set()
            else:
                gateway._observe_background_update(background_update(completed=True))

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        gateway.sessions.cancel = AsyncMock(side_effect=cancel)
        handling = asyncio.create_task(consumer.handle(claimed))
        await asyncio.wait_for(entered.wait(), 2)
        if pending != "foreground":
            async with asyncio.timeout(2):
                while "discussion" not in gateway._last_terminal:
                    await asyncio.sleep(0)
        assert store.get_message(request["id"], "beta")["state"] == "done"
        store.cancel(p["id"], "alpha")
        assert store.show(p["id"], "alpha")["status"] == "cancelling"
        control = store.claim_next("beta", consumer.instance)
        assert control and control["kind"] == "cancel"
        assert control["parent_claim"] == claimed["claim"]
        cancelling = asyncio.create_task(consumer.handle(control))
        await asyncio.wait_for(asyncio.gather(handling, cancelling), 4)
        gateway.sessions.cancel.assert_awaited_once()
        settled = store.get_message(request["id"], "beta")
        assert settled["state"] == "done" and settled["result"] == "Explicit early result"
        assert settled["evidence"]["artifact"] == "https://example.com/proof"
        assert not settled["execution_pending"] and not settled["execution_cancelling"]
        assert store.show(p["id"], "alpha")["status"] == "cancelled"
        assert store.show(unrelated["id"], "alpha")["status"] == "active"
        assert store.get_message(untouched["id"], "gamma")["state"] == "queued"
        assert not consumer.owns_thread("discussion")
        assert (
            len([m for m in store.show(p["id"], "alpha")["messages"] if m["kind"] == "result"]) == 1
        )
    finally:
        release.set()
        tasks = [task for task in [handling, cancelling] if task]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.close()
        await close_reports(gateway)
        await close_api(store, server, clients)


@pytest.mark.parametrize(
    "mismatch", ["session", "execution", "pending", "reservation", "no_dispatch", "nan"]
)
async def test_native_settlement_cannot_clear_early_result_cancellation_with_invalid_receipt(
    tmp_path, mismatch
):
    async with retained_parent(tmp_path) as (store, _clients, _gateway, consumer, p, parent, _old):
        store.reply(parent["id"], "beta", "Explicit early result", claim=parent["claim"])
        store.cancel(p["id"], "alpha")
        evidence = consumer.foreground(parent, terminal())
        if mismatch == "session":
            evidence["session_id"] = "another-session"
        elif mismatch == "execution":
            evidence["execution_id"] = "another-execution"
        elif mismatch == "pending":
            evidence["pending"] = True
        elif mismatch == "reservation":
            evidence["reservation_id"] = "stale-reservation"
        elif mismatch == "no_dispatch":
            evidence["not_dispatched"] = True
        else:
            evidence["completed_at"] = float("nan")
        with pytest.raises(ValueError, match="settlement"):
            store.update(
                parent["id"],
                "beta",
                consumer.instance,
                "execution_settled",
                evidence=evidence,
                claim=parent["claim"],
            )
        unresolved = store.get_message(parent["id"], "beta")
        assert unresolved["execution_pending"] and unresolved["execution_cancelling"]
        assert unresolved["result"] == "Explicit early result"
        assert store.show(p["id"], "alpha")["status"] == "cancelling"
        store.update(
            parent["id"],
            "beta",
            consumer.instance,
            "execution_settled",
            evidence=consumer.foreground(parent, terminal()),
            claim=parent["claim"],
        )
        assert store.show(p["id"], "alpha")["status"] == "cancelled"
        assert store.get_message(parent["id"], "beta")["state"] == "done"


async def test_early_result_retains_worker_slot_and_admission_claim_until_native_settlement(
    tmp_path,
):
    async with retained_parent(tmp_path) as (store, _clients, _gateway, consumer, p, parent, _old):
        p["limits"]["max_workers"] = 1
        with store.transaction():
            store.save_project(p)
        store.reply(parent["id"], "beta", "Explicit early result", claim=parent["claim"])
        next_request = ask(store, p["id"], recipient="gamma", key="next-worker")
        assert store.claim_next("gamma", "another-worker") is None
        assert [item["id"] for item in store.active_claims("beta", consumer.instance)] == [
            parent["id"]
        ]
        store.update(
            parent["id"],
            "beta",
            consumer.instance,
            "execution_settled",
            evidence=consumer.foreground(parent, terminal()),
            claim=parent["claim"],
        )
        assert store.active_claims("beta", consumer.instance) == []
        assert store.claim_next("gamma", "another-worker")["id"] == next_request["id"]


async def test_admission_cannot_forget_unobserved_native_execution_after_early_reply(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, _gateway, consumer, _p, parent, _old):
        store.reply(parent["id"], "beta", "Explicit early result", claim=parent["claim"])
        consumer.ownership.pop((parent["id"], parent["claim"]))
        consumer.claim_uncertain = True
        with pytest.raises(RuntimeError, match="native reconciliation"):
            await consumer.set_admission("sealed")
        assert consumer.claim_uncertain
        assert store.get_message(parent["id"], "beta")["execution_pending"]


async def test_posted_result_has_no_native_debt_and_cold_sidecar_can_seal(tmp_path):
    store, server, clients = await api_fixture(tmp_path, sidecar_actors={"alpha"})
    gateway = active_gateway(steering=False, busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.collab_sidecar = True
    gateway.config.collab_origin_actor = "alpha"
    gateway.client.user_id = "alpha-user"
    gateway.config.slash_port = 0
    gateway.config.slash_tokens = frozenset({"synthetic-slash-token"})
    gateway.sessions.broker = AcpBroker()
    gateway.sessions.busy_threads = set()
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    gateway.collaboration = consumer
    published = {}

    async def read(post):
        return (
            {"id": "origin", "channel_id": "home", "root_id": ""} if post == "origin" else published
        )

    async def publish(channel, message, *, root_id):
        published.update(id="notice", channel_id=channel, root_id=root_id, message=message)
        return published

    gateway.client.get_post = AsyncMock(side_effect=read)
    gateway.client.create_post = AsyncMock(side_effect=publish)
    maintenance = None
    try:
        p = sidecar_project(store)
        peer = ask(store, p["id"], recipient="beta")
        claimed = store.claim_next("beta", "peer")
        store.reply(peer["id"], "beta", "Verified result", claim=claimed["claim"])
        delivery = store.claim_next("alpha", consumer.instance)
        await consumer.handle(delivery)
        posted = store.get_message(delivery["id"], "alpha")
        assert posted["state"] == "done" and not posted["execution_pending"]
        assert "post_started" in posted["evidence"]
        assert "dispatch_started" not in posted["evidence"]
        assert store.active_claims("alpha", consumer.instance) == []
        consumer.claim_uncertain = True
        maintenance = await maintenance_client(gateway)
        headers = {"Authorization": "Bearer " + clients["alpha"].settings.token}
        assert (
            await maintenance.post("/maintenance", json={"action": "drain"}, headers=headers)
        ).status == 200
        assert (
            await maintenance.post("/maintenance", json={"action": "seal"}, headers=headers)
        ).status == 200
        assert gateway.sealed and not consumer.claim_uncertain
        assert not gateway.sessions.broker.is_running
        gateway.run_prompt.assert_not_awaited()
    finally:
        if maintenance:
            await maintenance.close()
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("retained", ["post-started", "post-receipt", "receipt-only"])
async def test_orphaned_post_attempt_cannot_requeue_or_duplicate_before_lease_expiry(
    tmp_path, retained
):
    store, server, clients = await api_fixture(tmp_path, sidecar_actors={"alpha"})
    now = [1000.0]
    store.clock = lambda: now[0]
    gateway = active_gateway(steering=False, busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.collab_sidecar = True
    gateway.config.collab_origin_actor = "alpha"
    gateway.client.user_id = "alpha-user"
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    gateway.collaboration = consumer
    posts = []

    async def publish(channel, message, *, root_id):
        post = {
            "id": "accepted-notification",
            "channel_id": channel,
            "root_id": root_id,
            "message": message,
        }
        posts.append(post)
        return post

    async def read(post):
        return (
            {"id": "origin", "channel_id": "home", "root_id": ""} if post == "origin" else posts[0]
        )

    gateway.client.create_post = AsyncMock(side_effect=publish)
    gateway.client.get_post = AsyncMock(side_effect=read)
    actual_update = consumer.update

    async def storage_outage(item, state, *args, **kwargs):
        if state in {"done", "interrupted"} or (
            state == "heartbeat" and retained == "post-started"
        ):
            raise OSError("Synthetic short hub storage outage")
        return await actual_update(item, state, *args, **kwargs)

    consumer.update = storage_outage
    try:
        p = sidecar_project(store)
        work = ask(store, p["id"])
        peer = store.claim_next("beta", "peer-worker")
        store.reply(work["id"], "beta", "Verified result", claim=peer["claim"])
        delivery = store.claim_next("alpha", consumer.instance)
        await consumer.handle(delivery)
        orphan = store.get_message(delivery["id"], "alpha")
        assert orphan["state"] == "running" and orphan["lease_until"] > now[0]
        assert len(posts) == 1 and not consumer.workers and not consumer.ownership
        assert "post_started" in orphan["evidence"] and not orphan["execution_pending"]
        if retained != "post-started":
            assert orphan["evidence"]["result_post_id"] == posts[0]["id"]
        else:
            assert "result_post_id" not in orphan["evidence"]
        if retained == "receipt-only":
            orphan["evidence"].pop("post_started")
            store.save_message(orphan)
        consumer.update = actual_update
        consumer.claim_uncertain = True
        with pytest.raises(RuntimeError, match="external-post reconciliation"):
            await consumer.set_admission("controls")
        assert consumer.claim_uncertain
        assert store.get_message(delivery["id"], "alpha") == orphan
        assert store.claim_next("alpha", "replacement-worker") is None
        assert len(posts) == 1
        now[0] += 46
        assert store.claim_next("alpha", "replacement-worker") is None
        interrupted = store.get_message(delivery["id"], "alpha")
        assert interrupted["state"] == "interrupted"
        assert interrupted["evidence"] == orphan["evidence"]
        verified = await gateway.client.get_post(posts[0]["id"])
        assert verified == posts[0]
        assert consumer.claim_uncertain and len(posts) == 1
        gateway.run_prompt.assert_not_awaited()
        gateway.sessions.cancel.assert_not_awaited()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


async def test_plain_text_unauthorized_hub_response_is_definite_rejection_without_native_fallback(
    tmp_path,
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.allow_user_ids = frozenset({"alpha-user"})
    gateway.config.all_channels = False
    gateway.config.exclude_channel_ids = frozenset()
    gateway.config.text_slash = False
    invalid = Settings(clients["beta"].settings.url, "invalid-actor-credential" * 3)
    consumer = CollaborationConsumer(gateway, invalid)
    gateway.collaboration = consumer
    try:
        p = project(store)
        before = store.show(p["id"], "alpha")["messages"]
        with pytest.raises(HubError, match="HTTP 401: Actor credential required") as failure:
            await consumer.hub.request("GET", "/identity")
        assert failure.value.status == 401
        await gateway.handle_posted(
            PostedEvent(
                post_id="rejected-post",
                channel_id="floor",
                user_id="alpha-user",
                message="@beta ASK: Verify the result",
                root_id="discussion",
            )
        )
        gateway.run_prompt.assert_not_awaited()
        gateway.sessions.store.get.assert_not_awaited()
        assert store.show(p["id"], "alpha")["messages"] == before
        assert len(gateway.client.posts) == 1
        notice = gateway.client.posts[0][1]
        assert "rejected" in notice and "unconfirmed" not in notice
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("length", [8000, 16000])
async def test_dense_result_posts_once_with_full_footer_and_durable_evidence(tmp_path, length):
    store, server, clients = await api_fixture(tmp_path)
    posts, reads = [], []

    async def create(request):
        body = await request.json()
        if len(body["message"]) > 16383:
            return web.json_response({"error": "Message exceeds Mattermost limit"}, status=400)
        post = {**body, "id": "notification"}
        posts.append(post)
        return web.json_response(post)

    async def read(request):
        post_id = request.match_info["post"]
        reads.append(post_id)
        if post_id == "origin":
            return web.json_response({"id": "origin", "channel_id": "home", "root_id": ""})
        assert post_id == "notification"
        return web.json_response(posts[0])

    app = web.Application()
    app.router.add_post("/api/v4/posts", create)
    app.router.add_get("/api/v4/posts/{post}", read)
    mm_server = TestServer(app)
    await mm_server.start_server()
    client = MattermostClient(str(mm_server.make_url("/")).rstrip("/"), "synthetic-mm-token")
    client.me = {"id": "alpha-user", "username": "alpha"}
    gateway = active_gateway(steering=False, busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.collab_sidecar = True
    gateway.client = client
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    gateway.collaboration = consumer
    answer = "@" * length
    provenance = {
        "session_id": "peer-native",
        "execution_id": "peer-execution",
        "artifact": "https://example.invalid/evidence",
    }
    try:
        p = store.create_project(
            "alpha",
            {
                "title": "@" * 200,
                "objective": "Verify bounded result delivery",
                "members": ["alpha", "beta", "gamma"],
                "channel_id": "floor",
                "thread_id": "discussion",
                "origin_channel_id": "home",
                "origin_thread_id": "origin",
            },
        )
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", "peer-worker")
        store.reply(request["id"], "beta", answer, claim=claimed["claim"], evidence=provenance)
        delivery = store.claim_next("alpha", consumer.instance)
        await consumer.handle(delivery)
        saved = store.get_message(delivery["id"], "alpha")
        assert saved["state"] == "done" and saved["result"] == answer
        assert saved["text"] == answer and saved["result_evidence"] == provenance
        assert not saved["execution_pending"] and "dispatch_started" not in saved["evidence"]
        assert saved["evidence"]["result_post_id"] == "notification"
        assert "post_started" in saved["evidence"]
        assert store.active_claims("alpha", consumer.instance) == [] and not consumer.ownership
        assert len(posts) == 1 and reads == ["origin", "notification"]
        notice = posts[0]["message"]
        assert posts[0]["channel_id"] == "home" and posts[0]["root_id"] == "origin"
        assert len(notice) <= 15000 and not re.search(r"@(?!\u200b)", notice)
        assert notice.endswith(
            f"Request: `{request['id']}`. Full result and evidence: `mm-work show {p['id']}`."
        )
        assert len(notice) < len(answer.replace("@", "@\u200b"))
        store.reply(request["id"], "beta", answer, claim=claimed["claim"], evidence=provenance)
        await consumer.handle(delivery)
        assert len(posts) == 1 and reads == ["origin", "notification"]
        assert store.claim_next("alpha", consumer.instance) is None
        parent = store.get_message(request["id"], "alpha")
        assert parent["result"] == answer and parent["evidence"] == provenance
        gateway.sessions.store.get.assert_not_awaited()
        gateway.sessions.hint.assert_not_awaited()
        gateway.sessions.cancel.assert_not_awaited()
        gateway.run_prompt.assert_not_awaited()
    finally:
        await consumer.close()
        await client.close()
        await mm_server.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("change", ["thread", "workspace"])
async def test_binding_cannot_move_early_replys_pending_native_execution(tmp_path, change):
    async with retained_parent(tmp_path) as (store, _clients, _gateway, consumer, p, parent, _old):
        store.reply(parent["id"], "beta", "Explicit early result", claim=parent["claim"])
        binding = {
            "thread_id": "another-thread" if change == "thread" else "discussion",
            "channel_id": "floor",
        }
        if change == "workspace":
            binding["workspace"] = str(tmp_path)
        original = store.get_project(p["id"], "beta")["bindings"]["beta"]
        with pytest.raises(ValueError, match="active requests"):
            store.bind(p["id"], "beta", binding)
        assert store.get_project(p["id"], "beta")["bindings"]["beta"] == original
        store.update(
            parent["id"],
            "beta",
            consumer.instance,
            "execution_settled",
            evidence=consumer.foreground(parent, terminal()),
            claim=parent["claim"],
        )
        assert store.bind(p["id"], "beta", binding) == binding


@pytest.mark.parametrize("failure", ["reply", "reply_lost_receipt", "prompt_required"])
async def test_proven_prewrite_outcomes_leave_no_false_native_claim(tmp_path, failure):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    original = consumer.update
    try:
        p = project(store)
        request = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)
        if failure == "prompt_required":
            gateway._reports["existing-native"] = idle_report()
            gateway.sessions.hint = AsyncMock(return_value="promptRequired")
            gateway.sessions.ensure_session = AsyncMock(
                side_effect=RuntimeError("Preparation failed before native work")
            )
        else:
            broker.capabilities.steering = False

            async def reply_during_reservation(item, state, *args, **kwargs):
                receipt = await original(item, state, *args, **kwargs)
                if state == "dispatch":
                    store.reply(
                        request["id"], "beta", "Explicit prewrite result", claim=claimed["claim"]
                    )
                    if failure == "reply_lost_receipt":
                        raise HubError("Reservation response lost before native prompt")
                    return store.get_message(item["id"], "beta")
                return receipt

            consumer.update = reply_during_reservation
        await consumer.handle(claimed)
        receipt = store.get_message(request["id"], "beta")
        assert not receipt["execution_pending"]
        assert store.active_claims("beta", consumer.instance) == []
        assert writer.messages == []
        assert not consumer.owns_thread("discussion")
        if failure != "prompt_required":
            assert receipt["state"] == "done" and receipt["result"] == "Explicit prewrite result"
            assert receipt["evidence"]["not_dispatched"]
        else:
            assert receipt["state"] == "queued"
    finally:
        await consumer.close()
        await close_reports(gateway)
        await close_api(store, server, clients)


@pytest.mark.parametrize("mismatch", ["claim", "session_id", "execution_id", "native_epoch"])
async def test_undispatched_proof_cannot_clear_a_changed_reservation(tmp_path, mismatch):
    async with retained_parent(tmp_path) as (store, _clients, _gateway, consumer, p, parent, _old):
        store.reply(parent["id"], "beta", "Explicit prewrite result", claim=parent["claim"])
        store.cancel(p["id"], "alpha")
        evidence = {
            **store.get_message(parent["id"], "beta")["evidence"],
            "not_dispatched": True,
            "native_dispatched": False,
        }
        claim = parent["claim"]
        if mismatch == "claim":
            claim = "stale-claim"
        else:
            evidence[mismatch] = "changed-reservation"
        with pytest.raises(PermissionError if mismatch == "claim" else ValueError):
            store.update(
                parent["id"],
                "beta",
                consumer.instance,
                "undispatched",
                evidence=evidence,
                claim=claim,
            )
        current = store.get_message(parent["id"], "beta")
        assert current["execution_pending"] and current["execution_cancelling"]
        assert current["result"] == "Explicit prewrite result"
        assert store.show(p["id"], "alpha")["status"] == "cancelling"


async def test_old_worker_cleanup_preserves_new_claim_owner_and_cancellation_fence(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, _p, parent, old):
        entered, release = asyncio.Event(), asyncio.Event()
        actual_request = consumer.hub.request
        delayed = False

        async def delayed_read(method, path, body=None):
            nonlocal delayed
            if method == "GET" and path == "/messages/" + parent["id"] and not delayed:
                delayed = True
                entered.set()
                await release.wait()
            return await actual_request(method, path, body)

        consumer.hub.request = delayed_read
        handling = asyncio.create_task(consumer.handle(dict(parent)))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            store.update(
                parent["id"], "beta", consumer.instance, "unconfirmed", "Old delivery lost"
            )
            store.retry(parent["id"], "beta", "Old execution reconciled")
            fresh = store.claim_next("beta", consumer.instance)
            assert fresh["claim"] != parent["claim"]
            consumer.release_ownership((parent["id"], parent["claim"]))
            current = idle_report()
            current.update(
                execution_id="new-execution",
                collaboration_project=old["collaboration_project"],
                active=True,
                foreground_complete=False,
            )
            current["reporter"].active_tools.add("pending-new-work")
            gateway._reports["existing-native"] = current
            assert await consumer.reserve(fresh, "existing-native")
            store.reply(fresh["id"], "beta", "New early result", claim=fresh["claim"])
            key = (fresh["id"], fresh["claim"])
            consumer.cancelled_claims.add(key)
            release.set()
            await asyncio.wait_for(handling, 3)
            assert consumer.ownership[key] is current
            assert key in consumer.cancelled_claims and consumer.owns_thread("discussion")
            assert current["reporter"].has_pending_work()
            gateway.run_prompt.assert_not_awaited()
        finally:
            release.set()
            if not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)


@pytest.mark.parametrize("reused", [False, True])
async def test_duplicate_floor_ask_after_cancellation_cannot_wake_new_native_work(tmp_path, reused):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.allow_user_ids = frozenset({"alpha-user"})
    gateway.config.all_channels = False
    gateway.config.exclude_channel_ids = frozenset()
    gateway.config.text_slash = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    posted = PostedEvent(
        post_id="post-1",
        channel_id="floor",
        user_id="alpha-user",
        message="@beta ASK: Verify the dependency",
        root_id="discussion",
    )
    body = {
        "post_id": posted.post_id,
        "author_id": posted.user_id,
        "channel_id": posted.channel_id,
        "thread_id": posted.root_id,
        "text": posted.message,
    }
    try:
        p = project(store)
        admitted = await clients["beta"].request("POST", "/mentions", body)
        store.cancel(p["id"], "alpha")
        if reused:
            store.create_project(
                "alpha",
                {
                    "title": "Next project",
                    "objective": "Use the same discussion",
                    "members": ["alpha", "beta", "gamma"],
                    "channel_id": "floor",
                    "thread_id": "discussion",
                    "origin_channel_id": "home",
                    "origin_thread_id": "origin",
                },
            )
        duplicate = await clients["beta"].request("POST", "/mentions", body)
        assert duplicate["id"] == admitted["id"] and duplicate["state"] == "cancelled"
        await gateway.handle_posted(posted)
        gateway.run_prompt.assert_not_awaited()
        all_messages = [
            m
            for item in store.list_projects("alpha")
            for m in store.show(item["id"], "alpha")["messages"]
        ]
        assert len(all_messages) == 1 and all_messages[0]["id"] == admitted["id"]
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("missing", [None, "completed_at", "dispatch_started"])
async def test_late_reply_cannot_reconcile_with_receipt_before_new_injection(tmp_path, missing):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, _p, parent, old):
        initial = store.get_message(parent["id"], "beta")["evidence"]["dispatch_started"]
        receipt = gateway._last_terminal["discussion"]
        receipt["completed_at"] = initial
        receipt["answer"] = "Output before the new injection"
        store.clock = lambda: initial + 1
        assert await consumer.reserve(parent, "existing-native")
        await consumer.update(parent, "unconfirmed", "New injection receipt unavailable")
        settled = store.reply(parent["id"], "beta", "Explicit late result", claim=parent["claim"])
        expected = dict(settled["evidence"])
        if missing == "completed_at":
            receipt.pop("completed_at")
        elif missing == "dispatch_started":
            settled["evidence"].pop("dispatch_started")
            with store.transaction():
                store.save_message(settled)
            expected = dict(settled["evidence"])

        await consumer.reconcile_ownership()
        assert consumer.ownership[(parent["id"], parent["claim"])] is old
        assert consumer.owns_thread("discussion")
        assert store.get_message(parent["id"], "beta")["evidence"] == expected

        if missing == "dispatch_started":
            settled["evidence"]["dispatch_started"] = initial + 1
            with store.transaction():
                store.save_message(settled)
        receipt.update(completed_at=initial + 1, answer="Output after the new injection")
        await consumer.reconcile_ownership()
        assert not consumer.owns_thread("discussion")
        final = store.get_message(parent["id"], "beta")
        assert final["result"] == "Explicit late result"
        assert final["evidence"]["foreground_answer"] == "Output after the new injection"


@pytest.mark.parametrize("limit", ["dispatch", "expiry", "membership", "sender-membership"])
async def test_rejected_project_ask_cannot_fall_through_to_allowlisted_native_wake(tmp_path, limit):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.allow_user_ids = frozenset({"alpha-user"})
    gateway.config.all_channels = False
    gateway.config.exclude_channel_ids = frozenset()
    gateway.config.text_slash = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    try:
        if "membership" in limit:
            p = store.create_project(
                "gamma" if limit == "sender-membership" else "alpha",
                {
                    "title": "Restricted participants",
                    "objective": "Verify project admission",
                    "members": ["beta", "gamma"]
                    if limit == "sender-membership"
                    else ["alpha", "gamma"],
                    "channel_id": "floor",
                    "thread_id": "discussion",
                    "origin_channel_id": "home",
                    "origin_thread_id": "origin",
                },
            )
        else:
            p = project(store, max_turns=1)
        if limit == "dispatch":
            ask(store, p["id"])
        elif limit == "expiry":
            store.clock = lambda: p["deadline"] + 1
        posted = PostedEvent(
            post_id="post-1",
            channel_id="floor",
            user_id="alpha-user",
            message="@beta ASK: Verify the dependency",
            root_id="discussion",
        )
        with pytest.raises(
            HubError, match="HTTP 403" if "membership" in limit else "HTTP 409"
        ) as rejection:
            await clients["beta"].request(
                "POST",
                "/mentions",
                {
                    "post_id": posted.post_id,
                    "author_id": posted.user_id,
                    "channel_id": posted.channel_id,
                    "thread_id": posted.root_id,
                    "text": posted.message,
                },
            )
        assert rejection.value.status == (403 if "membership" in limit else 409)
        before = store.show(p["id"], p["owner"])["messages"]
        before_budget = store.get_project(p["id"], p["owner"])["dispatched"]
        await gateway.handle_posted(posted)
        gateway.run_prompt.assert_not_awaited()
        assert store.show(p["id"], p["owner"])["messages"] == before
        assert store.get_project(p["id"], p["owner"])["dispatched"] == before_budget
        assert len(gateway.client.posts) == 1
        channel, notice, root = gateway.client.posts[0]
        assert channel == "floor" and root == "discussion"
        assert "rejected" in notice and "unconfirmed" not in notice
        assert "@" not in notice and "ASK:" not in notice

        unregistered = PostedEvent(
            post_id="post-2",
            channel_id="floor",
            user_id="alpha-user",
            message=posted.message,
            root_id="unregistered",
        )
        await gateway.handle_posted(unregistered)
        gateway.run_prompt.assert_awaited_once()
        assert gateway.run_prompt.call_args.kwargs["thread_id"] == "unregistered"
    finally:
        await consumer.close()
        await close_api(store, server, clients)


def test_message_expiry_index_upgrades_history_and_preserves_stale_lease_expiry(tmp_path):
    now = [1000.0]
    path = tmp_path / "hub.db"
    actors = {"alpha", "beta", "gamma"}
    store = CollaborationStore(path, actors, clock=lambda: now[0])
    try:
        p = project(store)
        completed = ask(store, p["id"])
        store.claim_next("beta", "history-worker")
        store.reply(completed["id"], "beta", "Completed historical task")
        completed = store.get_message(completed["id"], "beta")
        with store.transaction():
            for index in range(256):
                store.save_message(
                    {**completed, "id": f"history-{index}", "key": f"history-{index}"}
                )
            store.db.execute("DROP INDEX IF EXISTS message_expiry")
    finally:
        store.close()

    reopened = CollaborationStore(path, actors, clock=lambda: now[0])
    try:
        plan = reopened.db.execute(
            "EXPLAIN QUERY PLAN SELECT data FROM messages WHERE state='running'"
        ).fetchall()
        assert any("SEARCH messages USING INDEX message_expiry" in row[3] for row in plan)
        request = ask(reopened, p["id"], key="current-request")
        assert reopened.claim_next("beta", "current-worker")["id"] == request["id"]
        now[0] += 46
        reopened.expire()
        assert reopened.get_message(request["id"], "beta")["state"] == "interrupted"
        assert (
            reopened.db.execute("SELECT count(*) FROM messages WHERE state='done'").fetchone()[0]
            == 257
        )
    finally:
        reopened.close()


@pytest.mark.parametrize("directory_failure", [False, True])
async def test_stop_intent_syncs_directory_after_replacement_and_closes_on_failure(
    tmp_path, monkeypatch, directory_failure
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway()
    gateway.config.db_path = tmp_path / "nested" / "project.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    calls = []
    real_fsync = os.fsync

    def sync(descriptor):
        directory = stat.S_ISDIR(os.fstat(descriptor).st_mode)
        calls.append((descriptor, directory))
        if directory:
            assert json.loads(consumer.stops_path.read_text()) == consumer.pending_stops
            if directory_failure:
                raise OSError("Directory synchronization unavailable")
        real_fsync(descriptor)

    try:
        consumer.pending_stops = {"origin": 1000.0}
        with monkeypatch.context() as patch:
            patch.setattr(os, "fsync", sync)
            if directory_failure:
                with pytest.raises(OSError, match="Directory synchronization unavailable"):
                    consumer.save_stops()
            else:
                consumer.save_stops()
        assert [directory for _descriptor, directory in calls] == [False, True]
        with pytest.raises(OSError):
            os.fstat(calls[-1][0])
        assert consumer.pending_stops == {"origin": 1000.0}
        restarted = CollaborationConsumer(gateway, clients["beta"].settings)
        try:
            assert restarted.pending_stops == consumer.pending_stops
        finally:
            await restarted.close()
    finally:
        await consumer.close()
        await close_api(store, server, clients)


@asynccontextmanager
async def fresh_result(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    broker.capabilities.steering = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    try:
        p = project(store)
        peer = ask(store, p["id"], sender="beta", recipient="gamma")
        claimed = store.claim_next("gamma", "peer-worker")
        store.reply(peer["id"], "gamma", "Peer verified the dependency", claim=claimed["claim"])
        delivery = store.claim_next("beta", consumer.instance)
        assert delivery["kind"] == "result" and not delivery.get("continuation_parent")
        yield store, gateway, consumer, p, delivery
    finally:
        await consumer.close()
        await close_reports(gateway)
        await close_api(store, server, clients)


async def test_active_participant_result_turn_receives_native_cancellation(tmp_path):
    async with fresh_result(tmp_path) as (store, gateway, consumer, p, delivery):
        entered, stopped = asyncio.Event(), asyncio.Event()
        handling = cancellation = None

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            assert options["dispatch_guard"]()
            entered.set()
            await stopped.wait()
            return TurnResult("existing-native", "cancelled", "Native result work stopped", "")

        async def cancel(_thread, **_options):
            stopped.set()

        gateway.sessions.run_turn = native
        gateway.sessions.cancel = AsyncMock(side_effect=cancel)
        try:
            handling = asyncio.create_task(consumer.handle(delivery))
            await asyncio.wait_for(entered.wait(), 2)
            store.cancel(p["id"], "alpha")
            assert store.get_message(delivery["id"], "beta")["state"] == "cancelling"
            assert store.show(p["id"], "alpha")["status"] == "cancelling"
            control = store.claim_next("beta", consumer.instance)
            assert control["kind"] == "cancel" and control["parent"] == delivery["id"]
            cancellation = asyncio.create_task(consumer.handle(control))
            await asyncio.wait_for(asyncio.gather(handling, cancellation), 3)
            gateway.sessions.cancel.assert_awaited_once()
            call = gateway.sessions.cancel.call_args
            assert call.args == ("discussion",)
            assert call.kwargs["expected_session"] == "existing-native"
            assert store.get_message(delivery["id"], "beta")["state"] == "cancelled"
            assert store.get_message(control["id"], "beta")["state"] == "done"
            assert store.show(p["id"], "alpha")["status"] == "cancelled"
            assert not consumer.owns_thread("discussion")
        finally:
            stopped.set()
            tasks = [task for task in [handling, cancellation] if task]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_legacy_owner_home_result_keeps_native_cancellation_exemption(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        peer = ask(store, p["id"], recipient="gamma")
        store.claim_next("gamma", "peer-worker")
        store.reply(peer["id"], "gamma", "Owner peer result")
        delivery = store.claim_next("alpha", "owner-worker")
        assert delivery["binding"] == {"channel_id": "home", "thread_id": "origin"}
        store.update(
            delivery["id"],
            "alpha",
            "owner-worker",
            "dispatch",
            evidence={"session_id": "owner-native", "execution_id": "owner-execution"},
        )
        store.cancel(p["id"], "alpha")
        assert store.get_message(delivery["id"], "alpha")["state"] == "cancelled"
        assert not [m for m in store.show(p["id"], "alpha")["messages"] if m["kind"] == "cancel"]
        assert store.claim_next("alpha", "owner-worker", controls_only=True) is None
    finally:
        await close_api(store, server, clients)


@pytest.mark.parametrize("reset", ["requeue", "retry"])
async def test_result_provenance_never_becomes_recipient_dispatch_evidence(tmp_path, reset):
    store, server, clients = await api_fixture(tmp_path)
    try:
        p = project(store)
        peer = ask(store, p["id"], sender="beta", recipient="gamma")
        original = store.claim_next("gamma", "peer-worker")
        peer_evidence = {
            "session_id": "peer-native",
            "execution_id": "peer-execution",
            "dispatch_started": store.clock(),
            "stop_reason": "end_turn",
            "pending": False,
        }
        store.reply(peer["id"], "gamma", "Peer result", evidence=peer_evidence)
        queued = next(m for m in store.show(p["id"], "beta")["messages"] if m["kind"] == "result")
        assert queued["evidence"] == {} and queued["result_evidence"] == peer_evidence
        store.update(
            peer["id"], "gamma", "peer-worker", "evidence", evidence={"model": "peer-model"}
        )
        assert store.get_message(queued["id"], "beta")["evidence"] == {}
        delivery = store.claim_next("beta", "recipient-worker")
        recipient = store.update(
            delivery["id"],
            "beta",
            "recipient-worker",
            "dispatch",
            evidence={"session_id": "recipient-native", "execution_id": "recipient-execution"},
        )
        recipient_evidence = dict(recipient["evidence"])
        store.update(
            peer["id"],
            "gamma",
            "peer-worker",
            "evidence",
            evidence={"artifacts": ["https://example.com/peer-proof"]},
            claim=original["claim"],
        )
        enriched = store.get_message(delivery["id"], "beta")
        assert enriched["evidence"] == recipient_evidence
        provenance = {
            **peer_evidence,
            "model": "peer-model",
            "artifacts": ["https://example.com/peer-proof"],
        }
        assert enriched["result_evidence"] == provenance
        if reset == "retry":
            store.update(delivery["id"], "beta", "recipient-worker", "unconfirmed", "Receipt lost")
            reset_delivery = store.retry(delivery["id"], "beta", "Recipient execution reconciled")
        else:
            reset_delivery = store.update(delivery["id"], "beta", "recipient-worker", "queued")
        assert reset_delivery["evidence"] == {}
        assert reset_delivery["result_evidence"] == provenance
        assert reset_delivery["delivery_history"][-1] == recipient_evidence
    finally:
        await close_api(store, server, clients)


async def test_lost_result_mailbox_can_drain_without_treating_peer_as_local_native_work(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway = maintenance_gateway()
    gateway.config.db_path = tmp_path / "project.db"
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    consumer.actor = "beta"
    gateway.collaboration = consumer
    headers = {"Authorization": "Bearer " + consumer.hub.settings.token}
    actual_request = consumer.hub.request
    lost = asyncio.Event()

    async def lost_response(method, path, body=None):
        result = await actual_request(method, path, body)
        if path.startswith("/mailbox?") and result and not lost.is_set():
            assert result["kind"] == "result"
            lost.set()
            raise HubError("Result claim committed but response lost")
        return result

    consumer.hub.request = lost_response
    client = await maintenance_client(gateway)
    try:
        p = project(store)
        peer = ask(store, p["id"], sender="beta", recipient="gamma")
        store.claim_next("gamma", "peer-worker")
        peer_evidence = {"session_id": "peer-native", "execution_id": "peer-execution"}
        store.reply(peer["id"], "gamma", "Peer result", evidence=peer_evidence)
        consumer.start()
        await asyncio.wait_for(lost.wait(), 2)
        await asyncio.wait_for(consumer.claim_settled.wait(), 2)
        assert consumer.claim_uncertain and not gateway.maintenance_state()["idle"]
        drained = await client.post("/maintenance", json={"action": "drain"}, headers=headers)
        assert drained.status == 200 and gateway.maintenance_state()["idle"]
        result = next(m for m in store.show(p["id"], "beta")["messages"] if m["kind"] == "result")
        assert result["state"] == "queued" and result["evidence"] == {}
        assert result["result_evidence"] == peer_evidence
        sealed = await client.post("/maintenance", json={"action": "seal"}, headers=headers)
        assert sealed.status == 200 and gateway.sealed
        gateway.run_prompt.assert_not_awaited()
        gateway.sessions.cancel.assert_not_awaited()
    finally:
        await consumer.close()
        await client.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("cancel_during", ["origin_read", "post_write"])
async def test_owner_result_notification_cancellation_never_targets_peer_native(
    tmp_path, cancel_during
):
    store, server, clients = await api_fixture(tmp_path)
    gateway = active_gateway(steering=False, busy=False)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.collab_sidecar = True
    consumer = CollaborationConsumer(gateway, clients["alpha"].settings)
    gateway.collaboration = consumer
    entered, release = asyncio.Event(), asyncio.Event()
    handling = None
    published = {}

    async def read(post_id):
        if post_id == "origin":
            if cancel_during == "origin_read":
                entered.set()
                await release.wait()
            return {"id": "origin", "channel_id": "home", "root_id": ""}
        return published

    async def publish(channel, message, *, root_id):
        published.update(id="notification", channel_id=channel, root_id=root_id, message=message)
        if cancel_during == "post_write":
            entered.set()
            await release.wait()
        return published

    gateway.client.get_post = AsyncMock(side_effect=read)
    gateway.client.create_post = AsyncMock(side_effect=publish)
    try:
        p = sidecar_project(store)
        peer = ask(store, p["id"], recipient="beta")
        store.claim_next("beta", "peer-worker")
        provenance = {"session_id": "peer-native", "execution_id": "peer-execution"}
        store.reply(peer["id"], "beta", "Peer result", evidence=provenance)
        delivery = store.claim_next("alpha", consumer.instance)
        handling = asyncio.create_task(consumer.handle(delivery))
        await asyncio.wait_for(entered.wait(), 2)
        store.cancel(p["id"], "alpha")
        assert store.get_message(delivery["id"], "alpha")["state"] == "cancelling"
        if cancel_during == "post_write":
            control = store.claim_next("alpha", consumer.instance)
            await asyncio.wait_for(consumer.handle(control), 2)
            assert store.get_message(control["id"], "alpha")["state"] == "done"
        release.set()
        await asyncio.wait_for(handling, 3)
        if cancel_during == "origin_read":
            gateway.client.create_post.assert_not_awaited()
            control = store.claim_next("alpha", consumer.instance)
            await asyncio.wait_for(consumer.handle(control), 2)
        settled = store.get_message(delivery["id"], "alpha")
        assert settled["state"] == "cancelled" and not settled["evidence"].get("session_id")
        assert settled["result_evidence"] == provenance
        assert store.show(p["id"], "alpha")["status"] == "cancelled"
        gateway.run_prompt.assert_not_awaited()
        gateway.sessions.cancel.assert_not_awaited()
    finally:
        release.set()
        if handling and not handling.done():
            handling.cancel()
            await asyncio.gather(handling, return_exceptions=True)
        await consumer.close()
        await close_api(store, server, clients)


@pytest.mark.parametrize("background", ["tool", "worker"])
async def test_result_end_turn_settles_after_background_finishes_without_reply(
    tmp_path, background
):
    async with fresh_result(tmp_path) as (store, gateway, consumer, _p, delivery):
        handling = None

        def update(completed=False):
            if background == "worker":
                return SessionUpdate(
                    "existing-native",
                    "subagent_activity",
                    raw={"worker_id": "child", "state": "completed" if completed else "running"},
                )
            return SessionUpdate(
                "existing-native",
                "tool_call_update" if completed else "tool_call",
                tool_call_id="background-tool",
                tool_status="completed" if completed else "in_progress",
            )

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            await options["on_update"](update())
            return TurnResult("existing-native", "end_turn", "Peer result incorporated", "")

        gateway.sessions.run_turn = native
        try:
            handling = asyncio.create_task(consumer.handle(delivery))
            async with asyncio.timeout(2):
                while not store.get_message(delivery["id"], "beta")["evidence"].get("pending"):
                    await asyncio.sleep(0)
            assert not handling.done()
            assert consumer.owns_thread("discussion")
            assert gateway._last_terminal["discussion"]["pending"]
            gateway._observe_background_update(update(completed=True))
            await asyncio.wait_for(handling, 3)
            settled = store.get_message(delivery["id"], "beta")
            assert settled["state"] == "done" and settled["result"] == "Peer result incorporated"
            assert settled["evidence"]["foreground_answer"] == "Peer result incorporated"
            assert settled["evidence"]["stop_reason"] == "end_turn"
            assert not settled["evidence"]["pending"]
            assert not consumer.owns_thread("discussion")
            assert not [
                m
                for m in store.show(delivery["project"], "beta")["messages"]
                if (m["kind"] == "result" and m.get("parent") == delivery["id"])
            ]
        finally:
            if handling and not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)


async def test_lost_parent_dispatch_response_restores_ownership_before_native_write(tmp_path):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        before = dict(gateway._last_terminal["discussion"])
        old_evidence = dict(store.get_message(parent["id"], "beta")["evidence"])
        broker, writer = memory_broker()
        broker.capabilities.steering = False
        sessions = SessionService(broker, gateway.sessions.store, default_cwd=tmp_path)
        sessions.ensure_session = AsyncMock(return_value="existing-native")
        sessions.status = AsyncMock(return_value={"session_id": "existing-native"})
        gateway.sessions = sessions
        gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
        delivery = child_result(store, p, parent, consumer)
        actual_update = consumer.update
        discarded = []

        async def lost_response(item, state, *args, **options):
            receipt = await actual_update(item, state, *args, **options)
            if item["id"] == parent["id"] and state == "dispatch" and not discarded:
                discarded.append(consumer.ownership[(parent["id"], parent["claim"])])
                assert receipt["evidence"]["execution_id"] == discarded[0]["execution_id"]
                raise HubError("Parent dispatch committed but response lost")
            return receipt

        consumer.update = lost_response
        await asyncio.wait_for(consumer.handle(delivery), 3)
        assert writer.messages == []
        assert discarded and discarded[0] is not old
        assert discarded[0]["task"].done()
        assert consumer.ownership[(parent["id"], parent["claim"])] is old
        restored = store.get_message(parent["id"], "beta")["evidence"]
        assert restored["session_id"] == old_evidence["session_id"]
        assert restored["execution_id"] == old_evidence["execution_id"]
        assert gateway._last_terminal["discussion"] == before
        assert gateway._reports["existing-native"] is old
        assert not consumer.reserving
        clock = store.clock
        store.clock = lambda: clock() + 3
        prior_elapsed = store.elapsed
        store.elapsed = lambda: prior_elapsed() + 3
        retried = store.claim_next("beta", consumer.instance)
        assert retried["id"] == delivery["id"]
        assert (await consumer.continuation(retried))["report"] is old

        def prompt_reply(payload):
            message = json.loads(payload)
            writer.messages.append(message)
            broker._pending[message["id"]].set_result({"stopReason": "end_turn"})

        writer.write = prompt_reply
        await asyncio.wait_for(consumer.handle(retried), 3)
        assert len(writer.messages) == 1 and writer.messages[0]["method"] == "session/prompt"
        assert store.get_message(retried["id"], "beta")["state"] == "done"
        assert store.get_message(parent["id"], "beta")["state"] == "running"


@pytest.mark.parametrize(
    "unsettled",
    [
        None,
        "tool",
        "receipt_pending",
        "foreground",
        "active",
        "busy",
        "session",
        "execution",
        "undispatched",
    ],
)
async def test_cancelled_reconciled_retry_uses_history_only_for_settled_native_owner(
    tmp_path, unsettled
):
    async with retained_parent(tmp_path) as (store, _clients, gateway, consumer, p, parent, old):
        before = dict(store.get_message(parent["id"], "beta")["evidence"])
        store.update(parent["id"], "beta", consumer.instance, "unconfirmed", "Native receipt lost")
        queued = store.retry(parent["id"], "beta", "Old native work reconciled before retry")
        assert queued["evidence"] == {} and queued["delivery_history"][-1] == before
        gateway.draining = True
        store.cancel(p["id"], "alpha")
        cancelled = store.get_message(parent["id"], "beta")
        assert cancelled["state"] == "cancelled" and cancelled["claim"] == parent["claim"]
        receipt = gateway._last_terminal["discussion"]
        if unsettled == "tool":
            old["reporter"].has_pending_work = lambda: True
        elif unsettled == "receipt_pending":
            receipt["pending"] = True
        elif unsettled == "foreground":
            old["foreground_complete"] = False
        elif unsettled == "active":
            old["active"] = True
        elif unsettled == "busy":
            gateway.sessions.is_busy = lambda _thread: True
        elif unsettled == "session":
            receipt["session_id"] = "other-native"
        elif unsettled == "execution":
            receipt["execution_id"] = "other-execution"
        elif unsettled == "undispatched":
            receipt["native_dispatched"] = False
        await consumer.reconcile_ownership()
        if unsettled:
            assert consumer.ownership[(parent["id"], parent["claim"])] is old
            assert consumer.owns_thread("discussion")
        else:
            assert not consumer.owns_thread("discussion")
            assert not consumer.ownership and "discussion" not in gateway._last_terminal
            final = store.get_message(parent["id"], "beta")
            assert final["state"] == "cancelled"
            assert final["evidence"] == {} and final["delivery_history"][-1] == before
        gateway.sessions.cancel.assert_not_awaited()
        gateway.run_prompt.assert_not_awaited()
