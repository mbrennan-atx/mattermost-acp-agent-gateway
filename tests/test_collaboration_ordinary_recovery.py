"""Ordinary chat admission preserves native state until project recovery is verified."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_collaboration import api_fixture, ask, close_api, execution_report, project
from test_collaboration_continuation_review import terminal
from test_collaboration_dispatch_review import close_reports, native_gateway

from mattermost_acp.collaboration.client import HubError
from mattermost_acp.collaboration.consumer import CollaborationConsumer


@asynccontextmanager
async def ordinary_runtime(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "ordinary.db"
    gateway.collaboration_recovery_pending = False
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(
            session_id="existing-native", harness="claude", cwd=str(tmp_path)
        )
    )
    gateway.sessions.store.harness = "claude"
    gateway.sessions.cancel = AsyncMock(wraps=gateway.sessions.cancel)
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer

    def answer(payload):
        message = json.loads(payload)
        writer.messages.append(message)
        result = {"stopReason": "end_turn"} if message["method"] == "session/prompt" else {}
        broker._pending[message["id"]].set_result(result)

    writer.write = answer
    try:
        yield store, clients, gateway, broker, writer, consumer
    finally:
        await close_reports(gateway)
        await consumer.close()
        await close_api(store, server, clients)


def pending_request(store, p, *, key="old-request", session="existing-native"):
    work = ask(store, p["id"], key=key)
    claim = store.claim_next("beta", "old-worker")
    reserved = store.update(
        work["id"],
        "beta",
        "old-worker",
        "dispatch",
        evidence={"session_id": session, "execution_id": key + "-execution"},
        claim=claim["claim"],
    )
    store.reply(work["id"], "beta", "Early correlated result", claim=claim["claim"])
    return reserved


async def settle(clients, work):
    evidence = work["evidence"]
    await clients["beta"].request(
        "POST",
        f"/messages/{work['id']}/update",
        {
            "instance": work["instance"],
            "claim": work["claim"],
            "state": "execution_settled",
            "expected_execution_id": evidence["execution_id"],
            "evidence": {
                "session_id": evidence["session_id"],
                "execution_id": evidence["execution_id"],
                "native_dispatched": True,
                "stop_reason": "end_turn",
                "pending": False,
                "completed_at": evidence["dispatch_started"] + 1,
            },
        },
    )


@pytest.mark.parametrize("stage", ["before-consumer", "reading", "unavailable", "malformed"])
async def test_ordinary_prompts_refuse_before_initial_claim_recovery(tmp_path, stage):
    async with ordinary_runtime(tmp_path) as (store, _clients, gateway, broker, writer, consumer):
        p = project(store)
        work = pending_request(store, p)
        before = store.get_message(work["id"], "beta")
        recovering, release = asyncio.Event(), asyncio.Event()
        actual = consumer.hub.request
        recovery = None

        async def initial_read(method, path, body=None):
            if path == "/claims":
                if stage == "unavailable":
                    raise HubError("Claims unavailable", status=503)
                if stage == "malformed":
                    return None
                recovering.set()
                await release.wait()
            return await actual(method, path, body)

        try:
            if stage == "before-consumer":
                gateway.collaboration = None
                gateway.collaboration_recovery_pending = True
            else:
                consumer.hub.request = initial_read
                if stage in {"unavailable", "malformed"}:
                    with pytest.raises((HubError, RuntimeError)):
                        await consumer.reconcile_inherited_claims()
                else:
                    recovery = asyncio.create_task(consumer.reconcile_inherited_claims())
                    await asyncio.wait_for(recovering.wait(), 2)
            result = await gateway.run_prompt(
                thread_id="discussion",
                channel_id="floor",
                prompt="Ordinary follow-up",
                author_id="synthetic-user",
                author_name="synthetic-user",
            )
            assert result is None and gateway.collaboration_recovering()
            gateway.sessions.ensure_session.assert_not_awaited()
            gateway.sessions.cancel.assert_not_awaited()
            assert writer.messages == [] and broker._pending == {}
            assert store.get_message(work["id"], "beta") == before
            assert any("recovery is pending" in text for _, text, _ in gateway.client.posts)
        finally:
            release.set()
            if recovery:
                await asyncio.gather(recovery, return_exceptions=True)


async def test_successful_empty_recovery_admits_ordinary_native_work(tmp_path):
    async with ordinary_runtime(tmp_path) as (_store, _clients, gateway, _broker, writer, consumer):
        assert not gateway.ordinary_admitted("discussion")
        await consumer.reconcile_inherited_claims()
        assert gateway.ordinary_admitted("discussion")
        receipt = await gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="Ordinary follow-up",
            author_id="synthetic-user",
            author_name="synthetic-user",
        )
        assert receipt["stop_reason"] == "end_turn" and receipt["native_dispatched"]
        assert [message["method"] for message in writer.messages] == ["session/prompt"]
        gateway.sessions.cancel.assert_not_awaited()


async def test_trusted_local_guard_cannot_bypass_inherited_thread_debt(tmp_path):
    async with ordinary_runtime(tmp_path) as (store, _clients, gateway, broker, writer, consumer):
        assert not gateway.ordinary_admitted("discussion", ownership_guard=lambda: True)
        p = project(store)
        work = pending_request(store, p)
        before = store.get_message(work["id"], "beta")
        await consumer.reconcile_inherited_claims()
        assert consumer.inherited_thread_pending("discussion")
        assert not gateway.ordinary_admitted("discussion", ownership_guard=lambda: True)
        receipt = await gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="Ordinary follow-up",
            author_id="synthetic-user",
            author_name="synthetic-user",
            ownership_guard=lambda: True,
        )
        assert receipt is None
        gateway.sessions.ensure_session.assert_not_awaited()
        gateway.sessions.cancel.assert_not_awaited()
        assert writer.messages == [] and broker._pending == {}
        assert store.get_message(work["id"], "beta") == before


async def test_unconfigured_gateway_preserves_ordinary_unguarded_arguments(tmp_path):
    async with ordinary_runtime(tmp_path) as (
        _store,
        _clients,
        gateway,
        _broker,
        writer,
        _consumer,
    ):
        gateway.collaboration = None
        gateway.collaboration_recovery_pending = False
        gateway.sessions.run_turn = AsyncMock(wraps=gateway.sessions.run_turn)
        receipt = await gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="Standalone ordinary work",
            author_id="synthetic-user",
            author_name="synthetic-user",
        )
        assert receipt["stop_reason"] == "end_turn" and receipt["native_dispatched"]
        options = gateway.sessions.run_turn.await_args.kwargs
        assert "before_prompt" not in options and "dispatch_guard" not in options
        assert [message["method"] for message in writer.messages] == ["session/prompt"]


@pytest.mark.parametrize("stage", ["status-delivery", "native-write"])
async def test_recovery_during_ordinary_preparation_refuses_write_and_restores_predecessor(
    tmp_path, stage
):
    async with ordinary_runtime(tmp_path) as (store, _clients, gateway, broker, writer, consumer):
        await consumer.reconcile_inherited_claims()
        p = project(store)
        work = pending_request(store, p)
        before = store.get_message(work["id"], "beta")
        predecessor = execution_report(p["id"], work["evidence"]["execution_id"])
        prior_terminal = terminal(work["evidence"]["execution_id"])
        gateway._reports["existing-native"] = predecessor
        gateway._last_terminal["discussion"] = prior_terminal
        entered, release = asyncio.Event(), asyncio.Event()
        post = gateway.client.create_post
        handling = None

        async def delayed_status(*args, **kwargs):
            entered.set()
            await release.wait()
            return await post(*args, **kwargs)

        try:
            if stage == "status-delivery":
                gateway.client.create_post = delayed_status
                handling = asyncio.create_task(
                    gateway.run_prompt(
                        thread_id="discussion",
                        channel_id="floor",
                        prompt="Ordinary follow-up",
                        author_id="synthetic-user",
                        author_name="synthetic-user",
                    )
                )
                await asyncio.wait_for(entered.wait(), 2)
            else:
                await broker._write_lock.acquire()
                handling = asyncio.create_task(
                    gateway.run_prompt(
                        thread_id="discussion",
                        channel_id="floor",
                        prompt="Ordinary follow-up",
                        author_id="synthetic-user",
                        author_name="synthetic-user",
                    )
                )
                async with asyncio.timeout(2):
                    while not broker._pending:
                        await asyncio.sleep(0)
            candidate = gateway._reports["existing-native"]
            assert candidate is not predecessor and candidate["preparing"]
            assert candidate["previous"] is predecessor
            await consumer.reconcile_inherited_claims()
            assert consumer.owns_thread("discussion") and candidate["cancel_requested"]
            assert not predecessor.get("cancel_requested")
            release.set()
            if stage == "native-write":
                broker._write_lock.release()
            assert await asyncio.wait_for(handling, 3) is None
            assert writer.messages == [] and broker._pending == {}
            assert gateway._reports["existing-native"] is predecessor
            assert gateway._last_terminal["discussion"] is prior_terminal
            assert store.get_message(work["id"], "beta") == before
            gateway.sessions.cancel.assert_not_awaited()
            assert any("message did not start" in text for _, text, _ in gateway.client.posts)
        finally:
            release.set()
            if broker._write_lock.locked():
                broker._write_lock.release()
            if handling and not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)


async def test_ordinary_native_guard_refuses_unavailable_recovery_at_write(tmp_path):
    async with ordinary_runtime(tmp_path) as (_store, _clients, gateway, broker, writer, consumer):
        await consumer.reconcile_inherited_claims()
        actual = consumer.hub.request
        handling = None

        async def unavailable(method, path, body=None):
            if path == "/claims":
                raise HubError("Claims unavailable", status=503)
            return await actual(method, path, body)

        try:
            async with broker._write_lock:
                handling = asyncio.create_task(
                    gateway.run_prompt(
                        thread_id="discussion",
                        channel_id="floor",
                        prompt="Ordinary follow-up",
                        author_id="synthetic-user",
                        author_name="synthetic-user",
                    )
                )
                async with asyncio.timeout(2):
                    while not broker._pending:
                        await asyncio.sleep(0)
                consumer.hub.request = unavailable
                with pytest.raises(HubError):
                    await consumer.reconcile_inherited_claims()
                assert gateway.collaboration_recovering()
            assert await asyncio.wait_for(handling, 3) is None
            assert writer.messages == [] and broker._pending == {}
            gateway.sessions.cancel.assert_not_awaited()
        finally:
            if handling and not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)


async def test_settled_project_admits_ordinary_thread_while_other_project_remains_pending(tmp_path):
    async with ordinary_runtime(tmp_path) as (store, clients, gateway, _broker, writer, consumer):
        first = project(store)
        completed = pending_request(store, first)
        other = project(store)
        pending = pending_request(store, other, key="other-request", session="other-native")
        before = store.get_message(pending["id"], "beta")
        await settle(clients, completed)
        await consumer.reconcile_inherited_claims()
        assert gateway.ordinary_admitted("discussion")
        assert not gateway.ordinary_admitted(other["thread_id"])
        receipt = await gateway.run_prompt(
            thread_id="discussion",
            channel_id="floor",
            prompt="Ordinary follow-up",
            author_id="synthetic-user",
            author_name="synthetic-user",
        )
        assert receipt["stop_reason"] == "end_turn" and receipt["native_dispatched"]
        assert [message["method"] for message in writer.messages] == ["session/prompt"]
        assert writer.messages[0]["params"]["sessionId"] == "existing-native"
        assert store.get_message(pending["id"], "beta") == before
        gateway.sessions.cancel.assert_not_awaited()


async def test_recovery_outage_preserves_explicit_stop_for_already_dispatched_ordinary_work(
    tmp_path,
):
    async with ordinary_runtime(tmp_path) as (_store, _clients, gateway, broker, writer, consumer):
        await consumer.reconcile_inherited_claims()
        dispatched = asyncio.Event()
        prompt_id = None
        handling = None

        def active_native(payload):
            nonlocal prompt_id
            message = json.loads(payload)
            writer.messages.append(message)
            if message["method"] == "session/prompt":
                prompt_id = message["id"]
                dispatched.set()
            else:
                if "id" in message:
                    broker._pending[message["id"]].set_result({})
                if message["method"] == "session/cancel":
                    broker._pending[prompt_id].set_result({"stopReason": "cancelled"})

        writer.write = active_native
        actual = consumer.hub.request

        async def unavailable(method, path, body=None):
            if path == "/claims":
                raise HubError("Claims unavailable", status=503)
            return await actual(method, path, body)

        try:
            handling = asyncio.create_task(
                gateway.run_prompt(
                    thread_id="discussion",
                    channel_id="floor",
                    prompt="Ordinary active work",
                    author_id="synthetic-user",
                    author_name="synthetic-user",
                )
            )
            await asyncio.wait_for(dispatched.wait(), 2)
            consumer.hub.request = unavailable
            with pytest.raises(HubError):
                await consumer.reconcile_inherited_claims()
            assert gateway.collaboration_recovering()
            await gateway.dispatch_command(
                cmd="stop",
                arg="",
                thread_id="discussion",
                channel_id="floor",
                user_id="synthetic-user",
                user_name="synthetic-user",
            )
            receipt = await asyncio.wait_for(handling, 3)
            assert receipt["stop_reason"] == "cancelled" and receipt["native_dispatched"]
            gateway.sessions.cancel.assert_awaited_once()
            assert [message["method"] for message in writer.messages] == [
                "session/prompt",
                "session/cancel",
            ]
            assert all(
                message["params"]["sessionId"] == "existing-native" for message in writer.messages
            )
        finally:
            if handling and not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)
