"""Gateway startup and native evidence survive delivery preparation failures."""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_collaboration import api_fixture, ask, close_api, project
from test_collaboration_dispatch_review import close_reports, native_gateway

from mattermost_acp.acp import TurnResult
from mattermost_acp.collaboration import consumer as consumer_module
from mattermost_acp.collaboration.client import Settings
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.mm.client import MattermostError


@pytest.mark.parametrize("failure", [None, "mkdir", "write_text", "chmod"])
async def test_long_foreground_reply_keeps_terminal_evidence_when_artifact_fails(
    tmp_path, monkeypatch, failure
):
    monkeypatch.setenv("HOME", str(tmp_path / "synthetic-home"))
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    broker.capabilities.steering = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    answer = "Long native evidence.\n" * 600 + "FINAL_NATIVE_EVIDENCE"
    receipt, failures = {}, []
    folder = Path.home() / ".local/state/mattermost-acp-collaboration/results"
    if failure is not None:
        operation = getattr(Path, failure)

        def fail_artifact(path, *args, **kwargs):
            if path == folder or folder in path.parents:
                failures.append(dict(gateway._last_terminal["discussion"]))
                raise OSError("synthetic artifact storage failure")
            return operation(path, *args, **kwargs)

        monkeypatch.setattr(Path, failure, fail_artifact)
    actual_prompt = gateway.run_prompt

    async def capture(**options):
        result = await actual_prompt(**options)
        receipt.update(result)
        return result

    gateway.run_prompt = capture
    try:
        p = project(store)
        requested = ask(store, p["id"])
        claimed = store.claim_next("beta", consumer.instance)

        async def native(_thread, _prompt, **options):
            assert await options["before_prompt"]("existing-native")
            store.reply(
                requested["id"], "beta", "Explicit correlated result", claim=claimed["claim"]
            )
            return TurnResult("existing-native", "end_turn", answer, "")

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        await asyncio.wait_for(consumer.handle(claimed), 3)
        settled = store.get_message(requested["id"], "beta")
        evidence = settled["evidence"]
        assert settled["state"] == "done"
        assert settled["result"] == "Explicit correlated result"
        assert settled["execution_pending"] is False
        assert evidence["execution_id"] == receipt["execution_id"]
        assert evidence["session_id"] == "existing-native"
        assert evidence["native_dispatched"] is True
        assert evidence["stop_reason"] == "end_turn"
        assert evidence["response_delivered"] is True
        assert evidence["foreground_answer"] == answer[:8000]
        assert "FINAL_NATIVE_EVIDENCE" in "\n".join(
            message for _channel, message, _root in gateway.client.posts
        )
        assert not consumer.owns_thread("discussion")
        assert not consumer.ownership
        if failure is None:
            assert Path(evidence["result_file"]).read_text() == answer
            assert "result_file_error" not in evidence
        else:
            assert len(failures) == 1
            assert failures[0]["session_id"] == "existing-native"
            assert failures[0]["native_dispatched"] is True
            assert failures[0]["stop_reason"] == "end_turn"
            assert failures[0]["execution_id"] == receipt["execution_id"]
            assert "synthetic artifact storage failure" in evidence["result_file_error"]
            assert "result_file" not in evidence
            monkeypatch.setattr(Path, failure, operation)
        await consumer.reconcile_ownership()
        gateway.sessions.run_turn.assert_awaited_once()
    finally:
        await consumer.close()
        await close_reports(gateway)
        await close_api(store, server, clients)


async def test_queued_mailbox_work_waits_for_floor_join_before_binding_lookup(
    tmp_path, monkeypatch
):
    store, server, clients = await api_fixture(tmp_path)
    gateway, broker, _writer = native_gateway(tmp_path)
    gateway.config.db_path = tmp_path / "project.db"
    gateway.config.all_channels = False
    gateway.config.auto_join_channels = False
    gateway.sessions.store.get = AsyncMock(
        return_value=SimpleNamespace(session_id="existing-native")
    )
    gateway.client.get_me = AsyncMock()
    gateway._handle_pending_restart_bump = AsyncMock()
    broker.capabilities.steering = False
    join_entered, release_join, consumer_started, handled = (asyncio.Event() for _ in range(4))
    joined = False
    binding_reads = []
    actual_get_post = gateway.client.get_post

    async def join(channel):
        nonlocal joined
        assert channel == "floor"
        join_entered.set()
        await release_join.wait()
        joined = True

    async def get_post(thread):
        binding_reads.append(thread)
        if not joined:
            raise MattermostError("synthetic floor membership denied", status=403)
        return await actual_get_post(thread)

    async def events(*, stop):
        await stop.wait()
        if False:
            yield {}

    class ObservedConsumer(CollaborationConsumer):
        def start(self):
            consumer_started.set()
            super().start()

        async def handle(self, item):
            await super().handle(item)
            handled.set()

    gateway.client.join_channel = join
    gateway.client.get_post = get_post
    gateway.client.events = events
    monkeypatch.setattr(consumer_module, "CollaborationConsumer", ObservedConsumer)
    monkeypatch.setattr(Settings, "load", lambda: clients["beta"].settings)
    running = None
    try:
        p = project(store)
        requested = ask(store, p["id"])

        async def native(_thread, _prompt, **options):
            assert joined
            assert await options["before_prompt"]("existing-native")
            claimed = store.get_message(requested["id"], "beta")
            store.reply(requested["id"], "beta", "Joined floor before work", claim=claimed["claim"])
            return TurnResult("existing-native", "end_turn", "Verified joined-floor work", "")

        gateway.sessions.run_turn = AsyncMock(side_effect=native)
        running = asyncio.create_task(gateway.run())
        await asyncio.wait_for(join_entered.wait(), 2)
        assert not consumer_started.is_set()
        assert gateway.collaboration is None
        assert binding_reads == []
        assert store.get_message(requested["id"], "beta")["state"] == "queued"
        release_join.set()
        await asyncio.wait_for(handled.wait(), 3)
        settled = store.get_message(requested["id"], "beta")
        assert consumer_started.is_set()
        assert binding_reads == ["discussion"]
        assert settled["state"] == "done"
        assert settled["execution_pending"] is False
        assert settled["evidence"]["response_delivered"] is True
        assert not gateway.collaboration.owns_thread("discussion")
        gateway.sessions.run_turn.assert_awaited_once()
    finally:
        release_join.set()
        await gateway.close()
        if running:
            await asyncio.wait_for(running, 2)
        await close_reports(gateway)
        await close_api(store, server, clients)
