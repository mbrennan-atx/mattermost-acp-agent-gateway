"""Steering reservations bind only reports whose prompt bytes reached native IO."""

import asyncio
import json

import pytest
from test_collaboration import ask, execution_report, project
from test_collaboration_ordinary_recovery import ordinary_runtime
from test_collaboration_regressions import memory_broker

from mattermost_acp.acp.broker import NativeDispatchChanged


async def ordinary_followup(gateway):
    return await gateway.run_prompt(
        thread_id="discussion",
        channel_id="floor",
        prompt="Ordinary follow-up",
        author_id="synthetic-user",
        author_name="synthetic-user",
    )


@pytest.mark.parametrize("stage", ["status-delivery", "native-write"])
async def test_injection_cannot_own_ordinary_provisional_report_with_background_predecessor(
    tmp_path, stage
):
    async with ordinary_runtime(tmp_path) as (store, _clients, gateway, broker, writer, consumer):
        await consumer.reconcile_inherited_claims()
        p = project(store)
        previous = execution_report(p["id"], "accepted-predecessor")
        previous["native_dispatched"] = True
        previous["reporter"].active_tools.add("prior-background-tool")
        gateway._reports["existing-native"] = previous
        entered, release = asyncio.Event(), asyncio.Event()
        publish = gateway.client.create_post
        handling = None

        async def delayed_status(*args, **kwargs):
            entered.set()
            await release.wait()
            return await publish(*args, **kwargs)

        try:
            if stage == "status-delivery":
                gateway.client.create_post = delayed_status
            else:
                await broker._write_lock.acquire()
            handling = asyncio.create_task(ordinary_followup(gateway))
            if stage == "status-delivery":
                await asyncio.wait_for(entered.wait(), 2)
            else:
                async with asyncio.timeout(2):
                    while not broker._pending:
                        await asyncio.sleep(0)
            candidate = gateway._reports["existing-native"]
            assert candidate is not previous and candidate["preparing"]
            assert candidate["previous"] is previous
            requested = ask(store, p["id"])
            claimed = store.claim_next("beta", consumer.instance)
            await asyncio.wait_for(consumer.handle(claimed), 2)
            receipt = store.get_message(requested["id"], "beta")
            assert receipt["state"] == "queued" and not receipt["execution_pending"]
            assert receipt["evidence"] == {}
            assert all(not entry.get("execution_id") for entry in receipt["delivery_history"])
            assert "reservation_evidence" not in claimed
            assert not consumer.ownership and not consumer.owns_thread("discussion")
            assert gateway._reports["existing-native"] is candidate
            assert candidate["previous"] is previous
            assert previous["reporter"].active_tools == {"prior-background-tool"}
            assert not previous.get("cancel_requested")
            assert writer.messages == []
            gateway.sessions.cancel.assert_not_awaited()
            release.set()
            if stage == "native-write":
                broker._write_lock.release()
            ordinary = await asyncio.wait_for(handling, 3)
            assert ordinary["native_dispatched"] and ordinary["stop_reason"] == "end_turn"
            assert gateway._reports["existing-native"] is candidate
            assert candidate["native_dispatched"] and not candidate["preparing"]
            assert candidate["previous"] is previous
            assert [message["method"] for message in writer.messages] == ["session/prompt"]
            assert store.get_message(requested["id"], "beta") == receipt
        finally:
            release.set()
            if broker._write_lock.locked():
                broker._write_lock.release()
            if handling and not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)


async def test_written_foreground_prompt_is_steerable_before_native_answer(tmp_path):
    async with ordinary_runtime(tmp_path) as (store, _clients, gateway, broker, writer, consumer):
        await consumer.reconcile_inherited_claims()
        p = project(store)
        prompt_written, injection_written = asyncio.Event(), asyncio.Event()
        prompt_ids = []
        ordinary, injection = None, None

        def write(payload):
            message = json.loads(payload)
            writer.messages.append(message)
            if message["method"] == "session/prompt":
                prompt_ids.append(message["id"])
                prompt_written.set()
            else:
                assert message["method"] == "_session/steering"
                broker._pending[message["id"]].set_result({"outcome": "injected"})
                injection_written.set()

        writer.write = write
        try:
            ordinary = asyncio.create_task(ordinary_followup(gateway))
            await asyncio.wait_for(prompt_written.wait(), 2)
            accepted = gateway._reports["existing-native"]
            assert accepted["active"] and not accepted["foreground_complete"]
            assert accepted["native_dispatched"] and not accepted["preparing"]
            requested = ask(store, p["id"])
            claimed = store.claim_next("beta", consumer.instance)
            injection = asyncio.create_task(consumer.handle(claimed))
            await asyncio.wait_for(injection_written.wait(), 2)
            assert consumer.ownership[(requested["id"], claimed["claim"])] is accepted
            reserved = store.get_message(requested["id"], "beta")
            assert reserved["execution_pending"]
            assert reserved["evidence"]["execution_id"] == accepted["execution_id"]
            async with asyncio.timeout(2):
                while (
                    store.get_message(requested["id"], "beta")["evidence"].get("delivery")
                    != "injected"
                ):
                    await asyncio.sleep(0)
            store.reply(requested["id"], "beta", "Correlated result", claim=claimed["claim"])
            broker._pending[prompt_ids[0]].set_result({"stopReason": "end_turn"})
            result = await asyncio.wait_for(ordinary, 3)
            assert result["native_dispatched"]
            await asyncio.wait_for(injection, 4)
            settled = store.get_message(requested["id"], "beta")
            assert settled["state"] == "done" and not settled["execution_pending"]
            assert not consumer.owns_thread("discussion")
            assert [message["method"] for message in writer.messages] == [
                "session/prompt",
                "_session/steering",
            ]
        finally:
            for task in (ordinary, injection):
                if task and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)


async def test_steering_rechecks_preparing_state_at_physical_native_write(tmp_path):
    async with ordinary_runtime(tmp_path) as (store, _clients, gateway, broker, writer, consumer):
        await consumer.reconcile_inherited_claims()
        p = project(store)
        accepted = execution_report(p["id"], "accepted-native")
        accepted["native_dispatched"] = True
        gateway._reports["existing-native"] = accepted
        handling = None
        try:
            requested = ask(store, p["id"])
            claimed = store.claim_next("beta", consumer.instance)
            async with broker._write_lock:
                handling = asyncio.create_task(consumer.handle(claimed))
                async with asyncio.timeout(2):
                    while not broker._pending:
                        await asyncio.sleep(0)
                assert consumer.ownership[(requested["id"], claimed["claim"])] is accepted
                assert store.get_message(requested["id"], "beta")["execution_pending"]
                accepted["preparing"] = True
            await asyncio.wait_for(handling, 2)
            receipt = store.get_message(requested["id"], "beta")
            assert receipt["state"] == "queued" and not receipt["execution_pending"]
            assert receipt["evidence"] == {}
            assert receipt["delivery_history"][-1]["not_dispatched"]
            assert writer.messages == [] and broker._pending == {}
            assert gateway._reports["existing-native"] is accepted
            assert not accepted.get("cancel_requested")
            assert not consumer.owns_thread("discussion")
            gateway.sessions.cancel.assert_not_awaited()
        finally:
            if handling and not handling.done():
                handling.cancel()
                await asyncio.gather(handling, return_exceptions=True)


@pytest.mark.parametrize("failure", ["guard", "writer"])
async def test_refused_native_write_does_not_publish_dispatch_evidence(failure):
    broker, writer = memory_broker()
    dispatched = []
    if failure == "writer":

        def fail_write(_payload):
            raise BrokenPipeError("Synthetic native writer failed")

        writer.write = fail_write
    with pytest.raises(NativeDispatchChanged if failure == "guard" else BrokenPipeError):
        await broker.request(
            "session/prompt",
            {"sessionId": "native"},
            guard=lambda: failure != "guard",
            on_write=lambda: dispatched.append(True),
        )
    assert dispatched == [] and writer.messages == [] and broker._pending == {}


async def test_dispatch_evidence_is_visible_before_native_drain_finishes():
    broker, writer = memory_broker()
    entered, release = asyncio.Event(), asyncio.Event()
    dispatched = []

    async def drain():
        entered.set()
        await release.wait()

    writer.drain = drain
    handling = asyncio.create_task(
        broker.request("session/prompt", on_write=lambda: dispatched.append(True))
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert dispatched == [True] and len(writer.messages) == 1
        assert not handling.done()
        release.set()
        await asyncio.wait_for(handling, 2)
    finally:
        release.set()
        if not handling.done():
            handling.cancel()
            await asyncio.gather(handling, return_exceptions=True)
