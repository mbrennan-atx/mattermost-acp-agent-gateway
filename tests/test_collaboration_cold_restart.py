"""Cold harness restarts fence real mailbox polling before checking native idleness."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from test_collaboration import api_fixture, ask, close_api, project
from test_collaboration_regressions import maintenance_gateway

from mattermost_acp.collaboration.consumer import CollaborationConsumer


@asynccontextmanager
async def cold_runtime(tmp_path, monkeypatch, *, inherited=False):
    store, server, clients = await api_fixture(tmp_path)
    gateway = maintenance_gateway()
    gateway.config.db_path = tmp_path / "cold-projects.db"
    gateway.collaboration_recovery_pending = False
    gateway.run_prompt = type(gateway).run_prompt.__get__(gateway)
    gateway._run_prompt = AsyncMock()
    gateway._request_restart = AsyncMock(return_value="Synthetic restart scheduled")
    gateway.sessions.ensure_session = AsyncMock()
    gateway.sessions.run_turn = AsyncMock()
    gateway.sessions.store.get = AsyncMock(return_value=None)
    gateway.sessions.broker.start = AsyncMock()
    gateway.sessions.broker.is_running = False
    consumer = CollaborationConsumer(gateway, clients["beta"].settings)
    gateway.collaboration = consumer
    consumer.handle = AsyncMock(wraps=consumer.handle)
    polling = asyncio.Event()
    claim_next = store.claim_next
    debt = None
    if inherited:
        p = project(store)
        requested = ask(store, p["id"])
        store.claim_next("beta", "previous-worker")
        debt = store.get_message(requested["id"], "beta")

    def observed_claim(actor, instance, **options):
        if actor == "beta" and instance == consumer.instance and not options.get("controls_only"):
            polling.set()
        return claim_next(actor, instance, **options)

    monkeypatch.setattr(store, "claim_next", observed_claim)
    consumer.start()
    try:
        await asyncio.wait_for(polling.wait(), 2)
        assert consumer.claiming and not consumer.recovery_pending
        assert gateway.maintenance_state()["collaboration"] > 0
        yield store, clients, gateway, consumer, debt
    finally:
        await consumer.close()
        # Wake any server long poll that survived cancellation of its client.
        await clients["beta"].request(
            "POST", "/mailbox/admission", {"instance": consumer.instance, "mode": "sealed"}
        )
        await close_api(store, server, clients)


def restart_control(gateway, *, timeout_s=0):
    return gateway._track_control(
        gateway._restart_when_idle("discussion", "floor", "synthetic-user", timeout_s=timeout_s),
        "synthetic-cold-restart",
    )


def assert_no_native(gateway, consumer):
    consumer.handle.assert_not_awaited()
    gateway._run_prompt.assert_not_awaited()
    gateway.sessions.ensure_session.assert_not_awaited()
    gateway.sessions.run_turn.assert_not_awaited()
    gateway.sessions.broker.start.assert_not_awaited()
    gateway.sessions.cancel.assert_not_awaited()


async def test_empty_cold_gateway_restarts_after_real_long_poll_fence(tmp_path, monkeypatch):
    async with cold_runtime(tmp_path, monkeypatch) as (store, clients, gateway, consumer, _debt):
        controls_fenced, acknowledge = asyncio.Event(), asyncio.Event()
        actual = consumer.hub.request
        modes = []
        task = None

        async def hold_controls_ack(method, path, body=None):
            result = await actual(method, path, body)
            if path == "/mailbox/admission":
                modes.append(body["mode"])
                if body["mode"] == "controls":
                    controls_fenced.set()
                    await acknowledge.wait()
            return result

        def schedule(**options):
            assert options == {
                "thread_id": "discussion",
                "channel_id": "floor",
                "user_id": "synthetic-user",
                "reason": "harness_switch",
            }
            assert gateway.draining and gateway.sealed
            assert not consumer.claiming and not consumer.recovery_pending
            state = gateway.maintenance_state()
            assert state["commands"] == 1  # This restart control owns the idle decision.
            assert all(
                state[key] == 0
                for key in (
                    "foreground",
                    "responses",
                    "inbound",
                    "native_rpc",
                    "background",
                    "collaboration",
                )
            )
            return "Synthetic restart scheduled"

        consumer.hub.request = hold_controls_ack
        gateway._request_restart.side_effect = schedule
        try:
            task = restart_control(gateway, timeout_s=1)
            await asyncio.wait_for(controls_fenced.wait(), 2)
            assert gateway.draining
            p = project(store)
            work = ask(store, p["id"])
            queued = store.get_message(work["id"], "beta")
            assert (
                await clients["beta"].request(
                    "GET", f"/mailbox?instance={consumer.instance}&wait=0"
                )
                is None
            )
            await gateway.run_prompt(
                thread_id="discussion", channel_id="floor", prompt="Late ordinary work"
            )
            assert_no_native(gateway, consumer)
            acknowledge.set()
            await asyncio.wait_for(task, 2)
            gateway._request_restart.assert_awaited_once()
            assert modes == ["controls", "sealed"]
            assert gateway.draining and gateway.sealed
            assert store.get_message(work["id"], "beta") == queued
            assert (
                await clients["beta"].request(
                    "GET", f"/mailbox?instance={consumer.instance}&wait=0"
                )
                is None
            )
            assert_no_native(gateway, consumer)
        finally:
            acknowledge.set()
            if task and not task.done():
                task.cancel()
            if task:
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("boundary", ["unknown-native", "inherited-debt", "inbound-timeout"])
async def test_refused_cold_restart_restores_normal_admission_and_retains_work(
    tmp_path, monkeypatch, boundary
):
    async with cold_runtime(tmp_path, monkeypatch, inherited=boundary == "inherited-debt") as (
        store,
        _clients,
        gateway,
        consumer,
        debt,
    ):
        if boundary == "unknown-native":
            gateway.sessions.broker.native_boundary_idle = False
        elif boundary == "inbound-timeout":
            gateway._inbound_tasks.add("pending-inbound")
        actual = consumer.hub.request
        modes = []

        async def observed(method, path, body=None):
            result = await actual(method, path, body)
            if path == "/mailbox/admission":
                modes.append(body["mode"])
            return result

        consumer.hub.request = observed
        await asyncio.wait_for(restart_control(gateway), 2)
        gateway._request_restart.assert_not_awaited()
        assert modes == ["controls", "controls", "normal"]
        assert not gateway.draining and not gateway.sealed
        assert not consumer.claim_uncertain
        assert gateway.client.posts
        if debt:
            assert store.get_message(debt["id"], "beta") == debt
            assert len(consumer.inherited_claims) == 1
            receipt = consumer.inherited_claims[0]
            assert (
                receipt["project_deadline"]
                == store.get_project(debt["project"], "beta")["deadline"]
            )
            assert receipt["project_remaining_seconds"] > 0
            assert receipt["local_project_deadline"] > 0
            assert {
                key: value
                for key, value in receipt.items()
                if key
                not in {"project_deadline", "project_remaining_seconds", "local_project_deadline"}
            } == debt
        if boundary == "inbound-timeout":
            assert gateway._inbound_tasks == {"pending-inbound"}
        assert_no_native(gateway, consumer)


@pytest.mark.parametrize("lost_mode", ["controls", "sealed"])
async def test_lost_applied_admission_response_restores_verified_normal_mode(
    tmp_path, monkeypatch, lost_mode
):
    async with cold_runtime(tmp_path, monkeypatch) as (
        _store,
        _clients,
        gateway,
        consumer,
        _debt,
    ):
        actual = consumer.hub.request
        modes = []
        lost = False

        async def lose_response(method, path, body=None):
            nonlocal lost
            result = await actual(method, path, body)
            if path == "/mailbox/admission":
                mode = body["mode"]
                modes.append(mode)
                if mode == lost_mode and not lost:
                    lost = True
                    raise TimeoutError("Synthetic lost admission acknowledgement")
            return result

        consumer.hub.request = lose_response
        await asyncio.wait_for(restart_control(gateway), 2)
        gateway._request_restart.assert_not_awaited()
        assert modes == (
            ["controls", "controls", "normal"]
            if lost_mode == "controls"
            else ["controls", "sealed", "controls", "normal"]
        )
        assert not gateway.draining and not gateway.sealed
        assert not consumer.claim_uncertain
        assert_no_native(gateway, consumer)


@pytest.mark.parametrize("previous", [(False, False), (True, False), (True, True)])
async def test_unknown_native_restores_previous_gateway_flags_and_admission(
    tmp_path, monkeypatch, previous
):
    async with cold_runtime(tmp_path, monkeypatch) as (
        _store,
        _clients,
        gateway,
        consumer,
        _debt,
    ):
        gateway.draining, gateway.sealed = previous
        gateway.sessions.broker.native_boundary_idle = False
        actual = consumer.hub.request
        modes = []

        async def observed(method, path, body=None):
            result = await actual(method, path, body)
            if path == "/mailbox/admission":
                modes.append(body["mode"])
            return result

        consumer.hub.request = observed
        await asyncio.wait_for(restart_control(gateway), 2)
        expected = "sealed" if previous[1] else "controls" if previous[0] else "normal"
        assert modes == (
            ["controls", "controls", "normal"] if not any(previous) else ["controls", expected]
        )
        assert (gateway.draining, gateway.sealed) == previous
        gateway._request_restart.assert_not_awaited()
        assert_no_native(gateway, consumer)


async def test_restart_scheduling_error_restores_normal_admission(tmp_path, monkeypatch):
    async with cold_runtime(tmp_path, monkeypatch) as (
        _store,
        _clients,
        gateway,
        consumer,
        _debt,
    ):
        actual = consumer.hub.request
        modes = []

        async def observed(method, path, body=None):
            result = await actual(method, path, body)
            if path == "/mailbox/admission":
                modes.append(body["mode"])
            return result

        consumer.hub.request = observed
        gateway._request_restart.side_effect = RuntimeError("Synthetic restart request failure")
        await asyncio.wait_for(restart_control(gateway), 2)
        gateway._request_restart.assert_awaited_once()
        assert modes == ["controls", "sealed", "controls", "normal"]
        assert not gateway.draining and not gateway.sealed
        assert not consumer.claim_uncertain
        assert_no_native(gateway, consumer)


@pytest.mark.parametrize("accepted", [False, True])
async def test_failed_admission_restoration_keeps_native_admission_closed(
    tmp_path, monkeypatch, accepted
):
    async with cold_runtime(tmp_path, monkeypatch) as (
        store,
        clients,
        gateway,
        consumer,
        _debt,
    ):
        actual = consumer.hub.request
        modes = []

        async def failed_restore(method, path, body=None):
            if path == "/mailbox/admission" and body["mode"] == "normal":
                modes.append("normal")
                if accepted:
                    await actual(method, path, body)
                raise TimeoutError("Synthetic unverifiable admission restoration")
            result = await actual(method, path, body)
            if path == "/mailbox/admission":
                modes.append(body["mode"])
            return result

        consumer.hub.request = failed_restore
        gateway._request_restart.side_effect = RuntimeError("Synthetic restart request failure")
        await asyncio.wait_for(restart_control(gateway), 2)
        assert modes == ["controls", "sealed", "controls", "normal"]
        assert gateway.draining and gateway.sealed and consumer.claim_uncertain
        p = project(store)
        work = ask(store, p["id"])
        before = store.get_message(work["id"], "beta")
        await gateway.run_prompt(
            thread_id="discussion", channel_id="floor", prompt="Late ordinary work"
        )
        await clients["alpha"].request("GET", f"/projects/{p['id']}")
        assert store.get_message(work["id"], "beta") == before
        assert_no_native(gateway, consumer)


@pytest.mark.parametrize("boundary", ["inbound", "unknown-native"])
async def test_restart_rechecks_complete_idle_after_hub_sealing(tmp_path, monkeypatch, boundary):
    async with cold_runtime(tmp_path, monkeypatch) as (
        _store,
        _clients,
        gateway,
        consumer,
        _debt,
    ):
        actual = consumer.hub.request
        modes = []

        async def arrive_after_seal(method, path, body=None):
            result = await actual(method, path, body)
            if path == "/mailbox/admission":
                modes.append(body["mode"])
                if body["mode"] == "sealed":
                    if boundary == "inbound":
                        gateway._inbound_tasks.add("accepted-inbound")
                    else:
                        gateway.sessions.broker.native_boundary_idle = False
            return result

        consumer.hub.request = arrive_after_seal
        await asyncio.wait_for(restart_control(gateway), 2)
        gateway._request_restart.assert_not_awaited()
        assert modes == ["controls", "sealed", "controls", "normal"]
        assert not gateway.draining and not gateway.sealed
        if boundary == "inbound":
            assert gateway._inbound_tasks == {"accepted-inbound"}
        else:
            assert gateway.maintenance_state()["native_boundary_unknown"]
        assert_no_native(gateway, consumer)


async def test_normal_restoration_keeps_native_fence_until_real_acknowledgement(
    tmp_path, monkeypatch
):
    async with cold_runtime(tmp_path, monkeypatch) as (
        _store,
        _clients,
        gateway,
        consumer,
        _debt,
    ):
        normal_applied, acknowledge = asyncio.Event(), asyncio.Event()
        actual = consumer.hub.request
        task = None

        async def hold_normal_ack(method, path, body=None):
            result = await actual(method, path, body)
            if path == "/mailbox/admission" and body["mode"] == "normal":
                normal_applied.set()
                await acknowledge.wait()
            return result

        consumer.hub.request = hold_normal_ack
        gateway._request_restart.side_effect = RuntimeError("Synthetic restart request failure")
        try:
            task = restart_control(gateway)
            await asyncio.wait_for(normal_applied.wait(), 2)
            assert gateway.draining and gateway.sealed
            await gateway.run_prompt(
                thread_id="discussion", channel_id="floor", prompt="Late ordinary work"
            )
            assert_no_native(gateway, consumer)
            acknowledge.set()
            await asyncio.wait_for(task, 2)
            assert not gateway.draining and not gateway.sealed
        finally:
            acknowledge.set()
            if task and not task.done():
                task.cancel()
            if task:
                await asyncio.gather(task, return_exceptions=True)


async def test_failed_claim_reconciliation_blocks_normal_restoration(tmp_path, monkeypatch):
    async with cold_runtime(tmp_path, monkeypatch) as (
        _store,
        _clients,
        gateway,
        consumer,
        _debt,
    ):
        actual = consumer.hub.request
        modes = []

        async def lose_reconciliation_response(method, path, body=None):
            result = await actual(method, path, body)
            if path == "/mailbox/admission":
                mode = body["mode"]
                modes.append(mode)
                if mode == "controls" and modes.count("controls") == 2:
                    raise TimeoutError("Synthetic unverifiable claim reconciliation")
            return result

        consumer.hub.request = lose_reconciliation_response
        gateway._request_restart.side_effect = RuntimeError("Synthetic restart request failure")
        await asyncio.wait_for(restart_control(gateway), 2)
        assert modes == ["controls", "sealed", "controls"]
        assert gateway.draining and gateway.sealed and consumer.claim_uncertain
        await gateway.run_prompt(
            thread_id="discussion", channel_id="floor", prompt="Late ordinary work"
        )
        assert_no_native(gateway, consumer)
