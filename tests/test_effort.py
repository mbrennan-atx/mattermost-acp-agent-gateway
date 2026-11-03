"""Effort parsing + SessionService.set_effort."""

from __future__ import annotations

from pathlib import Path

import pytest
from fake_agent import spawn_fake_agent_process

from mattermost_acp.acp.broker import AcpBroker
from mattermost_acp.commands.effort import (
    normalize_effort,
    parse_model_and_effort,
)
from mattermost_acp.session_service import SessionService
from mattermost_acp.store import ThreadStore


def test_normalize_effort():
    assert normalize_effort("HIGH") == "high"
    assert normalize_effort("  xhigh ") == "xhigh"
    assert normalize_effort("nope") is None
    assert normalize_effort("") is None
    assert normalize_effort(None) is None


def test_parse_model_and_effort():
    assert parse_model_and_effort("hub-claude-5-sonnet high") == (
        "hub-claude-5-sonnet",
        "high",
    )
    assert parse_model_and_effort("grok-4.5:medium") == ("grok-4.5", "medium")
    assert parse_model_and_effort("hub-claude-5-sonnet") == (
        "hub-claude-5-sonnet",
        None,
    )
    assert parse_model_and_effort("hub-claude-5-sonnet potato") == (
        "hub-claude-5-sonnet potato",
        None,
    )


@pytest.fixture
async def service(tmp_path: Path):
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    svc = SessionService(broker, store, default_cwd=tmp_path)
    yield svc
    await broker.stop()
    await store.close()


@pytest.mark.asyncio
async def test_set_effort_on_current_model(service: SessionService) -> None:
    thread_id = "11"
    await service.ensure_session(thread_id)
    await service.switch_model(thread_id, "hub-claude-5-sonnet")
    model, effort = await service.set_effort(thread_id, "xhigh")
    assert effort == "xhigh"
    assert model == "hub-claude-5-sonnet"
    record = await service.store.get(thread_id)
    assert record is not None
    assert record.effort == "xhigh"
    assert record.model == "hub-claude-5-sonnet"


@pytest.mark.parametrize("backend,key", [("claude", "effort"), ("codex", "reasoning_effort")])
async def test_status_uses_live_adapter_effort(service, backend, key):
    await service.store.save("thread", "session", str(service.default_cwd), effort="low")
    service.broker.backend = backend
    service.broker.session_config_options["session"] = [{"id": key, "currentValue": "high"}]
    assert (await service.status("thread"))["effort"] == "high"


async def test_status_grok_effort_sources(service, tmp_path):
    import json
    from urllib.parse import quote

    service.grok_home = tmp_path
    await service.store.save("thread", "session", str(tmp_path), model="test-model")
    config = tmp_path / "config.toml"
    config.write_text('[models]\ndefault_reasoning_effort = "low"\n')
    assert (await service.status("thread"))["effort"] == "low"
    config.write_text(config.read_text() + '[model.test-model]\nreasoning_effort = "medium"\n')
    assert (await service.status("thread"))["effort"] == "medium"
    await service.store.save("thread", "session", str(tmp_path), effort="high")
    assert (await service.status("thread"))["effort"] == "high"
    summary = tmp_path / "sessions" / quote(str(tmp_path), safe="") / "session" / "summary.json"
    summary.parent.mkdir(parents=True)
    summary.write_text(json.dumps({"reasoning_effort": "max"}))
    assert (await service.status("thread"))["effort"] == "max"
