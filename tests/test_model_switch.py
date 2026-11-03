"""SessionService.switch_model — gateway-owned session/set_model, plus compaction."""

from __future__ import annotations

from pathlib import Path

import pytest
from fake_agent import spawn_fake_agent_process

from mattermost_acp.acp.broker import AcpBroker, AcpError
from mattermost_acp.session_service import (
    COMPACT_PROMPT,
    SessionService,
    needs_history_compaction,
)
from mattermost_acp.store import ThreadStore


@pytest.fixture
async def service(tmp_path: Path):
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    svc = SessionService(broker, store, default_cwd=tmp_path)
    yield svc
    await broker.stop()
    await store.close()


@pytest.mark.parametrize(
    ("previous", "target", "expected"),
    [
        ("hub-claude-opus-5", "grok-4.6", True),
        ("claude-opus-5", "hub-grok-4.6", True),
        ("grok-4.6", "hub-claude-opus-5", False),
        ("claude-opus-5", "hub-claude-sonnet-5", False),
        (None, "grok-4.6", False),
        ("", "grok-4.6", False),
    ],
)
def test_needs_history_compaction(previous, target, expected) -> None:
    assert needs_history_compaction(previous, target) is expected


@pytest.mark.asyncio
async def test_switch_to_grok_compacts_foreign_history_first(
    service: SessionService,
) -> None:
    thread_id = "21"
    await service.ensure_session(thread_id)
    await service.switch_model(thread_id, "hub-claude-5-sonnet")

    prompts: list[str] = []
    orig_prompt = service.broker.prompt_turn
    order: list[str] = []

    async def spy_prompt(session_id: str, text: str, **kwargs):
        prompts.append(text)
        order.append("prompt")
        return await orig_prompt(session_id, text, **kwargs)

    orig_set_model = service.broker.set_model

    async def spy_set_model(session_id: str, model_id: str, **kwargs):
        order.append(f"set_model:{model_id}")
        return await orig_set_model(session_id, model_id, **kwargs)

    service.broker.prompt_turn = spy_prompt  # type: ignore[method-assign]
    service.broker.set_model = spy_set_model  # type: ignore[method-assign]

    switch = await service.switch_model(thread_id, "grok-4.5")
    assert prompts == [COMPACT_PROMPT]
    # Loading the stored session re-pins its current model before compaction.
    assert order == ["set_model:hub-claude-5-sonnet", "prompt", "set_model:grok-4.5"]
    assert switch.compaction == "compacted"
    record = await service.store.get(thread_id)
    assert record is not None
    assert record.model == "grok-4.5"


@pytest.mark.asyncio
async def test_switch_away_from_grok_does_not_compact(service: SessionService) -> None:
    thread_id = "22"
    await service.ensure_session(thread_id)
    await service.switch_model(thread_id, "grok-4.5")

    prompts: list[str] = []
    orig_prompt = service.broker.prompt_turn

    async def spy_prompt(session_id: str, text: str, **kwargs):
        prompts.append(text)
        return await orig_prompt(session_id, text, **kwargs)

    service.broker.prompt_turn = spy_prompt  # type: ignore[method-assign]
    switch = await service.switch_model(thread_id, "hub-claude-5-sonnet")
    assert prompts == []
    assert switch.compaction == "not_needed"


@pytest.mark.asyncio
async def test_switch_still_happens_when_compaction_fails(
    service: SessionService,
) -> None:
    thread_id = "23"
    await service.ensure_session(thread_id)
    await service.switch_model(thread_id, "hub-claude-5-sonnet")

    async def boom(*args, **kwargs):
        raise AcpError("agent stdout closed")

    service.broker.prompt_turn = boom  # type: ignore[method-assign]
    switch = await service.switch_model(thread_id, "grok-4.5")
    assert switch.compaction == "failed"
    record = await service.store.get(thread_id)
    assert record is not None
    assert record.model == "grok-4.5"
