"""Model router: explicit aliases, rules, classifier, and new-thread wiring."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from fake_agent import spawn_fake_agent_process

from mattermost_acp.acp.broker import AcpBroker
from mattermost_acp.model_router import (
    Classifier,
    RouteDecision,
    RouteRequest,
    _pick,
    load_router,
)
from mattermost_acp.session_service import SessionService
from mattermost_acp.store import ThreadStore

ROUTER_TOML = """
[router]
harnesses = ["grok"]

[aliases]
grok = "grok-4.7"
opus = "hub-claude-opus-5-5 medium"
"opus 5.5" = "hub-claude-opus-5-5 high"
luna = { model = "hub-gpt-6-luna", effort = "max" }

[[rules]]
name = "cheap-summaries"
model = "hub-gpt-6-luna max"
keywords = ["summarize", "tl;dr"]

[[rules]]
name = "files"
model = "hub-claude-sonnet-5"
attachments = true

[[rules]]
name = "ea-channel"
model = "hub-gpt-6-luna"
channels = ["chan-ea"]
"""


@pytest.fixture
def router(tmp_path: Path):
    path = tmp_path / "router.toml"
    path.write_text(ROUTER_TOML)
    return load_router(path)


@pytest.mark.parametrize(
    ("text", "model", "effort"),
    [
        ("use grok to draft the release note", "grok-4.7", None),
        ("Please have Opus look at this design", "hub-claude-opus-5-5", "medium"),
        ("run this on opus 5.5", "hub-claude-opus-5-5", "high"),
        ("with luna, check the logs", "hub-gpt-6-luna", "max"),
    ],
)
def test_explicit_alias(router, text, model, effort) -> None:
    decision = router.explicit(text)
    assert decision is not None
    assert (decision.model, decision.effort) == (model, effort)


@pytest.mark.parametrize(
    "text",
    [
        "compare grok and claude for this",
        "grok's config is broken",
        "the opus-5 rollout",
        "is luna cheaper than sol?",
    ],
)
def test_mentions_without_request_do_not_match(router, text) -> None:
    assert router.explicit(text) is None


def test_rules_first_match(router) -> None:
    assert router.by_rule(RouteRequest("summarize this thread")).reason == "rule:cheap-summaries"
    assert router.by_rule(RouteRequest("look", has_attachments=True)).model == (
        "hub-claude-sonnet-5"
    )
    assert router.by_rule(RouteRequest("hello", channel_id="chan-ea")).reason == "rule:ea-channel"
    assert router.by_rule(RouteRequest("hello", channel_id="other")) is None
    assert router.by_rule(RouteRequest("summarization theory")) is None


async def test_explicit_beats_rules(router) -> None:
    decision = await router.route(RouteRequest("use grok to summarize this"))
    assert decision is not None and decision.reason == "explicit:grok"


async def test_no_match_without_classifier_is_default(router) -> None:
    assert await router.route(RouteRequest("debug the gateway crash")) is None


def test_pick_choice() -> None:
    choices = {"hub-gpt-6-luna": "", "hub-claude-opus-5-5": ""}
    assert _pick("hub-gpt-6-luna", choices) == "hub-gpt-6-luna"
    assert _pick("`hub-claude-opus-5-5`.", choices) == "hub-claude-opus-5-5"
    assert _pick("I pick hub-gpt-6-luna", choices) == "hub-gpt-6-luna"
    assert _pick("gpt-5", choices) is None


def test_classifier_config(tmp_path: Path) -> None:
    path = tmp_path / "router.toml"
    path.write_text(
        """
[classifier]
model = "hub-gpt-6-luna"
[classifier.choices]
"hub-gpt-6-luna" = { description = "simple", effort = "max" }
"hub-claude-opus-5-5" = "hard"
"""
    )
    cls = load_router(path, grok_home=tmp_path).classifier
    assert cls is not None
    assert cls.choices == {"hub-gpt-6-luna": "simple", "hub-claude-opus-5-5": "hard"}
    assert cls.choice_effort == {"hub-gpt-6-luna": "max"}
    assert cls.grok_config == tmp_path / "config.toml"


async def test_classifier_used_after_rules(tmp_path: Path) -> None:
    path = tmp_path / "router.toml"
    path.write_text(
        ROUTER_TOML
        + """
[classifier]
model = "hub-gpt-6-luna"
choices = { "hub-gpt-6-luna" = "simple", "hub-claude-opus-5-5" = "hard" }
"""
    )
    router = load_router(path, grok_home=tmp_path)

    async def fake_classify(cls: Classifier, text: str) -> RouteDecision:
        return RouteDecision("hub-claude-opus-5-5", None, "classifier")

    with patch("mattermost_acp.model_router.classify", fake_classify):
        decision = await router.route(RouteRequest("debug the gateway crash"))
        assert decision is not None and decision.reason == "classifier"
        decision = await router.route(RouteRequest("summarize the doc"))
        assert decision is not None and decision.reason == "rule:cheap-summaries"


@pytest.fixture
async def routed_service(tmp_path: Path, router):
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    svc = SessionService(
        broker, store, default_cwd=tmp_path, default_model="hub-claude-opus-5-5", router=router
    )
    yield svc
    await broker.stop()
    await store.close()


async def test_new_thread_is_routed(routed_service: SessionService) -> None:
    await routed_service.ensure_session("t1", route=RouteRequest("use grok to do x"))
    record = await routed_service.store.get("t1")
    assert record is not None and record.model == "grok-4.7"


async def test_existing_thread_is_not_rerouted(routed_service: SessionService) -> None:
    await routed_service.ensure_session("t2", route=RouteRequest("hello"))
    await routed_service.ensure_session("t2", route=RouteRequest("use grok now"))
    record = await routed_service.store.get("t2")
    assert record is not None and record.model == "hub-claude-opus-5-5"


async def test_explicit_setting_skips_router(routed_service: SessionService) -> None:
    await routed_service.store.set_model_setting("channel:c1", "hub-gpt-6-sol", None)
    await routed_service.ensure_session(
        "t3", channel_id="c1", route=RouteRequest("use grok to do x")
    )
    record = await routed_service.store.get("t3")
    assert record is not None and record.model == "hub-gpt-6-sol"


async def test_other_harness_skips_router(tmp_path: Path, router) -> None:
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(tmp_path / "t.db", harness="claude")
    await store.open()
    svc = SessionService(
        broker, store, default_cwd=tmp_path, default_model="opus", router=router
    )
    try:
        await svc.ensure_session("t4", route=RouteRequest("use grok to do x"))
        record = await store.get("t4")
        assert record is not None and record.model == "opus"
    finally:
        await broker.stop()
        await store.close()


async def test_recreated_session_keeps_thread_model(routed_service: SessionService) -> None:
    await routed_service.ensure_session("t5", route=RouteRequest("use grok to do x"))
    old = await routed_service.store.get("t5")
    assert old is not None

    async def fail_resume(*args, **kwargs):
        from mattermost_acp.acp.broker import AcpError

        raise AcpError("gone")

    with (
        patch.object(routed_service.broker, "session_resume", fail_resume),
        patch.object(routed_service.broker, "session_load", fail_resume),
    ):
        await routed_service.ensure_session("t5", route=RouteRequest("summarize"))
    record = await routed_service.store.get("t5")
    assert record is not None
    assert record.session_id != old.session_id
    assert record.model == "grok-4.7"
