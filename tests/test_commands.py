from mattermost_acp.commands.defs import (
    SLASH_VERBS,
    TEXT_COMMANDS,
    help_text,
    parse_text_command,
    resolve_passthrough_prompt,
)
from mattermost_acp.commands.restart import under_systemd


def test_passthrough_session_info():
    assert resolve_passthrough_prompt("session-info") == "/session-info"
    assert resolve_passthrough_prompt("info") == "/session-info"
    assert resolve_passthrough_prompt("/INFO") == "/session-info"


def test_passthrough_with_args():
    assert resolve_passthrough_prompt("compact", "keep auth") == "/compact keep auth"
    # /model and /effort are gateway-owned (session/set_model + reasoningEffort)
    assert resolve_passthrough_prompt("model", "grok-4") is None
    assert resolve_passthrough_prompt("effort", "high") is None
    assert resolve_passthrough_prompt("context") == "/context"
    assert resolve_passthrough_prompt("doctor") == "/doctor"
    assert resolve_passthrough_prompt("workflows") == "/workflows"
    assert (
        resolve_passthrough_prompt("workflow", "pause review-changes")
        == "/workflow pause review-changes"
    )
    assert resolve_passthrough_prompt("wf", "stop review-changes-2") == (
        "/workflow stop review-changes-2"
    )


def test_unknown_passthrough():
    assert resolve_passthrough_prompt("not-a-real-cmd") is None
    assert resolve_passthrough_prompt("stop") is None  # gateway-local


def test_text_commands_include_gateway_and_passthrough():
    assert "stop" in TEXT_COMMANDS
    assert "restart" in TEXT_COMMANDS
    assert "session-info" in TEXT_COMMANDS
    assert "info" in TEXT_COMMANDS
    assert "compact" in TEXT_COMMANDS
    assert "model" in TEXT_COMMANDS
    assert "models" in TEXT_COMMANDS
    assert "effort" in TEXT_COMMANDS
    assert "workflows" in TEXT_COMMANDS
    assert "workflow" in TEXT_COMMANDS
    assert "wf" in TEXT_COMMANDS


def test_help_text_lists_commands():
    text = help_text(home_channel_ids=frozenset({"town", "ai"}), harness="grok")
    assert "/restart" in text
    assert "/session-info" in text
    assert "/workflows" in text
    assert "/workflow" in text
    assert "/status" in text
    assert "MATTERMOST_MODEL_SET" in text
    assert "MATTERMOST_THREAD_RENAME" in text
    assert "town" in text and "ai" in text


def test_under_systemd_bool():
    assert isinstance(under_systemd(), bool)


def test_parse_text_command():
    assert parse_text_command("/new") == ("new", "")
    assert parse_text_command("/model grok-4.6 high") == ("model", "grok-4.6 high")
    assert parse_text_command("hello") is None
    assert parse_text_command("/not-a-command") is None


def test_slash_verbs_cover_gateway_and_passthrough():
    for name in ("stop", "new", "status", "model", "effort", "session-info", "workflow"):
        assert name in SLASH_VERBS


def test_harness_is_a_gateway_text_command() -> None:
    from mattermost_acp.commands.defs import SLASH_VERBS, parse_text_command

    assert parse_text_command("/harness claude opus") == ("harness", "claude opus")
    assert "harness" in SLASH_VERBS


def test_help_is_harness_specific():
    for harness in ("claude", "codex", "cursor"):
        text = help_text(home_channel_ids=[], harness=harness)
        assert "/workflow" not in text
        assert "grok mcp doctor" not in text
        assert "always-approve" not in text
        assert "/mcps" in text
