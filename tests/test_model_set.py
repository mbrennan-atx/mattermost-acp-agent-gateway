from mattermost_acp.commands.model_set import (
    extract_model_set_directive,
    parse_model_set_payload,
)


def test_parse_payload_defaults_to_thread() -> None:
    parsed = parse_model_set_payload("hub-claude-opus-5")
    assert parsed is not None
    assert parsed.query == "hub-claude-opus-5"
    assert parsed.effort is None
    assert parsed.scope == "thread"


def test_parse_payload_effort_and_scope() -> None:
    parsed = parse_model_set_payload("sonnet high channel")
    assert parsed is not None
    assert parsed.query == "sonnet"
    assert parsed.effort == "high"
    assert parsed.scope == "channel"


def test_parse_payload_system_maps_to_harness() -> None:
    parsed = parse_model_set_payload("grok-4.6 system")
    assert parsed is not None
    assert parsed.query == "grok-4.6"
    assert parsed.scope == "harness"


def test_parse_payload_scope_before_effort() -> None:
    parsed = parse_model_set_payload("opus channel high")
    assert parsed is not None
    assert parsed.query == "opus"
    assert parsed.effort == "high"
    assert parsed.scope == "channel"


def test_extract_strips_canonical_line() -> None:
    answer = (
        "Switching this thread to opus.\n"
        "DISCORD_MODEL_SET: hub-claude-opus-5 high thread\n"
    )
    cleaned, directive = extract_model_set_directive(answer)
    assert "DISCORD_MODEL_SET" not in cleaned
    assert "Switching this thread to opus." in cleaned
    assert directive is not None
    assert directive.query == "hub-claude-opus-5"
    assert directive.effort == "high"
    assert directive.scope == "thread"


def test_extract_last_match_wins_and_alt_form() -> None:
    answer = "MODEL_SET: sonnet\nDISCORD_MODEL_SET: opus harness\nDone."
    cleaned, directive = extract_model_set_directive(answer)
    assert "DISCORD_MODEL_SET" not in cleaned
    assert "MODEL_SET" not in cleaned
    assert cleaned == "Done."
    assert directive is not None
    assert directive.query == "opus"
    assert directive.scope == "harness"


def test_extract_none_when_missing() -> None:
    cleaned, directive = extract_model_set_directive("just a normal answer")
    assert cleaned == "just a normal answer"
    assert directive is None


def test_extract_mattermost_line() -> None:
    cleaned, directive = extract_model_set_directive(
        "ok\nMATTERMOST_MODEL_SET: hub-grok-4.6 high harness\n"
    )
    assert "MATTERMOST_MODEL_SET" not in cleaned
    assert directive is not None
    assert directive.query == "hub-grok-4.6"
    assert directive.effort == "high"
    assert directive.scope == "harness"
