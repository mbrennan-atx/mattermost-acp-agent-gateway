from mattermost_acp.channel_context import session_meta_rules, wrap_user_prompt


def test_session_rules_are_mattermost() -> None:
    meta = session_meta_rules()
    assert "Mattermost" in meta["rules"]
    assert "MATTERMOST_MODEL_SET" in meta["rules"]
    assert "MATTERMOST_THREAD_RENAME" in meta["rules"]
    assert "DISCORD_CHUNK" not in meta["rules"]


def test_wrap_user_prompt() -> None:
    text = wrap_user_prompt(
        "hello",
        thread_id="root1",
        thread_name="topic",
        author_name="Example User",
        author_id="u1",
        parent_channel_id="c1",
        team_id="t1",
        harness="grok",
    )
    assert "<channel_context>" in text
    assert "channel: mattermost" in text
    assert "harness: grok" in text
    assert "thread_id: root1" in text
    assert text.endswith("hello")


def test_wrap_includes_role_text() -> None:
    text = wrap_user_prompt(
        "hello",
        thread_id="root1",
        thread_name="topic",
        author_name="Example User",
        author_id="u1",
        role_text="You are Support Assistant.\nFix the box.",
    )
    assert "role:" in text
    assert "You are Support Assistant." in text
    assert text.index("role:") < text.index("</channel_context>")
    assert text.endswith("hello")


def test_selected_instruction_failure_is_explicit(tmp_path):
    import pytest

    from mattermost_acp.channel_context import load_role_text

    assert load_role_text(None) == ""
    with pytest.raises(ValueError, match="Selected instruction"):
        load_role_text(tmp_path / "missing")
    first, second = tmp_path / "shared.md", tmp_path / "role.md"
    first.write_text("  " + "a" * 5000 + "  ")
    second.write_text("  role  ")
    assert load_role_text(first, second) == "a" * 4000 + "\n\nrole"
