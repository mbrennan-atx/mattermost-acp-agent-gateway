from mattermost_acp.thread_title import (
    compose_titled_root,
    extract_rename_directive,
    fallback_title_from_prompt,
    heading_from_root,
    is_provisional_title,
    rename_instruction,
    sanitize_title,
)


def test_sanitize_strips_quotes_markdown_and_prefix() -> None:
    assert sanitize_title('Title: "**Fix the TeamCity MCP**"') == "Fix the TeamCity MCP"
    assert sanitize_title("  multi\nline  topic  ") == "multi line topic"


def test_sanitize_truncates() -> None:
    assert len(sanitize_title("x" * 200)) <= 80


def test_extract_rename_last_wins() -> None:
    answer = (
        "Looking into it.\n"
        "MATTERMOST_THREAD_RENAME: draft title\n"
        "More analysis.\n"
        "MATTERMOST_THREAD_RENAME: Gateway thread titles\n"
        "Done."
    )
    cleaned, title = extract_rename_directive(answer)
    assert title == "Gateway thread titles"
    assert "MATTERMOST_THREAD_RENAME" not in cleaned
    assert "Looking into it." in cleaned
    assert "Done." in cleaned


def test_extract_accepts_discord_and_alt_forms() -> None:
    cleaned, title = extract_rename_directive(
        "DISCORD_THREAD_RENAME: Clear stale slash commands\nOk"
    )
    assert title == "Clear stale slash commands"
    assert "DISCORD_THREAD_RENAME" not in cleaned
    cleaned, title = extract_rename_directive("THREAD_TITLE: Root post header\nOk")
    assert title == "Root post header"
    assert "THREAD_TITLE" not in cleaned


def test_extract_no_directive() -> None:
    cleaned, title = extract_rename_directive("just a normal answer")
    assert title is None
    assert cleaned == "just a normal answer"


def test_compose_prepends_then_replaces_heading() -> None:
    original = "can you set up the gateway to update the top level text"
    titled = compose_titled_root(original, "Gateway thread titles")
    assert titled.startswith("**Gateway thread titles**\n\n")
    assert original in titled
    retitled = compose_titled_root(titled, "Thread root titles")
    assert retitled.startswith("**Thread root titles**\n\n")
    assert "Gateway thread titles" not in retitled
    assert original in retitled


def test_heading_from_root() -> None:
    assert heading_from_root("**Gateway thread titles**\n\ncan you set up") == (
        "Gateway thread titles"
    )
    assert heading_from_root("plain first line\nmore") == "plain first line"
    assert heading_from_root("") == ""


def test_provisional_detects_raw_first_message_and_slash_stub() -> None:
    assert is_provisional_title("")
    assert is_provisional_title("(unnamed)")
    assert is_provisional_title("↗ `/new`")
    assert is_provisional_title("can you set up the gateway to update the top level text")
    assert is_provisional_title("is there anything we can do to make threads better?")
    assert not is_provisional_title("Gateway thread titles")
    assert not is_provisional_title("MCP health check")
    assert not is_provisional_title("Untitled topic")


def test_provisional_threshold_matches_instructed_max_length() -> None:
    assert not is_provisional_title("x" * 80)
    assert is_provisional_title("x" * 81)


def test_rename_instruction_requires_summary() -> None:
    text = rename_instruction("vague first message")
    assert "vague first message" in text
    assert "MATTERMOST_THREAD_RENAME:" in text
    assert "NOT the final title" in text


def test_fallback_from_prompt() -> None:
    t = fallback_title_from_prompt(
        "can you set up the gateway to update the top level text of a thread"
    )
    assert t
    assert len(t) <= 60
    assert "gateway" in t.lower() or "thread" in t.lower()
