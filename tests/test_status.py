import pytest

from mattermost_acp.acp import SessionUpdate
from mattermost_acp.mm.status import (
    TurnStatus,
    format_context,
    format_model_stamp,
    pretty_model_name,
    stamp_answer,
)


def test_whitespace_chunk_does_not_change_status() -> None:
    st = TurnStatus()
    before = st.render()
    st.observe(SessionUpdate(session_id="s", kind="agent_message_chunk", text="\n"))
    st.observe(SessionUpdate(session_id="s", kind="agent_message_chunk", text="   "))
    st.observe(SessionUpdate(session_id="s", kind="agent_message_chunk", text="hello"))
    assert st.render() == before
    assert st.tools == []


def test_tool_call_becomes_status_line_and_timeline() -> None:
    st = TurnStatus()
    st.observe(
        SessionUpdate(
            session_id="s",
            kind="tool_call",
            tool_call_id="t1",
            tool_title="read_file vault",
            tool_kind="read",
        )
    )
    line = st.render()
    assert line.startswith("📖 Reading…")
    assert "> 📖 read_file vault ·" in line
    assert st.tools == ["read_file vault"]


def test_tool_timeline_records_duration_and_failure() -> None:
    st = TurnStatus(started=0.0, done_tool_lines=4)
    st.on_tool_call(tool_call_id="t1", title="run_terminal_command", kind=None, now=0.0)
    st.on_tool_update(
        tool_call_id="t1", title=None, kind=None, status="completed", now=2.5
    )
    st.on_tool_call(tool_call_id="t2", title="grep", kind="search", now=3.0)
    st.on_tool_update(
        tool_call_id="t2", title=None, kind=None, status="failed", now=4.0
    )
    done = st.render(phase="done", now=5.0, stop_reason="end_turn")
    assert done.startswith("✅ Done · end_turn · ")
    assert "2 tools" in done
    assert "1 failed" in done
    assert "> 💻 run_terminal_command · 2.5s" in done
    assert "> 🔍 grep · 1.0s · failed" in done


def test_done_line_shows_context_usage_and_extra() -> None:
    st = TurnStatus(started=0.0)
    st.observe(
        SessionUpdate(
            session_id="s", kind="heartbeat", usage_used=84120, usage_size=500000
        )
    )
    done = st.render(phase="done", now=1.0, extra="titled → New name")
    assert "ctx 84,120/500,000 (17%)" in done
    assert done.rstrip().endswith("titled → New name")


def test_format_context() -> None:
    assert format_context(None, 100) == ""
    assert format_context(1234, None) == "ctx 1,234"
    assert format_context(50, 200) == "ctx 50/200 (25%)"


def test_failed_phase_renders_error_head() -> None:
    st = TurnStatus(started=0.0)
    assert st.render(phase="failed", now=3.0).startswith("❌ Failed · 3.0s")


def test_progress_lines_are_quoted_under_the_status_line() -> None:
    st = TurnStatus()
    st.set_progress_lines(["Checking the config", "Now reading the log"])
    line = st.render()
    assert "> Checking the config" in line
    assert "> Now reading the log" in line


def test_pretty_model_name() -> None:
    assert pretty_model_name("grok-4.6") == "Grok 4.6"
    assert pretty_model_name("hub-claude-4.5-sonnet") == "Claude 4.5 Sonnet"
    assert pretty_model_name(None) == "default"


@pytest.mark.parametrize(
    ("model_id", "label"),
    [
        ("claude-router-6770742d362e312d736f6c", "GPT-6.1 Sol"),
        ("hub-claude-router-6770742D362E312D736F6C", "GPT-6.1 Sol"),
        ("claude-router-" + b"claude-opus-5-5".hex(), "Claude Opus 5 5"),
        ("claude-router-" + b"future-model_v2.3".hex(), "Future Model V2.3"),
        ("hub-gpt-6.1-sol", "GPT-6.1 Sol"),
        ("Claude-Opus-5-5", "Claude Opus 5 5"),
        ("hub-deepseek-v4-pro", "Deepseek V4 Pro"),
    ],
)
def test_router_alias_model_label(model_id: str, label: str) -> None:
    assert pretty_model_name(model_id) == label


@pytest.mark.parametrize(
    "suffix",
    [
        "",  # Empty payload.
        "677",  # Odd-length hex.
        "zz",  # Non-hex.
        "ff",  # Invalid UTF-8.
        "67707420",  # Trailing whitespace.
        "6770740a736f6c",  # Newline.
        "00",  # NUL.
        "2a736f6c2a",  # Markdown.
        "e29883",  # Unicode outside model-id syntax.
        "6770 742d362e312d736f6c",  # Whitespace inside hex.
    ],
)
def test_malformed_router_alias_keeps_existing_label(suffix: str) -> None:
    model_id = "claude-router-" + suffix
    words = model_id.replace("-", " ").split()
    expected = " ".join(word.capitalize() for word in words)
    assert pretty_model_name(model_id) == expected


def test_router_alias_model_stamp() -> None:
    model_id = "claude-router-6770742d362e312d736f6c"
    stamp = format_model_stamp(model_id, effort="high")
    assert stamp == "> *Model: GPT-6.1 Sol · Effort: high*"
    status = TurnStatus(model_label=pretty_model_name(model_id), effort="high")
    assert status.render().startswith(stamp + "\n")
    assert status.render(phase="done").startswith(stamp + "\n")
    assert stamp_answer("hello", stamp) == stamp + "\n\nhello"


def test_status_carries_model_stamp() -> None:
    st = TurnStatus(model_label="Grok 4.6")
    line = st.render()
    assert line.startswith("> *Model: Grok 4.6 · Effort: default*\n")
    assert "Working…" in line
    done = st.render(phase="done")
    assert done.startswith("> *Model: Grok 4.6 · Effort: default*\n")
    assert "✅" in done


def test_stamp_answer_is_idempotent() -> None:
    stamp = format_model_stamp("grok-4.6")
    once = stamp_answer("hello", stamp)
    assert once == "> *Model: Grok 4.6 · Effort: default*\n\nhello"
    assert stamp_answer(once, stamp) == once


def test_live_timeline_keeps_only_the_last_tools() -> None:
    st = TurnStatus(started=0.0, live_tool_lines=3)
    for i in range(7):
        st.on_tool_call(tool_call_id=f"t{i}", title=f"tool_{i}", kind="read", now=float(i))
        st.on_tool_update(
            tool_call_id=f"t{i}", title=None, kind=None, status="completed", now=i + 0.5
        )
    line = st.render(now=8.0)
    assert "> … +4 earlier tools" in line
    assert "tool_0" not in line
    assert "tool_6" in line


def test_done_status_collapses_to_one_summary_line() -> None:
    st = TurnStatus(started=0.0)
    st.on_tool_call(tool_call_id="t1", title="read_file config.py", kind="read", now=0.0)
    st.on_tool_update(
        tool_call_id="t1", title=None, kind=None, status="completed", now=1.0
    )
    st.set_progress_lines(["Checking the config"])
    done = st.render(phase="done", now=2.0, stop_reason="end_turn")
    assert done == "✅ Done · end_turn · 2.0s · 1 tool"


def test_live_progress_trail_is_windowed() -> None:
    st = TurnStatus(live_progress_lines=2)
    st.set_progress_lines(["one", "two", "three", "four"])
    line = st.render()
    assert "> one" not in line
    assert "> …" in line
    assert "> three" in line and "> four" in line
