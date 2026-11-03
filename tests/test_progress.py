"""Tests for intermediate-vs-final answer assembly."""

from mattermost_acp.mm.progress import (
    INTERMEDIATE_LABEL,
    TurnProgressBuffer,
    format_progress_answer,
)
from mattermost_acp.mm.status import STATUS_BUDGET, TurnStatus


def test_buffer_tools_separate_intermediate_from_final() -> None:
    buf = TurnProgressBuffer()
    buf.on_agent_text("I'll start with vault session setup.")
    buf.on_tool()
    buf.on_agent_text("AWS is on BAR-Static.")
    buf.on_tool()
    buf.on_agent_text("## Research complete\n\nRoot cause was NFS growth.")

    body = buf.format_answer()
    assert body.startswith(INTERMEDIATE_LABEL)
    assert "> I'll start with vault session setup." in body
    assert "> AWS is on BAR-Static." in body
    assert "## Research complete" in body
    # Final answer must not be blockquoted
    assert "> ## Research" not in body
    assert "Root cause was NFS growth." in body


def test_buffer_pure_text_not_quoted() -> None:
    buf = TurnProgressBuffer()
    buf.on_agent_text("Just a normal reply with **markdown**.")
    body = buf.format_answer()
    assert INTERMEDIATE_LABEL not in body
    assert body == "Just a normal reply with **markdown**."


def test_streamed_bold_sentence_survives_final_answer_assembly() -> None:
    for with_tool in (False, True):
        buf = TurnProgressBuffer()
        if with_tool:
            buf.on_agent_text("Checking.")
            buf.on_tool()
        for part in ("**I haven’t verified that.", "**", " I traced the setup."):
            buf.on_agent_text(part)
        expected = "**I haven’t verified that.** I traced the setup."
        assert buf.progress_lines()[-1] == expected
        assert buf.format_answer(include_progress=False) == expected


def test_buffer_only_progress_no_final() -> None:
    buf = TurnProgressBuffer()
    buf.on_agent_text("Working on it.")
    buf.on_tool()
    body = buf.format_answer()
    assert INTERMEDIATE_LABEL in body
    assert "> Working on it." in body


def test_single_structured_paragraph_after_tools_is_the_answer() -> None:
    """A lone structured block is the answer, not narration to blockquote."""
    buf = TurnProgressBuffer()
    buf.on_agent_text("## Result\n- one\n- two")
    buf.on_tool()
    body = buf.format_answer()
    assert body == "## Result\n- one\n- two"
    assert INTERMEDIATE_LABEL not in body


def test_format_progress_answer_empty() -> None:
    assert format_progress_answer(intermediate=[], final="") == ""


def test_final_reply_omits_progress_history_but_preserves_no_final_fallback():
    buf = TurnProgressBuffer()
    for i in range(30):
        buf.on_agent_text(f"Progress checkpoint {i}.")
        buf.on_tool()
    buf.on_agent_text("Finished. CI passed.")
    assert buf.format_answer(include_progress=False) == "Finished. CI passed."

    buf.on_agent_text("## Result\nThe adapter returned its result before a tool.")
    buf.on_tool()
    assert "The adapter returned its result" in buf.format_answer(include_progress=False)


def test_status_progress_accumulates_sequentially() -> None:
    buf = TurnProgressBuffer()
    buf.on_agent_text("I'll start with vault session setup.")
    buf.on_tool()
    buf.on_agent_text("AWS is on BAR-Static.")
    buf.on_tool()
    buf.on_agent_text("Clear climb into mid-2026.")

    st = TurnStatus()
    st.on_tool_call(tool_call_id="1", title="read_file", kind=None)
    st.set_progress_lines(buf.progress_lines())
    body = st.render()
    assert "> I'll start with vault session setup." in body
    assert "> AWS is on BAR-Static." in body
    assert "> Clear climb into mid-2026." in body
    assert body.index("I'll start") < body.index("AWS is on") < body.index("Clear climb")


def test_status_progress_truncates_keeps_newest() -> None:
    st = TurnStatus()
    lines = [f"Step {i}: " + ("x" * 200) for i in range(40)]
    st.set_progress_lines(lines)
    body = st.render()
    assert len(body) <= STATUS_BUDGET
    assert "Step 39:" in body
    assert "> …" in body  # marker that older lines were omitted


def test_status_progress_truncates_single_long_line() -> None:
    st = TurnStatus()
    st.set_progress_lines(["x" * 20000])
    body = st.render()
    assert len(body) <= STATUS_BUDGET
    assert body.startswith("⏳")


def test_status_progress_with_model_stamp_truncates() -> None:
    st = TurnStatus(model_label="Grok 4.6")
    lines = [f"Step {i}: " + ("x" * 200) for i in range(40)]
    st.set_progress_lines(lines)
    body = st.render()
    assert len(body) <= STATUS_BUDGET
    assert body.startswith("> *Model: Grok 4.6 · Effort: default*")
    assert "Step 39:" in body
