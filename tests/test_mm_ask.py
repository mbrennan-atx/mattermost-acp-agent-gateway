"""mm-ask must ignore the live status post and wait for the final answer."""

from __future__ import annotations

import importlib.machinery
import importlib.util
from pathlib import Path

_PATH = Path(__file__).resolve().parents[1] / "scripts" / "mm-ask"
_loader = importlib.machinery.SourceFileLoader("mm_ask", str(_PATH))
_spec = importlib.util.spec_from_loader("mm_ask", _loader)
assert _spec is not None and _spec.loader is not None
mm_ask = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mm_ask)

TARGET = "sysadmin"
ROOT = "root"


def _post(post_id: str, message: str, *, user: str = TARGET, create_at: int = 1) -> dict:
    return {
        "id": post_id,
        "user_id": user,
        "message": message,
        "create_at": create_at,
    }


def test_working_placeholder_is_inflight() -> None:
    text = "> *Model: Grok 4.7*\n⏳ Working… (3s)"
    assert mm_ask.classify_reply(text) == "inflight"
    posts = {"s": _post("s", text)}
    assert mm_ask.select_final_reply(posts, TARGET, ROOT) == (None, "waiting")


def test_live_status_with_tool_trail_is_inflight() -> None:
    text = "> *Model: Grok 4.7*\n📖 · 💻 Running… (12s)\n_tools_\n> 💻 run_terminal_command · 1.2s…"
    assert mm_ask.classify_reply(text) == "inflight"


def test_done_summary_is_not_the_answer() -> None:
    status = "> *Model: Grok 4.7*\n✅ Done · end_turn · 1m 04s · 7 tools"
    answer = "> *Model: Grok 4.7*\n\nZoom is disabled on all five homes."
    posts = {
        "s": _post("s", status, create_at=1),
        "a": _post("a", answer, create_at=2),
        "root": _post(ROOT, "@system-admin please", user="other", create_at=0),
    }
    text, state = mm_ask.select_final_reply(posts, TARGET, ROOT)
    assert state == "ready"
    assert text == answer
    assert "Done" not in (text or "")


def test_answer_while_status_still_working_is_pending() -> None:
    status = "> *Model: Grok 4.7*\n⏳ Working… (3s)"
    answer = "> *Model: Grok 4.7*\n\nfixed"
    posts = {
        "s": _post("s", status, create_at=2),
        "a": _post("a", answer, create_at=3),
    }
    assert mm_ask.select_final_reply(posts, TARGET, ROOT) == (answer, "pending")


def test_failed_status_plus_error_post_is_ready() -> None:
    status = "> *Model: Grok 4.7*\n❌ Failed · error · 4s"
    error = "❌ ACP error: connection reset"
    posts = {
        "s": _post("s", status, create_at=1),
        "e": _post("e", error, create_at=2),
    }
    assert mm_ask.select_final_reply(posts, TARGET, ROOT) == (error, "ready")


def test_chunks_join_in_create_order() -> None:
    posts = {
        "b": _post("b", "second", create_at=20),
        "a": _post("a", "first", create_at=10),
        "s": _post("s", "✅ Done · end_turn · 8s", create_at=30),
    }
    text, state = mm_ask.select_final_reply(posts, TARGET, ROOT)
    assert state == "ready"
    assert text == "first\n\nsecond"


def test_other_user_and_empty_are_ignored() -> None:
    posts = {
        "o": _post("o", "not from sysadmin", user="someone", create_at=1),
        "e": _post("e", "   ", create_at=2),
    }
    assert mm_ask.select_final_reply(posts, TARGET, ROOT) == (None, "waiting")


def test_missing_destination_fails_before_network(monkeypatch):
    import pytest

    monkeypatch.setattr(mm_ask.sys, "argv", ["mm-ask", "help"])
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.com")
    monkeypatch.setenv("MATTERMOST_PEER_CHANNEL_ID", "peer")
    monkeypatch.delenv("MATTERMOST_SYSADMIN_USERNAME", raising=False)
    monkeypatch.setattr(mm_ask, "_request", lambda *a, **k: pytest.fail("network called"))
    monkeypatch.setattr(mm_ask, "_token", lambda: pytest.fail("token read"))
    with pytest.raises(SystemExit):
        mm_ask.main()
