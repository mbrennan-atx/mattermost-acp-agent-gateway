from pathlib import Path

import pytest

from mattermost_acp.store import ThreadStore


@pytest.mark.parametrize("current", ["same", "replaced", "foreign", "deleted"])
async def test_late_model_update_preserves_current_thread_identity_and_metadata(tmp_path, current):
    store = ThreadStore(tmp_path / "native.db", harness="claude")
    await store.open()
    try:
        await store.save(
            "thread",
            "old-session",
            "/isolated",
            model="old-model",
            effort="high",
            channel_id="home",
            title="Existing title",
        )
        if current == "replaced":
            await store.save("thread", "new-session", "/replacement", model="new-model")
        elif current == "foreign":
            store.harness = "codex"
            await store.save("thread", "old-session", "/replacement", model="new-model")
            store.harness = "claude"
        elif current == "deleted":
            await store.delete("thread")
        before = await store.get("thread")
        changed = await store.save_model_if_session("thread", "old-session", "observed-model")
        after = await store.get("thread")
        assert changed is (current == "same")
        if current == "same":
            assert after.model == "observed-model"
            after.model = before.model
        assert after == before
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_store_roundtrip(tmp_path: Path) -> None:
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    await store.save(
        "root111",
        "sess-abc",
        "/home/example/project",
        model="hub-kimi-k2.6",
    )
    rec = await store.get("root111")
    assert rec is not None
    assert rec.session_id == "sess-abc"
    assert rec.model == "hub-kimi-k2.6"
    assert rec.title is None
    assert rec.tag is None
    assert rec.channel_id is None
    assert rec.model_override is None
    assert rec.effort_override is None
    await store.set_title("root111", "TeamCity MCP log pagination")
    await store.set_tag("root111", "Code")
    rec2 = await store.get("root111")
    assert rec2 is not None
    assert rec2.title == "TeamCity MCP log pagination"
    assert rec2.tag == "Code"
    await store.clear_meta("root111")
    rec3 = await store.get("root111")
    assert rec3 is not None
    assert rec3.title == ""
    assert rec3.tag == ""
    await store.set_thread_override("root111", "chan222", "grok-4.6", "xhigh")
    await store.set_model_setting("channel:chan222", "hub-gpt-5.6-sol", "high")
    model, effort, scope = await store.model_preference("root111", "chan222")
    assert (model, effort, scope) == ("grok-4.6", "xhigh", "thread")
    await store.set_model_setting("harness", "hub-claude-opus-5")
    channel = await store.get_model_setting("channel:chan222")
    assert channel is not None
    assert (channel.model, channel.effort) == ("hub-gpt-5.6-sol", "high")
    assert await store.delete("root111") is True
    assert await store.get("root111") is None
    await store.close()


async def test_native_boundary_marker_survives_restart_without_thread_history(tmp_path):
    path = tmp_path / "native.db"
    store = ThreadStore(path, harness="claude")
    await store.open()
    assert not await store.native_boundary_unknown()
    await store.mark_native_boundary_unknown()
    assert await store.get("uncreated-thread") is None
    await store.close()
    reopened = ThreadStore(path, harness="codex")
    await reopened.open()
    try:
        assert await reopened.native_boundary_unknown()
    finally:
        await reopened.close()


async def test_legacy_thread_history_retains_warmth_after_mapping_deletion(tmp_path):
    path = tmp_path / "native.db"
    store = ThreadStore(path, harness="claude")
    await store.open()
    await store.save("historic-thread", "historic-native", "/isolated")
    await store.close()
    reopened = ThreadStore(path, harness="codex")
    await reopened.open()
    try:
        assert await reopened.native_boundary_unknown()
        await reopened.delete("historic-thread")
        assert await reopened.native_boundary_unknown()
    finally:
        await reopened.close()
