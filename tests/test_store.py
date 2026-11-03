from pathlib import Path

import pytest

from mattermost_acp.store import ThreadStore


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
