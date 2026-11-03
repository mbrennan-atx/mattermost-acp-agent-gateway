import json

from mattermost_acp.mm.chunk import chunk_message
from mattermost_acp.mm.client import parse_posted, ws_url


def test_ws_url() -> None:
    assert ws_url("https://chat.example.com") == "wss://chat.example.com/api/v4/websocket"
    assert (
        ws_url("http://localhost:8065/") == "ws://localhost:8065/api/v4/websocket"
    )


def test_parse_posted_json_string() -> None:
    post = {
        "id": "p1",
        "channel_id": "c1",
        "user_id": "u1",
        "message": "hi",
        "root_id": "",
        "file_ids": ["f1"],
        "type": "",
    }
    event = parse_posted({"event": "posted", "data": {"post": json.dumps(post)}})
    assert event is not None
    assert event.thread_id == "p1"
    assert event.file_ids == ("f1",)
    assert not event.is_thread_reply

    post["root_id"] = "p0"
    event = parse_posted({"event": "posted", "data": {"post": json.dumps(post)}})
    assert event is not None
    assert event.thread_id == "p0"
    assert event.is_thread_reply


def test_parse_posted_skips_system() -> None:
    post = {
        "id": "p1",
        "channel_id": "c1",
        "user_id": "u1",
        "message": "",
        "root_id": "",
        "type": "system_join_channel",
    }
    assert parse_posted({"event": "posted", "data": {"post": json.dumps(post)}}) is None


def test_chunk_message_splits() -> None:
    body = "a" * 20000
    parts = chunk_message(body, limit=8000)
    assert len(parts) >= 2
    assert "".join(parts) == body
