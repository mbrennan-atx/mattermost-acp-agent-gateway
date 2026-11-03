from datetime import UTC, datetime, timedelta
from pathlib import Path

from mattermost_acp.commands.pending_restart import (
    PendingRestart,
    consume_pending,
    load_pending,
    now_pending,
    pending_path,
    write_pending,
)


def test_pending_path_colocates_with_db(tmp_path: Path) -> None:
    db = tmp_path / "data" / "threads.db"
    assert pending_path(db) == (tmp_path / "data" / "pending_restart.json").resolve()


def test_write_load_consume(tmp_path: Path) -> None:
    path = tmp_path / "pending_restart.json"
    p = now_pending(
        thread_id="rootpostid00000000000001",
        session_id="sess-abc",
        user_id="userid00000000000000000001",
        reason="slash_restart",
        team_id="teamid00000000000000000001",
        parent_channel_id="chanid0000000000000000001",
    )
    write_pending(path, p)
    loaded = load_pending(path)
    assert loaded is not None
    assert loaded.thread_id == "rootpostid00000000000001"
    assert loaded.session_id == "sess-abc"
    assert loaded.user_id == "userid00000000000000000001"
    assert loaded.reason == "slash_restart"
    assert loaded.team_id == "teamid00000000000000000001"
    assert loaded.parent_channel_id == "chanid0000000000000000001"
    assert loaded.age_s < 5

    consumed = consume_pending(path)
    assert consumed is not None
    assert consumed.session_id == "sess-abc"
    assert not path.exists()
    assert consume_pending(path) is None


def test_stale_pending_is_dropped(tmp_path: Path) -> None:
    path = tmp_path / "pending_restart.json"
    old = PendingRestart(
        thread_id="1",
        session_id="x",
        user_id="1",
        requested_at=(datetime.now(UTC) - timedelta(hours=2)).isoformat(),
        reason="slash_restart",
        pid=1,
    )
    write_pending(path, old)
    assert consume_pending(path, max_age_s=60) is None
    assert not path.exists()
