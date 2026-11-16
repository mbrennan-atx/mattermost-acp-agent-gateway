"""Stop persistence and terminal-card work retain bounded, owned evidence."""

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from mattermost_acp.acp import SessionUpdate
from mattermost_acp.collaboration import consumer as consumer_module
from mattermost_acp.collaboration.consumer import CollaborationConsumer
from mattermost_acp.mm.reporting import TurnReporter


def stop_consumer(path):
    consumer = object.__new__(CollaborationConsumer)
    consumer.stops_path = path
    consumer.instance = "observed-instance"
    consumer.pending_stops = {"origin": {"key": "intent", "phase": "new"}}
    return consumer


def test_stop_save_preserves_preplaced_temporary_symlink(tmp_path):
    parent = tmp_path / "state"
    parent.mkdir(mode=0o700)
    victim = tmp_path / "peer.txt"
    victim.write_text("preserve peer bytes")
    consumer = stop_consumer(parent / "stops.json")
    old_temporary = parent / (consumer.stops_path.name + "." + consumer.instance)
    old_temporary.symlink_to(victim)
    consumer.save_stops()
    assert victim.read_text() == "preserve peer bytes"
    assert old_temporary.is_symlink()
    assert not consumer.stops_path.is_symlink()
    assert json.loads(consumer.stops_path.read_text()) == consumer.pending_stops
    assert consumer.stops_path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("mode", [0o777, 0o770])
def test_stop_save_refuses_shared_writable_parent_and_retains_intent(tmp_path, mode):
    parent = tmp_path / "state"
    parent.mkdir()
    parent.chmod(mode)
    consumer = stop_consumer(parent / "stops.json")
    with pytest.raises(PermissionError):
        consumer.save_stops()
    intent = consumer.record_stop("new-origin")
    assert consumer.pending_stops["new-origin"] == intent
    assert not consumer.stops_path.exists()
    assert not list(parent.iterdir())
    assert parent.stat().st_mode & 0o777 == mode


def test_stop_save_uses_descriptor_permissions_and_replaces_current_receipt(tmp_path, monkeypatch):
    consumer = stop_consumer(tmp_path / "state/stops.json")
    monkeypatch.setattr(Path, "chmod", lambda *_a, **_k: pytest.fail("No pathname chmod"))
    consumer.save_stops()
    consumer.pending_stops = {}
    consumer.save_stops()
    assert json.loads(consumer.stops_path.read_text()) == {}
    assert list(consumer.stops_path.parent.iterdir()) == [consumer.stops_path]


def test_stop_save_preserves_foreign_temporary_replacement(tmp_path, monkeypatch):
    consumer = stop_consumer(tmp_path / "state/stops.json")
    original_fsync = os.fsync
    replaced = None

    def replace_temporary(fd):
        nonlocal replaced
        original_fsync(fd)
        if replaced is None:
            files = list(consumer.stops_path.parent.glob("*.tmp"))
            assert len(files) == 1
            replaced = files[0]
            replaced.unlink()
            replaced.write_text("peer replacement")

    monkeypatch.setattr(os, "fsync", replace_temporary)
    with pytest.raises(OSError, match="temporary file changed"):
        consumer.save_stops()
    assert replaced.read_text() == "peer replacement"
    assert not consumer.stops_path.exists()


def test_stop_save_refuses_a_fresh_temporary_name_collision(tmp_path, monkeypatch):
    consumer = stop_consumer(tmp_path / "state/stops.json")
    consumer.stops_path.parent.mkdir(mode=0o700)
    consumer.stops_path.write_text("previous receipt")
    victim = tmp_path / "peer.txt"
    victim.write_text("preserve peer bytes")
    collision = consumer.stops_path.parent / ".stops.json.synthetic.tmp"
    collision.symlink_to(victim)
    monkeypatch.setattr(consumer_module.uuid, "uuid4", lambda: SimpleNamespace(hex="synthetic"))
    with pytest.raises(FileExistsError):
        consumer.save_stops()
    assert victim.read_text() == "preserve peer bytes"
    assert collision.is_symlink()
    assert consumer.stops_path.read_text() == "previous receipt"


def test_stop_save_refuses_a_symlinked_state_directory(tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir(mode=0o700)
    current = actual / "stops.json"
    current.write_text("preserve current receipt")
    linked = tmp_path / "linked"
    linked.symlink_to(actual, target_is_directory=True)
    consumer = stop_consumer(linked / "stops.json")
    with pytest.raises(OSError):
        consumer.save_stops()
    assert current.read_text() == "preserve current receipt"
    assert list(actual.iterdir()) == [current]


def test_stop_save_refuses_a_directory_owned_by_another_user(tmp_path, monkeypatch):
    consumer = stop_consumer(tmp_path / "state/stops.json")
    consumer.stops_path.parent.mkdir(mode=0o700)
    owner = consumer.stops_path.parent.stat().st_uid
    monkeypatch.setattr(os, "geteuid", lambda: owner + 1)
    with pytest.raises(PermissionError):
        consumer.save_stops()
    assert not list(consumer.stops_path.parent.iterdir())


def test_stop_save_failed_file_sync_preserves_current_receipt_and_closes_files(
    tmp_path, monkeypatch
):
    consumer = stop_consumer(tmp_path / "state/stops.json")
    consumer.save_stops()
    previous = consumer.stops_path.read_bytes()
    consumer.pending_stops["origin"]["phase"] = "uncertain"
    descriptors = []

    def unavailable(descriptor):
        descriptors.append(descriptor)
        raise OSError("File synchronization unavailable")

    monkeypatch.setattr(os, "fsync", unavailable)
    with pytest.raises(OSError, match="File synchronization unavailable"):
        consumer.save_stops()
    assert consumer.pending_stops["origin"]["phase"] == "uncertain"
    assert consumer.stops_path.read_bytes() == previous
    assert list(consumer.stops_path.parent.iterdir()) == [consumer.stops_path]
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def task_update(task, tool, state="completed"):
    return SessionUpdate(
        "native",
        "async_task_state_update",
        raw={"asyncTaskId": task, "toolCallId": tool, "state": state},
    )


def background_card(tool):
    return SessionUpdate(
        "native",
        "tool_call_update",
        tool_call_id=tool,
        tool_status="completed",
        raw={"_meta": {"jetbrains": {"air": {"asyncTasks": {"backgrounded": True}}}}},
    )


def test_background_card_does_not_walk_completed_root_history():
    reporter = TurnReporter()
    for index in range(512):
        reporter.observe(task_update(f"task-{index}", f"tool-{index}"))

    class History(dict):
        def values(self):
            pytest.fail("Routine card lookup scanned retained history")

    reporter.async_tasks = History(reporter.async_tasks)
    reporter.observe(background_card("tool-511"))
    assert "tool-511" not in reporter.detached_tools
    reporter.observe(background_card("unresolved-tool"))
    assert "unresolved-tool" in reporter.detached_tools
    assert len(reporter.async_tasks) == 512


def test_terminal_index_follows_current_task_tool_without_losing_other_receipts():
    reporter = TurnReporter()
    reporter.observe(task_update("first", "old-tool"))
    reporter.observe(task_update("second", "old-tool"))
    reporter.observe(task_update("first", "new-tool", "running"))
    reporter.observe(background_card("old-tool"))
    reporter.observe(background_card("new-tool"))
    assert not reporter.detached_tools
    assert reporter.async_tasks["first"]["state"] == "completed"
    reporter.observe(task_update("second", "third-tool"))
    reporter.observe(background_card("old-tool"))
    assert reporter.detached_tools == {"old-tool"}


def test_long_turn_background_card_lookup_work_grows_with_current_updates():
    visits = 0

    class History(dict):
        def values(self):
            nonlocal visits
            for task in super().values():
                visits += 1
                yield task

    reporter = TurnReporter()
    reporter.async_tasks = History()
    count = 512
    for index in range(count):
        tool = f"tool-{index}"
        reporter.observe(SessionUpdate("native", "tool_call", tool_call_id=tool))
        reporter.observe(task_update(f"task-{index}", tool))
        reporter.observe(background_card(tool))
        assert tool not in reporter.detached_tools
    assert len(reporter.async_tasks) == count
    assert visits <= count * 2, "Completed background cards must not rescan prior command history"
    reporter.observe(background_card("new-detached-command"))
    assert reporter.detached_tools == {"new-detached-command"}


@pytest.mark.parametrize("terminal", ["completed", "failed", "stopped"])
def test_late_partial_task_progress_retains_terminal_card_evidence(terminal):
    reporter = TurnReporter()
    reporter.observe(SessionUpdate("native", "tool_call", tool_call_id="bash"))
    reporter.observe(task_update("task", "bash", terminal))
    reporter.observe(
        SessionUpdate(
            "native", "async_task_progress", raw={"asyncTaskId": "task", "state": "running"}
        )
    )
    reporter.observe(background_card("bash"))
    assert reporter.async_tasks["task"]["state"] == terminal
    assert not reporter.has_pending_work()
    assert reporter.stoppable_async_tasks() == []
