"""SessionService.switch_model against the fake agent process."""

from __future__ import annotations

import asyncio
import json
import sys
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, patch
from urllib.parse import quote

import pytest
from fake_agent import spawn_fake_agent_process

from mattermost_acp.acp.broker import AcpBroker, AcpError, NativeDispatchChanged
from mattermost_acp.commands.models_catalog import ModelCatalog, ModelInfo
from mattermost_acp.main import build_workspace_guard
from mattermost_acp.session_service import SessionService, discover_session_model
from mattermost_acp.store import ThreadStore


@pytest.fixture
async def service(tmp_path: Path):
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    svc = SessionService(broker, store, default_cwd=tmp_path)
    yield svc
    await broker.stop()
    await store.close()


@pytest.mark.asyncio
async def test_switch_model_updates_store(service: SessionService) -> None:
    thread_id = "42"
    await service.ensure_session(thread_id)
    switch = await service.switch_model(thread_id, "hub-claude-5-sonnet")
    assert switch.model_id == "hub-claude-5-sonnet"
    assert switch.compaction == "not_needed"

    record = await service.store.get(thread_id)
    assert record is not None
    assert record.model == "hub-claude-5-sonnet"


async def test_compaction_fences_previous_project_cancellation(service):
    sid = await service.ensure_session("compact-thread")
    old_epoch = service.native_epoch("compact-thread")
    entered, release = asyncio.Event(), asyncio.Event()
    original_prompt = service.broker.prompt_turn

    async def compact(session, prompt):
        entered.set()
        await release.wait()
        return await original_prompt(session, prompt)

    service.broker.prompt_turn = compact
    task = asyncio.create_task(service._compact_history("compact-thread", sid))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert service.is_busy("compact-thread")
        with pytest.raises(NativeDispatchChanged):
            await service.cancel("compact-thread", expected_session=sid, expected_epoch=old_epoch)
        release.set()
        assert await task == "compacted"
        assert not service.is_busy("compact-thread")
        assert service.native_epoch("compact-thread") > old_epoch
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_workspace_guard_precedes_native_start_and_resume(service):
    events = []
    start = service.broker.start

    async def tracked_start():
        events.append("start")
        await start()

    async def guard(thread, cwd, prior):
        events.append((thread, cwd, prior))

    service.broker.start = tracked_start
    service.workspace_guard = guard
    sid = await service.ensure_session("guarded")
    assert events[0] == ("guarded", service.default_cwd.resolve(), None)
    events.clear()
    assert await service.ensure_session("guarded") == sid
    assert events[0] == ("guarded", service.default_cwd.resolve(), sid)
    assert events[1] == "start"


@pytest.mark.parametrize("operation", ["create", "resume", "replace"])
async def test_workspace_refusal_preserves_native_and_stored_session(service, operation):
    if operation != "create":
        await service.ensure_session("guarded")
    record = await service.store.get("guarded")
    epoch = service.native_epoch("guarded")

    async def refuse(*args):
        raise NativeDispatchChanged("Workspace settings changed")

    service.workspace_guard = refuse
    names = ["start", "session_new", "session_load", "session_resume", "session_close", "cancel"]
    with ExitStack() as stack:
        methods = [
            stack.enter_context(patch.object(service.broker, name, new_callable=AsyncMock))
            for name in names
        ]
        with pytest.raises(NativeDispatchChanged):
            if operation == "replace":
                await service.new_session("guarded")
            else:
                await service.ensure_session("guarded")
        for method in methods:
            method.assert_not_awaited()
    assert await service.store.get("guarded") == record
    assert service.native_epoch("guarded") == epoch


def guard_program(tmp_path, body):
    script = tmp_path / "guard with spaces"
    script.write_text(f"#!{sys.executable}\n" + body)
    script.chmod(0o700)
    return str(script)


async def test_workspace_executable_receives_literal_arguments_without_a_shell(tmp_path):
    output = tmp_path / "arguments.json"
    marker = tmp_path / "unexpected"
    command = guard_program(
        tmp_path,
        "import json,sys\nfrom pathlib import Path\n"
        f"Path({str(output)!r}).write_text(json.dumps(sys.argv[1:]))\n",
    )
    guard = build_workspace_guard(command)
    cwd = tmp_path / ("workspace $(touch " + str(marker) + ")")
    await guard("thread-$VALUE", cwd, None)
    assert json.loads(output.read_text()) == ["thread-$VALUE", str(cwd), ""]
    assert not marker.exists()
    await guard("thread-$VALUE", cwd, "previous-native-session")
    assert json.loads(output.read_text())[-1] == "previous-native-session"
    assert build_workspace_guard(None) is None


async def test_failed_workspace_executable_returns_only_a_safe_refusal(tmp_path):
    command = guard_program(tmp_path, "import sys\nprint('untrusted output')\nsys.exit(4)\n")
    with pytest.raises(NativeDispatchChanged, match="Workspace verification refused"):
        await build_workspace_guard(command)("thread", tmp_path, None)


@pytest.mark.parametrize("finish", ["timeout", "cancel"])
async def test_interrupted_workspace_guard_stops_its_owned_process_group(tmp_path, finish):
    pid_file = tmp_path / "child.pid"
    body = (
        "import subprocess,sys,time\nfrom pathlib import Path\n"
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])\n"
        f"Path({str(pid_file)!r}).write_text(str(child.pid))\n"
        "time.sleep(30)\n"
    )
    guard = build_workspace_guard(
        guard_program(tmp_path, body), timeout_s=0.3 if finish == "timeout" else 5
    )
    task = asyncio.create_task(guard("thread", tmp_path, None))
    async with asyncio.timeout(2):
        while not pid_file.exists():
            await asyncio.sleep(0.005)
    pid = int(pid_file.read_text())
    if finish == "cancel":
        task.cancel()
    with pytest.raises(asyncio.CancelledError if finish == "cancel" else NativeDispatchChanged):
        await task
    async with asyncio.timeout(2):
        while True:
            state = Path(f"/proc/{pid}/stat")
            if not state.exists() or state.read_text().split()[2] == "Z":
                break
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_switch_model_unknown_raises_and_does_not_persist(
    service: SessionService,
) -> None:
    thread_id = "43"
    await service.ensure_session(thread_id)
    with pytest.raises(AcpError):
        await service.switch_model(thread_id, "bogus-model")

    # Store keeps whatever model it had before (None here — never switched).
    record = await service.store.get(thread_id)
    assert record is not None
    assert record.model is None


@pytest.mark.asyncio
async def test_ensure_session_pins_configured_default(tmp_path: Path) -> None:
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    svc = SessionService(broker, store, default_cwd=tmp_path, default_model="grok-4.6")
    try:
        await svc.ensure_session("1")
        record = await store.get("1")
        assert record is not None
        assert record.model == "grok-4.6"
    finally:
        await broker.stop()
        await store.close()


@pytest.mark.asyncio
async def test_ensure_session_pins_catalog_default_when_unconfigured(
    tmp_path: Path,
) -> None:
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    svc = SessionService(broker, store, default_cwd=tmp_path, grok_command="grok")
    catalog = ModelCatalog(
        default_id="grok-4.6",
        models=[ModelInfo(id="grok-4.6", is_default=True)],
        raw="",
        fetched_at=0.0,
    )
    try:
        with patch(
            "mattermost_acp.session_service.fetch_catalog",
            new=AsyncMock(return_value=catalog),
        ):
            await svc.ensure_session("2")
        record = await store.get("2")
        assert record is not None
        assert record.model == "grok-4.6"
    finally:
        await broker.stop()
        await store.close()


@pytest.mark.asyncio
async def test_resume_reapplies_stored_model(service: SessionService) -> None:
    thread_id = "50"
    await service.ensure_session(thread_id)
    await service.switch_model(thread_id, "hub-gpt-5.6-sol")
    calls: list[str] = []
    orig = service.broker.set_model

    async def wrap(session_id: str, model_id: str, **kwargs):
        calls.append(model_id)
        return await orig(session_id, model_id, **kwargs)

    service.broker.set_model = wrap  # type: ignore[method-assign]
    assert thread_id in service._pinned
    # A second ensure resumes the adapter, which can restore its default.
    await service.ensure_session(thread_id)
    assert calls == ["hub-gpt-5.6-sol"]
    record = await service.store.get(thread_id)
    assert record is not None
    assert record.model == "hub-gpt-5.6-sol"


@pytest.mark.asyncio
async def test_resume_discovers_live_model_when_store_blank(
    tmp_path: Path,
) -> None:
    cwd = tmp_path / "vault"
    cwd.mkdir()
    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(tmp_path / "t.db")
    await store.open()
    grok_home = tmp_path / "grok-home"
    svc = SessionService(broker, store, default_cwd=cwd, grok_home=grok_home)
    try:
        sid = await svc.ensure_session("3", cwd=cwd)
        record = await store.get("3")
        assert record is not None
        assert record.model is None

        summary_dir = grok_home / "sessions" / quote(str(cwd.resolve()), safe="") / sid
        summary_dir.mkdir(parents=True)
        (summary_dir / "summary.json").write_text(
            json.dumps({"current_model_id": "gpt-5.6-sol"}),
            encoding="utf-8",
        )
        svc._pinned.clear()
        await svc.ensure_session("3")
        record = await store.get("3")
        assert record is not None
        assert record.model == "gpt-5.6-sol"
        st = await svc.status("3")
        assert st["model"] == "gpt-5.6-sol"
    finally:
        await broker.stop()
        await store.close()


@pytest.mark.asyncio
async def test_new_session_keeps_previous_model(service: SessionService) -> None:
    thread_id = "51"
    await service.ensure_session(thread_id)
    await service.switch_model(thread_id, "hub-claude-opus-5", effort="high")
    old = await service.store.get(thread_id)
    assert old is not None
    new_sid = await service.new_session(thread_id)
    assert new_sid != old.session_id
    record = await service.store.get(thread_id)
    assert record is not None
    assert record.model == "hub-claude-opus-5"
    assert record.effort == "high"


@pytest.mark.asyncio
async def test_channel_and_harness_defaults_apply_to_new_threads(
    service: SessionService,
) -> None:
    await service.store.set_model_setting("harness", "hub-claude-opus-5")
    await service.ensure_session("60", channel_id="700")
    harness_record = await service.store.get("60")
    assert harness_record is not None
    assert harness_record.model == "hub-claude-opus-5"
    assert harness_record.channel_id == "700"

    await service.store.set_model_setting("channel:700", "hub-gpt-5.6-sol", "high")
    await service.ensure_session("61", channel_id="700")
    channel_record = await service.store.get("61")
    assert channel_record is not None
    assert channel_record.model == "hub-gpt-5.6-sol"
    assert channel_record.effort == "high"


@pytest.mark.asyncio
async def test_scope_change_switches_current_thread_and_sets_default(
    service: SessionService,
) -> None:
    await service.ensure_session("70", channel_id="800")
    await service.switch_model(
        "70",
        "hub-claude-opus-5",
        scope="channel",
        channel_id="800",
    )
    current = await service.store.get("70")
    assert current is not None
    assert current.model == "hub-claude-opus-5"
    assert current.model_override is None

    await service.ensure_session("71", channel_id="800")
    sibling = await service.store.get("71")
    assert sibling is not None
    assert sibling.model == "hub-claude-opus-5"


@pytest.mark.asyncio
async def test_thread_override_wins_when_setting_channel_default(
    service: SessionService,
) -> None:
    await service.ensure_session("80", channel_id="900")
    await service.switch_model(
        "80",
        "hub-claude-opus-5",
        effort="high",
        scope="thread",
        channel_id="900",
    )
    result = await service.switch_model(
        "80",
        "hub-gpt-5.6-sol",
        effort="low",
        scope="channel",
        channel_id="900",
    )
    assert result.model_id == "hub-gpt-5.6-sol"
    assert result.active_model == "hub-claude-opus-5"
    current = await service.store.get("80")
    assert current is not None
    assert current.model == "hub-claude-opus-5"
    assert current.effort == "high"
    assert current.model_override == "hub-claude-opus-5"
    assert current.effort_override == "high"

    await service.ensure_session("81", channel_id="900")
    sibling = await service.store.get("81")
    assert sibling is not None
    assert sibling.model == "hub-gpt-5.6-sol"
    assert sibling.effort == "low"


def test_discover_session_model_reads_summary(tmp_path: Path) -> None:
    cwd = tmp_path / "proj"
    cwd.mkdir()
    sid = "01a000aa-2b55-73e3-b3a3-5e3d46d1dc05"
    dest = tmp_path / "sessions" / quote(str(cwd), safe="") / sid
    dest.mkdir(parents=True)
    (dest / "summary.json").write_text(
        json.dumps({"current_model_id": "gpt-5.6-sol"}), encoding="utf-8"
    )
    assert discover_session_model(sid, cwd, grok_home=tmp_path) == "gpt-5.6-sol"
    assert discover_session_model("missing", cwd, grok_home=tmp_path) is None


@pytest.mark.asyncio
async def test_ensure_session_starts_fresh_after_harness_switch(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    old = ThreadStore(db, harness="grok")
    await old.open()
    await old.save("70", "grok-session", str(tmp_path), model="grok-4.6", channel_id="c1")
    await old.set_title("70", "Kept title")
    await old.close()

    broker = AcpBroker(process_factory=spawn_fake_agent_process)
    store = ThreadStore(db, harness="claude")
    await store.open()
    marker = tmp_path / "t.busy.json"
    svc = SessionService(broker, store, default_cwd=tmp_path, busy_marker=marker)
    try:
        sid = await svc.ensure_session("70")
        assert sid != "grok-session"
        record = await store.get("70")
        assert record is not None
        assert record.harness == "claude"
        assert record.title == "Kept title"
        assert record.channel_id == "c1"
        assert record.model != "grok-4.6"
        assert await svc.ensure_session("70") == sid
        await svc.run_turn("70", "hello")
        assert not marker.exists()
    finally:
        await broker.stop()
        await store.close()
