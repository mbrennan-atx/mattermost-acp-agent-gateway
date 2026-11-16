"""Workspace guard refusal cleans up only the guard's owned process group."""

import asyncio
import contextlib
import json
import os
import select
import signal
import sys
from pathlib import Path

import pytest

from mattermost_acp.acp.broker import NativeDispatchChanged
from mattermost_acp.main import build_workspace_guard


@pytest.mark.skipif(os.name != "posix" or not Path("/proc").exists(), reason="Linux process groups")
@pytest.mark.parametrize("failure", ["exit", "signal"])
async def test_exited_guard_failure_stops_surviving_children(tmp_path, failure):
    pid_file = tmp_path / "guard-pids.json"
    program = tmp_path / "guard"
    program.write_text(
        f"#!{sys.executable}\n"
        "import json,os,signal,subprocess,sys\nfrom pathlib import Path\n"
        "children=[subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']) "
        "for _ in range(2)]\n"
        f"Path({str(pid_file)!r}).write_text("
        "json.dumps([os.getpid(),*[p.pid for p in children]]))\n"
        + ("sys.exit(4)\n" if failure == "exit" else "os.kill(os.getpid(),signal.SIGTERM)\n")
    )
    program.chmod(0o700)
    try:
        with pytest.raises(NativeDispatchChanged, match="Workspace verification refused"):
            await build_workspace_guard(str(program))("thread", tmp_path, None)
        parent, *children = json.loads(pid_file.read_text())
        assert len(children) == 2

        def alive(pid):
            try:
                return Path(f"/proc/{pid}/stat").read_text().split()[2] != "Z"
            except FileNotFoundError:
                return False

        async with asyncio.timeout(2):
            while any(alive(pid) for pid in children):
                await asyncio.sleep(0.005)
        assert not Path(f"/proc/{parent}").exists()
    finally:
        if pid_file.exists():
            parent, *_ = json.loads(pid_file.read_text())
            with contextlib.suppress(ProcessLookupError):
                os.killpg(parent, signal.SIGKILL)


async def _pidfd_exit(descriptor):
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    loop.add_reader(descriptor, lambda: done.done() or done.set_result(None))
    try:
        await done
    finally:
        loop.remove_reader(descriptor)


@pytest.mark.skipif(not hasattr(os, "pidfd_open"), reason="Linux process identity proof")
async def test_successful_guard_settles_its_forked_configuration_children(tmp_path):
    proof_file = tmp_path / "successful-guard.json"
    program = tmp_path / "successful-guard"
    program.write_text(
        f"#!{sys.executable}\nimport json,os,signal\n"
        "child=os.fork()\n"
        "if child == 0:\n    signal.pause()\n    os._exit(0)\n"
        f"with open({str(proof_file)!r},'w') as out:\n"
        "    out.write(json.dumps({'child':child,'group':os.getpgrp()}))\n"
    )
    program.chmod(0o700)
    await build_workspace_guard(str(program))("thread", tmp_path, None)
    proof = json.loads(proof_file.read_text())
    try:
        descriptor = os.pidfd_open(proof["child"])
    except ProcessLookupError:
        return
    try:
        async with asyncio.timeout(1):
            await _pidfd_exit(descriptor)
    finally:
        if not select.select([descriptor], [], [], 0)[0]:
            os.killpg(proof["group"], signal.SIGKILL)
            await _pidfd_exit(descriptor)
        os.close(descriptor)


@pytest.mark.skipif(not hasattr(os, "pidfd_open"), reason="Linux process identity proof")
@pytest.mark.parametrize("finish", ["cancel", "timeout"])
async def test_guard_launch_publication_interruption_stops_actual_forked_child(
    tmp_path, monkeypatch, finish
):
    """Interrupt publication after real fork, before the caller gets its handle."""
    ready_path = tmp_path / "ready.fifo"
    os.mkfifo(ready_path)
    ready_fd = os.open(ready_path, os.O_RDWR | os.O_NONBLOCK)
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await asyncio.get_running_loop().connect_read_pipe(
        lambda: protocol, os.fdopen(ready_fd, "rb", buffering=0)
    )
    program = tmp_path / "forking-guard"
    program.write_text(
        f"#!{sys.executable}\nimport json,os,signal\n"
        "child=os.fork()\n"
        "if child == 0:\n    signal.pause()\n    os._exit(0)\n"
        f"with open({str(ready_path)!r},'w') as out:\n"
        "    out.write(json.dumps({'parent':os.getpid(),'child':child,"
        "'group':os.getpgrp()})+'\\n')\n"
        "signal.pause()\n"
    )
    program.chmod(0o700)
    original_create = asyncio.create_subprocess_exec
    started, publish = asyncio.Event(), asyncio.Event()
    proof, process = {}, None

    async def delayed_publication(*args, **kwargs):
        nonlocal process
        process = await original_create(*args, **kwargs)
        proof.update(json.loads(await reader.readline()))
        started.set()
        try:
            await publish.wait()
        except asyncio.CancelledError:
            publish.set()
            raise
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_publication)
    task = asyncio.create_task(
        build_workspace_guard(str(program), timeout_s=1 if finish == "timeout" else 10)(
            "thread", tmp_path, None
        )
    )
    watcher, child_fd, parent_fd = None, None, None
    try:
        async with asyncio.timeout(3):
            await started.wait()
        child_fd = os.pidfd_open(proof["child"])
        parent_fd = os.pidfd_open(process.pid)

        async def release_after_owned_launch_exits():
            await _pidfd_exit(parent_fd)
            publish.set()

        watcher = asyncio.create_task(release_after_owned_launch_exits())
        if finish == "cancel":
            task.cancel()
            publish.set()
        with pytest.raises(asyncio.CancelledError if finish == "cancel" else NativeDispatchChanged):
            async with asyncio.timeout(3):
                await task
        async with asyncio.timeout(1):
            await _pidfd_exit(child_fd)
    finally:
        publish.set()
        if watcher is not None:
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher
        # Only a still-live, pidfd-proven fixture child permits this cleanup.
        if child_fd is not None and not select.select([child_fd], [], [], 0)[0]:
            os.killpg(proof["group"], signal.SIGKILL)
            await _pidfd_exit(child_fd)
        if process is not None:
            await process.wait()
        for descriptor in (parent_fd, child_fd):
            if descriptor is not None:
                os.close(descriptor)
        transport.close()


@pytest.mark.skipif(not hasattr(os, "pidfd_open"), reason="Linux process identity proof")
async def test_reaped_guard_group_identifier_cannot_signal_unrelated_sentinel(
    tmp_path, monkeypatch
):
    """Inject group-ID reuse after real reaping; observe the sentinel's lifetime."""
    sentinel = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import os,signal; os.write(1,b'ready\\n'); signal.pause()",
        stdout=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    await sentinel.stdout.readline()
    sentinel_fd = os.pidfd_open(sentinel.pid)
    program = tmp_path / "failed-guard"
    program.write_text(f"#!{sys.executable}\nraise SystemExit(4)\n")
    program.chmod(0o700)
    original_create, original_killpg = asyncio.create_subprocess_exec, os.killpg
    launched, reused = [], []

    async def capture_launch(*args, **kwargs):
        process = await original_create(*args, **kwargs)
        launched.append(process)
        return process

    def emulate_reused_group(group, selected_signal):
        if launched and group == launched[-1].pid and launched[-1].returncode is not None:
            # The original leader was actually reaped. Dispatch now resolves
            # its stale numeric identity to this separately owned live group.
            reused.append(group)
            return original_killpg(sentinel.pid, selected_signal)
        return original_killpg(group, selected_signal)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", capture_launch)
    monkeypatch.setattr(os, "killpg", emulate_reused_group)
    try:
        with pytest.raises(NativeDispatchChanged):
            await build_workspace_guard(str(program))("thread", tmp_path, None)
        if reused:
            await sentinel.wait()
        assert not select.select([sentinel_fd], [], [], 0)[0]
    finally:
        if sentinel.returncode is None:
            sentinel.kill()
        await sentinel.wait()
        os.close(sentinel_fd)
