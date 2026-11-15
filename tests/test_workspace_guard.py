"""Workspace guard refusal cleans up only the guard's owned process group."""

import asyncio
import contextlib
import json
import os
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
