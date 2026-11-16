"""Pinned guards retain checked source across warmed reuse and replacement races."""

import asyncio
import hashlib
import inspect
import json
import os
from unittest.mock import AsyncMock, patch

import pytest

import mattermost_acp.main as entry
from mattermost_acp.acp.broker import NativeDispatchChanged

pytest_plugins = ["test_session_service"]


def pinned_guard(path, **kwargs):
    # Exercise the old direct-exec behavior on the review baseline too.
    if "source_sha256" in inspect.signature(entry.build_workspace_guard).parameters:
        kwargs["source_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return entry.build_workspace_guard(str(path), **kwargs)


async def test_replaced_pinned_guard_cannot_bypass_actual_warmed_native_reuse(service):
    script = service.default_cwd / "workspace-guard"
    proof = service.default_cwd / "guard-arguments.json"
    script.write_text(
        "#!/usr/bin/python3\nimport json,sys\nfrom pathlib import Path\n"
        f"Path({str(proof)!r}).write_text(json.dumps(sys.argv))\n"
    )
    script.chmod(0o700)
    service.workspace_guard = pinned_guard(script)
    sid = await service.ensure_session("pinned-thread")
    assert proof.exists()
    assert service.broker.is_session_loaded(sid)
    prior = await service.store.get("pinned-thread")
    epoch = service.native_epoch("pinned-thread")
    proof.unlink()
    bypass = service.default_cwd / "bypass-ran"
    script.write_text(
        "#!/usr/bin/python3\nfrom pathlib import Path\n"
        f"Path({str(bypass)!r}).write_text('ran unchecked guard')\n"
    )
    with patch.object(service.broker, "start", new_callable=AsyncMock) as start:
        with pytest.raises(NativeDispatchChanged, match="Workspace verification refused"):
            await service.ensure_session("pinned-thread")
        start.assert_not_awaited()
    assert not bypass.exists()
    assert not proof.exists()
    assert await service.store.get("pinned-thread") == prior
    assert service.native_epoch("pinned-thread") == epoch
    assert service.broker.is_session_loaded(sid)


async def test_pinned_guard_executes_same_buffer_after_path_replacement(tmp_path, monkeypatch):
    script = tmp_path / "guard with spaces"
    good, bad = tmp_path / "checked.json", tmp_path / "unchecked"
    accepted = (
        "import json,os,sys\nfrom pathlib import Path\n"
        f"Path({str(good)!r}).write_text(json.dumps([sys.argv,__file__,os.getcwd()]))\n"
    ).encode()
    replacement = f"from pathlib import Path\nPath({str(bad)!r}).touch()\n".encode()
    script.write_bytes(accepted)
    intervention = (
        "import hashlib\nfrom pathlib import Path\noriginal=hashlib.sha256\n"
        "def replace_after_hash(data):\n    result=original(data)\n"
        f"    if data=={accepted!r}: Path({str(script)!r}).write_bytes({replacement!r})\n"
        "    return result\nhashlib.sha256=replace_after_hash\n"
    )
    monkeypatch.setattr(
        entry,
        "_WORKSPACE_GUARD_PINNED_SOURCE",
        intervention + entry._WORKSPACE_GUARD_PINNED_SOURCE,
    )
    await pinned_guard(script)("literal-$VALUE", tmp_path, "previous-session")
    argv, filename, cwd = json.loads(good.read_text())
    assert argv == [str(script), "literal-$VALUE", str(tmp_path), "previous-session"]
    assert filename == str(script)
    assert cwd == os.getcwd()
    assert not bad.exists()
    assert script.read_bytes() == replacement


@pytest.mark.parametrize("input_kind", ["fifo", "oversized", "symlink"])
async def test_pinned_guard_refuses_nonregular_or_oversized_source_promptly(tmp_path, input_kind):
    script = tmp_path / "guard"
    script.write_text("raise SystemExit(0)\n")
    guard = pinned_guard(script, timeout_s=10)
    script.unlink()
    if input_kind == "fifo":
        os.mkfifo(script)
    elif input_kind == "symlink":
        target = tmp_path / "target"
        target.write_text("raise SystemExit(0)\n")
        script.symlink_to(target)
    else:
        with script.open("wb") as stream:
            stream.truncate(8 * 1024 * 1024 + 1)
    async with asyncio.timeout(2):
        with pytest.raises(NativeDispatchChanged, match="Workspace verification refused"):
            await guard("thread", tmp_path, None)
