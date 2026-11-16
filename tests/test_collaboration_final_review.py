"""Long polling preserves real budgets; CLI restarts need native admission proof."""

import asyncio
import importlib.machinery
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_collaboration import api_fixture, close_api, project

from mattermost_acp.collaboration.client import Settings, elapsed_clock


async def test_mailbox_wait_before_project_creation_does_not_spend_its_budget(tmp_path):
    store, server, clients = await api_fixture(tmp_path)
    request = asyncio.create_task(
        clients["beta"].request("GET", "/mailbox?instance=worker&wait=20")
    )
    try:
        await asyncio.sleep(1.2)
        created = project(store, max_seconds=1)
        sent = await clients["alpha"].request(
            "POST",
            "/projects/" + created["id"] + "/messages",
            {"recipient": "beta", "kind": "ask", "text": "Check now", "key": "late-project"},
        )
        receipt = await asyncio.wait_for(request, 2)
        assert receipt["id"] == sent["id"]
        assert receipt["project_mailbox_wait_seconds"] >= 1
        assert receipt["local_project_deadline"] > elapsed_clock()
        assert receipt["local_project_deadline"] <= elapsed_clock() + 1
        # A following immediate read must retain a valid conservative budget.
        reread = await clients["beta"].request("GET", "/messages/" + sent["id"])
        assert reread["local_project_deadline"] > elapsed_clock()
        assert reread["local_project_deadline"] <= receipt["local_project_deadline"]
    finally:
        request.cancel()
        await asyncio.gather(request, return_exceptions=True)
        await close_api(store, server, clients)


@pytest.mark.parametrize("delimiter", ["?", "#", "?#"])
def test_empty_url_delimiters_cannot_swallow_endpoint_paths(tmp_path, monkeypatch, delimiter):
    token = tmp_path / "actor-token"
    token.write_text("synthetic-credential" * 3)
    monkeypatch.setenv("MATTERMOST_COLLAB_TOKEN_FILE", str(token))
    monkeypatch.setenv("MATTERMOST_COLLAB_URL", "http://127.0.0.1:18781" + delimiter)
    with pytest.raises(ValueError, match="plain loopback HTTP origin"):
        Settings.load()


@pytest.mark.parametrize("option,expected", [(None, 3), ("--no-restart", 0)])
def test_cli_switch_does_not_restart_from_a_cleared_foreground_marker(
    tmp_path, monkeypatch, option, expected
):
    path = Path(__file__).resolve().parents[1] / "scripts/set-harness"
    loader = importlib.machinery.SourceFileLoader("synthetic_switch", str(path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    command = importlib.util.module_from_spec(spec)
    loader.exec_module(command)
    plan = SimpleNamespace(
        username="assistant-a",
        old_harness="codex",
        new_harness="claude",
        model="synthetic-model",
        env_path=tmp_path / "assistant.env",
        unit="synthetic.service",
        backup_path=tmp_path / "backup.env",
        db_path=tmp_path / "threads.db",
    )
    restart = Mock(side_effect=AssertionError("Native work must remain running"))
    monkeypatch.setattr(command.hs, "apply_switch", lambda *args, **kwargs: plan)
    monkeypatch.setattr(command.hs, "wait_idle", lambda *args, **kwargs: [])
    monkeypatch.setattr(command.hs, "restart_unit", restart)
    monkeypatch.setattr(
        sys, "argv", [str(path), "assistant-a", "claude", *([option] if option else [])]
    )
    assert command.main() == expected
    restart.assert_not_called()
