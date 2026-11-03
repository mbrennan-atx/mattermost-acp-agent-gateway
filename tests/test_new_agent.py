"""Provisioning is explicit, previewable and does not copy native credentials."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "new_agent", Path(__file__).resolve().parents[1] / "scripts/new-agent.py"
)
new_agent = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(new_agent)


@pytest.fixture
def configured(tmp_path, monkeypatch):
    config = tmp_path / "private config"
    config.mkdir()
    (config / "harnesses.json").write_text(
        json.dumps({"schema_version": 1, "harnesses": {"codex": {"command": "codex-acp"}}})
    )
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: home)
    monkeypatch.setattr(new_agent, "SECRETS", tmp_path / "secrets")
    monkeypatch.setenv("MATTERMOST_CONFIG_DIR", str(config))
    monkeypatch.delenv("MATTERMOST_ROSTER_FILE", raising=False)
    monkeypatch.delenv("MATTERMOST_HARNESS_PROFILES_FILE", raising=False)
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.com")
    monkeypatch.setenv("MATTERMOST_TEAM_ID", "team")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    # No installed gateway or native harness is needed for this unit fixture.
    checkout = tmp_path / "checkout"
    executable = checkout / ".venv/bin/mattermost-acp-gateway"
    executable.parent.mkdir(parents=True)
    executable.touch()
    executable.chmod(0o755)
    monkeypatch.setattr(new_agent, "ROOT", checkout)
    argv = [
        "new-agent",
        "--username",
        "arbitrary",
        "--display",
        "Example Assistant",
        "--cwd",
        str(tmp_path),
        "--harness",
        "codex",
        "--existing",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    return config, home, argv


def test_preview_has_no_network_or_writes(configured, monkeypatch, capsys):
    config, home, argv = configured
    monkeypatch.setattr(new_agent, "_token", lambda: pytest.fail("credential read"))
    monkeypatch.setattr(new_agent, "_request", lambda *a, **k: pytest.fail("API called"))
    new_agent.main()
    assert "Preview only" in capsys.readouterr().out
    assert list(home.iterdir()) == []
    assert sorted(p.name for p in config.iterdir()) == ["harnesses.json"]


def test_conflict_rejected_before_api(configured, monkeypatch):
    config, home, argv = configured
    argv.append("--apply")
    (config / "arbitrary.env").write_text("retained")
    monkeypatch.setattr(new_agent, "_token", lambda: pytest.fail("credential read"))
    with pytest.raises(SystemExit):
        new_agent.main()
    assert (config / "arbitrary.env").read_text() == "retained"


@pytest.mark.parametrize("admin", [False, True])
def test_apply_grants_admin_only_by_explicit_flag(configured, monkeypatch, admin):
    from dotenv import dotenv_values

    config, home, argv = configured
    argv.append("--apply")
    if admin:
        argv.append("--system-admin")
    monkeypatch.setattr(new_agent, "_token", lambda: "synthetic")
    monkeypatch.setattr(new_agent, "_request", lambda *a, **k: (200, {"id": "owner"}))
    monkeypatch.setattr(new_agent, "_ensure_user", lambda *a: {"id": "assistant"})
    monkeypatch.setattr(new_agent, "_ensure_team", lambda *a: None)
    monkeypatch.setattr(new_agent, "_ensure_private_channel", lambda *a: {"id": "home"})
    monkeypatch.setattr(new_agent, "_add_member", lambda *a: None)
    monkeypatch.setattr(new_agent, "_ensure_token_file", lambda *a: home / "token")
    grants = []
    monkeypatch.setattr(new_agent, "_ensure_system_admin", lambda *a: grants.append(a))
    monkeypatch.setattr(new_agent.subprocess, "run", lambda *a, **k: pytest.fail("service started"))
    new_agent.main()
    assert bool(grants) == admin
    env = config / "arbitrary.env"
    values = dotenv_values(env, interpolate=False)
    assert values["AGENT_ALWAYS_APPROVE"] == "0"
    assert values["HARNESS"] == "codex"
    assert values["MATTERMOST_ROSTER_FILE"] == str(config / "roster.json")
    assert env.stat().st_mode & 0o777 == 0o600
    assert json.loads((config / "roster.json").read_text())["agents"][0]["user_id"] == "assistant"

    # The unit must be accepted by systemd itself, including space-containing paths.
    import shutil
    import subprocess

    if shutil.which("systemd-analyze"):
        unit = home / ".config/systemd/user/mattermost-acp-gateway-arbitrary.service"
        proc = subprocess.Popen(
            ["systemd-analyze", "verify", str(unit)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        _, errors = proc.communicate(timeout=15)
        assert proc.returncode == 0, errors
        assert "ignoring" not in errors.lower()
        assert "invalid" not in errors.lower()
