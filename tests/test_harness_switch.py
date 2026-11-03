import json
import os
from pathlib import Path

import pytest

from mattermost_acp import harness_switch as hs
from mattermost_acp.store import ThreadStore


def _setup(tmp_path: Path) -> tuple[Path, Path, Path]:
    home = tmp_path / "home"
    for rel in ("bin/grok", "bin/claude-acp"):
        exe = home / rel
        exe.parent.mkdir(parents=True, exist_ok=True)
        exe.write_text("#!/bin/sh\n")
        exe.chmod(0o755)
    profiles = tmp_path / "harnesses.json"
    profiles.write_text(
        json.dumps(
            {
                "harnesses": {
                    "grok": {"command": "~/bin/grok", "model": "hub-claude-opus-5-5"},
                    "claude": {
                        "command": "~/bin/claude-acp",
                        "model": "opus",
                        "path_prefix": ["~/node/bin"],
                        "env": {"CLAUDE_CONFIG_DIR": "~/.claude-mm"},
                    },
                }
            }
        )
    )
    root = tmp_path / "repo"
    (root / "agents").mkdir(parents=True)
    roster = root / "agents" / "roster.json"
    roster.write_text(
        json.dumps(
            {
                "agents": [
                    {
                        "username": "assistant-a",
                        "user_id": "u1",
                        "unit": "mattermost-acp-gateway.service",
                        "env_file": ".env",
                        "harness": "grok",
                    }
                ]
            }
        )
    )
    env = root / ".env"
    env.write_text(
        "MATTERMOST_URL=https://mm.example\n"
        "# comment HARNESS=keep\n"
        "HARNESS=grok\n"
        "AGENT_COMMAND=grok\n"
        "AGENT_MODEL=hub-claude-opus-5-5\n"
        "MATTERMOST_ACP_DB=./data/threads-mm.db\n"
    )
    env.chmod(0o600)
    return home, profiles, root


def test_switch_rewrites_env_and_roster(tmp_path: Path) -> None:
    home, profiles, root = _setup(tmp_path)
    plan = hs.apply_switch(
        "assistant-a", "claude", root=root, profiles_path=profiles, home=home
    )
    text = (root / ".env").read_text()
    assert plan.old_harness == "grok"
    assert "HARNESS=claude\n" in text
    assert f"AGENT_COMMAND={home}/bin/claude-acp\n" in text
    assert "AGENT_MODEL=opus\n" in text
    assert f"CLAUDE_CONFIG_DIR={home}/.claude-mm\n" in text
    assert f"PATH={home}/node/bin:{home}/.local/bin:" in text
    assert "# comment HARNESS=keep" in text
    assert "MATTERMOST_URL=https://mm.example" in text
    assert text.count("HARNESS=") == 2
    assert oct(os.stat(root / ".env").st_mode & 0o777) == "0o600"
    assert plan.backup_path is not None and "HARNESS=grok" in plan.backup_path.read_text()
    assert plan.db_path == (root / "data" / "threads-mm.db").resolve()
    roster = json.loads((root / "agents" / "roster.json").read_text())
    assert roster["agents"][0]["harness"] == "claude"

    hs.apply_switch(
        "assistant-a", "grok", model="grok-4.6", root=root, profiles_path=profiles, home=home
    )
    text = (root / ".env").read_text()
    assert "CLAUDE_CONFIG_DIR" not in text
    assert "AGENT_MODEL=grok-4.6\n" in text
    assert f"PATH={home}/.local/bin:" in text


def test_switch_drops_legacy_grok_keys(tmp_path: Path) -> None:
    home, profiles, root = _setup(tmp_path)
    env = root / ".env"
    env.write_text(env.read_text().replace("AGENT_", "GROK_"))
    hs.apply_switch("assistant-a", "claude", root=root, profiles_path=profiles, home=home)
    text = env.read_text()
    assert "GROK_" not in text
    assert "AGENT_MODEL=opus\n" in text


def test_switch_rejects_unknown_harness_and_agent(tmp_path: Path) -> None:
    home, profiles, root = _setup(tmp_path)
    with pytest.raises(hs.SwitchError, match="unknown harness"):
        hs.apply_switch("assistant-a", "cursor", root=root, profiles_path=profiles, home=home)
    with pytest.raises(hs.SwitchError, match="not in the roster"):
        hs.apply_switch("nobody", "claude", root=root, profiles_path=profiles, home=home)


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    home, profiles, root = _setup(tmp_path)
    before = (root / ".env").read_text()
    hs.apply_switch(
        "assistant-a", "claude", root=root, profiles_path=profiles, home=home, dry_run=True
    )
    assert (root / ".env").read_text() == before


def test_busy_marker_ignores_dead_pid(tmp_path: Path) -> None:
    marker = hs.busy_path(tmp_path / "t.db")
    marker.write_text(json.dumps({"pid": os.getpid(), "threads": ["a"]}))
    assert hs.busy_threads(marker) == ["a"]
    marker.write_text(json.dumps({"pid": 2**22 + 12345, "threads": ["a"]}))
    assert hs.busy_threads(marker) == []
    assert hs.wait_idle(tmp_path / "missing.db", timeout_s=0) == []


def test_repo_profiles_load(tmp_path, monkeypatch) -> None:
    _, profile_path, _ = _setup(tmp_path)
    monkeypatch.setenv("MATTERMOST_HARNESS_PROFILES_FILE", str(profile_path))
    profiles = hs.load_profiles()
    assert {"grok", "claude"} <= set(profiles)


@pytest.mark.asyncio
async def test_store_tags_rows_and_filters_other_harness(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    grok = ThreadStore(db, harness="grok")
    await grok.open()
    await grok.save("root1", "grok-sess", "/tmp", model="hub-claude-opus-5-5")
    await grok.set_thread_override("root1", "chan", "grok-4.6", "high")
    await grok.set_model_setting("harness", "grok-4.6", "high")
    await grok.close()

    claude = ThreadStore(db, harness="claude")
    await claude.open()
    rec = await claude.get("root1")
    assert rec is not None and rec.harness == "grok"
    assert await claude.model_preference("root1", "chan") == (None, None, "configuration")
    await claude.reset_for_harness("root1")
    rec = await claude.get("root1")
    assert rec is not None
    assert (rec.harness, rec.model, rec.model_override) == ("claude", None, None)
    await claude.close()


@pytest.mark.asyncio
async def test_store_backfills_legacy_rows(tmp_path: Path) -> None:
    import sqlite3

    db = tmp_path / "legacy.db"
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE threads (thread_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, "
        "cwd TEXT NOT NULL, model TEXT, title TEXT, created_at TEXT NOT NULL DEFAULT "
        "(datetime('now')), updated_at TEXT NOT NULL DEFAULT (datetime('now')))"
    )
    con.execute("INSERT INTO threads (thread_id, session_id, cwd) VALUES ('r', 's', '/tmp')")
    con.commit()
    con.close()
    store = ThreadStore(db, harness="codex")
    await store.open()
    rec = await store.get("r")
    assert rec is not None and rec.harness == "codex"
    await store.close()


def test_switch_tags_unmigrated_rows_with_old_harness(tmp_path: Path) -> None:
    import sqlite3

    home, profiles, root = _setup(tmp_path)
    (root / "data").mkdir()
    db = root / "data" / "threads-mm.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE threads (thread_id TEXT PRIMARY KEY, session_id TEXT)")
    con.execute("CREATE TABLE model_settings (scope TEXT PRIMARY KEY, model TEXT)")
    con.execute("INSERT INTO threads VALUES ('r', 's')")
    con.execute("INSERT INTO model_settings VALUES ('harness', 'grok-4.6')")
    con.commit()
    con.close()
    hs.apply_switch("assistant-a", "claude", root=root, profiles_path=profiles, home=home)
    con = sqlite3.connect(db)
    assert con.execute("SELECT harness FROM threads").fetchall() == [("grok",)]
    assert con.execute("SELECT harness FROM model_settings").fetchall() == [("grok",)]
    con.close()


def test_external_versioned_switch_resolves_env_beside_inventory(tmp_path, monkeypatch):
    home, profiles, _ = _setup(tmp_path)
    external = tmp_path / "private deployment"
    external.mkdir()
    env = external / "assistant.env"
    env.write_text("HARNESS=grok\nMATTERMOST_ACP_DB=/tmp/synthetic-session.db\n")
    roster = external / "roster.json"
    roster.write_text(json.dumps({"schema_version": 1, "agents": [{
        "username": "assistant", "env_file": "assistant.env", "unit": "custom.service",
    }]}))
    monkeypatch.setenv("MATTERMOST_ROSTER_FILE", str(roster))
    monkeypatch.setenv("MATTERMOST_HARNESS_PROFILES_FILE", str(profiles))
    plan = hs.apply_switch("assistant", "claude", home=home, dry_run=True)
    assert plan.env_path == env
    assert plan.unit == "custom.service"
    assert env.read_text().startswith("HARNESS=grok")
