"""retire-agent plans a removal and does not follow symlinks."""

from __future__ import annotations

import importlib.machinery
import importlib.util
import json
import sys
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[1] / "scripts" / "retire-agent.py"
_loader = importlib.machinery.SourceFileLoader("retire_agent", str(_PATH))
_spec = importlib.util.spec_from_loader("retire_agent", _loader)
assert _spec is not None and _spec.loader is not None
retire_agent = importlib.util.module_from_spec(_spec)
sys.modules["retire_agent"] = retire_agent
_spec.loader.exec_module(retire_agent)


def _layout(tmp_path: Path, username: str = "sample") -> dict:
    root = tmp_path / "repo"
    (root / "data").mkdir(parents=True)
    (root / "logs").mkdir()
    (root / "agents" / "roles").mkdir(parents=True)
    role = root / "agents" / "roles" / f"{username}.md"
    role.write_text("role\n", encoding="utf-8")
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    token = secrets / f"mattermost_user_{username}_pat"
    token.write_text("secret\n", encoding="utf-8")
    db = root / "data" / f"threads-mm-{username}.db"
    log = root / "logs" / f"mattermost-acp-{username}.log"
    db.write_text("db", encoding="utf-8")
    log.write_text("log", encoding="utf-8")
    env = root / f".env.{username}"
    env.write_text(
        "\n".join(
            [
                f"MATTERMOST_ACP_DB={db}",
                f"MATTERMOST_ACP_LOG_FILE={log}",
                f"MATTERMOST_TOKEN_FILE={token}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    home_root = tmp_path / "home"
    home_root.mkdir()
    shared = home_root / ".grok"
    shared.mkdir()
    (shared / "skills").mkdir()
    (shared / "skills" / "keep.txt").write_text("keep", encoding="utf-8")
    (home_root / "shared-profile").mkdir()
    mm = home_root / f".grok-mm-{username}"
    mm.mkdir()
    (mm / "skills").symlink_to(shared / "skills")
    (mm / "config.toml").write_text("local\n", encoding="utf-8")
    agent = {
        "username": username,
        "user_id": "uid",
        "home_channel_id": "chan",
        "grok_home": str(mm),
        "role": f"agents/roles/{username}.md",
        "unit": f"mattermost-acp-gateway-{username}.service",
        "env_file": f".env.{username}",
        "owned_paths": [str(p) for p in (db, log, env, token, role, mm)],
    }
    return {
        "root": root,
        "secrets": secrets,
        "home_root": home_root,
        "mm": mm,
        "shared_skills": shared / "skills",
        "agent": agent,
        "roster": {"agents": [agent], "floor_channel_id": "floor"},
    }


def _plan(layout: dict, *, force: bool = False) -> list:
    return retire_agent.plan(
        layout["agent"],
        layout["roster"],
        force=force,
        root=layout["root"],
        secrets=layout["secrets"],
        unit_dir=layout["home_root"] / "units",
        home_root=layout["home_root"],
    )


def test_plan_covers_declared_files_and_refuses_permanent(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    steps = _plan(layout)
    text = "\n".join(step.detail + step.api for step in steps)
    assert "permanent=true" not in text
    assert all("permanent" not in step.api for step in steps)
    assert any(step.api == "/api/v4/users/uid" for step in steps)
    assert any(step.api == "/api/v4/channels/chan" for step in steps)
    kinds = {step.kind for step in steps}
    assert "rm-home" in kinds
    assert "roster-remove" in kinds
    removed = {Path(step.path) for step in steps if step.kind == "rm-file"}
    root = layout["root"]
    assert root / "data" / "threads-mm-sample.db" in removed
    assert root / "logs" / "mattermost-acp-sample.log" in removed
    assert root / ".env.sample" in removed
    assert layout["secrets"] / "mattermost_user_sample_pat" in removed
    assert root / "agents" / "roles" / "sample.md" in removed


def test_protected_user_requires_force(tmp_path: Path) -> None:
    layout = _layout(tmp_path, "coordinator")
    layout["agent"]["protected"] = True
    layout["agent"]["grok_home"] = str(layout["home_root"] / "shared-profile")
    layout["agent"]["env_file"] = ".env.ea"
    layout["agent"]["unit"] = "mattermost-acp-gateway-ea.service"
    env = layout["root"] / ".env.ea"
    env.write_text(
        (layout["root"] / ".env.coordinator").read_text(encoding="utf-8"), encoding="utf-8"
    )
    with pytest.raises(SystemExit, match="without --force"):
        _plan(layout)
    shared = layout["home_root"] / "shared-profile"
    layout["agent"]["owned_paths"].append(str(shared))
    layout["agent"]["owned_paths"].append(str(env))
    layout["roster"]["shared_paths"] = [str(shared)]
    steps = _plan(layout, force=True)
    details = [step.detail for step in steps]
    assert any("retain protected/shared path" in line for line in details)
    assert not any(
        step.kind == "rm-home" and step.path.endswith("shared-profile") for step in steps
    )
    assert any(step.path.endswith(".env.ea") for step in steps)


def test_remove_home_unlinks_symlinks(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    retire_agent.remove_tree_unlink(layout["mm"])
    assert not layout["mm"].exists()
    assert (layout["shared_skills"] / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_dry_run_does_not_write_roster(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    layout = _layout(tmp_path)
    roster_path = tmp_path / "roster.json"
    roster_path.write_text(json.dumps(layout["roster"], indent=2) + "\n", encoding="utf-8")
    before = roster_path.read_text(encoding="utf-8")
    steps = _plan(layout)
    print(f"dry-run: retire {layout['agent']['username']}")
    for step in steps:
        print(f"dry-run: {step.detail}")
    captured = capsys.readouterr()
    assert "dry-run: retire sample" in captured.out
    assert "permanent=true" not in captured.out
    assert roster_path.read_text(encoding="utf-8") == before
    assert layout["mm"].exists()


def test_retirement_retains_undeclared_files(tmp_path):
    layout = _layout(tmp_path)
    del layout["agent"]["owned_paths"]
    assert not any(s.kind in {"rm-file", "rm-home"} for s in _plan(layout))


def test_shared_descendants_and_other_assistant_state_are_retained(tmp_path):
    layout = _layout(tmp_path)
    parent = layout["root"] / "data"
    shared = parent / "shared"
    shared.mkdir()
    other = {"username": "other", "env_file": ".env.other", "owned_paths": [str(shared)]}
    layout["roster"]["agents"].append(other)
    layout["agent"]["owned_paths"] += [str(parent), str(shared / "child")]
    steps = _plan(layout)
    retained = [s.detail for s in steps if s.kind == "skip"]
    assert any(str(parent) in value for value in retained)
    assert any(str(shared / "child") in value for value in retained)


def test_failed_stop_preserves_roster_and_files(tmp_path):
    layout = _layout(tmp_path)
    roster = tmp_path / "roster.json"
    roster.write_text(json.dumps(layout["roster"]))
    before = roster.read_bytes()

    def failed_run(argv):
        raise OSError("stop failed")

    with pytest.raises(SystemExit, match="remaining steps were not applied"):
        retire_agent.apply_steps(
            _plan(layout),
            username="sample",
            roster_path=roster,
            base="https://chat.example.com",
            token="synthetic",
            run=failed_run,
        )
    assert roster.read_bytes() == before
    assert layout["mm"].is_dir()


def test_symlinked_parent_cannot_delete_target(tmp_path):
    layout = _layout(tmp_path)
    link = layout["root"] / "alias"
    link.symlink_to(layout["mm"], target_is_directory=True)
    layout["agent"]["owned_paths"] = [str(link / "config.toml")]
    assert not any(s.kind == "rm-file" for s in _plan(layout))
