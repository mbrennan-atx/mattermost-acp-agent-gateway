"""Roster-driven fleet homes, units, and nudge targets."""

from __future__ import annotations

import importlib.machinery
import importlib.util
from pathlib import Path

_PATH = Path(__file__).resolve().parents[1] / "scripts" / "fleet_roster.py"
_loader = importlib.machinery.SourceFileLoader("fleet_roster", str(_PATH))
_spec = importlib.util.spec_from_loader("fleet_roster", _loader)
assert _spec is not None and _spec.loader is not None
fleet_roster = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fleet_roster)


def test_external_roster_covers_homes_units_and_nudges(tmp_path, monkeypatch) -> None:
    import json

    path = tmp_path / "roster.json"
    entries = [
        {"username": "assistant", "unit": "custom.service", "grok_home": "/tmp/custom",
         "protected": True},
        {"username": "operator", "nudge_enabled": False},
    ]
    path.write_text(json.dumps({"schema_version": 1, "agents": entries}))
    monkeypatch.setenv("MATTERMOST_ROSTER_FILE", str(path))
    roster = fleet_roster.load_roster()
    assert fleet_roster.fleet_units(roster) == [
        "custom.service", "mattermost-acp-gateway-operator.service",
    ]
    assert fleet_roster.nudge_usernames(roster) == ["assistant"]
    assert fleet_roster.protected_names(roster) == frozenset({"assistant"})
    assert Path("/tmp/custom") in fleet_roster.fleet_homes(roster)


def test_new_username_needs_no_special_case() -> None:
    assert fleet_roster.default_unit("sample") == "mattermost-acp-gateway-sample.service"
    assert fleet_roster.default_env_file("sample") == ".env.sample"
    roster = {
        "agents": [
            {
                "username": "sample",
                "grok_home": "/tmp/fleet-sample",
            }
        ]
    }
    assert fleet_roster.fleet_units(roster) == ["mattermost-acp-gateway-sample.service"]
    assert fleet_roster.nudge_usernames(roster) == ["sample"]


def test_config_source_skips_destination_and_uses_priority(tmp_path: Path) -> None:
    ea = tmp_path / "profile-low"
    other = tmp_path / "profile-primary"
    dest = tmp_path / "profile-destination"
    for home in (ea, other, dest):
        home.mkdir()
        (home / "config.toml").write_text("disabled_mcp_servers = []\n", encoding="utf-8")
    roster = {
        "agents": [
            {"username": "secondary", "grok_home": str(ea), "config_source_priority": 1},
            {"username": "primary", "grok_home": str(other)},
            {"username": "sample", "grok_home": str(dest)},
        ]
    }
    chosen = fleet_roster.config_source(roster, dest)
    assert chosen == other / "config.toml"
