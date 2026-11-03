from mattermost_acp.mm.slash import (
    autocomplete_items,
    parse_slash_payload,
    slash_authorized,
)


def test_parse_slash_payload_basic() -> None:
    req = parse_slash_payload(
        {
            "token": "tok",
            "command": "/new",
            "text": "",
            "user_id": "u1",
            "user_name": "matt",
            "channel_id": "c1",
            "team_id": "t1",
            "root_id": "p0",
        }
    )
    assert req.command == "new"
    assert req.root_id == "p0"
    assert req.user_id == "u1"


def test_parse_slash_payload_grok_prefix_and_catchall() -> None:
    req = parse_slash_payload({"command": "/grok-stop", "token": "t", "user_id": "u"})
    assert req.command == "stop"
    req = parse_slash_payload(
        {"command": "/grok", "text": "status", "token": "t", "user_id": "u"}
    )
    assert req.command == "status"
    assert req.text == ""


def test_slash_authorized() -> None:
    assert slash_authorized("abc", frozenset({"abc"})) is True
    assert slash_authorized("abc", frozenset({"xyz"})) is False
    assert slash_authorized("abc", frozenset()) is False


_IDS = ["grok-4.5", "hub-claude-4.6-sonnet", "hub-kimi-k2.6", "hub-gpt-5.4"]


def test_autocomplete_items_empty_query_default_first() -> None:
    items = autocomplete_items(_IDS, "grok-4.5", "")
    assert [i["Item"] for i in items][0] == "grok-4.5"
    assert items[0]["Hint"] == "default model"
    assert items[1]["Hint"] == ""
    assert all(set(i) == {"Item", "Hint", "HelpText"} for i in items)


def test_autocomplete_items_prefix_before_substring() -> None:
    items = autocomplete_items(_IDS, "grok-4.5", "hub-k")
    assert [i["Item"] for i in items] == ["hub-kimi-k2.6"]


def test_autocomplete_items_caps_at_25() -> None:
    ids = [f"m{i}" for i in range(40)]
    assert len(autocomplete_items(ids, "m0", "")) == 25


def _roster(tmp_path, agents):
    import json

    (tmp_path / "agents").mkdir()
    path = tmp_path / "agents" / "roster.json"
    path.write_text(json.dumps({"agents": agents}), encoding="utf-8")
    return path


def test_route_port_maps_home_channel_to_agent_port(tmp_path) -> None:
    from mattermost_acp.mm.slash_router import load_routes, route_port

    (tmp_path / ".env.a").write_text("MATTERMOST_SLASH_PORT=8771\n", encoding="utf-8")
    (tmp_path / ".env").write_text("MATTERMOST_SLASH_PORT=8766\n", encoding="utf-8")
    (tmp_path / ".env.off").write_text("MATTERMOST_SLASH_PORT=0\n", encoding="utf-8")
    roster = _roster(
        tmp_path,
        [
            {"home_channel_id": "ca", "env_file": ".env.a"},
            {"home_channel_id": "cm", "env_file": ".env"},
            {"home_channel_id": "coff", "env_file": ".env.off"},
            {"home_channel_id": "cmissing", "env_file": ".env.nope"},
        ],
    )
    assert load_routes(roster) == {"ca": 8771, "cm": 8766}
    assert route_port("ca", 8766, roster) == 8771
    assert route_port("cm", 8766, roster) is None  # own channel
    assert route_port("unknown", 8766, roster) is None


def test_route_port_bad_roster_handles_locally(tmp_path) -> None:
    import pytest

    from mattermost_acp.deployment import DeploymentError
    from mattermost_acp.mm.slash_router import route_port

    with pytest.raises(DeploymentError):
        route_port("ca", 8766, tmp_path / "missing.json")


def test_parse_slash_payload_agent_prefix_and_catchall() -> None:
    assert parse_slash_payload({"command": "/agent-status"}).command == "status"
    assert parse_slash_payload({"command": "/agent-help"}).command == "help"
    assert parse_slash_payload({"command": "/clear"}).command == "clear"
    req = parse_slash_payload({"command": "/agent", "text": "model opus high"})
    assert req.command == "model"
    assert req.text == "opus high"


def test_external_versioned_routes_and_optional_single_agent(tmp_path, monkeypatch):
    import json

    from mattermost_acp.mm.slash_router import load_routes

    monkeypatch.setenv("MATTERMOST_CONFIG_DIR", str(tmp_path))
    monkeypatch.delenv("MATTERMOST_ROSTER_FILE", raising=False)
    assert load_routes() == {}
    (tmp_path / "assistant.env").write_text("MATTERMOST_SLASH_PORT=8123\n")
    (tmp_path / "roster.json").write_text(json.dumps({
        "schema_version": 1,
        "agents": [{"username": "assistant", "home_channel_id": "channel-a",
                    "env_file": "assistant.env"}],
    }))
    assert load_routes() == {"channel-a": 8123}
