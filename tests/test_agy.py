"""Antigravity (agy) harness configuration, ACP broker, session and MCP tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fake_agent import spawn_fake_agent_process

from mattermost_acp.acp.broker import AcpBroker
from mattermost_acp.commands.defs import help_text
from mattermost_acp.commands.mcp_status import mcp_status_report
from mattermost_acp.config import Config, load_mcp_servers_file
from mattermost_acp.session_service import SessionService
from mattermost_acp.store import ThreadStore


def _base_env(monkeypatch, tmp_path: Path) -> None:
    for key in (
        "MATTERMOST_URL",
        "MATTERMOST_TOKEN",
        "MATTERMOST_TOKEN_FILE",
        "MATTERMOST_CHANNEL_ID",
        "MATTERMOST_CHANNEL_IDS",
        "HARNESS",
        "AGENT_COMMAND",
        "AGENT_MODEL",
        "AGENT_EFFORT",
        "AGENT_ALWAYS_APPROVE",
        "AGY_CLI_HOME",
        "AGY_APP_DATA_DIR",
        "AGY_DISABLE_TELEMETRY",
        "AGY_AUTH_METHOD",
        "AGY_MCP_SERVERS_FILE",
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "GOOGLE_GENAI_USE_VERTEXAI",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("MATTERMOST_URL", "https://chat.example.com")
    monkeypatch.setenv("MATTERMOST_TOKEN", "token-test")
    monkeypatch.setenv("HARNESS", "agy")
    monkeypatch.setenv("AGENT_COMMAND", "agy")
    monkeypatch.setenv("AGENT_CWD", str(tmp_path))


def test_config_loads_agy_settings(monkeypatch, tmp_path: Path) -> None:
    _base_env(monkeypatch, tmp_path)
    profile = tmp_path / "profile"
    profile.mkdir()
    servers_file = profile / "mcp.json"
    servers_file.write_text(json.dumps([{"name": "demo", "url": "http://127.0.0.1:9000/mcp"}]))
    monkeypatch.setenv("AGENT_MODEL", "gemini-4-neon")
    monkeypatch.setenv("AGENT_EFFORT", "high")
    monkeypatch.setenv("AGY_CLI_HOME", str(profile))
    monkeypatch.setenv("AGY_DISABLE_TELEMETRY", "1")
    monkeypatch.setenv("AGY_AUTH_METHOD", "gemini-api-key")
    monkeypatch.setenv("AGY_MCP_SERVERS_FILE", str(servers_file))

    cfg = Config.load("/dev/null")
    assert cfg.harness == "agy"
    assert cfg.grok_model == "gemini-4-neon"
    assert cfg.grok_effort == "high"
    assert cfg.agy_cli_home == profile
    assert cfg.agy_app_data_dir == "antigravity-cli"
    assert cfg.agy_disable_telemetry is True
    assert cfg.agy_auth_method == "gemini-api-key"
    assert cfg.agy_mcp_servers_file == servers_file
    assert load_mcp_servers_file(cfg.agy_mcp_servers_file) == [
        {"name": "demo", "url": "http://127.0.0.1:9000/mcp"}
    ]


def test_config_rejects_invalid_agy_auth_and_effort_and_mcp_file(
    monkeypatch, tmp_path: Path
) -> None:
    _base_env(monkeypatch, tmp_path)
    monkeypatch.setenv("AGY_AUTH_METHOD", "invalid-auth")
    with pytest.raises(ValueError, match="AGY_AUTH_METHOD"):
        Config.load("/dev/null")

    _base_env(monkeypatch, tmp_path)
    monkeypatch.setenv("AGENT_EFFORT", "turbo")
    with pytest.raises(ValueError, match="AGENT_EFFORT"):
        Config.load("/dev/null")

    _base_env(monkeypatch, tmp_path)
    bad = tmp_path / "bad.json"
    bad.write_text('{"not": "a list"}')
    monkeypatch.setenv("AGY_MCP_SERVERS_FILE", str(bad))
    with pytest.raises(ValueError, match="AGY_MCP_SERVERS_FILE"):
        Config.load("/dev/null")


def test_agy_broker_argv_and_auth_resolution(monkeypatch, tmp_path: Path) -> None:
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_GENAI_USE_VERTEXAI"):
        monkeypatch.delenv(key, raising=False)

    broker = AcpBroker(
        command="/opt/bin/agy",
        backend="agy",
        always_approve=True,
        agy_cli_home=tmp_path / "profile",
        agy_app_data_dir="antigravity-cli",
        agy_disable_telemetry=True,
    )
    assert broker._build_argv() == [
        "/opt/bin/agy",
        "acp",
        "--gemini-dir",
        str(tmp_path / "profile"),
        "--app-data-dir",
        "antigravity-cli",
        "--dangerously-skip-permissions",
        "--disable-telemetry",
    ]
    assert broker._resolve_agy_auth_method() == "oauth-personal"

    minimal = AcpBroker(command="agy", backend="agy", always_approve=False)
    assert minimal._build_argv() == ["agy", "acp"]

    api_key_broker = AcpBroker(command="agy", backend="agy", env={"GEMINI_API_KEY": "test"})
    assert api_key_broker._resolve_agy_auth_method() == "gemini-api-key"

    vertex_broker = AcpBroker(
        command="agy", backend="agy", env={"GOOGLE_GENAI_USE_VERTEXAI": "true"}
    )
    assert vertex_broker._resolve_agy_auth_method() == "agent-platform"

    explicit = AcpBroker(command="agy", backend="agy", agy_auth_method="oauth-business")
    assert explicit._resolve_agy_auth_method() == "oauth-business"

    disabled = AcpBroker(command="agy", backend="agy", agy_auth_method="none")
    assert disabled._resolve_agy_auth_method() is None


@pytest.mark.asyncio
async def test_agy_broker_start_authenticate_and_set_model_thought_level() -> None:
    servers = [{"name": "local", "url": "http://127.0.0.1:8080/mcp"}]
    broker = AcpBroker(
        command="agy",
        backend="agy",
        agy_auth_method="gemini-api-key",
        mcp_servers=servers,
        process_factory=spawn_fake_agent_process,
    )
    seen: list[tuple[str, dict]] = []
    orig_request = broker.request

    async def spy_request(method: str, params: dict, **kwargs):
        seen.append((method, params))
        if method == "session/set_config_option":
            if params["configId"] == "model":
                return {
                    "configOptions": [
                        {"id": "model", "currentValue": params["value"]},
                        {"id": "thought_level", "currentValue": "low"},
                    ]
                }
            return {
                "configOptions": [
                    {"id": "model", "currentValue": "gemini-4-neon"},
                    {"id": "thought_level", "currentValue": params["value"]},
                ]
            }
        return await orig_request(method, params, **kwargs)

    broker.request = spy_request  # type: ignore[method-assign]
    await broker.start()
    try:
        sid = await broker.session_new("/tmp")
        await broker.session_resume(sid, "/tmp")
        await broker.session_load(sid, "/tmp")
        resolved = await broker.set_model(sid, "gemini-4-neon", effort="high")
        assert resolved == "gemini-4-neon"
        assert ("authenticate", {"methodId": "gemini-api-key"}) in seen
        assert any(
            m == "session/new" and p.get("mcpServers") == servers for m, p in seen
        )
        assert any(
            m == "session/resume" and p.get("mcpServers") == servers for m, p in seen
        )
        assert any(
            m == "session/load" and p.get("mcpServers") == servers for m, p in seen
        )
        assert (
            "session/set_config_option",
            {"sessionId": sid, "configId": "model", "value": "gemini-4-neon"},
        ) in seen
        assert (
            "session/set_config_option",
            {"sessionId": sid, "configId": "thought_level", "value": "high"},
        ) in seen
    finally:
        await broker.stop()


@pytest.mark.asyncio
async def test_agy_session_service_pins_default_model_and_effort_and_reads_status(
    tmp_path: Path,
) -> None:
    broker = AcpBroker(
        command="agy",
        backend="agy",
        agy_auth_method="none",
        process_factory=spawn_fake_agent_process,
    )
    set_calls: list[tuple[str, str, str | None]] = []

    async def fake_set_model(session_id: str, model_id: str, *, effort: str | None = None) -> str:
        set_calls.append((session_id, model_id, effort))
        broker.session_config_options[session_id] = [
            {"id": "model", "currentValue": model_id},
            {"id": "thought_level", "currentValue": effort or "low"},
        ]
        return model_id

    broker.set_model = AsyncMock(side_effect=fake_set_model)
    store = ThreadStore(tmp_path / "agy.db", harness="agy")
    await store.open()
    try:
        svc = SessionService(
            broker,
            store,
            default_cwd=tmp_path,
            default_model="gemini-4-neon",
            default_effort="high",
        )
        sid = await svc.ensure_session("thread-1")
        assert set_calls == [(sid, "gemini-4-neon", "high")]
        info = await svc.status("thread-1")
        assert info["model"] == "gemini-4-neon"
        assert info["effort"] == "high"

        # Simulate restart/re-pin on resume when model_settings table has no row
        svc._pinned.clear()
        set_calls.clear()
        assert await svc.ensure_session("thread-1") == sid
        assert set_calls == [(sid, "gemini-4-neon", "high")]
    finally:
        await broker.stop()
        await store.close()


@pytest.mark.asyncio
async def test_agy_mcp_status_and_help(monkeypatch, tmp_path: Path) -> None:
    servers = tmp_path / "admitted.json"
    servers.write_text(
        json.dumps(
            [
                {
                    "name": "metrics",
                    "url": "http://127.0.0.1:9000/mcp",
                    "headers": [{"name": "Authorization", "value": "Bearer super-secret"}],
                },
                {"name": "local-tool", "command": "/usr/local/bin/tool"},
            ]
        )
    )
    res = await mcp_status_report("agy", "agy", mcp_servers_file=servers)
    assert res.ok is True
    assert "metrics: http://127.0.0.1:9000/mcp" in res.text
    assert "local-tool: /usr/local/bin/tool" in res.text
    assert "super-secret" not in res.text

    filtered = await mcp_status_report("agy", "agy", server="metrics", mcp_servers_file=servers)
    assert filtered.ok is True
    assert "metrics:" in filtered.text
    assert "local-tool" not in filtered.text

    missing = await mcp_status_report("agy", "agy", server="nope", mcp_servers_file=servers)
    assert missing.ok is False

    seen: list[list[str]] = []

    async def fake_cli(argv, label, **_kw):
        seen.append(argv)
        return type("R", (), {"ok": True, "text": label})()

    monkeypatch.setattr("mattermost_acp.commands.mcp_status.mcp_cli_report", fake_cli)
    await mcp_status_report("agy", "/opt/bin/agy")
    assert seen == [["/opt/bin/agy", "mcp", "list"]]

    rendered = help_text(home_channel_ids=[], harness="agy")
    assert "agy" in rendered
    assert "Antigravity" in rendered
