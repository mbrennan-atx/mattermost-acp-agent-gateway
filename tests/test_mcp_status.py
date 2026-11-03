from mattermost_acp.commands.mcp_status import format_doctor_json


def test_format_doctor_json_summary() -> None:
    data = {
        "servers": [
            {
                "name": "glean",
                "transport": "http",
                "target": "https://search.example.com/mcp/default",
                "source": "config",
                "healthy": True,
                "checks": [
                    {"label": "handshake OK", "passed": True},
                    {"label": "23 tools discovered", "passed": True},
                ],
            },
            {
                "name": "metastore",
                "transport": "http",
                "target": "http://localhost:8001/mcp/metastore",
                "source": "config",
                "healthy": False,
                "checks": [
                    {
                        "label": "handshake OK",
                        "passed": False,
                        "detail": "Access denied",
                    }
                ],
            },
        ]
    }
    text = format_doctor_json(data)
    assert "1/2 healthy" in text
    assert "`glean`" in text
    assert "`metastore`" in text
    assert "23 tools discovered" in text
    assert "normal chat" in text.lower()


def test_format_doctor_filter_missing() -> None:
    data = {"servers": [{"name": "slack", "healthy": True, "checks": []}]}
    text = format_doctor_json(data, server_filter="nope")
    assert "No MCP server named `nope`" in text
    assert "`slack`" in text


def test_format_doctor_filter_hit() -> None:
    data = {
        "servers": [
            {"name": "slack", "healthy": True, "transport": "http", "checks": []},
            {"name": "glean", "healthy": True, "transport": "http", "checks": []},
        ]
    }
    text = format_doctor_json(data, server_filter="slack")
    assert "`slack`" in text
    assert "`glean`" not in text


def test_mcp_status_report_uses_harness_cli(monkeypatch):
    import asyncio

    from mattermost_acp.commands import mcp_status

    seen: list[list[str]] = []

    async def fake_cli(argv, label, **_kw):
        seen.append(argv)
        return mcp_status.McpProbeResult(ok=True, text=label)

    async def fake_doctor(grok_command, **_kw):
        seen.append([grok_command, "doctor"])
        return mcp_status.McpProbeResult(ok=True, text="grok")

    monkeypatch.setattr(mcp_status, "mcp_cli_report", fake_cli)
    monkeypatch.setattr(mcp_status, "mcp_doctor_report", fake_doctor)
    monkeypatch.setattr(mcp_status.shutil, "which", lambda name: f"/bin/{name}")
    monkeypatch.delenv("CODEX_PATH", raising=False)

    asyncio.run(mcp_status.mcp_status_report("claude", "claude-agent-acp"))
    asyncio.run(mcp_status.mcp_status_report("codex", "codex-acp", server="glean"))
    asyncio.run(mcp_status.mcp_status_report("grok", "grok"))
    assert seen == [
        ["/bin/claude", "mcp", "list"],
        ["/bin/codex", "mcp", "get", "glean"],
        ["grok", "doctor"],
    ]
