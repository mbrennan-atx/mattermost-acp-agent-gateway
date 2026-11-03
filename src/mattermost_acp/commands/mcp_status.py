"""Connector status through the selected harness's native CLI.

These commands report status; configuration and authentication remain owned by
the native client. No connector is installed or authenticated here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
import shutil
from dataclasses import dataclass

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S = 90.0


@dataclass(frozen=True)
class McpProbeResult:
    ok: bool
    text: str
    raw: dict | list | None = None


async def run_grok_mcp(
    grok_command: str,
    args: list[str],
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> tuple[int, str, str]:
    """Run ``grok mcp …``; return (rc, stdout, stderr)."""
    argv = [grok_command, "mcp", *args]
    logger.info("MCP probe: %s", " ".join(shlex.quote(a) for a in argv[:8]))
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, "", f"timeout after {timeout_s:.0f}s"
    out = (stdout_b or b"").decode("utf-8", errors="replace")
    err = (stderr_b or b"").decode("utf-8", errors="replace")
    return int(proc.returncode or 0), out, err


def format_doctor_json(data: dict, *, server_filter: str | None = None) -> str:
    """Compact Mattermost markdown from ``grok mcp doctor --json``."""
    servers = data.get("servers") if isinstance(data, dict) else None
    if not isinstance(servers, list):
        return f"Unexpected doctor payload:\n```\n{json.dumps(data)[:1500]}\n```"

    rows: list[dict] = []
    for s in servers:
        if not isinstance(s, dict):
            continue
        name = str(s.get("name") or "?")
        if server_filter and name.lower() != server_filter.lower():
            continue
        rows.append(s)

    if server_filter and not rows:
        known = sorted(str(s.get("name")) for s in servers if isinstance(s, dict) and s.get("name"))
        return (
            f"No MCP server named `{server_filter}`.\n"
            f"Known: {', '.join(f'`{n}`' for n in known) or '—'}"
        )

    healthy = sum(1 for s in rows if s.get("healthy") is True)
    lines = [
        f"**MCP doctor** — {healthy}/{len(rows)} healthy",
        "",
    ]
    for s in sorted(rows, key=lambda x: str(x.get("name") or "").lower()):
        name = str(s.get("name") or "?")
        ok = s.get("healthy") is True
        mark = "✅" if ok else "❌"
        transport = s.get("transport") or "?"
        target = s.get("target") or ""
        source = s.get("source") or ""
        # Tool count from checks
        tools = ""
        failed_detail = ""
        for c in s.get("checks") or []:
            if not isinstance(c, dict):
                continue
            label = str(c.get("label") or "")
            if "tools discovered" in label.lower():
                tools = label
            if c.get("passed") is False:
                failed_detail = f"{label}: {c.get('detail') or 'failed'}"
        detail_bits = [str(transport)]
        if tools:
            detail_bits.append(tools)
        if source:
            detail_bits.append(f"src={source}")
        line = f"{mark} **`{name}`** — {', '.join(detail_bits)}"
        if target and len(target) < 80:
            line += f"\n  `{target}`"
        elif target:
            line += f"\n  `{target[:77]}…`"
        if failed_detail and not ok:
            line += f"\n  ⚠ {failed_detail}"
        lines.append(line)

    lines.append("")
    lines.append(
        "_MCP tools in normal chat use the native Grok configuration and permissions._ "
        "Ask for Glean/Slack/Jira/etc. — no special Mattermost MCP client needed."
    )
    return "\n".join(lines)


async def mcp_doctor_report(
    grok_command: str,
    *,
    server: str | None = None,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> McpProbeResult:
    args = ["doctor", "--json"]
    # `grok mcp doctor <name> --json` when filtering one server
    if server:
        args = ["doctor", server, "--json"]
    rc, out, err = await run_grok_mcp(grok_command, args, timeout_s=timeout_s)
    if rc != 0 and not out.strip():
        return McpProbeResult(
            ok=False,
            text=f"**MCP doctor failed** (rc={rc})\n```\n{(err or out)[:1500]}\n```",
        )
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        # Fallback: plain text doctor
        body = out.strip() or err.strip() or f"rc={rc}"
        return McpProbeResult(ok=rc == 0, text=f"**MCP doctor**\n```\n{body[:1800]}\n```")

    if not isinstance(data, dict):
        return McpProbeResult(ok=False, text=f"Unexpected JSON type: {type(data)}")

    text = format_doctor_json(data, server_filter=server)
    return McpProbeResult(ok=True, text=text, raw=data)


async def mcp_cli_report(
    argv: list[str],
    label: str,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> McpProbeResult:
    """Plain-text MCP health from a non-Grok CLI (``claude mcp list``, ``codex mcp list``)."""
    logger.info("MCP probe: %s", " ".join(shlex.quote(a) for a in argv[:8]))
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        return McpProbeResult(ok=False, text=f"**MCP status failed**: {exc}")
    try:
        out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return McpProbeResult(ok=False, text=f"**MCP status** timed out after {timeout_s:.0f}s")
    out = (out_b or b"").decode("utf-8", errors="replace").strip()
    err = (err_b or b"").decode("utf-8", errors="replace").strip()
    rc = int(proc.returncode or 0)
    body = out or err or f"(empty; rc={rc})"
    return McpProbeResult(ok=rc == 0, text=f"**MCP status** ({label})\n```\n{body[:1800]}\n```")


async def mcp_status_report(
    harness: str,
    grok_command: str,
    *,
    server: str | None = None,
) -> McpProbeResult:
    """Route ``/mcp`` to the CLI that owns this agent's MCP config."""
    if harness == "cursor":
        argv = (
            [grok_command, "mcp", "list-tools", server] if server else [grok_command, "mcp", "list"]
        )
        return await mcp_cli_report(argv, "cursor-agent mcp")
    if harness == "claude":
        argv = [shutil.which("claude") or "claude", "mcp", "list"]
        if server:
            argv = [argv[0], "mcp", "get", server]
        return await mcp_cli_report(argv, "claude mcp")
    if harness == "codex":
        codex = os.environ.get("CODEX_PATH") or shutil.which("codex") or "codex"
        argv = [codex, "mcp", "list"] if not server else [codex, "mcp", "get", server]
        return await mcp_cli_report(argv, "codex mcp")
    return await mcp_doctor_report(grok_command, server=server)


async def mcp_list_report(
    grok_command: str,
    *,
    timeout_s: float = 30.0,
) -> McpProbeResult:
    rc, out, err = await run_grok_mcp(grok_command, ["list"], timeout_s=timeout_s)
    body = (out or err).strip()
    if not body:
        body = f"(empty; rc={rc})"
    return McpProbeResult(
        ok=rc == 0,
        text=f"**MCP list** (rc={rc})\n```\n{body[:1800]}\n```",
    )
