"""Restart the Mattermost gateway process cleanly.

Strategy (under systemd with Restart=always):
  1. Caller writes a pending-restart marker (thread + session to bump).
  2. We cancel work, stop the ACP broker, then ``os._exit(0)``.
  3. systemd starts a new process; ``on_ready`` consumes the marker and bumps.

We intentionally do **not** call ``systemctl restart`` from inside the unit —
that races with our own SIGTERM and has hung/killed mid-restart before.
A clean exit is enough: systemd reloads EnvironmentFile on each start.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path

from mattermost_acp.commands.pending_restart import (
    PendingRestart,
    pending_path,
    write_pending,
)

logger = logging.getLogger(__name__)

DEFAULT_UNIT = "mattermost-acp-gateway.service"
StopCoro = Callable[[], Awaitable[None]]
CancelCoro = Callable[[], Awaitable[None]]


def under_systemd() -> bool:
    return bool(os.environ.get("INVOCATION_ID"))


async def restart_gateway(
    *,
    pending: PendingRestart | None = None,
    pending_file: Path | None = None,
    db_path: Path | None = None,
    delay_s: float = 0.8,
    stop_coro: StopCoro | None = None,
    cancel_coro: CancelCoro | None = None,
    unit: str = DEFAULT_UNIT,
) -> str:
    """Schedule a clean gateway exit and return a user-facing status string.

    ``pending`` (if given) is flushed to disk **before** the delayed kick so a
    crash mid-shutdown still leaves a marker for the next process.
    """
    if pending is not None:
        path = pending_file
        if path is None:
            if db_path is None:
                raise ValueError("pending requires pending_file or db_path")
            path = pending_path(db_path)
        write_pending(path, pending)

    if under_systemd():
        msg = (
            f"♻️ Restarting `{unit}`… "
            "I'll post back in this thread when I'm up."
            if pending and pending.thread_id
            else f"♻️ Restarting `{unit}`…"
        )
    else:
        msg = "♻️ Exiting process (dev / no systemd Restart=always)…"

    async def _kick() -> None:
        await asyncio.sleep(max(0.2, delay_s))
        if cancel_coro is not None:
            try:
                await cancel_coro()
            except Exception:
                logger.exception("cancel_coro failed during restart")
        if stop_coro is not None:
            try:
                await stop_coro()
            except Exception:
                logger.exception("stop_coro failed during restart")
        # Brief pause so Mattermost HTTP for the ack can finish.
        await asyncio.sleep(0.3)
        logger.info("Exiting for clean restart (pid=%s)", os.getpid())
        os._exit(0)

    asyncio.create_task(_kick(), name="mattermost-acp-restart")
    return msg
