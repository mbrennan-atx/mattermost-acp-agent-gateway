"""Persist which chat thread requested a gateway restart.

Survives process death so the next process can post a success bump and
re-warm the native ACP session that was in that thread.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_MAX_AGE_S = 15 * 60  # ignore stale markers (failed / abandoned restarts)
FILENAME = "pending_restart.json"


@dataclass
class PendingRestart:
    thread_id: str | None
    session_id: str | None
    user_id: str | None
    requested_at: str  # ISO-8601 UTC
    reason: str
    pid: int
    team_id: str | None = None
    parent_channel_id: str | None = None

    @property
    def age_s(self) -> float:
        try:
            ts = datetime.fromisoformat(self.requested_at)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            return max(0.0, (datetime.now(UTC) - ts).total_seconds())
        except ValueError:
            return float("inf")


def pending_path(db_path: Path) -> Path:
    """Colocate with the SQLite thread map."""
    return Path(db_path).expanduser().resolve().parent / FILENAME


def write_pending(path: Path, pending: PendingRestart) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(pending)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)
    logger.info(
        "Wrote pending restart thread=%s session=%s path=%s",
        pending.thread_id,
        pending.session_id,
        path,
    )


def load_pending(path: Path) -> PendingRestart | None:
    path = Path(path)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("Corrupt pending restart file %s", path, exc_info=True)
        return None
    try:
        return PendingRestart(
            thread_id=_opt_str(data.get("thread_id")),
            session_id=_opt_str(data.get("session_id")),
            user_id=_opt_str(data.get("user_id")),
            requested_at=str(data.get("requested_at") or ""),
            reason=str(data.get("reason") or "restart"),
            pid=int(data.get("pid") or 0),
            team_id=_opt_str(data.get("team_id") or data.get("guild_id")),
            parent_channel_id=_opt_str(data.get("parent_channel_id")),
        )
    except (TypeError, ValueError):
        logger.warning("Invalid pending restart payload in %s", path, exc_info=True)
        return None


def consume_pending(
    path: Path, *, max_age_s: float = DEFAULT_MAX_AGE_S
) -> PendingRestart | None:
    """Load and delete the marker. Drop stale markers silently."""
    path = Path(path)
    pending = load_pending(path)
    clear_pending(path)
    if pending is None:
        return None
    if pending.age_s > max_age_s:
        logger.warning(
            "Ignoring stale pending restart (age=%.0fs > %.0fs) thread=%s",
            pending.age_s,
            max_age_s,
            pending.thread_id,
        )
        return None
    return pending


def clear_pending(path: Path) -> None:
    path = Path(path)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        logger.warning("Failed to clear pending restart %s", path, exc_info=True)


def now_pending(
    *,
    thread_id: str | None,
    session_id: str | None,
    user_id: str | None,
    reason: str = "slash_restart",
    team_id: str | None = None,
    parent_channel_id: str | None = None,
) -> PendingRestart:
    return PendingRestart(
        thread_id=thread_id,
        session_id=session_id,
        user_id=user_id,
        requested_at=datetime.now(UTC).isoformat(),
        reason=reason,
        pid=os.getpid(),
        team_id=team_id,
        parent_channel_id=parent_channel_id,
    )


def _opt_int(v) -> int | None:
    if v is None or v == "":
        return None
    return int(v)


def _opt_str(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


__all__ = [
    "DEFAULT_MAX_AGE_S",
    "FILENAME",
    "PendingRestart",
    "clear_pending",
    "consume_pending",
    "load_pending",
    "now_pending",
    "pending_path",
    "write_pending",
]
