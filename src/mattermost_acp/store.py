"""SQLite map: Mattermost thread_id (root post id) → harness session_id."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import aiosqlite

_SCHEMA = """
CREATE TABLE IF NOT EXISTS threads (
    thread_id       TEXT    PRIMARY KEY,
    session_id      TEXT    NOT NULL,
    cwd             TEXT    NOT NULL,
    model           TEXT,
    title           TEXT,
    tag             TEXT,
    effort          TEXT,
    channel_id      TEXT,
    model_override  TEXT,
    effort_override TEXT,
    harness         TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS model_settings (
    scope       TEXT PRIMARY KEY,
    model       TEXT NOT NULL,
    effort      TEXT,
    harness     TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


@dataclass
class ThreadRecord:
    thread_id: str
    session_id: str
    cwd: str
    model: str | None
    title: str | None
    tag: str | None = None
    effort: str | None = None
    channel_id: str | None = None
    model_override: str | None = None
    effort_override: str | None = None
    harness: str | None = None


@dataclass(frozen=True)
class ModelSetting:
    model: str
    effort: str | None = None


class ThreadStore:
    """Rows are tagged with the harness that created them.

    Session ids and model ids are harness specific, so rows written under a
    different ``HARNESS`` are treated as stale by callers.
    """

    def __init__(self, db_path: Path, *, harness: str = "grok") -> None:
        self._path = db_path
        self.harness = harness
        self._db: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(_SCHEMA)
        await self._migrate()
        await self._db.commit()

    async def _migrate(self) -> None:
        assert self._db is not None
        cur = await self._db.execute("PRAGMA table_info(threads)")
        cols = {row[1] for row in await cur.fetchall()}
        if "tag" not in cols:
            await self._db.execute("ALTER TABLE threads ADD COLUMN tag TEXT")
        if "effort" not in cols:
            await self._db.execute("ALTER TABLE threads ADD COLUMN effort TEXT")
        if "channel_id" not in cols:
            await self._db.execute("ALTER TABLE threads ADD COLUMN channel_id TEXT")
        if "model_override" not in cols:
            await self._db.execute("ALTER TABLE threads ADD COLUMN model_override TEXT")
        if "effort_override" not in cols:
            await self._db.execute("ALTER TABLE threads ADD COLUMN effort_override TEXT")
        if "harness" not in cols:
            await self._db.execute("ALTER TABLE threads ADD COLUMN harness TEXT")
        cur = await self._db.execute("PRAGMA table_info(model_settings)")
        if "harness" not in {row[1] for row in await cur.fetchall()}:
            await self._db.execute("ALTER TABLE model_settings ADD COLUMN harness TEXT")
        # Untagged rows predate the column. Each database has only ever been
        # served by one harness, so the current one owns them.
        await self._db.execute(
            "UPDATE threads SET harness = ? WHERE harness IS NULL", (self.harness,)
        )
        await self._db.execute(
            "UPDATE model_settings SET harness = ? WHERE harness IS NULL",
            (self.harness,),
        )

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    def _conn(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("ThreadStore is not open")
        return self._db

    async def get(self, thread_id: str) -> ThreadRecord | None:
        cur = await self._conn().execute(
            "SELECT thread_id, session_id, cwd, model, title, tag, effort, "
            "channel_id, model_override, effort_override, harness "
            "FROM threads WHERE thread_id = ?",
            (thread_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        keys = row.keys()
        return ThreadRecord(
            thread_id=str(row["thread_id"]),
            session_id=row["session_id"],
            cwd=row["cwd"],
            model=row["model"],
            title=row["title"],
            tag=row["tag"],
            effort=row["effort"] if "effort" in keys else None,
            channel_id=(
                str(row["channel_id"])
                if "channel_id" in keys and row["channel_id"] is not None
                else None
            ),
            model_override=(
                row["model_override"] if "model_override" in keys else None
            ),
            effort_override=(
                row["effort_override"] if "effort_override" in keys else None
            ),
            harness=row["harness"] if "harness" in keys else None,
        )

    async def save(
        self,
        thread_id: str,
        session_id: str,
        cwd: str,
        *,
        model: str | None = None,
        title: str | None = None,
        tag: str | None = None,
        effort: str | None = None,
        channel_id: str | None = None,
        model_override: str | None = None,
    ) -> None:
        await self._conn().execute(
            """
            INSERT INTO threads (
                thread_id, session_id, cwd, model, title, tag, effort,
                channel_id, model_override, harness
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(thread_id) DO UPDATE SET
                session_id = excluded.session_id,
                harness = excluded.harness,
                cwd = excluded.cwd,
                model = COALESCE(excluded.model, threads.model),
                title = COALESCE(excluded.title, threads.title),
                tag = COALESCE(excluded.tag, threads.tag),
                effort = COALESCE(excluded.effort, threads.effort),
                channel_id = COALESCE(excluded.channel_id, threads.channel_id),
                model_override = COALESCE(
                    excluded.model_override, threads.model_override
                ),
                updated_at = datetime('now')
            """,
            (
                thread_id,
                session_id,
                cwd,
                model,
                title,
                tag,
                effort,
                channel_id,
                model_override,
                self.harness,
            ),
        )
        await self._conn().commit()

    async def reset_for_harness(self, thread_id: str) -> None:
        """Drop another harness's session and model state; keep title and channel."""
        await self._conn().execute(
            """
            UPDATE threads
            SET model = NULL, effort = NULL, model_override = NULL,
                effort_override = NULL, harness = ?, updated_at = datetime('now')
            WHERE thread_id = ?
            """,
            (self.harness, thread_id),
        )
        await self._conn().commit()

    async def set_thread_override(
        self,
        thread_id: str,
        channel_id: str | None,
        model: str,
        effort: str | None = None,
    ) -> None:
        await self._conn().execute(
            """
            UPDATE threads
            SET channel_id = ?, model_override = ?, effort_override = ?,
                updated_at = datetime('now')
            WHERE thread_id = ?
            """,
            (channel_id, model, effort, thread_id),
        )
        await self._conn().commit()

    async def set_model_setting(
        self,
        scope: str,
        model: str,
        effort: str | None = None,
    ) -> None:
        await self._conn().execute(
            """
            INSERT INTO model_settings (scope, model, effort, harness)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(scope) DO UPDATE SET
                model = excluded.model,
                effort = excluded.effort,
                harness = excluded.harness,
                updated_at = datetime('now')
            """,
            (scope, model, effort, self.harness),
        )
        await self._conn().commit()

    async def get_model_setting(self, scope: str) -> ModelSetting | None:
        cur = await self._conn().execute(
            "SELECT model, effort FROM model_settings WHERE scope = ? AND harness = ?",
            (scope, self.harness),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return ModelSetting(model=row["model"], effort=row["effort"])

    async def model_preference(
        self,
        thread_id: str,
        channel_id: str | None = None,
    ) -> tuple[str | None, str | None, str]:
        record = await self.get(thread_id)
        if record and record.model_override and record.harness == self.harness:
            return record.model_override, record.effort_override, "thread"
        resolved_channel = channel_id or (record.channel_id if record else None)
        if resolved_channel is not None:
            setting = await self.get_model_setting(f"channel:{resolved_channel}")
            if setting:
                return setting.model, setting.effort, "channel"
        setting = await self.get_model_setting("harness")
        if setting:
            return setting.model, setting.effort, "harness"
        return None, None, "configuration"

    async def set_title(self, thread_id: str, title: str) -> None:
        await self._conn().execute(
            """
            UPDATE threads
            SET title = ?, updated_at = datetime('now')
            WHERE thread_id = ?
            """,
            (title, thread_id),
        )
        await self._conn().commit()

    async def set_tag(self, thread_id: str, tag: str) -> None:
        await self._conn().execute(
            """
            UPDATE threads
            SET tag = ?, updated_at = datetime('now')
            WHERE thread_id = ?
            """,
            (tag, thread_id),
        )
        await self._conn().commit()

    async def clear_meta(self, thread_id: str) -> None:
        await self._conn().execute(
            """
            UPDATE threads
            SET title = '', tag = '', updated_at = datetime('now')
            WHERE thread_id = ?
            """,
            (thread_id,),
        )
        await self._conn().commit()

    async def delete(self, thread_id: str) -> bool:
        cur = await self._conn().execute(
            "DELETE FROM threads WHERE thread_id = ?",
            (thread_id,),
        )
        await self._conn().commit()
        return cur.rowcount > 0
