"""Transactional project state. Requests survive transport and native-process failures."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

ACTIVE = {"running", "unconfirmed", "cancelling"}
TERMINAL = {"done", "failed", "blocked", "cancelled"}
WORK = {"ask", "work"}
LEGACY_PENDING_EXECUTION = (
    "COALESCE(json_extract(data,'$.execution_pending'),"
    "(json_extract(data,'$.evidence.session_id') IS NOT NULL "
    "AND json_extract(data,'$.evidence.dispatch_started') IS NOT NULL))=1"
)

PENDING_EXECUTION = "execution_pending=1"


def identifier() -> str:
    return uuid.uuid4().hex


def text(value, name="text", maximum=16000) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be a nonempty string of at most {maximum} characters")
    return value.strip()


_PROCESS_ELAPSED_EPOCH = uuid.uuid4().hex


def elapsed_clock():
    # Linux BOOTTIME includes suspended time and survives a hub-process restart.
    return (
        time.clock_gettime(time.CLOCK_BOOTTIME)
        if hasattr(time, "CLOCK_BOOTTIME")
        else time.monotonic()
    )


def elapsed_epoch():
    try:
        boot = str(uuid.UUID(Path("/proc/sys/kernel/random/boot_id").read_text().strip()))
        return "linux-boottime:" + boot
    except (OSError, ValueError):
        # On platforms without a verified boot clock, a new process cannot reuse
        # another process's elapsed receipt. Existing work requires cancellation.
        return "process:" + _PROCESS_ELAPSED_EPOCH


class CollaborationStore:
    def __init__(
        self,
        path: Path,
        actors: set[str],
        *,
        clock=time.time,
        elapsed=elapsed_clock,
        epoch=None,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=10)
        path.chmod(0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.actors = actors
        self.clock = clock
        self.elapsed = elapsed
        self.elapsed_epoch = elapsed_epoch() if epoch is None else epoch
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS projects(
                id TEXT PRIMARY KEY, data TEXT NOT NULL, status TEXT, deadline REAL,
                created REAL, owner TEXT, origin_thread TEXT);
            CREATE TABLE IF NOT EXISTS messages(
                id TEXT PRIMARY KEY, project TEXT NOT NULL, sender TEXT NOT NULL,
                recipient TEXT, kind TEXT NOT NULL, state TEXT NOT NULL,
                created REAL NOT NULL, key TEXT NOT NULL, data TEXT NOT NULL,
                UNIQUE(project, sender, key));
            CREATE TABLE IF NOT EXISTS delivery_history(
                message TEXT NOT NULL, sequence INTEGER NOT NULL, data TEXT NOT NULL,
                PRIMARY KEY(message,sequence));
            CREATE INDEX IF NOT EXISTS mailbox ON messages(recipient,state,created);
            CREATE INDEX IF NOT EXISTS mailbox_priority ON messages(
                recipient,state,CASE kind WHEN 'cancel' THEN 0 WHEN 'result' THEN 1
                ELSE 2 END,created,id);
            CREATE INDEX IF NOT EXISTS message_expiry ON messages(state);
            CREATE TABLE IF NOT EXISTS notes(id TEXT PRIMARY KEY, project TEXT, data TEXT);
            CREATE TABLE IF NOT EXISTS creations(actor TEXT, key TEXT, data TEXT,
                PRIMARY KEY(actor,key));
            CREATE TABLE IF NOT EXISTS origin_stops(actor TEXT, thread TEXT, cutoff REAL NOT NULL,
                PRIMARY KEY(actor,thread));
            CREATE TABLE IF NOT EXISTS project_bindings(
                project TEXT NOT NULL, actor TEXT NOT NULL, thread TEXT NOT NULL,
                channel TEXT NOT NULL, PRIMARY KEY(project,actor));
            CREATE INDEX IF NOT EXISTS native_binding ON project_bindings(actor,thread,project);
        """)
        self.migrate_admission_order()
        # Backfill queryable metadata once for hubs created with the JSON-only
        # project schema. Frequent expiry polls must not decode cancelled history.
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(projects)")}
        with self.transaction():
            for name, kind in (
                ("status", "TEXT"),
                ("deadline", "REAL"),
                ("created", "REAL"),
                ("owner", "TEXT"),
                ("origin_thread", "TEXT"),
                ("channel", "TEXT"),
                ("thread", "TEXT"),
                ("elapsed_epoch", "TEXT"),
                ("elapsed_deadline", "REAL"),
                ("creation_key", "TEXT"),
                ("bindings_indexed", "INTEGER"),
            ):
                if name not in columns:
                    self.db.execute(f"ALTER TABLE projects ADD COLUMN {name} {kind}")
            missing = self.db.execute(
                "SELECT data FROM projects WHERE status IS NULL OR deadline IS NULL "
                "OR created IS NULL OR owner IS NULL OR origin_thread IS NULL "
                "OR channel IS NULL OR thread IS NULL"
            ).fetchall()
            for row in missing:
                self.save_project(json.loads(row[0]))
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS project_expiry ON projects(status,deadline)"
            )
            self.db.execute(
                "UPDATE projects SET elapsed_epoch=json_extract(data,'$.elapsed_epoch'),"
                "elapsed_deadline=json_extract(data,'$.elapsed_deadline') "
                "WHERE elapsed_epoch IS NULL OR elapsed_deadline IS NULL"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS project_elapsed_expiry "
                "ON projects(status,elapsed_epoch,elapsed_deadline)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS origin_projects "
                "ON projects(owner,origin_thread,status,created)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS origin_project_admission "
                "ON projects(owner,origin_thread,status,admission_order)"
            )
            self.db.execute(
                "UPDATE projects SET creation_key=json_extract(data,'$.key') "
                "WHERE creation_key IS NULL AND json_extract(data,'$.key') IS NOT NULL"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS project_creation_key ON projects(owner,creation_key)"
            )
            self.db.execute(
                "INSERT OR REPLACE INTO project_bindings(project,actor,thread,channel) "
                "SELECT p.id,b.key,json_extract(b.value,'$.thread_id'),"
                "json_extract(b.value,'$.channel_id') FROM projects p,"
                "json_each(p.data,'$.bindings') b WHERE p.bindings_indexed IS NULL"
            )
            self.db.execute("UPDATE projects SET bindings_indexed=1 WHERE bindings_indexed IS NULL")
        # Persist routing and execution metadata once. Runtime admission and
        # recovery read only matching rows, regardless of retained JSON history.
        with self.transaction():
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS project_discussion ON projects(channel,thread)"
            )
            message_columns = {row[1] for row in self.db.execute("PRAGMA table_info(messages)")}
            if "execution_pending" not in message_columns:
                self.db.execute("ALTER TABLE messages ADD COLUMN execution_pending INTEGER")
            self.db.execute(
                "UPDATE messages SET execution_pending=(" + LEGACY_PENDING_EXECUTION + ") "
                "WHERE execution_pending IS NULL"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS execution_debt ON messages(recipient,execution_pending)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS project_execution_debt "
                "ON messages(project,execution_pending,state,recipient)"
            )
            creation_columns = {row[1] for row in self.db.execute("PRAGMA table_info(creations)")}
            for name, kind in (("channel", "TEXT"), ("thread", "TEXT"), ("unresolved", "INTEGER")):
                if name not in creation_columns:
                    self.db.execute(f"ALTER TABLE creations ADD COLUMN {name} {kind}")
            for row in self.db.execute(
                "SELECT actor,key,data FROM creations WHERE channel IS NULL OR unresolved IS NULL"
            ).fetchall():
                self.save_creation(row[0], row[1], json.loads(row[2]))
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS creation_discussion ON creations(channel,thread)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS unresolved_creation ON creations(channel,unresolved)"
            )
            # Move legacy inline history once. Routine message reads/writes
            # must stay independent of the retained execution evidence count.
            for row in self.db.execute(
                "SELECT data FROM messages WHERE json_type(data,'$.delivery_history') IS NOT NULL"
            ).fetchall():
                self.save_message(json.loads(row[0]))
        # A new hub process cannot establish what a crashed native turn did.
        with self.transaction():
            for row in self.db.execute("SELECT data FROM messages WHERE state='running'"):
                item = json.loads(row[0])
                item["state"] = "queued" if item["kind"] == "cancel" else "interrupted"
                item["recovery"] = "Hub restarted; reconcile native execution before retry"
                self.save_message(item)

    def close(self):
        self.db.close()

    @contextmanager
    def transaction(self):
        if self.db.in_transaction:
            yield
            return
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def next_admission_order(self):
        self.db.execute("UPDATE admission_sequence SET value=value+1 WHERE id=1")
        return self.db.execute("SELECT value FROM admission_sequence WHERE id=1").fetchone()[0]

    def migrate_admission_order(self):
        """Freeze historical ordering once; new admissions use transaction order."""
        with self.transaction():
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS admission_sequence("
                "id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL)"
            )
            self.db.execute("INSERT OR IGNORE INTO admission_sequence VALUES(1,0)")
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS stop_boundaries("
                "actor TEXT NOT NULL,key TEXT NOT NULL,thread TEXT NOT NULL,"
                "admission_order INTEGER NOT NULL,PRIMARY KEY(actor,key))"
            )
            for table, fields in (
                ("projects", (("admission_order", "INTEGER"),)),
                (
                    "creations",
                    (
                        ("admission_order", "INTEGER"),
                        ("origin_thread", "TEXT"),
                        ("created", "REAL"),
                    ),
                ),
                ("origin_stops", (("admission_order", "INTEGER"),)),
            ):
                columns = {row[1] for row in self.db.execute(f"PRAGMA table_info({table})")}
                for name, kind in fields:
                    if name not in columns:
                        self.db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {kind}")
            # Existing clocks cannot prove true historical order. Preserve the
            # legacy timestamp convention only for the one-time backfill.
            missing = []
            for table in ("projects", "creations"):
                for row in self.db.execute(
                    f"SELECT rowid,data FROM {table} WHERE admission_order IS NULL"
                ):
                    item = json.loads(row[1])
                    missing.append((item.get("created", 0), table, row[0], item))
            correlated = (
                {
                    (actor, key): order
                    for actor, key, order in self.db.execute(
                        "SELECT actor,key,admission_order FROM creations "
                        "WHERE admission_order IS NOT NULL"
                    )
                }
                if missing
                else {}
            )
            for _created, table, rowid, item in sorted(missing, key=lambda row: row[:3]):
                if table == "creations":
                    actor, key = self.db.execute(
                        "SELECT actor,key FROM creations WHERE rowid=?", (rowid,)
                    ).fetchone()
                    correlation = (actor, key)
                else:
                    correlation = (item["owner"], item.get("key"))
                order = correlated.get(correlation)
                if order is None:
                    order = self.next_admission_order()
                    if correlation[1]:
                        correlated[correlation] = order
                self.db.execute(
                    f"UPDATE {table} SET admission_order=? WHERE rowid=?", (order, rowid)
                )
            for rowid, raw in self.db.execute(
                "SELECT rowid,data FROM creations WHERE origin_thread IS NULL OR created IS NULL"
            ).fetchall():
                item = json.loads(raw)
                body = item["body"]
                self.db.execute(
                    "UPDATE creations SET origin_thread=?,created=? WHERE rowid=?",
                    (
                        body.get("origin_thread_id") or item.get("thread_id"),
                        item.get("created", 0),
                        rowid,
                    ),
                )
            # Old stops are frozen at upgrade. Their MAX clock must never apply
            # to an admission allocated after this migration.
            for actor, thread, cutoff in self.db.execute(
                "SELECT actor,thread,cutoff FROM origin_stops WHERE admission_order IS NULL"
            ).fetchall():
                order = self.legacy_stop_boundary(actor, thread, cutoff, migrating=True)[
                    "admission_order"
                ]
                self.db.execute(
                    "UPDATE origin_stops SET admission_order=? WHERE actor=? AND thread=?",
                    (order, actor, thread),
                )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS origin_creation_admission "
                "ON creations(actor,origin_thread,admission_order)"
            )

    def save_project(self, item, *, admission_order=None):
        with self.transaction():
            existing = self.db.execute(
                "SELECT admission_order FROM projects WHERE id=?", (item["id"],)
            ).fetchone()
            order = existing[0] if existing else admission_order
            if order is None:
                order = self.next_admission_order()
            self.db.execute(
                "INSERT OR REPLACE INTO projects"
                "(id,data,status,deadline,created,owner,origin_thread,channel,thread,"
                "admission_order,elapsed_epoch,elapsed_deadline,creation_key,bindings_indexed) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
                (
                    item["id"],
                    json.dumps(item),
                    item["status"],
                    item["deadline"],
                    item["created"],
                    item["owner"],
                    item["origin_thread_id"],
                    item["channel_id"],
                    item["thread_id"],
                    order,
                    item.get("elapsed_epoch"),
                    item.get("elapsed_deadline"),
                    item.get("key"),
                ),
            )
            self.db.execute("DELETE FROM project_bindings WHERE project=?", (item["id"],))
            self.db.executemany(
                "INSERT INTO project_bindings(project,actor,thread,channel) VALUES(?,?,?,?)",
                (
                    (item["id"], actor, binding["thread_id"], binding["channel_id"])
                    for actor, binding in item.get("bindings", {}).items()
                ),
            )

    def save_message(self, item):
        if "delivery_history" in item:
            self.db.executemany(
                "INSERT INTO delivery_history(message,sequence,data) VALUES(?,?,?) "
                "ON CONFLICT(message,sequence) DO UPDATE SET data=excluded.data "
                "WHERE delivery_history.data<>excluded.data",
                (
                    (item["id"], sequence, json.dumps(evidence))
                    for sequence, evidence in enumerate(item["delivery_history"])
                ),
            )
        debt = item.get("execution_pending")
        if debt is None:
            evidence = item.get("evidence") or {}
            debt = (
                evidence.get("session_id") is not None
                and evidence.get("dispatch_started") is not None
            )
        self.db.execute(
            """INSERT INTO messages
            (id,project,sender,recipient,kind,state,created,key,data,execution_pending)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
            project=excluded.project,sender=excluded.sender,recipient=excluded.recipient,
            kind=excluded.kind,state=excluded.state,created=excluded.created,
            key=excluded.key,data=excluded.data,execution_pending=excluded.execution_pending""",
            (
                item["id"],
                item["project"],
                item["sender"],
                item.get("recipient"),
                item["kind"],
                item["state"],
                item["created"],
                item["key"],
                json.dumps(
                    {key: value for key, value in item.items() if key != "delivery_history"}
                ),
                int(debt == 1),
            ),
        )

    def get_project(self, pid, actor):
        row = self.db.execute("SELECT data FROM projects WHERE id=?", (pid,)).fetchone()
        if row is None:
            raise KeyError("Unknown project")
        item = json.loads(row[0])
        if actor not in item["members"]:
            raise PermissionError("Actor is not a project participant")
        return item

    def get_message(self, mid, actor):
        item = self.message_record(mid, actor)
        item["delivery_history"] = [
            json.loads(row[0])
            for row in self.db.execute(
                "SELECT data FROM delivery_history WHERE message=? ORDER BY sequence", (mid,)
            )
        ]
        return item

    def message_record(self, mid, actor):
        row = self.db.execute("SELECT data FROM messages WHERE id=?", (mid,)).fetchone()
        if row is None:
            raise KeyError("Unknown message")
        item = json.loads(row[0])
        self.get_project(item["project"], actor)
        return item

    def archive_delivery(self, item):
        evidence = item.get("evidence")
        if evidence:
            self.db.execute(
                "INSERT INTO delivery_history(message,sequence,data) "
                "SELECT ?,COALESCE(MAX(sequence)+1,0),? FROM delivery_history WHERE message=?",
                (item["id"], json.dumps(evidence), item["id"]),
            )

    def remaining_seconds(self, project):
        deadline = project.get("elapsed_deadline")
        if (
            project.get("elapsed_epoch") != self.elapsed_epoch
            or type(deadline) not in {int, float}
            or not math.isfinite(deadline)
        ):
            return 0.0
        return max(0.0, min(project["deadline"] - self.clock(), deadline - self.elapsed()))

    def deadline_receipt(self, item, actor):
        project = self.get_project(item["project"], actor)
        receipt = {
            **item,
            "project_deadline": project["deadline"],
            "project_remaining_seconds": self.remaining_seconds(project),
        }
        if project["status"] != "active":
            receipt.update(self.project_budget(project, actor))
        return receipt

    def project_budget(self, project, actor):
        if isinstance(project, str):
            project = self.get_project(project, actor)
        return {
            "project": project["id"],
            "project_deadline": project["deadline"],
            "project_remaining_seconds": self.remaining_seconds(project),
            "project_admission_closed": project["status"] != "active",
            "project_execution_pending": bool(
                self.db.execute(
                    "SELECT 1 FROM messages WHERE project=? AND (execution_pending=1 "
                    "OR state IN ('running','unconfirmed','interrupted','cancelling')) LIMIT 1",
                    (project["id"],),
                ).fetchone()
            ),
        }

    def prepare_creation(self, actor, body):
        """Persist intent before external posting; uncertain posts need reconciliation."""
        key = text(body.get("key"), "project idempotency key", 200)
        canonical = {k: v for k, v in body.items() if k != "thread_id"}
        canonical["key"] = key
        canonical.setdefault("owner_in_discussion", False)
        with self.transaction():
            self.expire()
            row = self.db.execute(
                "SELECT data FROM creations WHERE actor=? AND key=?", (actor, key)
            ).fetchone()
            if row:
                intent = self.get_creation(actor, key)
                intent["body"]["key"] = key
                intent["body"].setdefault("owner_in_discussion", False)
                if intent["body"] != canonical:
                    raise ValueError("Conflicting project creation key")
                if intent["status"] == "rejected" and not intent.get("thread_id"):
                    intent["thread_id"] = body.get("thread_id")
                    intent["creation_id"] = identifier() if not body.get("thread_id") else None
                    intent["status"] = "reserved"
                    self.save_creation(actor, key, intent)
                    return intent, True
                return intent, False
            # Validate project input and binding rules without retaining a
            # placeholder project or triggering any native work.
            self.db.execute("SAVEPOINT creation_validation")
            try:
                self.create_project(
                    actor,
                    {**canonical, "thread_id": body.get("thread_id") or "creation-" + identifier()},
                )
            finally:
                self.db.execute("ROLLBACK TO creation_validation")
                self.db.execute("RELEASE creation_validation")
            intent = {
                "body": canonical,
                "thread_id": body.get("thread_id"),
                "creation_id": identifier() if not body.get("thread_id") else None,
                "status": "reserved",
                "created": self.clock(),
                "elapsed_admission": {"epoch": self.elapsed_epoch, "started": self.elapsed()},
                "admission_order": self.next_admission_order(),
            }
            self.save_creation(actor, key, intent)
            return intent, True

    def get_creation(self, actor, key):
        key = text(key, "project idempotency key", 200)
        row = self.db.execute(
            "SELECT data FROM creations WHERE actor=? AND key=?", (actor, key)
        ).fetchone()
        if row is None:
            raise KeyError("Unknown project creation")
        return json.loads(row[0])

    def unresolved_creations(self, channel):
        return [
            (actor, key, json.loads(raw))
            for actor, key, raw in self.db.execute(
                "SELECT actor,key,data FROM creations WHERE channel=? AND unresolved=1",
                (channel,),
            )
        ]

    def known_discussion(self, channel, thread):
        return bool(
            self.db.execute(
                "SELECT 1 FROM projects WHERE channel=? AND thread=? LIMIT 1", (channel, thread)
            ).fetchone()
            or self.db.execute(
                "SELECT 1 FROM creations WHERE channel=? AND thread=? LIMIT 1", (channel, thread)
            ).fetchone()
        )

    def record_creation_root(self, actor, key, thread, *, creation_id=None):
        """Keep one root binding even when posting and verification finish out of order."""
        thread = text(thread, "discussion root", 128)
        with self.transaction():
            intent = self.get_creation(actor, key)
            if creation_id is not None and intent.get("creation_id") != creation_id:
                raise PermissionError("Discussion creation correlation changed")
            if intent.get("thread_id") and intent["thread_id"] != thread:
                raise ValueError("Conflicting project discussion for creation key")
            intent["thread_id"] = thread
            if intent["status"] != "complete":
                intent["status"] = "posted"
            self.save_creation(actor, key, intent)
            return intent

    def reject_creation(self, actor, key, creation_id):
        """A confirmed authorization rejection permits retry, without erasing a root."""
        with self.transaction():
            intent = self.get_creation(actor, key)
            if intent.get("creation_id") != creation_id:
                raise PermissionError("Discussion creation correlation changed")
            if not intent.get("thread_id") and intent["status"] == "reserved":
                intent["status"] = "rejected"
                self.save_creation(actor, key, intent)

    def save_creation(self, actor, key, intent):
        key = text(key, "project idempotency key", 200)
        with self.transaction():
            existing = self.db.execute(
                "SELECT admission_order FROM creations WHERE actor=? AND key=?", (actor, key)
            ).fetchone()
            order = existing[0] if existing else intent.get("admission_order")
            if order is None:
                order = self.next_admission_order()
            self.db.execute(
                "INSERT OR REPLACE INTO creations"
                "(actor,key,data,channel,thread,unresolved,admission_order,origin_thread,created) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    actor,
                    key,
                    json.dumps(intent),
                    intent["body"]["channel_id"],
                    intent.get("thread_id"),
                    int(
                        bool(intent.get("creation_id"))
                        and intent["status"] != "rejected"
                        and not intent.get("thread_id")
                    ),
                    order,
                    intent["body"].get("origin_thread_id") or intent.get("thread_id"),
                    intent.get("created", 0),
                ),
            )

    def finish_creation(self, actor, intent):
        with self.transaction():
            project = self.create_project(
                actor,
                {**intent["body"], "thread_id": intent["thread_id"]},
                created=intent.get("created"),
                elapsed_admission=intent.get("elapsed_admission"),
                admission_order=self.db.execute(
                    "SELECT admission_order FROM creations WHERE actor=? AND key=?",
                    (actor, intent["body"]["key"]),
                ).fetchone()[0],
            )
            stop = self.db.execute(
                "SELECT admission_order FROM origin_stops WHERE actor=? AND thread=?",
                (actor, project["origin_thread_id"]),
            ).fetchone()
            order = self.db.execute(
                "SELECT admission_order FROM projects WHERE id=?", (project["id"],)
            ).fetchone()[0]
            if (stop and order <= stop[0]) or self.remaining_seconds(project) <= 0:
                project = self.cancel(project["id"], actor)
            if "discussion_created_at" not in project and intent.get("discussion_created_at"):
                project["discussion_created_at"] = intent["discussion_created_at"]
                self.save_project(project)
            intent["status"] = "complete"
            self.save_creation(actor, intent["body"]["key"], intent)
            return project

    def create_project(
        self, actor, body, *, created=None, admission_order=None, elapsed_admission=None
    ):
        with self.transaction():
            self.expire()
            return self._create_project(
                actor,
                body,
                created=created,
                admission_order=admission_order,
                elapsed_admission=elapsed_admission,
            )

    def project_by_creation_key(self, actor, key):
        key = text(key, "project idempotency key", 200)
        row = self.db.execute(
            "SELECT data FROM projects WHERE owner=? AND creation_key=? ORDER BY rowid LIMIT 1",
            (actor, key),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def creation_evidence(self, actor, key):
        with self.transaction():
            self.expire()
            project = self.project_by_creation_key(actor, key)
            try:
                intent = self.get_creation(actor, key)
            except KeyError:
                intent = None
            return {"project": project, "creation": intent}

    def _create_project(
        self, actor, body, *, created=None, admission_order=None, elapsed_admission=None
    ):
        body = {"owner_in_discussion": False, **body}
        key = body.get("key")
        fingerprint = hashlib.sha256(
            json.dumps({k: v for k, v in body.items() if k != "key"}, sort_keys=True).encode()
        ).hexdigest()
        if key:
            key = text(key, "project idempotency key", 200)
            existing = self.project_by_creation_key(actor, key)
            if existing is not None:
                if existing.get("fingerprint") != fingerprint:
                    raise ValueError("Conflicting project idempotency key")
                return existing
        members = body.get("members", [])
        if (
            not isinstance(members, list)
            or not members
            or any(not isinstance(member, str) for member in members)
        ):
            raise ValueError("members must list declared actors")
        if actor not in members or not set(members) <= self.actors:
            raise PermissionError("Owner and all members must be declared actors")
        limits = {"max_workers": 3, "max_turns": 24, "max_depth": 2, "max_seconds": 3600}
        supplied = body.get("limits", {})
        if not isinstance(supplied, dict) or not set(supplied) <= limits.keys():
            raise ValueError("Unknown project limits")
        limits.update(supplied)
        if any(type(v) is not int or v <= 0 or v > 100000 for v in limits.values()):
            raise ValueError("Project limits must be positive bounded integers")
        channel = text(body.get("channel_id"), "channel_id", 128)
        thread = text(body.get("thread_id"), "thread_id", 128)
        admitted = self.clock() if created is None else created
        if elapsed_admission is None and created is None:
            elapsed_admission = {"epoch": self.elapsed_epoch, "started": self.elapsed()}
        elapsed_admission = elapsed_admission or {}
        elapsed_started = elapsed_admission.get("started")
        verified_elapsed = (
            elapsed_admission.get("epoch") == self.elapsed_epoch
            and type(elapsed_started) in {int, float}
            and math.isfinite(elapsed_started)
            and elapsed_started <= self.elapsed()
        )
        item = {
            "id": identifier(),
            "title": text(body.get("title"), "title", 200),
            "objective": text(body.get("objective"), "objective"),
            "owner": actor,
            "members": sorted(set(members)),
            "channel_id": channel,
            "thread_id": thread,
            "origin_thread_id": body.get("origin_thread_id") or thread,
            "origin_channel_id": body.get("origin_channel_id") or channel,
            "status": "active",
            "created": admitted,
            "limits": limits,
            "deadline": admitted + limits["max_seconds"],
            "elapsed_epoch": elapsed_admission.get("epoch") if verified_elapsed else None,
            "elapsed_deadline": elapsed_started + limits["max_seconds"]
            if verified_elapsed
            else None,
            "dispatched": 0,
            "cost_coverage": "unknown",
            "bindings": {},
            "owner_in_discussion": bool(body.get("owner_in_discussion", False)),
            "key": key,
            "fingerprint": fingerprint,
        }
        for member in members:
            item["bindings"][member] = {"thread_id": thread, "channel_id": channel}
        if not item["owner_in_discussion"]:
            item["bindings"][actor] = {
                "thread_id": item["origin_thread_id"],
                "channel_id": item["origin_channel_id"],
            }
        with self.transaction():
            for member, binding in item["bindings"].items():
                self.check_binding(item, member, binding)
            self.save_project(item, admission_order=admission_order)
        return item

    def check_binding(self, project, actor, binding):
        if (
            actor in getattr(self, "sidecar_actors", set())
            or (actor == project["owner"] and project.get("owner_in_discussion"))
        ) and (
            binding["thread_id"] != project["thread_id"]
            or binding["channel_id"] != project["channel_id"]
        ):
            raise ValueError("Sidecar bindings must stay in the project discussion")
        for (raw,) in self.db.execute(
            "SELECT p.data FROM project_bindings b JOIN projects p ON p.id=b.project "
            "WHERE b.actor=? AND b.thread=? AND p.id<>? AND (p.status<>'cancelled' "
            "OR EXISTS(SELECT 1 FROM messages m WHERE m.project=p.id AND m.recipient=? "
            "AND (m.execution_pending=1 OR m.state IN "
            "('running','unconfirmed','interrupted','cancelling'))))",
            (actor, binding["thread_id"], project["id"], actor),
        ):
            other = json.loads(raw)
            existing = other["bindings"][actor]
            if existing["thread_id"] != binding["thread_id"]:
                continue
            # Results may return to the same owner's conversation. Worker sessions
            # must remain exclusive, including sessions awaiting reconciliation.
            if (
                project["owner"] == actor == other["owner"]
                and actor not in getattr(self, "sidecar_actors", set())
                and not project.get("owner_in_discussion")
                and not other.get("owner_in_discussion")
                and binding["thread_id"] == project["origin_thread_id"]
                and binding["channel_id"] == project["origin_channel_id"]
                and existing["thread_id"] == other["origin_thread_id"]
                and existing["channel_id"] == other["origin_channel_id"]
            ):
                continue
            raise ValueError("Native worker binding belongs to another project")

    def list_projects(self, actor):
        return [
            item
            for (raw,) in self.db.execute("SELECT data FROM projects")
            if actor in (item := json.loads(raw))["members"]
        ]

    def show(self, pid, actor):
        item = self.get_project(pid, actor)
        item["messages"] = [
            self.get_message(row[0], actor)
            for row in self.db.execute(
                "SELECT id FROM messages WHERE project=? ORDER BY created,id", (pid,)
            )
        ]
        item["notes"] = [
            json.loads(row[0])
            for row in self.db.execute("SELECT data FROM notes WHERE project=?", (pid,))
        ]
        return item

    def bind(self, pid, actor, body):
        with self.transaction():
            self.expire()
            project = self.get_project(pid, actor)
            if self.db.execute(
                "SELECT 1 FROM messages WHERE project=? AND recipient=? "
                "AND (state IN ('running','unconfirmed','interrupted','cancelling') "
                "OR " + PENDING_EXECUTION + ")",
                (pid, actor),
            ).fetchone():
                raise ValueError("Cannot change a binding with active requests")
            binding = {
                "thread_id": text(body.get("thread_id"), "thread_id", 128),
                "channel_id": text(body.get("channel_id"), "channel_id", 128),
            }
            if body.get("workspace"):
                workspace = Path(body["workspace"])
                if not workspace.is_absolute() or not workspace.is_dir():
                    raise ValueError("workspace must be an existing absolute directory")
                binding["workspace"] = str(workspace.resolve())
            self.check_binding(project, actor, binding)
            project["bindings"][actor] = binding
            self.save_project(project)
            return binding

    def send(self, pid, actor, body):
        with self.transaction():
            project = self.get_project(pid, actor)
            kind = body.get("kind", "ask")
            if kind not in WORK:
                raise ValueError("Only substantive ask/work requests can be submitted")
            key = text(body.get("key"), "idempotency key", 200)
            if key.startswith(("result:", "cancel:")):
                raise ValueError("Reserved delivery key namespace")
            fingerprint = hashlib.sha256(
                json.dumps({k: v for k, v in body.items() if k != "key"}, sort_keys=True).encode()
            ).hexdigest()
            row = self.db.execute(
                "SELECT data FROM messages WHERE project=? AND sender=? AND key=?",
                (pid, actor, key),
            ).fetchone()
            if row:
                existing = json.loads(row[0])
                if existing.get("fingerprint") != fingerprint:
                    raise ValueError("Conflicting idempotency key payload")
                return existing
            if project["status"] != "active" or self.remaining_seconds(project) <= 0:
                raise ValueError("Project is inactive or expired")
            recipient = body.get("recipient")
            if recipient is not None and recipient not in project["members"]:
                raise PermissionError("Recipient is not a project participant")
            if recipient == actor:
                raise ValueError("Self-addressed work is not collaboration")
            parent_id = body.get("parent")
            depth = 0
            if parent_id:
                parent = self.message_record(parent_id, actor)
                if parent["project"] != pid or parent["recipient"] != actor:
                    raise PermissionError("Delegation parent must be addressed to the sender")
                depth = parent["depth"] + 1
            if depth > project["limits"]["max_depth"]:
                raise ValueError("Project delegation depth limit reached")
            self.check_ancestor_recipient(pid, actor, recipient, parent_id)
            dependencies = body.get("dependencies", [])
            if not isinstance(dependencies, list) or len(dependencies) > 100:
                raise ValueError("dependencies must be a bounded list")
            for dependency in dependencies:
                dep = self.message_record(dependency, actor)
                if dep["project"] != pid or dep["kind"] not in WORK:
                    raise ValueError("Dependency must be project work")
            if project["dispatched"] >= project["limits"]["max_turns"]:
                raise ValueError("Project dispatch limit reached")
            item = {
                "id": identifier(),
                "project": pid,
                "sender": actor,
                "recipient": recipient,
                "kind": kind,
                "text": text(body.get("text")),
                "key": key,
                "fingerprint": fingerprint,
                "parent": parent_id,
                "dependencies": dependencies,
                "depth": depth,
                "created": self.clock(),
                "state": "queued" if recipient else "open",
                "evidence": {},
                "attempt": 0,
            }
            project["dispatched"] += 1
            self.save_project(project)
            self.save_message(item)
            return item

    def check_ancestor_recipient(self, pid, actor, recipient, parent_id):
        if recipient is None:
            return
        seen = set()
        while parent_id:
            if parent_id in seen:
                raise ValueError("Delegation ancestry is invalid")
            seen.add(parent_id)
            parent = self.message_record(parent_id, actor)
            if parent["project"] != pid:
                raise ValueError("Delegation ancestry belongs to another project")
            if recipient in {parent["sender"], parent.get("recipient")}:
                raise ValueError(
                    "Cannot delegate to an ancestor; return a blocked result to the parent instead"
                )
            parent_id = parent.get("parent")

    def claim_open(self, mid, actor):
        with self.transaction():
            item = self.get_message(mid, actor)
            if item.get("recipient") == actor and item["kind"] == "work":
                return item
            if item["state"] != "open" or item.get("recipient"):
                raise ValueError("Task is already claimed")
            if item["sender"] == actor:
                raise ValueError("Claim a peer's task, not your own")
            project = self.get_project(item["project"], actor)
            if project["status"] != "active" or self.remaining_seconds(project) <= 0:
                raise ValueError("Project is inactive")
            self.check_ancestor_recipient(item["project"], actor, actor, item.get("parent"))
            item.update(recipient=actor, state="queued")
            self.save_message(item)
            return item

    def note(self, pid, actor, body, artifacts=None, key=None):
        with self.transaction():
            self.get_project(pid, actor)
            if not isinstance(artifacts or [], list):
                raise ValueError("artifacts must be a list")
            item = {
                "id": identifier(),
                "project": pid,
                "actor": actor,
                "text": text(body),
                "artifacts": artifacts or [],
                "created": self.clock(),
                "key": text(key, "note idempotency key", 200) if key else identifier(),
            }
            for (raw,) in self.db.execute("SELECT data FROM notes WHERE project=?", (pid,)):
                existing = json.loads(raw)
                if existing["actor"] == actor and existing.get("key") == item["key"]:
                    if (
                        existing["text"] != item["text"]
                        or existing["artifacts"] != item["artifacts"]
                    ):
                        raise ValueError("Conflicting note idempotency key")
                    return existing
            self.db.execute("INSERT INTO notes VALUES(?,?,?)", (item["id"], pid, json.dumps(item)))
            return item

    def expire(self):
        with self.transaction():
            now = self.clock()
            for row in self.db.execute(
                "SELECT data FROM projects WHERE status='active' AND "
                "(deadline<=? OR elapsed_epoch IS NULL OR elapsed_epoch<>? "
                "OR elapsed_deadline IS NULL OR elapsed_deadline<=?)",
                (now, self.elapsed_epoch, self.elapsed()),
            ).fetchall():
                project = json.loads(row[0])
                self.cancel(project["id"], project["owner"])
            for row in self.db.execute("SELECT data FROM messages WHERE state='running'"):
                item = json.loads(row[0])
                if item.get("lease_until", 0) <= self.clock():
                    item["state"] = "queued" if item["kind"] == "cancel" else "interrupted"
                    item["recovery"] = "Worker lease expired; reconcile before retry"
                    self.save_message(item)

    def active_claims(self, actor, instance=None):
        rows = self.db.execute(
            "SELECT data FROM messages WHERE recipient=? AND "
            "state IN ('running','cancelling','unconfirmed','interrupted') "
            "UNION ALL SELECT data FROM messages WHERE recipient=? AND execution_pending=1 "
            "AND state NOT IN ('running','cancelling','unconfirmed','interrupted')",
            (actor, actor),
        ).fetchall()
        return [
            item
            for row in rows
            if (item := json.loads(row[0]))
            and (instance is None or item.get("instance") == instance)
        ]

    def queued_candidates(self, actor, controls_only):
        priority = "CASE kind WHEN 'cancel' THEN 0 WHEN 'result' THEN 1 ELSE 2 END"
        after = None
        while True:
            rows = self.db.execute(
                f"SELECT id,created,{priority} AS priority FROM messages "
                "WHERE recipient=? AND state='queued' "
                + ("AND kind='cancel' " if controls_only else "")
                + (f"AND ({priority},created,id)>(?,?,?) " if after else "")
                + "ORDER BY priority,created,id LIMIT 32",
                (actor, *after) if after else (actor,),
            ).fetchall()
            if not rows:
                return
            for row in rows:
                payload = self.db.execute(
                    "SELECT data FROM messages WHERE id=? AND recipient=? AND state='queued'",
                    (row["id"], actor),
                ).fetchone()
                if payload:
                    yield json.loads(payload[0])
            last = rows[-1]
            after = last["priority"], last["created"], last["id"]

    def claim_next(self, actor, instance, *, controls_only=False):
        with self.transaction():
            self.expire()
            for item in self.queued_candidates(actor, controls_only):
                project = self.get_project(item["project"], actor)
                if item.get("not_before", 0) > self.clock():
                    continue
                if item["kind"] in WORK:
                    if project["status"] != "active":
                        continue
                    if self.db.execute(
                        "SELECT 1 FROM messages WHERE project=? AND recipient=? "
                        "AND (state IN ('unconfirmed','interrupted') OR ("
                        + PENDING_EXECUTION
                        + " AND json_extract(data,'$.instance') IS NOT ?))",
                        (project["id"], actor, instance),
                    ).fetchone():
                        continue
                    if any(
                        self.message_record(dep, actor)["state"] != "done"
                        for dep in item["dependencies"]
                    ):
                        continue
                    active = self.db.execute(
                        "SELECT count(*) FROM messages WHERE project=? "
                        "AND kind IN ('ask','work') AND (state IN "
                        "('running','unconfirmed','interrupted','cancelling') "
                        "OR " + PENDING_EXECUTION + ")",
                        (project["id"],),
                    ).fetchone()[0]
                    if active >= project["limits"]["max_workers"]:
                        # A parent waiting for a delegated result cannot release
                        # its slot yet. Loan one slot per active parent, bounded
                        # by depth and a hard total ceiling. Siblings serialize.
                        parent = (
                            self.message_record(item["parent"], actor)
                            if item.get("parent")
                            else None
                        )
                        sibling = self.db.execute(
                            "SELECT 1 FROM messages WHERE project=? AND kind IN ('ask','work') "
                            "AND (state IN ('running','unconfirmed','interrupted','cancelling') "
                            "OR " + PENDING_EXECUTION + ") "
                            "AND json_extract(data,'$.parent')=? LIMIT 1",
                            (project["id"], parent["id"] if parent else None),
                        ).fetchone()
                        if (
                            not parent
                            or parent["state"] != "running"
                            or sibling
                            or active
                            >= project["limits"]["max_workers"]
                            * (project["limits"]["max_depth"] + 1)
                        ):
                            continue
                        item["delegation_slot"] = True
                    # Only one work request can occupy an assistant's project thread.
                    if (
                        item["kind"] == "work"
                        and self.db.execute(
                            "SELECT 1 FROM messages WHERE project=? AND recipient=? "
                            "AND kind IN ('ask','work') AND (state IN "
                            "('running','unconfirmed','interrupted','cancelling') "
                            "OR " + PENDING_EXECUTION + ")",
                            (project["id"], actor),
                        ).fetchone()
                    ):
                        continue
                item.update(
                    state="running",
                    instance=instance,
                    lease_until=self.clock() + 45,
                    attempt=item["attempt"] + 1,
                    claim=identifier(),
                )
                item["binding"] = project["bindings"][actor]
                self.save_message(item)
                continuation = {}
                if item["kind"] == "result" and item.get("parent"):
                    child = self.message_record(item["parent"], actor)
                    if child.get("parent"):
                        parent = self.message_record(child["parent"], actor)
                        continuation["continuation_parent"] = parent["id"]
                        for source, target in (
                            ("claim", "continuation_claim"),
                            ("instance", "continuation_instance"),
                        ):
                            if parent.get(source):
                                continuation[target] = parent[source]
                return {
                    **item,
                    **continuation,
                    "project_title": project["title"],
                    "objective": project["objective"],
                    "project_owner": project["owner"],
                    "project_thread_id": project["thread_id"],
                    "project_channel_id": project["channel_id"],
                    "origin_thread_id": project["origin_thread_id"],
                    "origin_channel_id": project["origin_channel_id"],
                    "project_owner_in_discussion": project.get("owner_in_discussion", False),
                    "project_deadline": project["deadline"],
                    "project_remaining_seconds": self.remaining_seconds(project),
                }
            return None

    def update(
        self,
        mid,
        actor,
        instance,
        state,
        result="",
        evidence=None,
        claim=None,
        *,
        expected_execution_id=None,
        include_history=True,
    ):
        receipt = self._update(
            mid,
            actor,
            instance,
            state,
            result,
            evidence,
            claim,
            expected_execution_id=expected_execution_id,
        )
        return self.get_message(mid, actor) if include_history else receipt

    def _update(
        self,
        mid,
        actor,
        instance,
        state,
        result="",
        evidence=None,
        claim=None,
        *,
        expected_execution_id=None,
    ):
        with self.transaction():
            if isinstance(result, str):
                result = result.strip()
            item = self.message_record(mid, actor)
            if (
                item["recipient"] != actor
                or item.get("instance") != instance
                or (item.get("attempt", 0) > 1 and claim != item.get("claim"))
                or (claim is not None and claim != item.get("claim"))
            ):
                raise PermissionError("Stale or foreign worker instance")
            if (
                expected_execution_id is None
                and state not in {"dispatch", "post_dispatch"}
                and item.get("evidence", {}).get("execution_id") is not None
            ):
                expected_execution_id = (evidence or {}).get("execution_id")
            if expected_execution_id is not None:
                expected_execution_id = text(expected_execution_id, "expected execution ID", 200)
                if item.get("evidence", {}).get("execution_id") != expected_execution_id:
                    if state == "execution_settled":
                        raise ValueError("Native settlement must match the reserved execution")
                    raise ValueError("Native execution changed before update")
            if state in {"evidence", "execution_settled", "execution_resumed", "undispatched"}:
                if state == "execution_resumed":
                    receipt = evidence or {}
                    reserved = item.get("evidence", {})
                    if (
                        item["state"] not in TERMINAL
                        or receipt.get("pending") is not True
                        or receipt.get("native_dispatched") is not True
                        or not reserved.get("session_id")
                        or not reserved.get("execution_id")
                        or reserved.get("not_dispatched") is True
                        or reserved.get("native_dispatched") is False
                        or receipt.get("session_id") != reserved.get("session_id")
                        or receipt.get("execution_id") != reserved.get("execution_id")
                        or receipt.get("native_epoch", reserved.get("native_epoch"))
                        != reserved.get("native_epoch")
                    ):
                        raise ValueError("Resumed native work must match the reserved execution")
                    item["execution_pending"] = True
                    project = self.get_project(item["project"], actor)
                    binding = item.get("binding", {})
                    owner_origin = (
                        item["recipient"] == project["owner"]
                        and not project.get("owner_in_discussion")
                        and binding.get("thread_id") == project["origin_thread_id"]
                        and binding.get("channel_id") == project["origin_channel_id"]
                    )
                    if project["status"] in {"cancelling", "cancelled"} and not owner_origin:
                        project["status"] = "cancelling"
                        item["execution_cancelling"] = True
                        self.save_project(project)
                        self.queue_cancel_control(project, item)
                if state == "undispatched":
                    receipt = evidence or {}
                    reserved = item.get("evidence", {})
                    if (
                        receipt.get("native_dispatched") is not False
                        or receipt.get("not_dispatched") is not True
                        or not reserved.get("session_id")
                        or any(
                            receipt.get(field) != reserved.get(field)
                            for field in ("session_id", "execution_id", "native_epoch")
                        )
                    ):
                        raise ValueError("Undispatched proof must match the native reservation")
                    item["execution_pending"] = False
                    item["execution_cancelling"] = False
                    if item["state"] == "cancelling":
                        item.update(
                            state="cancelled",
                            result="Cancelled before native dispatch",
                            completed=self.clock(),
                        )
                if state == "execution_settled":
                    if item["state"] not in TERMINAL:
                        raise ValueError("Native settlement requires a terminal correlated result")
                    receipt = evidence or {}
                    reserved = item.get("evidence", {})
                    completed = receipt.get("completed_at")
                    linked = bool(
                        reserved.get("reservation_id")
                        and receipt.get("reservation_id") == reserved["reservation_id"]
                        and "native_epoch" in reserved
                        and receipt.get("native_epoch") == reserved["native_epoch"]
                    )
                    if receipt.get("not_dispatched") and reserved.get("session_id"):
                        raise ValueError("A reserved session requires native settlement evidence")
                    if not receipt.get("not_dispatched") and (
                        not reserved.get("session_id")
                        or not reserved.get("execution_id")
                        or receipt.get("session_id") != reserved.get("session_id")
                        or receipt.get("execution_id") != reserved.get("execution_id")
                        or receipt.get("reservation_id", reserved.get("reservation_id"))
                        != reserved.get("reservation_id")
                        or receipt.get("native_epoch", reserved.get("native_epoch"))
                        != reserved.get("native_epoch")
                        or not receipt.get("native_dispatched")
                        or not receipt.get("stop_reason")
                        or receipt.get("pending") is not False
                        or type(completed) not in {int, float}
                        or not math.isfinite(completed)
                        or (
                            not linked
                            and completed < reserved.get("dispatch_started", float("inf"))
                        )
                    ):
                        raise ValueError("Native settlement must match the reserved execution")
                    item["execution_pending"] = False
                    item["execution_cancelling"] = False
                item["evidence"].update(evidence or {})
                self.save_message(item)
                if item["kind"] in WORK:
                    for (raw,) in self.db.execute(
                        "SELECT data FROM messages WHERE project=? AND kind='result'",
                        (item["project"],),
                    ).fetchall():
                        delivery = json.loads(raw)
                        if delivery.get("parent") != mid:
                            continue
                        delivery.setdefault("result_evidence", {}).update(evidence or {})
                        self.save_message(delivery)
                if state in {"execution_settled", "undispatched"}:
                    self.finish_cancellation(item["project"], actor)
                return item
            if item["state"] in TERMINAL:
                if state == item["state"] and result == item.get("result", ""):
                    return item
                raise ValueError("Message already settled")
            if item["state"] not in ACTIVE:
                raise PermissionError("Delivery is no longer owned by this worker")
            if state in {"dispatch", "post_dispatch"}:
                self.expire()
                item = self.message_record(mid, actor)
                project = self.get_project(item["project"], actor)
                if item["state"] != "running" or project["status"] != "active":
                    return item
                if state == "dispatch" and not (evidence or {}).get("session_id"):
                    raise ValueError("Native dispatch requires its session")
                if state == "post_dispatch" and (
                    item["kind"] != "result"
                    or item.get("execution_pending")
                    or any(
                        (evidence or {}).get(field)
                        for field in ("session_id", "execution_id", "native_epoch")
                    )
                ):
                    raise ValueError("Post delivery cannot reserve native execution")
                item["evidence"].update(evidence or {})
                if state == "dispatch":
                    item["evidence"].pop("not_dispatched", None)
                    item["evidence"].pop("native_dispatched", None)
                item["evidence"]["dispatch_started" if state == "dispatch" else "post_started"] = (
                    self.clock()
                )
                item["execution_pending"] = state == "dispatch"
                item["lease_until"] = self.clock() + 45
            elif state == "heartbeat":
                item["lease_until"] = self.clock() + 45
                if evidence:
                    item["evidence"].update(evidence)
            elif state == "queued" and item["state"] == "cancelling":
                # Queue fallback is an explicit assertion that native dispatch
                # did not occur. Cancellation wins that race without a stop RPC.
                item.update(
                    state="cancelled",
                    result="Cancelled before native dispatch",
                    completed=self.clock(),
                )
                item["evidence"]["not_dispatched"] = True
                item["execution_pending"] = False
            elif state == "queued" and item["state"] == "running":
                self.archive_delivery(item)
                item["evidence"] = {}
                item["execution_pending"] = False
                item.update(state="queued", not_before=self.clock() + 2)
            elif state in {"unconfirmed", "interrupted"}:
                item["recovery"] = text(result, "recovery")
                if item["state"] != "cancelling":
                    item["state"] = "queued" if item["kind"] == "cancel" else state
                    if item["kind"] == "cancel":
                        item["not_before"] = self.clock() + 2
            elif state in TERMINAL:
                if item["state"] == "cancelling" and state != "cancelled":
                    if (evidence or {}).get("stop_reason") and not (evidence or {}).get("pending"):
                        item.update(
                            state=state,
                            result=result,
                            evidence={**item["evidence"], **(evidence or {})},
                            completion_during_cancellation=True,
                            completed=self.clock(),
                        )
                        self.save_message(item)
                        self.finish_cancellation(item["project"], actor)
                        return item
                    item["late_result"] = result
                    self.save_message(item)
                    return item
                if item["kind"] in WORK and state != "cancelled":
                    return self._reply(
                        mid, actor, result, state=state, evidence=evidence, claim=claim
                    )
                item.update(
                    state=state,
                    result=result,
                    evidence={**item["evidence"], **(evidence or {})},
                    completed=self.clock(),
                )
            else:
                raise ValueError("Invalid delivery transition")
            self.save_message(item)
            self.finish_cancellation(item["project"], actor)
            return item

    def reply(self, mid, actor, result, *, state="done", evidence=None, claim=None):
        self._reply(mid, actor, result, state=state, evidence=evidence, claim=claim)
        return self.get_message(mid, actor)

    def _reply(self, mid, actor, result, *, state="done", evidence=None, claim=None):
        with self.transaction():
            item = self.message_record(mid, actor)
            if item["recipient"] != actor or item["kind"] not in WORK:
                raise PermissionError("Only the addressed worker can reply")
            if (item.get("attempt", 0) > 1 and claim != item.get("claim")) or (
                claim is not None and claim != item.get("claim")
            ):
                raise PermissionError("Stale reply claim")
            result = text(result, "result")
            supplied_evidence = evidence or {}
            evidence = {**item["evidence"], **supplied_evidence}
            if state not in {"done", "failed", "blocked"}:
                raise ValueError("Invalid result state")
            if item["state"] in TERMINAL:
                if (
                    item.get("result") == result
                    and item["state"] == state
                    and (
                        "artifacts" not in supplied_evidence
                        or item["evidence"].get("artifacts", []) == supplied_evidence["artifacts"]
                    )
                ):
                    return item
                raise ValueError("Conflicting repeated result")
            if item["state"] == "cancelling":
                if "late_state" in item:
                    if (
                        item["late_result"] != result
                        or item["late_state"] != state
                        or (
                            "artifacts" in supplied_evidence
                            and item.get("late_evidence", {}).get("artifacts", [])
                            != supplied_evidence["artifacts"]
                        )
                    ):
                        raise ValueError("Conflicting repeated result")
                    return item
                item["late_result"] = result
                item["late_state"] = state
                item["late_evidence"] = evidence
                self.save_message(item)
                return item
            if item["state"] not in {"running", "unconfirmed", "interrupted"}:
                raise ValueError("Request has not been accepted")
            item.update(state=state, result=result, evidence=evidence, completed=self.clock())
            self.save_message(item)
            delivery = {
                "id": identifier(),
                "project": item["project"],
                "sender": actor,
                "recipient": item["sender"],
                "kind": "result",
                "text": result,
                "parent": mid,
                "depth": item["depth"],
                "dependencies": [],
                "key": "result:" + mid,
                "state": "queued",
                "created": self.clock(),
                "attempt": 0,
                "evidence": {},
                "result_evidence": evidence,
                "result_state": state,
            }
            self.save_message(delivery)
            return item

    def retry(self, mid, actor, reconciliation):
        with self.transaction():
            item = self.message_record(mid, actor)
            project = self.get_project(item["project"], actor)
            if actor not in {project["owner"], item["recipient"]}:
                raise PermissionError("Only owner or recipient can reconcile delivery")
            if item["state"] not in {"interrupted", "unconfirmed"}:
                raise ValueError("Only interrupted/unconfirmed delivery can be retried")
            if project["status"] != "active" or self.remaining_seconds(project) <= 0:
                raise ValueError("Project is inactive")
            if item["kind"] in WORK:
                if project["dispatched"] >= project["limits"]["max_turns"]:
                    raise ValueError("Project dispatch limit reached")
                project["dispatched"] += 1
                self.save_project(project)
            self.archive_delivery(item)
            item["evidence"] = {}
            item["execution_pending"] = False
            item.update(
                state="queued",
                reconciliation=text(reconciliation, "reconciliation"),
                instance=None,
                not_before=0,
            )
            self.save_message(item)
            return self.get_message(mid, actor)

    def cancel(self, pid, actor):
        with self.transaction():
            project = self.get_project(pid, actor)
            if project["owner"] != actor:
                raise PermissionError("Only the project owner can cancel")
            if project["status"] in {"cancelling", "cancelled"}:
                return project
            project["status"] = "cancelling"
            self.save_project(project)
            for row in self.db.execute(
                "SELECT data FROM messages WHERE project=?", (pid,)
            ).fetchall():
                item = json.loads(row[0])
                if item["kind"] not in WORK and item["kind"] != "result":
                    continue
                binding = item.get("binding", {})
                owner_origin = (
                    item["recipient"] == project["owner"]
                    and not project.get("owner_in_discussion")
                    and binding.get("thread_id") == project["origin_thread_id"]
                    and binding.get("channel_id") == project["origin_channel_id"]
                )
                if (
                    item["state"] in TERMINAL
                    and item.get(
                        "execution_pending",
                        bool(
                            item.get("evidence", {}).get("session_id")
                            and item.get("evidence", {}).get("dispatch_started") is not None
                        ),
                    )
                    and not owner_origin
                ):
                    # The correlated result is complete, but its reserved native
                    # execution remains cancellable until verified settlement.
                    item["execution_cancelling"] = True
                    self.save_message(item)
                if item["state"] in {"queued", "open"} or (
                    item["state"] not in TERMINAL and owner_origin
                ):
                    # Stop routing into the owner's conversation without stopping
                    # its existing work. Origin /stop separately cancels that turn.
                    item["state"] = "cancelled"
                    self.save_message(item)
                elif item["state"] in {"running", "unconfirmed", "interrupted"} or item.get(
                    "execution_cancelling"
                ):
                    if item["state"] not in TERMINAL:
                        item["state"] = "cancelling"
                    self.save_message(item)
                    self.queue_cancel_control(project, item)
            self.finish_cancellation(pid, actor)
            return self.get_project(pid, actor)

    def queue_cancel_control(self, project, item):
        if self.db.execute(
            "SELECT 1 FROM messages WHERE project=? AND kind='cancel' "
            "AND json_extract(data,'$.parent')=? "
            "AND json_extract(data,'$.parent_claim') IS ? "
            "AND state NOT IN ('done','failed','blocked','cancelled') LIMIT 1",
            (project["id"], item["id"], item.get("claim")),
        ).fetchone():
            return
        key = "cancel:" + item["id"]
        if self.db.execute(
            "SELECT 1 FROM messages WHERE project=? AND sender=? AND key=?",
            (project["id"], project["owner"], key),
        ).fetchone():
            key += ":" + identifier()
        self.save_message(
            {
                "id": identifier(),
                "project": project["id"],
                "sender": project["owner"],
                "recipient": item["recipient"],
                "kind": "cancel",
                "text": "Cancel project work",
                "parent": item["id"],
                "parent_claim": item.get("claim"),
                "dependencies": [],
                "depth": 0,
                "key": key,
                "state": "queued",
                "created": self.clock(),
                "attempt": 0,
                "evidence": {},
            }
        )

    def finish_cancellation(self, pid, actor):
        project = self.get_project(pid, actor)
        if (
            project["status"] == "cancelling"
            and not self.db.execute(
                "SELECT 1 FROM messages WHERE project=? AND (state='cancelling' "
                "OR json_extract(data,'$.execution_cancelling')=1) LIMIT 1",
                (pid,),
            ).fetchone()
        ):
            project["status"] = "cancelled"
            self.save_project(project)

    def get_stop_boundary(self, actor, key):
        key = text(key, "stop idempotency key", 200)
        row = self.db.execute(
            "SELECT thread,admission_order FROM stop_boundaries WHERE actor=? AND key=?",
            (actor, key),
        ).fetchone()
        if row is None:
            raise KeyError("Stop boundary was not captured")
        return {"key": key, "thread_id": row[0], "admission_order": row[1]}

    def _record_stop_boundary(self, actor, thread, key):
        thread = text(thread, "origin thread", 128)
        key = text(key, "stop idempotency key", 200)
        with self.transaction():
            row = self.db.execute(
                "SELECT thread FROM stop_boundaries WHERE actor=? AND key=?", (actor, key)
            ).fetchone()
            if row:
                if row[0] != thread:
                    raise ValueError("Conflicting stop idempotency key")
            else:
                order = self.db.execute(
                    "SELECT value FROM admission_sequence WHERE id=1"
                ).fetchone()[0]
                self.db.execute(
                    "INSERT INTO stop_boundaries VALUES(?,?,?,?)", (actor, key, thread, order)
                )
            return self.get_stop_boundary(actor, key)

    def capture_stop_boundary(self, actor, thread, key):
        """Commit the boundary and its cancellation intent together."""
        with self.transaction():
            receipt = self._record_stop_boundary(actor, thread, key)
            self.cancel_thread(actor, thread, boundary=receipt)
            return receipt

    def legacy_stop_boundary(self, actor, thread, cutoff, *, migrating=False):
        try:
            if type(cutoff) not in {int, float} or not math.isfinite(float(cutoff)):
                raise ValueError("Cancellation cutoff must be a finite timestamp")
            cutoff = float(cutoff)
        except OverflowError:
            raise ValueError("Cancellation cutoff must be a finite timestamp") from None
        key = "legacy-cutoff:" + cutoff.hex()
        receipt_key = hashlib.sha256((thread + ":" + key).encode()).hexdigest()
        with self.transaction():
            try:
                receipt = self.get_stop_boundary(actor, receipt_key)
            except KeyError:
                # Legacy bodies lack an admission receipt. Freeze their old
                # timestamp selection once, so repeated delivery cannot widen it.
                # Metadata is complete after startup. Only the one-time
                # pre-metadata upgrade needs SQLite to inspect legacy JSON.
                predicate = (
                    "json_extract(data,'$.owner')=? "
                    "AND json_extract(data,'$.origin_thread_id')=? "
                    "AND json_extract(data,'$.created')<=?"
                    if migrating
                    else "owner=? AND origin_thread=? AND created<=?"
                )
                project_order = self.db.execute(
                    "SELECT COALESCE(MAX(admission_order),0) FROM projects WHERE " + predicate,
                    (actor, thread, cutoff),
                ).fetchone()[0]
                creation_order = self.db.execute(
                    "SELECT COALESCE(MAX(admission_order),0) FROM creations "
                    "WHERE actor=? AND origin_thread=? AND created<=?",
                    (actor, thread, cutoff),
                ).fetchone()[0]
                # Bound length independently of the allowed origin identifier.
                self.db.execute(
                    "INSERT OR IGNORE INTO stop_boundaries VALUES(?,?,?,?)",
                    (actor, receipt_key, thread, max(project_order, creation_order)),
                )
                receipt = self.get_stop_boundary(actor, receipt_key)
            return receipt

    def cancel_thread(self, actor, thread, *, cutoff=None, boundary=None):
        thread = text(thread, "origin thread", 128)
        if boundary is not None and cutoff is not None:
            raise ValueError("Cancellation requires one recorded boundary")
        with self.transaction():
            if boundary is not None:
                if (
                    not isinstance(boundary, dict)
                    or set(boundary) != {"key", "thread_id", "admission_order"}
                    or type(boundary.get("admission_order")) is not int
                    or boundary["admission_order"] < 0
                ):
                    raise ValueError("Cancellation boundary must be a captured receipt")
                receipt = self.get_stop_boundary(actor, boundary.get("key"))
                if receipt != boundary or receipt["thread_id"] != thread:
                    raise ValueError("Cancellation boundary receipt changed")
            elif cutoff is not None:
                receipt = self.legacy_stop_boundary(actor, thread, cutoff)
            else:
                receipt = self._record_stop_boundary(actor, thread, identifier())
            order = receipt["admission_order"]
            self.db.execute(
                "INSERT INTO origin_stops(actor,thread,cutoff,admission_order) VALUES(?,?,?,?) "
                "ON CONFLICT(actor,thread) DO UPDATE SET "
                "cutoff=MAX(cutoff,excluded.cutoff),"
                "admission_order=MAX(admission_order,excluded.admission_order)",
                (actor, thread, float(cutoff) if cutoff is not None else 0.0, order),
            )
            rows = self.db.execute(
                "SELECT id FROM projects WHERE owner=? AND origin_thread=? AND status='active' "
                "AND admission_order<=?",
                (actor, thread, order),
            ).fetchall()
            return [self.cancel(row[0], actor) for row in rows]

    def mention(
        self, actor, channel, thread, recipient, body, key, *, post_created_at=None, project_id=None
    ):
        with self.transaction():
            projects = [
                json.loads(raw)
                for (raw,) in self.db.execute(
                    "SELECT data FROM projects WHERE channel=? AND thread=?", (channel, thread)
                )
            ]
            if not projects:
                # A recorded creation remains project-owned after cancellation
                # or failed finalization. Match the root without decoding history.
                if self.db.execute(
                    "SELECT 1 FROM creations WHERE channel=? AND thread=? LIMIT 1",
                    (channel, thread),
                ).fetchone():
                    raise PermissionError("Project discussion creation requires reconciliation")
                return None
            generations = len(projects)
            projects = [
                project
                for project in projects
                if actor in project["members"] and recipient in project["members"]
            ]
            if not projects:
                raise PermissionError("Both actors must participate in the project discussion")
            request = {"recipient": recipient, "kind": "ask", "text": body, "key": key}
            for project in projects:
                if self.db.execute(
                    "SELECT 1 FROM messages WHERE project=? AND sender=? AND key=?",
                    (project["id"], actor, key),
                ).fetchone():
                    return self.send(project["id"], actor, request)
            if project_id is not None:
                selected = next((p for p in projects if p["id"] == project_id), None)
                if selected is None:
                    raise PermissionError(
                        "Mention project does not match this discussion generation"
                    )
            elif generations > 1:
                raise PermissionError(
                    "Mention source predates or lacks this project discussion generation; "
                    "use ASK [project_id]: on a reused discussion"
                )
            else:
                selected = projects[0]
            # Compare only Mattermost-side timestamps. Explicit project IDs
            # distinguish generations when a discussion root is reused.
            if type(post_created_at) is not int or post_created_at <= 0:
                raise PermissionError("Mention source creation time could not be verified")
            if post_created_at < selected.get("discussion_created_at", 0):
                raise PermissionError("Mention source predates this project discussion generation")
            return self.send(selected["id"], actor, request)
