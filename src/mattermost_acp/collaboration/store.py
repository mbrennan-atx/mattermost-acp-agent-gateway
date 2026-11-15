"""Transactional project state. Requests survive transport and native-process failures."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

ACTIVE = {"running", "unconfirmed", "cancelling"}
TERMINAL = {"done", "failed", "blocked", "cancelled"}
WORK = {"ask", "work"}


def identifier() -> str:
    return uuid.uuid4().hex


def text(value, name="text", maximum=16000) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be a nonempty string of at most {maximum} characters")
    return value.strip()


class CollaborationStore:
    def __init__(self, path: Path, actors: set[str], *, clock=time.time):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=10)
        path.chmod(0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.actors = actors
        self.clock = clock
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS projects(id TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS messages(
                id TEXT PRIMARY KEY, project TEXT NOT NULL, sender TEXT NOT NULL,
                recipient TEXT, kind TEXT NOT NULL, state TEXT NOT NULL,
                created REAL NOT NULL, key TEXT NOT NULL, data TEXT NOT NULL,
                UNIQUE(project, sender, key));
            CREATE INDEX IF NOT EXISTS mailbox ON messages(recipient,state,created);
            CREATE TABLE IF NOT EXISTS notes(id TEXT PRIMARY KEY, project TEXT, data TEXT);
            CREATE TABLE IF NOT EXISTS creations(actor TEXT, key TEXT, data TEXT,
                PRIMARY KEY(actor,key));
        """)
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

    def save_project(self, item):
        self.db.execute(
            "INSERT OR REPLACE INTO projects VALUES(?,?)", (item["id"], json.dumps(item))
        )

    def save_message(self, item):
        self.db.execute(
            """INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
            project=excluded.project,sender=excluded.sender,recipient=excluded.recipient,
            kind=excluded.kind,state=excluded.state,created=excluded.created,
            key=excluded.key,data=excluded.data""",
            (
                item["id"],
                item["project"],
                item["sender"],
                item.get("recipient"),
                item["kind"],
                item["state"],
                item["created"],
                item["key"],
                json.dumps(item),
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
        row = self.db.execute("SELECT data FROM messages WHERE id=?", (mid,)).fetchone()
        if row is None:
            raise KeyError("Unknown message")
        item = json.loads(row[0])
        self.get_project(item["project"], actor)
        return item

    def prepare_creation(self, actor, body):
        """Persist intent before external posting; uncertain posts need reconciliation."""
        key = text(body.get("key"), "project idempotency key", 200)
        canonical = {k: v for k, v in body.items() if k != "thread_id"}
        canonical.setdefault("owner_in_discussion", False)
        with self.transaction():
            row = self.db.execute(
                "SELECT data FROM creations WHERE actor=? AND key=?", (actor, key)
            ).fetchone()
            if row:
                intent = json.loads(row[0])
                intent["body"].setdefault("owner_in_discussion", False)
                if intent["body"] != canonical:
                    raise ValueError("Conflicting project creation key")
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
            intent = {"body": canonical, "thread_id": body.get("thread_id"), "status": "reserved"}
            self.save_creation(actor, key, intent)
            return intent, True

    def save_creation(self, actor, key, intent):
        with self.transaction():
            self.db.execute(
                "INSERT OR REPLACE INTO creations VALUES(?,?,?)", (actor, key, json.dumps(intent))
            )

    def finish_creation(self, actor, intent):
        with self.transaction():
            project = self.create_project(
                actor, {**intent["body"], "thread_id": intent["thread_id"]}
            )
            intent["status"] = "complete"
            self.save_creation(actor, intent["body"]["key"], intent)
            return project

    def create_project(self, actor, body):
        body = {"owner_in_discussion": False, **body}
        key = body.get("key")
        fingerprint = hashlib.sha256(
            json.dumps({k: v for k, v in body.items() if k != "key"}, sort_keys=True).encode()
        ).hexdigest()
        if key:
            text(key, "project idempotency key", 200)
            for existing in self.list_projects(actor):
                if existing["owner"] == actor and existing.get("key") == key:
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
            "created": self.clock(),
            "limits": limits,
            "deadline": self.clock() + limits["max_seconds"],
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
            self.save_project(item)
        return item

    def check_binding(self, project, actor, binding):
        for other in self.list_projects(actor):
            if other["id"] == project["id"] or other["status"] == "cancelled":
                continue
            existing = other["bindings"][actor]
            if existing["thread_id"] != binding["thread_id"]:
                continue
            # Results may return to the same owner's conversation. Worker sessions
            # must remain exclusive, including sessions awaiting reconciliation.
            if project["owner"] == actor == other["owner"]:
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
            json.loads(row[0])
            for row in self.db.execute(
                "SELECT data FROM messages WHERE project=? ORDER BY created,id", (pid,)
            )
        ]
        item["notes"] = [
            json.loads(row[0])
            for row in self.db.execute("SELECT data FROM notes WHERE project=?", (pid,))
        ]
        return item

    def bind(self, pid, actor, body):
        with self.transaction():
            project = self.get_project(pid, actor)
            if self.db.execute(
                "SELECT 1 FROM messages WHERE project=? AND recipient=? "
                "AND state IN ('running','unconfirmed','interrupted','cancelling')",
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
            if project["status"] != "active" or project["deadline"] <= self.clock():
                raise ValueError("Project is inactive or expired")
            recipient = body.get("recipient")
            if recipient is not None and recipient not in project["members"]:
                raise PermissionError("Recipient is not a project participant")
            if recipient == actor:
                raise ValueError("Self-addressed work is not collaboration")
            parent_id = body.get("parent")
            if not parent_id:
                row = self.db.execute(
                    "SELECT id FROM messages WHERE project=? AND recipient=? "
                    "AND kind IN ('ask','work') AND state='running' "
                    "ORDER BY created DESC LIMIT 1",
                    (pid, actor),
                ).fetchone()
                parent_id = row[0] if row else None
            depth = 0
            if parent_id:
                parent = self.get_message(parent_id, actor)
                if parent["project"] != pid or parent["recipient"] != actor:
                    raise PermissionError("Delegation parent must be addressed to the sender")
                depth = parent["depth"] + 1
            if depth > project["limits"]["max_depth"]:
                raise ValueError("Project delegation depth limit reached")
            dependencies = body.get("dependencies", [])
            if not isinstance(dependencies, list) or len(dependencies) > 100:
                raise ValueError("dependencies must be a bounded list")
            for dependency in dependencies:
                dep = self.get_message(dependency, actor)
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

    def claim_open(self, mid, actor):
        with self.transaction():
            item = self.get_message(mid, actor)
            if item["state"] != "open" or item.get("recipient"):
                raise ValueError("Task is already claimed")
            if item["sender"] == actor:
                raise ValueError("Claim a peer's task, not your own")
            project = self.get_project(item["project"], actor)
            if project["status"] != "active":
                raise ValueError("Project is inactive")
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
            for row in self.db.execute("SELECT data FROM projects"):
                project = json.loads(row[0])
                if project["status"] == "active" and project["deadline"] <= self.clock():
                    self.cancel(project["id"], project["owner"])
            for row in self.db.execute("SELECT data FROM messages WHERE state='running'"):
                item = json.loads(row[0])
                if item.get("lease_until", 0) <= self.clock():
                    item["state"] = "queued" if item["kind"] == "cancel" else "interrupted"
                    item["recovery"] = "Worker lease expired; reconcile before retry"
                    self.save_message(item)

    def claim_next(self, actor, instance, *, controls_only=False):
        with self.transaction():
            self.expire()
            rows = self.db.execute(
                "SELECT data FROM messages WHERE recipient=? AND state='queued' "
                + ("AND kind='cancel' " if controls_only else "")
                + "ORDER BY CASE kind WHEN 'cancel' THEN 0 WHEN 'result' THEN 1 "
                "ELSE 2 END,created,id",
                (actor,),
            ).fetchall()
            for row in rows:
                item = json.loads(row[0])
                project = self.get_project(item["project"], actor)
                if item.get("not_before", 0) > self.clock():
                    continue
                if item["kind"] in WORK:
                    if project["status"] != "active":
                        continue
                    if self.db.execute(
                        "SELECT 1 FROM messages WHERE project=? AND recipient=? "
                        "AND state IN ('unconfirmed','interrupted')",
                        (project["id"], actor),
                    ).fetchone():
                        continue
                    if any(
                        self.get_message(dep, actor)["state"] != "done"
                        for dep in item["dependencies"]
                    ):
                        continue
                    active = self.db.execute(
                        "SELECT count(*) FROM messages WHERE project=? "
                        "AND kind IN ('ask','work') AND state IN "
                        "('running','unconfirmed','interrupted','cancelling')",
                        (project["id"],),
                    ).fetchone()[0]
                    if active >= project["limits"]["max_workers"]:
                        # A parent waiting for a delegated result cannot release
                        # its slot yet. Loan one slot per active parent, bounded
                        # by depth and a hard total ceiling. Siblings serialize.
                        parent = (
                            self.get_message(item["parent"], actor) if item.get("parent") else None
                        )
                        sibling = self.db.execute(
                            "SELECT data FROM messages WHERE project=? AND kind IN ('ask','work') "
                            "AND state IN ('running','unconfirmed','interrupted','cancelling')",
                            (project["id"],),
                        ).fetchall()
                        if (
                            not parent
                            or parent["state"] != "running"
                            or any(
                                json.loads(row[0]).get("parent") == parent["id"] for row in sibling
                            )
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
                            "AND kind IN ('ask','work') AND state IN "
                            "('running','unconfirmed','interrupted','cancelling')",
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
                return {
                    **item,
                    "project_title": project["title"],
                    "objective": project["objective"],
                    "project_owner": project["owner"],
                    "origin_thread_id": project["origin_thread_id"],
                    "origin_channel_id": project["origin_channel_id"],
                }
            return None

    def update(self, mid, actor, instance, state, result="", evidence=None, claim=None):
        with self.transaction():
            if isinstance(result, str):
                result = result.strip()
            item = self.get_message(mid, actor)
            if (
                item["recipient"] != actor
                or item.get("instance") != instance
                or (item.get("attempt", 0) > 1 and claim != item.get("claim"))
                or (claim is not None and claim != item.get("claim"))
            ):
                raise PermissionError("Stale or foreign worker instance")
            if state == "evidence":
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
                        if delivery["state"] == "queued":
                            delivery["evidence"].update(evidence or {})
                        self.save_message(delivery)
                return item
            if item["state"] in TERMINAL:
                if state == item["state"] and result == item.get("result", ""):
                    return item
                raise ValueError("Message already settled")
            if item["state"] not in ACTIVE:
                raise PermissionError("Delivery is no longer owned by this worker")
            if state == "dispatch":
                project = self.get_project(item["project"], actor)
                if item["state"] != "running" or project["status"] != "active":
                    return item
                item["evidence"].update(evidence or {})
                item["evidence"]["dispatch_started"] = self.clock()
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
            elif state == "queued" and item["state"] == "running":
                item.setdefault("delivery_history", []).append(item["evidence"])
                item["evidence"] = dict(item.get("result_evidence", {}))
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
                            evidence=evidence,
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
                    return self.reply(
                        mid, actor, result, state=state, evidence=evidence, claim=claim
                    )
                item.update(
                    state=state, result=result, evidence=evidence or {}, completed=self.clock()
                )
            else:
                raise ValueError("Invalid delivery transition")
            self.save_message(item)
            self.finish_cancellation(item["project"], actor)
            return item

    def reply(self, mid, actor, result, *, state="done", evidence=None, claim=None):
        with self.transaction():
            item = self.get_message(mid, actor)
            if item["recipient"] != actor or item["kind"] not in WORK:
                raise PermissionError("Only the addressed worker can reply")
            if (item.get("attempt", 0) > 1 and claim != item.get("claim")) or (
                claim is not None and claim != item.get("claim")
            ):
                raise PermissionError("Stale reply claim")
            result = text(result, "result")
            evidence = {**item["evidence"], **(evidence or {})}
            if state not in {"done", "failed", "blocked"}:
                raise ValueError("Invalid result state")
            if item["state"] in TERMINAL:
                if item.get("result") == result and item["state"] == state:
                    return item
                raise ValueError("Conflicting repeated result")
            if item["state"] == "cancelling":
                if "late_state" in item:
                    if item["late_result"] != result or item["late_state"] != state:
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
                "evidence": evidence,
                "result_evidence": evidence,
                "result_state": state,
            }
            self.save_message(delivery)
            return item

    def retry(self, mid, actor, reconciliation):
        with self.transaction():
            item = self.get_message(mid, actor)
            project = self.get_project(item["project"], actor)
            if actor not in {project["owner"], item["recipient"]}:
                raise PermissionError("Only owner or recipient can reconcile delivery")
            if item["state"] not in {"interrupted", "unconfirmed"}:
                raise ValueError("Only interrupted/unconfirmed delivery can be retried")
            if project["status"] != "active" or project["deadline"] <= self.clock():
                raise ValueError("Project is inactive")
            if project["dispatched"] >= project["limits"]["max_turns"]:
                raise ValueError("Project dispatch limit reached")
            project["dispatched"] += 1
            self.save_project(project)
            item.setdefault("delivery_history", []).append(item["evidence"])
            item["evidence"] = dict(item.get("result_evidence", {}))
            item.update(
                state="queued",
                reconciliation=text(reconciliation, "reconciliation"),
                instance=None,
                not_before=0,
            )
            self.save_message(item)
            return item

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
                if item["state"] in {"queued", "open"} or (
                    item["state"] not in TERMINAL
                    and (item["kind"] == "result" or item["recipient"] == project["owner"])
                ):
                    # Stop routing into the owner's conversation without stopping
                    # its existing work. Origin /stop separately cancels that turn.
                    item["state"] = "cancelled"
                    self.save_message(item)
                elif item["state"] in {"running", "unconfirmed", "interrupted"}:
                    item["state"] = "cancelling"
                    self.save_message(item)
                    control = {
                        "id": identifier(),
                        "project": pid,
                        "sender": actor,
                        "recipient": item["recipient"],
                        "kind": "cancel",
                        "text": "Cancel project work",
                        "parent": item["id"],
                        "dependencies": [],
                        "depth": 0,
                        "key": "cancel:" + item["id"],
                        "state": "queued",
                        "created": self.clock(),
                        "attempt": 0,
                        "evidence": {},
                    }
                    self.save_message(control)
            self.finish_cancellation(pid, actor)
            return self.get_project(pid, actor)

    def finish_cancellation(self, pid, actor):
        project = self.get_project(pid, actor)
        if (
            project["status"] == "cancelling"
            and not self.db.execute(
                "SELECT 1 FROM messages WHERE project=? AND state='cancelling'", (pid,)
            ).fetchone()
        ):
            project["status"] = "cancelled"
            self.save_project(project)

    def cancel_thread(self, actor, thread):
        return [
            self.cancel(p["id"], actor)
            for p in self.list_projects(actor)
            if p["owner"] == actor and p["origin_thread_id"] == thread and p["status"] == "active"
        ]

    def mention(self, actor, channel, thread, recipient, body, key):
        for project in self.list_projects(actor):
            if (
                project["channel_id"] == channel
                and project["thread_id"] == thread
                and project["status"] == "active"
            ):
                return self.send(
                    project["id"],
                    actor,
                    {"recipient": recipient, "kind": "ask", "text": body, "key": key},
                )
        return None
