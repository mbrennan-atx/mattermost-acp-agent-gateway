"""Optional loopback coordination service, independent of native inference and MCP."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import re
import secrets
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web

from mattermost_acp.collaboration.store import CollaborationStore, elapsed_clock
from mattermost_acp.mm.chunk import DEFAULT_LIMIT


def load_actors(path: Path) -> dict:
    config = json.loads(path.read_text())
    actors = config.get("actors", {})
    if not actors or not isinstance(actors, dict):
        raise ValueError("Collaboration configuration requires actors")
    loaded = {}
    for name, settings in actors.items():
        token = Path(settings["token_file"]).expanduser().read_text().strip()
        if len(token) < 32:
            raise ValueError("Actor credentials must contain at least 32 characters")
        if token in [v["token"] for v in loaded.values()]:
            raise ValueError("Actors require distinct credentials")
        loaded[name] = {
            "token": token,
            "user_id": settings.get("user_id", ""),
            "mattermost_url": config.get("mattermost_url", ""),
            "mattermost_token_file": settings.get("mattermost_token_file", ""),
            "sidecar": bool(settings.get("sidecar", False)),
        }
    return loaded


def build_app(
    store: CollaborationStore,
    actors: dict,
    *,
    verify_post=None,
    verify_recipient=None,
    create_post=None,
    verify_creation=None,
) -> web.Application:
    changed = asyncio.Event()
    admission_fences: dict[tuple[str, str], str] = {}
    store.sidecar_actors = {name for name, settings in actors.items() if settings.get("sidecar")}
    creation_lock = asyncio.Lock()

    @web.middleware
    async def authorize(request, handler):
        if request.path == "/healthz":
            return await handler(request)
        bearer = request.headers.get("Authorization", "")
        token = bearer.removeprefix("Bearer ") if bearer.startswith("Bearer ") else ""
        actor = next(
            (
                name
                for name, settings in actors.items()
                if token and secrets.compare_digest(token.encode(), settings["token"].encode())
            ),
            None,
        )
        if actor is None:
            raise web.HTTPUnauthorized(text="Actor credential required")
        request["actor"] = actor
        try:
            result = await handler(request)
        except PermissionError as exc:
            return web.json_response({"error": str(exc)}, status=403)
        except KeyError:
            return web.json_response({"error": "Unknown project or request"}, status=404)
        except (ValueError, TypeError):
            return web.json_response(
                {"error": "Invalid or conflicting collaboration request"}, status=409
            )
        if request.method == "POST":
            changed.set()
        return result

    app = web.Application(middlewares=[authorize], client_max_size=262144)

    async def projects(request):
        actor = request["actor"]
        if request.method == "GET":
            store.expire()
            return web.json_response(store.list_projects(actor))
        body = await request.json()
        body["owner_in_discussion"] = bool(actors[actor].get("sidecar"))
        return web.json_response(store.create_project(actor, body))

    async def creation_evidence(request):
        return web.json_response(
            store.creation_evidence(request["actor"], request.match_info["key"])
        )

    async def project_budget(request):
        store.expire()
        return web.json_response(store.project_budget(request.match_info["pid"], request["actor"]))

    async def read_creation_root(actor, thread, *, fallback_channel=None):
        if verify_creation is not None:
            return await verify_creation(actor, thread)
        settings = actors[actor]
        if settings.get("mattermost_url") and settings.get("mattermost_token_file"):
            token = Path(settings["mattermost_token_file"]).expanduser().read_text().strip()
            async with aiohttp.ClientSession(
                headers={"Authorization": "Bearer " + token},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as session:
                async with session.get(
                    settings["mattermost_url"].rstrip("/") + "/api/v4/posts/" + thread
                ) as response:
                    if response.status != 200:
                        raise PermissionError("Discussion root could not be independently verified")
                    return await response.json()
        if fallback_channel is not None:
            return {"id": thread, "channel_id": fallback_channel}
        raise PermissionError("Discussion root identity verifier is required")

    def verify_correlated_root(root, actor, intent, thread):
        if not isinstance(root, dict):
            raise PermissionError("Discussion root identity could not be verified")
        props = root.get("props") or {}
        if (
            root.get("id") != thread
            or root.get("channel_id") != intent["body"]["channel_id"]
            or root.get("root_id")
            or not actors[actor].get("user_id")
            or root.get("user_id") != actors[actor]["user_id"]
            or not isinstance(props, dict)
            or props.get("mattermost_acp_creation_id") != intent.get("creation_id")
            or props.get("mattermost_acp_creation_actor") != actor
        ):
            raise PermissionError("Discussion root does not match its creation identity")

    async def guard_unconfirmed_root(actor, body):
        if store.known_discussion(body["channel_id"], body["thread_id"]):
            return  # Existing project admission retains this discussion's ownership.
        candidates = store.unresolved_creations(body["channel_id"])
        if not candidates:
            return
        root = await read_creation_root(actor, body["thread_id"])
        if (
            not isinstance(root, dict)
            or root.get("id") != body["thread_id"]
            or root.get("channel_id") != body["channel_id"]
            or root.get("root_id")
        ):
            raise PermissionError("Discussion root does not match the real root binding")
        props = root.get("props") or {}
        if not isinstance(props, dict):
            raise PermissionError("Discussion root metadata could not be verified")
        if not {"mattermost_acp_creation_id", "mattermost_acp_creation_actor"} & props.keys():
            return  # Independently verified unrelated roots keep ordinary admission.
        for owner, key, intent in candidates:
            if props.get("mattermost_acp_creation_id") != intent["creation_id"]:
                continue
            verify_correlated_root(root, owner, intent, body["thread_id"])
            store.record_creation_root(
                owner, key, body["thread_id"], creation_id=intent["creation_id"]
            )
            raise PermissionError("Project discussion creation requires reconciliation")
        raise PermissionError("Discussion root has unknown creation correlation")

    async def creations(request):
        actor = request["actor"]
        body = await request.json()
        body["owner_in_discussion"] = bool(actors[actor].get("sidecar"))
        async with creation_lock:
            posting = None
            if not body.get("thread_id") and create_post is None:
                try:
                    retained = store.get_creation(actor, body.get("key"))
                except KeyError:
                    retained = None
                if retained is None or not retained.get("thread_id"):
                    # A configuration failure proves no POST occurred. It must
                    # not reserve an uncertain root or fence unrelated replies.
                    settings = actors[actor]
                    base = settings.get("mattermost_url")
                    path = settings.get("mattermost_token_file")
                    try:
                        parsed = urlsplit(base or "")
                        valid_port = parsed.port is None or 0 < parsed.port <= 65535
                        if (
                            parsed.scheme not in {"http", "https"}
                            or not parsed.hostname
                            or not valid_port
                            or not path
                            or not settings.get("user_id")
                        ):
                            raise ValueError("Mattermost posting configuration required")
                        token = Path(path).expanduser().read_text().strip()
                        if not token:
                            raise ValueError("Mattermost posting credential required")
                    except OSError as exc:
                        raise ValueError("Mattermost posting credential unavailable") from exc
                    posting = base, token
            intent, fresh = store.prepare_creation(actor, body)
            explicit = body.get("thread_id")
            if explicit:
                if not intent.get("thread_id") and intent.get("creation_id"):
                    root = await read_creation_root(actor, explicit)
                    verify_correlated_root(root, actor, intent, explicit)
                if intent.get("thread_id") and intent["thread_id"] != explicit:
                    raise ValueError("Conflicting project discussion for creation key")
                intent = store.record_creation_root(actor, body["key"], explicit)
            if not intent.get("thread_id"):
                if not fresh:
                    raise ValueError(
                        "Discussion posting is unconfirmed; reconcile with --discussion-thread"
                    )
                props = {
                    "mattermost_acp_creation_id": intent["creation_id"],
                    "mattermost_acp_creation_actor": actor,
                }
                if create_post is not None:
                    post = await create_post(actor, {**body, "props": props})
                else:
                    base, token = posting
                    async with aiohttp.ClientSession(
                        headers={"Authorization": "Bearer " + token},
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as session:
                        message = (
                            "Project collaboration: " + body["title"] + "\n\n" + body["objective"]
                        ).replace("@", "@\u200b")
                        if len(message) > DEFAULT_LIMIT:
                            suffix = "\n\nFull objective is saved in the project record."
                            message = message[: DEFAULT_LIMIT - len(suffix)].removesuffix("@")
                            message += suffix
                        try:
                            async with session.post(
                                base.rstrip("/") + "/api/v4/posts",
                                json={
                                    "channel_id": body["channel_id"],
                                    "message": message,
                                    "props": props,
                                },
                            ) as response:
                                if response.status in {401, 403}:
                                    store.reject_creation(actor, body["key"], intent["creation_id"])
                                if response.status >= 400:
                                    raise ValueError("Discussion creation was not confirmed")
                                post = await response.json()
                        except (TimeoutError, aiohttp.ClientError) as exc:
                            raise ValueError("Discussion creation was not confirmed") from exc
                if post.get("channel_id") != body["channel_id"] or not post.get("id"):
                    raise ValueError("Discussion creation was not confirmed")
                intent = store.record_creation_root(
                    actor, body["key"], post["id"], creation_id=intent["creation_id"]
                )
            verified = await read_creation_root(
                actor, intent["thread_id"], fallback_channel=body["channel_id"]
            )
            if (
                verified.get("id") != intent["thread_id"]
                or verified.get("channel_id") != body["channel_id"]
                or verified.get("root_id")
            ):
                raise ValueError("Discussion read-back did not match its root binding")
            if intent.get("creation_id"):
                verify_correlated_root(verified, actor, intent, intent["thread_id"])
            created_at = verified.get("create_at")
            if type(created_at) is int and created_at > 0:
                intent["discussion_created_at"] = created_at
            return web.json_response(store.finish_creation(actor, intent))

    async def show(request):
        store.expire()
        return web.json_response(store.show(request.match_info["pid"], request["actor"]))

    async def project_action(request):
        pid, actor = request.match_info["pid"], request["actor"]
        body = await request.json()
        action = request.match_info["action"]
        if action == "messages":
            result = store.send(pid, actor, body)
        elif action == "bindings":
            result = store.bind(pid, actor, body)
        elif action == "notes":
            result = store.note(
                pid, actor, body.get("text"), body.get("artifacts", []), body.get("key")
            )
        elif action == "cancel":
            result = store.cancel(pid, actor)
        else:
            raise KeyError("Unknown action")
        return web.json_response(result)

    async def message_action(request):
        mid, actor = request.match_info["mid"], request["actor"]
        body = await request.json()
        action = request.match_info["action"]
        if action == "update":
            result = store.update(
                mid,
                actor,
                body["instance"],
                body["state"],
                body.get("result", ""),
                body.get("evidence"),
                body.get("claim"),
                expected_execution_id=body.get("expected_execution_id"),
                include_history=False,
            )
        elif action == "reply":
            result = store.reply(
                mid,
                actor,
                body["result"],
                state=body.get("state", "done"),
                evidence=body.get("evidence"),
                claim=body.get("claim"),
            )
        elif action == "claim":
            result = store.claim_open(mid, actor)
        elif action == "retry":
            result = store.retry(mid, actor, body["reconciliation"])
        else:
            raise KeyError("Unknown action")
        return web.json_response(store.deadline_receipt(result, actor))

    async def mailbox_admission(request):
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError("Mailbox admission settings required")
        instance, mode = body.get("instance"), body.get("mode")
        if not isinstance(instance, str) or not instance or len(instance) > 128:
            raise ValueError("Worker instance required")
        if not isinstance(mode, str) or mode not in {"normal", "controls", "sealed"}:
            raise ValueError("Mailbox admission mode required")
        key = (request["actor"], instance)
        if mode == "normal":
            admission_fences.pop(key, None)
        else:
            admission_fences[key] = mode
        claims = store.active_claims(*key)
        changed.set()
        return web.json_response(
            [store.deadline_receipt(item, request["actor"]) for item in claims]
        )

    async def claims(request):
        # Recovery inspection spans this actor's instances without changing
        # leases, admission fences or the ownership of another instance.
        return web.json_response(
            [
                store.deadline_receipt(item, request["actor"])
                for item in store.active_claims(request["actor"])
            ]
        )

    async def mailbox(request):
        started = elapsed_clock()
        instance = request.query.get("instance", "")
        if not instance or len(instance) > 128:
            raise ValueError("Worker instance required")
        wait = min(20, max(0, float(request.query.get("wait", "20"))))
        deadline = asyncio.get_running_loop().time() + wait
        controls_only = request.query.get("controls_only") == "1"
        while True:
            changed.clear()
            mode = admission_fences.get((request["actor"], instance), "normal")
            if mode == "sealed":
                return web.json_response(None)
            # Measure only waiting before the budget snapshot, so response
            # serialization cannot extend the local project deadline.
            waited = max(0, elapsed_clock() - started)
            item = store.claim_next(
                request["actor"], instance, controls_only=controls_only or mode == "controls"
            )
            if (
                item is not None
                or asyncio.get_running_loop().time() >= deadline
                or (mode == "controls" and not controls_only)
            ):
                if item is not None:
                    item["project_mailbox_wait_seconds"] = waited
                return web.json_response(item)
            try:
                # Deterministic lease/deadline checks; no model is polled or woken.
                await asyncio.wait_for(changed.wait(), timeout=min(1, wait))
            except TimeoutError:
                pass

    async def message(request):
        store.expire()
        read = store.get_message if request.query.get("history") == "1" else store.message_record
        return web.json_response(
            store.deadline_receipt(
                read(request.match_info["mid"], request["actor"]), request["actor"]
            )
        )

    async def stop_boundaries(request):
        if request.method == "GET":
            return web.json_response(
                store.get_stop_boundary(request["actor"], request.match_info["key"])
            )
        body = await request.json()
        return web.json_response(
            store.capture_stop_boundary(request["actor"], body["thread_id"], body["key"])
        )

    async def cancel_thread(request):
        body = await request.json()
        if "cutoff" in body and type(body["cutoff"]) not in {int, float}:
            raise ValueError("Cancellation cutoff must be a finite timestamp")
        if "boundary" in body and not isinstance(body["boundary"], dict):
            raise ValueError("Cancellation boundary must be a captured receipt")
        return web.json_response(
            store.cancel_thread(
                request["actor"],
                body["thread_id"],
                cutoff=body.get("cutoff"),
                boundary=body.get("boundary"),
            )
        )

    def ordinary_mention_fallback(actor, body):
        # Unverified callers can fall back only outside registered discussions.
        store.mention(
            None,
            body["channel_id"],
            body["thread_id"],
            actor,
            body["text"],
            "post:" + body["post_id"],
        )
        return web.json_response(None)

    async def mention(request):
        body = await request.json()
        await guard_unconfirmed_root(request["actor"], body)
        sender = next(
            (
                name
                for name, settings in actors.items()
                if settings.get("user_id") and settings["user_id"] == body.get("author_id")
            ),
            None,
        )
        if sender is None:
            return ordinary_mention_fallback(request["actor"], body)
        if sender == request["actor"]:
            return web.json_response(None)
        if verify_post is not None:
            verified = await verify_post(body["post_id"], request["actor"])
            if verify_recipient is None:
                raise PermissionError("Recipient identity verifier is required")
            recipient = await verify_recipient(request["actor"])
        else:
            settings = actors[request["actor"]]
            base = settings.get("mattermost_url")
            token_path = settings.get("mattermost_token_file")
            if not base or not token_path:
                return ordinary_mention_fallback(request["actor"], body)
            token = Path(token_path).expanduser().read_text().strip()
            async with aiohttp.ClientSession(
                headers={"Authorization": "Bearer " + token},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as session:
                async with session.get(
                    base.rstrip("/") + "/api/v4/posts/" + body["post_id"]
                ) as resp:
                    if resp.status != 200:
                        raise PermissionError("Mention source could not be verified")
                    verified = await resp.json()
                async with session.get(
                    base.rstrip("/") + "/api/v4/users/" + settings["user_id"]
                ) as resp:
                    if resp.status != 200:
                        raise PermissionError("Recipient identity could not be verified")
                    recipient = await resp.json()
        if any(
            verified.get(source) != body.get(supplied)
            for source, supplied in (
                ("user_id", "author_id"),
                ("channel_id", "channel_id"),
                ("root_id", "thread_id"),
                ("message", "text"),
            )
        ):
            raise PermissionError("Mention source does not match the real post")
        if recipient.get("id") != actors[request["actor"]]["user_id"] or not recipient.get(
            "username"
        ):
            raise PermissionError("Recipient identity does not match the authenticated actor")
        addressed = re.match(
            r"^\s*@" + re.escape(recipient["username"]) + r"\s+ASK(?:\s+\[([0-9a-f]{32})\])?:\s+\S",
            verified.get("message", ""),
            re.IGNORECASE,
        )
        if not addressed:
            raise PermissionError("Mention must explicitly address this actor with ASK:")
        return web.json_response(
            store.mention(
                sender,
                body["channel_id"],
                body["thread_id"],
                request["actor"],
                body["text"],
                "post:" + body["post_id"],
                post_created_at=verified.get("create_at"),
                project_id=addressed.group(1).lower() if addressed.group(1) else None,
            )
        )

    async def health(_request):
        return web.json_response({"status": "ok"})

    async def directory(_request):
        return web.json_response(sorted(actors))

    async def identity(request):
        return web.json_response(
            {
                "actor": request["actor"],
                "user_id": actors[request["actor"]].get("user_id", ""),
                "sidecar": bool(actors[request["actor"]].get("sidecar")),
            }
        )

    app.router.add_get("/actors", directory)
    app.router.add_get("/identity", identity)
    app.router.add_get("/healthz", health)
    app.router.add_route("*", "/projects", projects)
    app.router.add_post("/creations", creations)
    app.router.add_get("/creations/{key}", creation_evidence)
    app.router.add_get("/projects/{pid}/budget", project_budget)
    app.router.add_get("/projects/{pid}", show)
    app.router.add_post("/projects/{pid}/{action}", project_action)
    app.router.add_get("/messages/{mid}", message)
    app.router.add_post("/messages/{mid}/{action}", message_action)
    app.router.add_get("/mailbox", mailbox)
    app.router.add_post("/mailbox/admission", mailbox_admission)
    app.router.add_get("/claims", claims)
    app.router.add_post("/stop-boundaries", stop_boundaries)
    app.router.add_get("/stop-boundaries/{key}", stop_boundaries)
    app.router.add_post("/cancel-thread", cancel_thread)
    app.router.add_post("/mentions", mention)
    return app


def main():
    parser = argparse.ArgumentParser(description="Local durable assistant collaboration hub")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--port", type=int, default=18780)
    args = parser.parse_args()
    actors = load_actors(args.config)
    args.db = args.db.expanduser().resolve()
    args.db.parent.mkdir(parents=True, exist_ok=True)
    with args.db.with_name(args.db.name + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # The path fence also covers creation; the inode fence covers hard-link
        # aliases before recovery can touch a running database.
        with os.fdopen(
            os.open(args.db, os.O_CREAT | os.O_RDWR | os.O_APPEND, 0o600), "a+b"
        ) as database:
            fcntl.flock(database, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if os.fstat(database.fileno()).st_nlink != 1:
                raise ValueError("Collaboration databases must not have hard-link aliases")
            store = CollaborationStore(args.db, set(actors))
            try:
                web.run_app(
                    build_app(store, actors), host="127.0.0.1", port=args.port, access_log=None
                )
            finally:
                store.close()


if __name__ == "__main__":
    main()
