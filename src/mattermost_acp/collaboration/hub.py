"""Optional loopback coordination service, independent of native inference and MCP."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import re
import secrets
from pathlib import Path

import aiohttp
from aiohttp import web

from mattermost_acp.collaboration.store import CollaborationStore


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
            return web.json_response(store.list_projects(actor))
        body = await request.json()
        body["owner_in_discussion"] = bool(actors[actor].get("sidecar"))
        return web.json_response(store.create_project(actor, body))

    async def creations(request):
        actor = request["actor"]
        body = await request.json()
        body["owner_in_discussion"] = bool(actors[actor].get("sidecar"))
        async with creation_lock:
            intent, fresh = store.prepare_creation(actor, body)
            explicit = body.get("thread_id")
            if explicit:
                if intent.get("thread_id") and intent["thread_id"] != explicit:
                    raise ValueError("Conflicting project discussion for creation key")
                intent["thread_id"] = explicit
                store.save_creation(actor, body["key"], intent)
            if not intent.get("thread_id"):
                if not fresh:
                    raise ValueError(
                        "Discussion posting is unconfirmed; reconcile with --discussion-thread"
                    )
                settings = actors[actor]
                if create_post is not None:
                    post = await create_post(actor, body)
                else:
                    base = settings.get("mattermost_url")
                    path = settings.get("mattermost_token_file")
                    if not base or not path:
                        raise ValueError("Mattermost posting configuration required")
                    token = Path(path).expanduser().read_text().strip()
                    async with aiohttp.ClientSession(
                        headers={"Authorization": "Bearer " + token},
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as session:
                        message = (
                            "Project collaboration: " + body["title"] + "\n\n" + body["objective"]
                        ).replace("@", "@\u200b")
                        async with session.post(
                            base.rstrip("/") + "/api/v4/posts",
                            json={"channel_id": body["channel_id"], "message": message},
                        ) as response:
                            if response.status >= 400:
                                raise ValueError("Discussion creation was not confirmed")
                            post = await response.json()
                if post.get("channel_id") != body["channel_id"] or not post.get("id"):
                    raise ValueError("Discussion creation was not confirmed")
                intent["thread_id"] = post["id"]
                intent["status"] = "posted"
                store.save_creation(actor, body["key"], intent)
            settings = actors[actor]
            if verify_creation is not None:
                verified = await verify_creation(actor, intent["thread_id"])
            elif settings.get("mattermost_url") and settings.get("mattermost_token_file"):
                token = Path(settings["mattermost_token_file"]).expanduser().read_text().strip()
                async with aiohttp.ClientSession(
                    headers={"Authorization": "Bearer " + token},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as session:
                    async with session.get(
                        settings["mattermost_url"].rstrip("/")
                        + "/api/v4/posts/"
                        + intent["thread_id"]
                    ) as response:
                        if response.status != 200:
                            raise ValueError("Discussion read-back failed")
                        verified = await response.json()
            else:
                verified = {"id": intent["thread_id"], "channel_id": body["channel_id"]}
            if (
                verified.get("id") != intent["thread_id"]
                or verified.get("channel_id") != body["channel_id"]
                or verified.get("root_id")
            ):
                raise ValueError("Discussion read-back did not match its root binding")
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
        return web.json_response(result)

    async def mailbox(request):
        instance = request.query.get("instance", "")
        if not instance or len(instance) > 128:
            raise ValueError("Worker instance required")
        wait = min(20, max(0, float(request.query.get("wait", "20"))))
        deadline = asyncio.get_running_loop().time() + wait
        while True:
            changed.clear()
            item = store.claim_next(
                request["actor"], instance, controls_only=request.query.get("controls_only") == "1"
            )
            if item is not None or asyncio.get_running_loop().time() >= deadline:
                return web.json_response(item)
            try:
                # Deterministic lease/deadline checks; no model is polled or woken.
                await asyncio.wait_for(changed.wait(), timeout=min(1, wait))
            except TimeoutError:
                pass

    async def message(request):
        return web.json_response(store.get_message(request.match_info["mid"], request["actor"]))

    async def cancel_thread(request):
        body = await request.json()
        return web.json_response(store.cancel_thread(request["actor"], body["thread_id"]))

    async def mention(request):
        body = await request.json()
        sender = next(
            (
                name
                for name, settings in actors.items()
                if settings.get("user_id") and settings["user_id"] == body.get("author_id")
            ),
            None,
        )
        if sender is None or sender == request["actor"]:
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
                return web.json_response(None)
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
        if not re.match(
            r"^\s*@" + re.escape(recipient["username"]) + r"\s+ASK:\s+\S",
            verified.get("message", ""),
            re.IGNORECASE,
        ):
            raise PermissionError("Mention must explicitly address this actor with ASK:")
        return web.json_response(
            store.mention(
                sender,
                body["channel_id"],
                body["thread_id"],
                request["actor"],
                body["text"],
                "post:" + body["post_id"],
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
    app.router.add_get("/projects/{pid}", show)
    app.router.add_post("/projects/{pid}/{action}", project_action)
    app.router.add_get("/messages/{mid}", message)
    app.router.add_post("/messages/{mid}/{action}", message_action)
    app.router.add_get("/mailbox", mailbox)
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
    args.db.parent.mkdir(parents=True, exist_ok=True)
    with args.db.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        store = CollaborationStore(args.db, set(actors))
        try:
            web.run_app(build_app(store, actors), host="127.0.0.1", port=args.port, access_log=None)
        finally:
            store.close()


if __name__ == "__main__":
    main()
