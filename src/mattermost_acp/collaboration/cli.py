"""Small project-work CLI used by all harnesses through their native shell tool."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

import aiohttp

from mattermost_acp.collaboration.client import HubClient, Settings
from mattermost_acp.config import Config


def content(args):
    return Path(args.text_file).read_text() if args.text_file else args.text


async def create_discussion(args):
    config = Config.load()
    base, floor, token = (
        config.mattermost_url.rstrip("/"),
        config.peer_channel_id,
        config.mattermost_token,
    )
    if not base or not floor or not token:
        raise ValueError(
            "Mattermost URL, floor and actor token file are required to create discussion"
        )
    async with aiohttp.ClientSession(
        headers={"Authorization": "Bearer " + token}, timeout=aiohttp.ClientTimeout(total=20)
    ) as session:
        body = {
            "channel_id": floor,
            "message": ("Project collaboration: " + args.title + "\n\n" + args.objective).replace(
                "@", "@\u200b"
            ),
        }
        async with session.post(base + "/api/v4/posts", json=body) as response:
            if response.status >= 400:
                raise ValueError("Mattermost discussion creation failed")
            post = await response.json()
        async with session.get(base + "/api/v4/posts/" + post["id"]) as response:
            verified = await response.json()
            if response.status >= 400 or verified.get("channel_id") != floor:
                raise ValueError("Mattermost discussion read-back failed")
        return floor, post["id"]


async def execute(args, hub):
    command = args.command
    if command == "list":
        return await hub.request("GET", "/projects")
    if command == "show":
        return await hub.request("GET", f"/projects/{args.project}")
    if command == "create":
        actor = (await hub.request("GET", "/identity"))["actor"]
        members = args.members.split(",") if args.members else await hub.request("GET", "/actors")
        body = {
            "title": args.title,
            "objective": args.objective,
            "members": members,
            "origin_thread_id": args.origin_thread,
            "origin_channel_id": args.origin_channel,
            "limits": {
                name: getattr(args, name)
                for name in ("max_workers", "max_turns", "max_depth", "max_seconds")
            },
            "key": args.key,
        }
        # A retry with the same key reuses the first discussion and project.
        for existing in await hub.request("GET", "/projects"):
            if existing.get("key") == args.key and existing["owner"] == actor:
                if any(
                    existing[name] != body[name]
                    for name in (
                        "title",
                        "objective",
                        "origin_thread_id",
                        "origin_channel_id",
                        "limits",
                    )
                ):
                    raise ValueError("Conflicting project creation key")
                if set(existing["members"]) != set(members):
                    raise ValueError("Conflicting project membership for creation key")
                return existing
        if args.discussion_thread:
            channel, thread = (
                os.environ.get("MATTERMOST_PEER_CHANNEL_ID", ""),
                args.discussion_thread,
            )
        else:
            channel, thread = await create_discussion(args)
        body.update(channel_id=channel, thread_id=thread)
        return await hub.request("POST", "/projects", body)
    if command in {"ask", "task"}:
        recipient = args.recipient if command == "ask" else args.to
        return await hub.request(
            "POST",
            f"/projects/{args.project}/messages",
            {
                "recipient": recipient,
                "kind": "ask" if command == "ask" else "work",
                "text": content(args),
                "key": args.key,
                "parent": args.parent,
                "dependencies": args.after or [],
            },
        )
    if command == "note":
        return await hub.request(
            "POST",
            f"/projects/{args.project}/notes",
            {
                "text": content(args),
                "artifacts": args.artifact or [],
                "key": args.key,
            },
        )
    if command == "bind":
        return await hub.request(
            "POST",
            f"/projects/{args.project}/bindings",
            {
                "thread_id": args.thread,
                "channel_id": args.channel,
                "workspace": args.workspace,
            },
        )
    if command == "cancel":
        return await hub.request("POST", f"/projects/{args.project}/cancel", {})
    if command in {"claim", "retry", "reply"}:
        body = {}
        if command == "retry":
            body["reconciliation"] = args.reconciliation
        elif command == "reply":
            body = {
                "result": content(args),
                "state": args.state,
                "evidence": {"artifacts": args.artifact or []},
                "claim": args.claim,
            }
        return await hub.request("POST", f"/messages/{args.message}/{command}", body)
    if command == "wait":
        deadline = asyncio.get_running_loop().time() + args.seconds
        while True:
            result = await hub.request("GET", f"/messages/{args.message}")
            if result["state"] not in {"open", "queued", "running", "cancelling"}:
                return result
            if asyncio.get_running_loop().time() >= deadline:
                return result
            await asyncio.sleep(1)
    raise ValueError("Unknown command")


def text_options(parser):
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--text")
    group.add_argument("--text-file")


def parser():
    root = argparse.ArgumentParser(description="Share project work between existing assistants")
    commands = root.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create")
    create.add_argument("--title", required=True)
    create.add_argument("--objective", required=True)
    create.add_argument(
        "--members", help="Comma-separated participants; default: configured actors"
    )
    create.add_argument("--origin-thread", required=True)
    create.add_argument("--origin-channel", required=True)
    create.add_argument("--discussion-thread", help="Reuse an existing floor root")
    create.add_argument("--key", default=uuid.uuid4().hex)
    for name, value in (("workers", 3), ("turns", 24), ("depth", 2), ("seconds", 3600)):
        create.add_argument("--max-" + name, type=int, default=value)
    commands.add_parser("list")
    for name in ("show", "cancel"):
        commands.add_parser(name).add_argument("project")
    for name in ("ask", "task"):
        command = commands.add_parser(name)
        command.add_argument("project")
        if name == "ask":
            command.add_argument("recipient")
        else:
            command.add_argument("--to")
        text_options(command)
        command.add_argument("--key", default=uuid.uuid4().hex)
        command.add_argument("--parent")
        command.add_argument("--after", action="append")
    command = commands.add_parser("note")
    command.add_argument("project")
    text_options(command)
    command.add_argument("--artifact", action="append")
    command.add_argument("--key", default=uuid.uuid4().hex)
    command = commands.add_parser("bind")
    command.add_argument("project")
    command.add_argument("--thread", required=True)
    command.add_argument("--channel", required=True)
    command.add_argument("--workspace")
    commands.add_parser("claim").add_argument("message")
    command = commands.add_parser("retry")
    command.add_argument("message")
    command.add_argument("--reconciliation", required=True)
    command = commands.add_parser("reply")
    command.add_argument("message")
    text_options(command)
    command.add_argument("--state", choices=["done", "blocked", "failed"], default="done")
    command.add_argument("--artifact", action="append")
    command.add_argument("--claim", help="Delivery claim nonce from the request prompt")
    command = commands.add_parser("wait")
    command.add_argument("message")
    command.add_argument("--seconds", type=int, choices=range(1, 46), default=30)
    return root


async def amain(args):
    settings = Settings.load()
    if settings is None:
        raise ValueError("Collaboration is not configured in this assistant's environment")
    hub = HubClient(settings)
    try:
        result = await execute(args, hub)
        print(json.dumps(result, indent=2))
    finally:
        await hub.close()


def main():
    args = parser().parse_args()
    try:
        asyncio.run(amain(args))
    except (Exception, KeyboardInterrupt) as exc:
        # Never echo transport exceptions that could contain request headers.
        print(
            f"Collaboration command failed ({type(exc).__name__}). "
            "Check shared state before retrying; use the same request key.",
            file=sys.stderr,
        )
        if hasattr(args, "key"):
            print("Request key: " + args.key, file=sys.stderr)
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
