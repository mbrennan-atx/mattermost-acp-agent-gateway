#!/usr/bin/env python3
"""Register /hint at the current /stop URL and retain its token outside Git.

Uses an explicitly selected administrator token. Does not restart gateways. Run with the
gateway venv, then reload idle gateways to pick up the shared slash token file.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--team-id", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--admin-token-file", type=Path, required=True)
    parser.add_argument("--slash-token-file", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not args.apply:
        print("Preview: register /hint using the existing /stop endpoint. Use --apply to write.")
        return
    pat = args.admin_token_file.expanduser().read_text().strip()

    def request(method, path, data=None):
        req = urllib.request.Request(
            args.url.rstrip("/") + "/api/v4" + path,
            data=json.dumps(data).encode() if data is not None else None,
            headers={"Authorization": "Bearer " + pat, "Content-Type": "application/json"},
            method=method,
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)

    def commands():
        return request("GET", f"/commands?team_id={args.team_id}&custom_only=true")

    existing = commands()
    stop = next(c for c in existing if c["trigger"] == "stop")
    hint = next((c for c in existing if c["trigger"] == "hint"), None)
    if hint and hint["url"] != stop["url"]:
        raise SystemExit("Existing /hint uses another URL; leaving it unchanged.")
    if hint is None:
        hint = request(
            "POST",
            "/commands",
            {
                "team_id": args.team_id,
                "trigger": "hint",
                "method": "P",
                "url": stop["url"],
                "display_name": "Agent hint",
                "auto_complete": True,
                "auto_complete_desc": (
                    "Add guidance to active agent work without cancelling the turn"
                ),
                "auto_complete_hint": "<guidance>",
            },
        )
    token_file = args.slash_token_file.expanduser()
    token_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tokens = token_file.read_text().splitlines() if token_file.exists() else []
    if hint["token"] not in tokens:
        tokens.append(hint["token"])
        temp = token_file.with_suffix(".hint-tmp")
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write("\n".join(tokens) + "\n")
        os.replace(temp, token_file)
    live = next(c for c in commands() if c["trigger"] == "hint")
    if live["id"] != hint["id"] or live["url"] != stop["url"]:
        raise SystemExit("/hint registration read-back failed.")
    print(f"/hint registered and verified at {live['url']}; token saved privately.")


if __name__ == "__main__":
    main()
