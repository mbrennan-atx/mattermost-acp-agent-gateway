#!/usr/bin/env python3
"""Add Claude's promptRequired idle opt-in to the installed Codex ACP adapter.

No active-process change: restart idle gateways after applying. Exact source
matches prevent silently patching an incompatible release. Original bytes are
kept beside the bundle. Reapply after reinstalling/upgrading codex-acp.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

REPLACEMENTS = (
    (
        "steering: {\n          supported: true\n        }",
        "steering: {\n          supported: true,\n"
        '          idleBehavior: "promptRequired"\n        }',
    ),
    (
        "    return await this.startNewTurnFromSteering(params);",
        '    if (params._meta?.steering?.idleBehavior === "promptRequired") {\n'
        '      return { outcome: "promptRequired", reason: "noRunningTurn" };\n'
        "    }\n    return await this.startNewTurnFromSteering(params);",
    ),
    (
        "  parseSessionSteerParams(params) {\n"
        '    const sessionId = params["sessionId"];\n'
        '    const prompt = params["prompt"];',
        "  parseSessionSteerParams(params) {\n"
        '    const sessionId = params["sessionId"];\n'
        '    const prompt = params["prompt"];\n'
        '    const _meta = params["_meta"];',
    ),
    (
        "    return {\n      sessionId,\n      prompt\n    };\n  }\n"
        "  createSessionConfigOptions(sessionState)",
        "    return {\n      sessionId,\n      prompt,\n      _meta\n    };\n  }\n"
        "  createSessionConfigOptions(sessionState)",
    ),
)


def patched_source(source: str) -> str:
    native_markers = (
        'idleBehavior: "promptRequired"',
        'if (params._meta?.steering?.idleBehavior === "promptRequired") {',
        'return { outcome: "promptRequired", reason: "noRunningTurn" };',
        'const meta3 = params["_meta"];',
        'const idleBehavior = steering?.["idleBehavior"];',
        "_meta: { steering: { idleBehavior } }",
        "return await this.startNewTurnFromSteering(params, clientUserMessageId);",
    )
    if all(marker in source for marker in native_markers):
        return source
    for before, after in REPLACEMENTS:
        if source.count(after) == 1:
            continue
        if source.count(before) != 1:
            raise ValueError(
                "Codex ACP source differs; no files changed. Check adapter compatibility."
            )
        source = source.replace(before, after, 1)
    return source


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bundle",
        type=Path,
        help="explicit installed codex-acp dist/index.js path",
    )
    args = parser.parse_args()
    source = args.bundle.read_text()
    updated = patched_source(source)
    if updated == source:
        print("Codex active-only steering patch already present.")
        return
    backup = args.bundle.with_suffix(".js.before-steering")
    if not backup.exists():
        backup.write_text(source)
    temp = args.bundle.with_suffix(".js.steering-tmp")
    temp.write_text(updated)
    temp.chmod(args.bundle.stat().st_mode & 0o777)
    os.replace(temp, args.bundle)
    print("Codex active-only steering patch applied; restart idle gateways to load it.")


if __name__ == "__main__":
    main()
