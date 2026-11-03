"""Mattermost custom slash-command HTTP payload → gateway verbs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mattermost_acp.commands.defs import TEXT_COMMANDS


@dataclass(frozen=True)
class SlashRequest:
    token: str
    command: str
    text: str
    user_id: str
    user_name: str
    channel_id: str
    team_id: str
    root_id: str
    response_url: str


def _first(form: dict[str, Any], *keys: str) -> str:
    for key in keys:
        val = form.get(key)
        if val is None:
            continue
        if isinstance(val, (list, tuple)):
            val = val[0] if val else ""
        s = str(val).strip()
        if s:
            return s
    return ""


def parse_slash_payload(form: dict[str, Any]) -> SlashRequest:
    """Parse Mattermost ``application/x-www-form-urlencoded`` slash POST."""
    raw_cmd = _first(form, "command").lstrip("/").lower()
    # `/agent-status` and `/agent-help` dodge Mattermost's built-in `/status`
    # and `/help`; legacy `/grok-*` triggers map the same way.
    for prefix in ("agent-", "grok-"):
        if raw_cmd.startswith(prefix):
            raw_cmd = raw_cmd[len(prefix) :]
            break
    text = _first(form, "text")
    if raw_cmd not in TEXT_COMMANDS and text:
        # Catch-all `/grok new …` — first token is the verb.
        parts = text.split(None, 1)
        head = parts[0].lstrip("/").lower()
        if head in TEXT_COMMANDS:
            raw_cmd = head
            text = parts[1] if len(parts) > 1 else ""
    return SlashRequest(
        token=_first(form, "token"),
        command=raw_cmd,
        text=text,
        user_id=_first(form, "user_id"),
        user_name=_first(form, "user_name"),
        channel_id=_first(form, "channel_id"),
        team_id=_first(form, "team_id"),
        root_id=_first(form, "root_id", "root_post_id", "parent_id"),
        response_url=_first(form, "response_url"),
    )


def slash_authorized(token: str, allowed: frozenset[str]) -> bool:
    if not allowed:
        return False
    return token in allowed


def autocomplete_items(
    catalog_ids: list[str],
    default_id: str | None,
    query: str,
    *,
    limit: int = 25,
) -> list[dict[str, str]]:
    """Map model ids onto Mattermost ``AutocompleteListItem`` dicts.

    Same ordering as Discord autocomplete: default first on empty query,
    then prefix matches, then substring matches.
    """
    cur = (query or "").strip().lower()
    if not cur:
        rest = sorted(i for i in catalog_ids if i != default_id)
        ordered = ([default_id] if default_id in catalog_ids else []) + rest
        picked = ordered[:limit]
    else:
        prefix = sorted(
            (i for i in catalog_ids if i.lower().startswith(cur)), key=str.lower
        )
        substr = sorted(
            (i for i in catalog_ids if cur in i.lower() and i not in prefix),
            key=str.lower,
        )
        picked = (prefix + substr)[:limit]
    return [
        {
            "Item": mid,
            "Hint": "default model" if mid == default_id else "",
            "HelpText": f"Switch to {mid}",
        }
        for mid in picked
    ]


def ephemeral(text: str) -> dict[str, str]:
    return {"response_type": "ephemeral", "text": text}


def in_channel(text: str) -> dict[str, str]:
    return {"response_type": "in_channel", "text": text}
