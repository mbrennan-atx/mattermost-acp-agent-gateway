"""Decide whether a Mattermost post should start a gateway turn.

Home channels wake for an allowlisted user, and so does their direct
message to any agent. The agents floor wakes for a root post that @mentions
exactly this agent. Agent thread replies on the floor never wake anyone, so a
peer can answer an ask without starting another turn. An allowlisted user's floor-thread
reply wakes the one agent they @mention, or the agent that started the thread
when they mention nobody.
"""

from __future__ import annotations

import re

_MENTION = re.compile(r"(?<![\w])@([A-Za-z0-9.\-_]+)")
_BROADCAST = frozenset({"all", "here", "channel"})


def sole_mention(message: str) -> str | None:
    """The single @username in ``message``, or None.

    Zero mentions, two mentions, or a broadcast (@all, @here, @channel) all
    return None so one post cannot wake more than one agent.
    """
    found: list[str] = []
    for match in _MENTION.finditer(message or ""):
        name = match.group(1).lower().strip(".")
        if not name or name in _BROADCAST:
            return None
        if name not in found:
            found.append(name)
    if len(found) != 1:
        return None
    return found[0]


def classify_wake(
    *,
    channel_id: str,
    author_id: str,
    self_id: str,
    message: str,
    root_id: str,
    username: str,
    home_channel_ids: frozenset[str],
    all_channels: bool,
    exclude_channel_ids: frozenset[str],
    allow_user_ids: frozenset[str],
    peer_channel_id: str | None,
    channel_type: str = "",
    root_author_id: str = "",
) -> tuple[str | None, str]:
    """Return ``(home|peer, reason)`` or ``(None, reason)``.

    ``root_author_id`` is the floor thread starter; the gateway looks it up
    only for an allowlisted human's floor-thread reply.
    """
    if self_id and author_id == self_id:
        return None, "self"
    me = (username or "").lower()
    if peer_channel_id and channel_id == peer_channel_id:
        if root_id:
            if not allow_user_ids or author_id not in allow_user_ids:
                return None, "peer-reply"
            target = sole_mention(message)
            if target is not None:
                if me and target == me:
                    return "peer", "owner-reply-mention"
                return None, "owner-reply-other"
            if self_id and root_author_id == self_id:
                return "peer", "owner-reply-starter"
            return None, "owner-reply-other"
        target = sole_mention(message)
        if not me or target != me:
            return None, "peer-mention"
        return "peer", "peer"
    if channel_type == "D" and allow_user_ids and author_id in allow_user_ids:
        # A DM has two members: this bot and the allowlisted human.
        return "home", "owner-dm"
    if channel_id in exclude_channel_ids:
        return None, "not-home"
    in_home = all_channels or not home_channel_ids or channel_id in home_channel_ids
    if not in_home:
        return None, "not-home"
    if allow_user_ids and author_id not in allow_user_ids:
        return None, "not-allowlisted"
    return "home", "home"
