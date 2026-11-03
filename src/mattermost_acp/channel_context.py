"""Mark Mattermost-origin turns so the agent can distinguish them from CLI/TUI chats."""

from __future__ import annotations

from typing import Any

MATTERMOST_SESSION_RULES = """\
# Channel: Mattermost (mattermost-acp-gateway)

You are talking to the user through **Mattermost**, using the configured agent harness.

Implications:
- Each Mattermost **thread** (root post) is one agent session. History is this thread only.
- Mattermost markdown is close to GFM. Tables may stay tables.
- Links in prose or bullets (clickable). Never bury URLs inside fences.
- Final answers become Mattermost posts (~16k char cap). Prefer one post.
  If longer, the gateway splits at headings / closed fences / paragraphs.
- Tool/status updates stream live as a status post in the thread.
- Do not assume interactive terminal UX (no keybinds, no full-screen TUI).
- Gateway commands are handled outside normal chat. Available commands and
  capabilities depend on the selected harness; use `/help` for that session.
- Connector tools depend on the configured harness. `/mcps` reports connector
  status; it does not grant access or install connectors.
- If the user asks in chat to change the model for this thread, this channel,
  or the harness/system default, do it. Include exactly one line in the final
  answer: `MATTERMOST_MODEL_SET: <id> [effort] [thread|channel|harness]`
  (`DISCORD_MODEL_SET:` is also accepted). Scope defaults to `thread`. Map
  "system" to `harness`. Catalog ids preferred; short names (`sonnet`,
  `opus`, `grok`) are fine. Only emit when they asked to switch — do not
  tell them to run `/model`. The gateway applies after this turn and strips
  the line; the next user message uses the new model.
- When asked (first turns / `/new`), include exactly one line:
  `MATTERMOST_THREAD_RENAME: <short topic title>`. The gateway prepends that
  as the root post's first line (the channel-list header) and strips it. Do
  not emit after the header is already a real title.
- When a turn-level `<channel_context>` block is present, treat it as gateway
  metadata (not user prose). Answer the user text after that block.
"""


def session_meta_rules() -> dict[str, Any]:
    """ACP ``session/new`` ``_meta`` fragment."""
    return {"rules": MATTERMOST_SESSION_RULES}


def load_role_text(*paths: Any) -> str:
    """Read explicitly selected inputs in order, preserving the 4,000 character cap."""
    chunks: list[str] = []
    for path in paths:
        if path is None:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            raise ValueError("Selected instruction file is unreadable") from None
        text = text.strip()
        if text:
            chunks.append(text[:4000])
    return "\n\n".join(chunks)


def wrap_user_prompt(
    user_text: str,
    *,
    thread_id: str,
    thread_name: str,
    author_name: str,
    author_id: str,
    parent_channel_id: str | None = None,
    team_id: str | None = None,
    harness: str = "",
    role_text: str = "",
) -> str:
    """Prepend a compact machine-readable channel header to the user message."""
    name = (thread_name or "").strip() or "(unnamed)"
    author = (author_name or "").strip() or "(unknown)"
    lines = [
        "<channel_context>",
        "channel: mattermost",
        "gateway: mattermost-acp-gateway",
        f"harness: {harness}",
        f"thread_id: {thread_id}",
        f'thread_name: "{name}"',
        f"author: {author} ({author_id})",
    ]
    if parent_channel_id is not None:
        lines.append(f"parent_channel_id: {parent_channel_id}")
    if team_id is not None:
        lines.append(f"team_id: {team_id}")
    lines.append("output: mattermost-md; GFM tables ok")
    lines.append(
        "model_switch: if asked, emit MATTERMOST_MODEL_SET: <id> [effort] [thread|channel|harness]"
    )
    role = (role_text or "").strip()
    if role:
        lines.append("role:")
        for line in role.splitlines():
            lines.append(f"  {line}")
    lines.append("</channel_context>")
    header = "\n".join(lines)
    body = user_text if user_text is not None else ""
    if not body.strip():
        return header
    return f"{header}\n\n{body}"
