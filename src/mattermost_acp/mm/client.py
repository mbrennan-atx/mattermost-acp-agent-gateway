"""Mattermost REST + WebSocket client, authenticated as a **user** PAT."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)


class MattermostError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None, data: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.data = data


def ws_url(http_url: str) -> str:
    parsed = urlparse(http_url.rstrip("/"))
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.netloc}{parsed.path}/api/v4/websocket"


@dataclass(frozen=True)
class PostedEvent:
    post_id: str
    channel_id: str
    user_id: str
    message: str
    root_id: str
    file_ids: tuple[str, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict)
    channel_type: str = ""

    @property
    def thread_id(self) -> str:
        """Root post id: the post itself if it is a channel root."""
        return self.root_id or self.post_id

    @property
    def is_thread_reply(self) -> bool:
        return bool(self.root_id)


def parse_posted(payload: dict[str, Any]) -> PostedEvent | None:
    """Parse a WebSocket ``posted`` event. ``data.post`` is a JSON string."""
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    raw_post = data.get("post")
    if isinstance(raw_post, str):
        try:
            post = json.loads(raw_post)
        except json.JSONDecodeError:
            return None
    elif isinstance(raw_post, dict):
        post = raw_post
    else:
        return None
    if not isinstance(post, dict):
        return None
    post_type = str(post.get("type") or "")
    if post_type:
        # system_join_channel etc.
        return None
    file_ids = post.get("file_ids") or []
    if not isinstance(file_ids, list):
        file_ids = []
    return PostedEvent(
        post_id=str(post.get("id") or ""),
        channel_id=str(post.get("channel_id") or ""),
        user_id=str(post.get("user_id") or ""),
        message=str(post.get("message") or ""),
        root_id=str(post.get("root_id") or ""),
        file_ids=tuple(str(fid) for fid in file_ids if fid),
        raw=post,
        channel_type=str(data.get("channel_type") or ""),
    )


class MattermostClient:
    def __init__(
        self,
        url: str,
        token: str,
        *,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.url = url.rstrip("/")
        self.token = token
        self._session = session
        self._own_session = session is None
        self.me: dict[str, Any] | None = None
        self._seq = 1

    @property
    def user_id(self) -> str | None:
        if self.me is None:
            return None
        return str(self.me.get("id") or "") or None

    @property
    def username(self) -> str:
        if self.me is None:
            return ""
        return str(self.me.get("username") or self.me.get("nickname") or "")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._headers())
            self._own_session = True
        return self._session

    async def close(self) -> None:
        if self._own_session and self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        session = await self._ensure_session()
        url = f"{self.url}{path}"
        async with session.request(method, url, **kwargs) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise MattermostError(
                    f"{method} {path} -> {resp.status}: {text[:300]}",
                    status=resp.status,
                    data=text,
                )
            if not text:
                return None
            try:
                return json.loads(text)
            except json.JSONDecodeError as exc:
                raise MattermostError(f"non-JSON from {path}", data=text[:300]) from exc

    async def get_me(self) -> dict[str, Any]:
        me = await self._request("GET", "/api/v4/users/me")
        if not isinstance(me, dict) or not me.get("id"):
            raise MattermostError("users/me missing id", data=me)
        self.me = me
        is_bot = bool(me.get("is_bot"))
        logger.info(
            "Mattermost identity user=%s id=%s is_bot=%s",
            me.get("username"),
            me.get("id"),
            is_bot,
        )
        if is_bot:
            logger.warning(
                "This token is a bot account. Plan is a real user PAT, not a bot."
            )
        return me

    async def get_team_public_channels(self, team_id: str) -> list[dict[str, Any]]:
        """Every public channel on the team (paginated)."""
        out: list[dict[str, Any]] = []
        page = 0
        while True:
            batch = await self._request(
                "GET",
                f"/api/v4/teams/{team_id}/channels",
                params={"page": page, "per_page": 200},
            )
            if not isinstance(batch, list) or not batch:
                break
            out.extend(item for item in batch if isinstance(item, dict))
            if len(batch) < 200:
                break
            page += 1
        return out

    async def get_my_channel_ids(self, team_id: str) -> set[str]:
        channels = await self._request(
            "GET", f"/api/v4/users/me/teams/{team_id}/channels"
        )
        if not isinstance(channels, list):
            return set()
        return {
            str(c.get("id"))
            for c in channels
            if isinstance(c, dict) and c.get("id")
        }

    async def join_channel(self, channel_id: str) -> None:
        user_id = self.user_id
        if not user_id:
            raise MattermostError("join_channel before get_me()")
        await self._request(
            "POST",
            f"/api/v4/channels/{channel_id}/members",
            json={"user_id": user_id},
        )

    async def create_post(
        self,
        channel_id: str,
        message: str,
        *,
        root_id: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"channel_id": channel_id, "message": message}
        if root_id:
            payload["root_id"] = root_id
        post = await self._request("POST", "/api/v4/posts", json=payload)
        if not isinstance(post, dict):
            raise MattermostError("create_post: unexpected payload", data=post)
        return post

    async def get_post(self, post_id: str) -> dict[str, Any]:
        post = await self._request("GET", f"/api/v4/posts/{post_id}")
        if not isinstance(post, dict):
            raise MattermostError("get_post: unexpected payload", data=post)
        return post

    async def update_post(self, post_id: str, message: str) -> dict[str, Any]:
        post = await self._request(
            "PUT",
            f"/api/v4/posts/{post_id}",
            json={"id": post_id, "message": message},
        )
        if not isinstance(post, dict):
            raise MattermostError("update_post: unexpected payload", data=post)
        return post

    async def patch_post(self, post_id: str, message: str) -> dict[str, Any]:
        """Partial update. Safer than ``update_post`` for editing someone else's root."""
        post = await self._request(
            "PUT",
            f"/api/v4/posts/{post_id}/patch",
            json={"message": message},
        )
        if not isinstance(post, dict):
            raise MattermostError("patch_post: unexpected payload", data=post)
        return post

    async def get_file(self, file_id: str) -> bytes:
        session = await self._ensure_session()
        url = f"{self.url}/api/v4/files/{file_id}"
        async with session.get(url) as resp:
            if resp.status >= 400:
                body = await resp.text()
                raise MattermostError(
                    f"GET files/{file_id} -> {resp.status}: {body[:200]}",
                    status=resp.status,
                )
            return await resp.read()

    async def get_file_info(self, file_id: str) -> dict[str, Any]:
        info = await self._request("GET", f"/api/v4/files/{file_id}/info")
        return info if isinstance(info, dict) else {}

    async def events(self, *, stop: asyncio.Event | None = None) -> AsyncIterator[dict[str, Any]]:
        """Yield WebSocket events; reconnects on drop. Heartbeat ping."""
        backoff = 1.0
        while stop is None or not stop.is_set():
            try:
                async for event in self._one_socket(stop=stop):
                    backoff = 1.0
                    yield event
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Mattermost websocket dropped; reconnecting in %.0fs", backoff)
                try:
                    await asyncio.wait_for(
                        (stop.wait() if stop is not None else asyncio.sleep(backoff)),
                        timeout=backoff,
                    )
                except TimeoutError:
                    pass
                backoff = min(30.0, backoff * 2)

    async def _one_socket(
        self, *, stop: asyncio.Event | None
    ) -> AsyncIterator[dict[str, Any]]:
        session = await self._ensure_session()
        url = ws_url(self.url)
        logger.info("Connecting websocket %s", url)
        async with session.ws_connect(url, heartbeat=30) as ws:
            self._seq += 1
            await ws.send_json(
                {
                    "seq": self._seq,
                    "action": "authentication_challenge",
                    "data": {"token": self.token},
                }
            )
            async for msg in ws:
                if stop is not None and stop.is_set():
                    await ws.close()
                    return
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                    except json.JSONDecodeError:
                        logger.warning("WS non-JSON: %s", str(msg.data)[:200])
                        continue
                    if isinstance(payload, dict):
                        if payload.get("event") == "ping":
                            self._seq += 1
                            await ws.send_json({"action": "ping", "seq": self._seq})
                            continue
                        yield payload
                elif msg.type in {
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                    aiohttp.WSMsgType.CLOSE,
                }:
                    logger.info("WS closed type=%s", msg.type)
                    return
