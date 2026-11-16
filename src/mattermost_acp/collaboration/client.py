"""Authenticated local API client. Credentials remain outside native prompts."""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import aiohttp


class HubError(Exception):
    def __init__(self, message, *, status=None):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Settings:
    url: str
    token: str

    @classmethod
    def load(cls):
        url = os.environ.get("MATTERMOST_COLLAB_URL", "").strip().rstrip("/")
        if not url:
            return None
        parsed = urlparse(url)
        try:
            port = parsed.port
        except ValueError:
            raise ValueError("Collaboration URL must use a valid TCP port") from None
        if (
            parsed.scheme != "http"
            or (port is not None and not 0 < port <= 65535)
            or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.username
            or parsed.password
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Collaboration URL must be a plain loopback HTTP origin")
        path = os.environ.get("MATTERMOST_COLLAB_TOKEN_FILE", "")
        if not path:
            raise ValueError("Collaboration actor token file required")
        token = Path(path).expanduser().read_text().strip()
        if len(token) < 32:
            raise ValueError("Collaboration actor credential is invalid")
        return cls(url, token)


class HubClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.deadlines = {}
        self.session = aiohttp.ClientSession(
            json_serialize=lambda value: json.dumps(value, ensure_ascii=False),
            headers={"Authorization": "Bearer " + settings.token},
            timeout=aiohttp.ClientTimeout(total=25),
        )

    async def request(self, method, path, body=None):
        started = time.monotonic()
        async with self.session.request(method, self.settings.url + path, json=body) as response:
            if response.status >= 400:
                detail = "request failed"
                try:
                    raw = await response.text()
                    try:
                        result = json.loads(raw)
                    except ValueError:
                        detail = raw.strip() or detail
                    else:
                        if isinstance(result, dict):
                            detail = str(result.get("error") or detail)
                except (aiohttp.ClientError, TimeoutError, ValueError):
                    pass  # Preserve the status even when its error body is unreadable.
                raise HubError(
                    f"Collaboration HTTP {response.status}: {detail[:1000]}",
                    status=response.status,
                )
            result = await response.json()
            for item in result if isinstance(result, list) else [result]:
                if not isinstance(item, dict):
                    continue
                remaining = item.get("project_remaining_seconds")
                pid = item.get("project")
                if (
                    isinstance(pid, str)
                    and type(remaining) in {int, float}
                    and math.isfinite(remaining)
                    and remaining >= 0
                ):
                    deadline = min(self.deadlines.get(pid, float("inf")), started + remaining)
                    self.deadlines[pid] = deadline
                    item["local_project_deadline"] = deadline
            return result

    async def close(self):
        await self.session.close()
