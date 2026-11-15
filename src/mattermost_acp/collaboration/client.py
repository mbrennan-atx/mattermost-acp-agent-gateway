"""Authenticated local API client. Credentials remain outside native prompts."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import aiohttp


class HubError(Exception):
    pass


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
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.username
            or parsed.password
            or parsed.path
            or parsed.query
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
        self.session = aiohttp.ClientSession(
            json_serialize=lambda value: json.dumps(value, ensure_ascii=False),
            headers={"Authorization": "Bearer " + settings.token},
            timeout=aiohttp.ClientTimeout(total=25),
        )

    async def request(self, method, path, body=None):
        async with self.session.request(method, self.settings.url + path, json=body) as response:
            result = await response.json()
            if response.status >= 400:
                raise HubError(
                    f"Collaboration HTTP {response.status}: "
                    f"{result.get('error', 'request rejected')}"
                )
            return result

    async def close(self):
        await self.session.close()
