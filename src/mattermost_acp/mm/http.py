"""Optional HTTP listener for Mattermost custom slash commands."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

import aiohttp
from aiohttp import web

from mattermost_acp.commands.models_catalog import fetch_catalog
from mattermost_acp.config import Config
from mattermost_acp.mm.slash import (
    SlashRequest,
    autocomplete_items,
    ephemeral,
    parse_slash_payload,
    slash_authorized,
)
from mattermost_acp.mm.slash_router import route_port

logger = logging.getLogger(__name__)

SlashHandler = Callable[[SlashRequest], Awaitable[dict]]

# Mattermost waits about 3s for a slash response.
_FORWARD_TIMEOUT = aiohttp.ClientTimeout(total=2.5)


async def _forward(port: int, form: dict, command: str) -> dict:
    """Relay a slash POST to the fleet gateway that owns the channel."""
    url = f"http://127.0.0.1:{port}/slash"
    try:
        async with aiohttp.ClientSession(timeout=_FORWARD_TIMEOUT) as session:
            async with session.post(url, data=form) as resp:
                logger.info(
                    "slash forwarded command=%s port=%s status=%s", command, port, resp.status
                )
                return await resp.json(content_type=None)
    except Exception:
        logger.exception("slash forward failed command=%s port=%s", command, port)
        return ephemeral(f"That agent's gateway (port {port}) did not answer.")


def build_slash_app(config: Config, handler: SlashHandler) -> web.Application:
    app = web.Application()

    async def slash(request: web.Request) -> web.Response:
        form = dict(await request.post())
        safe_keys = sorted(k for k in form if k != "token")
        logger.info(
            "slash POST keys=%s command=%s channel=%s user=%s root=%s",
            safe_keys,
            form.get("command"),
            form.get("channel_id"),
            form.get("user_id"),
            form.get("root_id") or form.get("root_post_id") or form.get("parent_id"),
        )
        req = parse_slash_payload(form)
        if not slash_authorized(req.token, config.slash_tokens):
            logger.warning("slash rejected: bad token command=%s", req.command)
            return web.json_response(ephemeral("Unauthorized slash token."), status=401)
        port = route_port(req.channel_id, config.slash_port)
        if port is not None:
            return web.json_response(await _forward(port, form, req.command))
        try:
            body = await handler(req)
        except Exception:
            logger.exception("slash handler failed command=%s", req.command)
            body = ephemeral("Slash command failed. Check gateway logs.")
        # Mattermost treats any non-2xx as "Command … failed." Keep 200.
        return web.json_response(body)

    app.router.add_post("/slash", slash)
    app.router.add_get("/healthz", lambda _r: web.Response(text="ok"))

    async def autocomplete_models(request: web.Request) -> web.Response:
        """Plugin dynamic-list lookup: ``[]AutocompleteListItem`` JSON.

        Called server-side by Mattermost with ``?user_input=…`` (plus
        channel_id/team_id/user_id context). No token check: the Mattermost
        server calls this, not the user, so slash tokens do not apply.
        """
        query = request.query.get("user_input", "")
        try:
            catalog = await fetch_catalog(config.grok_command)
        except Exception:
            logger.exception("model autocomplete catalog failed")
            return web.json_response([])
        items = autocomplete_items(catalog.ids, catalog.default_id, query)
        return web.json_response(items)

    app.router.add_get("/autocomplete/models", autocomplete_models)
    return app


async def start_slash_site(
    config: Config, handler: SlashHandler
) -> web.AppRunner | None:
    if config.slash_port <= 0:
        logger.info("Slash HTTP disabled (MATTERMOST_SLASH_PORT<=0)")
        return None
    if not config.slash_tokens:
        logger.warning(
            "Slash HTTP listening but MATTERMOST_SLASH_TOKEN is empty — all posts 401"
        )
    app = build_slash_app(config, handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.slash_host, config.slash_port)
    await site.start()
    logger.info(
        "Slash HTTP on http://%s:%s/slash", config.slash_host, config.slash_port
    )
    return runner
