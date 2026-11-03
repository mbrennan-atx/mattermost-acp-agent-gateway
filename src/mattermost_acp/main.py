"""Entry point: ``mattermost-acp-gateway`` / ``python -m mattermost_acp.main``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from logging.handlers import RotatingFileHandler

from mattermost_acp.acp import AcpBroker
from mattermost_acp.config import Config
from mattermost_acp.harness_switch import busy_path
from mattermost_acp.mm.client import MattermostClient
from mattermost_acp.mm.gateway import MattermostGateway
from mattermost_acp.mm.http import start_slash_site
from mattermost_acp.model_router import load_router_from_env
from mattermost_acp.session_service import SessionService
from mattermost_acp.store import ThreadStore


def _setup_logging(log_file) -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=5)
        fh.setFormatter(fmt)
        root.addHandler(fh)


async def _amain() -> None:
    config = Config.load()
    _setup_logging(config.log_file)
    log = logging.getLogger("mattermost_acp")

    busy_path(config.db_path).unlink(missing_ok=True)
    store = ThreadStore(config.db_path, harness=config.harness)
    await store.open()

    broker = AcpBroker(
        command=config.grok_command,
        backend=config.harness,
        model=config.grok_model,
        always_approve=config.always_approve,
    )
    sessions = SessionService(
        broker,
        store,
        default_cwd=config.grok_cwd,
        default_model=config.grok_model,
        grok_command=config.grok_command,
        max_parallel_turns=config.max_parallel_turns,
        inject_channel_context=config.inject_channel_context,
        busy_marker=busy_path(config.db_path),
        router=load_router_from_env(),
    )

    client = MattermostClient(config.mattermost_url, config.mattermost_token)
    gateway = MattermostGateway(config, client, sessions)
    slash_runner = await start_slash_site(config, gateway.handle_slash)

    log.info(
        "Starting mattermost-acp-gateway cwd=%s harness=%s channels=%s "
        "channel_context=%s auto_rename=%s answer_mode=%s",
        config.grok_cwd,
        config.harness,
        "ALL" if config.all_channels else sorted(config.channel_ids),
        config.inject_channel_context,
        config.auto_rename_threads,
        config.answer_mode,
    )

    loop = asyncio.get_running_loop()
    closing = False

    def _on_signal(signame: str) -> None:
        nonlocal closing
        if closing:
            return
        closing = True
        log.info("Received %s — shutting down", signame)
        loop.create_task(gateway.close(), name=f"signal-close-{signame}")

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal, sig.name)
        except NotImplementedError:
            pass

    try:
        await gateway.run()
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, ValueError):
                pass
        await gateway.close()
        if slash_runner is not None:
            await slash_runner.cleanup()
        await client.close()
        await broker.stop()
        await store.close()
        log.info("Shutdown complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="Mattermost gateway for ACP agent harnesses")
    parser.add_argument(
        "--check", action="store_true", help="validate configuration without connecting"
    )
    args = parser.parse_args()
    if args.check:
        from mattermost_acp.validate import check

        try:
            check()
        except (ValueError, OSError):
            # Config values can contain credentials. Do not echo exception inputs.
            print(
                "Configuration check failed. Verify required runtime settings, selected files, "
                "schema versions, executable and working directory.",
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        print("Configuration valid; no connections or state changes made.")
        return
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
