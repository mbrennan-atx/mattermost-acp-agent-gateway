"""Entry point: ``mattermost-acp-gateway`` / ``python -m mattermost_acp.main``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from logging.handlers import RotatingFileHandler

from mattermost_acp.acp import AcpBroker
from mattermost_acp.acp.broker import NativeDispatchChanged
from mattermost_acp.config import Config, load_mcp_servers_file
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


_WORKSPACE_GUARD_SUPERVISOR = """
import os, selectors, signal, subprocess, sys, threading

# Only this still-live group member can authorize its group signal.
if os.getpgrp() != os.getpid():
    raise SystemExit(125)
try:
    control = int(sys.argv[1])
    guard = subprocess.Popen(sys.argv[2:], stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, close_fds=True)
    ready_read, ready_write = os.pipe()
    def finished():
        guard.wait()
        os.write(ready_write, b"x")
    threading.Thread(target=finished, daemon=True).start()
    selector = selectors.DefaultSelector()
    selector.register(control, selectors.EVENT_READ, "cancel")
    selector.register(ready_read, selectors.EVENT_READ, "finished")
    for key, _ in selector.select():
        if key.data == "finished":
            os.write(1, (str(guard.returncode) + "\\n").encode("ascii"))
        break
finally:
    # The supervisor remains in this group even after the guard is reaped.
    os.killpg(os.getpgrp(), signal.SIGKILL)
"""


_WORKSPACE_GUARD_PINNED_SOURCE = """
import hashlib, os, stat, sys
path, expected, *arguments = sys.argv[1:]
limit = 8 * 1024 * 1024
fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
with os.fdopen(fd, "rb") as stream:
    info = os.fstat(stream.fileno())
    if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
        raise SystemExit(125)
    source = stream.read(limit + 1)
if len(source) > limit or hashlib.sha256(source).hexdigest() != expected:
    raise SystemExit(125)
sys.argv = [path, *arguments]
exec(compile(source, path, "exec", dont_inherit=True), {
    "__name__": "__main__", "__file__": path, "__package__": None,
})
"""


async def _finish_guard_task(task):
    """Retain launch/wait ownership through repeated caller cancellation."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


def build_workspace_guard(command, *, timeout_s=15, source_sha256=None):
    """Run an optional trusted executable before native session admission."""
    if not command:
        return None

    async def verify(thread_id, cwd, prior_session):
        process, launch = None, None
        control_read, control_write = None, None
        try:
            async with asyncio.timeout(timeout_s):
                arguments = [command, thread_id, str(cwd), prior_session or ""]
                if source_sha256 is not None:
                    arguments = [
                        sys.executable,
                        "-I",
                        "-S",
                        "-c",
                        _WORKSPACE_GUARD_PINNED_SOURCE,
                        command,
                        source_sha256,
                        *arguments[1:],
                    ]
                options = {}
                if os.name == "posix":
                    control_read, control_write = os.pipe()
                    arguments = [
                        sys.executable,
                        "-I",
                        "-S",
                        "-c",
                        _WORKSPACE_GUARD_SUPERVISOR,
                        str(control_read),
                        *arguments,
                    ]
                    options = {
                        "pass_fds": (control_read,),
                        "start_new_session": True,
                    }
                launch = asyncio.create_task(
                    asyncio.create_subprocess_exec(
                        *arguments,
                        stdout=(
                            asyncio.subprocess.PIPE
                            if os.name == "posix"
                            else asyncio.subprocess.DEVNULL
                        ),
                        stderr=asyncio.subprocess.DEVNULL,
                        **options,
                    )
                )
                process = await asyncio.shield(launch)
                if os.name == "posix":
                    result = await process.stdout.readline()
                    refused = result != b"0\n" or await process.wait() != -signal.SIGKILL
                else:
                    refused = await process.wait() != 0
                if refused:
                    raise NativeDispatchChanged("Workspace verification refused native preparation")
        except BaseException as exc:
            # Closing the lease lets the live supervisor terminate its own group.
            # Never signal a numeric group ID belonging to a reaped process.
            if control_write is not None:
                os.close(control_write)
                control_write = None
            if launch is not None:
                try:
                    process = await _finish_guard_task(launch)
                except BaseException:
                    process = None
            if process is not None:
                try:
                    if os.name != "posix" and process.returncode is None:
                        process.kill()
                except ProcessLookupError:
                    pass
                await _finish_guard_task(asyncio.create_task(process.wait()))
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise NativeDispatchChanged(
                "Workspace verification refused native preparation"
            ) from exc
        finally:
            for descriptor in (control_read, control_write):
                if descriptor is not None:
                    os.close(descriptor)

    return verify


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
        agy_cli_home=config.agy_cli_home,
        agy_app_data_dir=config.agy_app_data_dir,
        agy_disable_telemetry=config.agy_disable_telemetry,
        agy_auth_method=config.agy_auth_method,
        mcp_servers=load_mcp_servers_file(config.agy_mcp_servers_file),
        initial_native_uncertainty=await store.native_boundary_unknown(),
        on_native_start=store.mark_native_boundary_unknown,
    )
    sessions = SessionService(
        broker,
        store,
        default_cwd=config.grok_cwd,
        default_model=config.grok_model,
        default_effort=config.grok_effort,
        grok_command=config.grok_command,
        max_parallel_turns=config.max_parallel_turns,
        inject_channel_context=config.inject_channel_context,
        busy_marker=busy_path(config.db_path),
        router=load_router_from_env(),
        workspace_guard=build_workspace_guard(
            config.workspace_guard_command,
            timeout_s=config.workspace_guard_timeout_s,
            source_sha256=config.workspace_guard_source_sha256,
        ),
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
