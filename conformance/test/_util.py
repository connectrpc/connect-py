from __future__ import annotations

import asyncio
import faulthandler
import os
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

if TYPE_CHECKING:
    from coverage import Coverage

VERSION_CONFORMANCE = "v1.0.7"

# Directory that conformance server processes write Python stacks to when they
# receive SIGUSR1, so that a hung run can be diagnosed.
TRACEBACK_DIR_ENV = "CONFORMANCE_TRACEBACK_DIR"

_traceback_file: TextIO | None = None


def dump_tracebacks_on_sigusr1() -> None:
    """Writes this process's Python stacks to a file in TRACEBACK_DIR_ENV on SIGUSR1."""
    global _traceback_file  # noqa: PLW0603
    directory = os.environ.get(TRACEBACK_DIR_ENV)
    if not directory or not hasattr(signal, "SIGUSR1") or _traceback_file:
        return
    # Kept open for the life of the process, so faulthandler can write to it.
    _traceback_file = (Path(directory) / f"{os.getpid()}.txt").open("w")
    _traceback_file.write(f"pid {os.getpid()}, argv {sys.argv}\n")
    _traceback_file.flush()
    faulthandler.register(signal.SIGUSR1, file=_traceback_file, all_threads=True)


async def create_standard_streams():
    loop = asyncio.get_event_loop()
    stdin = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(stdin)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout
    )
    stdout = asyncio.StreamWriter(w_transport, w_protocol, stdin, loop)
    return stdin, stdout


def maybe_patch_args_with_debug(args: list[str]) -> list[str]:
    # Do a best effort to invoke the child with debugging.
    # This invokes internal methods from bundles provided by the IDE
    # and may not always work.
    try:
        from pydevd import _pydev_bundle  # ty: ignore[import-not-found] - provided by IDE  # noqa: PLC0415

        return _pydev_bundle.pydev_monkey.patch_args(args)
    except Exception:
        return args


def coverage_env(cov: Coverage | None) -> dict[str, str] | None:
    if cov is None:
        return None
    env: dict[str, str] = {**os.environ}
    # cov.config.source only contains . but we need .. too.
    # It should be fine to just hard-code this.
    env["COV_CORE_SOURCE"] = os.pathsep.join((".", ".."))
    if cov.config.config_file:
        env["COV_CORE_CONFIG"] = cov.config.config_file
    if cov.config.data_file:
        env["COV_CORE_DATAFILE"] = cov.config.data_file
    if cov.config.branch:
        env["COV_CORE_BRANCH"] = "enabled"
    if cov.config.context:
        env["COV_CORE_CONTEXT"] = cov.config.context

    return env
