from __future__ import annotations

import asyncio
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from coverage import Coverage

VERSION_CONFORMANCE = "v1.0.5"

# CONFORMANCE_RUNNER is the command that runs the conformance runner: the
# binary at $CONFORMANCE_RUNNER if set, otherwise VERSION_CONFORMANCE via `go run`.
CONFORMANCE_RUNNER = (
    [runner]
    if (runner := os.environ.get("CONFORMANCE_RUNNER"))
    else [
        "go",
        "run",
        f"connectrpc.com/conformance/cmd/connectconformance@{VERSION_CONFORMANCE}",
    ]
)


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
        from pydevd import (  # ty: ignore[unresolved-import] - provided by IDE  # noqa: PLC0415
            _pydev_bundle,
        )

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
