from __future__ import annotations

import asyncio
import sys

VERSION_CONFORMANCE = "v1.0.5"


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
