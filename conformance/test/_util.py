from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from coverage import Coverage

VERSION_CONFORMANCE = "v1.0.6"

# Below the CI job's 10-minute limit, so a hung run fails with its output.
_RUNNER_TIMEOUT = 300


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
        from pydevd import _pydev_bundle  # ty: ignore[unresolved-import] - provided by IDE  # noqa: PLC0415

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


def run_conformance(args: list[str], env: dict[str, str] | None) -> str | None:
    """Run the conformance runner with args, returning its output if it fails.

    The runner, its implementation under test, and anything those start are
    killed if they outlive the timeout.
    """
    proc = subprocess.Popen(
        [
            "go",
            "run",
            f"connectrpc.com/conformance/cmd/connectconformance@{VERSION_CONFORMANCE}",
            *args,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=_RUNNER_TIMEOUT)
    except subprocess.TimeoutExpired:
        processes = _list_process_group(proc.pid)
        os.killpg(proc.pid, signal.SIGKILL)
        stdout, stderr = proc.communicate()
        return (
            f"\ntimed out after {_RUNNER_TIMEOUT}s, with these processes running:"
            f"\n{processes}\n{stdout}\n{stderr}"
        )
    if proc.returncode != 0:
        return f"\n{stdout}\n{stderr}"
    return None


def _list_process_group(pgid: int) -> str:
    ps = subprocess.run(
        ["ps", "-eo", "pid,ppid,pgid,stat,etime,args"],
        capture_output=True,
        text=True,
        check=False,
    )
    header, *rows = ps.stdout.splitlines()
    return "\n".join([header, *(r for r in rows if r.split()[2] == str(pgid))])
