from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _util import (
    TRACEBACK_DIR_ENV,
    VERSION_CONFORMANCE,
    coverage_env,
    maybe_patch_args_with_debug,
)

if TYPE_CHECKING:
    from coverage import Coverage

_current_dir = Path(__file__).parent
_server_py_path = str(_current_dir / "server.py")
_config_path = str(_current_dir / "config.yaml")

_RUNNER_TIMEOUT_SECONDS = 300


# Servers often run out of file descriptors due to low default ulimit.
# We go ahead and raise it automatically so tests can pass without special
# configuration.
@pytest.fixture(autouse=True, scope="session")
def macos_raise_ulimit():
    if os.name != "posix":
        return

    import resource  # noqa: PLC0415

    resource.setrlimit(resource.RLIMIT_NOFILE, (16384, 16384))


# There is a relatively low time limit for the server to respond with a resource error
# for this test. In resource limited environments such as CI, it doesn't seem to be enough,
# notably it is the first message that will take the longest to process as it also sets up
# the request. We can consider raising this delay in the runner to see if it helps.
#
# https://github.com/connectrpc/conformance/blob/main/internal/app/connectconformance/testsuites/data/server_message_size.yaml#L46
_known_flaky = [
    "--known-flaky",
    "Server Message Size/HTTPVersion:1/**/first-request-exceeds-server-limit",
]


def run_conformance(
    args: list[str],
    env: dict[str, str] | None,
    timeout: float = _RUNNER_TIMEOUT_SECONDS,
) -> None:
    """Run the conformance runner, failing with its output if it fails or hangs.

    If it hangs, the failure also lists the processes still running and their
    stacks.
    """
    with tempfile.TemporaryDirectory() as traceback_dir:
        env = {**(os.environ if env is None else env), TRACEBACK_DIR_ENV: traceback_dir}
        with subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            start_new_session=True,
        ) as process:
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                if not hasattr(os, "killpg"):
                    process.kill()
                    stdout, stderr = process.communicate()
                    pytest.fail(
                        f"conformance runner did not finish within {timeout}s"
                        f"\n{stdout}\n{stderr}"
                    )
                processes, tracebacks, stdout, stderr = _dump_and_kill(
                    process, Path(traceback_dir)
                )
                pytest.fail(
                    f"conformance runner did not finish within {timeout}s, with"
                    f" these processes running:\n{processes}\n\nPython stacks:"
                    f"\n{tracebacks}\n\nrunner stdout:\n{stdout}\n\nrunner"
                    f" stderr, ending with Go stacks:\n{stderr}"
                )
    if process.returncode != 0:
        pytest.fail(f"\n{stdout}\n{stderr}")


def _dump_and_kill(
    process: subprocess.Popen[str], traceback_dir: Path
) -> tuple[str, str, str, str]:
    """Collect stacks from a hung process group, then kill it.

    Python processes write their stacks to traceback_dir on SIGUSR1, which Go
    ignores. SIGQUIT then makes the Go runner print its goroutines to stderr
    and exit.
    """
    pgid = process.pid
    processes = _list_process_group(pgid)
    os.killpg(pgid, signal.SIGUSR1)
    time.sleep(1)
    os.killpg(pgid, signal.SIGQUIT)
    try:
        stdout, stderr = process.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(pgid, signal.SIGKILL)
        stdout, stderr = process.communicate()
    else:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
    # Processes that exited before SIGUSR1 leave only the header line.
    tracebacks = "\n".join(
        text
        for p in sorted(traceback_dir.iterdir())
        if (text := p.read_text()).count("\n") > 1
    )
    return processes, tracebacks, stdout, stderr


def _list_process_group(pgid: int) -> str:
    ps = subprocess.run(
        ["ps", "-eo", "pid,ppid,pgid,stat,etime,args"],
        capture_output=True,
        text=True,
        check=False,
    )
    header, *rows = ps.stdout.splitlines()
    return "\n".join([header, *(r for r in rows if r.split()[2] == str(pgid))])


@pytest.mark.parametrize("server", ["gunicorn", "pyvoy"])
def test_server_sync(server: str, cov: Coverage) -> None:
    args = maybe_patch_args_with_debug(
        [sys.executable, _server_py_path, "--mode", "sync", "--server", server]
    )
    # WSGI applications can't enforce timeouts while a handler is blocked, and
    # the conformance handlers block for longer than the timeout.
    opts = ["--skip", "Timeouts/**"]
    match server:
        case "gunicorn":
            # gunicorn doesn't support HTTP/2 or 3
            opts += ["--skip", "**/HTTPVersion:2/**", "--skip", "**/HTTPVersion:3/**"]

    run_conformance(
        [
            "go",
            "run",
            f"connectrpc.com/conformance/cmd/connectconformance@{VERSION_CONFORMANCE}",
            "--conf",
            _config_path,
            "--mode",
            "server",
            *opts,
            *_known_flaky,
            "--",
            *args,
        ],
        coverage_env(cov),
    )


@pytest.mark.parametrize("server", ["pyvoy", "pyvoy-trio", "uvicorn"])
def test_server_async(server: str, cov: Coverage) -> None:
    args = maybe_patch_args_with_debug(
        [sys.executable, _server_py_path, "--mode", "async", "--server", server]
    )
    opts = []
    match server:
        case "uvicorn":
            # uvicorn doesn't support HTTP/2 or 3
            opts = ["--skip", "**/HTTPVersion:2/**", "--skip", "**/HTTPVersion:3/**"]
    run_conformance(
        [
            "go",
            "run",
            f"connectrpc.com/conformance/cmd/connectconformance@{VERSION_CONFORMANCE}",
            "-v",
            "--conf",
            _config_path,
            "--mode",
            "server",
            *opts,
            *_known_flaky,
            "--known-flaky",
            # There is a race between our server and the conformance runner itself completing a timeout
            # that causes errors.
            # TODO: Remove after https://github.com/connectrpc/conformance/pull/1155
            "Timeouts/**",
            "--",
            *args,
        ],
        coverage_env(cov),
    )
