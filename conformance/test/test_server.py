from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _util import VERSION_CONFORMANCE, coverage_env, maybe_patch_args_with_debug

if TYPE_CHECKING:
    from coverage import Coverage

_current_dir = Path(__file__).parent
_server_py_path = str(_current_dir / "server.py")
_config_path = str(_current_dir / "config.yaml")


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


@pytest.mark.parametrize("server", ["gunicorn", "pyvoy"])
def test_server_sync(server: str, cov: Coverage) -> None:
    args = maybe_patch_args_with_debug(
        [sys.executable, _server_py_path, "--mode", "sync", "--server", server]
    )
    opts = []
    match server:
        case "gunicorn":
            # gunicorn doesn't support HTTP/2 or 3
            opts = ["--skip", "**/HTTPVersion:2/**", "--skip", "**/HTTPVersion:3/**"]

    result = subprocess.run(
        [
            "go",
            "run",
            f"connectrpc.com/conformance/cmd/connectconformance@{VERSION_CONFORMANCE}",
            "--conf",
            _config_path,
            "--mode",
            "server",
            # Print an HTTP trace with timings for each failing case.
            "--trace",
            *opts,
            *_known_flaky,
            "--",
            *args,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=coverage_env(cov),
    )
    if result.returncode != 0:
        pytest.fail(f"\n{result.stdout}\n{result.stderr}")


@pytest.mark.parametrize("server", ["pyvoy", "uvicorn"])
def test_server_async(server: str, cov: Coverage) -> None:
    args = maybe_patch_args_with_debug(
        [sys.executable, _server_py_path, "--mode", "async", "--server", server]
    )
    opts = []
    match server:
        case "uvicorn":
            # uvicorn doesn't support HTTP/2 or 3
            opts = ["--skip", "**/HTTPVersion:2/**", "--skip", "**/HTTPVersion:3/**"]
    result = subprocess.run(
        [
            "go",
            "run",
            f"connectrpc.com/conformance/cmd/connectconformance@{VERSION_CONFORMANCE}",
            "-v",
            "--conf",
            _config_path,
            "--mode",
            "server",
            # Print an HTTP trace with timings for each failing case.
            "--trace",
            *opts,
            *_known_flaky,
            "--",
            *args,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=coverage_env(cov),
    )
    if result.returncode != 0:
        pytest.fail(f"\n{result.stdout}\n{result.stderr}")
