from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _util import CONFORMANCE_RUNNER, coverage_env, maybe_patch_args_with_debug

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
            *CONFORMANCE_RUNNER,
            "--conf",
            _config_path,
            "--mode",
            "server",
            *opts,
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
            *CONFORMANCE_RUNNER,
            "-v",
            "--conf",
            _config_path,
            "--mode",
            "server",
            *opts,
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
