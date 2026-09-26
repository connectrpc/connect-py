from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _util import CONFORMANCE_RUNNER, coverage_env, maybe_patch_args_with_debug

if TYPE_CHECKING:
    from coverage import Coverage

_current_dir = Path(__file__).parent
_client_py_path = str(_current_dir / "client.py")
_config_path = str(_current_dir / "config.yaml")

_skipped_tests_sync = [
    # Need to use async APIs for proper cancellation support in Python.
    "--skip",
    "Client Cancellation/**",
]

# TODO: Investigate if this is an issue with the client.
# "http: invalid Read on closed Body" is common for issues with body handling in Go code,
# and the test itself is using an old, hacky approach to serve HTTP/1 with gRPC which
# expects HTTP/2. It seems reasonable the conformance runner may have some issues here
# but we don't seem to see failures in other repos so maybe not.
_flaky_tests = ["--known-flaky", "**/HTTPVersion:1/**/(grpc server impl)/**"]

# The client doesn't yet fail streams whose Connect response has no end-stream
# message.
_known_failing = ["--known-failing", "Connect Unexpected Responses/**/no-end-stream"]

_skipped_tests = []
if sys.platform == "darwin":
    # TODO: Investigate HTTP/3 conformance test failures on macOS more.
    # Currently, it seems to be an issue with the conformance runner itself and we see this log message.
    # 2026/03/05 01:54:19 failed to sufficiently increase receive buffer size (was: 768 kiB, wanted: 7168 kiB, got: 6144 kiB). See https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes for details.
    # known-flaky does not work for these errors for some reason so we just skip.
    _skipped_tests += ["--skip", "**/HTTPVersion:3/**"]


def test_client_sync(cov: Coverage | None) -> None:
    args = maybe_patch_args_with_debug(
        [sys.executable, _client_py_path, "--mode", "sync"]
    )

    result = subprocess.run(
        [
            *CONFORMANCE_RUNNER,
            "--conf",
            _config_path,
            "--mode",
            "client",
            *_skipped_tests_sync,
            *_skipped_tests,
            *_flaky_tests,
            *_known_failing,
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


def test_client_async(cov: Coverage | None) -> None:
    args = maybe_patch_args_with_debug(
        [sys.executable, _client_py_path, "--mode", "async"]
    )

    result = subprocess.run(
        [
            *CONFORMANCE_RUNNER,
            "--conf",
            _config_path,
            "--mode",
            "client",
            *_skipped_tests,
            *_flaky_tests,
            *_known_failing,
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
