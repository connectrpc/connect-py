from __future__ import annotations

import subprocess
from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).parent.parent
_RELEASE_PACKAGES = ("connectrpc", "protoc-gen-connectrpc")


def _package_version(package: str) -> str:
    return subprocess.run(
        ["uv", "version", "--short", "--package", package],
        cwd=_REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_joint_release_package_versions_match() -> None:
    versions = {package: _package_version(package) for package in _RELEASE_PACKAGES}

    assert len(set(versions.values())) == 1, (
        "packages published together by release.yaml must have the same version: "
        + ", ".join(f"{package}={version}" for package, version in versions.items())
    )
