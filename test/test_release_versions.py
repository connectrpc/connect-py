from __future__ import annotations

from importlib.metadata import version


def test_joint_release_package_versions_match() -> None:
    assert version("connectrpc") == version("protoc-gen-connectrpc")
