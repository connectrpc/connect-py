# ruff: noqa: INP001 # a standalone script, not part of a package
"""Import a published package with its dependencies at their minimum versions.

The workspace resolves every package and dev tool together, so CI's
lowest-direct resolution can't lower a dependency below what another workspace
member or dev tool requires, and `tool.uv.sources` replaces published
dependencies with workspace code. This installs one package in an isolated
environment with only its own requirements, resolved to their lowest allowed
versions, and imports all of its modules there.

Usage: `uv run --no-project python scripts/check_dependency_floors.py <directory>`
"""

from __future__ import annotations

import argparse
import importlib
import pkgutil
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Package:
    """A published package in the workspace."""

    module: str
    # Dev group requirements the package needs at runtime without declaring them.
    dev_requirements: tuple[str, ...] = ()
    # Modules that need packages users install themselves.
    skipped: tuple[str, ...] = ()


PACKAGES = {
    ".": Package(
        "connectrpc",
        skipped=(
            "connectrpc.compat",
            "connectrpc.compression.brotli",
            "connectrpc.compression.zstd",
        ),
    ),
    "connectrpc-grpcreflect": Package("connectrpc_grpcreflect"),
    # Auto-instrumentation doesn't depend on the library it instruments.
    "connectrpc-otel": Package("connectrpc_otel", dev_requirements=("connectrpc",)),
    "protoc-gen-connectrpc": Package("protoc_gen_connectrpc"),
}


def requirements(directory: str) -> list[str]:
    """Return the requirements of the package in directory."""
    with (ROOT / directory / "pyproject.toml").open("rb") as f:
        pyproject = tomllib.load(f)
    dev = pyproject.get("dependency-groups", {}).get("dev", [])
    return [
        *pyproject["project"]["dependencies"],
        *(
            r
            for r in dev
            if isinstance(r, str)
            and re.split(r"[<>=!~;\[ ]", r, maxsplit=1)[0]
            in PACKAGES[directory].dev_requirements
        ),
    ]


def check(directory: str) -> int:
    """Run this script with --import in an environment at the package's floors."""
    command = [
        "uv",
        "run",
        "--isolated",
        "--no-project",
        "--no-sources",
        "--python",
        sys.executable,
        "--resolution",
        "lowest-direct",
        "--with",
        str(ROOT / directory),
    ]
    for requirement in requirements(directory):
        command += ["--with", requirement]
    command += ["python", __file__, "--import", directory]
    # The command is built from this repository's own pyproject.toml files.
    return subprocess.run(command, check=False).returncode  # noqa: S603


def import_all(directory: str) -> None:
    """Import every module of the package, except the skipped ones."""
    package = PACKAGES[directory]
    module = importlib.import_module(package.module)
    for info in pkgutil.walk_packages(module.__path__, package.module + "."):
        if not info.name.startswith(package.skipped):
            importlib.import_module(info.name)


def main() -> int:
    """Check the package in the directory given on the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", choices=PACKAGES)
    parser.add_argument(
        "--import",
        dest="import_all",
        action="store_true",
        help="import the package's modules in the current environment",
    )
    args = parser.parse_args()
    if args.import_all:
        import_all(args.directory)
        return 0
    return check(args.directory)


if __name__ == "__main__":
    sys.exit(main())
