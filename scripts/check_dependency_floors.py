# /// script
# requires-python = ">=3.11"
# dependencies = ["packaging==26.0"]
# ///
"""Test built wheels with their dependencies at their minimum versions.

The workspace resolves every package and dev tool together, so CI's
lowest-direct resolution can't lower a dependency below what another workspace
member or dev tool requires, and `tool.uv.sources` replaces published
dependencies with workspace code. This installs each wheel in its own isolated
environment with its package's test group, resolving every direct requirement
to its lowest allowed version, checks that each one got there, imports all of
the package's modules, and runs its tests.

Usage: `uv run --python 3.11 scripts/check_dependency_floors.py dist/*.whl`
"""

from __future__ import annotations

import argparse
import email
import importlib
import json
import pkgutil
import subprocess
import sys
import tomllib
import zipfile
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Package:
    """A published package, keyed by its module name."""

    directory: str
    # Modules that need packages users install themselves.
    skipped: tuple[str, ...] = ()
    # Tests that need the workspace rather than installed packages.
    ignored: tuple[str, ...] = ()


PACKAGES = {
    "connectrpc": Package(
        ".",
        skipped=(
            "connectrpc.compat",
            "connectrpc.compression.brotli",
            "connectrpc.compression.zstd",
        ),
        ignored=("test/test_release_versions.py",),
    ),
    "connectrpc_grpcreflect": Package("connectrpc-grpcreflect"),
    "connectrpc_otel": Package("connectrpc-otel"),
    "protoc_gen_connectrpc": Package("protoc-gen-connectrpc"),
}


def pyproject(directory: Path) -> dict:
    """Return the parsed pyproject.toml in directory."""
    with (directory / "pyproject.toml").open("rb") as f:
        return tomllib.load(f)


def wheel_requirements(wheel: Path) -> list[str]:
    """Return the requirements in the wheel's metadata, except optional ones."""
    with zipfile.ZipFile(wheel) as f:
        name = next(n for n in f.namelist() if n.endswith(".dist-info/METADATA"))
        message = email.message_from_bytes(f.read(name))
    return [r for r in message.get_all("Requires-Dist", []) if "extra ==" not in r]


def test_requirements(directory: Path) -> list[str]:
    """Return the test group in directory, with workspace members as paths.

    A workspace member named without a version, like `example`, is installed
    from its directory; one with a version comes from the index at its floor.
    """
    members = {
        pyproject(d)["project"]["name"]: d
        for m in [".", *pyproject(ROOT)["tool"]["uv"]["workspace"]["members"]]
        for d in [ROOT / m]
    }
    groups = pyproject(directory).get("dependency-groups", {})

    def expand(group: str) -> list[str]:
        return [
            r
            for entry in groups.get(group, [])
            for r in (
                expand(entry["include-group"]) if isinstance(entry, dict) else [entry]
            )
        ]

    return [str(members.get(r, r)) for r in expand("test")]


def floor(requirement: str) -> tuple[str, str] | None:
    """Return the requirement's name and lowest allowed version, if it has one."""
    from packaging.requirements import Requirement  # noqa: PLC0415 # see check
    from packaging.version import Version  # noqa: PLC0415

    parsed = Requirement(requirement)
    if parsed.marker and not parsed.marker.evaluate():
        return None
    versions = [
        s.version
        for s in parsed.specifier
        if s.operator in {">=", "==", "~="} and not s.version.endswith(".*")
    ]
    return (parsed.name, max(versions, key=Version)) if versions else None


def check(wheel: Path) -> bool:
    """Check one wheel in an environment at its floors and return whether it passed.

    This script also runs inside that environment with --versions or --import,
    which only use the standard library, so packaging is imported where needed.
    """
    from packaging.version import Version  # noqa: PLC0415

    # Wheel filenames start with the distribution name, which matches the module.
    module = wheel.name.split("-", 1)[0]
    package = PACKAGES[module]
    directory = ROOT / package.directory
    requirements = [*wheel_requirements(wheel), *test_requirements(directory)]
    run = [
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
        str(wheel),
    ]
    for requirement in requirements:
        run += ["--with", requirement]
    print(f"Checking {wheel.name}", flush=True)  # noqa: T201 # labels uv's output

    floors = dict(
        f for r in requirements if not Path(r).is_absolute() and (f := floor(r))
    )
    # The commands are built from this repository's own wheels and pyproject files.
    versions = subprocess.run(  # noqa: S603
        [*run, "python", __file__, "--versions", *floors],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout
    passed = True
    for name, installed in json.loads(versions).items():
        if Version(installed) != Version(floors[name]):
            print(f"  {name} {installed} is above its floor {floors[name]}")  # noqa: T201
            passed = False

    commands = [["python", __file__, "--import", module]]
    if (directory / "test").is_dir():
        ignored = [f"--ignore={i}" for i in package.ignored]
        commands.append(
            [
                "pytest",
                "-p",
                "no:cacheprovider",
                "--import-mode=importlib",
                *ignored,
                "test",
            ]
        )
    for command in commands:
        result = subprocess.run([*run, *command], check=False, cwd=directory)  # noqa: S603
        passed &= result.returncode == 0
    return passed


def import_all(module: str) -> None:
    """Import every module of the package, except the skipped ones."""
    skipped = PACKAGES[module].skipped
    package = importlib.import_module(module)
    for info in pkgutil.walk_packages(package.__path__, module + "."):
        if not info.name.startswith(skipped):
            importlib.import_module(info.name)


def main() -> int:
    """Check the wheels given on the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheels", nargs="*", type=Path)
    parser.add_argument(
        "--import",
        dest="module",
        choices=PACKAGES,
        help="import the package's modules in the current environment",
    )
    parser.add_argument(
        "--versions",
        nargs="+",
        metavar="NAME",
        help="print the installed versions of these distributions as JSON",
    )
    args = parser.parse_args()
    if args.versions:
        print(json.dumps({n: metadata.version(n) for n in args.versions}))  # noqa: T201
        return 0
    if args.module:
        import_all(args.module)
        return 0
    if not args.wheels:
        parser.error("no wheels given")
    results = [check(wheel.resolve()) for wheel in args.wheels]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
