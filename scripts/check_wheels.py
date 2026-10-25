# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Test built wheels with every dependency at its lowest allowed version.

Each wheel is installed in its own environment with its package's test group,
resolving direct and transitive dependencies to their lowest versions. Then all
of the package's modules are imported and its tests are run.

Usage: `uv run --python 3.11 scripts/check_wheels.py dist/*.whl`
"""

from __future__ import annotations

import importlib
import os
import pkgutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Requirements each package's tests need beyond its test group.
EXTRA = {
    "connectrpc": [
        # trio doesn't bound sortedcontainers or idna. Their oldest releases
        # predate Python 3, and sortedcontainers 2.0 needs collections.Sequence.
        "sortedcontainers>=2.1",
        "idna>=2",
    ],
    "connectrpc_otel": [
        # The tests need this repository's connectrpc.
        "dist/connectrpc-*.whl",
        # Two of opentelemetry-instrumentation's transitive floors don't
        # install: packaging 18 needs pyparsing 2.0.2, whose sdist is corrupt,
        # and wrapt 1.0 predates Python 3.
        "packaging>=22",
        "wrapt>=1.14",
    ],
}

# Tests that need the workspace rather than installed packages.
IGNORED = {"connectrpc": ["test/test_release_versions.py"]}


def check(wheel: Path, envs: Path) -> None:
    """Install the wheel at its floors, import its modules, and run its tests."""
    module = wheel.name.split("-", 1)[0]
    directory = ROOT if module == "connectrpc" else ROOT / module.replace("_", "-")
    install = [str(wheel), str(ROOT / "example")]
    if (directory / "test").is_dir():
        install += ["--group", f"{directory / 'pyproject.toml'}:test"]
    for extra in EXTRA.get(module, []):
        install += [str(p) for p in ROOT.glob(extra)] or [extra]

    print(f"== {wheel.name}", flush=True)  # noqa: T201
    env = {**os.environ, "VIRTUAL_ENV": str(envs / module)}
    python = envs / module / "bin" / "python"
    # The commands are built from this repository's own wheels and pyproject files.
    subprocess.run(  # noqa: S603
        ["uv", "venv", "-q", "--python", "3.11", env["VIRTUAL_ENV"]],  # noqa: S607
        check=True,
    )
    subprocess.run(  # noqa: S603
        [  # noqa: S607
            "uv",
            "pip",
            "install",
            "-q",
            "--no-sources",
            "--resolution",
            "lowest",
            # Resolve unreleased wheels if we have bumped the floor to it
            "--find-links",
            str(wheel.parent),
            *install,
        ],
        check=True,
        env=env,
    )
    subprocess.run([python, __file__, "--import", module], check=True, cwd=directory)  # noqa: S603
    if (directory / "test").is_dir():
        ignored = [f"--ignore={i}" for i in IGNORED.get(module, [])]
        subprocess.run(  # noqa: S603
            [
                python,
                "-m",
                "pytest",
                "-q",
                "-p",
                "no:cacheprovider",
                "--import-mode=importlib",
                *ignored,
                "test",
            ],
            check=True,
            cwd=directory,
        )


def import_all(module: str) -> None:
    """Import every module of the package."""
    package = importlib.import_module(module)
    for info in pkgutil.walk_packages(package.__path__, module + "."):
        importlib.import_module(info.name)


def main() -> None:
    """Check the wheels given on the command line."""
    if sys.argv[1] == "--import":
        import_all(sys.argv[2])
        return
    with tempfile.TemporaryDirectory() as envs:
        for wheel in sys.argv[1:]:
            check(Path(wheel).resolve(), Path(envs))


if __name__ == "__main__":
    main()
