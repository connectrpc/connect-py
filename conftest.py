"""Configures async tests of every package in the repository."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session", params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    """Return each backend async tests run on, with one runner for the session."""
    return request.param


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Deselects `asyncio_only` tests on the other backends."""
    kept: list[pytest.Item] = []
    dropped: list[pytest.Item] = []
    for item in items:
        callspec = getattr(item, "callspec", None)
        backend = callspec.params.get("anyio_backend") if callspec is not None else None
        if backend not in (None, "asyncio") and item.get_closest_marker("asyncio_only"):
            dropped.append(item)
        else:
            kept.append(item)
    if dropped:
        config.hook.pytest_deselected(items=dropped)
        items[:] = kept
