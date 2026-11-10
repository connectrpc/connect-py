"""Shared pytest configuration for every package in the repository."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session", params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    """Run async tests on both asyncio and trio."""
    return request.param


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Deselect tests marked `asyncio_only` when running on trio."""
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
