from __future__ import annotations

from importlib.util import find_spec

# The trio server tests import trio and hypercorn's trio worker at collection.
collect_ignore = [] if find_spec("trio") else ["test_server_trio_hypercorn.py"]
