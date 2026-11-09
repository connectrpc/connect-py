from __future__ import annotations

import re
from typing import TYPE_CHECKING

from protobuf.plugin import escape_public_identifier, python_keywords

if TYPE_CHECKING:
    from protobuf import DescMethod

_RESERVED = frozenset(
    python_keywords()
    | {
        # While not strictly necessary, we go ahead and share reserved names for
        # service and client to have the same method names in generated code for
        # both.
        "desc",
        "execute_unary",
        "execute_client_stream",
        "execute_server_stream",
        "execute_bidi_stream",
    }
)

_RE_UPPER_TO_LOWER = re.compile("([^_])([A-Z][a-z]+)")
_RE_LOWER_TO_UPPER = re.compile("([a-z])([A-Z])")


def method_local_name(method: DescMethod) -> str:
    """Return the name of the Python method generated for a Protobuf method."""
    return escape_public_identifier(_pascal_to_snake_case(method.name), _RESERVED)


def _pascal_to_snake_case(text: str) -> str:
    """Convert a PascalCase name to snake_case."""
    s1 = _RE_UPPER_TO_LOWER.sub(r"\1_\2", text)
    return _RE_LOWER_TO_UPPER.sub(r"\1_\2", s1).lower()
