from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

from protobuf import (
    DescEnum,
    DescFieldValueEnum,
    DescFieldValueList,
    DescFieldValueMap,
    DescFieldValueMessage,
    DescMessage,
)
from pydantic import TypeAdapter

from ._router import MESSAGE_MARKER, METHOD_ATTRIBUTE

if TYPE_CHECKING:
    from collections.abc import Iterable

    from fastapi import FastAPI
    from protobuf import DescMethod

_REF_PREFIX = "#/components/schemas/"


def add_connect_schemas(app: FastAPI) -> None:
    """Add the schemas of Connect messages to the application's OpenAPI schema.

    The routes created by [connect_router][connectrpc_fastapi.connect_router]
    document their messages with placeholder schemas. This replaces them with
    references to the messages' schemas, keyed by type name, and adds the schemas.
    Without it, the OpenAPI schema is valid and documents the messages as objects
    without fields.

    Call it after any other replacement of the application's `openapi` method.
    Routers may be included before or after. The schema is returned by the application's
    `openapi` method. The application's `openapi_schema` attribute keeps the
    placeholders.

    Args:
        app: The FastAPI application serving Connect routes.

    Raises:
        ValueError: If a schema of a message's type name is already present with
            different content. Raised by the application's `openapi` method.

    """
    original = app.openapi
    processed: tuple[dict[str, Any], dict[str, Any]] | None = None

    def openapi() -> dict[str, Any]:
        """Return a copy of the OpenAPI schema with the Connect message schemas added.

        The copy is kept until FastAPI generates a new schema.
        """
        nonlocal processed
        source = original()
        if processed is not None and processed[0] is source:
            return processed[1]
        schema = copy.deepcopy(source)
        messages = _route_messages(app.routes)
        placeholders = _find_placeholders(schema, messages)
        if placeholders:
            components = schema.setdefault("components", {}).setdefault("schemas", {})
            schemas = message_schemas(
                message
                for type_name, definitions in messages.items()
                if type_name in placeholders
                for message in definitions
            )
            for name, message_schema in schemas.items():
                existing = components.get(name)
                if existing is not None and existing != message_schema:
                    msg = f"OpenAPI schema {name} already exists with different content"
                    raise ValueError(msg)
            for type_name, nodes in placeholders.items():
                for node in nodes:
                    node.clear()
                    node["$ref"] = _REF_PREFIX + type_name
            components.update(schemas)
        processed = (source, schema)
        return schema

    app.openapi = openapi  # type: ignore[method-assign]


def _route_messages(routes: Iterable[Any]) -> dict[str, list[DescMessage]]:
    """Return the messages of the Connect routes, by type name.

    Routes of included routers are found through FastAPI's wrapper of the included
    router. Routes FastAPI represents differently are not found, and their
    messages stay documented as placeholders.
    """
    messages: dict[str, list[DescMessage]] = {}

    def add(message: DescMessage) -> None:
        definitions = messages.setdefault(message.type_name, [])
        if not any(definition is message for definition in definitions):
            definitions.append(message)

    def visit(routes: Iterable[Any]) -> None:
        for route in routes:
            method: DescMethod | None = getattr(
                getattr(route, "endpoint", None), METHOD_ATTRIBUTE, None
            )
            if method is not None:
                add(method.input)
                add(method.output)
            included = getattr(route, "original_router", None)
            if included is not None:
                visit(getattr(included, "routes", ()))

    visit(routes)
    return messages


def _find_placeholders(
    schema: Any, messages: dict[str, list[DescMessage]]
) -> dict[str, list[dict[str, Any]]]:
    """Return the placeholder schemas of the messages, by type name."""
    placeholders: dict[str, list[dict[str, Any]]] = {}

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            type_name = node.get(MESSAGE_MARKER)
            if isinstance(type_name, str):
                if type_name in messages:
                    placeholders.setdefault(type_name, []).append(node)
                return
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(schema)
    return placeholders


def message_schemas(messages: Iterable[DescMessage]) -> dict[str, Any]:
    """Return the JSON schemas of the messages, keyed by type name.

    The first of several messages with the same type name is used.
    """
    all_messages: dict[str, DescMessage] = {}
    for message in messages:
        _collect_messages(message, all_messages)
    schemas, definitions = TypeAdapter.json_schemas(
        [
            (type_name, "validation", TypeAdapter(message.type))
            for type_name, message in all_messages.items()
        ],
        ref_template=_REF_PREFIX + "{model}",
    )
    defs = definitions.get("$defs", {})
    names: dict[str, str] = {}
    for (type_name, _mode), schema in schemas.items():
        ref = schema.get("$ref")
        if ref is None:
            msg = f"pydantic returned no reference for the schema of {type_name}"
            raise ValueError(msg)
        names[str(ref).removeprefix(_REF_PREFIX)] = type_name
    pydantic_names = {type_name: name for name, type_name in names.items()}
    for type_name, message in all_messages.items():
        names.update(_enum_names(message, defs[pydantic_names[type_name]]))
    unnamed = set(defs) - set(names)
    if unnamed:
        msg = f"type names of schemas are unknown: {', '.join(sorted(unnamed))}"
        raise ValueError(msg)
    return {
        names[name]: _rename_refs(definition, names)
        for name, definition in defs.items()
    }


def _enum_names(message: DescMessage, definition: dict[str, Any]) -> dict[str, str]:
    """Return pydantic's names of the enums of the message's fields, by type name."""
    names: dict[str, str] = {}
    for field in message.fields:
        property_schema = definition.get("properties", {}).get(field.json_name, {})
        match field.value:
            case DescFieldValueEnum(enum=enum):
                ref = property_schema.get("$ref")
            case DescFieldValueList(element=DescEnum() as enum):
                ref = property_schema.get("items", {}).get("$ref")
            case DescFieldValueMap(value=DescEnum() as enum):
                ref = property_schema.get("additionalProperties", {}).get("$ref")
            case _:
                continue
        if isinstance(ref, str):
            names[ref.removeprefix(_REF_PREFIX)] = enum.type_name
    return names


def _collect_messages(message: DescMessage, into: dict[str, DescMessage]) -> None:
    if message.type_name in into:
        return
    into[message.type_name] = message
    for field in message.fields:
        match field.value:
            case DescFieldValueMessage(message=nested):
                _collect_messages(nested, into)
            case DescFieldValueList(element=element) | DescFieldValueMap(value=element):
                if isinstance(element, DescMessage):
                    _collect_messages(element, into)
            case _:
                pass


def _rename_refs(schema: Any, names: dict[str, str]) -> Any:
    if isinstance(schema, dict):
        return {
            key: (
                _REF_PREFIX + names[value.removeprefix(_REF_PREFIX)]
                if key == "$ref" and isinstance(value, str)
                else _rename_refs(value, names)
            )
            for key, value in schema.items()
        }
    if isinstance(schema, list):
        return [_rename_refs(item, names) for item in schema]
    return schema
