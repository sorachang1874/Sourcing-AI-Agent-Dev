from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any


class MiniDraft202012Error(ValueError):
    """Raised when a payload violates the supported Draft 2020-12 subset."""


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def load_contract_schema(filename: str) -> dict[str, Any]:
    path = project_root() / "contracts" / filename
    schema = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(schema, dict):
        raise MiniDraft202012Error("contract_schema_not_object")
    return schema


def contract_schema_sha256(filename: str) -> str:
    path = project_root() / "contracts" / filename
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json_equal(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return set(left) == set(right) and all(_json_equal(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(_json_equal(a, b) for a, b in zip(left, right, strict=True))
    return left == right


def _resolve_ref(root: dict[str, Any], ref: Any) -> Any:
    if not isinstance(ref, str) or not ref.startswith("#/"):
        raise MiniDraft202012Error("unsupported_schema_ref")
    target: Any = root
    for token in ref[2:].split("/"):
        decoded = token.replace("~1", "/").replace("~0", "~")
        if not isinstance(target, dict) or decoded not in target:
            raise MiniDraft202012Error("unresolved_schema_ref")
        target = target[decoded]
    return target


def _type_matches(instance: Any, expected: str) -> bool:
    checks = {
        "array": lambda value: isinstance(value, list),
        "boolean": lambda value: isinstance(value, bool),
        "integer": lambda value: type(value) is int,
        "null": lambda value: value is None,
        "number": lambda value: type(value) in {int, float} and math.isfinite(value),
        "object": lambda value: isinstance(value, dict),
        "string": lambda value: isinstance(value, str),
    }
    if expected not in checks:
        raise MiniDraft202012Error("unsupported_schema_type")
    return checks[expected](instance)


def schema_errors(
    instance: Any,
    schema: Any,
    *,
    root: dict[str, Any] | None = None,
    path: str = "$",
) -> list[str]:
    if root is None:
        if not isinstance(schema, dict):
            return [f"{path}: schema_not_object"]
        root = schema
    if schema is True:
        return []
    if schema is False:
        return [f"{path}: false_schema"]
    if not isinstance(schema, dict):
        return [f"{path}: schema_not_object"]
    if "$ref" in schema:
        return schema_errors(instance, _resolve_ref(root, schema["$ref"]), root=root, path=path)

    errors: list[str] = []
    for child in schema.get("allOf", []):
        errors.extend(schema_errors(instance, child, root=root, path=path))
    if "oneOf" in schema:
        matches = sum(not schema_errors(instance, child, root=root, path=path) for child in schema["oneOf"])
        if matches != 1:
            errors.append(f"{path}: oneOf")
    if "anyOf" in schema:
        matches = sum(not schema_errors(instance, child, root=root, path=path) for child in schema["anyOf"])
        if matches < 1:
            errors.append(f"{path}: anyOf")
    if "const" in schema and not _json_equal(instance, schema["const"]):
        errors.append(f"{path}: const")
    if "enum" in schema and not any(_json_equal(instance, option) for option in schema["enum"]):
        errors.append(f"{path}: enum")

    expected_type = schema.get("type")
    if expected_type is not None:
        allowed = [expected_type] if isinstance(expected_type, str) else expected_type
        if not isinstance(allowed, list) or not all(isinstance(item, str) for item in allowed):
            raise MiniDraft202012Error("invalid_schema_type")
        if not any(_type_matches(instance, item) for item in allowed):
            errors.append(f"{path}: type")
            return errors

    if isinstance(instance, str):
        if len(instance) < schema.get("minLength", 0):
            errors.append(f"{path}: minLength")
        if "maxLength" in schema and len(instance) > schema["maxLength"]:
            errors.append(f"{path}: maxLength")
        if "pattern" in schema and re.search(schema["pattern"], instance) is None:
            errors.append(f"{path}: pattern")

    if type(instance) in {int, float}:
        if not math.isfinite(instance):
            errors.append(f"{path}: non_finite")
        if "minimum" in schema and instance < schema["minimum"]:
            errors.append(f"{path}: minimum")
        if "maximum" in schema and instance > schema["maximum"]:
            errors.append(f"{path}: maximum")

    if isinstance(instance, dict):
        if len(instance) < schema.get("minProperties", 0):
            errors.append(f"{path}: minProperties")
        if "maxProperties" in schema and len(instance) > schema["maxProperties"]:
            errors.append(f"{path}: maxProperties")
        for required in schema.get("required", []):
            if required not in instance:
                errors.append(f"{path}: missing:{required}")
        property_names = schema.get("propertyNames")
        if property_names is not None:
            for key in instance:
                errors.extend(schema_errors(key, property_names, root=root, path=f"{path}.<key>"))
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", {})
        for key, value in instance.items():
            if key in properties:
                errors.extend(schema_errors(value, properties[key], root=root, path=f"{path}.{key}"))
            elif additional is False:
                errors.append(f"{path}: additional:{key}")
            elif isinstance(additional, dict) or additional is True:
                errors.extend(schema_errors(value, additional, root=root, path=f"{path}.{key}"))
            else:
                raise MiniDraft202012Error("invalid_additional_properties_schema")

    if isinstance(instance, list):
        if len(instance) < schema.get("minItems", 0):
            errors.append(f"{path}: minItems")
        if "maxItems" in schema and len(instance) > schema["maxItems"]:
            errors.append(f"{path}: maxItems")
        if schema.get("uniqueItems") is True:
            identities = [canonical_json(value) for value in instance]
            if len(identities) != len(set(identities)):
                errors.append(f"{path}: uniqueItems")
        item_schema = schema.get("items")
        if item_schema is not None:
            for index, value in enumerate(instance):
                errors.extend(schema_errors(value, item_schema, root=root, path=f"{path}[{index}]"))
    return errors


def assert_schema_valid(instance: Any, filename: str) -> None:
    schema = load_contract_schema(filename)
    errors = schema_errors(instance, schema)
    if errors:
        raise MiniDraft202012Error(f"schema_validation_failed:{filename}:{errors[0]}")
