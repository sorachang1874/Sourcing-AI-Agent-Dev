"""Type-strict comparison helpers for persisted JSON contracts."""

from __future__ import annotations

import json
import math
from typing import Any


class JsonContractShapeError(ValueError):
    """Persisted JSON is invalid or has the wrong top-level container."""


def decode_json_contract(value: Any, *, expected_type: type[dict] | type[list]) -> dict[Any, Any] | list[Any]:
    """Decode persisted JSON without normalizing malformed or wrong-container values."""

    if type(value) is expected_type:
        return dict(value) if expected_type is dict else list(value)
    if not isinstance(value, str) or not value:
        raise JsonContractShapeError("persisted_json_missing_or_non_text")
    parsed = loads_json_contract_strict(value)
    if type(parsed) is not expected_type:
        raise JsonContractShapeError(f"persisted_json_expected_{expected_type.__name__}")
    return dict(parsed) if expected_type is dict else list(parsed)


def json_contract_equal(left: Any, right: Any) -> bool:
    """Compare JSON values by exact JSON type and content, never by serialization.

    ``json.dumps`` coercion must never decide contract equality: object keys
    must be strings (``{1: "x"}`` never equals ``{"1": "x"}``), numbers must be
    finite (``NaN``/``Infinity`` are not contract values), and non-JSON types
    never alias canonical JSON (``(1, 2)`` never equals ``[1, 2]``).  ``bool``
    is distinct from ``int`` and ``int`` from ``float``, so ``1`` never equals
    ``True`` or ``1.0``.
    """

    return _json_contract_value_equal(left, right)


def _json_contract_value_equal(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if left is None or type(left) in {str, bool, int}:
        return left == right
    if type(left) is float:
        return math.isfinite(left) and left == right
    if type(left) is dict:
        if any(type(key) is not str for key in left):
            return False
        if set(left) != set(right):
            return False
        return all(_json_contract_value_equal(left[key], right[key]) for key in left)
    if type(left) is list:
        if len(left) != len(right):
            return False
        return all(
            _json_contract_value_equal(left_item, right_item) for left_item, right_item in zip(left, right, strict=True)
        )
    return False


def _reject_duplicate_json_object_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise JsonContractShapeError(f"persisted_json_duplicate_key:{key}")
        result[key] = value
    return result


def _reject_non_finite_json_constant(value: str) -> Any:
    raise JsonContractShapeError(f"persisted_json_non_finite_number:{value}")


def _parse_finite_json_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise JsonContractShapeError(f"persisted_json_non_finite_number:{text}")
    return value


def _validate_json_contract_value(value: Any) -> Any:
    """Require one recursively exact JSON-typed value.

    Parsed contract payloads may contain only ``None``, ``str``, ``bool``,
    ``int``, finite ``float``, ``list``, and ``dict`` with string keys;
    anything else is non-canonical and must fail instead of being normalized.
    """

    if value is None or type(value) in {str, bool, int}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise JsonContractShapeError("persisted_json_non_finite_number")
        return value
    if type(value) is list:
        for item in value:
            _validate_json_contract_value(item)
        return value
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise JsonContractShapeError(f"persisted_json_non_string_key:{key!r}")
            _validate_json_contract_value(item)
        return value
    raise JsonContractShapeError(f"persisted_json_non_json_type:{type(value).__name__}")


def loads_json_contract_strict(text: str) -> Any:
    """Parse contract JSON, rejecting every non-canonical form.

    ``json.loads`` silently keeps the last value of a duplicated object key,
    accepts the non-standard ``NaN`` / ``Infinity`` / ``-Infinity`` constants,
    and lets overflowing exponents such as ``1e9999`` coerce to ``inf`` through
    the default float parser.  All three forms — plus any non-JSON-typed or
    non-string-keyed content — are non-canonical for persisted JSON contracts
    and must fail instead of being normalized into a value the contract never
    recorded.
    """

    try:
        parsed = json.loads(
            str(text),
            object_pairs_hook=_reject_duplicate_json_object_keys,
            parse_constant=_reject_non_finite_json_constant,
            parse_float=_parse_finite_json_float,
        )
    except JsonContractShapeError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise JsonContractShapeError("persisted_json_invalid") from exc
    return _validate_json_contract_value(parsed)
