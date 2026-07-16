"""Type-strict comparison helpers for persisted JSON contracts."""

from __future__ import annotations

import json
from typing import Any


class JsonContractShapeError(ValueError):
    """Persisted JSON is invalid or has the wrong top-level container."""


def decode_json_contract(value: Any, *, expected_type: type[dict] | type[list]) -> dict[Any, Any] | list[Any]:
    """Decode persisted JSON without normalizing malformed or wrong-container values."""

    if type(value) is expected_type:
        return dict(value) if expected_type is dict else list(value)
    if not isinstance(value, str) or not value:
        raise JsonContractShapeError("persisted_json_missing_or_non_text")
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise JsonContractShapeError("persisted_json_invalid") from exc
    if type(parsed) is not expected_type:
        raise JsonContractShapeError(f"persisted_json_expected_{expected_type.__name__}")
    return dict(parsed) if expected_type is dict else list(parsed)


def json_contract_equal(left: Any, right: Any) -> bool:
    """Compare JSON values canonically without Python's bool/int equality aliasing."""

    try:
        return json.dumps(
            left,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ) == json.dumps(
            right,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        return False
