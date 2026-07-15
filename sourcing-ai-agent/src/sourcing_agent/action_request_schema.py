"""Closed request-schema construction for owner-bound Agent actions."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any


class ActionRequestSchemaBuilder:
    """Build the one allowed closed ``input_payload``/``target_ref`` shape.

    Validation and canonical digest ownership remain in ``ToolSpec``. This
    builder only prevents action declarations from hand-rolling open roots or
    segments before they reach that canonical validator.
    """

    def build(
        self,
        *,
        input_properties: Mapping[str, Mapping[str, Any]],
        input_required: Sequence[str] = (),
        target_properties: Mapping[str, Mapping[str, Any]],
        target_required: Sequence[str] = (),
    ) -> dict[str, Any]:
        normalized_input = self._copy_properties(input_properties, segment="input_payload")
        normalized_target = self._copy_properties(target_properties, segment="target_ref")
        overlap = sorted(set(normalized_input) & set(normalized_target))
        if overlap:
            raise ValueError(f"action request fields cannot have dual owners: {','.join(overlap)}")
        required_input = self._normalize_required(
            input_required,
            properties=normalized_input,
            segment="input_payload",
        )
        required_target = self._normalize_required(
            target_required,
            properties=normalized_target,
            segment="target_ref",
        )
        return {
            "type": "object",
            "properties": {
                "input_payload": {
                    "type": "object",
                    "properties": normalized_input,
                    "required": required_input,
                    "additionalProperties": False,
                },
                "target_ref": {
                    "type": "object",
                    "properties": normalized_target,
                    "required": required_target,
                    "additionalProperties": False,
                },
            },
            "required": ["input_payload", "target_ref"],
            "additionalProperties": False,
        }

    @staticmethod
    def _copy_properties(
        properties: Mapping[str, Mapping[str, Any]],
        *,
        segment: str,
    ) -> dict[str, Any]:
        if not isinstance(properties, Mapping):
            raise ValueError(f"action request {segment} properties must be a mapping")
        copied: dict[str, Any] = {}
        for field_name, field_schema in properties.items():
            if (
                not isinstance(field_name, str)
                or not field_name
                or field_name != field_name.strip()
                or not isinstance(field_schema, Mapping)
            ):
                raise ValueError(f"action request {segment} property is invalid")
            try:
                copied[field_name] = json.loads(
                    json.dumps(
                        dict(field_schema),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                )
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(f"action request {segment} property is not JSON") from exc
        return copied

    @staticmethod
    def _normalize_required(
        required: Sequence[str],
        *,
        properties: Mapping[str, Any],
        segment: str,
    ) -> list[str]:
        normalized = list(required)
        if (
            any(not isinstance(field_name, str) or not field_name for field_name in normalized)
            or len(normalized) != len(set(normalized))
            or not set(normalized).issubset(properties)
        ):
            raise ValueError(f"action request {segment} required fields are invalid")
        return normalized


DEFAULT_ACTION_REQUEST_SCHEMA_BUILDER = ActionRequestSchemaBuilder()
