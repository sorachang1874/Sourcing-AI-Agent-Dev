from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TYPES_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.ts"
SCHEMA_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.schema.json"
ADAPTER_PATH = REPO_ROOT / "contracts" / "frontend_api_adapter.ts"
DEMO_API_PATH = REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts"

PIN_FIELDS = ("request_schema_version", "request_schema_digest")


def _typescript_interface(source: str, name: str) -> str:
    return source.split(f"export interface {name}", 1)[1].split("\n}", 1)[0]


def _typescript_function(source: str, name: str) -> str:
    return source.split(f"function {name}", 1)[1].split("\n}", 1)[0]


def _exported_typescript_function(source: str, name: str) -> str:
    return source.split(f"export function {name}", 1)[1].split("\n}", 1)[0]


def test_operation_records_expose_optional_physical_request_schema_pins() -> None:
    types_source = TYPES_PATH.read_text(encoding="utf-8")
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    for record_name in ("OperationActionRecord", "OperationRunRecord"):
        interface = _typescript_interface(types_source, record_name)
        properties = schema["$defs"][record_name]["properties"]
        required = set(schema["$defs"][record_name].get("required", []))
        for field in PIN_FIELDS:
            assert f"{field}?: string;" in interface
            assert properties[field] == {"type": "string"}
            assert field not in required


def test_public_adapter_round_trips_optional_pins_without_deriving_them() -> None:
    adapter_source = ADAPTER_PATH.read_text(encoding="utf-8")

    for mapper_name in ("mapOperationActionRecord", "mapOperationRunRecord"):
        mapper = _exported_typescript_function(adapter_source, mapper_name)
        for field in PIN_FIELDS:
            assert f"{field}: asOptionalString(source.{field})" in mapper
        assert "request_schema_version:" in mapper
        assert "request_schema_digest:" in mapper


def test_demo_mapper_preserves_optional_and_empty_schema_less_pins() -> None:
    demo_source = DEMO_API_PATH.read_text(encoding="utf-8")

    for record_name in ("OperationActionRecord", "OperationRunRecord"):
        interface = _typescript_interface(demo_source, record_name)
        assert "requestSchemaVersion?: string;" in interface
        assert "requestSchemaDigest?: string;" in interface

    for mapper_name in ("deriveOperationActionRecord", "deriveOperationRunRecord"):
        mapper = _typescript_function(demo_source, mapper_name)
        assert "requestSchemaVersion: asOptionalString(record.request_schema_version)" in mapper
        assert "requestSchemaDigest: asOptionalString(record.request_schema_digest)" in mapper

    optional_string = _typescript_function(demo_source, "asOptionalString")
    assert 'typeof value === "string" ? value : undefined' in optional_string
    assert "trim" not in optional_string
