"""Track B B4.2 — byte-equivalence harness for spec-driven descriptor conversion.

Reads a JSON file of mapper specs (the classification workflow's MECHANICAL output), builds an
in-memory ``TableDescriptor`` for each, and compares ``descriptor.from_row(row)`` against the live
hand-written ``ControlPlaneStore._<method>`` across a battery of per-column value profiles
(absent / empty / typical / edge / native-object). Prints PASS/FAIL per method with the diff.

This is the deterministic correctness gate: only methods that PASS here are delegated in storage.py,
so an agent mis-transcription cannot ship. Usage:  python scripts/_descriptor_equiv_harness.py specs.json
"""

from __future__ import annotations

import json
import sys
from typing import Any

from sourcing_agent.control_plane_repository import Column, Kind, TableDescriptor
from sourcing_agent.storage import ControlPlaneStore

_KIND = {k.name: k for k in Kind}

PROFILES: dict[Kind, list[Any]] = {
    Kind.STR: ["", "  spaced  ", "value", None, 0, 123],
    Kind.INT: [0, 5, "7", None, ""],
    Kind.FLOAT: [0.0, 0.85, "0.5", None, ""],
    Kind.BOOL_INT: [0, 1, True, False, None, 2],
    Kind.JSON: ["{}", '{"a":1}', "[1]", "bad", {"x": 1}, None, "", '{"n":{"y":2}}'],
    Kind.JSON_LIST: ["[]", "[1,2]", '{"a":1}', "bad", [1, 2], None, "", '["x", null]'],
    Kind.JSON_STR_LIST: ["[]", '["a"," b ",""]', "bad", None, "", [1, " z "]],
}


def _alias(source_key: str):
    return lambda mapped, k=source_key: mapped.get(k)


def build_descriptor(spec: dict[str, Any]) -> TableDescriptor:
    cols = []
    for c in spec.get("columns", []):
        kind = _KIND[c["kind"]]
        field = c.get("field") or None
        read_default = c.get("read_default") or None
        cols.append(Column(c["name"], kind, field=field, read_default=read_default))
    derived = tuple((d["name"], _alias(d["source_key"])) for d in spec.get("derived", []))
    return TableDescriptor(
        table=spec.get("table") or spec["method"],
        columns=tuple(cols),
        pk=tuple(spec.get("pk", ())),
        derived=derived,
    )


def battery_rows(desc: TableDescriptor) -> list[dict[str, Any]]:
    cols = desc.columns
    max_len = max((len(PROFILES[c.kind]) for c in cols), default=1)
    rows: list[dict[str, Any]] = [{}]  # all-absent -> exercises read_default/defaults
    for i in range(max_len):
        row = {}
        for c in cols:
            vals = PROFILES[c.kind]
            row[c.name] = vals[i % len(vals)]
        rows.append(row)
    return rows


def main(path: str) -> int:
    specs = json.loads(open(path).read())
    if isinstance(specs, dict):
        specs = specs.get("mechanical", specs.get("mappers", []))
    passed, failed = [], []
    for spec in specs:
        method = spec["method"]
        hand = getattr(ControlPlaneStore, method, None)
        if hand is None:
            failed.append((method, "method not found on ControlPlaneStore"))
            continue
        try:
            desc = build_descriptor(spec)
        except Exception as exc:  # noqa: BLE001 — surface build errors as FAIL
            failed.append((method, f"descriptor build error: {exc!r}"))
            continue
        bad = None
        for row in battery_rows(desc):
            try:
                expected = hand(None, row)
            except Exception as exc:  # noqa: BLE001
                bad = f"hand mapper raised on {row!r}: {exc!r}"
                break
            actual = desc.from_row(row)
            if expected != actual:
                diff = {k: (expected.get(k), actual.get(k)) for k in set(expected) | set(actual)
                        if expected.get(k) != actual.get(k)}
                bad = f"diff on row {row!r}: {diff}"
                break
        if bad:
            failed.append((method, bad))
        else:
            passed.append(method)

    print(f"PASS {len(passed)} / FAIL {len(failed)} of {len(specs)} mechanical specs")
    for m in passed:
        print(f"  PASS {m}")
    for m, why in failed:
        print(f"  FAIL {m}: {why}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
