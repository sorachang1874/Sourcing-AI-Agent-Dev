"""Track B B4.2 — typed control-plane repository foundation.

Replaces the inherited per-table hand-written row mappers and ad-hoc SQL with declarative
``TableDescriptor``s: ONE source of truth per table (columns + types + primary key + conflict policy)
that generates the row<->dict mapping and the upsert/select/delete SQL, executed over the existing
``LiveControlPlanePostgresAdapter`` primitives. Per-domain ``Repository`` subclasses compose descriptors
with domain logic; ``ControlPlaneStore`` delegates to them and callers migrate to the repositories
directly (owner-ratified 2026-06-21).

Design notes:
- Column ``Kind`` is the typed contract. ``JSON``/``JSON_STR_LIST`` today (de)serialize a TEXT column
  that holds JSON; the B4.2 schema migration adds ``JSONB`` and ``TIMESTAMPTZ`` kinds whose only change
  is the storage/coercion, leaving every descriptor and every caller untouched. That is the whole point
  of routing all (de)serialization through one declarative place.
- Row values arrive as ``dict`` from the PG adapter (psycopg ``dict_row``). ``_row_value`` also tolerates
  mapping-like rows for safety.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import Enum
from typing import Any


class Kind(Enum):
    """The typed contract for a control-plane column."""

    STR = "str"  # text column -> str(v or "")
    INT = "int"  # integer column -> int(v or 0)
    BOOL_INT = "bool_int"  # 0/1 integer column <-> bool
    JSON = "json"  # text holding a JSON object -> dict (non-dict/parse-fail -> {}); == _loads_json_dict
    JSON_LIST = "json_list"  # text holding a JSON array -> list verbatim (non-list/fail -> []); == _loads_json_list
    JSON_STR_LIST = "json_str_list"  # text holding a JSON array -> list[non-empty stripped str] (filtered)
    # B4.2 schema migration will add: JSONB, TIMESTAMPTZ (same descriptor API, different coercion).


def _row_value(row: Any, name: str, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(name, default)
    try:
        return row[name]
    except (KeyError, IndexError, TypeError):
        return default


@dataclass(frozen=True)
class Column:
    """One PG column + how it maps to/from the public dict.

    ``field`` is the public dict key (defaults to the column name). It differs from the column name for
    JSON list columns whose public name drops the ``_json`` suffix (e.g. column ``source_shards_json`` ->
    field ``source_shards``).
    """

    name: str
    kind: Kind = Kind.STR
    field: str | None = None
    default: Any = None  # WRITE default when the (stripped) value is falsy, e.g. status -> "queued"
    read_default: Any = None  # READ default when the stored value is falsy (mapper `str(x or "queued")` fallback)

    @property
    def key(self) -> str:
        return self.field or self.name


def _decode(col: Column, value: Any) -> Any:
    if col.kind is Kind.STR:
        text = str(value or "")
        if not text and col.read_default is not None:
            return str(col.read_default)
        return text
    if col.kind is Kind.INT:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0
    if col.kind is Kind.BOOL_INT:
        try:
            return bool(int(value or 0))
        except (TypeError, ValueError):
            return False
    if col.kind is Kind.JSON:  # == _loads_json_dict: dict passthrough; non-dict/parse-fail -> {}
        if isinstance(value, dict):
            return dict(value)
        try:
            parsed = json.loads(str(value or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    if col.kind is Kind.JSON_LIST:  # == _loads_json_list: list/tuple passthrough; non-list/fail -> []
        if isinstance(value, (list, tuple)):
            return list(value)
        try:
            parsed = json.loads(str(value or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
        return list(parsed) if isinstance(parsed, list) else []
    if col.kind is Kind.JSON_STR_LIST:
        try:
            parsed = json.loads(value if value not in (None, "") else "[]")
        except (json.JSONDecodeError, TypeError):
            parsed = []
        return [str(item).strip() for item in (parsed or []) if str(item).strip()]
    return value


def _encode(col: Column, value: Any, *, strip_text: bool, clamp_int: bool) -> Any:
    if col.kind in (Kind.JSON, Kind.JSON_LIST, Kind.JSON_STR_LIST):
        empty: Any = {} if col.kind is Kind.JSON else []
        return json.dumps(value if value is not None else empty, ensure_ascii=False)
    if col.kind is Kind.INT:
        try:
            number = int(value or 0)
        except (TypeError, ValueError):
            number = 0
        return max(0, number) if clamp_int else number
    if col.kind is Kind.BOOL_INT:
        return 1 if bool(value) else 0
    text = str(value or "")
    if strip_text:
        text = text.strip()
    if not text and col.default is not None:
        text = str(col.default)
    return text


@dataclass(frozen=True)
class TableDescriptor:
    """Declarative single source of truth for one control-plane table.

    Generates the row<->dict mapping and the upsert SQL. The conflict policy is ``REPLACE_ALL`` by default
    (``ON CONFLICT (pk) DO UPDATE SET <non-pk> = excluded.<col>``), which is correct for the many tables
    that compute their merged row in Python before writing (read-merge-write). Tables that need per-column
    SQL merge (GREATEST / keep-if-nonempty) declare ``merge`` overrides; today this foundation only needs
    REPLACE_ALL for the pilot domain, and the override hook is the extension point.
    """

    table: str
    columns: tuple[Column, ...]
    pk: tuple[str, ...]
    merge: dict[str, str] = dataclass_field(default_factory=dict)  # column -> SQL expr override for DO UPDATE
    strip_text: bool = True  # write-normalize: strip whitespace on text columns (control-plane default)
    clamp_non_negative_int: bool = True  # write-normalize: max(0, int) on integer columns
    # Derived public fields not backed by a column, computed from the column-mapped dict (e.g. a lease's
    # `expired` from its expires-at text). Appended after the column mapping; read-only (never persisted).
    derived: tuple[tuple[str, Callable[[dict[str, Any]], Any]], ...] = ()

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def from_row(self, row: Any) -> dict[str, Any]:
        """Map a DB row to the public dict (+ derived fields). Returns {} for a missing row."""
        if row is None:
            return {}
        mapped = {c.key: _decode(c, _row_value(row, c.name)) for c in self.columns}
        for field_name, compute in self.derived:
            mapped[field_name] = compute(mapped)
        return mapped

    def from_rows(self, rows: Any) -> list[dict[str, Any]]:
        return [self.from_row(r) for r in (rows or [])]

    def to_columns(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Map a public payload to the column->value dict to persist (encoded + write-normalized)."""
        return {
            c.name: _encode(c, payload.get(c.key), strip_text=self.strip_text, clamp_int=self.clamp_non_negative_int)
            for c in self.columns
        }

    def upsert_sql(self) -> str:
        cols = self.column_names()
        placeholders = ", ".join(["%s"] * len(cols))
        non_pk = [c for c in cols if c not in self.pk]
        assignments = ", ".join(
            f"{c} = {self.merge.get(c, f'excluded.{c}')}" for c in non_pk
        )
        conflict = ", ".join(self.pk)
        return (
            f"INSERT INTO {self.table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {assignments} RETURNING *"
        )

    def upsert_params(self, payload: dict[str, Any]) -> list[Any]:
        columns = self.to_columns(payload)
        return [columns[c] for c in self.column_names()]


class Repository:
    """Base per-domain repository over the PG adapter primitives + a descriptor.

    ``adapter`` is the ``LiveControlPlanePostgresAdapter``; this layer issues descriptor-generated SQL
    through it (``select_one``/``select_many`` and ``_execute_returning_one``) and maps rows through the
    descriptor. No SQLite, no row_builder indirection, no getattr-by-string dispatch.
    """

    descriptor: TableDescriptor

    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter

    def get(self, *, where_sql: str, params: list[Any]) -> dict[str, Any] | None:
        row = self._adapter.select_one(self.descriptor.table, where_sql=where_sql, params=params)
        return self.descriptor.from_row(row) if row is not None else None

    def select(
        self,
        *,
        where_sql: str = "",
        params: list[Any] | None = None,
        order_by_sql: str = "",
        limit: int = 0,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        rows = self._adapter.select_many(
            self.descriptor.table,
            where_sql=where_sql,
            params=list(params or []),
            order_by_sql=order_by_sql,
            limit=limit,
            offset=offset,
        )
        return self.descriptor.from_rows(rows)

    def upsert(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = self._adapter._execute_returning_one(  # noqa: SLF001 — repository owns the adapter
            self.descriptor.upsert_sql(),
            self.descriptor.upsert_params(payload),
        )
        return self.descriptor.from_row(row)
