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
from typing import Any, NoReturn

from .control_plane_serde import json_safe_payload


class Kind(Enum):
    """The typed contract for a control-plane column."""

    STR = "str"  # text column -> str(v or "")
    INT = "int"  # integer column -> int(v or 0)
    FLOAT = "float"  # real column -> float(v or 0.0) (parse-fail -> 0.0); == _coerce_public_web_float
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
    if col.kind is Kind.INT:  # int(value or read_default) — read_default defaults to 0
        fallback = col.read_default if col.read_default is not None else 0
        try:
            return int(value or fallback)
        except (TypeError, ValueError):
            try:
                return int(fallback)
            except (TypeError, ValueError):
                return 0
    if col.kind is Kind.FLOAT:  # == _coerce_public_web_float; float(value or read_default), default 0.0
        fallback = col.read_default if col.read_default is not None else 0.0
        try:
            return float(value or fallback)
        except (TypeError, ValueError):
            return 0.0
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
        # json_safe_payload mirrors the former hand builders' json.dumps(_json_safe_payload(...)) so the
        # typed write path is byte-identical (Path/datetime/bytes/to_record/sets coerced before dumps).
        return json.dumps(json_safe_payload(value) if value is not None else empty, ensure_ascii=False)
    if col.kind is Kind.INT:
        try:
            number = int(value or 0)
        except (TypeError, ValueError):
            number = 0
        return max(0, number) if clamp_int else number
    if col.kind is Kind.FLOAT:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
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
        assignments = ", ".join(f"{c} = {self.merge.get(c, f'excluded.{c}')}" for c in non_pk)
        conflict = ", ".join(self.pk)
        return (
            f"INSERT INTO {self.table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {assignments} RETURNING *"
        )

    def upsert_params(self, payload: dict[str, Any]) -> list[Any]:
        columns = self.to_columns(payload)
        return [columns[c] for c in self.column_names()]


class Repository:
    """Base per-domain repository over the PG adapter primitives + descriptors.

    ``adapter`` is the ``LiveControlPlanePostgresAdapter``. The public surface of a domain repository is
    its typed domain methods ONLY; the protected primitives below are the fail-closed adapter plumbing,
    ported 1:1 from the retired ``ControlPlaneStore`` wrappers so every migrated method keeps the exact
    authority/error semantics (raise on authoritative failure, sentinel on non-authoritative miss).

    Write discipline (②.0 protocol): repository writes route through the adapter primitives
    (``upsert_row``/``bulk_upsert_rows``/``insert_row_with_generated_id``/``delete_rows``) whose conflict
    targets are covered by the pg-onconflict guard via ``_PRIMARY_KEY_COLUMNS``. Literal ``ON CONFLICT``
    SQL must not live in ``repositories/``; the guard scans this package and requires adapter primitives.
    """

    descriptor: TableDescriptor

    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter

    # --- authority predicates (== the retired ControlPlaneStore wrappers, over the adapter) ---

    def _should_prefer_read(self, table_name: str) -> bool:
        return bool(self._adapter.should_prefer_read(table_name))

    def _is_authoritative(self, table_name: str) -> bool:
        predicate = getattr(self._adapter, "is_authoritative", None)
        if not callable(predicate):
            return False
        try:
            return bool(predicate(table_name))
        except Exception:
            return False

    def _strict_authoritative(self, table_name: str) -> bool:
        # == ControlPlaneStore._control_plane_postgres_should_skip_sqlite_fallback
        return bool(self._should_prefer_read(table_name) and self._is_authoritative(table_name))

    # --- fail-closed error surface (message format preserved byte-identically) ---

    def _raise_write_failure(
        self,
        *,
        table_name: str,
        method_name: str,
        reason: str,
        error: Exception | None = None,
    ) -> NoReturn:
        message = f"Postgres authoritative write failed for {table_name} via {method_name}: {reason}"
        if error is not None:
            raise RuntimeError(message) from error
        raise RuntimeError(message)

    def _raise_read_failure(
        self,
        *,
        table_name: str,
        method_name: str,
        reason: str,
        error: Exception | None = None,
    ) -> NoReturn:
        message = f"Postgres authoritative read failed for {table_name} via {method_name}: {reason}"
        if error is not None:
            raise RuntimeError(message) from error
        raise RuntimeError(message)

    def _raise_postgres_only_invariant(self, *, table_name: str, method_name: str) -> NoReturn:
        raise RuntimeError(
            f"postgres-only invariant violated for {table_name} in {method_name}: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    # --- fail-closed read/write primitives (== _select_control_plane_row(s) / _write_control_plane_row_to_postgres) ---

    def _select_row(
        self,
        table_name: str,
        *,
        row_builder: Any,
        where_sql: str,
        params: list[Any] | tuple[Any, ...],
        order_by_sql: str = "",
    ) -> dict[str, Any] | None:
        if not self._should_prefer_read(table_name):
            return None
        try:
            row = self._adapter.select_one(
                table_name,
                where_sql=where_sql,
                params=list(params),
                order_by_sql=order_by_sql,
            )
        except Exception as exc:
            if self._strict_authoritative(table_name):
                self._raise_read_failure(
                    table_name=table_name,
                    method_name="select_one",
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
            return None
        if row is None:
            return None
        return row_builder(row)

    def _select_rows(
        self,
        table_name: str,
        *,
        row_builder: Any,
        where_sql: str = "",
        params: list[Any] | tuple[Any, ...] = (),
        order_by_sql: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if not self._should_prefer_read(table_name):
            return []
        try:
            rows = self._adapter.select_many(
                table_name,
                where_sql=where_sql,
                params=list(params),
                order_by_sql=order_by_sql,
                limit=limit,
                offset=offset,
            )
        except Exception as exc:
            if self._strict_authoritative(table_name):
                self._raise_read_failure(
                    table_name=table_name,
                    method_name="select_many",
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
            return []
        if not rows:
            return []
        return [row_builder(row) for row in rows]

    def _write_row(self, table_name: str, row: dict[str, Any] | None) -> bool:
        if row is None or not self._should_prefer_read(table_name):
            return False
        try:
            self._adapter.upsert_row(table_name, dict(row))
        except Exception as exc:
            if self._strict_authoritative(table_name):
                self._raise_write_failure(
                    table_name=table_name,
                    method_name="upsert_row",
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
            return False
        return True

    def _call_native_write(self, method_name: str, /, *, table_name: str, **kwargs: Any) -> Any:
        # == ControlPlaneStore._call_control_plane_postgres_native restricted to native WRITERS with an
        # explicit table_name (insert_row_with_generated_id / update_row_returning /
        # upsert_row_with_generated_id / delete_rows): swallow to None when non-authoritative, raise when strict.
        # Native reads in repositories/ go through _select_row(s); there is no read branch here.
        strict_no_fallback = bool(table_name and self._strict_authoritative(table_name))
        method = getattr(self._adapter, method_name, None)
        if method is None:
            if strict_no_fallback:
                self._raise_write_failure(
                    table_name=table_name,
                    method_name=method_name,
                    reason="native writer is unavailable",
                )
            return None
        try:
            return method(table_name=table_name, **kwargs)
        except Exception as exc:
            if strict_no_fallback:
                self._raise_write_failure(
                    table_name=table_name,
                    method_name=method_name,
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
            return None
