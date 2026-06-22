"""Track B B4.2 — control-plane timestamp utilities (extracted from the storage God-class).

Parses and compares the control plane's text timestamps. The names retain the legacy ``sqlite`` term for
caller continuity; the values are plain ISO-ish text. The B4.2 schema migration moves these columns to
``timestamptz``, at which point the ``TableDescriptor``'s future ``TIMESTAMPTZ`` kind owns the parsing
boundary and these helpers compare native ``datetime``s (or disappear).
"""

from __future__ import annotations

from datetime import datetime, timezone


def parse_sqlite_timestamp(value: str) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    for format_string in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            parsed = datetime.strptime(normalized, format_string)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def is_sqlite_timestamp_expired(value: str, *, now: datetime | None = None) -> bool:
    parsed = parse_sqlite_timestamp(value)
    if parsed is None:
        return True
    reference = now or datetime.now(timezone.utc)
    return parsed <= reference
