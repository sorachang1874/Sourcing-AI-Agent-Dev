"""Track B B4.2 — control-plane JSON serialization helpers (extracted from the storage God-class).

``json_safe_payload`` recursively coerces a value into a JSON-serializable shape (Path -> str,
datetime -> isoformat, bytes/memoryview -> utf-8 text, ``to_record`` objects -> their record,
dict/list/tuple/set -> recursed) before ``json.dumps``. It is the canonical write-side normalization
for every JSON control-plane column; the ``TableDescriptor`` JSON kinds apply it so the typed write
path produces byte-identical output to the former hand-written ``json.dumps(_json_safe_payload(...))``
column builders. Dependency-free (stdlib only) so both ``storage`` and ``control_plane_repository`` can
import it without a cycle.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any


def json_safe_payload(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return bytes(value).decode("utf-8", errors="replace")
    to_record = getattr(value, "to_record", None)
    if callable(to_record):
        return json_safe_payload(to_record())
    if isinstance(value, dict):
        return {str(key): json_safe_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe_payload(item) for item in value]
    return value
