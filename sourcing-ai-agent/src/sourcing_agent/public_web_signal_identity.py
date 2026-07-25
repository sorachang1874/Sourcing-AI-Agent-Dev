"""Stable identity helpers for user-reviewable Public Web signals."""

from __future__ import annotations

from hashlib import sha1
from typing import Any

from .public_web_search import normalize_public_web_url_key


def public_web_signal_identity_key(
    *,
    person_identity_key: str = "",
    record_id: str = "",
    signal_kind: str = "",
    signal_type: str = "",
    normalized_value: str = "",
    value: str = "",
    url: str = "",
    source_url: str = "",
) -> str:
    """Stable identity for a user-reviewable Public Web signal.

    Signal identity must survive force-refresh runs. Run IDs, provider result
    order, and batch IDs are execution facts; they are not part of the user
    confirmation contract.
    """
    owner_key = str(person_identity_key or "").strip()
    if not owner_key:
        record_key = str(record_id or "").strip()
        owner_key = f"record:{record_key}" if record_key else "owner:unknown"
    normalized_kind = str(signal_kind or "").strip() or "unknown"
    normalized_type = str(signal_type or "").strip() or "unknown"
    raw_value = str(normalized_value or value or url or source_url or "").strip()
    if normalized_kind == "email_candidate":
        identity_value = raw_value.lower()
    else:
        identity_value = normalize_public_web_url_key(raw_value) or raw_value.lower()
    if not identity_value:
        return ""
    return "|".join((owner_key, normalized_kind, normalized_type, identity_value))


def public_web_signal_id_for_identity(**kwargs: Any) -> str:
    identity_key = public_web_signal_identity_key(**kwargs)
    if not identity_key:
        return ""
    return "person-public-web-signal-" + sha1(identity_key.encode("utf-8")).hexdigest()[:16]
