"""Shared contracts for model-mediated native-X evidence.

The helpers in this module deliberately do two narrow jobs:

* normalize explicit model support claims without inventing a temporal value
  for legacy string-only claims; and
* attribute a query to an authored-post or authored-reply surface only when it
  contains exactly one unambiguous ``from:<handle>`` operator.

Neither helper upgrades model-mediated text to source-bound evidence.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

THREAD_RELATIONS = frozenset({"self_post", "reply", "quote", "thread_root", "thread_reply"})
SUPPORT_DIMENSIONS = frozenset({"target_lab_affiliation_state", "pretraining_experience_state"})
TEMPORAL_STATES = frozenset({"current", "historical", "ambiguous", "unsupported"})
QUERY_SURFACES = frozenset({"authored_post", "authored_reply"})

_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_FROM_OPERATOR_RE = re.compile(
    r"(?:^|[\s(])(?P<negated>-?)from:(?P<handle>[A-Za-z0-9_]{1,15})(?=$|[\s)])",
    re.IGNORECASE,
)
_ANY_FROM_OPERATOR_RE = re.compile(r"(?i)(?:-?from:)")
_REPLY_FILTER_RE = re.compile(
    r"(?:^|[\s(])(?P<negated>-?)filter:replies(?=$|[\s)])",
    re.IGNORECASE,
)
_ANY_REPLY_FILTER_RE = re.compile(r"(?i)(?:-?filter:replies)")


def normalize_support_claims(
    supports: Any,
    *,
    allow_legacy: bool,
) -> tuple[dict[str, str], ...]:
    """Validate and normalize support claims.

    New claims must have the exact shape ``{dimension, asserted_value}``.
    When ``allow_legacy`` is true, a legacy dimension string is retained as a
    one-key mapping.  It intentionally has no ``asserted_value`` because the
    old artifact did not bind one and inferring it would rewrite history.

    ``ValueError`` is raised for malformed, empty, or duplicate claims.
    """

    if not isinstance(supports, list) or not supports:
        raise ValueError("support_claims_invalid")
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str | None]] = set()
    for item in supports:
        if isinstance(item, str) and allow_legacy:
            dimension = item
            asserted_value: str | None = None
        elif isinstance(item, Mapping) and set(item) == {"dimension", "asserted_value"}:
            dimension = item.get("dimension")
            asserted_value = item.get("asserted_value")
            if not isinstance(asserted_value, str) or asserted_value not in TEMPORAL_STATES:
                raise ValueError("support_claims_invalid")
        else:
            raise ValueError("support_claims_invalid")
        if not isinstance(dimension, str) or dimension not in SUPPORT_DIMENSIONS:
            raise ValueError("support_claims_invalid")
        key = (dimension, asserted_value)
        if key in seen:
            raise ValueError("support_claims_invalid")
        seen.add(key)
        claim = {"dimension": dimension}
        if asserted_value is not None:
            claim["asserted_value"] = asserted_value
        normalized.append(claim)
    return tuple(
        sorted(
            normalized,
            key=lambda claim: (claim["dimension"], claim.get("asserted_value", "")),
        )
    )


def classify_single_handle_query_surface(query: Any) -> tuple[str, str] | None:
    """Return ``(casefold_handle, surface)`` for a strict single-handle query.

    A positive ``filter:replies`` classifies ``authored_reply``.  A negated or
    absent reply filter classifies ``authored_post``.  Mixed reply filters,
    negated ``from`` operators, multiple handles, malformed operator tokens,
    and global queries are deliberately unattributed.
    """

    if not isinstance(query, str) or not query.strip() or len(query) > 2_000:
        return None
    from_matches = list(_FROM_OPERATOR_RE.finditer(query))
    if len(from_matches) != 1 or len(_ANY_FROM_OPERATOR_RE.findall(query)) != 1:
        return None
    match = from_matches[0]
    handle = match.group("handle")
    if match.group("negated") or _HANDLE_RE.fullmatch(handle) is None:
        return None

    reply_matches = list(_REPLY_FILTER_RE.finditer(query))
    if len(reply_matches) != len(_ANY_REPLY_FILTER_RE.findall(query)):
        return None
    positive_reply = any(not item.group("negated") for item in reply_matches)
    negative_reply = any(bool(item.group("negated")) for item in reply_matches)
    if positive_reply and negative_reply:
        return None
    return handle.casefold(), "authored_reply" if positive_reply else "authored_post"
