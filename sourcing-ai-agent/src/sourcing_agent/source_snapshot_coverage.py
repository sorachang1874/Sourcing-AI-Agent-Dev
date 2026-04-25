from __future__ import annotations

from typing import Any


_PROMOTED_AGGREGATE_PROOF_STATUSES = {
    "promoted",
    "coverage_proven",
    "ready",
    "verified",
    "active",
}
_PROMOTED_AGGREGATE_PROOF_KINDS = {
    "promoted_authoritative_aggregate",
    "authoritative_aggregate",
    "authoritative_aggregate_coverage",
    "coverage_proven_subset",
    "promoted_aggregate_coverage",
}
_PROMOTED_AGGREGATE_CONTRACT_NAMES = {
    "promoted_aggregate_coverage",
    "authoritative_aggregate_coverage",
    "coverage_proven_subset",
}


def promoted_aggregate_coverage_contract(selection: dict[str, Any] | None) -> dict[str, Any]:
    """Return normalized explicit aggregate coverage proof.

    The old planner treated selection mode strings such as
    ``preferred_snapshot_subset`` as coverage proof. This helper intentionally
    does not: a multi-snapshot aggregate must carry an explicit proof payload or
    explicit legacy proof booleans.
    """

    payload = dict(selection or {})
    proof = _first_dict(
        payload.get("promoted_aggregate_coverage"),
        payload.get("aggregate_coverage_proof"),
        payload.get("coverage_proof"),
    )
    legacy_boolean_proof = bool(payload.get("coverage_proven") or payload.get("authoritative_aggregate"))
    merged = {
        **(
            {
                "coverage_proven": True,
                "proof_kind": (
                    "authoritative_aggregate"
                    if payload.get("authoritative_aggregate")
                    else "coverage_proven_subset"
                ),
            }
            if legacy_boolean_proof
            else {}
        ),
        **proof,
    }
    proof_kind = _normalize_text(merged.get("proof_kind") or merged.get("kind") or merged.get("type")).lower()
    status = _normalize_text(merged.get("status")).lower()
    contract = _normalize_text(
        merged.get("contract")
        or merged.get("coverage_contract")
        or merged.get("contract_name")
        or payload.get("coverage_contract")
    ).lower()
    explicit_contract = bool(
        merged
        and (
            bool(merged.get("coverage_proven"))
            or bool(merged.get("promoted"))
            or status in _PROMOTED_AGGREGATE_PROOF_STATUSES
            or proof_kind in _PROMOTED_AGGREGATE_PROOF_KINDS
            or contract in _PROMOTED_AGGREGATE_CONTRACT_NAMES
            or bool(merged.get("contract_version"))
        )
    )
    selected_snapshot_ids = _dedupe_strings(
        payload.get("selected_snapshot_ids")
        or merged.get("selected_snapshot_ids")
        or merged.get("covered_snapshot_ids")
        or []
    )
    proof_snapshot_ids = _dedupe_strings(
        merged.get("selected_snapshot_ids")
        or merged.get("covered_snapshot_ids")
        or merged.get("source_snapshot_ids")
        or selected_snapshot_ids
    )
    if selected_snapshot_ids and proof_snapshot_ids:
        proof_id_set = {item.lower() for item in proof_snapshot_ids}
        proof_covers_selection = all(item.lower() in proof_id_set for item in selected_snapshot_ids)
    else:
        proof_covers_selection = True

    coverage_proven = bool(explicit_contract and proof_covers_selection)
    return {
        "coverage_proven": coverage_proven,
        "explicit_contract": bool(explicit_contract),
        "proof_covers_selection": bool(proof_covers_selection),
        "proof_kind": proof_kind,
        "status": status,
        "contract": contract,
        "selected_snapshot_ids": selected_snapshot_ids,
        "proof_snapshot_ids": proof_snapshot_ids,
        "source": "explicit_promoted_aggregate_contract" if coverage_proven else "missing_explicit_contract",
    }


def source_snapshot_selection_has_coverage_proof(selection: dict[str, Any] | None) -> bool:
    return bool(promoted_aggregate_coverage_contract(selection).get("coverage_proven"))


def _first_dict(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, dict):
            return dict(value)
    return {}


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _dedupe_strings(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    result: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = _normalize_text(item)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
    return result
