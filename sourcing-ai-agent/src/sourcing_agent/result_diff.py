from __future__ import annotations

from typing import Any


def build_result_diff(
    baseline_results: list[dict[str, Any]],
    rerun_results: list[dict[str, Any]],
    *,
    baseline_job_id: str = "",
    rerun_job_id: str = "",
    baseline_version: dict[str, Any] | None = None,
    rerun_version: dict[str, Any] | None = None,
    trigger_feedback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result_changes = _build_result_changes(
        baseline_results,
        rerun_results,
        baseline_job_id=baseline_job_id,
        rerun_job_id=rerun_job_id,
    )
    rule_changes = build_rule_change_diff(
        baseline_version,
        rerun_version,
        trigger_feedback=trigger_feedback or {},
    )
    candidate_impacts = _build_candidate_impacts(
        baseline_results=baseline_results,
        rerun_results=rerun_results,
        rule_changes=rule_changes,
    )
    impact_explanations = _impact_explanations(
        rule_changes=rule_changes,
        result_changes=result_changes,
        candidate_impacts=candidate_impacts,
    )
    summary = {
        **result_changes["summary"],
        "baseline_version_id": int((baseline_version or {}).get("version_id") or 0),
        "rerun_version_id": int((rerun_version or {}).get("version_id") or 0),
        "added_pattern_count": rule_changes["summary"]["added_pattern_count"],
        "removed_pattern_count": rule_changes["summary"]["removed_pattern_count"],
        "updated_pattern_count": rule_changes["summary"]["updated_pattern_count"],
        "request_change_count": rule_changes["summary"]["request_change_count"],
        "plan_change_count": rule_changes["summary"]["plan_change_count"],
        "rule_explanation_count": len(rule_changes["explanations"]),
        "result_explanation_count": len(result_changes["explanations"]),
        "candidate_impact_count": len(candidate_impacts["items"]),
        "impact_explanation_count": len(impact_explanations),
        "explanation_count": (
            len(rule_changes["explanations"])
            + len(result_changes["explanations"])
            + len(impact_explanations)
        ),
    }
    return {
        "summary": summary,
        "rule_changes": rule_changes,
        "result_changes": result_changes,
        "candidate_impacts": candidate_impacts,
        "impact_explanations": impact_explanations,
        "added": result_changes["added"],
        "removed": result_changes["removed"],
        "moved": result_changes["moved"],
    }


def build_rule_change_diff(
    baseline_version: dict[str, Any] | None,
    rerun_version: dict[str, Any] | None,
    *,
    trigger_feedback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    baseline_version = baseline_version or {}
    rerun_version = rerun_version or {}
    trigger_feedback = trigger_feedback or {}

    baseline_patterns = _normalize_patterns(baseline_version.get("patterns") or [])
    rerun_patterns = _normalize_patterns(rerun_version.get("patterns") or [])
    baseline_pattern_map = {item["pattern_key"]: item for item in baseline_patterns}
    rerun_pattern_map = {item["pattern_key"]: item for item in rerun_patterns}

    added_patterns: list[dict[str, Any]] = []
    removed_patterns: list[dict[str, Any]] = []
    updated_patterns: list[dict[str, Any]] = []

    for key, item in rerun_pattern_map.items():
        baseline_item = baseline_pattern_map.get(key)
        if baseline_item is None:
            added_patterns.append(item)
            continue
        field_changes: list[dict[str, Any]] = []
        for field in ["status", "confidence", "target_company"]:
            if baseline_item.get(field) != item.get(field):
                field_changes.append({"field": field, "old": baseline_item.get(field), "new": item.get(field)})
        if baseline_item.get("metadata") != item.get("metadata"):
            field_changes.append({"field": "metadata", "old": baseline_item.get("metadata"), "new": item.get("metadata")})
        if field_changes:
            updated_patterns.append(
                {
                    "pattern_key": key,
                    "pattern_type": item.get("pattern_type") or "",
                    "subject": item.get("subject") or "",
                    "value": item.get("value") or "",
                    "changes": field_changes,
                }
            )

    for key, item in baseline_pattern_map.items():
        if key not in rerun_pattern_map:
            removed_patterns.append(item)

    request_changes = _structured_field_changes(
        baseline_version.get("request") or {},
        rerun_version.get("request") or {},
        [
            "asset_view",
            "categories",
            "employment_statuses",
            "keywords",
            "must_have_facets",
            "must_have_primary_role_buckets",
            "must_have_keywords",
            "exclude_keywords",
            "organization_keywords",
            "retrieval_strategy",
            "target_scope",
        ],
    )
    plan_changes = _plan_changes(baseline_version.get("plan") or {}, rerun_version.get("plan") or {})
    explanations = _rule_change_explanations(
        added_patterns=added_patterns,
        removed_patterns=removed_patterns,
        updated_patterns=updated_patterns,
        request_changes=request_changes,
        plan_changes=plan_changes,
        trigger_feedback=trigger_feedback,
    )

    summary = {
        "baseline_version_id": int(baseline_version.get("version_id") or 0),
        "rerun_version_id": int(rerun_version.get("version_id") or 0),
        "added_pattern_count": len(added_patterns),
        "removed_pattern_count": len(removed_patterns),
        "updated_pattern_count": len(updated_patterns),
        "request_change_count": len(request_changes),
        "plan_change_count": len(plan_changes),
    }
    return {
        "summary": summary,
        "pattern_changes": {
            "added": added_patterns,
            "removed": removed_patterns,
            "updated": updated_patterns,
        },
        "added_patterns": added_patterns,
        "removed_patterns": removed_patterns,
        "updated_patterns": updated_patterns,
        "request_changes": request_changes,
        "plan_changes": plan_changes,
        "explanations": explanations,
    }


def _build_result_changes(
    baseline_results: list[dict[str, Any]],
    rerun_results: list[dict[str, Any]],
    *,
    baseline_job_id: str = "",
    rerun_job_id: str = "",
) -> dict[str, Any]:
    baseline_map = {str(item.get("candidate_id") or ""): item for item in baseline_results if item.get("candidate_id")}
    rerun_map = {str(item.get("candidate_id") or ""): item for item in rerun_results if item.get("candidate_id")}

    added: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    moved: list[dict[str, Any]] = []

    for candidate_id, item in rerun_map.items():
        baseline_item = baseline_map.get(candidate_id)
        if baseline_item is None:
            added.append(_diff_projection(item, rank_key="rank"))
            continue
        old_rank = int(baseline_item.get("rank") or 0)
        new_rank = int(item.get("rank") or 0)
        old_score = float(baseline_item.get("score") or 0.0)
        new_score = float(item.get("score") or 0.0)
        old_confidence = str(baseline_item.get("confidence_label") or "")
        new_confidence = str(item.get("confidence_label") or "")
        if old_rank != new_rank or old_score != new_score or old_confidence != new_confidence:
            moved.append(
                {
                    "candidate_id": candidate_id,
                    "display_name": item.get("display_name") or baseline_item.get("display_name") or "",
                    "old_rank": old_rank,
                    "new_rank": new_rank,
                    "rank_delta": old_rank - new_rank if old_rank and new_rank else 0,
                    "old_score": round(old_score, 2),
                    "new_score": round(new_score, 2),
                    "old_confidence_label": old_confidence,
                    "new_confidence_label": new_confidence,
                }
            )

    for candidate_id, item in baseline_map.items():
        if candidate_id not in rerun_map:
            removed.append(_diff_projection(item, rank_key="rank"))

    baseline_top = baseline_results[0] if baseline_results else {}
    rerun_top = rerun_results[0] if rerun_results else {}
    top_candidate_changed = str(baseline_top.get("candidate_id") or "") != str(rerun_top.get("candidate_id") or "")

    summary = {
        "baseline_job_id": baseline_job_id,
        "rerun_job_id": rerun_job_id,
        "baseline_count": len(baseline_results),
        "rerun_count": len(rerun_results),
        "added_count": len(added),
        "removed_count": len(removed),
        "moved_count": len(moved),
        "top_candidate_changed": top_candidate_changed,
        "baseline_top_candidate": baseline_top.get("display_name") or "",
        "rerun_top_candidate": rerun_top.get("display_name") or "",
        "baseline_available": bool(baseline_results),
    }
    explanations = _result_change_explanations(
        added=added,
        removed=removed,
        moved=moved,
        summary=summary,
    )
    return {
        "summary": summary,
        "added": added,
        "removed": removed,
        "moved": moved,
        "explanations": explanations,
    }


def _normalize_patterns(patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in patterns:
        if not isinstance(item, dict):
            continue
        normalized_item = {
            "pattern_key": _pattern_key(item),
            "pattern_type": str(item.get("pattern_type") or "").strip(),
            "subject": str(item.get("subject") or "").strip(),
            "value": str(item.get("value") or "").strip(),
            "status": str(item.get("status") or "").strip(),
            "confidence": str(item.get("confidence") or "").strip(),
            "target_company": str(item.get("target_company") or "").strip(),
            "metadata": dict(item.get("metadata") or {}),
        }
        normalized.append(normalized_item)
    normalized.sort(key=lambda item: item["pattern_key"])
    return normalized


def _pattern_key(item: dict[str, Any]) -> str:
    parts = [
        str(item.get("pattern_type") or "").strip().lower(),
        str(item.get("subject") or "").strip().lower(),
        str(item.get("value") or "").strip().lower(),
    ]
    return "|".join(parts)


def _structured_field_changes(
    baseline_payload: dict[str, Any],
    rerun_payload: dict[str, Any],
    fields: list[str],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for field in fields:
        old_value = baseline_payload.get(field)
        new_value = rerun_payload.get(field)
        if old_value != new_value:
            change = {"field": field, "old": old_value, "new": new_value}
            old_list = _normalized_list(old_value)
            new_list = _normalized_list(new_value)
            if old_list is not None and new_list is not None:
                change["added_values"] = [item for item in new_list if item not in old_list]
                change["removed_values"] = [item for item in old_list if item not in new_list]
            changes.append(change)
    return changes


def _plan_changes(baseline_plan: dict[str, Any], rerun_plan: dict[str, Any]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    baseline_retrieval = baseline_plan.get("retrieval_plan") or {}
    rerun_retrieval = rerun_plan.get("retrieval_plan") or {}
    if baseline_retrieval.get("strategy") != rerun_retrieval.get("strategy"):
        changes.append(
            {
                "field": "retrieval_plan.strategy",
                "old": baseline_retrieval.get("strategy"),
                "new": rerun_retrieval.get("strategy"),
            }
        )
    baseline_acquisition = baseline_plan.get("acquisition_strategy") or {}
    rerun_acquisition = rerun_plan.get("acquisition_strategy") or {}
    for field in ["strategy_type", "target_population", "company_scope", "search_channel_order", "search_seed_queries"]:
        if baseline_acquisition.get(field) != rerun_acquisition.get(field):
            changes.append(
                {
                    "field": f"acquisition_strategy.{field}",
                    "old": baseline_acquisition.get(field),
                    "new": rerun_acquisition.get(field),
                }
            )
    baseline_publication = baseline_plan.get("publication_coverage") or {}
    rerun_publication = rerun_plan.get("publication_coverage") or {}
    for field in ["coverage_goal", "seed_queries", "extraction_strategy", "validation_steps"]:
        if baseline_publication.get(field) != rerun_publication.get(field):
            changes.append(
                {
                    "field": f"publication_coverage.{field}",
                    "old": baseline_publication.get(field),
                    "new": rerun_publication.get(field),
                }
            )
    return changes


def _rule_change_explanations(
    *,
    added_patterns: list[dict[str, Any]],
    removed_patterns: list[dict[str, Any]],
    updated_patterns: list[dict[str, Any]],
    request_changes: list[dict[str, Any]],
    plan_changes: list[dict[str, Any]],
    trigger_feedback: dict[str, Any],
) -> list[str]:
    explanations: list[str] = []
    feedback_type = str(trigger_feedback.get("feedback_type") or "").strip()
    subject = str(trigger_feedback.get("subject") or "").strip()
    value = str(trigger_feedback.get("value") or "").strip()
    if feedback_type or subject or value:
        explanations.append(
            "Feedback trigger: "
            + ", ".join(part for part in [feedback_type or "", subject or "", value or ""] if part)
        )
    for item in added_patterns[:4]:
        explanations.append(
            f"Added {item['pattern_type']} rule: {item['subject']} -> {item['value']} "
            f"(status={item['status'] or 'active'}, confidence={item['confidence'] or 'unknown'})."
        )
    for item in removed_patterns[:4]:
        explanations.append(
            f"Removed {item['pattern_type']} rule: {item['subject']} -> {item['value']}."
        )
    for item in updated_patterns[:4]:
        changed_fields = ", ".join(change["field"] for change in item["changes"])
        explanations.append(
            f"Updated {item['pattern_type']} rule: {item['subject']} -> {item['value']} ({changed_fields})."
        )
    for item in request_changes[:4]:
        details = _change_details_suffix(item)
        explanations.append(f"Request field changed: {item['field']}{details}.")
    for item in plan_changes[:4]:
        details = _change_details_suffix(item)
        explanations.append(f"Plan field changed: {item['field']}{details}.")
    if not plan_changes and (added_patterns or removed_patterns or updated_patterns):
        explanations.append("Plan stayed stable; expected impact is mainly on retrieval scoring and confidence banding.")
    if not explanations:
        explanations.append("No explicit rule changes were detected between baseline and rerun criteria versions.")
    return explanations


def _result_change_explanations(
    *,
    added: list[dict[str, Any]],
    removed: list[dict[str, Any]],
    moved: list[dict[str, Any]],
    summary: dict[str, Any],
) -> list[str]:
    explanations: list[str] = []
    if added:
        explanations.append(
            f"{len(added)} candidate(s) entered the rerun results: {_display_names(added)}."
        )
    if removed:
        explanations.append(
            f"{len(removed)} candidate(s) left the rerun results: {_display_names(removed)}."
        )
    if moved:
        top_move = moved[0]
        explanations.append(
            f"{len(moved)} candidate(s) changed rank or confidence; "
            f"largest visible movement: {top_move.get('display_name') or top_move.get('candidate_id') or 'candidate'} "
            f"{top_move.get('old_rank') or '?'} -> {top_move.get('new_rank') or '?'}."
        )
    if summary.get("top_candidate_changed"):
        explanations.append(
            f"Top candidate changed from {summary.get('baseline_top_candidate') or 'n/a'} "
            f"to {summary.get('rerun_top_candidate') or 'n/a'}."
        )
    if not explanations:
        explanations.append("Result set stayed stable after criteria recompilation.")
    return explanations


def _impact_explanations(
    *,
    rule_changes: dict[str, Any],
    result_changes: dict[str, Any],
    candidate_impacts: dict[str, Any],
) -> list[str]:
    explanations: list[str] = []
    rule_summary = rule_changes.get("summary") or {}
    result_summary = result_changes.get("summary") or {}
    impact_summary = candidate_impacts.get("summary") or {}
    changed_pattern_total = (
        int(rule_summary.get("added_pattern_count") or 0)
        + int(rule_summary.get("removed_pattern_count") or 0)
        + int(rule_summary.get("updated_pattern_count") or 0)
    )
    added_count = int(result_summary.get("added_count") or 0)
    removed_count = int(result_summary.get("removed_count") or 0)
    moved_count = int(result_summary.get("moved_count") or 0)
    if changed_pattern_total and (added_count or removed_count or moved_count):
        explanations.append(
            "Criteria changes materially affected retrieval output: "
            f"{changed_pattern_total} rule change(s) produced "
            f"{added_count} added, {removed_count} removed, and {moved_count} moved result(s)."
        )
    elif changed_pattern_total:
        explanations.append(
            "Criteria changes were recorded, but the top-k retrieval output remained stable in this rerun."
        )
    elif added_count or removed_count or moved_count:
        explanations.append(
            "Retrieval output changed even though no explicit criteria-rule delta was detected; "
            "inspect request or plan changes for the cause."
        )
    if int(impact_summary.get("attributed_candidate_count") or 0):
        top_item = (candidate_impacts.get("items") or [])[0]
        if isinstance(top_item, dict):
            explanations.append(
                f"Candidate-level attribution is available for {impact_summary.get('attributed_candidate_count')} "
                f"impacted candidate(s); top example: {top_item.get('display_name') or top_item.get('candidate_id') or 'candidate'}."
            )
    if not explanations:
        explanations.append("No material rule or result change was detected in the rerun diff.")
    return explanations


def _build_candidate_impacts(
    *,
    baseline_results: list[dict[str, Any]],
    rerun_results: list[dict[str, Any]],
    rule_changes: dict[str, Any],
) -> dict[str, Any]:
    baseline_map = {str(item.get("candidate_id") or ""): item for item in baseline_results if item.get("candidate_id")}
    rerun_map = {str(item.get("candidate_id") or ""): item for item in rerun_results if item.get("candidate_id")}
    ordered_ids: list[str] = []
    for items in [rerun_results, baseline_results]:
        for item in items:
            candidate_id = str(item.get("candidate_id") or "")
            if candidate_id and candidate_id not in ordered_ids:
                ordered_ids.append(candidate_id)

    items: list[dict[str, Any]] = []
    for candidate_id in ordered_ids:
        baseline_item = baseline_map.get(candidate_id)
        rerun_item = rerun_map.get(candidate_id)
        if baseline_item is None and rerun_item is None:
            continue
        impact = _candidate_impact_entry(
            baseline_item=baseline_item,
            rerun_item=rerun_item,
            rule_changes=rule_changes,
        )
        if impact is not None:
            items.append(impact)

    items.sort(
        key=lambda item: (
            _impact_priority(str(item.get("change_type") or "")),
            int(item.get("new_rank") or item.get("old_rank") or 9999),
            str(item.get("display_name") or item.get("candidate_id") or ""),
        )
    )
    summary = {
        "candidate_impact_count": len(items),
        "attributed_candidate_count": sum(1 for item in items if item.get("rule_triggers")),
        "unattributed_candidate_count": sum(1 for item in items if not item.get("rule_triggers")),
    }
    return {"summary": summary, "items": items}


def _candidate_impact_entry(
    *,
    baseline_item: dict[str, Any] | None,
    rerun_item: dict[str, Any] | None,
    rule_changes: dict[str, Any],
) -> dict[str, Any] | None:
    candidate = rerun_item or baseline_item or {}
    candidate_id = str(candidate.get("candidate_id") or "")
    if not candidate_id:
        return None

    change_type = _candidate_change_type(baseline_item, rerun_item)
    if not change_type:
        return None

    baseline_fields = baseline_item.get("matched_fields") if baseline_item else []
    rerun_fields = rerun_item.get("matched_fields") if rerun_item else []
    matched_field_delta = _matched_field_delta(baseline_fields, rerun_fields)
    rule_triggers = _candidate_rule_triggers(
        baseline_item=baseline_item,
        rerun_item=rerun_item,
        rule_changes=rule_changes,
        matched_field_delta=matched_field_delta,
    )
    explanation = _candidate_impact_explanation(
        candidate=candidate,
        change_type=change_type,
        baseline_item=baseline_item,
        rerun_item=rerun_item,
        matched_field_delta=matched_field_delta,
        rule_triggers=rule_triggers,
    )
    return {
        "candidate_id": candidate_id,
        "display_name": candidate.get("display_name") or "",
        "change_type": change_type,
        "old_rank": int((baseline_item or {}).get("rank") or 0),
        "new_rank": int((rerun_item or {}).get("rank") or 0),
        "old_confidence_label": str((baseline_item or {}).get("confidence_label") or ""),
        "new_confidence_label": str((rerun_item or {}).get("confidence_label") or ""),
        "matched_field_delta": matched_field_delta,
        "rule_triggers": rule_triggers,
        "baseline_explanation": str((baseline_item or {}).get("explanation") or ""),
        "rerun_explanation": str((rerun_item or {}).get("explanation") or ""),
        "baseline_confidence_reason": str((baseline_item or {}).get("confidence_reason") or ""),
        "rerun_confidence_reason": str((rerun_item or {}).get("confidence_reason") or ""),
        "attribution_explanation": explanation,
    }


def _candidate_change_type(
    baseline_item: dict[str, Any] | None,
    rerun_item: dict[str, Any] | None,
) -> str:
    if baseline_item is None and rerun_item is not None:
        return "added"
    if baseline_item is not None and rerun_item is None:
        return "removed"
    if baseline_item is None or rerun_item is None:
        return ""
    old_rank = int(baseline_item.get("rank") or 0)
    new_rank = int(rerun_item.get("rank") or 0)
    old_confidence = str(baseline_item.get("confidence_label") or "")
    new_confidence = str(rerun_item.get("confidence_label") or "")
    old_score = float(baseline_item.get("score") or 0.0)
    new_score = float(rerun_item.get("score") or 0.0)
    if old_rank != new_rank or old_confidence != new_confidence or old_score != new_score:
        return "moved"
    return ""


def _matched_field_delta(
    baseline_fields: list[dict[str, Any]] | None,
    rerun_fields: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    baseline_fields = [item for item in (baseline_fields or []) if isinstance(item, dict)]
    rerun_fields = [item for item in (rerun_fields or []) if isinstance(item, dict)]
    baseline_map = {_matched_field_key(item): item for item in baseline_fields}
    rerun_map = {_matched_field_key(item): item for item in rerun_fields}
    added = [rerun_map[key] for key in rerun_map if key not in baseline_map]
    removed = [baseline_map[key] for key in baseline_map if key not in rerun_map]
    stable = [rerun_map[key] for key in rerun_map if key in baseline_map]
    return {"added": added, "removed": removed, "stable": stable}


def _candidate_rule_triggers(
    *,
    baseline_item: dict[str, Any] | None,
    rerun_item: dict[str, Any] | None,
    rule_changes: dict[str, Any],
    matched_field_delta: dict[str, Any],
) -> list[dict[str, Any]]:
    triggers: list[dict[str, Any]] = []
    change_type = _candidate_change_type(baseline_item, rerun_item)
    candidate_fields = {
        "added": matched_field_delta.get("added") or [],
        "removed": matched_field_delta.get("removed") or [],
        "current": list((rerun_item or baseline_item or {}).get("matched_fields") or []),
    }
    pattern_changes = rule_changes.get("pattern_changes") or {}
    for bucket_name, pattern_type in [("added", "pattern_added"), ("removed", "pattern_removed"), ("updated", "pattern_updated")]:
        for pattern in pattern_changes.get(bucket_name) or []:
            matched_fields = _pattern_matched_fields(
                pattern=pattern,
                change_type=change_type,
                candidate_fields=candidate_fields,
            )
            if not matched_fields:
                continue
            triggers.append(
                {
                    "source": pattern_type,
                    "pattern_type": pattern.get("pattern_type") or "",
                    "subject": pattern.get("subject") or "",
                    "value": pattern.get("value") or "",
                    "matched_fields": matched_fields,
                    "rationale": _pattern_trigger_rationale(pattern, change_type, matched_fields),
                }
            )
    for request_change in rule_changes.get("request_changes") or []:
        matched_fields = _request_change_matched_fields(request_change, candidate_fields)
        if not matched_fields:
            continue
        triggers.append(
            {
                "source": "request_change",
                "field": request_change.get("field") or "",
                "matched_fields": matched_fields,
                "rationale": _request_trigger_rationale(request_change, change_type, matched_fields),
            }
        )
    for plan_change in rule_changes.get("plan_changes") or []:
        if str(plan_change.get("field") or "").startswith("retrieval_plan."):
            triggers.append(
                {
                    "source": "plan_change",
                    "field": plan_change.get("field") or "",
                    "matched_fields": [],
                    "rationale": f"Plan field {plan_change.get('field') or ''} changed and may have affected ranking policy.",
                }
            )
    return triggers


def _pattern_matched_fields(
    *,
    pattern: dict[str, Any],
    change_type: str,
    candidate_fields: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    if change_type == "added":
        fields = candidate_fields["added"] or candidate_fields["current"]
    elif change_type == "removed":
        fields = candidate_fields["removed"] or candidate_fields["current"]
    else:
        fields = candidate_fields["added"] or candidate_fields["current"]
    subject = str(pattern.get("subject") or "").strip().lower()
    value = str(pattern.get("value") or "").strip().lower()
    matched: list[dict[str, Any]] = []
    for item in fields:
        keyword = str(item.get("keyword") or "").strip().lower()
        matched_on = str(item.get("matched_on") or "").strip().lower()
        if subject and keyword == subject:
            matched.append(_matched_field_projection(item))
            continue
        if value and matched_on == value:
            matched.append(_matched_field_projection(item))
            continue
        if value and value in matched_on:
            matched.append(_matched_field_projection(item))
    return matched


def _request_change_matched_fields(
    request_change: dict[str, Any],
    candidate_fields: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    field = str(request_change.get("field") or "")
    added_values = [str(item).strip().lower() for item in (request_change.get("added_values") or []) if str(item).strip()]
    removed_values = [str(item).strip().lower() for item in (request_change.get("removed_values") or []) if str(item).strip()]
    fields = candidate_fields["current"]
    matched: list[dict[str, Any]] = []
    for item in fields:
        keyword = str(item.get("keyword") or "").strip().lower()
        matched_on = str(item.get("matched_on") or "").strip().lower()
        if any(value == keyword or value == matched_on or value in matched_on for value in added_values + removed_values):
            matched.append(_matched_field_projection(item))
    if matched:
        return matched
    if field in {"categories", "employment_statuses"}:
        return [{}]
    return []


def _candidate_impact_explanation(
    *,
    candidate: dict[str, Any],
    change_type: str,
    baseline_item: dict[str, Any] | None,
    rerun_item: dict[str, Any] | None,
    matched_field_delta: dict[str, Any],
    rule_triggers: list[dict[str, Any]],
) -> str:
    name = candidate.get("display_name") or candidate.get("candidate_id") or "candidate"
    if rule_triggers:
        top_trigger = rule_triggers[0]
        rationale = str(top_trigger.get("rationale") or "").strip()
        if rationale:
            return f"{name}: {rationale}"
    if change_type == "added":
        fields = matched_field_delta.get("added") or matched_field_delta.get("stable") or []
        return f"{name} entered the rerun results with new evidence on {_matched_field_summary(fields)}."
    if change_type == "removed":
        fields = matched_field_delta.get("removed") or []
        return f"{name} left the rerun results after losing matches on {_matched_field_summary(fields)}."
    old_rank = int((baseline_item or {}).get("rank") or 0)
    new_rank = int((rerun_item or {}).get("rank") or 0)
    old_conf = str((baseline_item or {}).get("confidence_label") or "")
    new_conf = str((rerun_item or {}).get("confidence_label") or "")
    if old_conf != new_conf:
        return f"{name} changed confidence from {old_conf or 'n/a'} to {new_conf or 'n/a'}."
    return f"{name} moved from rank {old_rank or '?'} to {new_rank or '?'}."


def _pattern_trigger_rationale(
    pattern: dict[str, Any],
    change_type: str,
    matched_fields: list[dict[str, Any]],
) -> str:
    label = f"{pattern.get('pattern_type') or 'rule'} {pattern.get('subject') or ''}->{pattern.get('value') or ''}".strip()
    summary = _matched_field_summary(matched_fields)
    if change_type == "added":
        return f"Entered results because {label} created a new match on {summary}."
    if change_type == "removed":
        return f"Left results because {label} was removed or disabled, removing matches on {summary}."
    return f"Rank/confidence changed because {label} affected matches on {summary}."


def _request_trigger_rationale(
    request_change: dict[str, Any],
    change_type: str,
    matched_fields: list[dict[str, Any]],
) -> str:
    field = request_change.get("field") or "request"
    details = _change_details_suffix(request_change).strip()
    summary = _matched_field_summary(matched_fields)
    if change_type == "added":
        return f"Entered results because request field {field}{details} and the candidate matches {summary}."
    if change_type == "removed":
        return f"Left results because request field {field}{details} and prior matches no longer satisfy it."
    return f"Rank/confidence changed after request field {field}{details}; relevant matches: {summary}."


def _matched_field_key(item: dict[str, Any]) -> str:
    return "|".join(
        [
            str(item.get("keyword") or "").strip().lower(),
            str(item.get("matched_on") or "").strip().lower(),
            str(item.get("field") or "").strip().lower(),
        ]
    )


def _matched_field_projection(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "keyword": item.get("keyword") or "",
        "matched_on": item.get("matched_on") or "",
        "field": item.get("field") or "",
        "value": item.get("value") or "",
    }


def _matched_field_summary(items: list[dict[str, Any]], *, limit: int = 2) -> str:
    parts: list[str] = []
    for item in items[:limit]:
        field = str(item.get("field") or "").strip()
        matched_on = str(item.get("matched_on") or item.get("keyword") or "").strip()
        if field and matched_on:
            parts.append(f"{field}:{matched_on}")
        elif matched_on:
            parts.append(matched_on)
    if not parts:
        return "current filters"
    suffix = "" if len(items) <= limit else ", ..."
    return ", ".join(parts) + suffix


def _impact_priority(change_type: str) -> int:
    order = {"added": 0, "removed": 1, "moved": 2}
    return order.get(change_type, 9)


def _diff_projection(item: dict[str, Any], *, rank_key: str) -> dict[str, Any]:
    return {
        "candidate_id": item.get("candidate_id") or "",
        "display_name": item.get("display_name") or "",
        "rank": int(item.get(rank_key) or 0),
        "score": round(float(item.get("score") or 0.0), 2),
        "confidence_label": item.get("confidence_label") or "",
    }


def _normalized_list(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    return [str(item) for item in value]


def _display_names(items: list[dict[str, Any]], *, limit: int = 3) -> str:
    names = [str(item.get("display_name") or item.get("candidate_id") or "").strip() for item in items]
    visible = [name for name in names if name][:limit]
    if not visible:
        return "n/a"
    suffix = "" if len(items) <= limit else ", ..."
    return ", ".join(visible) + suffix


def _change_details_suffix(change: dict[str, Any]) -> str:
    added_values = change.get("added_values") or []
    removed_values = change.get("removed_values") or []
    details: list[str] = []
    if added_values:
        details.append(f"added [{', '.join(str(item) for item in added_values[:4])}]")
    if removed_values:
        details.append(f"removed [{', '.join(str(item) for item in removed_values[:4])}]")
    if details:
        return f" ({'; '.join(details)})"
    old_value = change.get("old")
    new_value = change.get("new")
    if old_value is None and new_value is not None:
        return f" (set to {new_value})"
    if old_value is not None and new_value is None:
        return " (cleared)"
    return ""
