from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any
from urllib import parse

from .public_web_search import normalize_email, normalize_model_link_signal_type, public_web_link_shape_warnings

PROFILE_LINK_TYPES_REQUIRING_IDENTITY = {
    "github_url",
    "x_url",
    "substack_url",
    "scholar_url",
    "personal_homepage",
    "resume_url",
    "academic_profile",
    "publication_url",
}
SOCIAL_OR_MEDIA_LINK_TYPES = {"github_url", "x_url", "substack_url", "scholar_url"}
TRUSTED_IDENTITY_LABELS = {"confirmed", "likely_same_person", "strong"}
REVIEW_IDENTITY_LABELS = {"", "unreviewed", "needs_review", "needs_ai_review", "ambiguous_identity"}
BAD_IDENTITY_LABELS = {"not_same_person", "wrong_person"}
QUALITY_SIGNAL_COLUMNS = [
    "candidate_name",
    "record_id",
    "artifact_path",
    "signal_group",
    "signal_type",
    "raw_signal_type",
    "value",
    "source_url",
    "source_domain",
    "source_family",
    "identity_match_label",
    "identity_match_score",
    "confidence_label",
    "confidence_score",
    "publishable",
    "promotion_status",
    "suppression_reason",
    "evidence_present",
    "severity",
    "issues",
]


def evaluate_public_web_quality_paths(paths: list[str | Path]) -> dict[str, Any]:
    signal_paths = _discover_signal_paths(paths)
    candidates = [evaluate_public_web_signals_file(path) for path in signal_paths]
    summary = _summarize_quality(candidates)
    return {
        "status": "ok" if signal_paths else "not_found",
        "summary": summary,
        "candidates": candidates,
    }


def evaluate_public_web_signals_file(path: str | Path) -> dict[str, Any]:
    signal_path = Path(path).expanduser()
    try:
        payload = json.loads(signal_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "artifact_path": str(signal_path),
            "status": "invalid",
            "reason": f"{type(exc).__name__}: {exc}",
            "signals": [],
            "issues": [
                {
                    "severity": "high",
                    "code": "signals_json_unreadable",
                    "message": "signals.json could not be read as JSON",
                }
            ],
        }
    if not isinstance(payload, dict):
        return {
            "artifact_path": str(signal_path),
            "status": "invalid",
            "reason": "signals payload is not an object",
            "signals": [],
            "issues": [
                {
                    "severity": "high",
                    "code": "signals_json_invalid_shape",
                    "message": "signals.json must contain an object payload",
                }
            ],
        }
    return evaluate_public_web_signals_payload(payload, artifact_path=str(signal_path))


def evaluate_public_web_signals_payload(payload: dict[str, Any], *, artifact_path: str = "") -> dict[str, Any]:
    candidate = dict(payload.get("candidate") or {})
    candidate_name = str(candidate.get("candidate_name") or "").strip()
    record_id = str(candidate.get("record_id") or "").strip()
    signal_rows: list[dict[str, Any]] = []
    for email in list(payload.get("email_candidates") or []):
        if isinstance(email, dict):
            signal_rows.append(_evaluate_email_signal(email, candidate=candidate, artifact_path=artifact_path))
    for link in list(payload.get("entry_links") or []):
        if isinstance(link, dict):
            signal_rows.append(_evaluate_link_signal(link, candidate=candidate, artifact_path=artifact_path))
    issues = [
        {
            "severity": row["severity"],
            "code": code,
            "message": _issue_message(code),
            "signal_group": row["signal_group"],
            "signal_type": row["signal_type"],
            "value": row["value"],
            "source_url": row["source_url"],
        }
        for row in signal_rows
        for code in row["issues"]
    ]
    return {
        "status": "ok",
        "artifact_path": artifact_path,
        "candidate_name": candidate_name,
        "record_id": record_id,
        "candidate_id": str(candidate.get("candidate_id") or ""),
        "email_candidate_count": sum(1 for row in signal_rows if row["signal_group"] == "email"),
        "profile_link_count": sum(1 for row in signal_rows if row["signal_group"] == "profile_link"),
        "promotion_recommended_email_count": sum(
            1
            for row in signal_rows
            if row["signal_group"] == "email" and row["promotion_status"] == "promotion_recommended"
        ),
        "trusted_media_link_count": sum(
            1
            for row in signal_rows
            if row["signal_group"] == "profile_link"
            and row["signal_type"] in SOCIAL_OR_MEDIA_LINK_TYPES
            and row["identity_match_label"] in TRUSTED_IDENTITY_LABELS
            and row["publishable"]
        ),
        "issue_count": len(issues),
        "high_issue_count": sum(1 for issue in issues if issue["severity"] == "high"),
        "medium_issue_count": sum(1 for issue in issues if issue["severity"] == "medium"),
        "signals": signal_rows,
        "issues": issues,
    }


def write_public_web_quality_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, str]:
    target_dir = Path(output_dir).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / "public_web_quality_report.json"
    csv_path = target_dir / "public_web_quality_signals.csv"
    markdown_path = target_dir / "public_web_quality_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    rows = _flatten_quality_rows(report)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=QUALITY_SIGNAL_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in QUALITY_SIGNAL_COLUMNS})
    markdown_path.write_text(_quality_report_markdown(report), encoding="utf-8")
    return {
        "json": str(json_path),
        "csv": str(csv_path),
        "markdown": str(markdown_path),
    }


def _discover_signal_paths(paths: list[str | Path]) -> list[Path]:
    discovered: list[Path] = []
    seen: set[str] = set()
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        if path.is_file() and path.name == "signals.json":
            candidates = [path]
        elif path.is_dir():
            candidates = sorted(path.glob("**/signals.json"))
        else:
            candidates = []
        for candidate_path in candidates:
            key = str(candidate_path.resolve())
            if key in seen:
                continue
            seen.add(key)
            discovered.append(candidate_path)
    return discovered


def _evaluate_email_signal(email: dict[str, Any], *, candidate: dict[str, Any], artifact_path: str) -> dict[str, Any]:
    value = str(email.get("normalized_value") or email.get("value") or "").strip()
    source_url = str(email.get("source_url") or "").strip()
    identity_label = _normalize_identity_label(email.get("identity_match_label"))
    confidence_score = _coerce_float(email.get("confidence_score"))
    publishable = bool(email.get("publishable"))
    promotion_status = str(email.get("promotion_status") or "").strip()
    suppression_reason = str(email.get("suppression_reason") or "").strip()
    email_type = str(email.get("email_type") or "unknown").strip() or "unknown"
    issues: list[str] = []
    if not normalize_email(value):
        issues.append("invalid_email_value")
    if not source_url:
        issues.append("email_missing_source_url")
    if not str(email.get("source_family") or "").strip():
        issues.append("email_missing_source_family")
    if promotion_status == "promotion_recommended" and not publishable:
        issues.append("promotion_recommended_but_not_publishable")
    if promotion_status == "promotion_recommended" and suppression_reason:
        issues.append("promotion_recommended_but_suppressed")
    if promotion_status == "promotion_recommended" and identity_label not in TRUSTED_IDENTITY_LABELS:
        issues.append("promotion_recommended_without_trusted_identity")
    if promotion_status == "promotion_recommended" and email_type in {"generic", "unknown"}:
        issues.append("promotion_recommended_generic_or_unknown_email")
    if promotion_status == "promotion_recommended" and not str(email.get("evidence_excerpt") or "").strip():
        issues.append("promotion_recommended_missing_evidence_excerpt")
    if identity_label in BAD_IDENTITY_LABELS and publishable:
        issues.append("bad_identity_marked_publishable")
    return _quality_row(
        candidate=candidate,
        artifact_path=artifact_path,
        signal_group="email",
        signal_type=email_type,
        raw_signal_type=email_type,
        value=value,
        source_url=source_url,
        source_domain=str(email.get("source_domain") or _domain_from_url(source_url)),
        source_family=str(email.get("source_family") or ""),
        identity_match_label=identity_label,
        identity_match_score=_coerce_float(email.get("identity_match_score")),
        confidence_label=str(email.get("confidence_label") or ""),
        confidence_score=confidence_score,
        publishable=publishable,
        promotion_status=promotion_status,
        suppression_reason=suppression_reason,
        evidence_present=bool(str(email.get("evidence_excerpt") or "").strip()),
        issues=issues,
    )


def _evaluate_link_signal(link: dict[str, Any], *, candidate: dict[str, Any], artifact_path: str) -> dict[str, Any]:
    url = str(link.get("normalized_url") or link.get("url") or "").strip()
    raw_entry_type = str(link.get("entry_type") or "other").strip() or "other"
    entry_type = _normalize_quality_link_type(raw_entry_type, url=url)
    identity_label = _normalize_identity_label(link.get("identity_match_label"))
    source_domain = str(link.get("source_domain") or _domain_from_url(url))
    source_family = str(link.get("source_family") or "").strip()
    issues: list[str] = []
    if entry_type in PROFILE_LINK_TYPES_REQUIRING_IDENTITY and not url:
        issues.append("profile_link_missing_url")
    if entry_type in PROFILE_LINK_TYPES_REQUIRING_IDENTITY and identity_label in REVIEW_IDENTITY_LABELS:
        issues.append("profile_link_needs_identity_review")
    if entry_type in PROFILE_LINK_TYPES_REQUIRING_IDENTITY and identity_label in BAD_IDENTITY_LABELS:
        issues.append("profile_link_bad_identity")
    if entry_type in SOCIAL_OR_MEDIA_LINK_TYPES and identity_label not in TRUSTED_IDENTITY_LABELS:
        issues.append("media_link_without_trusted_identity")
    for warning in public_web_link_shape_warnings(entry_type, url):
        if warning not in issues:
            issues.append(warning)
    if entry_type in PROFILE_LINK_TYPES_REQUIRING_IDENTITY and not source_domain:
        issues.append("profile_link_missing_source_domain")
    if entry_type in PROFILE_LINK_TYPES_REQUIRING_IDENTITY and not source_family:
        issues.append("profile_link_missing_source_family")
    if raw_entry_type != entry_type:
        issues.append("non_canonical_profile_link_type")
    return _quality_row(
        candidate=candidate,
        artifact_path=artifact_path,
        signal_group="profile_link",
        signal_type=entry_type,
        raw_signal_type=raw_entry_type,
        value=url,
        source_url=url,
        source_domain=source_domain,
        source_family=source_family,
        identity_match_label=identity_label,
        identity_match_score=_coerce_float(link.get("identity_match_score")),
        confidence_label=str(link.get("confidence_label") or ""),
        confidence_score=_coerce_float(link.get("score")),
        publishable=identity_label in TRUSTED_IDENTITY_LABELS
        and not public_web_link_shape_warnings(entry_type, url),
        promotion_status="",
        suppression_reason="",
        evidence_present=bool(str(link.get("snippet") or link.get("title") or "").strip()),
        issues=issues,
    )


def _quality_row(
    *,
    candidate: dict[str, Any],
    artifact_path: str,
    signal_group: str,
    signal_type: str,
    raw_signal_type: str,
    value: str,
    source_url: str,
    source_domain: str,
    source_family: str,
    identity_match_label: str,
    identity_match_score: float,
    confidence_label: str,
    confidence_score: float,
    publishable: bool,
    promotion_status: str,
    suppression_reason: str,
    evidence_present: bool,
    issues: list[str],
) -> dict[str, Any]:
    return {
        "candidate_name": str(candidate.get("candidate_name") or ""),
        "record_id": str(candidate.get("record_id") or ""),
        "artifact_path": artifact_path,
        "signal_group": signal_group,
        "signal_type": signal_type,
        "raw_signal_type": raw_signal_type,
        "value": value,
        "source_url": source_url,
        "source_domain": source_domain,
        "source_family": source_family,
        "identity_match_label": identity_match_label,
        "identity_match_score": identity_match_score,
        "confidence_label": confidence_label,
        "confidence_score": confidence_score,
        "publishable": publishable,
        "promotion_status": promotion_status,
        "suppression_reason": suppression_reason,
        "evidence_present": evidence_present,
        "severity": _severity_for_issues(issues),
        "issues": issues,
    }


def _summarize_quality(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    signal_rows = [row for candidate in candidates for row in list(candidate.get("signals") or [])]
    issue_rows = [issue for candidate in candidates for issue in list(candidate.get("issues") or [])]
    severity_counts = Counter(str(issue.get("severity") or "unknown") for issue in issue_rows)
    return {
        "candidate_count": len(candidates),
        "signal_count": len(signal_rows),
        "email_candidate_count": sum(1 for row in signal_rows if row["signal_group"] == "email"),
        "profile_link_count": sum(1 for row in signal_rows if row["signal_group"] == "profile_link"),
        "promotion_recommended_email_count": sum(
            1
            for row in signal_rows
            if row["signal_group"] == "email" and row["promotion_status"] == "promotion_recommended"
        ),
        "trusted_media_link_count": sum(
            1
            for row in signal_rows
            if row["signal_group"] == "profile_link"
            and row["signal_type"] in SOCIAL_OR_MEDIA_LINK_TYPES
            and row["identity_match_label"] in TRUSTED_IDENTITY_LABELS
            and row["publishable"]
        ),
        "issue_count": len(issue_rows),
        "high_issue_count": int(severity_counts.get("high", 0)),
        "medium_issue_count": int(severity_counts.get("medium", 0)),
        "low_issue_count": int(severity_counts.get("low", 0)),
        "issue_code_counts": dict(Counter(str(issue.get("code") or "") for issue in issue_rows)),
        "email_type_counts": dict(Counter(row["signal_type"] for row in signal_rows if row["signal_group"] == "email")),
        "email_source_family_counts": dict(
            Counter(row["source_family"] for row in signal_rows if row["signal_group"] == "email")
        ),
        "profile_link_type_counts": dict(
            Counter(row["signal_type"] for row in signal_rows if row["signal_group"] == "profile_link")
        ),
        "profile_source_family_counts": dict(
            Counter(row["source_family"] for row in signal_rows if row["signal_group"] == "profile_link")
        ),
        "trusted_media_type_counts": dict(
            Counter(
                row["signal_type"]
                for row in signal_rows
                if row["signal_group"] == "profile_link"
                and row["signal_type"] in SOCIAL_OR_MEDIA_LINK_TYPES
                and row["identity_match_label"] in TRUSTED_IDENTITY_LABELS
                and row["publishable"]
            )
        ),
        "review_media_type_counts": dict(
            Counter(
                row["signal_type"]
                for row in signal_rows
                if row["signal_group"] == "profile_link"
                and row["signal_type"] in SOCIAL_OR_MEDIA_LINK_TYPES
                and (row["identity_match_label"] not in TRUSTED_IDENTITY_LABELS or not row["publishable"])
            )
        ),
        "identity_label_counts": dict(Counter(row["identity_match_label"] for row in signal_rows)),
    }


def _flatten_quality_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in list(report.get("candidates") or []):
        for row in list(dict(candidate).get("signals") or []):
            flattened = dict(row)
            flattened["issues"] = ";".join(str(item) for item in list(row.get("issues") or []))
            rows.append(flattened)
    return rows


def _quality_report_markdown(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    lines = [
        "# Public Web Quality Report",
        "",
        "> Status: Generated Public Web quality report. Do not treat runtime reports as source documentation.",
        "",
        f"- Candidates: {int(summary.get('candidate_count') or 0)}",
        f"- Signals: {int(summary.get('signal_count') or 0)}",
        f"- Email candidates: {int(summary.get('email_candidate_count') or 0)}",
        f"- Promotion-recommended emails: {int(summary.get('promotion_recommended_email_count') or 0)}",
        f"- Trusted media links: {int(summary.get('trusted_media_link_count') or 0)}",
        f"- Issues: {int(summary.get('issue_count') or 0)} "
        f"(high={int(summary.get('high_issue_count') or 0)}, "
        f"medium={int(summary.get('medium_issue_count') or 0)}, "
        f"low={int(summary.get('low_issue_count') or 0)})",
        "",
        "## Top Issue Codes",
        "",
    ]
    issue_code_counts = Counter(dict(summary.get("issue_code_counts") or {}))
    if issue_code_counts:
        for code, count in issue_code_counts.most_common(20):
            lines.append(f"- `{code}`: {count}")
    else:
        lines.append("- No issues found.")
    lines.extend(["", "## Media Link Quality", ""])
    trusted_media_counts = Counter(dict(summary.get("trusted_media_type_counts") or {}))
    review_media_counts = Counter(dict(summary.get("review_media_type_counts") or {}))
    media_types = sorted(set(trusted_media_counts) | set(review_media_counts) | SOCIAL_OR_MEDIA_LINK_TYPES)
    for media_type in media_types:
        lines.append(
            f"- `{media_type}`: trusted={int(trusted_media_counts.get(media_type, 0))}, "
            f"needs_review={int(review_media_counts.get(media_type, 0))}"
        )
    lines.extend(["", "## Candidate Issues", ""])
    for candidate in list(report.get("candidates") or []):
        issues = list(dict(candidate).get("issues") or [])
        if not issues:
            continue
        lines.append(f"### {dict(candidate).get('candidate_name') or dict(candidate).get('record_id') or 'Unknown'}")
        for issue in issues[:20]:
            lines.append(
                f"- `{issue.get('severity')}` `{issue.get('code')}` "
                f"{issue.get('signal_type') or issue.get('signal_group')}: "
                f"{issue.get('value') or issue.get('source_url') or ''}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _severity_for_issues(issues: list[str]) -> str:
    high_codes = {
        "invalid_email_value",
        "email_missing_source_url",
        "promotion_recommended_but_not_publishable",
        "promotion_recommended_but_suppressed",
        "promotion_recommended_without_trusted_identity",
        "promotion_recommended_generic_or_unknown_email",
        "bad_identity_marked_publishable",
        "profile_link_missing_url",
    }
    medium_codes = {
        "email_missing_source_family",
        "promotion_recommended_missing_evidence_excerpt",
        "profile_link_needs_identity_review",
        "profile_link_bad_identity",
        "media_link_without_trusted_identity",
        "github_repository_or_deep_link_not_profile",
        "x_link_not_profile",
        "substack_link_not_profile_or_publication",
        "scholar_link_not_profile",
        "non_canonical_profile_link_type",
        "profile_link_missing_source_domain",
        "profile_link_missing_source_family",
    }
    if any(code in high_codes for code in issues):
        return "high"
    if any(code in medium_codes for code in issues):
        return "medium"
    if issues:
        return "low"
    return "ok"


def _issue_message(code: str) -> str:
    messages = {
        "invalid_email_value": "Email candidate is not a syntactically valid email.",
        "email_missing_source_url": "Email candidate has no source URL.",
        "email_missing_source_family": "Email candidate has no source family.",
        "promotion_recommended_but_not_publishable": "Email is promotion-recommended but not publishable.",
        "promotion_recommended_but_suppressed": "Email is promotion-recommended despite suppression.",
        "promotion_recommended_without_trusted_identity": "Email is promotion-recommended without trusted identity.",
        "promotion_recommended_generic_or_unknown_email": "Generic/unknown email type should not be recommended.",
        "promotion_recommended_missing_evidence_excerpt": "Recommended email has no evidence excerpt.",
        "bad_identity_marked_publishable": "Signal is publishable despite bad identity.",
        "profile_link_missing_url": "Profile/media link has no URL.",
        "profile_link_needs_identity_review": "Profile/media link still needs identity review.",
        "profile_link_bad_identity": "Profile/media link is marked as a wrong identity.",
        "media_link_without_trusted_identity": "Media link lacks confirmed/likely identity.",
        "github_repository_or_deep_link_not_profile": "GitHub URL appears to be a repo/deep link, not a profile.",
        "x_link_not_profile": "X/Twitter URL appears to be a post, search, or utility page instead of a profile.",
        "substack_link_not_profile_or_publication": "Substack URL does not look like a profile or publication page.",
        "scholar_link_not_profile": "Scholar URL does not look like a Google Scholar profile.",
        "non_canonical_profile_link_type": "Profile/media link used a legacy or model-specific signal type.",
        "profile_link_missing_source_domain": "Profile/media link has no source domain.",
        "profile_link_missing_source_family": "Profile/media link has no source family.",
    }
    return messages.get(code, code)


def _normalize_identity_label(value: Any) -> str:
    return str(value or "").strip().lower()


def _coerce_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _domain_from_url(url: str) -> str:
    try:
        return parse.urlparse(str(url or "")).netloc.lower()
    except Exception:
        return ""


def _normalize_quality_link_type(raw_entry_type: str, *, url: str) -> str:
    normalized = normalize_model_link_signal_type(raw_entry_type, fallback="other")
    if normalized != "other":
        return normalized
    parsed = parse.urlparse(str(url or ""))
    host = parsed.netloc.lower().removeprefix("www.")
    path = parsed.path.lower()
    host_and_path = f"{host}{path}"
    if "linkedin.com/in/" in host_and_path:
        return "linkedin_url"
    if host == "github.com":
        return "github_url"
    if host == "scholar.google.com" or "scholar.google." in host:
        return "scholar_url"
    if host in {"x.com", "twitter.com"}:
        return "x_url"
    if host.endswith("substack.com") or "substack" in host:
        return "substack_url"
    if path.endswith(".pdf") or any(token in path for token in ("/cv", "/resume")):
        return "resume_url"
    return "other"
