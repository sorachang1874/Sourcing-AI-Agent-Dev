from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from .asset_catalog import AssetCatalog
from .domain import (
    Candidate,
    EvidenceRecord,
    format_display_name,
    make_candidate_id,
    make_evidence_id,
    merge_candidate,
)
from .xlsx_reader import XlsxWorkbook


@dataclass(slots=True)
class BootstrapBundle:
    candidates: list[Candidate]
    evidence: list[EvidenceRecord]
    stats: dict[str, Any]


def load_bootstrap_bundle(catalog: AssetCatalog) -> BootstrapBundle:
    workbook = XlsxWorkbook(catalog.anthropic_workbook)
    candidates: dict[str, Candidate] = {}
    evidence: list[EvidenceRecord] = []
    stats = {
        "assets": {
            "workbook": str(catalog.anthropic_workbook),
            "scholar_scan_results": str(catalog.scholar_scan_results),
            "investor_members_json": str(catalog.investor_members_json),
        },
        "sheets": {},
    }

    current_rows = workbook.read_sheet("在职华人员工")
    stats["sheets"]["在职华人员工"] = len(current_rows)
    for row in current_rows:
        candidate = _current_employee_candidate(row, catalog.anthropic_workbook)
        _merge_candidate(candidates, candidate)
        evidence.append(_sheet_evidence(candidate, "Anthropic 在职华人员工", row, catalog.anthropic_workbook))

    former_rows = workbook.read_sheet("已离职华人员工")
    stats["sheets"]["已离职华人员工"] = len(former_rows)
    for row in former_rows:
        candidate = _former_employee_candidate(row, catalog.anthropic_workbook)
        _merge_candidate(candidates, candidate)
        evidence.append(_sheet_evidence(candidate, "Anthropic 已离职华人员工", row, catalog.anthropic_workbook))

    investor_rows = workbook.read_sheet("主要投资方华人成员")
    stats["sheets"]["主要投资方华人成员"] = len(investor_rows)
    for row in investor_rows:
        candidate = _investor_candidate(row, catalog.anthropic_workbook)
        _merge_candidate(candidates, candidate)
        evidence.append(_sheet_evidence(candidate, "Anthropic 主要投资方华人成员", row, catalog.anthropic_workbook))

    scholar_payload = json.loads(catalog.scholar_scan_results.read_text())
    scholar_stats = {}
    for section in ("new_chinese_candidates", "pending_review"):
        items = scholar_payload.get(section, [])
        scholar_stats[section] = len(items)
        for item in items:
            candidate = _scholar_candidate(item, section, catalog.scholar_scan_results)
            _merge_candidate(candidates, candidate)
            evidence.extend(_scholar_evidence(candidate, item, section, catalog.scholar_scan_results))
    stats["scholar"] = scholar_stats

    investor_payload = json.loads(catalog.investor_members_json.read_text())
    raw_institutions = investor_payload.get("institutions", {})
    stats["investor_json_institutions"] = len(raw_institutions)
    for institution, payload in raw_institutions.items():
        for member in payload.get("members", []):
            candidate = _investor_json_candidate(institution, member, catalog.investor_members_json)
            _merge_candidate(candidates, candidate)
            evidence.append(_investor_json_evidence(candidate, institution, member, catalog.investor_members_json))

    category_counts: dict[str, int] = {}
    for candidate in candidates.values():
        category_counts[candidate.category] = category_counts.get(candidate.category, 0) + 1
    stats["candidate_counts"] = category_counts
    stats["evidence_count"] = len(evidence)
    return BootstrapBundle(candidates=list(candidates.values()), evidence=evidence, stats=stats)


def _merge_candidate(store: dict[str, Candidate], candidate: Candidate) -> None:
    if candidate.candidate_id in store:
        store[candidate.candidate_id] = merge_candidate(store[candidate.candidate_id], candidate)
    else:
        store[candidate.candidate_id] = candidate


def _current_employee_candidate(row: dict[str, str], source_path: Path) -> Candidate:
    name_en, name_zh = _split_name(row.get("姓名", ""))
    role, team = _split_role_team(row.get("现任职位/团队", ""))
    candidate_id = make_candidate_id(name_en, "Anthropic", "Anthropic")
    return Candidate(
        candidate_id=candidate_id,
        name_en=name_en,
        name_zh=name_zh,
        display_name=format_display_name(name_en, name_zh),
        category="employee",
        target_company="Anthropic",
        organization="Anthropic",
        employment_status="current",
        role=role,
        team=team,
        joined_at=row.get("加入时间", ""),
        focus_areas=row.get("主要研究方向", ""),
        education=row.get("教育经历", ""),
        work_history=row.get("主要工作经历", ""),
        notes=row.get("备注", ""),
        linkedin_url=row.get("LinkedIn链接", ""),
        media_url=row.get("媒体链接", ""),
        source_dataset="anthropic_workbook_current",
        source_path=f"{source_path}#在职华人员工",
        metadata={"sheet": "在职华人员工"},
    )


def _former_employee_candidate(row: dict[str, str], source_path: Path) -> Candidate:
    name_en, name_zh = _split_name(row.get("姓名", ""))
    role, team = _split_role_team(row.get("原职位/团队", ""))
    candidate_id = make_candidate_id(name_en, "Anthropic", "Anthropic")
    return Candidate(
        candidate_id=candidate_id,
        name_en=name_en,
        name_zh=name_zh,
        display_name=format_display_name(name_en, name_zh),
        category="former_employee",
        target_company="Anthropic",
        organization="Anthropic",
        employment_status="former",
        role=role,
        team=team,
        joined_at=row.get("加入时间", ""),
        left_at=row.get("离职时间", ""),
        current_destination=row.get("当前去向", ""),
        focus_areas=row.get("主要研究方向", ""),
        education=row.get("教育经历", ""),
        work_history=row.get("主要工作经历", ""),
        linkedin_url=row.get("LinkedIn链接", ""),
        media_url=row.get("媒体链接", ""),
        source_dataset="anthropic_workbook_former",
        source_path=f"{source_path}#已离职华人员工",
        metadata={"sheet": "已离职华人员工"},
    )


def _investor_candidate(row: dict[str, str], source_path: Path) -> Candidate:
    name_en, name_zh = _split_name(row.get("姓名", ""))
    role, team = _split_role_team(row.get("职位", ""))
    organization = row.get("机构", "")
    candidate_id = make_candidate_id(name_en, organization, "Anthropic")
    return Candidate(
        candidate_id=candidate_id,
        name_en=name_en,
        name_zh=name_zh,
        display_name=format_display_name(name_en, name_zh),
        category="investor",
        target_company="Anthropic",
        organization=organization,
        employment_status="current",
        role=role,
        team=team,
        ethnicity_background=row.get("族裔背景", ""),
        investment_involvement=row.get("是否涉及Anthropic投资", ""),
        focus_areas=row.get("投资方向", ""),
        notes=row.get("备注", ""),
        linkedin_url=row.get("LinkedIn链接", ""),
        media_url=row.get("媒体链接", ""),
        source_dataset="anthropic_workbook_investor",
        source_path=f"{source_path}#主要投资方华人成员",
        metadata={"sheet": "主要投资方华人成员"},
    )


def _scholar_candidate(item: dict[str, Any], section: str, source_path: Path) -> Candidate:
    name_en = _stringify(item.get("name_en"))
    name_zh = _stringify(item.get("name_zh"))
    candidate_id = make_candidate_id(name_en, "Anthropic", "Anthropic")
    education = _stringify(item.get("education"))
    notes = item.get("background_note", "") or item.get("chinese_background", "")
    return Candidate(
        candidate_id=candidate_id,
        name_en=name_en,
        name_zh=name_zh,
        display_name=format_display_name(name_en, name_zh),
        category="lead",
        target_company="Anthropic",
        organization=item.get("affiliation", "Anthropic"),
        employment_status="unknown",
        role=item.get("position", ""),
        team=item.get("team", ""),
        focus_areas=_stringify(item.get("research_areas")),
        education=education,
        notes=notes,
        linkedin_url=item.get("linkedin", ""),
        source_dataset=f"scholar_scan_{section}",
        source_path=f"{source_path}#{section}",
        metadata={"section": section, "scholar_url": item.get("scholar_url", "")},
    )


def _investor_json_candidate(institution: str, member: dict[str, Any], source_path: Path) -> Candidate:
    name_en, name_zh = _split_name(member.get("name", ""))
    role, team = _split_role_team(member.get("title", ""))
    candidate_id = make_candidate_id(name_en, institution, "Anthropic")
    return Candidate(
        candidate_id=candidate_id,
        name_en=name_en,
        name_zh=name_zh,
        display_name=format_display_name(name_en, name_zh),
        category="investor",
        target_company="Anthropic",
        organization=institution,
        employment_status="current",
        role=role,
        team=team,
        focus_areas=member.get("focus", ""),
        notes=member.get("note", ""),
        source_dataset="investor_members_json",
        source_path=str(source_path),
        metadata={"confidence": member.get("confidence", "")},
    )


def _sheet_evidence(candidate: Candidate, title: str, row: dict[str, str], source_path: Path) -> EvidenceRecord:
    summary = " | ".join(f"{key}: {value}" for key, value in row.items() if value)
    return EvidenceRecord(
        evidence_id=make_evidence_id(candidate.candidate_id, candidate.source_dataset, title, candidate.linkedin_url),
        candidate_id=candidate.candidate_id,
        source_type="xlsx_sheet",
        title=title,
        url=candidate.linkedin_url or candidate.media_url,
        summary=summary,
        source_dataset=candidate.source_dataset,
        source_path=f"{source_path}#{title}",
        metadata={"row_keys": list(row.keys())},
    )


def _scholar_evidence(
    candidate: Candidate, item: dict[str, Any], section: str, source_path: Path
) -> list[EvidenceRecord]:
    results: list[EvidenceRecord] = []
    sources = [
        ("scholar_profile", item.get("scholar_url", ""), item.get("research_areas", "")),
        ("homepage", item.get("homepage", ""), item.get("background_note", "") or item.get("chinese_background", "")),
    ]
    for source_type, url, note in sources:
        if not url:
            continue
        title = f"Scholar {section} {source_type}"
        results.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(candidate.candidate_id, candidate.source_dataset, title, url),
                candidate_id=candidate.candidate_id,
                source_type=source_type,
                title=title,
                url=url,
                summary=_stringify(note) or "Scholar-derived lead",
                source_dataset=candidate.source_dataset,
                source_path=f"{source_path}#{section}",
                metadata={"section": section},
            )
        )
    return results


def _investor_json_evidence(
    candidate: Candidate, institution: str, member: dict[str, Any], source_path: Path
) -> EvidenceRecord:
    summary = " | ".join(
        part
        for part in [institution, member.get("title", ""), member.get("focus", ""), member.get("note", "")]
        if part
    )
    return EvidenceRecord(
        evidence_id=make_evidence_id(candidate.candidate_id, "investor_members_json", institution, ""),
        candidate_id=candidate.candidate_id,
        source_type="json_record",
        title=f"{institution} raw investor record",
        url="",
        summary=summary,
        source_dataset="investor_members_json",
        source_path=str(source_path),
        metadata={"institution": institution},
    )


def _split_name(raw: str) -> tuple[str, str]:
    parts = [part.strip() for part in raw.splitlines() if part.strip()]
    if not parts:
        return "", ""
    if len(parts) == 1:
        if _contains_cjk(parts[0]):
            return "", parts[0]
        return parts[0], ""
    first, second = parts[0], parts[1]
    if _contains_cjk(first) and not _contains_cjk(second):
        return second, first
    return first, second


def _split_role_team(raw: str) -> tuple[str, str]:
    parts = [part.strip() for part in raw.splitlines() if part.strip()]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " | ".join(parts[1:])


def _contains_cjk(value: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in value)


def _stringify(value: Any) -> str:
    if isinstance(value, list):
        return " | ".join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip() if value is not None else ""
