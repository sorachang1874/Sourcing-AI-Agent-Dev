#!/usr/bin/env python3
"""Layer 1-3 → CRM + X-First export CSV (13 columns, committed builder).

One-pass successor of the TML/OpenAI /tmp builders (build_tml_csv.py +
openai_csv_backfill.py).  Column contract:

  CRM (mirrors orchestrator._build_target_candidate_export_csv):
    姓名, LinkedIn 链接, 华人线索信息分层结果, 在职状态, 工作经历, 教育经历, 地区, Email
  X-First:
    X账号, X账号已确认, Pre-train方向经历, 判断置信度, 证据摘要

Inputs: snapshot candidate_documents (+ enriched profile fields) + layering
analysis + grok collection (optional) + luna/judge batch (optional) +
account-resolution conflicts (optional).  With no grok/luna inputs it emits
the CRM columns + "待采集"/"待判断" placeholders so the file can be produced
early and regenerated later (deterministic, idempotent).  Offline.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from pathlib import Path
from typing import Any

FIELDS = [
    "姓名", "LinkedIn 链接", "华人线索信息分层结果", "在职状态",
    "工作经历", "教育经历", "地区", "Email",
    "X账号", "X账号已确认", "Pre-train方向经历", "判断置信度", "证据摘要",
]

STATE_LABEL = {
    "current": "有（current）",
    "historical": "有（historical）",
    "ambiguous": "疑似（ambiguous）",
    "unsupported": "无",
}


def _period(start: Any, end: Any) -> str:
    s = str(dict(start or {}).get("text") or "") if isinstance(start, dict) else str(start or "")
    e = str(dict(end or {}).get("text") or "") if isinstance(end, dict) else str(end or "")
    return f"{s} – {e}".strip(" –")


def _work_history(doc: dict[str, Any], limit: int = 8) -> str:
    entries = doc.get("experience") or []
    if not isinstance(entries, list):
        return str(doc.get("role") or "")
    parts = []
    for entry in entries[:limit]:
        if not isinstance(entry, dict):
            continue
        parts.append(
            f"{entry.get('position') or ''} @ {entry.get('companyName') or ''} ({_period(entry.get('startDate'), entry.get('endDate'))})".strip()
        )
    return " | ".join(p for p in parts if p)


def _education(doc: dict[str, Any], limit: int = 4) -> str:
    entries = doc.get("education") or []
    if not isinstance(entries, list):
        return str(doc.get("education") or "")
    parts = []
    for entry in entries[:limit]:
        if not isinstance(entry, dict):
            continue
        parts.append(" ".join(p for p in [entry.get("schoolName"), entry.get("degree"), entry.get("fieldOfStudy")] if p))
    return " | ".join(p for p in parts if p)


def _evidence_summary(review: dict[str, Any], bundle: dict[str, Any], seed_facts: dict[str, dict[str, Any]] | None = None) -> str:
    items = {str(i.get("stable_post_id")): i for i in list(bundle.get("items") or [])}
    bio = str((bundle.get("x_bio") or {}).get("text") or "").strip()
    snippets: list[str] = []
    for cit in list(review.get("pretraining_experience_evidence_citations") or [])[:4]:
        cit = str(cit)
        if cit == "x_bio":
            if bio:
                snippets.append(f"bio: {bio[:160]}")
        elif cit.startswith("post:") and cit[5:] in items:
            text = " ".join(str(items[cit[5:]].get("text") or "").split())
            kind = items[cit[5:]].get("kind") or "post"
            snippets.append(f"{kind}: {text[:180]}")
        elif cit.startswith("seed_fact:") and seed_facts:
            fact = seed_facts.get(cit[len("seed_fact:"):])
            if fact:
                value = " ".join(str(fact.get("value") or "").split())
                snippets.append(f"profile[{fact.get('fact_type') or 'fact'}]: {value[:180]}")
    return " | ".join(snippets)[:600]


def main() -> int:
    import argparse as _ap

    parser = _ap.ArgumentParser(description=__doc__, formatter_class=_ap.RawDescriptionHelpFormatter)
    parser.add_argument("--snapshot-dir", required=True, type=Path)
    parser.add_argument("--layered-analysis", required=True, type=Path)
    parser.add_argument("--grok-collection", type=Path, default=None)
    parser.add_argument("--luna-batch", type=Path, default=None)
    parser.add_argument("--account-conflicts", type=Path, default=None)
    parser.add_argument("--seeds", type=Path, default=None, help="seeds file (enables rendering seed_fact citations into 证据摘要)")
    parser.add_argument("--judge-model-note", default="deepseek-v4-flash")
    parser.add_argument("--seed-ref-prefix", required=True, help="seed_ref prefix, e.g. 'gdm-l123' (must match the seeds file)")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    payload = json.loads((args.snapshot_dir.resolve() / "candidate_documents.json").read_text(encoding="utf-8"))
    docs = {str(c.get("candidate_id")): c for c in list((payload.get("candidates") if isinstance(payload, dict) else payload) or [])}
    analysis = json.loads(args.layered_analysis.read_text(encoding="utf-8"))
    layer_entries = [c for c in list(analysis.get("candidates") or []) if int(dict(c).get("final_layer") or 0) >= 1]

    bundles: dict[str, dict[str, Any]] = {}
    grok_failed: set[str] = set()
    if args.grok_collection:
        coll = json.loads(args.grok_collection.read_text(encoding="utf-8"))
        for r in list(coll.get("results") or []):
            ref = str(r.get("candidate_ref") or "")
            if r.get("status") == "completed":
                bundles[ref] = dict(r.get("bundle") or {})
            else:
                grok_failed.add(ref)

    reviews: dict[str, dict[str, Any]] = {}
    review_failed: set[str] = set()
    if args.luna_batch:
        batch = json.loads(args.luna_batch.read_text(encoding="utf-8"))
        for r in list(batch.get("results") or []):
            ref = str(r.get("candidate_ref") or "")
            if r.get("status") == "completed":
                reviews[ref] = dict(r.get("review") or {})
            else:
                review_failed.add(ref)

    conflicts: set[str] = set()
    if args.account_conflicts:
        conflicts = {str(v) for v in json.loads(args.account_conflicts.read_text(encoding="utf-8"))}

    seed_facts_by_ref: dict[str, dict[str, dict[str, Any]]] = {}
    if args.seeds:
        seeds_payload = json.loads(args.seeds.read_text(encoding="utf-8"))
        for seed in list(seeds_payload.get("seeds") or []):
            ref = str(seed.get("seed_ref") or "")
            seed_facts_by_ref[ref] = {
                str(f.get("evidence_ref")): dict(f) for f in list(seed.get("professional_facts") or [])
            }

    seed_ref_by_candidate: dict[str, str] = {}
    rows: list[dict[str, str]] = []
    for entry in layer_entries:
        candidate_id = str(entry.get("candidate_id") or "")
        doc = docs.get(candidate_id)
        if doc is None:
            continue
        seed_ref = seed_ref_by_candidate.setdefault(candidate_id, f"{args.seed_ref_prefix}-{candidate_id}")
        display_name = str(doc.get("display_name") or doc.get("name_en") or "").strip()
        row = {
            "姓名": display_name,
            "LinkedIn 链接": str(doc.get("linkedin_url") or ""),
            "华人线索信息分层结果": f"Layer {int(dict(entry).get('final_layer') or 0)}",
            "在职状态": str(doc.get("employment_status") or ""),
            "工作经历": _work_history(doc),
            "教育经历": _education(doc),
            "地区": str(doc.get("location") or ""),
            "Email": str(doc.get("email") or ""),
            "X账号": "",
            "X账号已确认": "否",
            "Pre-train方向经历": "待采集" if not args.grok_collection else "无X账号",
            "判断置信度": "",
            "证据摘要": "",
        }
        if candidate_id in conflicts or seed_ref in conflicts:
            row["Pre-train方向经历"] = "账号解析冲突（两人同号，已剔除）"
        elif candidate_id in grok_failed or seed_ref in grok_failed:
            row["Pre-train方向经历"] = "X采集失败"
        else:
            bundle = bundles.get(candidate_id) or bundles.get(seed_ref) or {}
            resolution = bundle.get("account_resolution") or {}
            confidence = str(resolution.get("resolution_confidence") or "")
            if bundle and confidence and confidence != "not_found":
                row["X账号"] = str(resolution.get("handle") or "")
                row["X账号已确认"] = "是"
                review = reviews.get(candidate_id) or reviews.get(seed_ref)
                if review is not None:
                    state = str(review.get("proposed_pretraining_experience_state") or "ambiguous")
                    row["Pre-train方向经历"] = STATE_LABEL.get(state, f"疑似（{state}）")
                    n_cit = len(review.get("pretraining_experience_evidence_citations") or [])
                    row["判断置信度"] = f"账号{confidence}；证据引用{n_cit}条；{args.judge_model_note}"
                    row["证据摘要"] = _evidence_summary(review, bundle, seed_facts_by_ref.get(seed_ref) or seed_facts_by_ref.get(candidate_id))
                elif candidate_id in review_failed or seed_ref in review_failed:
                    row["Pre-train方向经历"] = "判断失败"
                    row["判断置信度"] = confidence
                else:
                    row["Pre-train方向经历"] = "待判断"
            elif bundle and confidence == "not_found":
                review = reviews.get(candidate_id) or reviews.get(seed_ref)
                if review is not None:
                    # No X account, but the judge still evaluated the profile axis
                    # (operator: LinkedIn profile is a first-class evidence source) —
                    # surface the judgment instead of masking it behind "无X账号";
                    # X账号已确认=否 already carries the no-account state.
                    state = str(review.get("proposed_pretraining_experience_state") or "ambiguous")
                    row["Pre-train方向经历"] = STATE_LABEL.get(state, f"疑似（{state}）") + "（无X账号·据profile）"
                    n_cit = len(review.get("pretraining_experience_evidence_citations") or [])
                    row["判断置信度"] = f"无X账号；证据引用{n_cit}条；{args.judge_model_note}（据profile）"
                    row["证据摘要"] = _evidence_summary(review, bundle, seed_facts_by_ref.get(seed_ref) or seed_facts_by_ref.get(candidate_id))
                else:
                    row["Pre-train方向经历"] = "无X账号"
            else:
                row["Pre-train方向经历"] = "待采集"  # no grok result for this person (partial collection)
        rows.append(row)

    rows.sort(key=lambda r: (r["华人线索信息分层结果"], r["姓名"]))
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(buf.getvalue(), encoding="utf-8-sig", newline="")

    from collections import Counter

    dist = Counter(r["Pre-train方向经历"] for r in rows)
    accounts = sum(1 for r in rows if r["X账号已确认"] == "是")
    print(f"rows={len(rows)} accounts_confirmed={accounts} -> {args.out}")
    for key, count in dist.most_common():
        print(f"  {count:5d}  {key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
