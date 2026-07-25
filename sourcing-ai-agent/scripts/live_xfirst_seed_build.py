#!/usr/bin/env python3
"""Build X-First seed inputs for Layer 1-3 candidates (offline, $0).

Produces the same seed shape the TML/OpenAI live runs consumed
(``{"seeds": [{seed_ref, source_kind, external_record_ref,
source_record_sha256, source_status, source_profile_url, name_text,
x_handle_proposals, professional_facts}]}``; contract:
x-first `contracts/x.portable.research_campaign.request.v1.schema.json`
`$defs.seed_input`).

OPERATOR DIRECTIVE (2026-07-20): the judge input must carry the COMPLETE
LinkedIn profile, not distilled candidate fields — people describe their
concrete projects in Bio/About, work-experience and education sections.  So
professional_facts are built from the FULL profile envelope
(``harvest_profiles/<sha1(url)>.json`` → ``item``), covering: affiliations,
every experience entry (position @ org (period): description), education
entries, headline, About (full text, ordered ≤1000-char parts), skills,
languages, certifications, honors, LinkedIn projects, patents, publications,
and profile location.  Each fact is citable via ``seed_fact:<evidence_ref>``
per the v1 review contract.  Candidate-document fields are the fallback when
a profile envelope is absent.  No provider calls.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from sourcing_agent.harvest_connectors import _profile_cache_key  # noqa: E402

_FACT_TYPES = {"affiliation", "role", "education", "project", "location", "other"}
_TEMPORAL_STATES = {"current", "historical", "ambiguous", "not_applicable"}


def _sha256_record(record: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def _norm(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _period_text(start: Any, end: Any) -> str:
    s = str(dict(start or {}).get("text") or "") if isinstance(start, dict) else str(start or "")
    e = str(dict(end or {}).get("text") or "") if isinstance(end, dict) else str(end or "")
    return f"{s} – {e}".strip(" –")


def _is_lab_company(company: str, lab_names: set[str]) -> bool:
    normalized = company.strip().lower()
    return any(lab in normalized or normalized in lab for lab in lab_names if lab)


def _list(value: Any) -> list[Any]:
    return [item for item in value if item] if isinstance(value, list) else []


def _url_keys(url: str) -> set[str]:
    """All match keys for a LinkedIn profile url: full normalized url + last
    segment (ACw id or public slug) — envelopes may be keyed by either form."""
    normalized = str(url or "").strip().lower().rstrip("/")
    keys = {normalized} if normalized else set()
    segment = normalized.rsplit("/", 1)[-1] if normalized else ""
    if segment:
        keys.add(segment)
    return {k for k in keys if k}


class ProfileEnvelopeIndex:
    """One-pass index of a snapshot's harvest_profiles dir.

    Maps every known url form (envelope request url, item.linkedinUrl,
    item.publicIdentifier, originalQuery url) to the envelope file, so seeds
    resolve regardless of which url spelling they carry (ACw id vs slug).
    """

    def __init__(self, snapshot_dir: Path) -> None:
        self._dir = snapshot_dir / "harvest_profiles"
        self._by_key: dict[str, Path] = {}
        self._built = False

    def _build(self) -> None:
        if self._built or not self._dir.is_dir():
            self._built = True
            return
        for path in self._dir.glob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict) or not isinstance(payload.get("item"), dict):
                continue
            item = payload["item"]
            candidates = [str(payload.get("_harvest_request", {}).get("profile_url") or payload.get("_harvest_request", {}).get("value") or "")]
            candidates.append(str(item.get("linkedinUrl") or ""))
            candidates.append(str(item.get("publicIdentifier") or ""))
            original = item.get("originalQuery")
            if isinstance(original, dict):
                candidates.append(str(original.get("url") or ""))
            for candidate in candidates:
                for key in _url_keys(candidate):
                    self._by_key.setdefault(key, path)
        self._built = True

    def load(self, linkedin_url: str) -> dict[str, Any]:
        self._build()
        path = None
        for key in _url_keys(linkedin_url):
            path = self._by_key.get(key)
            if path:
                break
        if path is None:
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        item = payload.get("item") if isinstance(payload, dict) else None
        return item if isinstance(item, dict) else {}


def _load_profile_item(snapshot_dir: Path, linkedin_url: str) -> dict[str, Any]:
    path = snapshot_dir / "harvest_profiles" / f"{_profile_cache_key(linkedin_url)}.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    item = payload.get("item")
    return item if isinstance(item, dict) else {}


def _build_profile_facts(
    doc: dict[str, Any],
    item: dict[str, Any],
    *,
    lab_name: str,
    lab_aliases: set[str],
    evidence_base: str,
) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def _add(fact_type: str, value: str, temporal: str) -> None:
        assert fact_type in _FACT_TYPES and temporal in _TEMPORAL_STATES
        value = _norm(value)[:1000]
        if not value or (fact_type, value.lower(), temporal) in seen:
            return
        seen.add((fact_type, value.lower(), temporal))
        facts.append(
            {
                "fact_type": fact_type,
                "value": value,
                "temporal_state": temporal,
                "evidence_ref": f"{evidence_base}/f{len(facts)}",
            }
        )

    source = item or doc  # full profile envelope preferred; candidate doc fallback
    status = str(doc.get("employment_status") or "").strip().lower()
    _add("affiliation", lab_name, "current" if status == "current" else "historical")

    for index, entry in enumerate(e for e in _list(source.get("experience")) if isinstance(e, dict)):
        company = _norm(entry.get("companyName") or entry.get("company") or entry.get("companyUniversalName"))
        position = _norm(entry.get("position") or entry.get("title"))
        end = entry.get("endDate")
        end_text = str(dict(end or {}).get("text") or "").strip().lower() if isinstance(end, dict) else str(end or "").strip().lower()
        temporal = "current" if (index == 0 and (not end_text or end_text == "present")) else "historical"
        if company and not _is_lab_company(company, lab_aliases | {lab_name.lower()}):
            _add("affiliation", company, temporal)
        if position:
            role_value = f"{position} @ {company} ({_period_text(entry.get('startDate'), entry.get('endDate'))})".strip()
            description = _norm(entry.get("description"))[:700]
            if description:
                role_value = f"{role_value}: {description}"
            _add("role", role_value, temporal)

    for entry in (e for e in _list(source.get("education")) if isinstance(e, dict)):
        value = " ".join(
            part
            for part in [_norm(entry.get("schoolName")), _norm(entry.get("degree")), _norm(entry.get("fieldOfStudy"))]
            if part
        )
        note = _norm(entry.get("description") or entry.get("activities"))[:500]
        if note:
            value = f"{value}: {note}" if value else note
        if value:
            _add("education", value, "not_applicable")

    headline = _norm(source.get("headline"))
    if headline:
        _add("other", f"Headline: {headline}", "current")

    about = _norm(source.get("about"))
    for part_index in range(0, min(len(about), 8000), 1000):
        _add("other", f"About: {about[part_index:part_index + 1000]}", "current")

    location = _norm(doc.get("location")) or _norm(dict(source.get("location") or {}).get("linkedinText") if isinstance(source.get("location"), dict) else source.get("location"))
    if location:
        _add("location", location, "current")

    skill_names = [_norm(s.get("name") if isinstance(s, dict) else s) for s in _list(source.get("skills"))]
    skill_names = [n for n in skill_names if n][:60]
    if skill_names:
        _add("other", "Skills: " + ", ".join(skill_names), "current")

    language_names = [_norm(l.get("name") if isinstance(l, dict) else l) for l in _list(source.get("languages"))]
    language_names = [n for n in language_names if n]
    if language_names:
        _add("other", "Languages: " + ", ".join(language_names), "current")

    for entry in [e for e in _list(source.get("certifications")) if isinstance(e, dict)][:12]:
        name = _norm(entry.get("name") or entry.get("title"))
        issuer = _norm(entry.get("authority") or entry.get("issuer") or entry.get("issuedBy"))
        if name:
            _add("other", f"Certification: {name}" + (f" ({issuer})" if issuer else ""), "not_applicable")

    for entry in [e for e in _list(source.get("honorsAndAwards")) if isinstance(e, dict)][:12]:
        title = _norm(entry.get("title"))
        issuer = _norm(entry.get("issuedBy") or entry.get("issuer"))
        if title:
            _add("other", f"Honor: {title}" + (f" ({issuer})" if issuer else ""), "not_applicable")

    for entry in [e for e in _list(source.get("projects")) if isinstance(e, dict)][:12]:
        name = _norm(entry.get("title") or entry.get("name"))
        description = _norm(entry.get("description"))[:600]
        if name:
            _add("project", f"{name}: {description}" if description else name, "not_applicable")

    for section in ("patents", "publications"):
        for entry in _list(source.get(section))[:12]:
            if isinstance(entry, dict):
                title = _norm(entry.get("title") or entry.get("name"))
                extra = _norm(entry.get("publisher") or entry.get("issuer") or entry.get("date"))
            else:
                title, extra = _norm(entry), ""
            if title:
                _add("project", f"{title} ({extra})" if extra else title, "not_applicable")
    return facts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--snapshot-dir", required=True, type=Path)
    parser.add_argument("--layered-analysis", required=True, type=Path)
    parser.add_argument("--lab-tag", required=True, help="short tag for seed_ref, e.g. gdm")
    parser.add_argument("--lab-name", required=True, help="canonical lab affiliation value, e.g. 'Google DeepMind'")
    parser.add_argument("--lab-alias", action="append", default=[], help="extra alias for lab-company detection")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    snapshot_dir: Path = args.snapshot_dir.resolve()
    analysis = json.loads(args.layered_analysis.read_text(encoding="utf-8"))
    selected: dict[str, str] = {}
    for c in list(analysis.get("candidates") or []):
        if int(dict(c).get("final_layer") or 0) >= 1:
            cid = str(c.get("candidate_id") or "").strip()
            if cid:
                selected[cid] = str(c.get("display_name") or "").strip()
    selected_ids = list(selected)
    payload = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
    documents = payload.get("candidates") if isinstance(payload, dict) else payload
    by_id = {str(d.get("candidate_id") or "").strip(): d for d in list(documents or []) if isinstance(d, dict)}

    snapshot_id = snapshot_dir.name
    lab_aliases = {str(a).strip().lower() for a in args.lab_alias if str(a).strip()}
    envelope_index = ProfileEnvelopeIndex(snapshot_dir)
    seeds: list[dict[str, Any]] = []
    missing = no_profile = 0
    for candidate_id in selected_ids:
        doc = by_id.get(candidate_id)
        if doc is None:
            missing += 1
            continue
        url = str(doc.get("linkedin_url") or "").strip()
        name = str(doc.get("display_name") or doc.get("name_en") or "").strip() or selected.get(candidate_id, "")
        if not url or not name:
            missing += 1
            continue
        item = envelope_index.load(url) or _load_profile_item(snapshot_dir, url)
        if not item:
            no_profile += 1
        record = dict(doc)
        record.pop("candidate_id", None)
        seeds.append(
            {
                "seed_ref": f"{args.lab_tag}-l123-{candidate_id}",
                "source_kind": "linkedin_profile",
                "external_record_ref": candidate_id,
                "source_record_sha256": _sha256_record(record),
                "source_status": "source_bound",
                "source_profile_url": url,
                "name_text": name,
                "x_handle_proposals": [],
                "professional_facts": _build_profile_facts(
                    doc,
                    item,
                    lab_name=args.lab_name,
                    lab_aliases=lab_aliases,
                    evidence_base=f"snapshot:{snapshot_id}/candidate_documents.json/{candidate_id}",
                ),
            }
        )

    seed_refs = [s["seed_ref"] for s in seeds]
    assert len(seed_refs) == len(set(seed_refs)), "duplicate seed_ref"
    for seed in seeds:
        refs = [f["evidence_ref"] for f in seed["professional_facts"]]
        assert len(refs) == len(set(refs)), f"duplicate evidence_ref in {seed['seed_ref']}"
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"seeds": seeds}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    status_counts: dict[str, int] = {}
    fact_counts: list[int] = []
    for s in seeds:
        doc = by_id[s["external_record_ref"]]
        key = str(doc.get("employment_status") or "").strip() or "unknown"
        status_counts[key] = status_counts.get(key, 0) + 1
        fact_counts.append(len(s["professional_facts"]))
    fact_counts.sort()
    print(f"seeds={len(seeds)} skipped_missing={missing} no_profile_envelope={no_profile} by_status={status_counts}")
    print(f"facts min/median/max: {fact_counts[0]}/{fact_counts[len(fact_counts)//2]}/{fact_counts[-1]} out={args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
