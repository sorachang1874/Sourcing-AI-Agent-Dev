from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha1
import json
from pathlib import Path
import re
import time
from typing import TYPE_CHECKING, Any
from urllib import error, parse, request

from .asset_logger import AssetLogger
from .company_registry import BUILTIN_COMPANY_IDENTITIES, COMPANY_ALIASES, normalize_company_key
from .domain import Candidate, EvidenceRecord, format_display_name, make_evidence_id, normalize_candidate, normalize_name_token

if TYPE_CHECKING:
    from .model_provider import ModelClient

@dataclass(slots=True)
class RapidApiAccount:
    account_id: str
    source: str
    provider: str
    host: str
    base_url: str
    api_key: str
    status: str = ""
    monthly_limit: int = 0
    total_used: int = 0
    endpoint_company: str = ""
    endpoint_search: str = ""
    endpoint_profile: str = ""
    method: str = "GET"
    note: str = ""

    @property
    def estimated_remaining(self) -> int:
        if self.monthly_limit <= 0:
            return 0
        return max(self.monthly_limit - self.total_used, 0)

    @property
    def supports_company_roster(self) -> bool:
        text = f"{self.host} {self.endpoint_company}".lower()
        return "company/people" in text or "company-employees" in text

    @property
    def supports_profile_detail(self) -> bool:
        text = f"{self.host} {self.endpoint_profile}".lower()
        return "/people/profile" in text or "/profile/detail" in text or "/profile?" in text

    @property
    def supports_people_search(self) -> bool:
        text = f"{self.host} {self.endpoint_search}".lower()
        return "search/people" in text or "z-real-time-linkedin-scraper-api1" in text


@dataclass(slots=True)
class CompanyIdentity:
    requested_name: str
    canonical_name: str
    company_key: str
    linkedin_slug: str = ""
    linkedin_company_url: str = ""
    domain: str = ""
    aliases: list[str] = field(default_factory=list)
    resolver: str = "heuristic"
    confidence: str = "medium"
    notes: str = ""
    local_asset_available: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CompanyRosterSnapshot:
    snapshot_id: str
    target_company: str
    company_identity: CompanyIdentity
    snapshot_dir: Path
    raw_entries: list[dict[str, Any]]
    visible_entries: list[dict[str, Any]]
    headless_entries: list[dict[str, Any]]
    page_summaries: list[dict[str, Any]]
    accounts_used: list[str]
    errors: list[str]
    stop_reason: str
    merged_path: Path
    visible_path: Path
    headless_path: Path
    summary_path: Path

    def to_record(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "target_company": self.target_company,
            "company_identity": self.company_identity.to_record(),
            "snapshot_dir": str(self.snapshot_dir),
            "raw_entry_count": len(self.raw_entries),
            "visible_entry_count": len(self.visible_entries),
            "headless_entry_count": len(self.headless_entries),
            "page_count": len(self.page_summaries),
            "accounts_used": self.accounts_used,
            "errors": self.errors,
            "stop_reason": self.stop_reason,
            "merged_path": str(self.merged_path),
            "visible_path": str(self.visible_path),
            "headless_path": str(self.headless_path),
            "summary_path": str(self.summary_path),
        }

def resolve_company_identity(
    target_company: str,
    legacy_company_ids_path: Path | None = None,
    *,
    model_client: "ModelClient | None" = None,
    observed_companies: list[dict[str, Any]] | None = None,
) -> CompanyIdentity:
    requested = target_company.strip()
    company_key = normalize_company_key(requested)
    alias_key = COMPANY_ALIASES.get(company_key, company_key)
    builtin = BUILTIN_COMPANY_IDENTITIES.get(alias_key)
    if builtin:
        linkedin_slug = str(builtin.get("linkedin_slug", "")).strip()
        return CompanyIdentity(
            requested_name=requested,
            canonical_name=str(builtin.get("canonical_name", requested)).strip(),
            company_key=alias_key,
            linkedin_slug=linkedin_slug,
            linkedin_company_url=_company_url(linkedin_slug),
            domain=str(builtin.get("domain", "")).strip(),
            aliases=list(builtin.get("aliases", [])),
            resolver=str(builtin.get("resolver", "builtin")).strip(),
            confidence=str(builtin.get("confidence", "high")).strip(),
            notes=str(builtin.get("notes", "")).strip(),
            local_asset_available=bool(builtin.get("local_asset_available")),
        )

    company_ids = _load_legacy_company_ids(legacy_company_ids_path)
    legacy_slug = company_ids.get(company_key, "")
    if legacy_slug:
        heuristic_slug = legacy_slug
        confidence = "medium"
        notes = "Resolved from legacy company_ids mapping."
    else:
        model_identity = _resolve_company_identity_from_observed_candidates(
            requested,
            company_key,
            model_client=model_client,
            observed_companies=observed_companies,
        )
        if model_identity is not None:
            return model_identity
        heuristic_slug = company_key
        confidence = "low"
        notes = "Resolved heuristically from the company name."
    return CompanyIdentity(
        requested_name=requested,
        canonical_name=requested,
        company_key=company_key,
        linkedin_slug=heuristic_slug,
        linkedin_company_url=_company_url(heuristic_slug),
        aliases=[],
        resolver="legacy_mapping" if legacy_slug else "heuristic",
        confidence=confidence,
        notes=notes,
        local_asset_available=False,
    )


def _resolve_company_identity_from_observed_candidates(
    requested_name: str,
    company_key: str,
    *,
    model_client: "ModelClient | None",
    observed_companies: list[dict[str, Any]] | None,
) -> CompanyIdentity | None:
    observed = _normalize_observed_company_candidates(observed_companies or [])
    if not observed:
        return None
    exact_match = _find_exact_observed_company_match(requested_name, company_key, observed)
    if exact_match is not None:
        return exact_match
    plausible_observed = _select_plausible_observed_company_candidates(requested_name, company_key, observed)
    if model_client is None or not plausible_observed:
        return None
    decision = model_client.judge_company_equivalence(
        {
            "target_company": requested_name,
            "observed_companies": plausible_observed,
            "instruction": (
                "Choose same_company only when the candidate clearly refers to the requested company. "
                "Prefer LinkedIn company labels and be conservative with short or ambiguous names."
            ),
        }
    )
    if str(decision.get("decision") or "").strip() != "same_company":
        return None
    confidence = str(decision.get("confidence_label") or "low").strip().lower() or "low"
    if confidence not in {"high", "medium"}:
        return None
    matched_label = str(decision.get("matched_label") or "").strip()
    selected = next(
        (
            item
            for item in plausible_observed
            if matched_label and str(item.get("label") or "").strip() == matched_label
        ),
        None,
    )
    if selected is None and len(plausible_observed) == 1:
        selected = plausible_observed[0]
    if selected is None:
        return None
    linkedin_slug = str(selected.get("linkedin_slug") or "").strip()
    if not linkedin_slug:
        return None
    aliases = _dedupe_company_aliases(
        [
            requested_name,
            str(selected.get("label") or "").strip(),
            str(selected.get("linkedin_slug") or "").strip(),
            str(selected.get("domain_hint") or "").strip(),
        ]
    )
    rationale = str(decision.get("rationale") or "").strip()
    return CompanyIdentity(
        requested_name=requested_name,
        canonical_name=str(selected.get("label") or requested_name).strip() or requested_name,
        company_key=_canonical_company_key_for_identity(
            requested_name=requested_name,
            label=str(selected.get("label") or requested_name).strip() or requested_name,
            linkedin_slug=linkedin_slug,
            fallback_company_key=company_key,
        ),
        linkedin_slug=linkedin_slug,
        linkedin_company_url=str(selected.get("linkedin_company_url") or _company_url(linkedin_slug)).strip(),
        domain=str(selected.get("domain_hint") or "").strip(),
        aliases=[alias for alias in aliases if normalize_company_key(alias) != normalize_company_key(requested_name)],
        resolver="observed_candidates_model_assisted",
        confidence=confidence,
        notes=rationale or "Resolved from observed LinkedIn company candidates with model-assisted adjudication.",
        local_asset_available=False,
        metadata={"matched_label": str(selected.get("label") or "").strip()},
    )


def _normalize_observed_company_candidates(observed_companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in observed_companies:
        if not isinstance(item, dict):
            continue
        company_url = str(item.get("linkedin_company_url") or "").strip()
        linkedin_slug = _extract_company_slug(company_url) or str(item.get("linkedin_slug") or "").strip()
        label = " ".join(str(item.get("label") or "").split()).strip()
        if not linkedin_slug or not label:
            continue
        key = (label.lower(), linkedin_slug.lower())
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "label": label,
                "linkedin_slug": linkedin_slug,
                "linkedin_company_url": company_url or _company_url(linkedin_slug),
                "domain_hint": str(item.get("domain_hint") or "").strip(),
            }
        )
    return normalized


def _find_exact_observed_company_match(
    requested_name: str,
    company_key: str,
    observed_companies: list[dict[str, Any]],
) -> CompanyIdentity | None:
    target_tokens = {
        normalize_company_key(requested_name),
        company_key,
    }
    target_tokens.discard("")
    for item in observed_companies:
        label = str(item.get("label") or "").strip()
        linkedin_slug = str(item.get("linkedin_slug") or "").strip()
        candidate_tokens = {
            normalize_company_key(label),
            normalize_company_key(linkedin_slug),
        }
        candidate_tokens.discard("")
        if not target_tokens or not candidate_tokens or not (target_tokens & candidate_tokens):
            continue
        return CompanyIdentity(
            requested_name=requested_name,
            canonical_name=label or requested_name,
            company_key=_canonical_company_key_for_identity(
                requested_name=requested_name,
                label=label or requested_name,
                linkedin_slug=linkedin_slug,
                fallback_company_key=company_key,
            ),
            linkedin_slug=linkedin_slug,
            linkedin_company_url=str(item.get("linkedin_company_url") or _company_url(linkedin_slug)).strip(),
            domain=str(item.get("domain_hint") or "").strip(),
            aliases=[alias for alias in _dedupe_company_aliases([requested_name, label, linkedin_slug]) if normalize_company_key(alias) != normalize_company_key(requested_name)],
            resolver="observed_candidates_exact_match",
            confidence="medium",
            notes="Resolved from observed LinkedIn company candidates by exact normalized match.",
            local_asset_available=False,
        )
    return None


def _select_plausible_observed_company_candidates(
    requested_name: str,
    company_key: str,
    observed_companies: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    plausible: list[dict[str, Any]] = []
    requested_key = normalize_company_key(requested_name)
    for item in observed_companies:
        label = str(item.get("label") or "").strip()
        linkedin_slug = str(item.get("linkedin_slug") or "").strip()
        if _looks_plausibly_related_company_label(
            requested_name=requested_name,
            requested_key=requested_key or company_key,
            label=label,
            linkedin_slug=linkedin_slug,
        ):
            plausible.append(item)
    return plausible[:5]


def _looks_plausibly_related_company_label(
    *,
    requested_name: str,
    requested_key: str,
    label: str,
    linkedin_slug: str,
) -> bool:
    requested_norm = normalize_company_key(requested_name)
    label_norm = normalize_company_key(label)
    slug_norm = normalize_company_key(linkedin_slug)
    if not requested_norm or not (label_norm or slug_norm):
        return False
    if requested_norm in {label_norm, slug_norm} or requested_key in {label_norm, slug_norm}:
        return True

    requested_tokens = _company_resolution_tokens(requested_name)
    candidate_tokens = _company_resolution_tokens(f"{label} {linkedin_slug}")
    if requested_tokens and candidate_tokens:
        overlap = requested_tokens & candidate_tokens
        if len(overlap) >= 2:
            return True
        if overlap and len(requested_tokens) == 1:
            return True

    for candidate_norm in [label_norm, slug_norm]:
        if not candidate_norm:
            continue
        prefix_length = _common_company_prefix_length(requested_norm, candidate_norm)
        if prefix_length >= 6 and prefix_length / max(1, min(len(requested_norm), len(candidate_norm))) >= 0.6:
            return True
    return False


def _company_resolution_tokens(value: str) -> set[str]:
    normalized = (
        str(value or "")
        .lower()
        .replace("&", " and ")
        .replace("+", " ")
    )
    stopwords = {"and", "the", "inc", "llc", "corp", "co", "company", "limited", "ltd", "lab"}
    return {
        token
        for token in re.split(r"[^a-z0-9]+", normalized)
        if token and token not in stopwords and len(token) >= 2
    }


def _common_company_prefix_length(left: str, right: str) -> int:
    count = 0
    for left_char, right_char in zip(left, right):
        if left_char != right_char:
            break
        count += 1
    return count


def _extract_company_slug(url: str) -> str:
    match = re.search(r"linkedin\.com/company/([^/?#]+)", str(url or "").strip(), flags=re.IGNORECASE)
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def _dedupe_company_aliases(values: list[str]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = " ".join(str(value or "").split()).strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(normalized)
    return results


def _canonical_company_key_for_identity(
    *,
    requested_name: str,
    label: str,
    linkedin_slug: str,
    fallback_company_key: str,
) -> str:
    ordered_candidates = [
        normalize_company_key(linkedin_slug),
        normalize_company_key(label),
        normalize_company_key(requested_name),
        normalize_company_key(fallback_company_key),
    ]
    for candidate in ordered_candidates:
        if not candidate:
            continue
        return COMPANY_ALIASES.get(candidate, candidate)
    return normalize_company_key(fallback_company_key)


def load_rapidapi_accounts(secret_file: Path | None = None, legacy_file: Path | None = None) -> list[RapidApiAccount]:
    parsed: dict[str, RapidApiAccount] = {}
    for source, path in (("legacy_asset", legacy_file), ("runtime_secret", secret_file)):
        if path is None or not path.exists():
            continue
        for account in _parse_accounts_from_file(path, source):
            parsed[account.account_id] = account
    return list(parsed.values())


def company_roster_accounts(accounts: list[RapidApiAccount]) -> list[RapidApiAccount]:
    candidates = [account for account in accounts if account.supports_company_roster and _account_is_usable(account)]
    return sorted(candidates, key=_company_roster_sort_key)


def profile_detail_accounts(accounts: list[RapidApiAccount]) -> list[RapidApiAccount]:
    candidates = [account for account in accounts if account.supports_profile_detail and _account_is_usable(account)]
    return sorted(candidates, key=_profile_detail_sort_key)


def search_people_accounts(accounts: list[RapidApiAccount]) -> list[RapidApiAccount]:
    candidates = [account for account in accounts if account.supports_people_search and _account_is_usable(account)]
    return sorted(candidates, key=_search_people_sort_key)


class LinkedInCompanyRosterConnector:
    def __init__(
        self,
        accounts: list[RapidApiAccount],
        request_timeout_seconds: int = 30,
        per_page_delay_seconds: float = 0.8,
    ) -> None:
        self.accounts = company_roster_accounts(accounts)
        self.request_timeout_seconds = request_timeout_seconds
        self.per_page_delay_seconds = per_page_delay_seconds
        self._exhausted_account_ids: set[str] = set()

    def fetch_company_roster(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
        max_pages: int = 10,
        page_limit: int = 50,
    ) -> CompanyRosterSnapshot:
        if not identity.linkedin_slug:
            raise RuntimeError(f"No LinkedIn company slug is available for {identity.requested_name}.")
        if not self.accounts:
            raise RuntimeError("No usable RapidAPI company roster accounts are available.")

        snapshot_id = snapshot_dir.name
        raw_dir = snapshot_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)

        all_entries: list[dict[str, Any]] = []
        page_summaries: list[dict[str, Any]] = []
        accounts_used: list[str] = []
        errors: list[str] = []
        seen_signatures: set[str] = set()
        stop_reason = "max_pages_reached"

        for page in range(1, max_pages + 1):
            page_payload, source_account, page_errors = self._fetch_page(identity.linkedin_slug, page, page_limit)
            errors.extend(page_errors)
            if page_payload is None or source_account is None:
                stop_reason = f"connector_failure_page_{page}"
                break

            if source_account.account_id not in accounts_used:
                accounts_used.append(source_account.account_id)

            page_path = raw_dir / f"linkedin_company_people_page{page:03d}.json"
            logger.write_json(
                page_path,
                page_payload,
                asset_type="linkedin_company_people_page",
                source_kind="company_roster_connector",
                is_raw_asset=True,
                model_safe=False,
                metadata={"page": page, "company": identity.canonical_name},
            )

            container = page_payload.get("data") or {}
            rows = list(container.get("data") or [])
            pagination = container.get("pagination") or {}
            signature = _page_signature(rows)

            page_summaries.append(
                {
                    "page": page,
                    "row_count": len(rows),
                    "signature": signature,
                    "has_more": pagination.get("hasMore"),
                    "account_id": source_account.account_id,
                    "raw_path": str(page_path),
                }
            )

            if not rows:
                stop_reason = f"empty_page_{page}"
                break
            if signature in seen_signatures:
                stop_reason = f"repeated_page_signature_{page}"
                break

            seen_signatures.add(signature)
            for row in rows:
                all_entries.append(_normalize_roster_entry(row, page, source_account.account_id))

            if page < max_pages:
                time.sleep(self.per_page_delay_seconds)

        deduped_entries = _dedupe_roster_entries(all_entries)
        visible_entries = [entry for entry in deduped_entries if not entry["is_headless"]]
        headless_entries = [entry for entry in deduped_entries if entry["is_headless"]]

        merged_path = snapshot_dir / "linkedin_company_people_all.json"
        visible_path = snapshot_dir / "linkedin_company_people_visible.json"
        headless_path = snapshot_dir / "linkedin_company_people_headless.json"
        summary_path = snapshot_dir / "linkedin_company_people_summary.json"

        logger.write_json(
            merged_path,
            deduped_entries,
            asset_type="linkedin_company_people_merged",
            source_kind="company_roster_connector",
            is_raw_asset=False,
            model_safe=True,
            metadata={"company": identity.canonical_name},
        )
        logger.write_json(
            visible_path,
            visible_entries,
            asset_type="linkedin_company_people_visible",
            source_kind="company_roster_connector",
            is_raw_asset=False,
            model_safe=True,
            metadata={"company": identity.canonical_name},
        )
        logger.write_json(
            headless_path,
            headless_entries,
            asset_type="linkedin_company_people_headless",
            source_kind="company_roster_connector",
            is_raw_asset=False,
            model_safe=True,
            metadata={"company": identity.canonical_name},
        )
        logger.write_json(
            summary_path,
            {
                "snapshot_id": snapshot_id,
                "target_company": identity.canonical_name,
                "company_identity": identity.to_record(),
                "raw_entry_count": len(all_entries),
                "deduped_entry_count": len(deduped_entries),
                "visible_entry_count": len(visible_entries),
                "headless_entry_count": len(headless_entries),
                "page_count": len(page_summaries),
                "accounts_used": accounts_used,
                "errors": errors,
                "stop_reason": stop_reason,
                "page_summaries": page_summaries,
            },
            asset_type="linkedin_company_people_summary",
            source_kind="company_roster_connector",
            is_raw_asset=False,
            model_safe=True,
        )

        return CompanyRosterSnapshot(
            snapshot_id=snapshot_id,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=deduped_entries,
            visible_entries=visible_entries,
            headless_entries=headless_entries,
            page_summaries=page_summaries,
            accounts_used=accounts_used,
            errors=errors,
            stop_reason=stop_reason,
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )

    def _fetch_page(
        self,
        linkedin_slug: str,
        page: int,
        page_limit: int,
    ) -> tuple[dict[str, Any] | None, RapidApiAccount | None, list[str]]:
        errors_seen: list[str] = []
        for account in self.accounts:
            if account.account_id in self._exhausted_account_ids:
                continue

            base_url = account.base_url.rstrip("/")
            url = f"{base_url}/api/company/people?{parse.urlencode({'username': linkedin_slug, 'page': page, 'limit': page_limit})}"
            request_headers = {
                "x-rapidapi-host": account.host,
                "x-rapidapi-key": account.api_key,
                "User-Agent": "Mozilla/5.0",
            }
            http_request = request.Request(url, headers=request_headers, method="GET")

            try:
                with request.urlopen(http_request, timeout=self.request_timeout_seconds) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                return payload, account, errors_seen
            except error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="ignore")
                message = f"{account.account_id}: HTTP {exc.code}"
                if detail:
                    message += f" {detail[:160]}"
                errors_seen.append(message)
                if exc.code == 429:
                    self._exhausted_account_ids.add(account.account_id)
                continue
            except error.URLError as exc:
                errors_seen.append(f"{account.account_id}: URLError {exc.reason}")
                continue
            except TimeoutError:
                errors_seen.append(f"{account.account_id}: timeout")
                continue
            except Exception as exc:  # pragma: no cover - defensive
                errors_seen.append(f"{account.account_id}: {type(exc).__name__} {str(exc)[:160]}")
                continue
        return None, None, errors_seen


def build_candidates_from_roster(snapshot: CompanyRosterSnapshot) -> tuple[list[Candidate], list[EvidenceRecord]]:
    dataset_name = f"{snapshot.company_identity.company_key}_linkedin_company_people"
    source_path = str(snapshot.visible_path)
    company_name = snapshot.company_identity.canonical_name
    candidates: list[Candidate] = []
    evidence_items: list[EvidenceRecord] = []

    for row in snapshot.visible_entries:
        full_name = str(row.get("full_name", "")).strip()
        if not full_name:
            continue

        member_key = str(row.get("member_key", "")).strip()
        candidate_id = sha1(
            "|".join([normalize_name_token(company_name), normalize_name_token(full_name), member_key]).encode("utf-8")
        ).hexdigest()[:16]
        headline = str(row.get("headline", "")).strip()
        location = str(row.get("location", "")).strip()
        linkedin_url = str(row.get("linkedin_url", "")).strip()
        team = _infer_team_from_headline(headline)
        notes = "LinkedIn company roster baseline."
        if location:
            notes += f" Location: {location}."
        if row.get("source_account_id"):
            notes += f" Source account: {row['source_account_id']}."

        candidate = normalize_candidate(
            Candidate(
            candidate_id=candidate_id,
            name_en=full_name,
            display_name=format_display_name(full_name, ""),
            category="employee",
            target_company=company_name,
            organization=company_name,
            employment_status="current",
            role=headline,
            team=team,
            focus_areas=headline,
            work_history="",
            notes=notes,
            linkedin_url=linkedin_url,
            source_dataset=dataset_name,
            source_path=source_path,
            metadata={
                "member_id": row.get("member_id", ""),
                "urn": row.get("urn", ""),
                "profile_url": linkedin_url,
                "location": location,
                "page": row.get("page"),
                "source_account_id": row.get("source_account_id", ""),
                "snapshot_id": snapshot.snapshot_id,
            },
            )
        )
        candidates.append(candidate)

        evidence_url = snapshot.company_identity.linkedin_company_url
        evidence_title = headline or f"{company_name} LinkedIn roster entry"
        evidence_summary = f"{full_name} appears in the LinkedIn company roster for {company_name}."
        if location:
            evidence_summary += f" Location: {location}."
        evidence_items.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    candidate_id,
                    dataset_name,
                    evidence_title,
                    evidence_url or f"linkedin-company:{snapshot.company_identity.linkedin_slug}",
                ),
                candidate_id=candidate_id,
                source_type="linkedin_company_people",
                title=evidence_title,
                url=evidence_url,
                summary=evidence_summary,
                source_dataset=dataset_name,
                source_path=source_path,
                metadata={
                    "member_id": row.get("member_id", ""),
                    "urn": row.get("urn", ""),
                    "profile_url": linkedin_url,
                    "page": row.get("page"),
                    "source_account_id": row.get("source_account_id", ""),
                    "snapshot_id": snapshot.snapshot_id,
                },
            )
        )
    return candidates, evidence_items


def _parse_accounts_from_file(path: Path, source: str) -> list[RapidApiAccount]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return []

    if source == "runtime_secret":
        connector_payload = payload.get("connectors") or payload
    else:
        connector_payload = payload

    parsed: list[RapidApiAccount] = []
    seen_ids: set[str] = set()
    for entry in _iter_account_entries(connector_payload):
        account = _build_account(entry, source)
        if account is None or account.account_id in seen_ids:
            continue
        seen_ids.add(account.account_id)
        parsed.append(account)
    return parsed


def _iter_account_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if isinstance(payload.get("rapidapi_accounts"), list):
        entries.extend(item for item in payload["rapidapi_accounts"] if isinstance(item, dict))
    if isinstance(payload.get("accounts"), list):
        entries.extend(item for item in payload["accounts"] if isinstance(item, dict))
    for key, value in payload.items():
        if key.startswith("account_") and isinstance(value, dict):
            entry = dict(value)
            entry.setdefault("id", key)
            entries.append(entry)
    return entries


def _build_account(entry: dict[str, Any], source: str) -> RapidApiAccount | None:
    api_key = str(entry.get("api_key", "")).strip()
    account_id = str(entry.get("id") or entry.get("account_id") or "").strip()
    host = str(entry.get("host") or entry.get("rapidapi_host") or "").strip()
    base_url = str(entry.get("base_url") or "").strip()
    if not api_key or not account_id or not host:
        return None
    if not base_url:
        base_url = f"https://{host}"
    endpoint_company = _extract_endpoint(entry, "company")
    endpoint_search = _extract_endpoint(entry, "search")
    endpoint_profile = _extract_endpoint(entry, "profile")
    return RapidApiAccount(
        account_id=account_id,
        source=source,
        provider=str(entry.get("provider") or entry.get("provider_name") or "").strip(),
        host=host,
        base_url=base_url,
        api_key=api_key,
        status=str(entry.get("status", "")).strip(),
        monthly_limit=_parse_int(entry.get("monthly_limit")),
        total_used=_parse_int(entry.get("total_used")),
        endpoint_company=endpoint_company,
        endpoint_search=endpoint_search,
        endpoint_profile=endpoint_profile,
        method=str(entry.get("method", "GET")).strip() or "GET",
        note=str(entry.get("note") or entry.get("notes") or "").strip(),
    )


def _extract_endpoint(entry: dict[str, Any], kind: str) -> str:
    if kind == "company":
        direct = str(entry.get("endpoint_company") or "").strip()
        endpoint = str(entry.get("endpoint") or "").strip()
        if direct:
            return direct
        if "/company" in endpoint:
            return endpoint
        endpoints = entry.get("endpoints") or {}
        if isinstance(endpoints, dict):
            item = endpoints.get("company_people") or endpoints.get("company")
            if isinstance(item, str):
                return item
            if isinstance(item, dict):
                return str(item.get("path") or item.get("endpoint") or "").strip()
    elif kind == "search":
        direct = str(entry.get("endpoint_search") or "").strip()
        endpoint = str(entry.get("endpoint") or "").strip()
        if direct:
            return direct
        if "search/people" in endpoint:
            return endpoint
        endpoints = entry.get("endpoints") or {}
        if isinstance(endpoints, dict):
            item = endpoints.get("search_people") or endpoints.get("people_search") or endpoints.get("search")
            if isinstance(item, str):
                return item
            if isinstance(item, dict):
                return str(item.get("path") or item.get("endpoint") or "").strip()
    else:
        direct = str(entry.get("endpoint_profile") or "").strip()
        endpoint = str(entry.get("endpoint") or "").strip()
        if direct:
            return direct
        if "/profile" in endpoint:
            return endpoint
        endpoints = entry.get("endpoints") or {}
        if isinstance(endpoints, dict):
            item = endpoints.get("profile_detail") or endpoints.get("profile")
            if isinstance(item, str):
                return item
            if isinstance(item, dict):
                return str(item.get("path") or item.get("endpoint") or "").strip()
    return ""


def _load_legacy_company_ids(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}

    resolved: dict[str, str] = {}
    for key, value in payload.items():
        normalized = normalize_company_key(str(key))
        if not normalized:
            continue
        if isinstance(value, str):
            resolved[normalized] = value.strip()
        elif isinstance(value, dict):
            slug = str(value.get("linkedin_slug") or value.get("slug") or "").strip()
            if slug:
                resolved[normalized] = slug
    return resolved


def _company_url(linkedin_slug: str) -> str:
    if not linkedin_slug:
        return ""
    return f"https://www.linkedin.com/company/{linkedin_slug}/"


def _account_is_usable(account: RapidApiAccount) -> bool:
    status = account.status.lower()
    blocked_tokens = ["stopped", "停服", "停用"]
    if any(token in status for token in blocked_tokens):
        return False
    if "exhausted" in status or "耗尽" in status:
        return False
    return True


def _company_roster_sort_key(account: RapidApiAccount) -> tuple[int, int, str]:
    host_bonus = 10 if "z-real-time-linkedin-scraper-api1" in account.host else 0
    return (-(account.estimated_remaining + host_bonus), account.total_used, account.account_id)


def _profile_detail_sort_key(account: RapidApiAccount) -> tuple[int, int, str]:
    host_bonus = 10 if "real-time-linkedin-data-scraper-api" in account.host else 0
    return (-(account.estimated_remaining + host_bonus), account.total_used, account.account_id)


def _search_people_sort_key(account: RapidApiAccount) -> tuple[int, int, str]:
    host_bonus = 10 if "z-real-time-linkedin-scraper-api1" in account.host else 0
    return (-(account.estimated_remaining + host_bonus), account.total_used, account.account_id)


def _parse_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if value is None:
        return 0
    matches = re.findall(r"\d+", str(value))
    if not matches:
        return 0
    return max(int(item) for item in matches)


def _normalize_roster_entry(row: dict[str, Any], page: int, source_account_id: str) -> dict[str, Any]:
    member_id = str(row.get("id") or "").strip()
    full_name = str(row.get("fullName") or "").strip()
    urn = str(row.get("urn") or "").strip()
    headline = str(row.get("headline") or "").strip()
    location = str(row.get("location") or "").strip()
    is_headless = full_name == "LinkedIn Member" or member_id == "headless"
    member_key = member_id or urn or sha1("|".join([full_name, headline, location]).encode("utf-8")).hexdigest()[:16]
    avatar = row.get("avatar") or []
    avatar_url = ""
    if isinstance(avatar, list) and avatar:
        avatar_url = str((avatar[0] or {}).get("url") or "").strip()
    return {
        "member_key": member_key,
        "member_id": member_id,
        "urn": urn,
        "full_name": full_name,
        "headline": headline,
        "location": location,
        "avatar_url": avatar_url,
        "is_headless": is_headless,
        "page": page,
        "source_account_id": source_account_id,
    }


def _dedupe_roster_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for entry in entries:
        member_key = str(entry.get("member_key", "")).strip()
        if not member_key or member_key in seen_keys:
            continue
        seen_keys.add(member_key)
        deduped.append(entry)
    return deduped


def _page_signature(rows: list[dict[str, Any]]) -> str:
    payload = "\n".join(f"{row.get('id', '')}|{row.get('fullName', '')}|{row.get('urn', '')}" for row in rows)
    return sha1(payload.encode("utf-8")).hexdigest()[:12]


def _infer_team_from_headline(headline: str) -> str:
    text = headline.lower()
    if "infra" in text or "infrastructure" in text:
        return "Infrastructure"
    if "research" in text:
        return "Research"
    if "training" in text or "pretrain" in text:
        return "Training"
    if "inference" in text or "serving" in text:
        return "Inference"
    if "recruit" in text or "talent" in text:
        return "Recruiting"
    return ""
