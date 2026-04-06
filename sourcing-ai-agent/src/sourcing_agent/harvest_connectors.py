from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
from hashlib import sha1
import json
from pathlib import Path
from shutil import copyfile
from typing import Any
from urllib import error, parse, request

from .asset_logger import AssetLogger
from .connectors import CompanyIdentity, CompanyRosterSnapshot
from .settings import HarvestActorSettings


@dataclass(slots=True)
class HarvestProfileConnector:
    settings: HarvestActorSettings

    def fetch_profile_by_url(self, profile_url: str, snapshot_dir: Path, asset_logger: AssetLogger | None = None) -> dict[str, Any] | None:
        results = self.fetch_profiles_by_urls([profile_url], snapshot_dir, asset_logger=asset_logger)
        return results.get(str(profile_url or "").strip())

    def fetch_profiles_by_urls(
        self,
        profile_urls: list[str],
        snapshot_dir: Path,
        asset_logger: AssetLogger | None = None,
    ) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        normalized_urls: list[str] = []
        for item in profile_urls:
            profile_url = str(item or "").strip()
            if profile_url and profile_url not in normalized_urls:
                normalized_urls.append(profile_url)
        if not self.settings.enabled or not normalized_urls:
            return results
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        pending_urls: list[str] = []
        for profile_url in normalized_urls:
            raw_path = harvest_dir / f"{_profile_cache_key(profile_url)}.json"
            if raw_path.exists():
                try:
                    payload = json.loads(raw_path.read_text())
                    logger.record_existing(
                        raw_path,
                        asset_type="harvest_profile_payload",
                        source_kind="harvest_profile_scraper",
                        content_type="application/json",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={"profile_url": profile_url, "cached": True},
                    )
                    results[profile_url] = {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": parse_harvest_profile_payload(payload),
                    }
                    continue
                except json.JSONDecodeError:
                    pass
            pending_urls.append(profile_url)

        if not pending_urls:
            return results

        payload = {
            "urls": pending_urls,
            "profileScraperMode": _profile_scraper_mode(self.settings),
        }
        body = _run_harvest_actor(self.settings, payload)
        if body is None:
            return results

        items = body if isinstance(body, list) else [body]
        for requested_url, item in zip(pending_urls, items):
            if not isinstance(item, dict):
                continue
            raw_path = harvest_dir / f"{_profile_cache_key(requested_url)}.json"
            logger.write_json(
                raw_path,
                item,
                asset_type="harvest_profile_payload",
                source_kind="harvest_profile_scraper",
                is_raw_asset=True,
                model_safe=False,
                metadata={"profile_url": requested_url, "cached": False},
            )
            results[requested_url] = {
                "raw_path": raw_path,
                "account_id": "harvest_profile_scraper",
                "parsed": parse_harvest_profile_payload(item),
            }
        return results


@dataclass(slots=True)
class HarvestProfileSearchConnector:
    settings: HarvestActorSettings

    def search_profiles(
        self,
        *,
        query_text: str,
        filter_hints: dict[str, list[str]],
        employment_status: str,
        discovery_dir: Path,
        asset_logger: AssetLogger | None = None,
        limit: int = 25,
        pages: int = 1,
    ) -> dict[str, Any] | None:
        query_text = str(query_text or "").strip()
        search_dir = discovery_dir / "harvest_profile_search"
        search_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(discovery_dir.parent)
        take_pages = max(1, min(int(pages or 1), 10))
        max_items = max(limit, take_pages * 25)
        payload = {
            "profileScraperMode": _profile_search_mode(self.settings),
            "maxItems": max(1, min(max_items, self.settings.max_paid_items)),
            "startPage": 1,
            "takePages": take_pages,
        }
        if query_text:
            payload["searchQuery"] = query_text
        _apply_harvest_search_filters(payload, filter_hints, employment_status)
        raw_path = search_dir / f"{_payload_cache_key(payload)}.json"
        if raw_path.exists():
            try:
                cached = json.loads(raw_path.read_text())
                logger.record_existing(
                    raw_path,
                    asset_type="harvest_profile_search_payload",
                    source_kind="harvest_profile_search",
                    content_type="application/json",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"query": query_text, "cached": True},
                )
                return {
                    "raw_path": raw_path,
                    "account_id": "harvest_profile_search",
                    "rows": parse_harvest_search_rows(cached),
                    "payload": cached,
                }
            except json.JSONDecodeError:
                pass
        cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
            discovery_dir,
            logical_name="harvest_profile_search",
            payload=payload,
        )
        if cached_body is not None:
            logger.write_json(
                raw_path,
                cached_body,
                asset_type="harvest_profile_search_payload",
                source_kind="harvest_profile_search",
                is_raw_asset=True,
                model_safe=False,
                metadata={
                    "query": query_text,
                    "cached": True,
                    "cache_source": cache_source,
                    "cache_origin": str(cache_origin or ""),
                },
            )
            return {
                "raw_path": raw_path,
                "account_id": "harvest_profile_search",
                "rows": parse_harvest_search_rows(cached_body),
                "payload": cached_body,
            }
        if not self.settings.enabled:
            return None
        effective_settings = self.settings
        if take_pages > 1:
            effective_settings = replace(
                self.settings,
                max_total_charge_usd=max(
                    float(self.settings.max_total_charge_usd or 0.0),
                    round(0.10 * take_pages, 2),
                ),
            )
        body = _run_harvest_actor(effective_settings, payload)
        if body is None:
            return None
        logger.write_json(
            raw_path,
            body,
            asset_type="harvest_profile_search_payload",
            source_kind="harvest_profile_search",
            is_raw_asset=True,
            model_safe=False,
            metadata={"query": query_text, "cached": False},
        )
        _persist_shared_harvest_payload(discovery_dir, logical_name="harvest_profile_search", payload=payload, body=body)
        return {
            "raw_path": raw_path,
            "account_id": "harvest_profile_search",
            "rows": parse_harvest_search_rows(body),
            "payload": body,
        }


@dataclass(slots=True)
class HarvestCompanyEmployeesConnector:
    settings: HarvestActorSettings

    def fetch_company_roster(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
        max_pages: int = 5,
        page_limit: int = 25,
    ) -> CompanyRosterSnapshot:
        company_url = str(identity.linkedin_company_url or "").strip()
        if not company_url:
            raise RuntimeError(f"No LinkedIn company URL is available for {identity.requested_name}.")
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        payload = {
            "profileScraperMode": _company_employees_mode(self.settings),
            "companies": [company_url],
            "takePages": max(1, min(max_pages, 100)),
            "maxItems": max(1, min(max_pages * max(1, page_limit), self.settings.max_paid_items)),
        }
        raw_path = harvest_dir / "harvest_company_employees_raw.json"
        body = None
        if raw_path.exists():
            try:
                body = json.loads(raw_path.read_text())
                logger.record_existing(
                    raw_path,
                    asset_type="harvest_company_employees_payload",
                    source_kind="harvest_company_employees",
                    content_type="application/json",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"company": identity.canonical_name, "cached": True},
                )
            except json.JSONDecodeError:
                body = None
        if body is None:
            cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                snapshot_dir,
                logical_name="harvest_company_employees",
                payload=payload,
            )
            if cached_body is not None:
                body = cached_body
                logger.write_json(
                    raw_path,
                    body,
                    asset_type="harvest_company_employees_payload",
                    source_kind="harvest_company_employees",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={
                        "company": identity.canonical_name,
                        "cached": True,
                        "cache_source": cache_source,
                        "cache_origin": str(cache_origin or ""),
                    },
                )
        if body is None:
            if not self.settings.enabled:
                raise RuntimeError("Harvest company-employees connector is not enabled.")
            body = _run_harvest_actor(self.settings, payload)
            if body is None:
                raise RuntimeError("Harvest company-employees run failed.")
            logger.write_json(
                raw_path,
                body,
                asset_type="harvest_company_employees_payload",
                source_kind="harvest_company_employees",
                is_raw_asset=True,
                model_safe=False,
                metadata={"company": identity.canonical_name, "cached": False},
            )
            _persist_shared_harvest_payload(snapshot_dir, logical_name="harvest_company_employees", payload=payload, body=body)
        rows = parse_harvest_company_employee_rows(body)
        deduped_entries = _dedupe_harvest_roster_entries(rows)
        visible_entries = [entry for entry in deduped_entries if not entry["is_headless"]]
        headless_entries = [entry for entry in deduped_entries if entry["is_headless"]]
        merged_path = harvest_dir / "harvest_company_employees_merged.json"
        visible_path = harvest_dir / "harvest_company_employees_visible.json"
        headless_path = harvest_dir / "harvest_company_employees_headless.json"
        summary_path = harvest_dir / "harvest_company_employees_summary.json"
        page_summaries = [
            {
                "page": 1,
                "entry_count": len(rows),
                "visible_entry_count": len(visible_entries),
                "headless_entry_count": len(headless_entries),
                "account_id": "harvest_company_employees",
            }
        ]
        logger.write_json(
            merged_path,
            deduped_entries,
            asset_type="company_roster_merged",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            visible_path,
            visible_entries,
            asset_type="company_roster_visible",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            headless_path,
            headless_entries,
            asset_type="company_roster_headless",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            summary_path,
            {
                "company_identity": identity.to_record(),
                "raw_entry_count": len(rows),
                "visible_entry_count": len(visible_entries),
                "headless_entry_count": len(headless_entries),
                "page_summaries": page_summaries,
                "accounts_used": ["harvest_company_employees"],
                "errors": [],
                "stop_reason": "completed",
            },
            asset_type="company_roster_summary",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        return CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=deduped_entries,
            visible_entries=visible_entries,
            headless_entries=headless_entries,
            page_summaries=page_summaries,
            accounts_used=["harvest_company_employees"],
            errors=[],
            stop_reason="completed",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )


def parse_harvest_profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("item") if isinstance(payload.get("item"), dict) else payload
    full_name = str(data.get("fullName") or data.get("full_name") or data.get("name") or "").strip()
    headline = str(data.get("headline") or data.get("occupation") or "").strip()
    profile_url = str(data.get("linkedinUrl") or data.get("profileUrl") or data.get("url") or "").strip()
    public_identifier = str(data.get("publicIdentifier") or data.get("public_identifier") or "").strip()
    summary = str(data.get("about") or data.get("summary") or data.get("description") or "").strip()
    location = str(data.get("location") or data.get("locationName") or "").strip()
    experience = _coerce_list(data.get("experience") or data.get("experiences") or data.get("positions"))
    education = _coerce_list(data.get("education") or data.get("educations") or data.get("schools"))
    publications = _coerce_list(data.get("publications") or data.get("posts") or [])
    current_company = _extract_current_company(data, experience)
    more_profiles = _coerce_list(data.get("moreProfiles") or data.get("more_profiles") or [])
    return {
        "full_name": full_name,
        "first_name": str(data.get("firstName") or data.get("first_name") or "").strip(),
        "last_name": str(data.get("lastName") or data.get("last_name") or "").strip(),
        "headline": headline,
        "profile_url": profile_url,
        "public_identifier": public_identifier,
        "summary": summary,
        "location": location,
        "current_company": current_company,
        "experience": experience,
        "education": education,
        "publications": publications,
        "more_profiles": more_profiles,
    }


def parse_harvest_search_rows(payload: Any) -> list[dict[str, Any]]:
    items = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        data = item.get("item") if isinstance(item.get("item"), dict) else item
        profile_url = str(data.get("linkedinUrl") or data.get("profileUrl") or data.get("url") or "").strip()
        rows.append(
            {
                "full_name": _full_name_from_payload(data),
                "headline": _headline_from_payload(data),
                "location": _location_text(data.get("location") or data.get("locationName")),
                "profile_url": profile_url,
                "username": str(data.get("publicIdentifier") or data.get("public_identifier") or "").strip() or _slug_from_linkedin_url(profile_url),
                "current_company": str(data.get("currentCompany") or data.get("current_company") or "").strip(),
            }
        )
    return rows


def parse_harvest_company_employee_rows(payload: Any) -> list[dict[str, Any]]:
    items = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        data = item.get("item") if isinstance(item.get("item"), dict) else item
        full_name = _full_name_from_payload(data)
        headline = _headline_from_payload(data)
        location = _location_text(data.get("location") or data.get("locationName"))
        profile_url = str(data.get("linkedinUrl") or data.get("profileUrl") or data.get("url") or "").strip()
        public_identifier = str(data.get("publicIdentifier") or data.get("public_identifier") or "").strip()
        member_key = public_identifier or _slug_from_linkedin_url(profile_url) or sha1("|".join([full_name, headline, location]).encode("utf-8")).hexdigest()[:16]
        rows.append(
            {
                "member_key": member_key,
                "member_id": public_identifier,
                "urn": str(data.get("entityUrn") or data.get("urn") or "").strip(),
                "full_name": full_name,
                "headline": headline,
                "location": location,
                "avatar_url": str(data.get("photoUrl") or data.get("profilePicture") or "").strip(),
                "is_headless": full_name == "LinkedIn Member" or not full_name,
                "page": 1,
                "source_account_id": "harvest_company_employees",
                "linkedin_url": profile_url,
            }
        )
    return rows


def discover_legacy_harvest_token(legacy_accounts_path: Path | None) -> str:
    if legacy_accounts_path is None or not legacy_accounts_path.exists():
        return ""
    try:
        payload = json.loads(legacy_accounts_path.read_text())
    except json.JSONDecodeError:
        return ""
    for item in payload.get("accounts", []) or []:
        if isinstance(item, dict) and str(item.get("provider") or "").strip().lower() == "apify":
            return str(item.get("api_token") or "").strip()
    return ""


def _runtime_dir_from_path(base_path: Path) -> Path | None:
    current = base_path.resolve()
    for candidate in [current, *current.parents]:
        if candidate.name == "runtime":
            return candidate
    return None


def _shared_harvest_cache_path(base_path: Path, logical_name: str, payload: dict[str, Any]) -> Path | None:
    runtime_dir = _runtime_dir_from_path(base_path)
    if runtime_dir is None:
        return None
    cache_dir = runtime_dir / "provider_cache" / logical_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{_payload_cache_key(payload)}.json"


def _persist_shared_harvest_payload(base_path: Path, *, logical_name: str, payload: dict[str, Any], body: Any) -> None:
    cache_path = _shared_harvest_cache_path(base_path, logical_name, payload)
    if cache_path is None:
        return
    try:
        cache_path.write_text(json.dumps(body, ensure_ascii=False, indent=2))
    except Exception:
        return


def _load_cached_harvest_payload(
    base_path: Path,
    *,
    logical_name: str,
    payload: dict[str, Any],
) -> tuple[Any | None, str | None, Path | None]:
    cache_path = _shared_harvest_cache_path(base_path, logical_name, payload)
    if cache_path and cache_path.exists():
        try:
            return json.loads(cache_path.read_text()), "shared_cache", cache_path
        except json.JSONDecodeError:
            pass

    runtime_dir = _runtime_dir_from_path(base_path)
    if runtime_dir is None:
        return None, None, None
    live_tests_dir = runtime_dir / "live_tests"
    if not live_tests_dir.exists():
        return None, None, None

    requested_payload = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    for summary_path in sorted(live_tests_dir.rglob("*summary.json")):
        try:
            summary_payload = json.loads(summary_path.read_text())
        except json.JSONDecodeError:
            continue
        candidate_payload = summary_payload.get("input_payload")
        if not isinstance(candidate_payload, dict):
            continue
        if json.dumps(candidate_payload, sort_keys=True, ensure_ascii=False) != requested_payload:
            continue
        raw_path = _infer_live_test_raw_path(summary_path, summary_payload)
        if raw_path is None or not raw_path.exists():
            continue
        try:
            body = json.loads(raw_path.read_text())
        except json.JSONDecodeError:
            continue
        if cache_path is not None:
            try:
                copyfile(raw_path, cache_path)
            except Exception:
                pass
        return body, "live_test_bridge", raw_path
    return None, None, None


def _infer_live_test_raw_path(summary_path: Path, summary_payload: dict[str, Any]) -> Path | None:
    raw_path_value = str(summary_payload.get("raw_path") or "").strip()
    if raw_path_value:
        return Path(raw_path_value)
    name = summary_path.name
    if name.endswith("_summary.json"):
        return summary_path.with_name(name.replace("_summary.json", "_raw.json"))
    return None


def _profile_cache_key(profile_url: str) -> str:
    import hashlib

    return hashlib.sha1(profile_url.strip().encode("utf-8")).hexdigest()[:16]


def _profile_scraper_mode(settings: HarvestActorSettings) -> str:
    if settings.collect_email:
        return "Profile details + email search ($10 per 1k)"
    return "Profile details no email ($4 per 1k)" if settings.default_mode.lower() == "full" else settings.default_mode


def _profile_search_mode(settings: HarvestActorSettings) -> str:
    if settings.collect_email:
        return "Full + email search"
    return "Full" if settings.default_mode.lower() == "full" else "Short"


def _company_employees_mode(settings: HarvestActorSettings) -> str:
    if settings.collect_email:
        return "Full + email search ($12 per 1k)"
    return "Full ($8 per 1k)" if settings.default_mode.lower() == "full" else "Short ($4 per 1k)"


def _run_harvest_actor(settings: HarvestActorSettings, payload: dict[str, Any]) -> Any | None:
    endpoint = (
        f"https://api.apify.com/v2/acts/{parse.quote(settings.actor_id, safe='')}/run-sync-get-dataset-items?"
        + parse.urlencode(
            {
                "token": settings.api_token,
                "timeout": settings.timeout_seconds,
                "format": "json",
                "clean": "true",
                "maxItems": settings.max_paid_items,
                "maxTotalChargeUsd": settings.max_total_charge_usd,
            }
        )
    )
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    http_request = request.Request(endpoint, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(http_request, timeout=settings.timeout_seconds + 15) as response:
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError:
        return None
    except Exception:
        return None


def _apply_harvest_search_filters(payload: dict[str, Any], filter_hints: dict[str, list[str]], employment_status: str) -> None:
    mapping = {
        "current_companies": "currentCompanies",
        "past_companies": "pastCompanies",
        "exclude_current_companies": "excludeCurrentCompanies",
        "exclude_past_companies": "excludePastCompanies",
        "job_titles": "currentJobTitles" if employment_status != "former" else "pastJobTitles",
    }
    for source_key, target_key in mapping.items():
        values = [str(item).strip() for item in filter_hints.get(source_key) or [] if str(item).strip()]
        if values:
            payload[target_key] = values
    scope_keywords = [str(item).strip() for item in filter_hints.get("scope_keywords") or [] if str(item).strip()]
    keyword_values = [str(item).strip() for item in filter_hints.get("keywords") or [] if str(item).strip()]
    query_text = str(payload.get("searchQuery") or "").strip()
    if not query_text:
        terms = [*scope_keywords[:2], *keyword_values[:2]]
        if terms:
            payload["searchQuery"] = " ".join(dict.fromkeys(terms))


def _payload_cache_key(payload: dict[str, Any]) -> str:
    return sha1(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]


def _slug_from_linkedin_url(url: str) -> str:
    import re

    match = re.search(r"linkedin\\.com/in/([^/?#]+)", url)
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def _dedupe_harvest_roster_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries:
        member_key = str(entry.get("member_key") or "").strip()
        if not member_key or member_key in seen:
            continue
        seen.add(member_key)
        deduped.append(entry)
    return deduped


def _coerce_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _full_name_from_payload(data: dict[str, Any]) -> str:
    full_name = str(data.get("fullName") or data.get("full_name") or data.get("name") or "").strip()
    if full_name:
        return full_name
    first_name = str(data.get("firstName") or data.get("first_name") or "").strip()
    last_name = str(data.get("lastName") or data.get("last_name") or "").strip()
    return " ".join(part for part in [first_name, last_name] if part).strip()


def _headline_from_payload(data: dict[str, Any]) -> str:
    direct = str(data.get("headline") or data.get("occupation") or data.get("position") or "").strip()
    if direct:
        return direct
    current_positions = data.get("currentPositions") or data.get("currentPosition") or []
    if isinstance(current_positions, list) and current_positions:
        first = current_positions[0] or {}
        if isinstance(first, dict):
            position = str(first.get("position") or first.get("title") or "").strip()
            company_name = str(first.get("companyName") or first.get("company") or "").strip()
            if position and company_name:
                return f"{position} at {company_name}"
            return position or company_name
    return ""


def _location_text(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("linkedinText") or value.get("text") or value.get("city") or "").strip()
    return str(value or "").strip()


def _extract_current_company(data: dict[str, Any], experience: list[dict[str, Any]]) -> str:
    for item in experience:
        if bool(item.get("isCurrent") or item.get("is_current")):
            return str(item.get("company") or item.get("companyName") or item.get("company_name") or "").strip()
    return str(data.get("currentCompany") or data.get("current_company") or "").strip()
