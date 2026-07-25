from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
import re
import shutil
import sys
from urllib.parse import quote, urljoin, urlsplit
from urllib.request import Request, urlopen
import os


def _bootstrap_import_path() -> None:
    frontend_dir = Path(__file__).resolve().parents[1]
    project_root = frontend_dir.parent
    src_dir = project_root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


_bootstrap_import_path()

DEFAULT_BUNDLE_KIND = "company_snapshot"
DEFAULT_COMPANY_KEY = "thinkingmachineslab"
URL_REGEX = re.compile(r"(https?://[^\s<>'\"`]+|www\.[^\s<>'\"`]+)", re.IGNORECASE)
HREF_REGEX = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.IGNORECASE)


class ObjectStorageNotFoundError(RuntimeError):
    pass


class ObjectStorageConfig:
    def __init__(self, payload: dict) -> None:
        self.provider = str(payload.get("provider", "filesystem"))
        self.bucket = str(payload.get("bucket", ""))
        self.prefix = str(payload.get("prefix", "sourcing-ai-agent-dev"))
        self.endpoint_url = str(payload.get("endpoint_url", "")).rstrip("/")
        self.region = str(payload.get("region", "us-east-1"))
        self.access_key_id = str(payload.get("access_key_id", ""))
        self.secret_access_key = str(payload.get("secret_access_key", ""))
        self.timeout_seconds = int(payload.get("timeout_seconds", 60) or 60)
        self.force_path_style = bool(payload.get("force_path_style", True))


def _safe_copy(source: Path, destination: Path) -> bool:
    if not source.exists():
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return True


def _runtime_relative_from_source_path(source_path: str) -> str:
    marker = "/runtime/"
    if marker not in source_path:
        return ""
    return source_path.split(marker, 1)[1].lstrip("/")


def _clean_value(value: object) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null"}:
        return ""
    return text


def _extract_profile_photo_url(payload) -> str:
    if isinstance(payload, dict):
        profile_picture = payload.get("profilePicture") or {}
        direct = (
            _clean_value(payload.get("profile_photo_url", ""))
            or _clean_value(payload.get("photo", ""))
            or (_clean_value(profile_picture.get("url", "")) if isinstance(profile_picture, dict) else "")
        )
        if direct:
            return direct
        item = payload.get("item")
        if isinstance(item, dict):
            profile_picture = item.get("profilePicture") or {}
            return (
                _clean_value(item.get("profile_photo_url", ""))
                or _clean_value(item.get("photo", ""))
                or (_clean_value(profile_picture.get("url", "")) if isinstance(profile_picture, dict) else "")
            )
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                profile_picture = item.get("profilePicture") or {}
                value = (
                    _clean_value(item.get("profile_photo_url", ""))
                    or _clean_value(item.get("photo", ""))
                    or (_clean_value(profile_picture.get("url", "")) if isinstance(profile_picture, dict) else "")
                )
                if not value and isinstance(item.get("item"), dict):
                    nested = item.get("item") or {}
                    profile_picture = nested.get("profilePicture") or {}
                    value = (
                        _clean_value(nested.get("profile_photo_url", ""))
                        or _clean_value(nested.get("photo", ""))
                        or (_clean_value(profile_picture.get("url", "")) if isinstance(profile_picture, dict) else "")
                    )
                if value:
                    return value
    return ""


def _extract_linkedin_url(payload) -> str:
    if isinstance(payload, dict):
        direct = (
            _clean_value(payload.get("linkedin_url", ""))
            or _clean_value(payload.get("profile_url", ""))
            or _clean_value(payload.get("linkedinUrl", ""))
        )
        if direct:
            return direct
        item = payload.get("item")
        if isinstance(item, dict):
          return (
              _clean_value(item.get("linkedin_url", ""))
              or _clean_value(item.get("profile_url", ""))
              or _clean_value(item.get("linkedinUrl", ""))
          )
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                value = (
                    _clean_value(item.get("linkedin_url", ""))
                    or _clean_value(item.get("profile_url", ""))
                    or _clean_value(item.get("linkedinUrl", ""))
                )
                if not value and isinstance(item.get("item"), dict):
                    nested = item.get("item") or {}
                    value = (
                        _clean_value(nested.get("linkedin_url", ""))
                        or _clean_value(nested.get("profile_url", ""))
                        or _clean_value(nested.get("linkedinUrl", ""))
                    )
                if value:
                    return value
    return ""


def _pick_profile_item(payload):
    if isinstance(payload, dict):
        item = payload.get("item")
        if isinstance(item, dict):
            return item
        return payload
    if isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, dict):
                nested = entry.get("item")
                if isinstance(nested, dict):
                    return nested
                return entry
    return {}


def _profile_summary_from_payload(payload) -> dict:
    item = _pick_profile_item(payload)
    if not isinstance(item, dict):
        return {}
    education_lines = []
    educations = item.get("educations")
    if isinstance(educations, list):
        for edu in educations:
            if not isinstance(edu, dict):
                continue
            parts = [
                _clean_value(edu.get("schoolName", "")),
                _clean_value(edu.get("degreeName", "")),
                _clean_value(edu.get("fieldOfStudy", "")),
            ]
            line = " · ".join(part for part in parts if part)
            if line:
                education_lines.append(line)
    if not education_lines:
        top_education = item.get("profileTopEducation")
        if isinstance(top_education, list):
            for edu in top_education:
                if isinstance(edu, dict):
                    line = _clean_value(edu.get("schoolName", ""))
                    if line:
                        education_lines.append(line)

    current_position = {}
    positions = item.get("currentPosition")
    if isinstance(positions, list) and positions:
        first = positions[0]
        if isinstance(first, dict):
            current_position = first

    return {
        "headline": _clean_value(item.get("headline", "")),
        "about": _clean_value(item.get("about", "")),
        "location": _clean_value(((item.get("location") or {}).get("linkedinText") if isinstance(item.get("location"), dict) else "")),
        "linkedin_url": _clean_value(item.get("linkedinUrl", "")),
        "photo_url": _clean_value(item.get("photo", "")) or _clean_value(((item.get("profilePicture") or {}).get("url") if isinstance(item.get("profilePicture"), dict) else "")),
        "current_company": _clean_value(current_position.get("companyName", "")),
        "current_role": _clean_value(current_position.get("title", "")) or _clean_value(current_position.get("position", "")),
        "education_lines": education_lines,
        "school_line": education_lines[0] if education_lines else "",
        "top_skills": _clean_value(item.get("topSkills", "")),
    }


def _normalize_external_url(raw_url: str, base_url: str = "") -> str:
    candidate = _clean_value(raw_url)
    if not candidate:
        return ""
    if candidate.startswith("//"):
        candidate = f"https:{candidate}"
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", candidate):
        if candidate.startswith("mailto:"):
            return candidate
        if candidate.startswith("www."):
            candidate = f"https://{candidate}"
        elif base_url:
            candidate = urljoin(base_url, candidate)
        else:
            return ""
    parsed = urlsplit(candidate)
    if parsed.scheme not in {"http", "https", "mailto"}:
        return ""
    return candidate


def _extract_urls_from_text(value: object) -> list[str]:
    text = str(value or "")
    if not text:
        return []
    return [match.group(0) for match in URL_REGEX.finditer(text)]


def _extract_outbound_links_from_html(html: str, page_url: str) -> list[str]:
    links: list[str] = []
    for match in HREF_REGEX.finditer(html):
        normalized = _normalize_external_url(match.group(1), page_url)
        if normalized:
            links.append(normalized)
    return links


def _infer_contact_type(url: str) -> str:
    lower = url.lower()
    if lower.startswith("mailto:"):
        return "email"
    host = urlsplit(url).netloc.lower().replace("www.", "")
    if "linkedin.com" in host:
        return "linkedin"
    if "github.com" in host:
        return "github"
    if "twitter.com" in host or host == "x.com" or host.endswith(".x.com"):
        return "twitter"
    if "scholar.google" in host:
        return "scholar"
    return "website"


def _build_contact_label(contact_type: str, url: str) -> str:
    if contact_type == "email":
        return "Email"
    if contact_type == "github":
        return "GitHub"
    if contact_type == "twitter":
        return "X / Twitter"
    if contact_type == "scholar":
        return "Google Scholar"
    if contact_type == "linkedin":
        return "LinkedIn"
    host = urlsplit(url).netloc.lower().replace("www.", "")
    return host or "Website"


def _should_crawl_seed(url: str) -> bool:
    contact_type = _infer_contact_type(url)
    if contact_type in {"email", "linkedin", "github", "twitter", "scholar"}:
        return False
    parsed = urlsplit(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _is_static_asset_url(url: str) -> bool:
    path = urlsplit(url).path.lower()
    return any(
        path.endswith(ext)
        for ext in (
            ".css",
            ".js",
            ".mjs",
            ".json",
            ".xml",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".svg",
            ".webp",
            ".ico",
            ".woff",
            ".woff2",
            ".ttf",
            ".map",
            ".mp4",
            ".mp3",
            ".zip",
        )
    )


def _is_noise_website_host(host: str) -> bool:
    return any(
        token in host
        for token in (
            "greenhouse.io",
            "googleapis.com",
            "gstatic.com",
            "cdn.",
            "jsdelivr.net",
            "cdnjs.",
            "doubleclick.net",
        )
    )


def _fetch_html(url: str, timeout_seconds: int) -> str:
    request = Request(
        url,
        method="GET",
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; SourcingAIAgentFrontendDemo/1.0)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        content_type = str(response.headers.get("Content-Type", "")).lower()
        payload = response.read()
        if "text/html" not in content_type and "<html" not in payload[:2000].decode("utf-8", errors="ignore").lower():
            return ""
        return payload.decode("utf-8", errors="ignore")


def _extract_candidate_contact_links(
    candidate: dict,
    *,
    crawl_timeout_seconds: int,
    max_crawled_pages: int = 2,
) -> list[dict]:
    candidate_id = _clean_value(candidate.get("candidate_id", ""))
    if not candidate_id:
        return []

    raw_sources = [
        candidate.get("notes", ""),
        candidate.get("summary", ""),
        candidate.get("headline", ""),
        candidate.get("work_history", ""),
    ]
    metadata = candidate.get("metadata")
    if isinstance(metadata, dict):
        raw_sources.extend(
            [
                metadata.get("profile_url", ""),
                metadata.get("website", ""),
                metadata.get("personal_website", ""),
                metadata.get("notes", ""),
            ]
        )

    discovered_urls: list[str] = []
    for source in raw_sources:
        discovered_urls.extend(_extract_urls_from_text(source))

    linkedin_url = _clean_value(candidate.get("linkedin_url", ""))
    if linkedin_url:
        discovered_urls.append(linkedin_url)

    normalized_seeds = []
    seen_urls: set[str] = set()
    for raw in discovered_urls:
        normalized = _normalize_external_url(str(raw))
        if not normalized:
            continue
        if normalized in seen_urls:
            continue
        seen_urls.add(normalized)
        normalized_seeds.append(normalized)

    crawl_seed_hosts = {urlsplit(url).netloc.lower().replace("www.", "") for url in normalized_seeds if _should_crawl_seed(url)}
    discovered_from_pages: list[str] = []
    crawled_pages = 0
    for seed_url in list(normalized_seeds):
        if crawled_pages >= max_crawled_pages:
            break
        if not _should_crawl_seed(seed_url):
            continue
        try:
            html = _fetch_html(seed_url, timeout_seconds=crawl_timeout_seconds)
        except Exception:
            continue
        if not html:
            continue
        crawled_pages += 1
        for outbound in _extract_outbound_links_from_html(html, seed_url):
            if outbound not in seen_urls:
                seen_urls.add(outbound)
                discovered_from_pages.append(outbound)

    contacts: list[dict] = []
    seen_contact_urls: set[str] = set()
    for url in normalized_seeds + discovered_from_pages:
        contact_type = _infer_contact_type(url)
        if contact_type == "linkedin":
            continue
        parsed = urlsplit(url)
        host = parsed.netloc.lower().replace("www.", "")
        if contact_type == "website":
            if host in {"", "linkedin.com"}:
                continue
            if _is_noise_website_host(host):
                continue
            if _is_static_asset_url(url):
                continue
            if url in discovered_from_pages:
                # Keep only root-like pages on candidate-owned hosts from crawled content.
                normalized_path = (parsed.path or "/").strip()
                if host not in crawl_seed_hosts:
                    continue
                if normalized_path not in {"", "/"} and normalized_path.count("/") > 2:
                    continue
                if any(token in normalized_path.lower() for token in ("tag/", "category/", "feed", "archive")):
                    continue
        if url in seen_contact_urls:
            continue
        seen_contact_urls.add(url)
        contacts.append(
            {
                "url": url,
                "type": contact_type,
                "label": _build_contact_label(contact_type, url),
            }
        )
    return contacts[:8]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _infer_snapshot_id(manifest: dict, fallback: str) -> str:
    metadata = manifest.get("metadata", {}) or {}
    stats = manifest.get("stats", {}) or {}
    return (
        str(metadata.get("snapshot_id", "")).strip()
        or str(metadata.get("latest_snapshot_id", "")).strip()
        or str(stats.get("snapshot_id", "")).strip()
        or fallback
    )


def _normalize_header_value(value: str) -> str:
    return " ".join(value.strip().split())


def _aws_v4_signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    key_date = hmac.new(("AWS4" + secret_key).encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    key_region = hmac.new(key_date, region.encode("utf-8"), hashlib.sha256).digest()
    key_service = hmac.new(key_region, service.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(key_service, b"aws4_request", hashlib.sha256).digest()


def _join_key(prefix: str, object_key: str) -> str:
    return "/".join(part.strip("/") for part in (prefix, object_key) if part and part.strip("/"))


def _build_url(config, object_key: str) -> str:
    endpoint = str(config.endpoint_url).rstrip("/")
    parsed = urlsplit(endpoint)
    key_path = quote(_join_key(str(config.prefix), object_key), safe="/-_.~")
    base_path = parsed.path.rstrip("/")
    if bool(config.force_path_style):
      path = "/".join(part for part in [base_path, str(config.bucket), key_path] if part)
      return f"{parsed.scheme}://{parsed.netloc}/{path.lstrip('/')}"
    host = parsed.netloc
    if not host.startswith(f"{config.bucket}."):
      host = f"{config.bucket}.{host}"
    path = "/".join(part for part in [base_path, key_path] if part)
    return f"{parsed.scheme}://{host}/{path.lstrip('/')}"


def _signed_request(config, *, method: str, object_key: str, payload: bytes = b"") -> tuple[str, dict[str, str]]:
    url = _build_url(config, object_key)
    parsed = urlsplit(url)
    timestamp = datetime.now(timezone.utc)
    amz_date = timestamp.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = timestamp.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(payload).hexdigest()
    headers = {
      "host": parsed.netloc,
      "x-amz-content-sha256": payload_hash,
      "x-amz-date": amz_date,
    }
    signed_header_names = sorted(headers.keys())
    canonical_headers = "".join(f"{name}:{_normalize_header_value(headers[name])}\n" for name in signed_header_names)
    signed_headers = ";".join(signed_header_names)
    canonical_request = "\n".join(
      [
        method,
        parsed.path or "/",
        parsed.query or "",
        canonical_headers,
        signed_headers,
        payload_hash,
      ]
    )
    credential_scope = f"{date_stamp}/{config.region}/s3/aws4_request"
    string_to_sign = "\n".join(
      [
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
      ]
    )
    signing_key = _aws_v4_signing_key(str(config.secret_access_key), date_stamp, str(config.region), "s3")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = (
      "AWS4-HMAC-SHA256 "
      f"Credential={config.access_key_id}/{credential_scope}, "
      f"SignedHeaders={signed_headers}, "
      f"Signature={signature}"
    )
    final_headers = {name.title(): value for name, value in headers.items()}
    final_headers["Authorization"] = authorization
    return url, final_headers


def _download_bytes(config, object_key: str) -> bytes:
    url, headers = _signed_request(config, method="GET", object_key=object_key, payload=b"")
    request = Request(url, method="GET", headers=headers)
    try:
      with urlopen(request, timeout=int(config.timeout_seconds)) as response:
        return response.read()
    except Exception as exc:
      message = str(exc)
      if "404" in message:
        raise ObjectStorageNotFoundError(f"Object not found: {object_key}") from exc
      raise


def _download_file(config, object_key: str, local_path: Path) -> None:
    payload = _download_bytes(config, object_key)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(payload)


def _pick_latest_bundle_id(config, bundle_kind: str, company_key: str) -> str:
    payload = json.loads(_download_bytes(config, "indexes/bundle_index.json").decode("utf-8"))
    bundles = payload.get("bundles", [])
    normalized_company = company_key.strip().lower()
    candidates = []
    for item in bundles:
        if str(item.get("bundle_kind", "")).strip() != bundle_kind:
            continue
        bundle_id = str(item.get("bundle_id", "")).strip()
        metadata = item.get("metadata", {}) or {}
        haystacks = [
            bundle_id.lower(),
            str(metadata.get("company", "")).strip().lower(),
            str(metadata.get("company_key", "")).strip().lower(),
            str(metadata.get("requested_company", "")).strip().lower(),
        ]
        if not any(normalized_company and normalized_company in haystack for haystack in haystacks):
            continue
        candidates.append(item)

    if not candidates:
        raise ObjectStorageNotFoundError(
            f"No remote bundle_index entry found for bundle_kind={bundle_kind} company={company_key}"
        )

    def sort_key(item: dict) -> tuple[str, str]:
        return (
            str(item.get("last_uploaded_at") or item.get("last_downloaded_at") or item.get("updated_at") or ""),
            str(item.get("bundle_id") or ""),
        )

    latest = sorted(candidates, key=sort_key)[-1]
    bundle_id = str(latest.get("bundle_id", "")).strip()
    if not bundle_id:
        raise ObjectStorageNotFoundError(f"Latest bundle entry is missing bundle_id for company={company_key}")
    return bundle_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull Thinking Machines Lab frontend JSON assets from configured object storage.")
    parser.add_argument("--bundle-kind", default=DEFAULT_BUNDLE_KIND, help="Bundle kind in object storage")
    parser.add_argument("--bundle-id", default="", help="Bundle id in object storage; if omitted, fetch latest bundle for company")
    parser.add_argument("--company-key", default=DEFAULT_COMPANY_KEY, help="Normalized company key used to auto-discover latest bundle")
    parser.add_argument("--output-dir", default="", help="Optional output directory; defaults to frontend-demo/public/tml")
    parser.add_argument("--download-dir", default="", help="Optional temp download directory; defaults to runtime/asset_imports_frontend")
    args = parser.parse_args()

    frontend_dir = Path(__file__).resolve().parents[1]
    project_root = frontend_dir.parent
    output_dir = Path(args.output_dir) if args.output_dir else frontend_dir / "public" / "tml"
    download_root = Path(args.download_dir) if args.download_dir else project_root / "runtime" / "asset_imports_frontend"

    secrets_path = project_root / "runtime" / "secrets" / "providers.local.json"
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secret config not found: {secrets_path}")
    secret_payload = json.loads(secrets_path.read_text())
    config = ObjectStorageConfig(secret_payload.get("object_storage", {}))

    bundle_id = args.bundle_id.strip() or _pick_latest_bundle_id(config, args.bundle_kind, args.company_key)
    remote_prefix = f"bundles/{args.bundle_kind}/{bundle_id}"
    bundle_dir = download_root / bundle_id
    bundle_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = bundle_dir / "bundle_manifest.json"

    _download_file(config, f"{remote_prefix}/bundle_manifest.json", manifest_path)
    manifest = json.loads(manifest_path.read_text())

    desired_runtime_paths = {
      "normalized_candidates.json": "company_assets/thinkingmachineslab/20260406T172703/normalized_artifacts/normalized_candidates.json",
      "materialized_candidate_documents.json": "company_assets/thinkingmachineslab/20260406T172703/normalized_artifacts/materialized_candidate_documents.json",
      "reusable_candidate_documents.json": "company_assets/thinkingmachineslab/20260406T172703/normalized_artifacts/reusable_candidate_documents.json",
      "manual_review_backlog.json": "company_assets/thinkingmachineslab/20260406T172703/normalized_artifacts/manual_review_backlog.json",
      "profile_completion_backlog.json": "company_assets/thinkingmachineslab/20260406T172703/normalized_artifacts/profile_completion_backlog.json",
      "asset_registry.json": "company_assets/thinkingmachineslab/20260406T172703/asset_registry.json",
      "latest_snapshot.json": "company_assets/thinkingmachineslab/latest_snapshot.json",
    }

    manifest_files = {
        str(item.get("runtime_relative_path", "")).strip(): str(item.get("payload_relative_path", "")).strip()
        for item in manifest.get("files", [])
        if str(item.get("runtime_relative_path", "")).strip()
    }

    snapshot_id = _infer_snapshot_id(manifest, "20260406T172703")
    latest_runtime_path = "company_assets/thinkingmachineslab/latest_snapshot.json"
    latest_payload_path = manifest_files.get(latest_runtime_path, "")
    if latest_payload_path:
        local_latest = bundle_dir / latest_payload_path
        _download_file(config, f"{remote_prefix}/{latest_payload_path}", local_latest)
        latest_payload = json.loads(local_latest.read_text())
        snapshot_id = (
            str(latest_payload.get("snapshot_id", "")).strip()
            or str(latest_payload.get("latest_snapshot_id", "")).strip()
            or snapshot_id
        )

    desired_runtime_paths = {
        "normalized_candidates.json": f"company_assets/thinkingmachineslab/{snapshot_id}/normalized_artifacts/normalized_candidates.json",
        "materialized_candidate_documents.json": f"company_assets/thinkingmachineslab/{snapshot_id}/normalized_artifacts/materialized_candidate_documents.json",
        "reusable_candidate_documents.json": f"company_assets/thinkingmachineslab/{snapshot_id}/normalized_artifacts/reusable_candidate_documents.json",
        "manual_review_backlog.json": f"company_assets/thinkingmachineslab/{snapshot_id}/normalized_artifacts/manual_review_backlog.json",
        "profile_completion_backlog.json": f"company_assets/thinkingmachineslab/{snapshot_id}/normalized_artifacts/profile_completion_backlog.json",
        "candidate_documents.json": f"company_assets/thinkingmachineslab/{snapshot_id}/candidate_documents.json",
        "snapshot_manifest.json": f"company_assets/thinkingmachineslab/{snapshot_id}/manifest.json",
        "asset_registry.json": f"company_assets/thinkingmachineslab/{snapshot_id}/asset_registry.json",
        "artifact_summary.json": f"company_assets/thinkingmachineslab/{snapshot_id}/normalized_artifacts/artifact_summary.json",
        "harvest_company_employees_visible.json": f"company_assets/thinkingmachineslab/{snapshot_id}/harvest_company_employees/harvest_company_employees_visible.json",
        "latest_snapshot.json": latest_runtime_path,
    }

    downloaded_files: list[str] = []
    for output_name, runtime_relative_path in desired_runtime_paths.items():
        payload_relative_path = manifest_files.get(runtime_relative_path, "")
        if not payload_relative_path:
            continue
        local_download_path = bundle_dir / payload_relative_path
        local_download_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            _download_file(config, f"{remote_prefix}/{payload_relative_path}", local_download_path)
        except ObjectStorageNotFoundError:
            continue
        if _safe_copy(local_download_path, output_dir / output_name):
            downloaded_files.append(output_name)

    index_payload = {
        "company": "thinkingmachineslab",
        "snapshotId": snapshot_id,
        "bundleKind": args.bundle_kind,
        "bundleId": bundle_id,
        "files": downloaded_files,
        "source": "object_storage",
    }
    _write_json(output_dir / "index.json", index_payload)

    candidate_documents_path = output_dir / "candidate_documents.json"
    if candidate_documents_path.exists():
        payload = json.loads(candidate_documents_path.read_text())
        candidates = payload.get("candidates", []) if isinstance(payload, dict) else []
        visible_roster_path = output_dir / "harvest_company_employees_visible.json"
        visible_roster = json.loads(visible_roster_path.read_text()) if visible_roster_path.exists() else []
        visible_by_linkedin_url = {}
        for item in visible_roster:
            if not isinstance(item, dict):
                continue
            linkedin_url = str(item.get("linkedin_url", "")).strip()
            avatar_url = str(item.get("avatar_url", "")).strip()
            if linkedin_url and avatar_url:
                visible_by_linkedin_url[linkedin_url] = avatar_url
        profiles_dir = output_dir / "profiles"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        downloaded_profiles = []
        profile_photo_map = {}
        profile_summary_map = {}
        enriched_contact_map = {}
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            candidate_id = str(candidate.get("candidate_id", "")).strip()
            if candidate_id:
                contacts = _extract_candidate_contact_links(
                    candidate,
                    crawl_timeout_seconds=max(5, int(config.timeout_seconds // 2)),
                )
                if contacts:
                    enriched_contact_map[candidate_id] = contacts
            source_path = str(candidate.get("source_path", "")).strip()
            runtime_relative_path = _runtime_relative_from_source_path(source_path)
            if not runtime_relative_path:
                continue
            payload_relative_path = manifest_files.get(runtime_relative_path, "")
            if not payload_relative_path:
                continue
            destination_name = os.path.basename(runtime_relative_path)
            local_download_path = bundle_dir / payload_relative_path
            try:
                _download_file(config, f"{remote_prefix}/{payload_relative_path}", local_download_path)
            except ObjectStorageNotFoundError:
                continue
            if _safe_copy(local_download_path, profiles_dir / destination_name):
                downloaded_profiles.append(destination_name)
                if destination_name.endswith(".json"):
                    try:
                        profile_payload = json.loads((profiles_dir / destination_name).read_text())
                    except Exception:
                        profile_payload = {}
                    candidate_id = str(candidate.get("candidate_id", "")).strip()
                    summary = _profile_summary_from_payload(profile_payload)
                    if candidate_id and summary:
                        profile_summary_map[candidate_id] = summary
                    photo_url = _extract_profile_photo_url(profile_payload)
                    linkedin_url = _extract_linkedin_url(profile_payload)
                    if photo_url:
                        if candidate_id:
                            profile_photo_map[candidate_id] = photo_url
                    elif linkedin_url and linkedin_url in visible_by_linkedin_url:
                        if candidate_id:
                            profile_photo_map[candidate_id] = visible_by_linkedin_url[linkedin_url]

        for candidate in candidates:
            candidate_id = str(candidate.get("candidate_id", "")).strip()
            if not candidate_id or candidate_id in profile_photo_map:
                continue
            linkedin_url = str(candidate.get("linkedin_url", "")).strip()
            if linkedin_url and linkedin_url in visible_by_linkedin_url:
                profile_photo_map[candidate_id] = visible_by_linkedin_url[linkedin_url]

        materialized_path = output_dir / "materialized_candidate_documents.json"
        if materialized_path.exists():
            try:
                materialized_payload = json.loads(materialized_path.read_text())
            except Exception:
                materialized_payload = {}
            materialized_candidates = materialized_payload.get("candidates", []) if isinstance(materialized_payload, dict) else []
            for candidate in materialized_candidates:
                if not isinstance(candidate, dict):
                    continue
                candidate_id = _clean_value(candidate.get("candidate_id", ""))
                if not candidate_id or candidate_id in profile_photo_map:
                    continue
                source_path = _clean_value(candidate.get("source_path", ""))
                destination_name = os.path.basename(source_path) if source_path else ""
                if destination_name:
                    profile_path = profiles_dir / destination_name
                    if profile_path.exists():
                        try:
                            profile_payload = json.loads(profile_path.read_text())
                        except Exception:
                            profile_payload = {}
                        photo_url = _extract_profile_photo_url(profile_payload)
                        if photo_url:
                            profile_photo_map[candidate_id] = photo_url
                            continue
                linkedin_url = _clean_value(candidate.get("linkedin_url", ""))
                if linkedin_url and linkedin_url in visible_by_linkedin_url:
                    profile_photo_map[candidate_id] = visible_by_linkedin_url[linkedin_url]
                if candidate_id and candidate_id not in enriched_contact_map:
                    contacts = _extract_candidate_contact_links(
                        candidate,
                        crawl_timeout_seconds=max(5, int(config.timeout_seconds // 2)),
                    )
                    if contacts:
                        enriched_contact_map[candidate_id] = contacts

        index_payload["profileFiles"] = downloaded_profiles
        _write_json(output_dir / "index.json", index_payload)
        _write_json(output_dir / "profile_photos.json", profile_photo_map)
        _write_json(output_dir / "profile_summaries.json", profile_summary_map)
        _write_json(output_dir / "enriched_contact_links.json", enriched_contact_map)

    print(json.dumps(index_payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
