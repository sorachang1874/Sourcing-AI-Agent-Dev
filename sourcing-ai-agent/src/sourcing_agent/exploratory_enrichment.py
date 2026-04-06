from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from html import unescape
from pathlib import Path
import re
from typing import Any
from urllib import parse

from .agent_runtime import AgentRuntimeCoordinator
from .asset_policy import (
    DEFAULT_MODEL_CONTEXT_CHAR_LIMIT,
    RAW_ASSET_POLICY_SUMMARY,
    is_binary_like_url,
)
from .asset_logger import AssetLogger
from .domain import Candidate, EvidenceRecord, JobRequest, make_evidence_id, merge_candidate
from .model_provider import DeterministicModelClient, ModelClient
from .search_provider import BaseSearchProvider, DuckDuckGoHtmlSearchProvider, search_response_to_record
from .web_fetch import fetch_text_url
from .worker_daemon import AutonomousWorkerDaemon


@dataclass(slots=True)
class ExploratoryEnrichmentResult:
    candidates: list[Candidate]
    evidence: list[EvidenceRecord]
    artifact_paths: dict[str, str] = field(default_factory=dict)
    explored_candidates: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class ExploratoryWebEnricher:
    def __init__(
        self,
        model_client: ModelClient | None = None,
        *,
        worker_runtime: AgentRuntimeCoordinator | None = None,
        search_provider: BaseSearchProvider | None = None,
    ) -> None:
        self.model_client = model_client or DeterministicModelClient()
        self.worker_runtime = worker_runtime
        self.search_provider = search_provider or DuckDuckGoHtmlSearchProvider()

    def enrich(
        self,
        snapshot_dir: Path,
        candidates: list[Candidate],
        *,
        target_company: str,
        max_candidates: int,
        asset_logger: AssetLogger | None = None,
        job_id: str = "",
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        runtime_mode: str = "workflow",
        parallel_workers: int = 2,
    ) -> ExploratoryEnrichmentResult:
        exploration_dir = snapshot_dir / "exploration"
        exploration_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        merged_candidates: list[Candidate] = []
        evidence: list[EvidenceRecord] = []
        explored_candidates: list[dict[str, Any]] = []
        errors: list[str] = []

        selected_candidates = candidates[:max_candidates]
        pending_specs = [
            {
                "index": index,
                "candidate": candidate,
                "lane_id": "exploration_specialist",
                "worker_key": candidate.candidate_id,
                "label": candidate.display_name,
            }
            for index, candidate in enumerate(selected_candidates, start=1)
        ]
        max_workers = max(1, min(parallel_workers, len(selected_candidates) or 1))
        daemon_summary = {
            "results": [],
            "retried": [],
            "backlog": [],
            "lane_budget_used": {},
            "lane_budget_caps": {},
            "cycles": 0,
            "daemon_events": [],
        }
        if pending_specs:
            if self.worker_runtime is not None and job_id:
                daemon = AutonomousWorkerDaemon.from_plan(
                    plan_payload={"acquisition_strategy": {"cost_policy": {"parallel_exploration_workers": parallel_workers}}},
                    existing_workers=self.worker_runtime.list_workers(job_id=job_id, lane_id="exploration_specialist"),
                    total_limit=max_workers,
                )
                daemon_summary = daemon.run(
                    pending_specs,
                    executor=lambda spec: self._explore_candidate(
                        snapshot_dir=snapshot_dir,
                        candidate=spec["candidate"],
                        target_company=target_company,
                        logger=logger,
                        job_id=job_id,
                        request_payload=request_payload or {},
                        plan_payload=plan_payload or {},
                        runtime_mode=runtime_mode,
                    ),
                )
                daemon_results = list(daemon_summary.get("results") or [])
            else:
                with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(pending_specs)))) as executor:
                    futures = [
                        executor.submit(
                            self._explore_candidate,
                            snapshot_dir=snapshot_dir,
                            candidate=spec["candidate"],
                            target_company=target_company,
                            logger=logger,
                            job_id=job_id,
                            request_payload=request_payload or {},
                            plan_payload=plan_payload or {},
                            runtime_mode=runtime_mode,
                        )
                        for spec in pending_specs
                    ]
                    daemon_results = [future.result() for future in futures]
            for result in daemon_results:
                merged_candidates.append(result["candidate"])
                evidence.extend(result["evidence"])
                explored_candidates.append(result["summary"])
                errors.extend(result["errors"])

        summary_path = exploration_dir / "summary.json"
        logger.write_json(
            summary_path,
            {
                "target_company": target_company,
                "explored_candidates": explored_candidates,
                "errors": errors,
                "worker_daemon": {
                    "cycles": int(daemon_summary.get("cycles") or 0),
                    "retried": list(daemon_summary.get("retried") or []),
                    "backlog_count": len(list(daemon_summary.get("backlog") or [])),
                    "lane_budget_used": dict(daemon_summary.get("lane_budget_used") or {}),
                    "lane_budget_caps": dict(daemon_summary.get("lane_budget_caps") or {}),
                },
            },
            asset_type="exploration_summary",
            source_kind="further_exploration",
            is_raw_asset=False,
            model_safe=True,
        )
        return ExploratoryEnrichmentResult(
            candidates=merged_candidates,
            evidence=evidence,
            artifact_paths={"exploration_summary": str(summary_path)},
            explored_candidates=explored_candidates,
            errors=errors,
        )

    def _explore_candidate(
        self,
        *,
        snapshot_dir: Path,
        candidate: Candidate,
        target_company: str,
        logger: AssetLogger,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
    ) -> dict[str, Any]:
        exploration_dir = snapshot_dir / "exploration"
        candidate_dir = exploration_dir / candidate.candidate_id
        candidate_dir.mkdir(parents=True, exist_ok=True)
        queries = _build_exploration_queries(candidate, target_company)
        result_summaries: list[dict[str, Any]] = []
        gathered_signals = {
            "linkedin_urls": [],
            "x_urls": [],
            "github_urls": [],
            "personal_urls": [],
            "resume_urls": [],
            "descriptions": [],
            "validated_summaries": [],
            "role_signals": [],
            "analysis_notes": [],
        }
        errors: list[str] = []
        worker_handle = None
        completed_queries = set()
        interrupted = False
        if self.worker_runtime is not None and job_id:
            worker_handle = self.worker_runtime.begin_worker(
                job_id=job_id,
                request=JobRequest.from_payload(request_payload),
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                lane_id="exploration_specialist",
                worker_key=candidate.candidate_id,
                stage="enriching",
                span_name=f"explore_candidate:{candidate.display_name}",
                budget_payload={"max_queries": len(queries), "max_pages_per_query": 5},
                input_payload={
                    "candidate_id": candidate.candidate_id,
                    "display_name": candidate.display_name,
                    "candidate": candidate.to_record(),
                },
                metadata={
                    "target_company": target_company,
                    "snapshot_dir": str(snapshot_dir),
                    "request_payload": request_payload,
                    "plan_payload": plan_payload,
                    "runtime_mode": runtime_mode,
                },
                handoff_from_lane="public_media_specialist",
            )
            existing = self.worker_runtime.get_worker(worker_handle.worker_id) or {}
            checkpoint = dict(existing.get("checkpoint") or {})
            completed_queries = set(checkpoint.get("completed_queries") or [])
            result_summaries = list(checkpoint.get("result_summaries") or [])
            stored_signals = dict(checkpoint.get("gathered_signals") or {})
            if stored_signals:
                for key, values in stored_signals.items():
                    gathered_signals[key] = list(values or [])
            errors = list(checkpoint.get("errors") or [])
            output_payload = dict(existing.get("output") or {})
            if str(existing.get("status") or "") == "completed" and output_payload:
                self.worker_runtime.complete_worker(
                    worker_handle,
                    status="completed",
                    checkpoint_payload=checkpoint,
                    output_payload=output_payload,
                    handoff_to_lane="enrichment_specialist",
                )
                return {
                    "candidate": Candidate(**dict(output_payload.get("candidate") or candidate.to_record())),
                    "evidence": [EvidenceRecord(**item) for item in list(output_payload.get("evidence") or [])],
                    "summary": dict(output_payload.get("summary") or {"candidate_id": candidate.candidate_id}),
                    "errors": list(output_payload.get("errors") or []),
                    "worker_status": "completed",
                    "daemon_action": "reused_output",
                }

        for index, query_text in enumerate(queries, start=1):
            if str(index) in completed_queries:
                continue
            if worker_handle and self.worker_runtime and self.worker_runtime.should_interrupt_worker(worker_handle):
                interrupted = True
                break
            raw_search_path = candidate_dir / f"query_{index:02d}.html"
            try:
                response = self.search_provider.search(query_text, max_results=10)
                raw_search_path = candidate_dir / f"query_{index:02d}.{ 'json' if response.raw_format == 'json' else 'html'}"
                if response.raw_format == "json":
                    logger.write_json(
                        raw_search_path,
                        search_response_to_record(response),
                        asset_type="exploration_search_payload",
                        source_kind="further_exploration",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={
                            "candidate_id": candidate.candidate_id,
                            "query": query_text,
                            "provider_name": response.provider_name,
                        },
                    )
                else:
                    logger.write_text(
                        raw_search_path,
                        str(response.raw_payload or ""),
                        asset_type="exploration_search_html",
                        source_kind="further_exploration",
                        content_type=response.content_type or "text/html",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={
                            "candidate_id": candidate.candidate_id,
                            "query": query_text,
                            "provider_name": response.provider_name,
                        },
                    )
                results = [item.to_record() for item in response.results]
            except Exception as exc:
                errors.append(f"exploration_search:{candidate.display_name}:{str(exc)[:120]}")
                result_summaries.append({"query": query_text, "raw_path": str(raw_search_path), "error": str(exc)})
                continue

            page_summaries: list[dict[str, Any]] = []
            for result_index, item in enumerate(results[:5], start=1):
                if worker_handle and self.worker_runtime and self.worker_runtime.should_interrupt_worker(worker_handle):
                    interrupted = True
                    break
                page_summary = {"title": item.get("title", ""), "url": item.get("url", "")}
                try:
                    if _is_fetchable_detail_page(item.get("url", "")):
                        page_html = _fetch_html(item.get("url", ""))
                        page_path = candidate_dir / f"query_{index:02d}_page_{result_index:02d}.html"
                        logger.write_text(
                            page_path,
                            page_html,
                            asset_type="exploration_detail_html",
                            source_kind="further_exploration",
                            content_type="text/html",
                            is_raw_asset=True,
                            model_safe=False,
                            metadata={"candidate_id": candidate.candidate_id, "source_url": item.get("url", "")},
                        )
                        signals = extract_page_signals(page_html, item.get("url", ""))
                        analysis_input = build_page_analysis_input(
                            candidate=candidate,
                            target_company=target_company,
                            source_url=item.get("url", ""),
                            html_text=page_html,
                            extracted_links=signals,
                        )
                        analysis_input_path = candidate_dir / f"query_{index:02d}_page_{result_index:02d}_analysis_input.json"
                        logger.write_json(
                            analysis_input_path,
                            analysis_input,
                            asset_type="exploration_analysis_input",
                            source_kind="further_exploration",
                            is_raw_asset=False,
                            model_safe=True,
                            metadata={"candidate_id": candidate.candidate_id, "source_url": item.get("url", "")},
                        )
                        analysis = self.model_client.analyze_page_asset(analysis_input)
                        analysis_path = candidate_dir / f"query_{index:02d}_page_{result_index:02d}_analysis.json"
                        logger.write_json(
                            analysis_path,
                            analysis,
                            asset_type="exploration_analysis_output",
                            source_kind="further_exploration",
                            is_raw_asset=False,
                            model_safe=True,
                            metadata={"candidate_id": candidate.candidate_id, "source_url": item.get("url", "")},
                        )
                        page_summary["detail_path"] = str(page_path)
                        page_summary["analysis_input_path"] = str(analysis_input_path)
                        page_summary["analysis_path"] = str(analysis_path)
                        page_summary["signals"] = signals
                        page_summary["analysis"] = analysis
                        _merge_signal_map(gathered_signals, signals)
                        _merge_analysis_map(gathered_signals, analysis)
                except Exception as exc:
                    page_summary["error"] = str(exc)
                page_summaries.append(page_summary)
            result_summaries.append(
                {
                    "query": query_text,
                    "raw_path": str(raw_search_path),
                    "provider_name": response.provider_name,
                    "result_count": len(results),
                    "pages": page_summaries,
                }
            )
            completed_queries.add(str(index))
            if worker_handle and self.worker_runtime:
                self.worker_runtime.checkpoint_worker(
                    worker_handle,
                    checkpoint_payload={
                        "completed_queries": sorted(completed_queries),
                        "result_summaries": result_summaries,
                        "gathered_signals": gathered_signals,
                        "errors": errors,
                    },
                    output_payload={"explored_query_count": len(completed_queries)},
                )

        merged_candidate, candidate_evidence = _merge_exploration(candidate, gathered_signals, candidate_dir)
        summary = {
            "candidate_id": candidate.candidate_id,
            "display_name": candidate.display_name,
            "queries": result_summaries,
            "signals": gathered_signals,
        }
        if interrupted:
            summary["status"] = "interrupted"
        if worker_handle and self.worker_runtime:
            if interrupted:
                self.worker_runtime.complete_worker(
                    worker_handle,
                    status="interrupted",
                    checkpoint_payload={
                        "completed_queries": sorted(completed_queries),
                        "result_summaries": result_summaries,
                        "gathered_signals": gathered_signals,
                        "errors": errors,
                        "stage": "interrupted",
                    },
                    output_payload={
                        "signal_counts": {key: len(value) for key, value in gathered_signals.items()},
                        "candidate": merged_candidate.to_record(),
                        "evidence": [item.to_record() for item in candidate_evidence],
                        "summary": summary,
                        "errors": errors,
                    },
                    handoff_to_lane="review_specialist",
                )
            else:
                self.worker_runtime.complete_worker(
                    worker_handle,
                    status="completed",
                    checkpoint_payload={
                        "completed_queries": sorted(completed_queries),
                        "result_summaries": result_summaries,
                        "gathered_signals": gathered_signals,
                        "errors": errors,
                        "stage": "completed",
                    },
                    output_payload={
                        "signal_counts": {key: len(value) for key, value in gathered_signals.items()},
                        "candidate": merged_candidate.to_record(),
                        "evidence": [item.to_record() for item in candidate_evidence],
                        "summary": summary,
                        "errors": errors,
                    },
                    handoff_to_lane="enrichment_specialist",
                )
        return {
            "candidate": merged_candidate,
            "evidence": candidate_evidence,
            "summary": summary,
            "errors": errors,
            "worker_status": "interrupted" if interrupted else "completed",
        }


def extract_page_signals(html_text: str, base_url: str) -> dict[str, list[str]]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    title = " ".join(unescape(title_match.group(1)).split()) if title_match else ""
    meta_match = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        html_text,
        flags=re.IGNORECASE,
    )
    if not meta_match:
        meta_match = re.search(
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
            html_text,
            flags=re.IGNORECASE,
        )
    description = " ".join(unescape(meta_match.group(1)).split()) if meta_match else ""
    links = re.findall(r'href=["\']([^"\']+)["\']', html_text, flags=re.IGNORECASE)
    absolute_links = [_normalize_url(parse.urljoin(base_url, link)) for link in links]
    linkedin_urls = _dedupe([link for link in absolute_links if "linkedin.com/in/" in link])
    x_urls = _dedupe([link for link in absolute_links if "x.com/" in link or "twitter.com/" in link])
    github_urls = _dedupe([link for link in absolute_links if "github.com/" in link])
    resume_urls = _dedupe(
        [
            link
            for link in absolute_links
            if link.endswith(".pdf") or any(token in link.lower() for token in ["/cv", "/resume", "resume", "curriculum-vitae"])
        ]
    )
    personal_urls = _dedupe(
        [
            link
            for link in absolute_links
            if link.startswith("http")
            and "linkedin.com" not in link
            and "x.com" not in link
            and "twitter.com" not in link
            and "github.com" not in link
            and "duckduckgo.com" not in link
            and _is_meaningful_personal_url(link)
        ]
    )
    descriptions = _dedupe([item for item in [title, description] if item])
    return {
        "linkedin_urls": linkedin_urls,
        "x_urls": x_urls,
        "github_urls": github_urls,
        "personal_urls": personal_urls,
        "resume_urls": resume_urls,
        "descriptions": descriptions,
        "validated_summaries": [],
        "role_signals": [],
        "analysis_notes": [],
    }


def _build_exploration_queries(candidate: Candidate, target_company: str) -> list[str]:
    name = candidate.display_name or candidate.name_en
    queries = [
        f'"{name}" "{target_company}"',
        f'"{name}" "{target_company}" site:linkedin.com/in',
        f'"{name}" "{target_company}" LinkedIn',
        f'"{name}" "{target_company}" X',
        f'"{name}" "{target_company}" GitHub',
        f'"{name}" "{target_company}" CV',
        f'"{name}" "{target_company}" homepage',
    ]
    deduped: list[str] = []
    for query in queries:
        normalized = " ".join(query.split())
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped


def _fetch_html(url: str) -> str:
    return fetch_text_url(url, timeout=30).text


def _is_fetchable_detail_page(url: str) -> bool:
    url = str(url or "").strip().lower()
    if not url.startswith("http"):
        return False
    blocked = ["duckduckgo.com", "google.com/search", "bing.com/search"]
    if is_binary_like_url(url):
        return False
    return not any(item in url for item in blocked)


def _normalize_url(url: str) -> str:
    return str(url or "").strip()


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(normalized)
    return results


def _is_meaningful_personal_url(url: str) -> bool:
    lower = str(url or "").strip().lower()
    if not lower.startswith("http"):
        return False
    blocked_tokens = [
        "static.licdn.com",
        "media.licdn.com",
        "www.gstatic.com",
        "ssl.gstatic.com",
        "fonts.googleapis.com",
        "fonts.gstatic.com",
        "chrome.google.com",
    ]
    if any(token in lower for token in blocked_tokens):
        return False
    blocked_suffixes = [
        ".css",
        ".js",
        ".png",
        ".jpg",
        ".jpeg",
        ".svg",
        ".ico",
        ".woff",
        ".woff2",
    ]
    return not any(lower.endswith(suffix) for suffix in blocked_suffixes)


def _merge_signal_map(target: dict[str, list[str]], incoming: dict[str, list[str]]) -> None:
    for key, values in incoming.items():
        existing = target.setdefault(key, [])
        for value in values:
            if value not in existing:
                existing.append(value)


def _merge_analysis_map(target: dict[str, list[str]], analysis: dict[str, Any]) -> None:
    summary = str(analysis.get("summary") or "").strip()
    if summary:
        summaries = target.setdefault("validated_summaries", [])
        if summary not in summaries:
            summaries.append(summary)
    notes = str(analysis.get("notes") or "").strip()
    if notes:
        note_items = target.setdefault("analysis_notes", [])
        if notes not in note_items:
            note_items.append(notes)
    for signal in analysis.get("role_signals") or []:
        normalized = str(signal or "").strip()
        if not normalized:
            continue
        items = target.setdefault("role_signals", [])
        if normalized not in items:
            items.append(normalized)


def _merge_exploration(candidate: Candidate, signals: dict[str, list[str]], candidate_dir: Path) -> tuple[Candidate, list[EvidenceRecord]]:
    notes = candidate.notes
    note_segments: list[str] = []
    if signals["descriptions"]:
        note_segments.append(" | ".join(signals["descriptions"][:2]))
    if signals.get("validated_summaries"):
        note_segments.append(" | ".join(signals["validated_summaries"][:2]))
    if note_segments:
        notes = " ".join([candidate.notes, "Exploration:", " || ".join(note_segments)]).strip()
    metadata = {
        **candidate.metadata,
        "exploration_links": {
            "linkedin": signals["linkedin_urls"],
            "x": signals["x_urls"],
            "github": signals["github_urls"],
            "personal": signals["personal_urls"],
            "resume": signals["resume_urls"],
        },
        "exploration_descriptions": signals["descriptions"][:4],
        "exploration_role_signals": signals.get("role_signals", [])[:8],
        "exploration_validated_summaries": signals.get("validated_summaries", [])[:4],
    }
    incoming = Candidate(
        candidate_id=candidate.candidate_id,
        name_en=candidate.name_en,
        name_zh=candidate.name_zh,
        display_name=candidate.display_name,
        category=candidate.category,
        target_company=candidate.target_company,
        organization=candidate.organization,
        employment_status=candidate.employment_status,
        role=candidate.role,
        team=candidate.team,
        focus_areas=candidate.focus_areas,
        education=candidate.education,
        work_history=candidate.work_history,
        notes=notes,
        linkedin_url=candidate.linkedin_url or (signals["linkedin_urls"][0] if signals["linkedin_urls"] else ""),
        media_url=candidate.media_url or _pick_media_url(signals),
        source_dataset=candidate.source_dataset,
        source_path=str(candidate_dir),
        metadata=metadata,
    )
    merged = merge_candidate(candidate, incoming)
    evidence: list[EvidenceRecord] = []
    for title, urls, source_type in [
        ("Exploration LinkedIn URL", signals["linkedin_urls"][:1], "exploration_linkedin"),
        ("Exploration X URL", signals["x_urls"][:1], "exploration_x"),
        ("Exploration GitHub URL", signals["github_urls"][:1], "exploration_github"),
        ("Exploration Personal URL", signals["personal_urls"][:1], "exploration_personal"),
        ("Exploration Resume URL", signals["resume_urls"][:1], "exploration_resume"),
    ]:
        for url in urls:
            evidence.append(
                EvidenceRecord(
                    evidence_id=make_evidence_id(candidate.candidate_id, source_type, title, url),
                    candidate_id=candidate.candidate_id,
                    source_type=source_type,
                    title=title,
                    url=url,
                    summary=f"Further exploration recovered {title.lower()} for {candidate.display_name}.",
                    source_dataset=source_type,
                    source_path=str(candidate_dir),
                    metadata={"descriptions": signals["descriptions"][:3]},
                )
            )
    if signals["descriptions"]:
        evidence.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(candidate.candidate_id, "exploration_summary", candidate.display_name, str(candidate_dir)),
                candidate_id=candidate.candidate_id,
                source_type="exploration_summary",
                title=f"{candidate.display_name} exploration summary",
                url="",
                summary=" | ".join(signals["descriptions"][:2]),
                source_dataset="exploration_summary",
                source_path=str(candidate_dir),
                metadata={"descriptions": signals["descriptions"][:4]},
            )
        )
    if signals.get("validated_summaries"):
        evidence.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(candidate.candidate_id, "exploration_model_summary", candidate.display_name, str(candidate_dir)),
                candidate_id=candidate.candidate_id,
                source_type="exploration_model_summary",
                title=f"{candidate.display_name} analyzed exploration summary",
                url="",
                summary=" | ".join(signals["validated_summaries"][:2]),
                source_dataset="exploration_model_summary",
                source_path=str(candidate_dir),
                metadata={
                    "role_signals": signals.get("role_signals", [])[:8],
                    "analysis_notes": signals.get("analysis_notes", [])[:4],
                },
            )
        )
    return merged, evidence


def _pick_media_url(signals: dict[str, list[str]]) -> str:
    for key in ["x_urls", "github_urls", "personal_urls", "resume_urls"]:
        if signals[key]:
            return signals[key][0]
    return ""


def build_page_analysis_input(
    *,
    candidate: Candidate,
    target_company: str,
    source_url: str,
    html_text: str,
    extracted_links: dict[str, list[str]],
    max_excerpt_chars: int = DEFAULT_MODEL_CONTEXT_CHAR_LIMIT,
) -> dict[str, Any]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    title = " ".join(unescape(title_match.group(1)).split()) if title_match else ""
    meta_match = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        html_text,
        flags=re.IGNORECASE,
    )
    if not meta_match:
        meta_match = re.search(
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
            html_text,
            flags=re.IGNORECASE,
        )
    description = " ".join(unescape(meta_match.group(1)).split()) if meta_match else ""
    visible_text = re.sub(r"<script.*?</script>|<style.*?</style>", " ", html_text, flags=re.IGNORECASE | re.DOTALL)
    visible_text = re.sub(r"<[^>]+>", " ", visible_text)
    visible_text = " ".join(unescape(visible_text).split())
    excerpt = visible_text[:max_excerpt_chars]
    return {
        "candidate_id": candidate.candidate_id,
        "candidate_name": candidate.display_name,
        "target_company": target_company,
        "source_url": source_url,
        "title": title,
        "description": description,
        "text_excerpt": excerpt,
        "excerpt_char_count": len(excerpt),
        "raw_asset_policy": RAW_ASSET_POLICY_SUMMARY,
        "extracted_links": {
            "linkedin_urls": extracted_links.get("linkedin_urls", [])[:3],
            "x_urls": extracted_links.get("x_urls", [])[:3],
            "github_urls": extracted_links.get("github_urls", [])[:3],
            "personal_urls": extracted_links.get("personal_urls", [])[:3],
            "resume_urls": extracted_links.get("resume_urls", [])[:3],
        },
    }
