from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .agent_runtime import AgentRuntimeCoordinator
from .asset_policy import (
    DEFAULT_MODEL_CONTEXT_CHAR_LIMIT,
    is_binary_like_url,
)
from .asset_logger import AssetLogger
from .document_extraction import (
    analyze_remote_document,
    build_candidate_patch_from_signal_bundle,
    build_page_analysis_input as _build_page_analysis_input_impl,
    empty_signal_bundle,
    extract_page_signals as _extract_page_signals_impl,
    merge_signal_bundle,
)
from .domain import Candidate, EvidenceRecord, JobRequest, make_evidence_id, merge_candidate
from .model_provider import DeterministicModelClient, ModelClient
from .search_provider import BaseSearchProvider, DuckDuckGoHtmlSearchProvider, search_response_to_record
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
        gathered_signals = empty_signal_bundle()
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
                        analyzed_page = analyze_remote_document(
                            candidate=candidate,
                            target_company=target_company,
                            source_url=item.get("url", ""),
                            asset_dir=candidate_dir,
                            asset_logger=logger,
                            model_client=self.model_client,
                            source_kind="further_exploration",
                            asset_prefix=f"query_{index:02d}_page_{result_index:02d}",
                        )
                        page_summary["detail_path"] = analyzed_page.raw_path
                        page_summary["analysis_input_path"] = analyzed_page.analysis_input_path
                        page_summary["analysis_path"] = analyzed_page.analysis_path
                        if analyzed_page.extracted_text_path:
                            page_summary["extracted_text_path"] = analyzed_page.extracted_text_path
                        page_summary["signals"] = analyzed_page.signals
                        page_summary["analysis"] = analyzed_page.analysis
                        page_summary["document_type"] = analyzed_page.document_type
                        merge_signal_bundle(gathered_signals, analyzed_page.signals)

                        supporting_documents: list[dict[str, Any]] = []
                        for doc_index, resume_url in enumerate(list(analyzed_page.signals.get("resume_urls") or [])[:2], start=1):
                            normalized_resume_url = str(resume_url or "").strip()
                            if not normalized_resume_url or normalized_resume_url == str(analyzed_page.final_url or "").strip():
                                continue
                            try:
                                analyzed_resume = analyze_remote_document(
                                    candidate=candidate,
                                    target_company=target_company,
                                    source_url=normalized_resume_url,
                                    asset_dir=candidate_dir,
                                    asset_logger=logger,
                                    model_client=self.model_client,
                                    source_kind="further_exploration",
                                    asset_prefix=f"query_{index:02d}_page_{result_index:02d}_support_{doc_index:02d}",
                                )
                                supporting_documents.append(
                                    {
                                        "url": analyzed_resume.final_url,
                                        "document_type": analyzed_resume.document_type,
                                        "detail_path": analyzed_resume.raw_path,
                                        "analysis_input_path": analyzed_resume.analysis_input_path,
                                        "analysis_path": analyzed_resume.analysis_path,
                                        "extracted_text_path": analyzed_resume.extracted_text_path,
                                    }
                                )
                                merge_signal_bundle(gathered_signals, analyzed_resume.signals)
                            except Exception as exc:
                                supporting_documents.append({"url": normalized_resume_url, "error": str(exc)})
                        if supporting_documents:
                            page_summary["supporting_documents"] = supporting_documents
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

        merged_candidate, candidate_evidence = _merge_exploration(candidate, gathered_signals, candidate_dir, target_company=target_company)
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


def extract_page_signals(html_text: str, base_url: str) -> dict[str, Any]:
    return _extract_page_signals_impl(html_text, base_url)


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
def _is_fetchable_detail_page(url: str) -> bool:
    url = str(url or "").strip().lower()
    if not url.startswith("http"):
        return False
    blocked = ["duckduckgo.com", "google.com/search", "bing.com/search"]
    if is_binary_like_url(url):
        return False
    return not any(item in url for item in blocked)
def _merge_exploration(candidate: Candidate, signals: dict[str, Any], candidate_dir: Path, *, target_company: str) -> tuple[Candidate, list[EvidenceRecord]]:
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
        "exploration_education_signals": list(signals.get("education_signals") or [])[:6],
        "exploration_work_history_signals": list(signals.get("work_history_signals") or [])[:8],
        "exploration_affiliation_signals": list(signals.get("affiliation_signals") or [])[:6],
        "exploration_document_types": list(signals.get("document_types") or [])[:6],
    }
    candidate_patch = build_candidate_patch_from_signal_bundle(
        candidate,
        signals,
        target_company=target_company,
        source_url=str(candidate_dir),
    )
    incoming = Candidate(
        candidate_id=candidate.candidate_id,
        name_en=candidate.name_en,
        name_zh=candidate.name_zh,
        display_name=candidate.display_name,
        category=candidate.category,
        target_company=candidate.target_company,
        organization=str(candidate_patch.get("organization") or candidate.organization),
        employment_status=candidate.employment_status,
        role=str(candidate_patch.get("role") or candidate.role),
        team=candidate.team,
        focus_areas=str(candidate_patch.get("focus_areas") or candidate.focus_areas),
        education=str(candidate_patch.get("education") or candidate.education),
        work_history=str(candidate_patch.get("work_history") or candidate.work_history),
        notes=notes,
        linkedin_url=str(candidate_patch.get("linkedin_url") or candidate.linkedin_url or (signals["linkedin_urls"][0] if signals["linkedin_urls"] else "")),
        media_url=str(candidate_patch.get("media_url") or candidate.media_url or _pick_media_url(signals)),
        source_dataset=candidate.source_dataset,
        source_path=str(candidate_dir),
        metadata={**metadata, **dict(candidate_patch.get("metadata") or {})},
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
                        "education_signals": list(signals.get("education_signals") or [])[:4],
                        "work_history_signals": list(signals.get("work_history_signals") or [])[:4],
                    },
                )
            )
    return merged, evidence


def _pick_media_url(signals: dict[str, Any]) -> str:
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
    return _build_page_analysis_input_impl(
        candidate=candidate,
        target_company=target_company,
        source_url=source_url,
        html_text=html_text,
        extracted_links=extracted_links,
        max_excerpt_chars=max_excerpt_chars,
    )
