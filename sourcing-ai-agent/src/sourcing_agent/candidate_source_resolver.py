"""CandidateSourceResolver — Block (a) of the serving-mesh ownership boundary.

Extracted verbatim from ``orchestrator._resolve_job_candidate_source`` +
``orchestrator._build_job_results_context`` (WS2 slice 2, 2026-07-22; boundary
frozen in docs/SERVING_MESH_OWNERSHIP_BOUNDARY.md §2a, extraction pulled
forward by operator decision 2026-07-22). This module is the single owner of
candidate_source / result_view resolution. Read-model paging (dashboard /
candidate page) consumes ``build_results_context`` output and never re-resolves
(Edge B stays broken — pinned by tests/test_serving_mesh_boundary.py).

Dependency direction (frozen): the resolver reaches DOWN into store reads and
the injected resolution ladder only. It must never import the orchestrator,
the projection command bands (plan/enqueue/process/drain), or read-model
paging. Deeper ladder members (result_view stub/lifecycle/recovery families,
boundary doc §2a) still live in the orchestrator and are injected as explicit
callables; they migrate here in later slices without changing this interface.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .domain import JobRequest
from .retrieval_runtime import candidate_source_is_snapshot_authoritative
from datetime import datetime, timezone


def _utc_now_iso() -> str:
    # Replicated from orchestrator (trivial utility; import direction stays one-way).
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _coerce_int(value: Any, default: int) -> int:
    # Replicated from orchestrator (trivial utility; import direction stays one-way).
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    raw = str(value).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


class _ResolverStore(Protocol):
    """Store surface the resolver reads (never writes)."""

    def get_job(self, job_id: str) -> dict[str, Any] | None: ...

    def get_job_result_view(self, *, job_id: str) -> dict[str, Any] | None: ...

    def get_job_result_lifecycle(self, job_id: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class CandidateSourceResolverDeps:
    """Explicit dependency registry for the resolution ladder.

    Every member is a bound callable still owned by the orchestrator; the
    registry (not ad-hoc attribute reach-through) is the seam later slices
    shrink as ladder members migrate into this module.
    """

    apply_job_result_view_to_candidate_source: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]
    strip_superseded_asset_population_overlay_from_public_source: Callable[
        [dict[str, Any], dict[str, Any]], tuple[dict[str, Any], dict[str, Any]]
    ]
    project_canonical_lifecycle_payload: Callable[..., dict[str, Any]]
    candidate_source_has_manifest_invalid: Callable[[dict[str, Any]], bool]
    resolve_candidate_source_snapshot_dir: Callable[..., Path | None]
    candidate_source_materialized_path: Callable[..., Path | None]
    maybe_promote_workflow_job_to_completed_from_final_results: Callable[[dict[str, Any]], dict[str, Any]]
    load_workflow_stage_summaries: Callable[..., dict[str, Any]]
    stage1_progress_payload_from_lifecycle_row: Callable[..., dict[str, Any] | None]
    build_public_linkedin_stage1_progress_payload: Callable[..., dict[str, Any]]
    safe_list_persisted_job_workers: Callable[[str], list[dict[str, Any]]]
    organization_execution_profile_from_plan_payload: Callable[..., dict[str, Any]]
    build_effective_execution_semantics: Callable[..., dict[str, Any]]
    snapshot_publishable_email_lookup_for_request: Callable[..., dict[str, Any]]
    provider_execution_manifest_from_plan_payload: Callable[[dict[str, Any] | None], dict[str, Any]]


class CandidateSourceResolver:
    """Owner of ``resolve`` (frozen interface, boundary doc §2a) and the
    results-context assembly that read-model paging consumes."""

    def __init__(self, *, store: _ResolverStore, deps: CandidateSourceResolverDeps) -> None:
        self._store = store
        self._deps = deps

    def _candidate_source_result_view_stub(self, candidate_source: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(candidate_source or {})
        nested = dict(payload.get("result_view") or {})
        summary_payload = dict(nested.get("summary") or payload.get("result_view_summary") or {})
        metadata_payload = dict(nested.get("metadata") or payload.get("result_view_metadata") or {})
        if str(payload.get("asset_population_overlay_path") or "").strip():
            metadata_payload.setdefault(
                "asset_population_overlay_path",
                str(payload.get("asset_population_overlay_path") or "").strip(),
            )
        if dict(payload.get("asset_population_patch") or {}):
            metadata_payload.setdefault("asset_population_patch", dict(payload.get("asset_population_patch") or {}))
        stub = {
            "view_id": str(nested.get("view_id") or payload.get("result_view_id") or "").strip(),
            "job_id": str(nested.get("job_id") or payload.get("job_id") or "").strip(),
            "target_company": str(nested.get("target_company") or payload.get("target_company") or "").strip(),
            "source_kind": str(nested.get("source_kind") or payload.get("source_kind") or "").strip(),
            "view_kind": str(nested.get("view_kind") or payload.get("result_view_kind") or "").strip(),
            "snapshot_id": str(nested.get("snapshot_id") or payload.get("snapshot_id") or "").strip(),
            "asset_view": str(nested.get("asset_view") or payload.get("asset_view") or "canonical_merged").strip()
            or "canonical_merged",
            "source_path": str(nested.get("source_path") or payload.get("source_path") or "").strip(),
            "authoritative_snapshot_id": str(
                nested.get("authoritative_snapshot_id") or payload.get("authoritative_snapshot_id") or ""
            ).strip(),
            "materialization_generation_key": str(
                nested.get("materialization_generation_key") or payload.get("materialization_generation_key") or ""
            ).strip(),
            "request_signature": str(nested.get("request_signature") or payload.get("request_signature") or "").strip(),
            "summary": summary_payload,
            "metadata": metadata_payload,
        }
        if not any(
            [
                stub["view_id"],
                stub["source_kind"],
                stub["view_kind"],
                stub["snapshot_id"],
                stub["source_path"],
                stub["authoritative_snapshot_id"],
                stub["materialization_generation_key"],
            ]
        ):
            return {}
        return stub


    def _attach_persisted_lifecycle_to_candidate_source(
        self,
        *,
        job_id: str,
        candidate_source: dict[str, Any],
        result_view: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        normalized_job_id = str(job_id or "").strip()
        resolved = dict(candidate_source or {})
        result_view_payload = dict(result_view or {})
        if not normalized_job_id:
            return resolved, result_view_payload
        lifecycle_row = self._store.get_job_result_lifecycle(normalized_job_id)
        if str(dict(lifecycle_row or {}).get("source_validation_status") or "").strip() != "validated":
            return resolved, result_view_payload
        lifecycle_payload = self._deps.project_canonical_lifecycle_payload(row=dict(lifecycle_row or {}))
        metadata_payload = dict(resolved.get("result_view_metadata") or {})
        metadata_payload["result_view_lifecycle"] = lifecycle_payload
        resolved["result_view_metadata"] = metadata_payload
        resolved["result_view_lifecycle"] = lifecycle_payload
        if result_view_payload:
            view_metadata = dict(result_view_payload.get("metadata") or {})
            view_metadata["result_view_lifecycle"] = lifecycle_payload
            result_view_payload["metadata"] = view_metadata
            resolved["result_view"] = {
                **dict(resolved.get("result_view") or {}),
                "metadata": view_metadata,
            }
        resolved, result_view_payload = self._deps.strip_superseded_asset_population_overlay_from_public_source(
            resolved,
            result_view_payload,
        )
        return resolved, result_view_payload


    def _annotate_running_result_view_publication_gap(
        self,
        *,
        job_id: str,
        request: JobRequest,
        result_view: dict[str, Any],
        summary_payloads: list[dict[str, Any] | None],
    ) -> dict[str, Any]:
        """Report an in-flight serving gap without publishing from a public read."""

        view_payload = dict(result_view or {})
        served_snapshot_id = str(view_payload.get("snapshot_id") or "").strip()
        if not job_id or not served_snapshot_id:
            return view_payload

        detected_source: dict[str, Any] = {}
        for payload in list(summary_payloads or []):
            summary = dict(payload or {})
            candidates = [
                dict(summary.get("candidate_source") or {}),
                dict(summary.get("linkedin_stage_1") or {}),
                dict(summary.get("public_web_stage_2") or {}),
                dict(summary.get("background_snapshot_materialization") or {}),
                dict(dict(summary.get("stage1_preview") or {}).get("candidate_source") or {}),
            ]
            for source in candidates:
                snapshot_id = str(source.get("snapshot_id") or "").strip()
                if snapshot_id and snapshot_id != served_snapshot_id:
                    detected_source = source
                    break
            if detected_source:
                break

        current_snapshot_id = str(detected_source.get("snapshot_id") or "").strip()
        if not current_snapshot_id or current_snapshot_id == served_snapshot_id:
            return view_payload

        source_updated_at = ""
        for timestamp_key in ("completed_at", "updated_at", "finished_at", "materialized_at", "created_at"):
            source_updated_at = str(detected_source.get(timestamp_key) or "").strip()
            if source_updated_at:
                break
        metadata = dict(view_payload.get("metadata") or {})
        metadata["serving_publication_gap"] = {
            "status": "pending_event_time_publication",
            "reason": "running_public_read_does_not_publish_result_view",
            "job_id": str(job_id or "").strip(),
            "target_company": str(request.target_company or detected_source.get("target_company") or "").strip(),
            "served_snapshot_id": served_snapshot_id,
            "current_snapshot_id": current_snapshot_id,
            "source_kind": str(detected_source.get("source_kind") or "").strip(),
            "source_path": str(
                detected_source.get("source_path") or detected_source.get("candidate_doc_path") or ""
            ).strip(),
            "source_updated_at": source_updated_at,
            "observed_at": _utc_now_iso(),
            "candidate_count": _coerce_int(detected_source.get("candidate_count"), 0),
        }
        view_payload["metadata"] = metadata
        return view_payload


    def resolve(
        self,
        *,
        job: dict[str, Any] | None,
        request: JobRequest,
        job_summary: dict[str, Any] | None = None,
        stage1_preview_summary: dict[str, Any] | None = None,
        linkedin_stage_1_progress: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        summary_payload = dict(job_summary or {})
        preview_payload = dict(stage1_preview_summary or {})
        candidate_source = dict(
            summary_payload.get("candidate_source") or preview_payload.get("candidate_source") or {}
        )
        if str(request.target_company or "").strip() and not str(candidate_source.get("target_company") or "").strip():
            candidate_source["target_company"] = str(request.target_company or "").strip()
        result_view = self._candidate_source_result_view_stub(candidate_source)
        normalized_job_id = str(dict(job or {}).get("job_id") or "").strip()
        # Calibration vs the verbatim source: the original bound stored_view
        # only under `if normalized_job_id:` and read it unconditionally — an
        # empty job_id was a latent NameError (unreachable in practice). An
        # empty job_id now resolves with no stored view instead of crashing.
        stored_view = self._store.get_job_result_view(job_id=normalized_job_id) if normalized_job_id else None
        if stored_view:
            result_view = stored_view
        resolved_source = self._deps.apply_job_result_view_to_candidate_source(candidate_source, result_view)
        resolved_source, result_view = self._attach_persisted_lifecycle_to_candidate_source(
            job_id=normalized_job_id,
            candidate_source=resolved_source,
            result_view=result_view,
        )
        resolved_source, result_view = self._deps.strip_superseded_asset_population_overlay_from_public_source(
            resolved_source,
            result_view,
        )
        job_status = str(dict(job or {}).get("status") or "").strip().lower()
        if job_status != "completed":
            result_view = self._annotate_running_result_view_publication_gap(
                job_id=normalized_job_id,
                request=request,
                result_view=result_view,
                summary_payloads=[summary_payload, preview_payload, linkedin_stage_1_progress],
            )
            if result_view:
                resolved_source["result_view"] = dict(result_view)
        if candidate_source_is_snapshot_authoritative(resolved_source):
            if self._deps.candidate_source_has_manifest_invalid(resolved_source):
                if dict(resolved_source.get("result_view") or {}):
                    result_view = dict(resolved_source.get("result_view") or {})
                return resolved_source, dict(result_view or {})
            snapshot_dir = self._deps.resolve_candidate_source_snapshot_dir(
                request=request,
                candidate_source=resolved_source,
            )
            if snapshot_dir is not None:
                materialized_path = self._deps.candidate_source_materialized_path(
                    snapshot_dir=snapshot_dir,
                    asset_view=str(
                        resolved_source.get("asset_view") or request.asset_view or "canonical_merged"
                    ).strip()
                    or "canonical_merged",
                )
                if materialized_path is not None:
                    source_path = Path(str(resolved_source.get("source_path") or "").strip()).expanduser()
                    if not source_path.exists():
                        resolved_source["source_path"] = str(materialized_path)
                        if result_view:
                            result_view = {
                                **dict(result_view),
                                "source_path": str(materialized_path),
                            }
        return resolved_source, dict(result_view or {})

    def build_results_context(
        self,
        job_id: str,
        *,
        include_publishable_email_lookup: bool = True,
    ) -> dict[str, Any] | None:
        job = self._store.get_job(job_id)
        if job is None:
            return None
        job = self._deps.maybe_promote_workflow_job_to_completed_from_final_results(job)
        workflow_stage_summaries = self._deps.load_workflow_stage_summaries(job=job)
        plan_payload = dict(job.get("plan") or {})
        job_summary = dict(job.get("summary") or {})
        stage1_preview_summary = dict(
            dict(workflow_stage_summaries.get("summaries") or {}).get("stage_1_preview") or {}
        )
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        asset_reuse_plan = dict(plan_payload.get("asset_reuse_plan") or {})
        lifecycle_row = self._store.get_job_result_lifecycle(job_id)
        dynamic_stage1_progress = dict(job_summary.get("linkedin_stage_1") or {})
        linkedin_stage_1_progress = (
            self._deps.stage1_progress_payload_from_lifecycle_row(
                lifecycle_row,
                dynamic_progress=dynamic_stage1_progress,
            )
            or dynamic_stage1_progress
        )
        if not linkedin_stage_1_progress:
            # Stage 1 detail is a separate progress/debug line. It may be
            # reconstructed from persisted worker facts for legacy/reuse rows,
            # but board denominators still come only from job_result_lifecycle.
            linkedin_stage_1_progress = self._deps.build_public_linkedin_stage1_progress_payload(
                job=job,
                workers=self._deps.safe_list_persisted_job_workers(job_id),
            )
        lifecycle_stage1_progress = self._deps.stage1_progress_payload_from_lifecycle_row(
            lifecycle_row,
            dynamic_progress=linkedin_stage_1_progress,
        )
        if lifecycle_stage1_progress:
            linkedin_stage_1_progress = lifecycle_stage1_progress
        candidate_source, result_view = self.resolve(
            job=job,
            request=request,
            job_summary=job_summary,
            stage1_preview_summary=stage1_preview_summary,
            linkedin_stage_1_progress=linkedin_stage_1_progress,
        )
        organization_execution_profile = self._deps.organization_execution_profile_from_plan_payload(
            plan_payload=plan_payload,
            request=request,
            candidate_source=candidate_source,
        )
        effective_execution_semantics = self._deps.build_effective_execution_semantics(
            request=request,
            organization_execution_profile=organization_execution_profile,
            asset_reuse_plan=asset_reuse_plan,
            candidate_source=candidate_source,
        )
        publishable_email_lookup = (
            self._deps.snapshot_publishable_email_lookup_for_request(
                request=request,
                candidate_source=candidate_source,
            )
            if include_publishable_email_lookup
            else {}
        )
        return {
            "job": job,
            "workflow_stage_summaries": workflow_stage_summaries,
            "plan_payload": plan_payload,
            "provider_execution_manifest": self._deps.provider_execution_manifest_from_plan_payload(plan_payload),
            "job_summary": job_summary,
            "stage1_preview_summary": stage1_preview_summary,
            "request": request,
            "candidate_source": candidate_source,
            "result_view": result_view,
            "effective_execution_semantics": effective_execution_semantics,
            "publishable_email_lookup": publishable_email_lookup,
            "linkedin_stage_1_progress": linkedin_stage_1_progress,
        }
