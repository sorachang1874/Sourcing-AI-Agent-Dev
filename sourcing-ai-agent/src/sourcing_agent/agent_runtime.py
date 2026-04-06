from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from .domain import JobRequest


@dataclass(slots=True)
class AgentTraceHandle:
    session_id: int
    span_id: int
    lane_id: str
    span_name: str


@dataclass(slots=True)
class AgentWorkerHandle:
    session_id: int
    span_id: int
    worker_id: int
    lane_id: str
    worker_key: str


def build_specialist_lanes(request: JobRequest, plan_payload: dict[str, Any]) -> list[dict[str, Any]]:
    acquisition_strategy = dict(plan_payload.get("acquisition_strategy") or {})
    retrieval_plan = dict(plan_payload.get("retrieval_plan") or {})
    search_strategy = dict(plan_payload.get("search_strategy") or {})
    query_bundles = list(search_strategy.get("query_bundles") or [])
    source_families = sorted(
        {
            str(item.get("source_family") or "").strip()
            for item in query_bundles
            if isinstance(item, dict) and str(item.get("source_family") or "").strip()
        }
    )
    lanes = [
        {
            "lane_id": "triage_planner",
            "lane_type": "planner",
            "objective": "Interpret the request, define scope, and compile the sourcing plan.",
        },
        {
            "lane_id": "search_planner",
            "lane_type": "planner",
            "objective": "Design low-cost-first search bundles and escalation rules.",
            "source_families": source_families,
        },
        {
            "lane_id": "acquisition_specialist",
            "lane_type": "acquisition",
            "objective": "Acquire the baseline roster or search-seed population.",
            "strategy_type": str(acquisition_strategy.get("strategy_type") or ""),
        },
        {
            "lane_id": "enrichment_specialist",
            "lane_type": "enrichment",
            "objective": "Resolve profiles, merge public evidence, and promote weak leads when possible.",
        },
        {
            "lane_id": "exploration_specialist",
            "lane_type": "exploration",
            "objective": "Investigate unresolved leads through low-cost public web exploration and recover profile evidence.",
        },
        {
            "lane_id": "retrieval_specialist",
            "lane_type": "retrieval",
            "objective": "Apply structured filters, semantic retrieval, and confidence policy.",
            "retrieval_strategy": str(retrieval_plan.get("strategy") or "hybrid"),
        },
        {
            "lane_id": "review_specialist",
            "lane_type": "review",
            "objective": "Handle plan review, manual review queues, and criteria evolution feedback.",
        },
    ]
    if str(acquisition_strategy.get("strategy_type") or "") == "investor_firm_roster":
        lanes.append(
            {
                "lane_id": "investor_graph_specialist",
                "lane_type": "graph_acquisition",
                "objective": "Enumerate investor firms, tier them, and expand each firm's roster.",
            }
        )
    if "public_interviews" in source_families:
        lanes.append(
            {
                "lane_id": "public_media_specialist",
                "lane_type": "search_surface",
                "objective": "Score interview, podcast, and video results as reusable public-media assets.",
            }
        )
    return lanes


class AgentRuntimeCoordinator:
    def __init__(self, store) -> None:
        self.store = store

    def ensure_session(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan_payload: dict[str, Any],
        runtime_mode: str,
    ) -> dict[str, Any]:
        existing = self.store.get_agent_runtime_session(job_id=job_id)
        if existing is not None:
            return existing
        lanes = build_specialist_lanes(request, plan_payload)
        return self.store.create_agent_runtime_session(
            job_id=job_id,
            target_company=request.target_company,
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            lanes=lanes,
            metadata={"target_scope": request.target_scope},
        )

    @contextmanager
    def traced_lane(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan_payload: dict[str, Any],
        runtime_mode: str,
        lane_id: str,
        span_name: str,
        stage: str,
        input_payload: dict[str, Any] | None = None,
        parent_span_id: int = 0,
        handoff_from_lane: str = "",
    ) -> Iterator[AgentTraceHandle]:
        session = self.ensure_session(job_id=job_id, request=request, plan_payload=plan_payload, runtime_mode=runtime_mode)
        span = self.store.create_agent_trace_span(
            session_id=int(session["session_id"]),
            job_id=job_id,
            lane_id=lane_id,
            span_name=span_name,
            stage=stage,
            parent_span_id=parent_span_id,
            handoff_from_lane=handoff_from_lane,
            handoff_to_lane=lane_id if handoff_from_lane else "",
            input_payload=input_payload or {},
            metadata={"runtime_mode": runtime_mode},
        )
        handle = AgentTraceHandle(
            session_id=int(session["session_id"]),
            span_id=int(span["span_id"]),
            lane_id=lane_id,
            span_name=span_name,
        )
        try:
            yield handle
        except Exception as exc:
            self.store.complete_agent_trace_span(
                handle.span_id,
                status="failed",
                output_payload={"error": str(exc)},
            )
            raise

    def complete_span(
        self,
        handle: AgentTraceHandle,
        *,
        status: str,
        output_payload: dict[str, Any] | None = None,
        handoff_to_lane: str = "",
    ) -> dict[str, Any]:
        return self.store.complete_agent_trace_span(
            handle.span_id,
            status=status,
            output_payload=output_payload or {},
            handoff_to_lane=handoff_to_lane,
        )

    def begin_worker(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan_payload: dict[str, Any],
        runtime_mode: str,
        lane_id: str,
        worker_key: str,
        stage: str,
        span_name: str,
        budget_payload: dict[str, Any] | None = None,
        input_payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        handoff_from_lane: str = "",
    ) -> AgentWorkerHandle:
        session = self.ensure_session(job_id=job_id, request=request, plan_payload=plan_payload, runtime_mode=runtime_mode)
        span = self.store.create_agent_trace_span(
            session_id=int(session["session_id"]),
            job_id=job_id,
            lane_id=lane_id,
            span_name=span_name,
            stage=stage,
            handoff_from_lane=handoff_from_lane,
            handoff_to_lane=lane_id if handoff_from_lane else "",
            input_payload=input_payload or {},
            metadata={"runtime_mode": runtime_mode, **dict(metadata or {})},
        )
        worker = self.store.create_or_resume_agent_worker(
            session_id=int(session["session_id"]),
            job_id=job_id,
            span_id=int(span["span_id"]),
            lane_id=lane_id,
            worker_key=worker_key,
            budget_payload=budget_payload or {},
            input_payload=input_payload or {},
            metadata=metadata or {},
        )
        self.store.clear_interrupt_agent_worker(int(worker["worker_id"]))
        self.store.mark_agent_worker_running(int(worker["worker_id"]))
        return AgentWorkerHandle(
            session_id=int(session["session_id"]),
            span_id=int(span["span_id"]),
            worker_id=int(worker["worker_id"]),
            lane_id=lane_id,
            worker_key=worker_key,
        )

    def checkpoint_worker(
        self,
        handle: AgentWorkerHandle,
        *,
        checkpoint_payload: dict[str, Any] | None = None,
        output_payload: dict[str, Any] | None = None,
        status: str = "running",
    ) -> dict[str, Any] | None:
        return self.store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload=checkpoint_payload or {},
            output_payload=output_payload or {},
            status=status,
        )

    def get_worker(self, worker_id: int) -> dict[str, Any] | None:
        return self.store.get_agent_worker(worker_id=worker_id)

    def should_interrupt_worker(self, handle: AgentWorkerHandle) -> bool:
        worker = self.get_worker(handle.worker_id)
        return bool((worker or {}).get("interrupt_requested"))

    def complete_worker(
        self,
        handle: AgentWorkerHandle,
        *,
        status: str,
        checkpoint_payload: dict[str, Any] | None = None,
        output_payload: dict[str, Any] | None = None,
        handoff_to_lane: str = "",
    ) -> dict[str, Any] | None:
        self.store.complete_agent_trace_span(
            handle.span_id,
            status=status,
            output_payload=output_payload or {},
            handoff_to_lane=handoff_to_lane,
        )
        return self.store.complete_agent_worker(
            handle.worker_id,
            status=status,
            checkpoint_payload=checkpoint_payload or {},
            output_payload=output_payload or {},
        )

    def list_workers(self, *, job_id: str = "", session_id: int = 0, lane_id: str = "") -> list[dict[str, Any]]:
        return self.store.list_agent_workers(job_id=job_id, session_id=session_id, lane_id=lane_id)

    def interrupt_worker(self, worker_id: int) -> dict[str, Any] | None:
        worker = self.store.request_interrupt_agent_worker(worker_id)
        if worker is None:
            return None
        span_id = int(worker.get("span_id") or 0)
        if span_id:
            self.store.complete_agent_trace_span(
                span_id,
                status="interrupt_requested",
                output_payload={"interrupt_requested": True},
            )
        return self.store.get_agent_worker(worker_id=worker_id)
