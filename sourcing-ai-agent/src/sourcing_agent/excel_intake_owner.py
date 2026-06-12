"""Excel intake domain owner extracted from ``SourcingOrchestrator`` (Phase 3).

Start/continue entry points, prepared-contact-batch contract persistence,
queue/plan/run/drain of ``excel.intake.run`` commands, the command thread
body, cancel/resume lifecycle, job-state/row-manifest persistence, artifact
repair, progress payloads, and stale-job recovery for the Excel intake
cluster.  Bodies are moved verbatim from ``orchestrator.py``; the only body
edit is kernel-wrapper calls ``self._x(...)`` -> ``self._kernel._x(...)``.
``SourcingOrchestrator`` keeps signature-identical delegating wrappers for
every moved method, and injects its shared spine helpers as bound callables
stored under the same attribute names so moved bodies stay verbatim.

``_run_excel_intake_workflow`` (the workflow domain body) intentionally stays
on the orchestrator: it leans on the shared retrieval/asset-population spine
(``_build_effective_execution_semantics``, ``_execute_asset_population_fast_path``,
``_load_retrieval_candidate_source``, ``_persist_workflow_stage_summary_file``,
``_sync_frontend_history_phase_for_job``, ``mark_job_result_lifecycle_terminal``)
and existing tests patch it on the orchestrator instance; the owner reaches it
through a re-resolving injected callable under the same attribute name.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .command_kernel import CommandKernel
from .company_registry import normalize_company_key
from .domain import JobRequest
from .durable_runtime import (
    EXCEL_INTAKE_RUN_COMMAND_TYPE,
    EXCEL_INTAKE_RUN_OWNER,
    default_readiness_effect_for_command_type,
    excel_intake_run_idempotency_key,
)
from .excel_intake import ExcelIntakeService, group_contacts_by_company_hints
from .public_candidate_facets import (
    EXCEL_INTAKE_CURRENT_JOB_MARKER_ID,
    EXCEL_INTAKE_CURRENT_JOB_MARKER_LABEL,
)
from .storage import _json_safe_payload as _storage_json_safe_payload


# NOTE: the helpers below duplicate small module-level helpers in
# ``orchestrator.py`` (which imports this module — importing them back from
# orchestrator would create a cycle).  The bodies are copied verbatim; several
# other ``sourcing_agent`` modules already carry the same local copies.


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(value)


def _coerce_int(value: Any, default: int) -> int:
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


def _dedupe_texts(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = " ".join(str(item or "").split()).strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
    return result


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_timestamp(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class _ExcelIntakeCommandCancelled(RuntimeError):
    """Raised inside the Excel owner thread when command control cancels it."""


class ExcelIntakeOwner:
    """Owner of the Excel intake command/workflow cluster (Phase 3 extraction)."""

    def __init__(
        self,
        *,
        store: Any,
        command_kernel: CommandKernel,
        durable_runtime_writer: Any,
        runtime_dir: Path | str,
        jobs_dir: Path | str,
        acquisition_engine: Any,
        model_client: Any,
        persist_frontend_history_link: Callable[..., Any],
        sync_operation_run_from_workflow_command_control: Callable[..., dict[str, Any]],
        normalized_job_scoped_candidate_markers: Callable[..., Any],
        enqueue_snapshot_full_materialization_item: Callable[..., Any],
        run_snapshot_full_materialization_queue_once: Callable[..., Any],
        snapshot_full_materialization_item_id: Callable[..., Any],
        job_run_lock: Callable[..., Any],
        run_excel_intake_workflow: Callable[..., Any],
    ) -> None:
        self.store = store
        self._kernel = command_kernel
        self.durable_runtime_writer = durable_runtime_writer
        self.runtime_dir = runtime_dir
        self.jobs_dir = jobs_dir
        self.acquisition_engine = acquisition_engine
        self.model_client = model_client
        # Injected cross-domain/spine callables, stored under the same names the
        # moved bodies already use so the bodies stay verbatim.
        self._persist_frontend_history_link = persist_frontend_history_link
        self._sync_operation_run_from_workflow_command_control = sync_operation_run_from_workflow_command_control
        self._normalized_job_scoped_candidate_markers = normalized_job_scoped_candidate_markers
        self._enqueue_snapshot_full_materialization_item = enqueue_snapshot_full_materialization_item
        self._run_snapshot_full_materialization_queue_once = run_snapshot_full_materialization_queue_once
        self._snapshot_full_materialization_item_id = snapshot_full_materialization_item_id
        self._job_run_lock = job_run_lock
        # The excel workflow domain body stays orchestrator-side (see module
        # docstring); stored under the same attribute name so the moved thread
        # body calls it verbatim.
        self._run_excel_intake_workflow = run_excel_intake_workflow

    def continue_excel_intake_review(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            service = ExcelIntakeService(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.acquisition_engine.settings,
                model_client=self.model_client,
            )
            return service.continue_review(payload)
        except Exception as exc:
            return {
                "status": "invalid",
                "reason": str(exc),
            }

    def start_excel_intake_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        if (
            not str(payload.get("file_path") or "").strip()
            and not str(payload.get("file_content_base64") or "").strip()
        ):
            return {
                "status": "invalid",
                "reason": "file_path or file_content_base64 is required",
            }

        normalized_payload = dict(payload)
        explicit_target_company = str(normalized_payload.get("target_company") or "").strip()
        parent_history_id = str(normalized_payload.get("history_id") or "").strip()
        attach_to_snapshot = _coerce_bool(normalized_payload.get("attach_to_snapshot"), True)
        build_artifacts = _coerce_bool(normalized_payload.get("build_artifacts"), True)
        filename = str(normalized_payload.get("filename") or "").strip()
        batch_id = uuid.uuid4().hex[:12]
        service = ExcelIntakeService(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.acquisition_engine.settings,
            model_client=self.model_client,
        )
        prepare_dir = self.runtime_dir / "excel_intake_batches" / batch_id
        prepare_dir.mkdir(parents=True, exist_ok=True)
        try:
            prepared_contact_batch = service.prepare_contacts(normalized_payload, intake_dir=prepare_dir)
        except Exception as exc:
            return {
                "status": "invalid",
                "reason": str(exc),
            }

        prepared_contacts = [
            dict(item) for item in list(prepared_contact_batch.get("contacts") or []) if isinstance(item, dict)
        ]
        if not prepared_contacts:
            return {
                "status": "invalid",
                "reason": "no_contacts_detected",
            }

        grouped_contacts: dict[str, Any]
        if explicit_target_company:
            grouped_contacts = {
                "groups": [
                    {
                        "company": explicit_target_company,
                        "company_key": normalize_company_key(explicit_target_company),
                        "contacts": [
                            {
                                **contact,
                                "company": explicit_target_company,
                                "route_target_company": explicit_target_company,
                                "uploaded_company": str(contact.get("company") or "").strip(),
                                "company_hints": [explicit_target_company],
                            }
                            for contact in prepared_contacts
                        ],
                        "source_companies": sorted(
                            {
                                str(contact.get("company") or "").strip()
                                for contact in prepared_contacts
                                if str(contact.get("company") or "").strip()
                            }
                        ),
                        "row_count": len(prepared_contacts),
                    }
                ],
                "unassigned_contacts": [],
                "unassigned_row_count": 0,
            }
        else:
            grouped_contacts = group_contacts_by_company_hints(prepared_contacts)

        group_items = grouped_contacts.get("groups")
        if not isinstance(group_items, list):
            group_items = []
        groups = [
            dict(item) for item in group_items if isinstance(item, dict) and str(item.get("company") or "").strip()
        ]
        if not groups:
            return {
                "status": "invalid",
                "reason": "no_company_groups_detected",
                "unassigned_row_count": _coerce_int(grouped_contacts.get("unassigned_row_count"), 0),
            }

        queued_groups: list[dict[str, Any]] = []
        for group in groups:
            target_company = str(group.get("company") or "").strip()
            group_history_id = parent_history_id if len(groups) == 1 and parent_history_id else str(uuid.uuid4())
            group_query_text = (
                str(normalized_payload.get("query_text") or "").strip()
                if explicit_target_company and len(groups) == 1
                else ""
            ) or f"Excel 批量导入 {target_company} 候选人"
            queued = self._queue_excel_intake_workflow_job(
                target_company=target_company,
                history_id=group_history_id,
                parent_history_id=parent_history_id,
                query_text=group_query_text,
                filename=filename,
                attach_to_snapshot=attach_to_snapshot,
                build_artifacts=build_artifacts,
                batch_id=batch_id,
                source_companies=[
                    str(item or "").strip()
                    for item in list(group.get("source_companies") or [])
                    if str(item or "").strip()
                ],
                prepared_contact_batch={
                    "workbook": dict(prepared_contact_batch.get("workbook") or {}),
                    "schema_inference": dict(prepared_contact_batch.get("schema_inference") or {}),
                    "contacts": [dict(item) for item in list(group.get("contacts") or []) if isinstance(item, dict)],
                },
            )
            queued["row_count"] = int(group.get("row_count") or 0)
            queued["source_companies"] = [
                str(item or "").strip() for item in list(group.get("source_companies") or []) if str(item or "").strip()
            ]
            queued_groups.append(queued)

        unassigned_contact_items = grouped_contacts.get("unassigned_contacts")
        if not isinstance(unassigned_contact_items, list):
            unassigned_contact_items = []
        response = {
            "status": "queued",
            "workflow_kind": "excel_intake_batch"
            if (len(queued_groups) > 1 or not explicit_target_company)
            else "excel_intake",
            "batch_id": batch_id,
            "input_filename": filename,
            "total_row_count": int(
                dict(prepared_contact_batch.get("workbook") or {}).get("detected_contact_row_count") or 0
            ),
            "created_job_count": len(queued_groups),
            "group_count": len(queued_groups),
            "unassigned_row_count": _coerce_int(grouped_contacts.get("unassigned_row_count"), 0),
            "unassigned_rows": [
                {
                    "row_key": str(item.get("row_key") or "").strip(),
                    "name": str(item.get("name") or "").strip(),
                    "company": str(item.get("company") or "").strip(),
                    "title": str(item.get("title") or "").strip(),
                }
                for item in unassigned_contact_items[:12]
                if isinstance(item, dict)
            ],
            "groups": queued_groups,
        }
        if len(queued_groups) == 1:
            first_group = queued_groups[0]
            response["job_id"] = str(first_group.get("job_id") or "")
            response["history_id"] = str(first_group.get("history_id") or "")
            response["query_text"] = str(first_group.get("query_text") or "")
            response["workflow_command"] = dict(first_group.get("workflow_command") or {})
            response["command_result"] = dict(first_group.get("command_result") or {})
        return response

    def _persist_excel_intake_prepared_batch_contract(
        self,
        *,
        job_id: str,
        batch_id: str,
        target_company: str,
        filename: str,
        prepared_contact_batch: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        normalized_batch_id = str(batch_id or "").strip() or normalized_job_id
        if not normalized_job_id:
            return {}
        prepared_dir = self.runtime_dir / "excel_intake_batches" / normalized_batch_id / normalized_job_id
        prepared_dir.mkdir(parents=True, exist_ok=True)
        contacts = [
            dict(item)
            for item in list(dict(prepared_contact_batch or {}).get("contacts") or [])
            if isinstance(item, dict)
        ]
        prepared_payload = {
            "schema_version": 1,
            "job_id": normalized_job_id,
            "batch_id": normalized_batch_id,
            "target_company": str(target_company or "").strip(),
            "input_filename": str(filename or "").strip(),
            "prepared_at": _utc_now_iso(),
            "prepared_contact_batch": {
                "workbook": dict(dict(prepared_contact_batch or {}).get("workbook") or {}),
                "schema_inference": dict(dict(prepared_contact_batch or {}).get("schema_inference") or {}),
                "contacts": contacts,
            },
        }
        batch_path = prepared_dir / "prepared_contact_batch.json"
        batch_path.write_text(
            json.dumps(_storage_json_safe_payload(prepared_payload), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        payload_hash = hashlib.sha256(batch_path.read_bytes()).hexdigest()
        manifest = {
            "schema_version": 1,
            "job_id": normalized_job_id,
            "batch_id": normalized_batch_id,
            "target_company": str(target_company or "").strip(),
            "input_filename": str(filename or "").strip(),
            "prepared_contact_batch_path": str(batch_path),
            "prepared_contact_count": len(contacts),
            "payload_sha256": payload_hash,
            "prepared_at": str(prepared_payload.get("prepared_at") or ""),
        }
        manifest_path = prepared_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(_storage_json_safe_payload(manifest), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {
            "prepared_contact_batch_path": str(batch_path),
            "prepared_contact_batch_manifest_path": str(manifest_path),
            "prepared_contact_count": len(contacts),
            "prepared_contact_batch_sha256": payload_hash,
            "prepared_at": str(prepared_payload.get("prepared_at") or ""),
        }

    def _build_excel_intake_execution_bundle(
        self,
        *,
        batch_id: str,
        target_company: str,
        history_id: str,
        parent_history_id: str,
        query_text: str,
        filename: str,
        attach_to_snapshot: bool,
        build_artifacts: bool,
        source_companies: list[str],
        prepared_batch_contract: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "workflow_kind": "excel_intake",
            "excel_intake": {
                "schema_version": 1,
                "state": "prepared",
                "batch_id": str(batch_id or "").strip(),
                "target_company": str(target_company or "").strip(),
                "history_id": str(history_id or "").strip(),
                "parent_history_id": str(parent_history_id or "").strip(),
                "query_text": str(query_text or "").strip(),
                "input_filename": str(filename or "").strip(),
                "attach_to_snapshot": bool(attach_to_snapshot),
                "build_artifacts": bool(build_artifacts),
                "source_companies": [
                    str(item or "").strip()
                    for item in list(source_companies or [])
                    if str(item or "").strip()
                ],
                **dict(prepared_batch_contract or {}),
            },
        }

    def _load_excel_intake_prepared_contact_batch_from_path(self, path_value: str) -> dict[str, Any]:
        path = Path(str(path_value or "").strip()).expanduser()
        if not path.exists() or not path.is_file():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        prepared = dict(payload.get("prepared_contact_batch") or payload)
        contacts = [dict(item) for item in list(prepared.get("contacts") or []) if isinstance(item, dict)]
        if not contacts:
            return {}
        return {
            "workbook": dict(prepared.get("workbook") or {}),
            "schema_inference": dict(prepared.get("schema_inference") or {}),
            "contacts": contacts,
        }

    def _excel_intake_payload_from_execution_bundle(
        self,
        *,
        job: dict[str, Any],
        fallback_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        fallback_payload = dict(fallback_payload or {})
        execution_bundle = dict(job.get("execution_bundle") or {})
        excel_bundle = dict(execution_bundle.get("excel_intake") or {})
        prepared_batch_path = str(
            fallback_payload.get("prepared_contact_batch_path")
            or excel_bundle.get("prepared_contact_batch_path")
            or ""
        ).strip()
        prepared_batch = dict(fallback_payload.get("prepared_contact_batch") or {})
        if not prepared_batch and prepared_batch_path:
            prepared_batch = self._load_excel_intake_prepared_contact_batch_from_path(prepared_batch_path)
        request_payload = dict(job.get("request") or {})
        payload = {
            "filename": str(
                fallback_payload.get("filename")
                or excel_bundle.get("input_filename")
                or request_payload.get("filename")
                or ""
            ).strip(),
            "attach_to_snapshot": _coerce_bool(
                fallback_payload.get("attach_to_snapshot", excel_bundle.get("attach_to_snapshot", True)),
                True,
            ),
            "build_artifacts": _coerce_bool(
                fallback_payload.get("build_artifacts", excel_bundle.get("build_artifacts", True)),
                True,
            ),
            "target_company": str(
                fallback_payload.get("target_company")
                or excel_bundle.get("target_company")
                or request_payload.get("target_company")
                or ""
            ).strip(),
            "query_text": str(
                fallback_payload.get("query_text")
                or excel_bundle.get("query_text")
                or request_payload.get("raw_user_request")
                or request_payload.get("query")
                or ""
            ).strip(),
            "prepared_contact_batch_path": prepared_batch_path,
        }
        if prepared_batch:
            payload["prepared_contact_batch"] = prepared_batch
        return payload

    def _queue_excel_intake_workflow_job(
        self,
        *,
        target_company: str,
        history_id: str,
        parent_history_id: str,
        query_text: str,
        filename: str,
        attach_to_snapshot: bool,
        build_artifacts: bool,
        batch_id: str,
        source_companies: list[str],
        prepared_contact_batch: dict[str, Any],
    ) -> dict[str, Any]:
        request = JobRequest.from_payload(
            {
                "raw_user_request": query_text,
                "query": query_text,
                "target_company": target_company,
                "target_scope": "full_company_asset",
                "asset_view": "canonical_merged",
                "retrieval_strategy": "asset_population",
                "planning_mode": "excel_intake",
                "analysis_stage_mode": "single_stage",
                "employment_statuses": ["current", "former"],
            }
        )
        job_id = uuid.uuid4().hex[:12]
        prepared_contacts = [
            dict(item) for item in list(prepared_contact_batch.get("contacts") or []) if isinstance(item, dict)
        ]
        prepared_batch_contract = self._persist_excel_intake_prepared_batch_contract(
            job_id=job_id,
            batch_id=batch_id,
            target_company=target_company,
            filename=filename,
            prepared_contact_batch=prepared_contact_batch,
        )
        execution_bundle = self._build_excel_intake_execution_bundle(
            batch_id=batch_id,
            target_company=target_company,
            history_id=history_id,
            parent_history_id=parent_history_id,
            query_text=query_text,
            filename=filename,
            attach_to_snapshot=attach_to_snapshot,
            build_artifacts=build_artifacts,
            source_companies=source_companies,
            prepared_batch_contract=prepared_batch_contract,
        )
        initial_stage_payload = {
            "status": "running",
            "title": "Excel 上传",
            "text": "正在解析 Excel 文件并对联系人做本地去重。",
            "workflow_kind": "excel_intake",
            "target_company": target_company,
            "input_filename": filename,
            "row_count": len(prepared_contacts),
            "started_at": _utc_now_iso(),
        }
        initial_summary = {
            "workflow_kind": "excel_intake",
            "message": "Excel intake queued.",
            "target_company": target_company,
            "input_filename": filename,
            "attach_to_snapshot": attach_to_snapshot,
            "build_artifacts": build_artifacts,
            "batch_id": batch_id,
            "row_count": len(prepared_contacts),
            "linkedin_stage_1": initial_stage_payload,
        }
        self._save_excel_intake_job_state(
            job_id=job_id,
            request=request,
            status="queued",
            stage="acquiring",
            summary_payload=initial_summary,
            execution_bundle_payload=execution_bundle,
        )
        self.store.append_job_event(
            job_id,
            "acquiring",
            "queued",
            "Excel intake workflow created.",
            {
                "workflow_kind": "excel_intake",
                "target_company": target_company,
                "input_filename": filename,
                "row_count": len(prepared_contacts),
                "batch_id": batch_id,
            },
        )
        self._persist_frontend_history_link(
            history_id=history_id,
            query_text=query_text,
            target_company=target_company,
            job_id=job_id,
            phase="running",
            request_payload=request.to_record(),
            plan_payload=None,
            metadata={
                "source": "excel_intake_workflow",
                "workflow_kind": "excel_intake",
                "attach_to_snapshot": attach_to_snapshot,
                "build_artifacts": build_artifacts,
                "input_filename": filename,
                "batch_id": batch_id,
                "parent_history_id": parent_history_id,
                "row_count": len(prepared_contacts),
                "source_companies": source_companies,
            },
        )
        command = self._plan_excel_intake_run_command(
            job_id=job_id,
            request=request,
            payload={
                "filename": filename,
                "attach_to_snapshot": attach_to_snapshot,
                "build_artifacts": build_artifacts,
                "target_company": target_company,
                "query_text": query_text,
                "batch_id": batch_id,
                "history_id": history_id,
                "parent_history_id": parent_history_id,
                "source_companies": source_companies,
                "prepared_contact_batch_path": str(prepared_batch_contract.get("prepared_contact_batch_path") or ""),
                "prepared_contact_count": len(prepared_contacts),
            },
        )
        command_result = self._run_excel_intake_run_command(command) if command else {
            "status": "failed",
            "reason": "excel_intake_run_command_enqueue_failed",
            "command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE,
            "owner": EXCEL_INTAKE_RUN_OWNER,
            "report_visible": True,
        }
        command_observation = dict(command_result.get("workflow_command") or {}) if isinstance(command_result, dict) else {}
        if not command_observation:
            command_observation = self._kernel._workflow_command_observation(
                command,
                migration_phase="W7_excel_intake_run_command_owner",
            )
        return {
            "status": "queued",
            "job_id": job_id,
            "history_id": history_id,
            "query_text": query_text,
            "workflow_kind": "excel_intake",
            "target_company": target_company,
            "workflow_command": command_observation,
            "command_result": command_result,
        }

    def _excel_intake_workflow_run_id(self, job_id: str) -> str:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return ""
        return f"wf_excel_intake_{normalized_job_id}"

    def _excel_intake_operation_id(self, job_id: str) -> str:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return ""
        return f"op_excel_intake_{normalized_job_id}"

    def _plan_excel_intake_run_command(
        self,
        *,
        job_id: str,
        request: JobRequest,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        command_payload = dict(payload or {})
        if not normalized_job_id:
            return {}
        workflow_run_id = self._excel_intake_workflow_run_id(normalized_job_id)
        operation_id = self._excel_intake_operation_id(normalized_job_id)
        idempotency_key = excel_intake_run_idempotency_key(
            job_id=normalized_job_id,
            batch_id=str(command_payload.get("batch_id") or ""),
            target_company=str(command_payload.get("target_company") or request.target_company or ""),
            prepared_contact_batch_path=str(command_payload.get("prepared_contact_batch_path") or ""),
            run_scope=str(command_payload.get("run_scope") or ""),
        )
        if not workflow_run_id or not operation_id or not idempotency_key:
            return {}
        command_payload.update(
            {
                "job_id": normalized_job_id,
                "job_ids": [normalized_job_id],
                "job_count": 1,
                "request": request.to_record(),
                "row_count": int(command_payload.get("prepared_contact_count") or 0),
                "materialization_metadata": {
                    "command_payload_storage": "workflow_commands",
                    "write_owner": EXCEL_INTAKE_RUN_OWNER,
                    "migration_phase": "W7_excel_intake_run_command_owner",
                },
            }
        )
        try:
            self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="WorkflowStarted",
                idempotency_key=f"{workflow_run_id}:excel_intake_started",
                actor="excel_intake_planner",
                source="excel_intake",
                payload={
                    "workflow_type": "excel_intake",
                    "stage_key": "excel_intake",
                    "job_id": normalized_job_id,
                    "target_company": str(request.target_company or ""),
                    "migration_phase": "W7_excel_intake_run_command_owner",
                },
            )
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor="excel_intake_planner",
                source="excel_intake",
                payload={
                    "workflow_type": "excel_intake",
                    "stage_key": "excel_intake",
                    "command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE,
                    "idempotency_key": idempotency_key,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "excel_intake_run",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _run_excel_intake_run_command(self, command: dict[str, Any]) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        if not command_id:
            return {"status": "failed", "reason": "excel_intake_run_command_id_missing"}
        latest_status = str(command_payload.get("status") or "").strip()
        if latest_status == "succeeded":
            return {
                "status": "completed",
                "reason": "excel_intake_run_command_already_succeeded",
                "workflow_command": self._kernel._workflow_command_observation(
                    command_payload,
                    migration_phase="W7_excel_intake_run_command_owner",
                ),
            }
        lease_owner = f"{EXCEL_INTAKE_RUN_OWNER}-{uuid.uuid4().hex[:8]}"
        claimed = self.store.claim_workflow_command(command_id, lease_owner=lease_owner, lease_seconds=300)
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            return {
                "status": "skipped",
                "reason": "excel_intake_run_command_not_claimed",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W7_excel_intake_run_command_owner",
                ),
            }
        self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner)
        latest_command = self.store.get_workflow_command(command_id) or claimed
        payload = dict(latest_command.get("payload") or {})
        job_id = str(payload.get("job_id") or "").strip()
        request_payload = dict(payload.get("request") or {})
        if not job_id or not request_payload:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text="excel_intake_run_command_missing_job_or_request",
                retryable=False,
            )
            return {
                "status": "failed",
                "reason": "excel_intake_run_command_missing_job_or_request",
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or latest_command,
                    migration_phase="W7_excel_intake_run_command_owner",
                ),
            }
        request = JobRequest.from_payload(request_payload)
        activity, attempt = self._kernel._start_workflow_command_activity_attempt(
            latest_command,
            activity_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
            owner=EXCEL_INTAKE_RUN_OWNER,
            phase="excel_intake_started",
            lease_owner=lease_owner,
            provider="excel_intake_local",
            provider_request_ref=job_id,
            input_payload={
                "job_id": job_id,
                "filename": str(payload.get("filename") or ""),
                "target_company": str(payload.get("target_company") or request.target_company or ""),
                "query_text": str(payload.get("query_text") or request.query or ""),
                "prepared_contact_batch_path": str(payload.get("prepared_contact_batch_path") or ""),
            },
            entity_counts={
                "excel_intake_job_count": 1,
                "prepared_contact_count": int(payload.get("prepared_contact_count") or payload.get("row_count") or 0),
            },
            metadata={
                "async_thread_owner": True,
                "migration_phase": "W7_excel_intake_run_command_owner",
            },
            attempt_suffix="excel_intake_run",
        )
        thread = threading.Thread(
            target=self._run_excel_intake_workflow_command_thread,
            kwargs={
                "command_id": command_id,
                "job_id": job_id,
                "request": request,
                "activity": activity,
                "attempt": attempt,
                "payload": {
                    "workflow_command_id": command_id,
                    "filename": str(payload.get("filename") or ""),
                    "attach_to_snapshot": _coerce_bool(payload.get("attach_to_snapshot"), True),
                    "build_artifacts": _coerce_bool(payload.get("build_artifacts"), True),
                    "target_company": str(payload.get("target_company") or request.target_company or ""),
                    "query_text": str(payload.get("query_text") or request.query or ""),
                    "prepared_contact_batch_path": str(payload.get("prepared_contact_batch_path") or ""),
                },
            },
            name=f"excel-intake-{job_id}",
            daemon=True,
        )
        thread.start()
        return {
            "status": "queued",
            "reason": "excel_intake_run_command_owner",
            "job_id": job_id,
            "thread_name": thread.name,
            "workflow_command": self._kernel._workflow_command_observation(
                self.store.get_workflow_command(command_id) or latest_command,
                migration_phase="W7_excel_intake_run_command_owner",
            ),
        }

    def _record_excel_intake_command_cancelled_terminal(
        self,
        *,
        command_id: str,
        job_id: str,
        activity: dict[str, Any] | None = None,
        attempt: dict[str, Any] | None = None,
        reason: str = "excel_intake_run_cancelled_by_command_control",
        actor: str = EXCEL_INTAKE_RUN_OWNER,
    ) -> dict[str, Any]:
        normalized_command_id = str(command_id or "").strip()
        normalized_job_id = str(job_id or "").strip()
        command = self.store.get_workflow_command(normalized_command_id) if normalized_command_id else {}
        if not command:
            return {"status": "not_found", "command_id": normalized_command_id}
        command_payload = dict(command.get("payload") or {})
        request_payload = dict(command_payload.get("request") or {})
        request = JobRequest.from_payload(request_payload) if request_payload else JobRequest(raw_user_request="")
        cancel_reason = str(reason or "excel_intake_run_cancelled_by_command_control").strip()
        existing_job = self.store.get_job(normalized_job_id) if normalized_job_id else {}
        existing_summary = dict(dict(existing_job or {}).get("summary") or {})
        if normalized_job_id:
            cancelled_at = _utc_now_iso()
            self._save_excel_intake_job_state(
                job_id=normalized_job_id,
                request=request,
                status="cancelled",
                stage="cancelled",
                summary_payload={
                    **existing_summary,
                    "workflow_kind": "excel_intake",
                    "message": "Excel intake was cancelled by command control.",
                    "cancelled_at": cancelled_at,
                    "cancelled_reason": cancel_reason,
                    "workflow_command_id": normalized_command_id,
                },
            )
            self.store.append_job_event(
                normalized_job_id,
                "cancelled",
                "cancelled",
                "Excel intake command cancelled.",
                {
                    "workflow_kind": "excel_intake",
                    "workflow_command_id": normalized_command_id,
                    "reason": cancel_reason,
                    "actor": str(actor or EXCEL_INTAKE_RUN_OWNER).strip() or EXCEL_INTAKE_RUN_OWNER,
                },
            )
        current_status = str(command.get("status") or "").strip().lower()
        updated = command
        if current_status not in {"cancelled", "canceled"}:
            updated = self.store.cancel_workflow_command(
                normalized_command_id,
                reason=cancel_reason,
                actor=str(actor or EXCEL_INTAKE_RUN_OWNER).strip() or EXCEL_INTAKE_RUN_OWNER,
                result={
                    "control_source": "api.workflow_command_owner_specific_cancel",
                    "control_action": "cancel",
                    "owner_specific_control": True,
                    "job_id": normalized_job_id,
                    "activity_run_id": str(dict(activity or {}).get("activity_run_id") or "").strip(),
                    "activity_attempt_id": str(dict(attempt or {}).get("attempt_id") or "").strip(),
                    "thread_terminal_checkpoint": True,
                    "module_state_mutated": True,
                },
                from_statuses=("claimed", "running"),
            ) or command
        if not activity or not attempt:
            activity_rows = self.store.list_workflow_activity_runs(
                command_id=normalized_command_id,
                activity_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
                limit=1,
            )
            activity = dict(activity_rows[0]) if activity_rows else {}
            attempts = (
                self.store.list_workflow_activity_attempts(
                    activity_run_id=str(activity.get("activity_run_id") or ""),
                    limit=1,
                )
                if activity
                else []
            )
            attempt = dict(attempts[0]) if attempts else {}
        if not activity or not attempt:
            activity, attempt = self._kernel._start_workflow_command_activity_attempt(
                updated or command,
                activity_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
                owner=EXCEL_INTAKE_RUN_OWNER,
                phase="owner_specific_cancel",
                lease_owner=str(actor or EXCEL_INTAKE_RUN_OWNER).strip() or EXCEL_INTAKE_RUN_OWNER,
                provider="excel_intake_local",
                provider_request_ref=normalized_job_id or normalized_command_id,
                input_payload={
                    "job_id": normalized_job_id,
                    "control_action": "cancel",
                    "reason": cancel_reason,
                },
                entity_counts={"excel_intake_job_count": 1 if normalized_job_id else 0},
                metadata={
                    "owner_specific_control": True,
                    "async_thread_owner": True,
                    "migration_phase": "W11_excel_intake_owner_specific_cancel",
                },
                attempt_suffix="owner_specific_cancel",
            )
        final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
            activity=dict(activity or {}),
            attempt=dict(attempt or {}),
            status="cancelled",
            phase="cancelled",
            output={
                "job_id": normalized_job_id,
                "control_action": "cancel",
                "reason": cancel_reason,
                "thread_terminal_checkpoint": True,
            },
            entity_counts={"excel_intake_job_count": 1 if normalized_job_id else 0},
            error={"reason": cancel_reason, "control_action": "cancel"},
            metadata={
                "owner_specific_control": True,
                "async_thread_owner": True,
                "migration_phase": "W11_excel_intake_owner_specific_cancel",
            },
            attempt_status="cancelled",
        )
        delta = self._kernel._record_command_activity_entity_delta(
            command=updated or command,
            activity=final_activity or dict(activity or {}),
            attempt=final_attempt or dict(attempt or {}),
            entity_type="excel_intake_job",
            entity_key=normalized_job_id or normalized_command_id,
            delta_kind=default_readiness_effect_for_command_type(EXCEL_INTAKE_RUN_COMMAND_TYPE),
            status="cancelled",
            reason=cancel_reason,
            source_ref={"job_id": normalized_job_id, "command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE},
            entity_payload={
                "job_id": normalized_job_id,
                "control_action": "cancel",
                "thread_terminal_checkpoint": True,
            },
            metadata={"owner": EXCEL_INTAKE_RUN_OWNER, "owner_specific_control": True},
            idempotency_scope="excel_intake_run_cancelled",
        )
        operation_sync = self._sync_operation_run_from_workflow_command_control(
            updated or command,
            control_action="cancel",
            actor=str(actor or EXCEL_INTAKE_RUN_OWNER).strip() or EXCEL_INTAKE_RUN_OWNER,
            source="api.workflow_command_owner_specific_cancel",
        )
        return {
            "status": "cancelled",
            "workflow_command": self._kernel._workflow_command_api_record(self.store.get_workflow_command(normalized_command_id) or updated or command),
            "operation_sync": operation_sync,
            "workflow_activity": final_activity or dict(activity or {}),
            "workflow_activity_attempt": final_attempt or dict(attempt or {}),
            "workflow_entity_delta": delta,
            "module_state_mutated": True,
            "owner_specific_control": True,
            "contract": "w11_workflow_command_owner_specific_control_v1",
        }

    def _raise_if_excel_intake_command_cancelled(
        self,
        *,
        command_id: str,
        job_id: str,
        request: JobRequest,
        checkpoint: str,
    ) -> None:
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id or not self._kernel._workflow_command_is_cancelled(normalized_command_id):
            return
        existing_summary = dict((self.store.get_job(job_id) or {}).get("summary") or {})
        self._save_excel_intake_job_state(
            job_id=job_id,
            request=request,
            status="cancelled",
            stage="cancelled",
            summary_payload={
                **existing_summary,
                "workflow_kind": "excel_intake",
                "message": "Excel intake was cancelled by command control.",
                "cancelled_at": _utc_now_iso(),
                "cancelled_checkpoint": str(checkpoint or "").strip(),
                "workflow_command_id": normalized_command_id,
            },
        )
        raise _ExcelIntakeCommandCancelled(
            f"excel_intake_command_cancelled:{str(checkpoint or 'checkpoint').strip()}"
        )

    def _run_excel_intake_workflow_command_thread(
        self,
        *,
        command_id: str,
        job_id: str,
        request: JobRequest,
        payload: dict[str, Any],
        activity: dict[str, Any] | None = None,
        attempt: dict[str, Any] | None = None,
    ) -> None:
        normalized_command_id = str(command_id or "").strip()
        try:
            self._run_excel_intake_workflow(job_id=job_id, request=request, payload=payload)
            if self._kernel._workflow_command_is_cancelled(normalized_command_id):
                self._record_excel_intake_command_cancelled_terminal(
                    command_id=normalized_command_id,
                    job_id=job_id,
                    activity=dict(activity or {}),
                    attempt=dict(attempt or {}),
                    reason="excel_intake_cancelled_before_thread_terminal",
                    actor=EXCEL_INTAKE_RUN_OWNER,
                )
                return
            refreshed = self.store.get_job(job_id) or {}
            status = str(refreshed.get("status") or "").strip().lower()
            if status == "completed":
                final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                    activity=dict(activity or {}),
                    attempt=dict(attempt or {}),
                    status="succeeded",
                    phase="excel_intake_completed",
                    output={
                        "job_id": job_id,
                        "job_status": status,
                        "row_count": int(payload.get("row_count") or payload.get("prepared_contact_count") or 0),
                    },
                    entity_counts={
                        "excel_intake_job_count": 1,
                        "row_count": int(payload.get("row_count") or payload.get("prepared_contact_count") or 0),
                    },
                    metadata={"terminal_job_status": status},
                    attempt_status="succeeded",
                )
                entity_delta = self._kernel._record_command_activity_entity_delta(
                    command=self.store.get_workflow_command(normalized_command_id) or {"command_id": normalized_command_id},
                    activity=final_activity or dict(activity or {}),
                    attempt=final_attempt or dict(attempt or {}),
                    entity_type="excel_intake_job",
                    entity_key=job_id,
                    delta_kind=default_readiness_effect_for_command_type(EXCEL_INTAKE_RUN_COMMAND_TYPE),
                    status="recorded",
                    reason="excel_intake_run_completed",
                    source_ref={"job_id": job_id, "command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE},
                    entity_payload={
                        "job_id": job_id,
                        "job_status": status,
                        "row_count": int(payload.get("row_count") or payload.get("prepared_contact_count") or 0),
                    },
                    metadata={"owner": EXCEL_INTAKE_RUN_OWNER},
                    idempotency_scope="excel_intake_run",
                )
                self.store.mark_workflow_command_succeeded(
                    normalized_command_id,
                    result={
                        "job_id": job_id,
                        "job_status": status,
                        "completed_async": True,
                        "activity_run_id": str((final_activity or dict(activity or {})).get("activity_run_id") or "").strip(),
                        "activity_attempt_id": str((final_attempt or dict(attempt or {})).get("attempt_id") or "").strip(),
                        "entity_delta_id": str(entity_delta.get("delta_id") or "").strip(),
                        "row_count": int(payload.get("row_count") or payload.get("prepared_contact_count") or 0),
                        "migration_phase": "W7_excel_intake_run_command_owner",
                    },
                )
            elif status in {"cancelled", "canceled"}:
                self._record_excel_intake_command_cancelled_terminal(
                    command_id=normalized_command_id,
                    job_id=job_id,
                    activity=dict(activity or {}),
                    attempt=dict(attempt or {}),
                    reason="excel_intake_job_cancelled",
                    actor=EXCEL_INTAKE_RUN_OWNER,
                )
            else:
                final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                    activity=dict(activity or {}),
                    attempt=dict(attempt or {}),
                    status="failed",
                    phase="failed",
                    output={"job_id": job_id, "job_status": status},
                    entity_counts={"excel_intake_job_count": 1},
                    error={"reason": f"excel_intake_job_terminal_status:{status or 'unknown'}"},
                    metadata={"terminal_job_status": status},
                    attempt_status="failed",
                )
                self._kernel._record_command_activity_entity_delta(
                    command=self.store.get_workflow_command(normalized_command_id) or {"command_id": normalized_command_id},
                    activity=final_activity or dict(activity or {}),
                    attempt=final_attempt or dict(attempt or {}),
                    entity_type="excel_intake_job",
                    entity_key=job_id,
                    delta_kind=default_readiness_effect_for_command_type(EXCEL_INTAKE_RUN_COMMAND_TYPE),
                    status="failed",
                    reason=f"excel_intake_job_terminal_status:{status or 'unknown'}",
                    source_ref={"job_id": job_id, "command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE},
                    entity_payload={"job_id": job_id, "job_status": status},
                    metadata={"owner": EXCEL_INTAKE_RUN_OWNER},
                    idempotency_scope="excel_intake_run",
                )
                self.store.mark_workflow_command_failed(
                    normalized_command_id,
                    error_text=f"excel_intake_job_terminal_status:{status or 'unknown'}",
                    retryable=False,
                )
        except _ExcelIntakeCommandCancelled as exc:
            self._record_excel_intake_command_cancelled_terminal(
                command_id=normalized_command_id,
                job_id=job_id,
                activity=dict(activity or {}),
                attempt=dict(attempt or {}),
                reason=str(exc) or "excel_intake_command_cancelled",
                actor=EXCEL_INTAKE_RUN_OWNER,
            )
        except Exception as exc:
            final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                activity=dict(activity or {}),
                attempt=dict(attempt or {}),
                status="failed",
                phase="failed",
                output={"job_id": job_id},
                entity_counts={"excel_intake_job_count": 1},
                error={"message": str(exc), "reason": "excel_intake_run_failed"},
                metadata={"terminal_job_status": "exception"},
                attempt_status="failed",
            )
            self._kernel._record_command_activity_entity_delta(
                command=self.store.get_workflow_command(normalized_command_id) or {"command_id": normalized_command_id},
                activity=final_activity or dict(activity or {}),
                attempt=final_attempt or dict(attempt or {}),
                entity_type="excel_intake_job",
                entity_key=job_id,
                delta_kind=default_readiness_effect_for_command_type(EXCEL_INTAKE_RUN_COMMAND_TYPE),
                status="failed",
                reason="excel_intake_run_failed",
                source_ref={"job_id": job_id, "command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE},
                entity_payload={"job_id": job_id, "error": str(exc)},
                metadata={"owner": EXCEL_INTAKE_RUN_OWNER},
                idempotency_scope="excel_intake_run",
            )
            self.store.mark_workflow_command_failed(
                normalized_command_id,
                error_text=f"excel_intake_run_failed:{exc}",
                retryable=True,
                retry_delay_seconds=10,
            )

    def _excel_intake_command_is_active(self, command: dict[str, Any]) -> bool:
        status = str(dict(command or {}).get("status") or "").strip().lower()
        if status not in {"claimed", "running"}:
            return False
        lease_expires_at = str(dict(command or {}).get("lease_expires_at") or "").strip()
        if not lease_expires_at:
            return False
        parsed = _parse_timestamp(lease_expires_at)
        return bool(parsed and parsed > datetime.now(timezone.utc))

    def _latest_excel_intake_run_command_for_job(self, job_id: str) -> dict[str, Any]:
        workflow_run_id = self._excel_intake_workflow_run_id(job_id)
        if not workflow_run_id:
            return {}
        commands = [
            dict(command)
            for command in self.store.list_workflow_commands(
                workflow_run_id=workflow_run_id,
                owner=EXCEL_INTAKE_RUN_OWNER,
                limit=0,
            )
            if str(dict(command).get("command_type") or "").strip() == EXCEL_INTAKE_RUN_COMMAND_TYPE
        ]
        if not commands:
            return {}
        commands.sort(key=lambda command: str(command.get("updated_at") or command.get("created_at") or ""))
        return commands[-1]

    def _drain_excel_intake_run_commands(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = dict(payload or {})
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        job_id = str(normalized.get("job_id") or normalized.get("excel_intake_job_id") or "").strip()
        if not workflow_run_id and job_id:
            workflow_run_id = self._excel_intake_workflow_run_id(job_id)
        limit = max(1, _coerce_int(normalized.get("excel_intake_command_owner_limit"), 10))
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=workflow_run_id,
            owner=EXCEL_INTAKE_RUN_OWNER,
            command_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
            limit=limit,
        )
        results: list[dict[str, Any]] = []
        for command in ready_commands:
            results.append(self._run_excel_intake_run_command(command))
        started_count = sum(
            1
            for result in results
            if str(dict(result).get("reason") or "").strip() == "excel_intake_run_command_owner"
        )
        skipped_count = sum(1 for result in results if str(dict(result).get("status") or "") == "skipped")
        failed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "failed")
        return {
            "status": "completed",
            "owner": EXCEL_INTAKE_RUN_OWNER,
            "command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE,
            "ready_command_count": len(ready_commands),
            "started_count": started_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
            "results": results,
        }

    def _excel_intake_current_job_marker_from_job(self, job: dict[str, Any] | None) -> dict[str, Any]:
        job_payload = dict(job or {})
        job_id = str(job_payload.get("job_id") or "").strip()
        summary_payload = dict(job_payload.get("summary") or {})
        result_view = self.store.get_job_result_view(job_id=job_id) if job_id else None
        result_view_payload = dict(result_view or {})
        result_view_metadata = dict(result_view_payload.get("metadata") or {})
        candidate_sources: list[tuple[str, dict[str, Any]]] = []
        if result_view_payload:
            candidate_sources.append(
                (
                    "job_result_view",
                    {
                        "job_scoped_candidate_markers": list(
                            result_view_metadata.get("job_scoped_candidate_markers") or []
                        ),
                        "result_view": result_view_payload,
                        "result_view_metadata": result_view_metadata,
                    },
                )
            )
        candidate_sources.append(("job_summary", summary_payload))
        summary_candidate_source = dict(summary_payload.get("candidate_source") or {})
        if summary_candidate_source:
            candidate_sources.append(("job_summary_candidate_source", summary_candidate_source))
        stage1_preview = dict(summary_payload.get("stage1_preview") or {})
        stage1_candidate_source = dict(stage1_preview.get("candidate_source") or {})
        if stage1_candidate_source:
            candidate_sources.append(("stage1_preview_candidate_source", stage1_candidate_source))
        linkedin_stage = dict(summary_payload.get("linkedin_stage_1") or {})
        if linkedin_stage:
            candidate_sources.append(("linkedin_stage_1", linkedin_stage))

        for source_name, candidate_source in candidate_sources:
            for marker in self._normalized_job_scoped_candidate_markers(candidate_source):
                if str(marker.get("marker_id") or "").strip() != EXCEL_INTAKE_CURRENT_JOB_MARKER_ID:
                    continue
                candidate_ids = _dedupe_texts(marker.get("candidate_ids") or [])
                if not candidate_ids:
                    continue
                return {
                    **marker,
                    "candidate_ids": candidate_ids,
                    "candidate_count": len(candidate_ids),
                    "resolved_from": source_name,
                }
        return {}

    def _excel_intake_artifact_materialization_state(
        self,
        *,
        job: dict[str, Any],
        summary_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        summary = dict(summary_payload or {})
        workflow_kind = str(summary.get("workflow_kind") or job.get("job_type") or "").strip().lower()
        if workflow_kind not in {"excel_intake", "excel_intake_batch"}:
            return {}
        public_stage = dict(summary.get("public_web_stage_2") or {})
        if str(public_stage.get("artifact_build_status") or "").strip().lower() == "completed":
            return {}
        if not _coerce_bool(public_stage.get("artifact_build_deferred"), False):
            return {}
        snapshot_id = str(public_stage.get("snapshot_id") or summary.get("snapshot_id") or "").strip()
        if not snapshot_id:
            return {}
        return {
            "status": "scheduled",
            "workflow_kind": "excel_intake",
            "snapshot_id": snapshot_id,
            "candidate_doc_path": str(public_stage.get("candidate_doc_path") or "").strip(),
            "reason": str(
                public_stage.get("artifact_build_deferred_reason")
                or "excel_workflow_serves_result_view_overlay"
            ).strip(),
            "public_web_stage_2": public_stage,
        }

    def repair_excel_intake_artifacts(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            return {"status": "invalid", "reason": "job_id_missing"}
        job = self.store.get_job(job_id)
        if job is None:
            return {"status": "not_found", "reason": "job_missing", "job_id": job_id}
        if str(job.get("job_type") or "").strip() != "excel_intake":
            return {"status": "skipped", "reason": "not_excel_intake_job", "job_id": job_id}
        summary = dict(job.get("summary") or {})
        materialization_state = self._excel_intake_artifact_materialization_state(
            job=job,
            summary_payload=summary,
        )
        snapshot_id = str(
            materialization_state.get("snapshot_id")
            or dict(summary.get("public_web_stage_2") or {}).get("snapshot_id")
            or ""
        ).strip()
        item_id = (
            self._snapshot_full_materialization_item_id(job_id=job_id, snapshot_id=snapshot_id)
            if snapshot_id
            else ""
        )
        existing_item = self.store.get_job_materialization_item(item_id) if item_id else {}
        dry_run = _coerce_bool(payload.get("dry_run"), True)
        run_now = _coerce_bool(payload.get("run_now"), False)
        base_result = {
            "job_id": job_id,
            "snapshot_id": snapshot_id,
            "item_kind": "snapshot_full_materialization",
            "item_id": item_id,
            "materialization_required": bool(materialization_state),
            "existing_item_status": str(existing_item.get("status") or ""),
        }
        if dry_run:
            return {
                "status": "dry_run",
                "reason": (
                    "excel_artifact_materialization_required"
                    if materialization_state
                    else "excel_artifact_materialization_not_required"
                ),
                "would_enqueue": bool(materialization_state),
                **base_result,
            }
        if not materialization_state and not existing_item:
            return {
                "status": "skipped",
                "reason": "excel_artifact_materialization_not_required",
                **base_result,
            }
        item = existing_item or self._enqueue_snapshot_full_materialization_item(
            job_id=job_id,
            source=str(payload.get("source") or "excel_artifact_repair_admin").strip(),
            metadata={"enqueue_event": "excel_artifact_repair_admin"},
        )
        result = {
            "status": "enqueued" if item else "failed",
            "reason": "excel_artifact_materialization_item_enqueued" if item else "excel_artifact_materialization_enqueue_failed",
            "item": item,
            **base_result,
        }
        if run_now and item:
            result["queue"] = self._run_snapshot_full_materialization_queue_once(
                {
                    "job_id": job_id,
                    "owner_id": str(payload.get("owner_id") or "excel-artifact-repair-admin").strip(),
                    "snapshot_full_materialization_item_limit": 1,
                }
            )
            result["status"] = "completed" if int(dict(result["queue"]).get("completed_count") or 0) > 0 else "enqueued"
        return result

    def _build_excel_intake_progress_payload(self, *, job: dict[str, Any]) -> dict[str, Any]:
        job_payload = dict(job or {})
        job_summary = dict(job_payload.get("summary") or {})
        workflow_kind = str(job_summary.get("workflow_kind") or job_payload.get("job_type") or "").strip().lower()
        if workflow_kind not in {"excel_intake", "excel_intake_batch"}:
            return {}
        intake_summary = dict(job_summary.get("intake_summary") or {})
        row_manifest = dict(job_summary.get("excel_row_manifest") or {})
        linkedin_stage = dict(job_summary.get("linkedin_stage_1") or {})
        status_counts = {
            str(key or "").strip(): int(value or 0)
            for key, value in dict(row_manifest.get("status_counts") or {}).items()
            if str(key or "").strip()
        }
        manual_review_row_count = sum(
            int(intake_summary.get(field_name) or 0)
            for field_name in ("manual_review_local_count", "manual_review_search_count")
        )
        if manual_review_row_count == 0 and status_counts:
            manual_review_row_count = int(status_counts.get("manual_review_local") or 0) + int(
                status_counts.get("manual_review_search") or 0
            )
        unresolved_row_count = int(intake_summary.get("unresolved_count") or status_counts.get("unresolved") or 0)
        invalid_row_count = int(intake_summary.get("invalid_count") or status_counts.get("invalid") or 0)
        total_row_count = int(
            row_manifest.get("total_row_count")
            or intake_summary.get("total_rows")
            or linkedin_stage.get("total_rows")
            or job_summary.get("row_count")
            or 0
        )
        matched_row_count = int(
            row_manifest.get("matched_row_count")
            or intake_summary.get("persisted_candidate_count")
            or linkedin_stage.get("persisted_candidate_count")
            or 0
        )
        review_row_count = int(
            row_manifest.get("review_row_count")
            or (manual_review_row_count + unresolved_row_count + invalid_row_count)
        )
        target_candidate_count = int(
            linkedin_stage.get("excel_import_candidate_count")
            or intake_summary.get("persisted_candidate_count")
            or matched_row_count
            or 0
        )
        return {
            "workflow_kind": workflow_kind,
            "target_company": str(job_summary.get("target_company") or job_payload.get("target_company") or "").strip(),
            "input_filename": str(job_summary.get("input_filename") or "").strip(),
            "total_row_count": total_row_count,
            "matched_row_count": matched_row_count,
            "target_candidate_count": target_candidate_count,
            "manual_review_row_count": manual_review_row_count,
            "unresolved_row_count": unresolved_row_count,
            "invalid_row_count": invalid_row_count,
            "review_row_count": review_row_count,
            "status_counts": status_counts,
            "row_manifest_available": bool(row_manifest),
            "row_manifest_truncated": bool(row_manifest.get("truncated")),
        }

    @staticmethod
    def _format_excel_intake_progress_message(excel_intake_progress: dict[str, Any]) -> str:
        progress = dict(excel_intake_progress or {})
        total_row_count = int(progress.get("total_row_count") or 0)
        matched_row_count = int(progress.get("matched_row_count") or 0)
        manual_review_row_count = int(progress.get("manual_review_row_count") or 0)
        unresolved_row_count = int(progress.get("unresolved_row_count") or 0)
        invalid_row_count = int(progress.get("invalid_row_count") or 0)
        unresolved_label = "未解析/无效" if invalid_row_count > 0 else "未解析"
        unresolved_display_count = unresolved_row_count + invalid_row_count
        if total_row_count > 0:
            return (
                f"Excel 导入完成：共 {total_row_count} 行，已匹配 {matched_row_count} 行，"
                f"需人工审核 {manual_review_row_count} 行，{unresolved_label} {unresolved_display_count} 行。"
            )
        return "Excel 导入完成。"

    def _cancel_running_excel_intake_command(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        body = dict(command_payload.get("payload") or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        job_id = str(body.get("job_id") or "").strip()
        actor = str(payload.get("actor") or payload.get("operator") or "api").strip() or "api"
        reason = str(payload.get("reason") or "cancelled_by_owner_specific_command_control").strip()
        if not job_id:
            return {
                "status": "invalid",
                "reason": "excel_intake_running_cancel_missing_job_id",
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        result = self._record_excel_intake_command_cancelled_terminal(
            command_id=command_id,
            job_id=job_id,
            reason=reason,
            actor=actor,
        )
        if result.get("workflow_command"):
            result["workflow_command"] = self._kernel._workflow_command_api_record(
                self.store.get_workflow_command(command_id) or command_payload
            )
        result.update(self._kernel._workflow_command_control_response_policy_records(self.store.get_workflow_command(command_id) or command_payload))
        return result

    def _resume_running_excel_intake_command(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        command_type = str(command_payload.get("command_type") or "").strip()
        body = dict(command_payload.get("payload") or {})
        actor = str(payload.get("actor") or payload.get("operator") or "api").strip() or "api"
        reason = str(payload.get("reason") or "resumed_by_owner_specific_command_control").strip()
        force = _coerce_bool(payload.get("force"), False)
        if self._kernel._workflow_command_lease_active(command_payload) and not force:
            return {
                "status": "invalid",
                "reason": "workflow_command_running_resume_requires_expired_lease_or_force",
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        job_id = str(body.get("job_id") or command_payload.get("workflow_run_id") or command_id).strip()
        activity_rows = self.store.list_workflow_activity_runs(
            command_id=command_id,
            activity_type=command_type,
            limit=1,
        )
        activity = dict(activity_rows[0]) if activity_rows else {}
        attempts = (
            self.store.list_workflow_activity_attempts(
                activity_run_id=str(activity.get("activity_run_id") or ""),
                limit=1,
            )
            if activity
            else []
        )
        attempt = dict(attempts[0]) if attempts else {}
        if not activity or not attempt:
            activity, attempt = self._kernel._start_workflow_command_activity_attempt(
                command_payload,
                activity_type=command_type,
                owner=EXCEL_INTAKE_RUN_OWNER,
                phase="owner_specific_resume",
                lease_owner=actor,
                provider="excel_intake_owner",
                provider_request_ref=job_id or command_id,
                input_payload={
                    "job_id": job_id,
                    "command_type": command_type,
                    "control_action": "resume",
                    "force": force,
                },
                entity_counts={"excel_intake_command_count": 1},
                metadata={
                    "owner_specific_control": True,
                    "excel_intake_resume_policy": "resume_requeues_without_request_path_thread_start",
                },
                attempt_suffix="owner_specific_resume",
            )
        final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
            activity=activity,
            attempt=attempt,
            status="queued",
            phase="owner_specific_resume_queued",
            output={
                "command_id": command_id,
                "command_type": command_type,
                "job_id": job_id,
                "control_action": "resume",
                "force": force,
                "reason": reason,
            },
            entity_counts={"excel_intake_command_count": 1},
            metadata={
                "owner_specific_control": True,
                "resume_mode": "owner_specific_requeue",
                "excel_intake_resume_policy": "resume_requeues_without_request_path_thread_start",
            },
            attempt_status="succeeded",
        )
        delta = self._kernel._record_command_activity_entity_delta(
            command=command_payload,
            activity=final_activity or activity,
            attempt=final_attempt or attempt,
            entity_type="excel_intake_command",
            entity_key=job_id or command_id,
            delta_kind="excel_intake_command_resume_queued",
            status="queued",
            reason="excel_intake_command_resumed_by_command_control",
            source_ref={"job_id": job_id, "command_type": command_type},
            entity_payload={
                "job_id": job_id,
                "command_id": command_id,
                "command_type": command_type,
                "control_action": "resume",
                "resume_reason": reason,
                "force": force,
            },
            projection_effect={"entered_projection": False, "excel_intake_requeued": True},
            metadata={
                "owner_specific_control": True,
                "resume_mode": "owner_specific_requeue",
            },
            idempotency_scope="owner_specific_resume",
        )
        updated = self.store.mark_workflow_command_partial_progress(
            command_id,
            result={
                **dict(command_payload.get("result") or {}),
                "control_source": "api.workflow_command_owner_specific_resume",
                "control_action": "resume",
                "owner_specific_control": True,
                "activity_run_id": str((final_activity or activity).get("activity_run_id") or "").strip(),
                "activity_attempt_id": str((final_attempt or attempt).get("attempt_id") or "").strip(),
                "entity_delta_id": str(delta.get("delta_id") or "").strip(),
                "resume_mode": "owner_specific_requeue",
                "excel_intake_resume_policy": "resume_requeues_without_request_path_thread_start",
                "force": force,
                "reason": reason,
            },
        )
        if not updated:
            return {
                "status": "invalid",
                "reason": "workflow_command_owner_specific_resume_not_applied",
                "workflow_command": self._kernel._workflow_command_api_record(
                    self.store.get_workflow_command(command_id) or command_payload
                ),
                **self._kernel._workflow_command_control_response_policy_records(
                    self.store.get_workflow_command(command_id) or command_payload
                ),
                "module_state_mutated": True,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        operation_sync = self._sync_operation_run_from_workflow_command_control(
            updated,
            control_action="resume",
            actor=actor,
            source="api.workflow_command_owner_specific_resume",
        )
        return {
            "status": "queued",
            "workflow_command": self._kernel._workflow_command_api_record(updated),
            "operation_sync": operation_sync,
            **self._kernel._workflow_command_control_response_policy_records(updated),
            "workflow_activity": final_activity or activity,
            "workflow_activity_attempt": final_attempt or attempt,
            "workflow_entity_delta": delta,
            "module_state_mutated": True,
            "owner_specific_control": True,
            "contract": "w11_workflow_command_owner_specific_control_v1",
        }

    def _load_excel_intake_row_manifest_for_export(
        self,
        *,
        payload: dict[str, Any],
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        requested_job_id = str(payload.get("job_id") or "").strip()
        record_job_ids = _dedupe_texts(str(record.get("job_id") or "").strip() for record in records)
        job_id = requested_job_id or (record_job_ids[0] if len(record_job_ids) == 1 else "")
        if not job_id:
            return {}
        job = self.store.get_job(job_id)
        if job is None:
            return {}
        summary_payload = dict(job.get("summary") or {})
        workflow_kind = str(summary_payload.get("workflow_kind") or job.get("job_type") or "").strip().lower()
        if workflow_kind != "excel_intake":
            return {}
        manifest_ref = dict(summary_payload.get("excel_row_manifest") or {})
        manifest_path_value = str(manifest_ref.get("path") or "").strip()
        if not manifest_path_value:
            return {
                "job_id": job_id,
                "intake_id": str(summary_payload.get("intake_id") or "").strip(),
                "status": "missing",
                "reason": "excel_row_manifest_missing",
            }
        manifest_path = Path(manifest_path_value).expanduser()
        try:
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {
                "job_id": job_id,
                "intake_id": str(summary_payload.get("intake_id") or "").strip(),
                "status": "missing",
                "reason": "excel_row_manifest_unreadable",
                "path": manifest_path_value,
            }
        manifest = dict(manifest_payload) if isinstance(manifest_payload, dict) else {}
        review_rows = [dict(item) for item in list(manifest.get("review_rows") or []) if isinstance(item, dict)]
        matched_rows = [dict(item) for item in list(manifest.get("matched_rows") or []) if isinstance(item, dict)]
        return {
            "job_id": job_id,
            "intake_id": str(manifest.get("intake_id") or summary_payload.get("intake_id") or "").strip(),
            "status": "available",
            "path": manifest_path_value,
            "total_row_count": int(manifest.get("total_row_count") or 0),
            "matched_row_count": int(manifest.get("matched_row_count") or len(matched_rows) or 0),
            "review_row_count": int(manifest.get("review_row_count") or len(review_rows) or 0),
            "status_counts": dict(manifest.get("status_counts") or {}),
            "truncated": bool(manifest.get("truncated")),
            "matched_rows": matched_rows,
            "review_rows": review_rows,
        }

    def _save_excel_intake_job_state(
        self,
        *,
        job_id: str,
        request: JobRequest,
        status: str,
        stage: str,
        summary_payload: dict[str, Any],
        artifact_path: str = "",
        execution_bundle_payload: dict[str, Any] | None = None,
    ) -> None:
        resolved_artifact_path = str(artifact_path or "").strip()
        if not resolved_artifact_path:
            resolved_artifact_path = str((self.store.get_job(job_id) or {}).get("artifact_path") or "")
        self.store.save_job(
            job_id=job_id,
            job_type="excel_intake",
            status=status,
            stage=stage,
            request_payload=request.to_record(),
            plan_payload={},
            execution_bundle_payload=execution_bundle_payload,
            summary_payload=summary_payload,
            artifact_path=resolved_artifact_path,
        )

    @staticmethod
    def _compact_excel_intake_review_candidate(candidate: dict[str, Any], *, ordinal: int) -> dict[str, Any]:
        candidate_payload = dict(candidate or {})
        return {
            key: value
            for key, value in {
                "ordinal": ordinal,
                "candidate_id": str(candidate_payload.get("candidate_id") or candidate_payload.get("id") or "").strip(),
                "name": str(
                    candidate_payload.get("display_name")
                    or candidate_payload.get("name_en")
                    or candidate_payload.get("name")
                    or ""
                ).strip(),
                "company": str(
                    candidate_payload.get("current_company")
                    or candidate_payload.get("organization")
                    or candidate_payload.get("company")
                    or ""
                ).strip(),
                "title": str(candidate_payload.get("title") or candidate_payload.get("headline") or "").strip(),
                "linkedin_url": str(candidate_payload.get("linkedin_url") or candidate_payload.get("profile_url") or "").strip(),
                "match_score": candidate_payload.get("match_score"),
                "match_reason": str(candidate_payload.get("match_reason") or "").strip(),
            }.items()
            if value not in (None, "", [])
        }

    @classmethod
    def _compact_excel_intake_row_lineage(cls, row: dict[str, Any]) -> dict[str, Any]:
        row_payload = dict(row or {})
        matched_candidate = dict(row_payload.get("matched_candidate") or {})
        matched_candidate_id = str(matched_candidate.get("candidate_id") or matched_candidate.get("id") or "").strip()
        manual_review_candidates = [
            cls._compact_excel_intake_review_candidate(dict(candidate), ordinal=index)
            for index, candidate in enumerate(
                [item for item in list(row_payload.get("manual_review_candidates") or []) if isinstance(item, dict)],
                start=1,
            )
        ]
        manual_review_candidates = [item for item in manual_review_candidates if item]
        compact = {
            "row_key": str(row_payload.get("row_key") or "").strip(),
            "status": str(row_payload.get("status") or "").strip(),
            "name": str(row_payload.get("name") or "").strip(),
            "company": str(row_payload.get("company") or "").strip(),
            "title": str(row_payload.get("title") or "").strip(),
            "linkedin_url": str(row_payload.get("linkedin_url") or "").strip(),
            "email": str(row_payload.get("email") or "").strip(),
            "reason": str(row_payload.get("reason") or "").strip(),
            "match_reason": str(row_payload.get("match_reason") or "").strip(),
            "matched_candidate_id": matched_candidate_id,
            "manual_review_candidate_count": len(manual_review_candidates),
        }
        compact = {key: value for key, value in compact.items() if value not in (None, "", [])}
        if manual_review_candidates:
            compact["manual_review_candidates"] = manual_review_candidates[:10]
        search_result = dict(row_payload.get("search_result") or {})
        if search_result:
            compact["search_result"] = {
                "status": str(search_result.get("status") or "").strip(),
                "reason": str(search_result.get("reason") or "").strip(),
                "ranked_candidate_count": len(
                    [item for item in list(search_result.get("ranked_candidates") or []) if isinstance(item, dict)]
                ),
                "attempt_count": len([item for item in list(search_result.get("attempts") or []) if isinstance(item, dict)]),
            }
            compact["search_result"] = {
                key: value for key, value in compact["search_result"].items() if value not in (None, "", [])
            }
        return compact

    def _persist_excel_intake_row_manifest(
        self,
        *,
        job_id: str,
        result: dict[str, Any],
        max_review_rows: int = 500,
        max_matched_rows: int = 500,
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        rows = [dict(item) for item in list(dict(result or {}).get("results") or []) if isinstance(item, dict)]
        if not normalized_job_id or not rows:
            return {}
        matched_rows: list[dict[str, Any]] = []
        review_rows: list[dict[str, Any]] = []
        status_counts: Counter[str] = Counter()
        for row in rows:
            status = str(row.get("status") or "").strip()
            if status:
                status_counts[status] += 1
            compact_row = self._compact_excel_intake_row_lineage(row)
            if not compact_row:
                continue
            if str(compact_row.get("matched_candidate_id") or "").strip():
                matched_rows.append(compact_row)
            if status in {"manual_review_local", "manual_review_search", "unresolved", "invalid"}:
                review_rows.append(compact_row)
        manifest = {
            "schema_version": 1,
            "job_id": normalized_job_id,
            "intake_id": str(dict(result or {}).get("intake_id") or "").strip(),
            "generated_at": _utc_now_iso(),
            "total_row_count": len(rows),
            "matched_row_count": len(matched_rows),
            "review_row_count": len(review_rows),
            "status_counts": dict(status_counts),
            "matched_rows": matched_rows[:max_matched_rows],
            "review_rows": review_rows[:max_review_rows],
            "truncated": bool(len(matched_rows) > max_matched_rows or len(review_rows) > max_review_rows),
            "limits": {
                "max_matched_rows": max_matched_rows,
                "max_review_rows": max_review_rows,
            },
        }
        manifest_path = self.jobs_dir / f"{normalized_job_id}.excel_intake_row_manifest.json"
        manifest_path.write_text(
            json.dumps(_storage_json_safe_payload(manifest), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {
            "path": str(manifest_path),
            "schema_version": 1,
            "total_row_count": len(rows),
            "matched_row_count": len(matched_rows),
            "review_row_count": len(review_rows),
            "status_counts": dict(status_counts),
            "truncated": bool(manifest.get("truncated")),
        }

    def _compact_excel_intake_job_summary(
        self,
        *,
        job_id: str,
        result: dict[str, Any],
        request: JobRequest,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        summary_payload = dict(result.get("summary") or {})
        attachment_summary = dict(result.get("attachment_summary") or {})
        attachment_artifact = dict(attachment_summary.get("artifact_result") or {})
        workbook_payload = dict(result.get("workbook") or {})
        row_manifest = self._persist_excel_intake_row_manifest(job_id=job_id, result=result)
        compact_summary = {
            "workflow_kind": "excel_intake",
            "intake_id": str(result.get("intake_id") or "").strip(),
            "target_company": str(request.target_company or "").strip(),
            "input_filename": str(payload.get("filename") or workbook_payload.get("source_path") or "").strip(),
            "attach_to_snapshot": _coerce_bool(payload.get("attach_to_snapshot"), True),
            "build_artifacts": _coerce_bool(payload.get("build_artifacts"), True),
            "workbook": {
                "sheet_count": int(workbook_payload.get("sheet_count") or 0),
                "detected_contact_row_count": int(workbook_payload.get("detected_contact_row_count") or 0),
                "sheet_names": [
                    str(item or "").strip()
                    for item in list(workbook_payload.get("sheet_names") or [])
                    if str(item or "").strip()
                ],
            },
            "intake_summary": {
                "total_rows": int(summary_payload.get("total_rows") or 0),
                "persisted_candidate_count": int(summary_payload.get("persisted_candidate_count") or 0),
                "persisted_evidence_count": int(summary_payload.get("persisted_evidence_count") or 0),
                "local_exact_hit_count": int(summary_payload.get("local_exact_hit_count") or 0),
                "manual_review_local_count": int(summary_payload.get("manual_review_local_count") or 0),
                "fetched_direct_linkedin_count": int(summary_payload.get("fetched_direct_linkedin_count") or 0),
                "fetched_via_search_count": int(summary_payload.get("fetched_via_search_count") or 0),
                "manual_review_search_count": int(summary_payload.get("manual_review_search_count") or 0),
                "unresolved_count": int(summary_payload.get("unresolved_count") or 0),
            },
            "attachment_summary": {
                "status": str(attachment_summary.get("status") or "").strip(),
                "reason": str(attachment_summary.get("reason") or "").strip(),
                "snapshot_id": str(attachment_summary.get("snapshot_id") or "").strip(),
                "candidate_count": int(attachment_summary.get("candidate_count") or 0),
                "evidence_count": int(attachment_summary.get("evidence_count") or 0),
                "candidate_doc_path": str(attachment_summary.get("candidate_doc_path") or "").strip(),
                "stage_candidate_doc_path": str(attachment_summary.get("stage_candidate_doc_path") or "").strip(),
                "summary_path": str(attachment_summary.get("summary_path") or "").strip(),
                "artifact_result": {
                    "status": str(attachment_artifact.get("status") or "").strip(),
                    "generation_key": str(attachment_artifact.get("generation_key") or "").strip(),
                    "generation_sequence": int(attachment_artifact.get("generation_sequence") or 0),
                },
            },
            "artifact_paths": {
                key: str(value or "").strip()
                for key, value in dict(result.get("artifact_paths") or {}).items()
                if str(key or "").strip() and str(value or "").strip()
            },
        }
        if row_manifest:
            compact_summary["excel_row_manifest"] = row_manifest
        return compact_summary

    def _excel_intake_current_job_candidate_marker(self, result: dict[str, Any]) -> dict[str, Any]:
        candidate_ids: list[str] = []
        row_lineage: list[dict[str, str]] = []
        for row in list(dict(result or {}).get("results") or []):
            if not isinstance(row, dict):
                continue
            matched_candidate = dict(row.get("matched_candidate") or {})
            candidate_id = str(matched_candidate.get("candidate_id") or matched_candidate.get("id") or "").strip()
            if candidate_id:
                candidate_ids.append(candidate_id)
                row_lineage.append(
                    {
                        key: value
                        for key, value in {
                            "candidate_id": candidate_id,
                            "row_key": str(row.get("row_key") or "").strip(),
                            "status": str(row.get("status") or "").strip(),
                            "name": str(row.get("name") or "").strip(),
                            "company": str(row.get("company") or "").strip(),
                            "title": str(row.get("title") or "").strip(),
                            "linkedin_url": str(row.get("linkedin_url") or "").strip(),
                        }.items()
                        if value
                    }
                )
        candidate_ids = _dedupe_texts(candidate_ids)
        if not candidate_ids:
            return {}
        marker = {
            "marker_id": EXCEL_INTAKE_CURRENT_JOB_MARKER_ID,
            "label": EXCEL_INTAKE_CURRENT_JOB_MARKER_LABEL,
            "source_kind": "excel_intake",
            "intake_id": str(dict(result or {}).get("intake_id") or "").strip(),
            "candidate_ids": candidate_ids,
            "candidate_count": len(candidate_ids),
        }
        if row_lineage:
            marker["row_lineage"] = row_lineage
        return marker

    def _excel_intake_job_age_seconds(self, job: dict[str, Any]) -> float | None:
        updated_at = str(dict(job or {}).get("updated_at") or "").strip()
        if not updated_at:
            return None
        try:
            parsed = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, datetime.now(timezone.utc).timestamp() - parsed.timestamp())

    def _excel_intake_job_has_durable_prepared_batch(self, job: dict[str, Any]) -> bool:
        excel_bundle = dict(dict(job.get("execution_bundle") or {}).get("excel_intake") or {})
        prepared_batch_path = str(excel_bundle.get("prepared_contact_batch_path") or "").strip()
        if not prepared_batch_path:
            return False
        return Path(prepared_batch_path).expanduser().is_file()

    def _excel_intake_job_recovery_eligible(
        self,
        job: dict[str, Any],
        *,
        stale_after_seconds: int,
    ) -> tuple[bool, str]:
        status = str(dict(job or {}).get("status") or "").strip().lower()
        stage = str(dict(job or {}).get("stage") or "").strip().lower()
        if status not in {"queued", "running"}:
            return False, "not_active"
        if stage not in {"", "acquiring"}:
            return False, "stage_not_replay_safe"
        summary = dict(dict(job or {}).get("summary") or {})
        if str(summary.get("intake_id") or "").strip():
            return False, "intake_already_started"
        linkedin_stage = dict(summary.get("linkedin_stage_1") or {})
        if str(linkedin_stage.get("status") or "").strip().lower() == "completed":
            return False, "intake_stage_already_completed"
        if not self._excel_intake_job_has_durable_prepared_batch(job):
            return False, "prepared_batch_missing"
        normalized_stale_after = max(0, int(stale_after_seconds or 0))
        age_seconds = self._excel_intake_job_age_seconds(job)
        if normalized_stale_after > 0 and (age_seconds is None or age_seconds < normalized_stale_after):
            return False, "not_stale"
        return True, "eligible"

    def _recover_stale_excel_intake_jobs_once(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        if not _coerce_bool(
            payload.get("excel_intake_recovery_enabled"),
            _env_bool("EXCEL_INTAKE_RECOVERY_ENABLED", True),
        ):
            return {"status": "skipped", "reason": "excel_intake_recovery_disabled", "results": []}
        explicit_job_id = str(payload.get("job_id") or payload.get("excel_intake_job_id") or "").strip()
        stale_after_seconds = max(
            0,
            _coerce_int(
                payload.get("excel_intake_stale_after_seconds"),
                _env_int("EXCEL_INTAKE_RECOVERY_STALE_AFTER_SECONDS", 60),
            ),
        )
        limit = max(1, _coerce_int(payload.get("excel_intake_recovery_limit"), 10))
        if explicit_job_id:
            job = self.store.get_job(explicit_job_id)
            candidates = [job] if isinstance(job, dict) and str(job.get("job_type") or "") == "excel_intake" else []
        else:
            candidates = self.store.list_jobs(
                job_type="excel_intake",
                statuses=["queued", "running"],
                limit=limit,
            )
        results: list[dict[str, Any]] = []
        for job in candidates[:limit]:
            job_id = str(dict(job or {}).get("job_id") or "").strip()
            if not job_id:
                continue
            eligible, reason = self._excel_intake_job_recovery_eligible(
                job,
                stale_after_seconds=stale_after_seconds,
            )
            if not eligible:
                results.append({"job_id": job_id, "status": "skipped", "reason": reason})
                continue
            with self._job_run_lock(job_id) as lock_handle:
                if lock_handle is None:
                    results.append({"job_id": job_id, "status": "skipped", "reason": "job_lock_unavailable"})
                    continue
                latest_job = self.store.get_job(job_id) or job
                eligible, reason = self._excel_intake_job_recovery_eligible(
                    latest_job,
                    stale_after_seconds=stale_after_seconds,
                )
                if not eligible:
                    results.append({"job_id": job_id, "status": "skipped", "reason": reason})
                    continue
                request = JobRequest.from_payload(dict(latest_job.get("request") or {}))
                recovery_payload = self._excel_intake_payload_from_execution_bundle(job=latest_job)
                if not dict(recovery_payload.get("prepared_contact_batch") or {}):
                    results.append({"job_id": job_id, "status": "skipped", "reason": "prepared_batch_unreadable"})
                    continue
                command = self._latest_excel_intake_run_command_for_job(job_id)
                command_status = str(command.get("status") or "").strip().lower()
                if command and self._excel_intake_command_is_active(command):
                    results.append(
                        {
                            "job_id": job_id,
                            "status": "skipped",
                            "reason": "excel_intake_run_command_active",
                            "workflow_command": self._kernel._workflow_command_observation(
                                command,
                                migration_phase="W7_excel_intake_run_command_owner",
                            ),
                        }
                    )
                    continue
                if command and command_status in {"claimed", "running", "retry_wait"}:
                    command = self.store.mark_workflow_command_partial_progress(
                        str(command.get("command_id") or ""),
                        result={
                            "status": "requeued_for_excel_intake_stale_recovery",
                            "job_id": job_id,
                            "recovery_source": "excel_intake_recovery",
                            "requeued_at": _utc_now_iso(),
                        },
                    ) or command
                if not command or command_status in {"succeeded", "failed_terminal", "cancelled", "canceled", "superseded"}:
                    recovery_payload.update(
                        {
                            "run_scope": "stale_recovery",
                            "job_id": job_id,
                            "prepared_contact_count": len(
                                [
                                    item
                                    for item in list(
                                        dict(recovery_payload.get("prepared_contact_batch") or {}).get("contacts") or []
                                    )
                                    if isinstance(item, dict)
                                ]
                            ),
                        }
                    )
                    command = self._plan_excel_intake_run_command(
                        job_id=job_id,
                        request=request,
                        payload=recovery_payload,
                    )
                if not command:
                    results.append({"job_id": job_id, "status": "failed", "reason": "excel_intake_run_command_enqueue_failed"})
                    continue
                self.store.append_job_event(
                    job_id,
                    "acquiring",
                    "running",
                    "Recovering stale Excel intake job through excel.intake.run command owner.",
                    {
                        "workflow_kind": "excel_intake",
                        "recovery_source": "excel_intake_run_command_owner",
                        "prepared_contact_batch_path": str(
                            recovery_payload.get("prepared_contact_batch_path") or ""
                        ),
                        "workflow_command": self._kernel._workflow_command_observation(
                            command,
                            migration_phase="W7_excel_intake_run_command_owner",
                        ),
                    },
                )
                owner_result = self._drain_excel_intake_run_commands(
                    {
                        **payload,
                        "job_id": job_id,
                        "workflow_run_id": self._excel_intake_workflow_run_id(job_id),
                        "excel_intake_command_owner_limit": 1,
                    }
                )
                refreshed = self.store.get_job(job_id) or {}
                results.append(
                    {
                        "job_id": job_id,
                        "status": str(refreshed.get("status") or "unknown"),
                        "stage": str(refreshed.get("stage") or ""),
                        "reason": "recovered_via_excel_intake_run_command",
                        "owner_result": owner_result,
                        "workflow_command": self._kernel._workflow_command_observation(
                            self.store.get_workflow_command(str(command.get("command_id") or "")) or command,
                            migration_phase="W7_excel_intake_run_command_owner",
                        ),
                    }
                )
        recovered_count = sum(
            1 for item in results if str(item.get("reason") or "") == "recovered_via_excel_intake_run_command"
        )
        return {
            "status": "completed",
            "stale_after_seconds": stale_after_seconds,
            "scanned_count": len(candidates),
            "recovered_count": recovered_count,
            "results": results,
        }
