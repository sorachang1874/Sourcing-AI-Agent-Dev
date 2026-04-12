from __future__ import annotations

import base64
from dataclasses import fields
from datetime import datetime, timezone
from difflib import SequenceMatcher
import json
from pathlib import Path
import re
from typing import Any

from .asset_logger import AssetLogger
from .company_asset_completion import CompanyAssetCompletionManager
from .company_registry import normalize_company_key
from .connectors import resolve_company_identity
from .domain import (
    Candidate,
    EvidenceRecord,
    format_display_name,
    make_candidate_id,
    make_evidence_id,
    merge_candidate,
    normalize_candidate,
    normalize_requested_facet,
    normalize_requested_role_bucket,
)
from .enrichment import _format_education, _format_experience, _format_profile_languages, _format_profile_skills
from .harvest_connectors import HarvestProfileSearchConnector
from .model_provider import ModelClient
from .query_signal_knowledge import role_bucket_function_ids, role_bucket_role_hints
from .settings import AppSettings
from .storage import SQLiteStore
from .xlsx_reader import XlsxWorkbook


_CANDIDATE_FIELD_NAMES = {field.name for field in fields(Candidate)}
_MAX_LOCAL_NEAR_MATCHES = 5
_LOCAL_NEAR_MATCH_THRESHOLD = 68.0
_MIN_LOCAL_NEAR_MATCH_NAME_SIMILARITY = 0.9
_SEARCH_AUTO_FETCH_THRESHOLD = 86.0
_MAX_SEARCH_ROWS_PER_CONTACT = 5
_PROFILE_DETAIL_METADATA_FIELDS = ("headline", "summary", "languages", "skills", "public_identifier")


class ExcelIntakeService:
    def __init__(
        self,
        *,
        runtime_dir: str | Path,
        store: SQLiteStore,
        settings: AppSettings,
        model_client: ModelClient,
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.store = store
        self.settings = settings
        self.model_client = model_client
        self.profile_completion_manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            model_client=self.model_client,
        )
        self.harvest_profile_search_connector = HarvestProfileSearchConnector(self.settings.harvest.profile_search)

    def ingest_contacts(self, payload: dict[str, Any]) -> dict[str, Any]:
        intake_id = _utc_timestamp_slug()
        intake_dir = self.runtime_dir / "excel_intake" / intake_id
        intake_dir.mkdir(parents=True, exist_ok=True)
        logger = AssetLogger(intake_dir)

        workbook_path = self._materialize_workbook(payload=payload, intake_dir=intake_dir)
        workbook = XlsxWorkbook(workbook_path)
        sheet_names = workbook.sheet_names()
        sheet_rows = {sheet_name: workbook.read_sheet(sheet_name) for sheet_name in sheet_names}
        schema_payload = _build_schema_payload(workbook_path=workbook_path, sheet_rows=sheet_rows)
        inferred_schema = self.model_client.normalize_spreadsheet_contacts(schema_payload)
        normalized_schema = _normalize_schema_payload(inferred_schema or {}, fallback_payload=schema_payload)

        contacts = _extract_contacts_from_workbook(
            sheet_rows=sheet_rows,
            schema_payload=normalized_schema,
            workbook_path=workbook_path,
        )

        inventory = _build_local_candidate_inventory(self.runtime_dir, self.store)
        processed_rows: list[dict[str, Any]] = []
        persisted_candidate_count = 0
        persisted_evidence_count = 0

        for contact in contacts:
            row_result = self._process_contact_row(
                intake_dir=intake_dir,
                logger=logger,
                contact=contact,
                inventory=inventory,
            )
            processed_rows.append(row_result["result"])
            persisted_candidate_count += int(row_result.get("persisted_candidate_count") or 0)
            persisted_evidence_count += int(row_result.get("persisted_evidence_count") or 0)
            persisted_candidate = row_result.get("persisted_candidate")
            if isinstance(persisted_candidate, Candidate):
                _update_inventory_with_candidate(inventory, persisted_candidate)

        status_counts = _count_row_statuses(processed_rows)
        summary = {
            "status": "completed",
            "intake_id": intake_id,
            "workbook": {
                "source_path": str(workbook_path),
                "sheet_count": len(sheet_names),
                "sheet_names": sheet_names,
                "detected_contact_row_count": len(contacts),
            },
            "schema_inference": normalized_schema,
            "inventory": {
                "candidate_count": int(inventory.get("candidate_count") or 0),
                "linkedin_index_count": int(inventory.get("linkedin_index_count") or 0),
                "name_index_count": int(inventory.get("name_index_count") or 0),
            },
            "summary": {
                "total_rows": len(processed_rows),
                "local_exact_hit_count": status_counts.get("local_exact_hit", 0),
                "manual_review_local_count": status_counts.get("manual_review_local", 0),
                "fetched_direct_linkedin_count": status_counts.get("fetched_direct_linkedin", 0),
                "fetched_via_search_count": status_counts.get("fetched_via_search", 0),
                "manual_review_search_count": status_counts.get("manual_review_search", 0),
                "unresolved_count": status_counts.get("unresolved", 0),
                "persisted_candidate_count": persisted_candidate_count,
                "persisted_evidence_count": persisted_evidence_count,
            },
            "results": processed_rows,
        }

        schema_path = logger.write_json(
            intake_dir / "schema_inference.json",
            normalized_schema,
            asset_type="excel_intake_schema_inference",
            source_kind="excel_intake",
            is_raw_asset=False,
            model_safe=True,
        )
        contacts_path = logger.write_json(
            intake_dir / "normalized_contacts.json",
            contacts,
            asset_type="excel_intake_normalized_contacts",
            source_kind="excel_intake",
            is_raw_asset=False,
            model_safe=True,
        )
        summary_path = logger.write_json(
            intake_dir / "summary.json",
            summary,
            asset_type="excel_intake_summary",
            source_kind="excel_intake",
            is_raw_asset=False,
            model_safe=True,
        )
        summary["artifact_paths"] = {
            "schema_inference": str(schema_path),
            "normalized_contacts": str(contacts_path),
            "summary": str(summary_path),
        }
        return summary

    def continue_review(self, payload: dict[str, Any]) -> dict[str, Any]:
        intake_id = str(payload.get("intake_id") or "").strip()
        if not intake_id:
            raise ValueError("intake_id is required")
        decisions = payload.get("decisions")
        if isinstance(decisions, dict):
            decisions = [decisions]
        if not isinstance(decisions, list) or not decisions:
            single_decision = {
                "row_key": str(payload.get("row_key") or "").strip(),
                "action": str(payload.get("action") or "").strip(),
                "selected_candidate_id": str(payload.get("selected_candidate_id") or "").strip(),
                "selected_profile_url": str(payload.get("selected_profile_url") or "").strip(),
                "confirmed_new_person": bool(payload.get("confirmed_new_person", False)),
            }
            if single_decision["row_key"] and single_decision["action"]:
                decisions = [single_decision]
            else:
                raise ValueError("decisions or row_key/action is required")

        intake_dir = self.runtime_dir / "excel_intake" / intake_id
        if not intake_dir.exists():
            raise ValueError(f"excel intake {intake_id} not found")
        logger = AssetLogger(intake_dir)
        summary_payload = _read_json_dict(intake_dir / "summary.json")
        if not summary_payload:
            raise ValueError(f"excel intake {intake_id} summary missing")
        normalized_contacts_payload = json.loads((intake_dir / "normalized_contacts.json").read_text())
        contacts_by_row_key = {
            str(item.get("row_key") or "").strip(): dict(item)
            for item in list(normalized_contacts_payload or [])
            if isinstance(item, dict) and str(item.get("row_key") or "").strip()
        }
        previous_results = {
            str(item.get("row_key") or "").strip(): dict(item)
            for item in list(summary_payload.get("results") or [])
            if isinstance(item, dict) and str(item.get("row_key") or "").strip()
        }

        inventory = _build_local_candidate_inventory(self.runtime_dir, self.store)
        decision_results: list[dict[str, Any]] = []
        persisted_candidate_count = 0
        persisted_evidence_count = 0
        continuation_id = _utc_timestamp_slug()
        continuation_root = intake_dir / "continuations"
        continuation_root.mkdir(parents=True, exist_ok=True)

        for raw_decision in decisions:
            if not isinstance(raw_decision, dict):
                continue
            row_key = str(raw_decision.get("row_key") or "").strip()
            if not row_key:
                continue
            contact = contacts_by_row_key.get(row_key)
            previous_result = previous_results.get(row_key)
            if contact is None or previous_result is None:
                decision_results.append(
                    {
                        "row_key": row_key,
                        "status": "invalid",
                        "reason": "row_not_found",
                    }
                )
                continue
            row_result = self._continue_contact_row_review(
                intake_dir=intake_dir,
                logger=logger,
                contact=contact,
                previous_result=previous_result,
                decision=raw_decision,
                inventory=inventory,
            )
            decision_results.append(row_result["result"])
            persisted_candidate_count += int(row_result.get("persisted_candidate_count") or 0)
            persisted_evidence_count += int(row_result.get("persisted_evidence_count") or 0)
            persisted_candidate = row_result.get("persisted_candidate")
            if isinstance(persisted_candidate, Candidate):
                _update_inventory_with_candidate(inventory, persisted_candidate)

        continuation_payload = {
            "status": "completed",
            "intake_id": intake_id,
            "continuation_id": continuation_id,
            "decision_count": len(decision_results),
            "persisted_candidate_count": persisted_candidate_count,
            "persisted_evidence_count": persisted_evidence_count,
            "results": decision_results,
        }
        continuation_path = logger.write_json(
            continuation_root / f"{continuation_id}.json",
            continuation_payload,
            asset_type="excel_intake_review_continuation",
            source_kind="excel_intake",
            is_raw_asset=False,
            model_safe=True,
        )
        continuation_payload["artifact_path"] = str(continuation_path)
        return continuation_payload

    def _materialize_workbook(self, *, payload: dict[str, Any], intake_dir: Path) -> Path:
        file_path = Path(str(payload.get("file_path") or "").strip()).expanduser()
        if file_path.exists():
            return file_path
        encoded = str(payload.get("file_content_base64") or "").strip()
        if not encoded:
            raise ValueError("file_path or file_content_base64 is required")
        filename = str(payload.get("filename") or "uploaded.xlsx").strip() or "uploaded.xlsx"
        target_path = intake_dir / filename
        target_path.write_bytes(base64.b64decode(encoded))
        return target_path

    def _process_contact_row(
        self,
        *,
        intake_dir: Path,
        logger: AssetLogger,
        contact: dict[str, Any],
        inventory: dict[str, Any],
    ) -> dict[str, Any]:
        profile_url = str(contact.get("linkedin_url") or "").strip()
        exact_candidate = _find_exact_local_linkedin_match(contact, inventory) if profile_url else None
        if exact_candidate is not None:
            return self._resolve_local_exact_hit(
                intake_dir=intake_dir,
                logger=logger,
                contact=contact,
                candidate=exact_candidate,
                match_reason="linkedin_url",
            )

        exact_candidate = _find_exact_local_structured_match(contact, inventory)
        if exact_candidate is not None:
            exact_match_reason = _exact_match_reason(contact, exact_candidate)
            return self._resolve_local_exact_hit(
                intake_dir=intake_dir,
                logger=logger,
                contact=contact,
                candidate=exact_candidate,
                match_reason=exact_match_reason,
            )

        if profile_url:
            fetched_payload, fetch_errors = self._fetch_profiles(
                profile_urls=[profile_url],
                intake_dir=intake_dir,
                row_key=str(contact.get("row_key") or ""),
                logger=logger,
            )
            fetched = fetched_payload.get(profile_url)
            if fetched is not None:
                candidate, evidence = self._persist_contact_profile(contact=contact, fetched_payload=fetched)
                return {
                    "result": {
                        **contact,
                        "status": "fetched_direct_linkedin",
                        "matched_candidate": candidate.to_record(),
                        "fetched_profile": _compact_profile_payload(fetched),
                        "fetch_errors": fetch_errors,
                    },
                    "persisted_candidate_count": 1,
                    "persisted_evidence_count": len(evidence),
                    "persisted_candidate": candidate,
                }

        local_near_matches = _find_local_near_matches(contact, inventory)
        if local_near_matches:
            result = {
                **contact,
                "status": "manual_review_local",
                "manual_review_candidates": local_near_matches,
                "match_reason": "local_near_match_candidates",
            }
            if profile_url:
                result["linkedin_fetch_attempted"] = True
            return {
                "result": result,
                "persisted_candidate_count": 0,
                "persisted_evidence_count": 0,
            }

        return self._resolve_contact_via_search(
            intake_dir=intake_dir,
            logger=logger,
            contact=contact,
        )

    def _continue_contact_row_review(
        self,
        *,
        intake_dir: Path,
        logger: AssetLogger,
        contact: dict[str, Any],
        previous_result: dict[str, Any],
        decision: dict[str, Any],
        inventory: dict[str, Any],
    ) -> dict[str, Any]:
        previous_status = str(previous_result.get("status") or "").strip()
        if previous_status not in {"manual_review_local", "manual_review_search", "unresolved"}:
            return {
                "result": {
                    **contact,
                    "status": "invalid",
                    "previous_status": previous_status,
                    "reason": "row_not_reviewable",
                },
                "persisted_candidate_count": 0,
                "persisted_evidence_count": 0,
            }

        action = str(decision.get("action") or "").strip().lower()
        if action in {"select_local_candidate", "select_local", "use_local_candidate"}:
            selected_candidate_id = str(decision.get("selected_candidate_id") or "").strip()
            if not selected_candidate_id:
                return {
                    "result": {
                        **contact,
                        "status": "invalid",
                        "reason": "selected_candidate_id_required",
                    },
                    "persisted_candidate_count": 0,
                    "persisted_evidence_count": 0,
                }
            selected_candidate = self.store.get_candidate(selected_candidate_id)
            if selected_candidate is None:
                selected_candidate = _candidate_from_manual_review_candidates(previous_result, selected_candidate_id)
            if selected_candidate is None:
                return {
                    "result": {
                        **contact,
                        "status": "invalid",
                        "reason": "candidate_not_found",
                        "selected_candidate_id": selected_candidate_id,
                    },
                    "persisted_candidate_count": 0,
                    "persisted_evidence_count": 0,
                }
            resolved = self._resolve_local_exact_hit(
                intake_dir=intake_dir,
                logger=logger,
                contact=contact,
                candidate=selected_candidate,
                match_reason="manual_review_selected_local",
            )
            result_payload = dict(resolved["result"])
            result_payload["status"] = "resolved_manual_review_local"
            result_payload["selected_candidate_id"] = selected_candidate_id
            resolved["result"] = result_payload
            return resolved

        if action in {"select_search_profile", "select_profile", "fetch_selected_profile"}:
            selected_profile_url = str(decision.get("selected_profile_url") or "").strip()
            if not selected_profile_url:
                return {
                    "result": {
                        **contact,
                        "status": "invalid",
                        "reason": "selected_profile_url_required",
                    },
                    "persisted_candidate_count": 0,
                    "persisted_evidence_count": 0,
                }
            fetched_payload, fetch_errors = self._fetch_profiles(
                profile_urls=[selected_profile_url],
                intake_dir=intake_dir,
                row_key=str(contact.get("row_key") or ""),
                logger=logger,
            )
            fetched = fetched_payload.get(selected_profile_url)
            if fetched is None:
                return {
                    "result": {
                        **contact,
                        "status": "unresolved",
                        "selected_profile_url": selected_profile_url,
                        "fetch_errors": fetch_errors,
                        "reason": "selected_profile_fetch_failed",
                    },
                    "persisted_candidate_count": 0,
                    "persisted_evidence_count": 0,
                }
            candidate, evidence = self._persist_contact_profile(contact=contact, fetched_payload=fetched)
            return {
                "result": {
                    **contact,
                    "status": "resolved_manual_review_search",
                    "matched_candidate": candidate.to_record(),
                    "selected_profile_url": selected_profile_url,
                    "fetched_profile": _compact_profile_payload(fetched),
                    "fetch_errors": fetch_errors,
                },
                "persisted_candidate_count": 1,
                "persisted_evidence_count": len(evidence),
                "persisted_candidate": candidate,
            }

        if action in {"continue_search", "search", "reject_local_and_search", "fetch_or_search"}:
            exact_candidate = _find_exact_local_match(contact, inventory)
            if exact_candidate is not None:
                resolved = self._resolve_local_exact_hit(
                    intake_dir=intake_dir,
                    logger=logger,
                    contact=contact,
                    candidate=exact_candidate,
                    match_reason="continuation_exact_hit",
                )
                result_payload = dict(resolved["result"])
                result_payload["status"] = "resolved_manual_review_local"
                result_payload["search_continuation_reused_local"] = True
                resolved["result"] = result_payload
                return resolved
            return self._resolve_contact_via_search(
                intake_dir=intake_dir,
                logger=logger,
                contact=contact,
            )

        return {
            "result": {
                **contact,
                "status": "invalid",
                "reason": "unsupported_action",
                "action": action,
            },
            "persisted_candidate_count": 0,
            "persisted_evidence_count": 0,
        }

    def _resolve_local_exact_hit(
        self,
        *,
        intake_dir: Path,
        logger: AssetLogger,
        contact: dict[str, Any],
        candidate: Candidate,
        match_reason: str,
    ) -> dict[str, Any]:
        candidate, repaired = _repair_stale_target_company_mismatch(contact, candidate)
        profile_url = str(contact.get("linkedin_url") or "").strip()
        if _should_enrich_local_exact_match(contact, candidate, match_reason=match_reason):
            fetched_payload, fetch_errors = self._fetch_profiles(
                profile_urls=[profile_url],
                intake_dir=intake_dir,
                row_key=str(contact.get("row_key") or ""),
                logger=logger,
            )
            fetched = fetched_payload.get(profile_url)
            if fetched is not None:
                enriched_candidate, evidence = self._persist_contact_profile(
                    contact=contact,
                    fetched_payload=fetched,
                    seed_candidate=candidate,
                )
                return {
                    "result": {
                        **contact,
                        "status": "fetched_direct_linkedin",
                        "matched_candidate": enriched_candidate.to_record(),
                        "fetched_profile": _compact_profile_payload(fetched),
                        "fetch_errors": fetch_errors,
                        "local_exact_match_reason": match_reason,
                        "enriched_local_exact_hit": True,
                    },
                    "persisted_candidate_count": 1,
                    "persisted_evidence_count": len(evidence),
                    "persisted_candidate": enriched_candidate,
                }
            self.store.upsert_candidate(candidate)
            return {
                "result": {
                    **contact,
                    "status": "local_exact_hit",
                    "matched_candidate": candidate.to_record(),
                    "match_reason": match_reason,
                    "enrichment_attempted": True,
                    "fetch_errors": fetch_errors,
                },
                "persisted_candidate_count": 0,
                "persisted_evidence_count": 0,
            }

        self.store.upsert_candidate(candidate)
        return {
            "result": {
                **contact,
                "status": "local_exact_hit",
                "matched_candidate": candidate.to_record(),
                "match_reason": match_reason,
                "repaired_local_exact_candidate": repaired,
            },
            "persisted_candidate_count": 0,
            "persisted_evidence_count": 0,
        }

    def _fetch_profiles(
        self,
        *,
        profile_urls: list[str],
        intake_dir: Path,
        row_key: str,
        logger: AssetLogger,
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        source_shards_by_url = {
            str(profile_url or "").strip(): [f"excel_intake:{row_key}"]
            for profile_url in profile_urls
            if str(profile_url or "").strip()
        }
        return self.profile_completion_manager._fetch_profile_batches(  # noqa: SLF001
            profile_urls,
            snapshot_dir=intake_dir,
            logger=logger,
            use_cache=True,
            source_shards_by_url=source_shards_by_url,
        )

    def _search_contact(
        self,
        *,
        contact: dict[str, Any],
        intake_dir: Path,
        logger: AssetLogger,
    ) -> dict[str, Any]:
        if not self.harvest_profile_search_connector.settings.enabled:
            return {
                "status": "skipped",
                "reason": "harvest_profile_search_disabled",
                "manual_review_candidates": [],
            }

        search_root = intake_dir / "search" / str(contact.get("row_key") or "row")
        search_root.mkdir(parents=True, exist_ok=True)
        identity = resolve_company_identity(str(contact.get("company") or "").strip())
        query_text = _build_contact_search_query(contact)
        if not query_text:
            return {
                "status": "skipped",
                "reason": "search_query_missing_name",
                "manual_review_candidates": [],
            }
        search_rows: list[dict[str, Any]] = []
        attempts: list[dict[str, Any]] = []
        for employment_status in ("current", "former"):
            filter_hints = _build_contact_search_filter_hints(contact, identity, employment_status=employment_status)
            result = self.harvest_profile_search_connector.search_profiles(
                query_text=query_text,
                filter_hints=filter_hints,
                employment_status=employment_status,
                discovery_dir=search_root,
                asset_logger=logger,
                limit=10,
                pages=1,
                auto_probe=False,
            )
            rows = list((result or {}).get("rows") or [])
            search_rows.extend(rows)
            attempts.append(
                {
                    "employment_status": employment_status,
                    "query_text": query_text,
                    "filter_hints": filter_hints,
                    "returned_count": len(rows),
                    "pagination": dict((result or {}).get("pagination") or {}),
                    "raw_path": str((result or {}).get("raw_path") or ""),
                }
            )
        ranked_rows = _rank_search_rows_for_contact(contact, search_rows)
        auto_fetch_profile_url = ""
        if ranked_rows:
            top = ranked_rows[0]
            if float(top.get("match_score") or 0.0) >= _SEARCH_AUTO_FETCH_THRESHOLD and str(top.get("profile_url") or "").strip():
                auto_fetch_profile_url = str(top.get("profile_url") or "").strip()
        return {
            "status": "completed",
            "attempts": attempts,
            "ranked_candidates": ranked_rows[:_MAX_SEARCH_ROWS_PER_CONTACT],
            "manual_review_candidates": ranked_rows[:_MAX_SEARCH_ROWS_PER_CONTACT] if not auto_fetch_profile_url else [],
            "auto_fetch_profile_url": auto_fetch_profile_url,
        }

    def _resolve_contact_via_search(
        self,
        *,
        intake_dir: Path,
        logger: AssetLogger,
        contact: dict[str, Any],
    ) -> dict[str, Any]:
        search_result = self._search_contact(contact=contact, intake_dir=intake_dir, logger=logger)
        auto_fetch_url = str(search_result.get("auto_fetch_profile_url") or "").strip()
        if auto_fetch_url:
            fetched_payload, fetch_errors = self._fetch_profiles(
                profile_urls=[auto_fetch_url],
                intake_dir=intake_dir,
                row_key=str(contact.get("row_key") or ""),
                logger=logger,
            )
            fetched = fetched_payload.get(auto_fetch_url)
            if fetched is not None:
                candidate, evidence = self._persist_contact_profile(contact=contact, fetched_payload=fetched)
                return {
                    "result": {
                        **contact,
                        "status": "fetched_via_search",
                        "matched_candidate": candidate.to_record(),
                        "search_result": search_result,
                        "fetched_profile": _compact_profile_payload(fetched),
                        "fetch_errors": fetch_errors,
                    },
                    "persisted_candidate_count": 1,
                    "persisted_evidence_count": len(evidence),
                    "persisted_candidate": candidate,
                }

        search_candidates = list(search_result.get("manual_review_candidates") or [])
        if search_candidates:
            return {
                "result": {
                    **contact,
                    "status": "manual_review_search",
                    "search_result": search_result,
                    "manual_review_candidates": search_candidates,
                    "match_reason": "search_candidates_require_review",
                },
                "persisted_candidate_count": 0,
                "persisted_evidence_count": 0,
            }

        return {
            "result": {
                **contact,
                "status": "unresolved",
                "search_result": search_result,
            },
            "persisted_candidate_count": 0,
            "persisted_evidence_count": 0,
        }

    def _persist_contact_profile(
        self,
        *,
        contact: dict[str, Any],
        fetched_payload: dict[str, Any],
        seed_candidate: Candidate | None = None,
    ) -> tuple[Candidate, list[EvidenceRecord]]:
        profile = dict(fetched_payload.get("parsed") or {})
        raw_path = Path(str(fetched_payload.get("raw_path") or ""))
        requested_company = str(contact.get("company") or "").strip() or str(profile.get("current_company") or "").strip()
        identity = resolve_company_identity(requested_company)
        target_company = str(identity.canonical_name or requested_company).strip() or requested_company
        organization = requested_company or str(profile.get("current_company") or "").strip() or target_company
        base_name = str(contact.get("name") or profile.get("full_name") or "").strip()
        current_company = str(profile.get("current_company") or "").strip()
        current_identity = resolve_company_identity(current_company) if current_company else None
        company_match = _company_identity_matches(
            requested_company=requested_company,
            requested_identity=identity,
            current_company=current_company,
            current_identity=current_identity,
        )
        seed_patch = normalize_candidate(
            Candidate(
                candidate_id=(seed_candidate.candidate_id if isinstance(seed_candidate, Candidate) else make_candidate_id(base_name, organization, target_company)),
                name_en=base_name,
                display_name=format_display_name(base_name, ""),
                category=(seed_candidate.category if isinstance(seed_candidate, Candidate) else "lead"),
                target_company=(seed_candidate.target_company if isinstance(seed_candidate, Candidate) else target_company),
                organization=(seed_candidate.organization if isinstance(seed_candidate, Candidate) else organization),
                employment_status=(seed_candidate.employment_status if isinstance(seed_candidate, Candidate) else ""),
                role=str(contact.get("title") or "").strip() or (seed_candidate.role if isinstance(seed_candidate, Candidate) else ""),
                linkedin_url=str(contact.get("linkedin_url") or (seed_candidate.linkedin_url if isinstance(seed_candidate, Candidate) else "")).strip(),
                source_dataset=(seed_candidate.source_dataset if isinstance(seed_candidate, Candidate) else "excel_intake"),
                source_path=str(contact.get("source_path") or (seed_candidate.source_path if isinstance(seed_candidate, Candidate) else "")).strip(),
                metadata={
                    **(dict(seed_candidate.metadata or {}) if isinstance(seed_candidate, Candidate) else {}),
                    "excel_intake_row_key": str(contact.get("row_key") or ""),
                    "excel_uploaded_company": requested_company,
                    "excel_uploaded_title": str(contact.get("title") or "").strip(),
                    "excel_uploaded_email": str(contact.get("email") or "").strip(),
                },
            )
        )
        effective_seed_candidate = (
            _merge_profile_candidate_records(seed_candidate, seed_patch)
            if isinstance(seed_candidate, Candidate)
            else seed_patch
        )
        incoming_candidate = normalize_candidate(
            Candidate(
                candidate_id=effective_seed_candidate.candidate_id,
                name_en=str(profile.get("full_name") or effective_seed_candidate.name_en).strip() or effective_seed_candidate.name_en,
                display_name=format_display_name(
                    str(profile.get("full_name") or effective_seed_candidate.name_en).strip() or effective_seed_candidate.name_en,
                    "",
                ),
                category="employee" if company_match else "lead",
                target_company=effective_seed_candidate.target_company,
                organization=str(profile.get("current_company") or organization or effective_seed_candidate.organization).strip() or effective_seed_candidate.organization,
                employment_status="current" if company_match else effective_seed_candidate.employment_status,
                role=str(profile.get("headline") or effective_seed_candidate.role).strip() or effective_seed_candidate.role,
                team=effective_seed_candidate.team,
                focus_areas=str(profile.get("headline") or effective_seed_candidate.focus_areas).strip() or effective_seed_candidate.focus_areas,
                education=_format_education(list(profile.get("education") or [])),
                work_history=_format_experience(list(profile.get("experience") or [])),
                notes=_join_nonempty(
                    effective_seed_candidate.notes if not _candidate_looks_like_roster_baseline(effective_seed_candidate) else "",
                    str(profile.get("summary") or "").strip(),
                    str(profile.get("location") or "").strip(),
                ),
                linkedin_url=str(profile.get("profile_url") or effective_seed_candidate.linkedin_url).strip() or effective_seed_candidate.linkedin_url,
                source_dataset="excel_intake",
                source_path=str(raw_path),
                metadata={
                    **dict(effective_seed_candidate.metadata or {}),
                    "public_identifier": str(profile.get("public_identifier") or "").strip(),
                    "profile_location": str(profile.get("location") or "").strip(),
                    "headline": str(profile.get("headline") or "").strip(),
                    "summary": str(profile.get("summary") or "").strip(),
                    "languages": _format_profile_languages(list(profile.get("languages") or [])),
                    "skills": _format_profile_skills(list(profile.get("skills") or [])),
                    "excel_intake_source": "harvest_profile_scraper",
                    "target_company_mismatch": (not company_match) if current_company else False,
                },
            )
        )
        candidate = _merge_profile_candidate_records(effective_seed_candidate, incoming_candidate)
        upload_evidence = EvidenceRecord(
            evidence_id=make_evidence_id(candidate.candidate_id, "excel_upload_row", "Excel upload row", str(contact.get("row_key") or "")),
            candidate_id=candidate.candidate_id,
            source_type="excel_upload_row",
            title="Excel upload row",
            url=str(contact.get("linkedin_url") or "").strip(),
            summary=_build_contact_summary(contact),
            source_dataset="excel_upload_row",
            source_path=str(contact.get("source_path") or ""),
            metadata={
                "row_key": str(contact.get("row_key") or ""),
                "email": str(contact.get("email") or "").strip(),
            },
        )
        profile_evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                candidate.candidate_id,
                "excel_intake_linkedin_profile",
                str(profile.get("headline") or "LinkedIn profile detail"),
                str(profile.get("profile_url") or raw_path),
            ),
            candidate_id=candidate.candidate_id,
            source_type="excel_intake_linkedin_profile",
            title=str(profile.get("headline") or "LinkedIn profile detail"),
            url=str(profile.get("profile_url") or "").strip(),
            summary=f"Excel intake fetched LinkedIn profile detail for {candidate.display_name}.",
            source_dataset="excel_intake_linkedin_profile",
            source_path=str(raw_path),
            metadata={
                "public_identifier": str(profile.get("public_identifier") or "").strip(),
            },
        )
        self.store.upsert_candidate(candidate)
        self.store.upsert_evidence_records([upload_evidence, profile_evidence])
        return candidate, [upload_evidence, profile_evidence]


def _build_schema_payload(*, workbook_path: Path, sheet_rows: dict[str, list[dict[str, str]]]) -> dict[str, Any]:
    sheets: list[dict[str, Any]] = []
    for sheet_name, rows in sheet_rows.items():
        headers = list(rows[0].keys()) if rows else []
        sheets.append(
            {
                "sheet_name": sheet_name,
                "headers": headers,
                "row_count": len(rows),
                "sample_rows": rows[:5],
            }
        )
    return {
        "filename": workbook_path.name,
        "workbook_path": str(workbook_path),
        "sheets": sheets,
    }


def _normalize_schema_payload(payload: dict[str, Any], *, fallback_payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    if not normalized.get("selected_sheets") and isinstance(normalized.get("selected_sheet"), dict):
        normalized["selected_sheets"] = [dict(normalized.get("selected_sheet") or {})]
    selected_sheets = []
    for raw_sheet in list(normalized.get("selected_sheets") or []):
        if not isinstance(raw_sheet, dict):
            continue
        sheet_name = str(raw_sheet.get("sheet_name") or "").strip()
        raw_mapping = dict(raw_sheet.get("column_mapping") or {})
        column_mapping = {
            field_name: str(raw_mapping.get(field_name) or "").strip()
            for field_name in ("name", "company", "title", "linkedin_url", "email")
        }
        if not sheet_name:
            continue
        selected_sheets.append(
            {
                "sheet_name": sheet_name,
                "column_mapping": column_mapping,
                "confidence_label": str(raw_sheet.get("confidence_label") or "medium").strip().lower() or "medium",
                "notes": str(raw_sheet.get("notes") or "").strip(),
            }
        )
    contacts_detected = bool(normalized.get("contacts_detected")) and bool(selected_sheets)
    if not contacts_detected:
        normalized = _fallback_schema_from_payload(fallback_payload)
        selected_sheets = list(normalized.get("selected_sheets") or [])
        contacts_detected = bool(normalized.get("contacts_detected"))
    return {
        "contacts_detected": contacts_detected,
        "selected_sheets": selected_sheets,
        "ignored_sheets": [str(item or "").strip() for item in list(normalized.get("ignored_sheets") or []) if str(item or "").strip()],
        "notes": str(normalized.get("notes") or "").strip(),
    }


def _fallback_schema_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    selected_sheets: list[dict[str, Any]] = []
    ignored_sheets: list[str] = []
    for raw_sheet in list(payload.get("sheets") or []):
        if not isinstance(raw_sheet, dict):
            continue
        headers = [str(item or "").strip() for item in list(raw_sheet.get("headers") or []) if str(item or "").strip()]
        sheet_name = str(raw_sheet.get("sheet_name") or "").strip()
        if not headers or not sheet_name:
            continue
        mapping = _infer_header_mapping(headers)
        if mapping.get("name") and (mapping.get("company") or mapping.get("title") or mapping.get("linkedin_url")):
            selected_sheets.append(
                {
                    "sheet_name": sheet_name,
                    "column_mapping": mapping,
                    "confidence_label": "high" if sum(1 for value in mapping.values() if value) >= 4 else "medium",
                    "notes": "deterministic_header_match",
                }
            )
        else:
            ignored_sheets.append(sheet_name)
    return {
        "contacts_detected": bool(selected_sheets),
        "selected_sheets": selected_sheets,
        "ignored_sheets": ignored_sheets,
        "notes": "Deterministic spreadsheet schema inference fallback.",
    }


def _infer_header_mapping(headers: list[str]) -> dict[str, str]:
    normalized_headers = {_normalize_label(header): header for header in headers}
    aliases = {
        "name": ("name", "full name", "candidate", "person", "姓名"),
        "company": ("company", "organization", "org", "employer", "firm", "公司"),
        "title": ("title", "job title", "role", "position", "headline", "职位"),
        "linkedin_url": ("linkedin", "linkedin url", "linkedin profile", "profile url", "linkedin链接", "领英"),
        "email": ("email", "email address", "mail", "邮箱"),
    }
    mapping: dict[str, str] = {}
    for field_name, candidates in aliases.items():
        matched = ""
        for alias in candidates:
            normalized_alias = _normalize_label(alias)
            for normalized_header, raw_header in normalized_headers.items():
                if normalized_header == normalized_alias or normalized_alias in normalized_header:
                    matched = raw_header
                    break
            if matched:
                break
        mapping[field_name] = matched
    return mapping


def _extract_contacts_from_workbook(
    *,
    sheet_rows: dict[str, list[dict[str, str]]],
    schema_payload: dict[str, Any],
    workbook_path: Path,
) -> list[dict[str, Any]]:
    contacts: list[dict[str, Any]] = []
    for sheet_payload in list(schema_payload.get("selected_sheets") or []):
        sheet_name = str(sheet_payload.get("sheet_name") or "").strip()
        column_mapping = dict(sheet_payload.get("column_mapping") or {})
        rows = list(sheet_rows.get(sheet_name) or [])
        for row_index, row in enumerate(rows, start=1):
            contact = {
                "row_key": f"{sheet_name}#{row_index}",
                "sheet_name": sheet_name,
                "row_index": row_index,
                "name": str(row.get(str(column_mapping.get("name") or ""), "")).strip(),
                "company": str(row.get(str(column_mapping.get("company") or ""), "")).strip(),
                "title": str(row.get(str(column_mapping.get("title") or ""), "")).strip(),
                "linkedin_url": str(row.get(str(column_mapping.get("linkedin_url") or ""), "")).strip(),
                "email": str(row.get(str(column_mapping.get("email") or ""), "")).strip(),
                "source_path": f"{workbook_path}#{sheet_name}:{row_index}",
                "raw_row": {str(key): str(value) for key, value in row.items()},
            }
            if not any(contact.get(field_name) for field_name in ("name", "company", "title", "linkedin_url", "email")):
                continue
            contacts.append(contact)
    return contacts


def _build_local_candidate_inventory(runtime_dir: Path, store: SQLiteStore) -> dict[str, Any]:
    candidates_by_key: dict[str, Candidate] = {}

    def _ingest(candidate: Candidate) -> None:
        key = _inventory_candidate_key(candidate)
        existing = candidates_by_key.get(key)
        if existing is None or _candidate_richness_score(candidate) > _candidate_richness_score(existing):
            candidates_by_key[key] = candidate

    for candidate in store.list_candidates():
        _ingest(candidate)

    company_assets_dir = runtime_dir / "company_assets"
    if company_assets_dir.exists():
        materialized_paths = list(company_assets_dir.rglob("normalized_artifacts/materialized_candidate_documents.json"))
        for payload_path in materialized_paths:
            payload = _read_json_dict(payload_path)
            for item in list(payload.get("candidates") or []):
                candidate = _candidate_from_payload(item)
                if candidate is not None:
                    _ingest(candidate)
        for payload_path in company_assets_dir.rglob("candidate_documents.json"):
            if payload_path.parent.name == "normalized_artifacts":
                continue
            normalized_peer = payload_path.parent / "normalized_artifacts" / "materialized_candidate_documents.json"
            if normalized_peer.exists():
                continue
            payload = _read_json_dict(payload_path)
            for item in list(payload.get("candidates") or []):
                candidate = _candidate_from_payload(item)
                if candidate is not None:
                    _ingest(candidate)

    candidates = list(candidates_by_key.values())
    linkedin_index: dict[str, Candidate] = {}
    linkedin_slug_index: dict[str, Candidate] = {}
    email_index: dict[str, Candidate] = {}
    name_index: dict[str, list[Candidate]] = {}
    name_company_index: dict[tuple[str, str], list[Candidate]] = {}
    for candidate in candidates:
        linkedin_key = _normalize_linkedin_lookup_key(candidate.linkedin_url)
        if linkedin_key:
            existing = linkedin_index.get(linkedin_key)
            if existing is None or _candidate_richness_score(candidate) > _candidate_richness_score(existing):
                linkedin_index[linkedin_key] = candidate
        for slug_key in _candidate_linkedin_slugs(candidate):
            existing = linkedin_slug_index.get(slug_key)
            if existing is None or _candidate_richness_score(candidate) > _candidate_richness_score(existing):
                linkedin_slug_index[slug_key] = candidate
        for email_key in _candidate_email_keys(candidate):
            existing = email_index.get(email_key)
            if existing is None or _candidate_richness_score(candidate) > _candidate_richness_score(existing):
                email_index[email_key] = candidate
        name_key = _normalize_person_label(candidate.display_name or candidate.name_en)
        if name_key:
            name_index.setdefault(name_key, []).append(candidate)
            for company_key in _candidate_company_keys(candidate):
                name_company_index.setdefault((name_key, company_key), []).append(candidate)
    return {
        "candidates": candidates,
        "candidate_count": len(candidates),
        "linkedin_index": linkedin_index,
        "linkedin_index_count": len(linkedin_index),
        "linkedin_slug_index": linkedin_slug_index,
        "linkedin_slug_index_count": len(linkedin_slug_index),
        "email_index": email_index,
        "email_index_count": len(email_index),
        "name_index": name_index,
        "name_index_count": len(name_index),
        "name_company_index": name_company_index,
    }


def _update_inventory_with_candidate(inventory: dict[str, Any], candidate: Candidate) -> None:
    candidates = list(inventory.get("candidates") or [])
    existing_key = _inventory_candidate_key(candidate)
    replaced = False
    for index, current in enumerate(candidates):
        if _inventory_candidate_key(current) == existing_key:
            candidates[index] = candidate
            replaced = True
            break
    if not replaced:
        candidates.append(candidate)
    inventory.clear()
    inventory.update(_build_local_candidate_inventory_from_candidates(candidates))


def _build_local_candidate_inventory_from_candidates(
    candidates: list[Candidate],
) -> dict[str, Any]:
    linkedin_index: dict[str, Candidate] = {}
    linkedin_slug_index: dict[str, Candidate] = {}
    email_index: dict[str, Candidate] = {}
    name_index: dict[str, list[Candidate]] = {}
    name_company_index: dict[tuple[str, str], list[Candidate]] = {}
    for candidate in candidates:
        linkedin_key = _normalize_linkedin_lookup_key(candidate.linkedin_url)
        if linkedin_key:
            existing = linkedin_index.get(linkedin_key)
            if existing is None or _candidate_richness_score(candidate) > _candidate_richness_score(existing):
                linkedin_index[linkedin_key] = candidate
        for slug_key in _candidate_linkedin_slugs(candidate):
            existing = linkedin_slug_index.get(slug_key)
            if existing is None or _candidate_richness_score(candidate) > _candidate_richness_score(existing):
                linkedin_slug_index[slug_key] = candidate
        for email_key in _candidate_email_keys(candidate):
            existing = email_index.get(email_key)
            if existing is None or _candidate_richness_score(candidate) > _candidate_richness_score(existing):
                email_index[email_key] = candidate
        name_key = _normalize_person_label(candidate.display_name or candidate.name_en)
        if name_key:
            name_index.setdefault(name_key, []).append(candidate)
            for company_key in _candidate_company_keys(candidate):
                name_company_index.setdefault((name_key, company_key), []).append(candidate)
    return {
        "candidates": candidates,
        "candidate_count": len(candidates),
        "linkedin_index": linkedin_index,
        "linkedin_index_count": len(linkedin_index),
        "linkedin_slug_index": linkedin_slug_index,
        "linkedin_slug_index_count": len(linkedin_slug_index),
        "email_index": email_index,
        "email_index_count": len(email_index),
        "name_index": name_index,
        "name_index_count": len(name_index),
        "name_company_index": name_company_index,
    }


def _find_exact_local_match(contact: dict[str, Any], inventory: dict[str, Any]) -> Candidate | None:
    linkedin_key = _normalize_linkedin_lookup_key(contact.get("linkedin_url"))
    if linkedin_key:
        exact_by_linkedin = dict(inventory.get("linkedin_index") or {}).get(linkedin_key)
        if exact_by_linkedin is not None:
            return exact_by_linkedin
    return _find_exact_local_structured_match(contact, inventory)


def _find_exact_local_linkedin_match(contact: dict[str, Any], inventory: dict[str, Any]) -> Candidate | None:
    linkedin_key = _normalize_linkedin_lookup_key(contact.get("linkedin_url"))
    if linkedin_key:
        exact_by_linkedin = dict(inventory.get("linkedin_index") or {}).get(linkedin_key)
        if exact_by_linkedin is not None:
            return exact_by_linkedin
    return None


def _find_exact_local_structured_match(contact: dict[str, Any], inventory: dict[str, Any]) -> Candidate | None:
    for slug_key in _contact_linkedin_slugs(contact):
        exact_by_slug = dict(inventory.get("linkedin_slug_index") or {}).get(slug_key)
        if exact_by_slug is not None:
            return exact_by_slug
    email_key = _normalize_email(contact.get("email"))
    if email_key:
        exact_by_email = dict(inventory.get("email_index") or {}).get(email_key)
        if exact_by_email is not None:
            return exact_by_email
    name_key = _normalize_person_label(contact.get("name"))
    if not name_key:
        return None
    company_keys = _contact_company_keys(contact)
    candidates = []
    for company_key in company_keys:
        candidates.extend(list(dict(inventory.get("name_company_index") or {}).get((name_key, company_key)) or []))
    if not candidates:
        candidates = list(dict(inventory.get("name_index") or {}).get(name_key) or [])
    if not candidates:
        return None
    exact_title = _normalize_title(contact.get("title"))
    for candidate in candidates:
        if company_keys and not (_candidate_company_keys(candidate) & company_keys):
            continue
        if exact_title and exact_title and exact_title != _normalize_title(candidate.role):
            continue
        return candidate
    if len(candidates) == 1:
        return candidates[0]
    return None


def _exact_match_reason(contact: dict[str, Any], candidate: Candidate) -> str:
    if _normalize_linkedin_lookup_key(contact.get("linkedin_url")) and _normalize_linkedin_lookup_key(contact.get("linkedin_url")) == _normalize_linkedin_lookup_key(candidate.linkedin_url):
        return "linkedin_url"
    if _contact_linkedin_slugs(contact) & _candidate_linkedin_slugs(candidate):
        return "linkedin_slug"
    if _normalize_email(contact.get("email")) and _normalize_email(contact.get("email")) in _candidate_email_keys(candidate):
        return "email"
    return "name_company"


def _candidate_from_manual_review_candidates(previous_result: dict[str, Any], candidate_id: str) -> Candidate | None:
    for item in list(previous_result.get("manual_review_candidates") or []):
        if not isinstance(item, dict):
            continue
        payload = item.get("candidate") if isinstance(item.get("candidate"), dict) else item
        if not isinstance(payload, dict):
            continue
        if str(payload.get("candidate_id") or "").strip() != candidate_id:
            continue
        return _candidate_from_payload(payload)
    return None


def _find_local_near_matches(contact: dict[str, Any], inventory: dict[str, Any]) -> list[dict[str, Any]]:
    ranked: list[tuple[float, Candidate]] = []
    for candidate in list(inventory.get("candidates") or []):
        score = _candidate_contact_match_score_impl(contact, candidate, strict=True)
        if score < _LOCAL_NEAR_MATCH_THRESHOLD:
            continue
        ranked.append((score, candidate))
    ranked.sort(key=lambda item: (-item[0], item[1].display_name))
    results = []
    for score, candidate in ranked[:_MAX_LOCAL_NEAR_MATCHES]:
        results.append(
            {
                "match_score": round(score, 2),
                "candidate": candidate.to_record(),
                "match_reason": _build_near_match_reason(contact, candidate, score),
            }
        )
    return results


def _should_enrich_local_exact_match(contact: dict[str, Any], candidate: Candidate, *, match_reason: str) -> bool:
    profile_url = str(contact.get("linkedin_url") or "").strip()
    if not profile_url:
        return False
    contact_linkedin_key = _normalize_linkedin_lookup_key(profile_url)
    candidate_linkedin_key = _normalize_linkedin_lookup_key(candidate.linkedin_url)
    url_mismatch = bool(contact_linkedin_key and candidate_linkedin_key and contact_linkedin_key != candidate_linkedin_key)
    if match_reason == "name_company" and contact_linkedin_key and url_mismatch:
        return True
    if _candidate_looks_like_roster_baseline(candidate) and not _candidate_has_profile_detail(candidate):
        return True
    return False


def _candidate_has_profile_detail(candidate: Candidate) -> bool:
    if str(candidate.education or "").strip() or str(candidate.work_history or "").strip():
        return True
    metadata = dict(candidate.metadata or {})
    for field_name in _PROFILE_DETAIL_METADATA_FIELDS:
        value = metadata.get(field_name)
        if isinstance(value, list):
            if value:
                return True
            continue
        if str(value or "").strip():
            return True
    return False


def _candidate_looks_like_roster_baseline(candidate: Candidate) -> bool:
    dataset = str(candidate.source_dataset or "").strip().lower()
    notes = str(candidate.notes or "").strip().lower()
    metadata = dict(candidate.metadata or {})
    source_account = str(metadata.get("source_account_id") or "").strip().lower()
    return bool(
        "company roster baseline" in notes
        or dataset.endswith("_linkedin_company_people")
        or dataset == "google_linkedin_company_people"
        or source_account == "harvest_company_employees"
    )


def _repair_stale_target_company_mismatch(contact: dict[str, Any], candidate: Candidate) -> tuple[Candidate, bool]:
    metadata = dict(candidate.metadata or {})
    if not bool(metadata.get("target_company_mismatch")):
        return candidate, False
    requested_company = str(contact.get("company") or candidate.target_company or "").strip()
    candidate_company = str(candidate.organization or candidate.target_company or "").strip()
    if not requested_company or not candidate_company:
        return candidate, False
    requested_identity = resolve_company_identity(requested_company)
    candidate_identity = resolve_company_identity(candidate_company)
    if not _company_identity_matches(
        requested_company=requested_company,
        requested_identity=requested_identity,
        current_company=candidate_company,
        current_identity=candidate_identity,
    ):
        return candidate, False

    repaired_record = candidate.to_record()
    repaired_metadata = dict(metadata)
    repaired_metadata.pop("target_company_mismatch", None)
    repaired_record["metadata"] = repaired_metadata
    employment_status = str(candidate.employment_status or "").strip().lower()
    if employment_status == "former":
        repaired_record["category"] = "former_employee"
    elif employment_status == "current" or requested_identity.company_key == candidate_identity.company_key:
        repaired_record["category"] = "employee"
    elif str(candidate.category or "").strip().lower() == "non_member":
        repaired_record["category"] = "lead"
    repaired_candidate = normalize_candidate(Candidate(**repaired_record))
    return repaired_candidate, True


def _company_identity_matches(
    *,
    requested_company: str,
    requested_identity: Any,
    current_company: str,
    current_identity: Any,
) -> bool:
    requested_company_key = normalize_company_key(requested_company)
    current_company_key = normalize_company_key(current_company)
    if requested_company_key and current_company_key and requested_company_key == current_company_key:
        return True

    requested_alias_key = normalize_company_key(str(getattr(requested_identity, "company_key", "") or ""))
    current_alias_key = normalize_company_key(str(getattr(current_identity, "company_key", "") or ""))
    if requested_alias_key and current_alias_key and requested_alias_key == current_alias_key:
        return True

    requested_canonical_key = normalize_company_key(str(getattr(requested_identity, "canonical_name", "") or requested_company))
    current_canonical_key = normalize_company_key(str(getattr(current_identity, "canonical_name", "") or current_company))
    if requested_canonical_key and current_canonical_key and requested_canonical_key == current_canonical_key:
        return True
    return False


def _rank_search_rows_for_contact(contact: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        profile_url = str(row.get("profile_url") or "").strip()
        dedupe_key = profile_url or "|".join(
            [
                _normalize_person_label(row.get("full_name")),
                _normalize_person_label(row.get("current_company")),
                _normalize_title(row.get("headline")),
            ]
        )
        current = dict(deduped.get(dedupe_key) or {})
        if current and str(current.get("profile_url") or "").strip():
            continue
        deduped[dedupe_key] = dict(row)
    ranked: list[dict[str, Any]] = []
    for row in deduped.values():
        match_score = _search_row_match_score(contact, row)
        ranked.append(
            {
                **row,
                "match_score": round(match_score, 2),
            }
        )
    ranked.sort(key=lambda item: (-float(item.get("match_score") or 0.0), str(item.get("full_name") or "")))
    return ranked


def _build_contact_search_filter_hints(
    contact: dict[str, Any],
    identity: Any,
    *,
    employment_status: str,
) -> dict[str, list[str]]:
    company_url = str(getattr(identity, "linkedin_company_url", "") or "").strip()
    title = str(contact.get("title") or "").strip()
    role_bucket = normalize_requested_role_bucket(title)
    requested_facet = normalize_requested_facet(title)
    role_buckets = [item for item in [role_bucket, requested_facet] if item and item != "generalist"]
    function_ids = role_bucket_function_ids(role_buckets)
    role_hints = role_bucket_role_hints(role_buckets)
    filter_hints: dict[str, list[str]] = {
        "job_titles": [title] if title else [],
        "function_ids": function_ids[:3],
    }
    if company_url:
        if employment_status == "former":
            filter_hints["past_companies"] = [company_url]
        else:
            filter_hints["current_companies"] = [company_url]
    if role_hints and not filter_hints["job_titles"]:
        filter_hints["job_titles"] = role_hints[:2]
    return filter_hints


def _build_contact_search_query(contact: dict[str, Any]) -> str:
    parts = [
        str(contact.get("name") or "").strip(),
    ]
    return " ".join(part for part in parts if part)


def _count_row_statuses(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in rows:
        status = str(item.get("status") or "").strip()
        if not status:
            continue
        counts[status] = counts.get(status, 0) + 1
    return counts


def _candidate_from_payload(payload: dict[str, Any]) -> Candidate | None:
    if not isinstance(payload, dict):
        return None
    record = {field_name: payload.get(field_name) for field_name in _CANDIDATE_FIELD_NAMES if field_name in payload}
    if not record.get("candidate_id") or not record.get("name_en"):
        return None
    record.setdefault("name_zh", "")
    record.setdefault("display_name", format_display_name(str(record.get("name_en") or ""), str(record.get("name_zh") or "")))
    record.setdefault("category", "")
    record.setdefault("target_company", "")
    record.setdefault("organization", "")
    record.setdefault("employment_status", "")
    record.setdefault("role", "")
    record.setdefault("team", "")
    record.setdefault("joined_at", "")
    record.setdefault("left_at", "")
    record.setdefault("current_destination", "")
    record.setdefault("ethnicity_background", "")
    record.setdefault("investment_involvement", "")
    record.setdefault("focus_areas", "")
    record.setdefault("education", "")
    record.setdefault("work_history", "")
    record.setdefault("notes", "")
    record.setdefault("linkedin_url", "")
    record.setdefault("media_url", "")
    record.setdefault("source_dataset", "")
    record.setdefault("source_path", "")
    record.setdefault("metadata", dict(payload.get("metadata") or {}))
    return normalize_candidate(Candidate(**record))


def _inventory_candidate_key(candidate: Candidate) -> str:
    linkedin_key = _normalize_person_label(candidate.linkedin_url)
    if linkedin_key:
        return f"linkedin::{linkedin_key}"
    return "|".join(
        [
            _normalize_person_label(candidate.display_name or candidate.name_en),
            normalize_company_key(candidate.target_company or candidate.organization),
            _normalize_title(candidate.role),
        ]
    )


def _candidate_richness_score(candidate: Candidate) -> int:
    score = 0
    for value in (
        candidate.linkedin_url,
        candidate.role,
        candidate.team,
        candidate.focus_areas,
        candidate.education,
        candidate.work_history,
        candidate.notes,
    ):
        if str(value or "").strip():
            score += 1
    metadata = dict(candidate.metadata or {})
    for key in ("headline", "summary", "languages", "skills", "profile_location", "public_identifier"):
        value = metadata.get(key)
        if isinstance(value, list):
            if value:
                score += 1
        elif str(value or "").strip():
            score += 1
    return score


def _candidate_company_keys(candidate: Candidate) -> set[str]:
    keys = {
        normalize_company_key(candidate.target_company),
        normalize_company_key(candidate.organization),
        normalize_company_key(str(dict(candidate.metadata or {}).get("excel_uploaded_company") or "")),
    }
    return {item for item in keys if item}


def _candidate_email_keys(candidate: Candidate) -> set[str]:
    metadata = dict(candidate.metadata or {})
    keys = {
        _normalize_email(metadata.get("excel_uploaded_email")),
        _normalize_email(metadata.get("email")),
    }
    return {item for item in keys if item}


def _candidate_linkedin_slugs(candidate: Candidate) -> set[str]:
    metadata = dict(candidate.metadata or {})
    keys = {
        _normalize_linkedin_slug(metadata.get("public_identifier")),
        _normalize_linkedin_slug(candidate.linkedin_url),
    }
    return {item for item in keys if item}


def _contact_company_keys(contact: dict[str, Any]) -> set[str]:
    company = str(contact.get("company") or "").strip()
    identity = resolve_company_identity(company) if company else None
    keys = {
        normalize_company_key(company),
        normalize_company_key(str(getattr(identity, "canonical_name", "") or "")),
    }
    return {item for item in keys if item}


def _contact_linkedin_slugs(contact: dict[str, Any]) -> set[str]:
    key = _normalize_linkedin_slug(contact.get("linkedin_url"))
    return {key} if key else set()


def _candidate_contact_match_score(contact: dict[str, Any], candidate: Candidate) -> float:
    return _candidate_contact_match_score_impl(contact, candidate, strict=False)


def _candidate_contact_match_score_impl(contact: dict[str, Any], candidate: Candidate, *, strict: bool) -> float:
    score = 0.0
    contact_linkedin = _normalize_linkedin_lookup_key(contact.get("linkedin_url"))
    candidate_linkedin = _normalize_linkedin_lookup_key(candidate.linkedin_url)
    if contact_linkedin and candidate_linkedin and contact_linkedin == candidate_linkedin:
        return 100.0

    name_ratio = _string_similarity(contact.get("name"), candidate.display_name or candidate.name_en)
    title_ratio = _string_similarity(contact.get("title"), candidate.role)
    company_overlap = bool(_contact_company_keys(contact) & _candidate_company_keys(candidate))

    if strict:
        if name_ratio < _MIN_LOCAL_NEAR_MATCH_NAME_SIMILARITY:
            return 0.0
        corroboration_count = 0
        if company_overlap:
            corroboration_count += 1
        if title_ratio >= 0.45 or (
            _normalize_title(contact.get("title"))
            and _normalize_title(contact.get("title")) == _normalize_title(candidate.role)
        ):
            corroboration_count += 1
        if name_ratio >= 0.975:
            corroboration_count += 1
        if corroboration_count == 0:
            return 0.0
        if company_overlap and title_ratio < 0.2 and name_ratio < 0.95:
            return 0.0

    if name_ratio >= 0.95:
        score += 60.0
    elif name_ratio >= 0.8:
        score += 42.0
    else:
        score += name_ratio * 40.0

    if company_overlap:
        score += 25.0

    score += title_ratio * 15.0

    if _normalize_title(contact.get("title")) and _normalize_title(contact.get("title")) == _normalize_title(candidate.role):
        score += 8.0
    return score


def _search_row_match_score(contact: dict[str, Any], row: dict[str, Any]) -> float:
    score = 0.0
    name_ratio = _string_similarity(contact.get("name"), row.get("full_name"))
    if name_ratio >= 0.95:
        score += 70.0
    elif name_ratio >= 0.8:
        score += 48.0
    else:
        score += name_ratio * 45.0

    company_keys = _contact_company_keys(contact)
    row_company_key = normalize_company_key(str(row.get("current_company") or "").strip())
    if company_keys and row_company_key and row_company_key in company_keys:
        score += 15.0

    title_ratio = _string_similarity(contact.get("title"), row.get("headline"))
    score += title_ratio * 12.0
    return score


def _build_near_match_reason(contact: dict[str, Any], candidate: Candidate, score: float) -> str:
    reasons: list[str] = [f"score={round(score, 2)}"]
    if _string_similarity(contact.get("name"), candidate.display_name or candidate.name_en) >= 0.95:
        reasons.append("name_exact_or_near_exact")
    if _contact_company_keys(contact) & _candidate_company_keys(candidate):
        reasons.append("company_overlap")
    if _normalize_title(contact.get("title")) and _normalize_title(contact.get("title")) == _normalize_title(candidate.role):
        reasons.append("title_exact")
    return ",".join(reasons)


def _merge_profile_candidate_records(existing: Candidate, incoming: Candidate) -> Candidate:
    merged = existing.to_record()
    incoming_record = incoming.to_record()
    metadata = dict(existing.metadata or {})
    metadata.update(dict(incoming.metadata or {}))
    merged["metadata"] = metadata
    merged_category_preview = merge_candidate(existing, incoming).category

    if str(incoming_record.get("category") or "").strip():
        if bool(metadata.get("target_company_mismatch")) or str(incoming_record.get("category") or "").strip() == "non_member":
            merged["category"] = str(incoming_record.get("category") or "").strip()
        elif str(incoming_record.get("category") or "").strip() and (
            str(merged.get("category") or "").strip() == ""
            or str(incoming_record.get("category") or "").strip() == "employee"
        ):
            merged["category"] = str(incoming_record.get("category") or "").strip()
        elif merged_category_preview != existing.category:
            merged["category"] = merged_category_preview

    override_fields = {
        "organization",
        "employment_status",
        "role",
        "focus_areas",
        "education",
        "work_history",
        "linkedin_url",
        "source_dataset",
        "source_path",
    }
    for key, value in incoming_record.items():
        if key in {"candidate_id", "target_company", "name_zh", "team", "joined_at", "left_at", "current_destination", "ethnicity_background", "investment_involvement", "media_url", "metadata", "category"}:
            continue
        normalized_value = str(value or "").strip()
        if key == "notes":
            baseline_notes = "" if (_candidate_looks_like_roster_baseline(existing) and normalized_value) else merged.get("notes")
            merged["notes"] = _join_nonempty(baseline_notes, normalized_value)
            continue
        if key in override_fields:
            if normalized_value:
                merged[key] = value
            continue
        if not str(merged.get(key) or "").strip() and normalized_value:
            merged[key] = value
    if not str(merged.get("display_name") or "").strip():
        merged["display_name"] = format_display_name(str(merged.get("name_en") or ""), str(merged.get("name_zh") or ""))
    return normalize_candidate(Candidate(**merged))


def _compact_profile_payload(fetched_payload: dict[str, Any]) -> dict[str, Any]:
    profile = dict(fetched_payload.get("parsed") or {})
    return {
        "full_name": str(profile.get("full_name") or "").strip(),
        "headline": str(profile.get("headline") or "").strip(),
        "profile_url": str(profile.get("profile_url") or "").strip(),
        "current_company": str(profile.get("current_company") or "").strip(),
        "location": str(profile.get("location") or "").strip(),
        "raw_path": str(fetched_payload.get("raw_path") or ""),
    }


def _build_contact_summary(contact: dict[str, Any]) -> str:
    return " | ".join(
        [
            f"Name: {str(contact.get('name') or '').strip()}",
            f"Company: {str(contact.get('company') or '').strip()}",
            f"Title: {str(contact.get('title') or '').strip()}",
            f"LinkedIn: {str(contact.get('linkedin_url') or '').strip()}",
            f"Email: {str(contact.get('email') or '').strip()}",
        ]
    )


def _normalize_person_label(value: Any) -> str:
    normalized = " ".join(str(value or "").strip().lower().replace("_", " ").split())
    if not normalized:
        return ""
    normalized = re.sub(r"[^\w\s:/.-]+", "", normalized)
    normalized = normalized.rstrip("/")
    return normalized
def _normalize_linkedin_lookup_key(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    normalized = re.sub(r"[?#].*$", "", normalized).rstrip("/")
    return normalized.lower()


def _normalize_linkedin_slug(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "linkedin.com/" in raw.lower():
        match = re.search(r"linkedin\.com/(?:in|pub)/([^/?#]+)/?", raw, flags=re.IGNORECASE)
        if not match:
            return ""
        raw = str(match.group(1) or "").strip()
    return _normalize_person_label(raw)


def _normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_label(value: Any) -> str:
    normalized = " ".join(str(value or "").strip().lower().replace("_", " ").replace("-", " ").split())
    return normalized


def _normalize_title(value: Any) -> str:
    return _normalize_label(value)


def _string_similarity(left: Any, right: Any) -> float:
    left_text = _normalize_label(left)
    right_text = _normalize_label(right)
    if not left_text or not right_text:
        return 0.0
    return SequenceMatcher(a=left_text, b=right_text).ratio()


def _join_nonempty(*values: Any) -> str:
    parts: list[str] = []
    for value in values:
        text = " ".join(str(value or "").split()).strip()
        if not text:
            continue
        if text not in parts:
            parts.append(text)
    return " | ".join(parts)


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _utc_timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
