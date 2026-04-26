import io
import json
import os
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path
from unittest import mock
from urllib import request as urllib_request

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore


class ResultsApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.catalog = AssetCatalog.discover()
        self.store = ControlPlaneStore(f"{self.tempdir.name}/test.db")
        self.settings = AppSettings(
            project_root=Path(self.tempdir.name),
            runtime_dir=Path(self.tempdir.name),
            secrets_file=Path(self.tempdir.name) / "providers.local.json",
            jobs_dir=Path(self.tempdir.name) / "jobs",
            company_assets_dir=Path(self.tempdir.name) / "company_assets",
            db_path=Path(self.tempdir.name) / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.semantic_provider = LocalSemanticProvider()
        self.acquisition_engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=self.model_client,
            semantic_provider=self.semantic_provider,
            acquisition_engine=self.acquisition_engine,
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_materialized_snapshot_view(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        candidates: list[dict[str, object]],
        view: str = "canonical_merged",
        page_size: int = 50,
    ) -> Path:
        company_key = normalize_company_key(target_company)
        company_dir = Path(self.tempdir.name) / "company_assets" / company_key
        snapshot_dir = company_dir / snapshot_id
        normalized_dir = snapshot_dir / "normalized_artifacts"
        view_dir = normalized_dir if view == "canonical_merged" else (normalized_dir / view)
        (view_dir / "pages").mkdir(parents=True, exist_ok=True)
        (view_dir / "candidates").mkdir(parents=True, exist_ok=True)
        view_dir.mkdir(parents=True, exist_ok=True)

        company_identity = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": company_key,
        }
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": company_identity,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": company_identity,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (view_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "company_identity": company_identity,
                    },
                    "candidates": list(candidates or []),
                    "evidence": [],
                    "candidate_count": len(list(candidates or [])),
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (view_dir / "artifact_summary.json").write_text(
            json.dumps(
                {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "asset_view": view,
                    "candidate_count": len(list(candidates or [])),
                    "evidence_count": 0,
                    "candidate_page_size": page_size,
                    "candidate_page_count": max((len(list(candidates or [])) + page_size - 1) // page_size, 0),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        page_entries: list[dict[str, object]] = []
        shard_entries: list[dict[str, object]] = []
        publishable_lookup = {
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "asset_view": view,
            "by_candidate_id": {},
            "by_profile_url_key": {},
        }
        for index, candidate in enumerate(list(candidates or [])):
            candidate_id = str(candidate.get("candidate_id") or "").strip()
            page_number = (index // page_size) + 1 if page_size > 0 else 1
            shard_path = f"candidates/{candidate_id}.fixture.json"
            shard_entries.append(
                {
                    "candidate_id": candidate_id,
                    "fingerprint": "fixture",
                    "path": shard_path,
                    "page": page_number,
                    "display_name": str(candidate.get("display_name") or ""),
                    "status_bucket": str(candidate.get("status_bucket") or ""),
                    "has_profile_detail": bool(candidate.get("has_profile_detail")),
                    "needs_manual_review": bool(candidate.get("needs_manual_review")),
                    "needs_profile_completion": bool(candidate.get("needs_profile_completion")),
                }
            )
            (view_dir / shard_path).write_text(
                json.dumps(
                    {
                        "candidate_id": candidate_id,
                        "snapshot_id": snapshot_id,
                        "asset_view": view,
                        "materialized_candidate": dict(candidate),
                        "normalized_candidate": dict(candidate),
                        "reusable_document": {
                            "candidate_id": candidate_id,
                            "display_name": str(candidate.get("display_name") or ""),
                        },
                        "evidence": list(candidate.get("evidence") or []),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            metadata = dict(candidate.get("metadata") or {})
            primary_email = str(
                candidate.get("primary_email")
                or candidate.get("email")
                or metadata.get("primary_email")
                or metadata.get("email")
                or ""
            ).strip()
            if primary_email:
                overlay = {
                    "primary_email": primary_email,
                }
                primary_email_metadata = candidate.get("primary_email_metadata") or metadata.get(
                    "primary_email_metadata"
                )
                if isinstance(primary_email_metadata, dict) and primary_email_metadata:
                    overlay["primary_email_metadata"] = dict(primary_email_metadata)
                publishable_lookup["by_candidate_id"][candidate_id] = dict(overlay)
                linkedin_url = self.store.normalize_linkedin_profile_url(
                    str(candidate.get("linkedin_url") or "").strip()
                )
                if linkedin_url:
                    publishable_lookup["by_profile_url_key"][linkedin_url] = dict(overlay)
        for page_index in range(0, len(list(candidates or [])), page_size):
            page_number = (page_index // page_size) + 1 if page_size > 0 else 1
            page_candidates = list(candidates or [])[page_index : page_index + page_size]
            relative_path = f"pages/page-{page_number:04d}.json"
            page_entries.append(
                {
                    "page": page_number,
                    "path": relative_path,
                    "candidate_count": len(page_candidates),
                }
            )
            (view_dir / relative_path).write_text(
                json.dumps(
                    {
                        "target_company": target_company,
                        "snapshot_id": snapshot_id,
                        "asset_view": view,
                        "page": page_number,
                        "page_size": page_size,
                        "candidate_count": len(page_candidates),
                        "total_candidate_count": len(list(candidates or [])),
                        "candidates": list(page_candidates),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        (view_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "target_company": target_company,
                    "company_key": company_key,
                    "snapshot_id": snapshot_id,
                    "asset_view": view,
                    "candidate_count": len(list(candidates or [])),
                    "pagination": {
                        "page_size": page_size,
                        "page_count": len(page_entries),
                    },
                    "candidate_shards": shard_entries,
                    "pages": page_entries,
                    "backlogs": {
                        "manual_review": "backlogs/manual_review.json",
                        "profile_completion": "backlogs/profile_completion.json",
                    },
                    "auxiliary": {
                        "publishable_primary_emails": "publishable_primary_emails.json",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (view_dir / "publishable_primary_emails.json").write_text(
            json.dumps(publishable_lookup, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return snapshot_dir

    def _write_outreach_layering_analysis(
        self,
        *,
        snapshot_dir: Path,
        candidates: list[dict[str, object]],
    ) -> Path:
        analysis_dir = snapshot_dir / "layered_segmentation" / "greater_china_outreach_test"
        analysis_dir.mkdir(parents=True, exist_ok=True)
        primary_keys = [
            "layer_0_roster",
            "layer_1_name_signal",
            "layer_2_greater_china_region_experience",
            "layer_3_mainland_china_experience_or_chinese_language",
        ]
        counts = {key: 0 for key in primary_keys}
        final_distribution = {"layer_0": 0, "layer_1": 0, "layer_2": 0, "layer_3": 0}
        serialized_candidates: list[dict[str, object]] = []
        for item in candidates:
            final_layer = int(item.get("final_layer") or 0)
            distribution_key = f"layer_{final_layer}"
            if distribution_key in final_distribution:
                final_distribution[distribution_key] += 1
            counts["layer_0_roster"] += 1
            if final_layer >= 1:
                counts["layer_1_name_signal"] += 1
            if final_layer >= 2:
                counts["layer_2_greater_china_region_experience"] += 1
            if final_layer >= 3:
                counts["layer_3_mainland_china_experience_or_chinese_language"] += 1
            serialized_candidates.append(
                {
                    "candidate_id": str(item.get("candidate_id") or ""),
                    "display_name": str(item.get("display_name") or ""),
                    "final_layer": final_layer,
                    "final_layer_source": str(item.get("final_layer_source") or "deterministic"),
                }
            )
        payload = {
            "status": "completed",
            "layer_schema": {
                "primary_layer_keys": primary_keys,
            },
            "layers": {key: {"count": value} for key, value in counts.items()},
            "cumulative_layer_counts": {
                "layer_0_roster": counts["layer_0_roster"],
                "layer_1_or_higher": counts["layer_1_name_signal"],
                "layer_2_or_higher": counts["layer_2_greater_china_region_experience"],
                "layer_3_only": final_distribution["layer_3"],
            },
            "ai_verification": {"enabled": False},
            "candidates": serialized_candidates,
        }
        path = analysis_dir / "layered_analysis.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def test_job_results_use_profile_registry_raw_path_for_timeline_enrichment(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "anthropic" / "20260414T080000"
        profile_dir = snapshot_dir / "harvest_profiles"
        roster_dir = snapshot_dir / "harvest_company_employees"
        profile_dir.mkdir(parents=True, exist_ok=True)
        roster_dir.mkdir(parents=True, exist_ok=True)

        raw_profile_path = profile_dir / "dario.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "profilePicture": {
                            "url": "https://cdn.example.com/dario.jpg",
                        },
                        "emails": [
                            {
                                "email": "dario@anthropic.com",
                                "status": "valid",
                                "qualityScore": 100,
                                "foundInLinkedInProfile": True,
                            }
                        ],
                        "experience": [
                            {
                                "title": "Member of Technical Staff",
                                "companyName": "Anthropic",
                                "startDate": {"year": 2021},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "educations": [
                            {
                                "degreeName": "Bachelor",
                                "schoolName": "Caltech",
                                "fieldOfStudy": "Physics",
                                "startDate": {"year": 2007},
                                "endDate": {"year": 2011},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        roster_path = roster_dir / "harvest_company_employees_visible.json"
        roster_path.write_text(json.dumps({"items": []}, ensure_ascii=False, indent=2), encoding="utf-8")

        linkedin_url = "https://www.linkedin.com/in/dario-amodei/"
        candidate = Candidate(
            candidate_id="cand_dario",
            name_en="Dario Amodei",
            display_name="Dario Amodei",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="CEO",
            linkedin_url=linkedin_url,
            source_dataset="anthropic_linkedin_company_people",
            source_path=str(roster_path),
            metadata={
                "profile_url": linkedin_url,
                "source_path": str(roster_path),
                "function_ids": ["24"],
            },
        )
        self.store.upsert_candidate(candidate)
        self.store.mark_linkedin_profile_registry_fetched(
            linkedin_url,
            raw_path=str(raw_profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        job_id = "job_results_registry_timeline"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic做Coding方向的人",
                "query": "给我Anthropic做Coding方向的人",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "keywords": ["Coding"],
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.91,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "confidence_reason": "registry fallback",
                    "explanation": "Recovered from local profile registry raw path.",
                    "matched_fields": ["work_history"],
                    "outreach_layer": 0,
                    "outreach_layer_key": "layer_0_roster",
                    "outreach_layer_source": "deterministic",
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertEqual(
            payload["results"][0]["experience_lines"], ["2021~Present, Anthropic, Member of Technical Staff"]
        )
        self.assertEqual(payload["results"][0]["education_lines"], ["2007~2011, Bachelor, Caltech, Physics"])
        self.assertEqual(payload["results"][0]["avatar_url"], "https://cdn.example.com/dario.jpg")
        self.assertEqual(payload["results"][0]["primary_email"], "dario@anthropic.com")
        self.assertEqual(payload["results"][0]["primary_email_metadata"]["source"], "harvestapi")
        self.assertTrue(payload["results"][0]["primary_email_metadata"]["foundInLinkedInProfile"])
        self.assertEqual(payload["results"][0]["function_ids"], ["24"])
        self.assertEqual(payload["results"][0]["media_url"], "https://cdn.example.com/dario.jpg")

        detail = self.orchestrator.get_job_candidate_detail(job_id, candidate.candidate_id)
        assert detail is not None
        self.assertEqual(
            detail["candidate"]["experience_lines"],
            ["2021~Present, Anthropic, Member of Technical Staff"],
        )
        self.assertEqual(
            detail["candidate"]["education_lines"],
            ["2007~2011, Bachelor, Caltech, Physics"],
        )
        self.assertEqual(detail["candidate"]["avatar_url"], "https://cdn.example.com/dario.jpg")
        self.assertEqual(detail["candidate"]["primary_email"], "dario@anthropic.com")
        self.assertTrue(detail["candidate"]["primary_email_metadata"]["foundInLinkedInProfile"])
        self.assertEqual(detail["candidate"]["function_ids"], ["24"])

    def test_job_results_skip_ranked_results_for_full_company_asset_population_view(self) -> None:
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id="20260416T120000",
            candidates=[
                {
                    "candidate_id": "cand_asset_only",
                    "display_name": "Asset Only",
                    "name_en": "Asset Only",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "current",
                    "role": "Research Scientist",
                    "linkedin_url": "https://www.linkedin.com/in/asset-only/",
                    "function_ids": ["24"],
                }
            ],
        )
        job_id = "job_results_asset_population_only"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic的全部成员",
                "query": "给我Anthropic的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 3000,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260416T120000",
                    "asset_view": "canonical_merged",
                }
            },
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": "cand_asset_only",
                    "rank": 1,
                    "score": 0.99,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.99,
                    "confidence_reason": "not used in asset population mode",
                    "explanation": "Should be hidden when asset population is the default results view.",
                    "matched_fields": ["work_history"],
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertEqual(payload["results"], [])
        self.assertEqual(payload["ranked_results"], [])
        self.assertTrue(payload["asset_population"]["available"])
        self.assertEqual(payload["asset_population"]["candidate_count"], 1)

    def test_job_dashboard_is_summary_only_and_candidate_page_paginates_asset_population(self) -> None:
        snapshot_id = "20260417T100000"
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                {
                    "candidate_id": "cand_1",
                    "display_name": "Alice Example",
                    "name_en": "Alice Example",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "current",
                    "role": "Research Scientist",
                    "linkedin_url": "https://www.linkedin.com/in/alice-example/",
                    "function_ids": ["24"],
                },
                {
                    "candidate_id": "cand_2",
                    "display_name": "Bob Example",
                    "name_en": "Bob Example",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "current",
                    "role": "Research Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/bob-example/",
                    "function_ids": ["8"],
                },
                {
                    "candidate_id": "cand_3",
                    "display_name": "Carol Example",
                    "name_en": "Carol Example",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "former",
                    "role": "Member of Technical Staff",
                    "linkedin_url": "https://www.linkedin.com/in/carol-example/",
                    "function_ids": ["24"],
                },
            ],
        )
        job_id = "job_dashboard_summary_only"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic的全部成员",
                "query": "给我Anthropic的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 3,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 3,
                }
            },
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": "cand_1",
                    "rank": 1,
                    "score": 0.99,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.99,
                    "confidence_reason": "hidden by asset population default",
                    "explanation": "Should not be shipped in dashboard summary.",
                    "matched_fields": ["work_history"],
                }
            ],
        )

        dashboard_payload = self.orchestrator.get_job_dashboard(job_id)
        assert dashboard_payload is not None
        self.assertEqual(dashboard_payload["results"], [])
        self.assertEqual(dashboard_payload["ranked_results"], [])
        self.assertEqual(dashboard_payload["ranked_result_count"], 1)
        self.assertTrue(dashboard_payload["asset_population"]["available"])
        self.assertEqual(dashboard_payload["asset_population"]["candidate_count"], 3)
        self.assertEqual(len(dashboard_payload["asset_population"]["candidates"]), 3)
        self.assertEqual(dashboard_payload["asset_population"]["candidates"][0]["candidate_id"], "cand_1")

        page_payload = self.orchestrator.get_job_candidate_page(job_id, offset=1, limit=1)
        assert page_payload is not None
        self.assertEqual(page_payload["result_mode"], "asset_population")
        self.assertEqual(page_payload["total_candidates"], 3)
        self.assertEqual(page_payload["returned_count"], 1)
        self.assertTrue(page_payload["has_more"])
        self.assertEqual(page_payload["next_offset"], 2)
        self.assertEqual(page_payload["candidates"][0]["candidate_id"], "cand_2")
        self.assertEqual(
            page_payload["candidates"][0]["linkedin_url"],
            "https://www.linkedin.com/in/bob-example/",
        )

    def test_job_dashboard_and_candidate_page_ignore_incomplete_hot_cache_snapshot_shadow(self) -> None:
        snapshot_id = "20260417T100001"
        self._write_materialized_snapshot_view(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[
                {
                    "candidate_id": "cand_1",
                    "display_name": "Ada Example",
                    "name_en": "Ada Example",
                    "target_company": "Google",
                    "organization": "Google",
                    "employment_status": "current",
                    "role": "Research Scientist",
                    "linkedin_url": "https://www.linkedin.com/in/ada-example/",
                    "function_ids": ["24"],
                }
            ],
        )
        hot_cache_snapshot_dir = Path(self.tempdir.name) / "hot_cache_company_assets" / "google" / snapshot_id
        hot_cache_snapshot_dir.mkdir(parents=True, exist_ok=True)
        job_id = "job_dashboard_incomplete_hot_cache_shadow"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Google里做研究的人",
                "query": "给我Google里做研究的人",
                "target_company": "Google",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 1,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                }
            },
        )

        with mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(Path(self.tempdir.name) / "company_assets"),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(Path(self.tempdir.name) / "hot_cache_company_assets"),
            },
            clear=False,
        ):
            dashboard_payload = self.orchestrator.get_job_dashboard(job_id)
            page_payload = self.orchestrator.get_job_candidate_page(job_id, offset=0, limit=1, lightweight=True)

        assert dashboard_payload is not None
        assert page_payload is not None
        self.assertTrue(dashboard_payload["asset_population"]["available"])
        self.assertEqual(dashboard_payload["asset_population"]["candidate_count"], 1)
        self.assertEqual(len(dashboard_payload["asset_population"]["candidates"]), 1)
        self.assertEqual(dashboard_payload["asset_population"]["candidates"][0]["candidate_id"], "cand_1")
        self.assertEqual(page_payload["result_mode"], "asset_population")
        self.assertEqual(page_payload["total_candidates"], 1)
        self.assertEqual(page_payload["returned_count"], 1)
        self.assertEqual(page_payload["candidates"][0]["candidate_id"], "cand_1")

    def test_results_api_defaults_to_summary_only_for_asset_population_and_supports_opt_in_candidates(self) -> None:
        snapshot_id = "20260417T100500"
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                {
                    "candidate_id": "cand_1",
                    "display_name": "Alice Example",
                    "name_en": "Alice Example",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "current",
                    "role": "Research Scientist",
                    "linkedin_url": "https://www.linkedin.com/in/alice-example/",
                    "function_ids": ["24"],
                },
                {
                    "candidate_id": "cand_2",
                    "display_name": "Bob Example",
                    "name_en": "Bob Example",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "current",
                    "role": "Research Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/bob-example/",
                    "function_ids": ["8"],
                },
                {
                    "candidate_id": "cand_3",
                    "display_name": "Carol Example",
                    "name_en": "Carol Example",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "former",
                    "role": "Member of Technical Staff",
                    "linkedin_url": "https://www.linkedin.com/in/carol-example/",
                    "function_ids": ["24"],
                },
            ],
        )
        job_id = "job_results_api_summary_only"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic的全部成员",
                "query": "给我Anthropic的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 3,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 3,
                }
            },
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": "cand_1",
                    "rank": 1,
                    "score": 0.99,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.99,
                    "confidence_reason": "hidden by asset population default",
                    "explanation": "Should not be shipped in API summary mode by default.",
                    "matched_fields": ["work_history"],
                }
            ],
        )

        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            with opener.open(f"http://{host}:{port}/api/jobs/{job_id}/results") as response:
                summary_payload = json.loads(response.read().decode("utf-8"))
            with opener.open(
                f"http://{host}:{port}/api/jobs/{job_id}/results?include_candidates=1"
            ) as response:
                full_payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(summary_payload["ranked_result_count"], 1)
        self.assertEqual(summary_payload["asset_population"]["candidate_count"], 3)
        self.assertEqual(summary_payload["asset_population"]["candidates"], [])
        self.assertEqual(full_payload["asset_population"]["candidate_count"], 3)
        self.assertEqual(len(full_payload["asset_population"]["candidates"]), 3)
        self.assertEqual(full_payload["asset_population"]["candidates"][0]["candidate_id"], "cand_1")

    def test_job_candidate_page_includes_profile_timeline_preview_for_initial_asset_population_candidates(self) -> None:
        snapshot_id = "20260417T101500"
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                {
                    "candidate_id": "cand_preview",
                    "display_name": "Eve Preview",
                    "name_en": "Eve Preview",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "current",
                    "role": "Research Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/eve-preview/",
                    "source_path": "",
                    "function_ids": ["24"],
                },
            ],
        )
        raw_profile_path = snapshot_dir / "harvest_profiles" / "eve-preview.json"
        raw_profile_path.parent.mkdir(parents=True, exist_ok=True)
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "headline": "Research Engineer at Anthropic",
                        "experience": [
                            {
                                "title": "Research Engineer",
                                "companyName": "Anthropic",
                                "startDate": {"text": "2023"},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "education": [
                            {
                                "degreeName": "Bachelor",
                                "schoolName": "MIT",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"text": "2018"},
                                "endDate": {"text": "2022"},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        page_path = (
            snapshot_dir / "normalized_artifacts" / "pages" / "page-0001.json"
        )
        page_payload = json.loads(page_path.read_text(encoding="utf-8"))
        page_payload["candidates"][0]["source_path"] = str(raw_profile_path)
        page_payload["candidates"][0]["profile_capture_kind"] = "harvest_profile_detail"
        page_path.write_text(json.dumps(page_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        materialized_path = snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
        materialized_payload = json.loads(materialized_path.read_text(encoding="utf-8"))
        materialized_payload["candidates"][0]["source_path"] = str(raw_profile_path)
        materialized_payload["candidates"][0]["profile_capture_kind"] = "harvest_profile_detail"
        materialized_path.write_text(
            json.dumps(materialized_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        job_id = "job_candidate_page_timeline_preview"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic的全部成员",
                "query": "给我Anthropic的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 1,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                }
            },
        )

        payload = self.orchestrator.get_job_candidate_page(job_id, offset=0, limit=1)

        assert payload is not None
        self.assertEqual(
            payload["candidates"][0]["experience_lines"],
            ["2023~Present, Anthropic, Research Engineer"],
        )
        self.assertEqual(
            payload["candidates"][0]["education_lines"],
            ["2018~2022, Bachelor, MIT, Computer Science"],
        )

    def test_job_api_is_summary_only_by_default_and_detail_is_opt_in(self) -> None:
        job_id = "job_api_summary_default"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找 OpenAI 做 Coding 的人",
                "query": "OpenAI Coding",
                "target_company": "OpenAI",
                "target_scope": "full_company_asset",
            },
            plan_payload={"asset_reuse_plan": {"baseline_reuse_available": True}},
            summary_payload={
                "message": "Local asset population is ready.",
                "candidate_count": 26,
            },
        )
        self.store.append_job_event(
            job_id,
            "runtime_control",
            "completed",
            "Background runner finished.",
            {"detail": "Background runner finished."},
        )

        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            with opener.open(f"http://{host}:{port}/api/jobs/{job_id}") as response:
                summary_payload = json.loads(response.read().decode("utf-8"))
            with opener.open(f"http://{host}:{port}/api/jobs/{job_id}?include_details=1") as response:
                detail_payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(summary_payload["job_id"], job_id)
        self.assertEqual(summary_payload["summary"]["candidate_count"], 26)
        self.assertEqual(summary_payload["request"]["target_company"], "OpenAI")
        self.assertNotIn("events", summary_payload)
        self.assertNotIn("intent_rewrite", summary_payload)
        self.assertEqual(len(detail_payload["events"]), 1)
        self.assertIn("intent_rewrite", detail_payload)

    def test_job_candidate_page_lightweight_mode_skips_profile_timeline_enrichment(self) -> None:
        snapshot_id = "20260417T101600"
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                {
                    "candidate_id": "cand_lightweight",
                    "display_name": "Lightweight Preview",
                    "name_en": "Lightweight Preview",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "current",
                    "role": "Research Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/lightweight-preview/",
                    "source_path": "",
                    "function_ids": ["24"],
                },
            ],
        )
        raw_profile_path = snapshot_dir / "harvest_profiles" / "lightweight-preview.json"
        raw_profile_path.parent.mkdir(parents=True, exist_ok=True)
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "headline": "Research Engineer at Anthropic",
                        "experience": [
                            {
                                "title": "Research Engineer",
                                "companyName": "Anthropic",
                                "startDate": {"text": "2023"},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "education": [
                            {
                                "degreeName": "Bachelor",
                                "schoolName": "MIT",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"text": "2018"},
                                "endDate": {"text": "2022"},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        page_path = snapshot_dir / "normalized_artifacts" / "pages" / "page-0001.json"
        page_payload = json.loads(page_path.read_text(encoding="utf-8"))
        page_payload["candidates"][0]["source_path"] = str(raw_profile_path)
        page_payload["candidates"][0]["profile_capture_kind"] = "harvest_profile_detail"
        page_path.write_text(json.dumps(page_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        materialized_path = snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
        materialized_payload = json.loads(materialized_path.read_text(encoding="utf-8"))
        materialized_payload["candidates"][0]["source_path"] = str(raw_profile_path)
        materialized_payload["candidates"][0]["profile_capture_kind"] = "harvest_profile_detail"
        materialized_path.write_text(
            json.dumps(materialized_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        job_id = "job_candidate_page_lightweight"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic的全部成员",
                "query": "给我Anthropic的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 1,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                }
            },
        )

        payload = self.orchestrator.get_job_candidate_page(job_id, offset=0, limit=1, lightweight=True)

        assert payload is not None
        self.assertNotIn("experience_lines", payload["candidates"][0])
        self.assertNotIn("education_lines", payload["candidates"][0])

    def test_job_candidate_page_lightweight_mode_keeps_complete_embedded_profile_signals(self) -> None:
        snapshot_id = "20260422T124000"
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                {
                    "candidate_id": "cand_lightweight_embedded_complete",
                    "display_name": "Aaron Levin",
                    "name_en": "Aaron Levin",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "current",
                    "role": "Member of Technical Staff",
                    "headline": "Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/aaronmblevin/",
                    "source_path": "/tmp/anthropic/harvest_company_employees/harvest_company_employees_visible.json",
                    "experience_lines": [
                        "2024~Present, Anthropic, Member of Technical Staff",
                        "2022~2024, Fastly, Sr. Engineering Manager",
                    ],
                    "education_lines": [
                        "2006~2008, MSc, University of Alberta, Mathematics",
                    ],
                }
            ],
        )
        job_id = "job_candidate_page_lightweight_embedded_complete"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic的全部成员",
                "query": "给我Anthropic的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 1,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"),
                }
            },
        )

        payload = self.orchestrator.get_job_candidate_page(job_id, offset=0, limit=1, lightweight=True)

        assert payload is not None
        candidate = payload["candidates"][0]
        self.assertEqual(candidate["candidate_id"], "cand_lightweight_embedded_complete")
        self.assertEqual(
            candidate["experience_lines"],
            [
                "2024~Present, Anthropic, Member of Technical Staff",
                "2022~2024, Fastly, Sr. Engineering Manager",
            ],
        )
        self.assertEqual(
            candidate["education_lines"],
            ["2006~2008, MSc, University of Alberta, Mathematics"],
        )
        self.assertTrue(candidate["has_profile_detail"])
        self.assertFalse(candidate["needs_profile_completion"])
        self.assertFalse(candidate["low_profile_richness"])

    def test_job_candidate_page_prefers_incremental_page_artifacts_over_monolith_payload(self) -> None:
        snapshot_id = "20260414T083000"
        candidates = [
            {
                "candidate_id": "cand_1",
                "display_name": "Ada Example",
                "name_en": "Ada Example",
                "target_company": "Anthropic",
                "organization": "Anthropic",
                "employment_status": "current",
                "role": "Research Engineer",
                "linkedin_url": "https://www.linkedin.com/in/ada-example/",
                "function_ids": ["24"],
                "role_bucket": "research",
                "functional_facets": ["training"],
            },
            {
                "candidate_id": "cand_2",
                "display_name": "Ben Example",
                "name_en": "Ben Example",
                "target_company": "Anthropic",
                "organization": "Anthropic",
                "employment_status": "current",
                "role": "Research Scientist",
                "linkedin_url": "https://www.linkedin.com/in/ben-example/",
                "function_ids": ["24"],
                "role_bucket": "research",
                "functional_facets": ["pretrain"],
            },
        ]
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=candidates,
            view="canonical_merged",
        )
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=candidates,
            view="strict_roster_only",
        )

        job_id = "job_incremental_page_only"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic的全部成员",
                "query": "给我Anthropic的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 2,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 2,
                }
            },
        )

        with mock.patch(
            "sourcing_agent.orchestrator.load_company_snapshot_candidate_documents",
            side_effect=AssertionError("monolith payload should not be loaded"),
        ):
            page_payload = self.orchestrator.get_job_candidate_page(job_id, offset=1, limit=1)

        assert page_payload is not None
        self.assertEqual(page_payload["result_mode"], "asset_population")
        self.assertEqual(page_payload["returned_count"], 1)
        self.assertEqual(page_payload["total_candidates"], 2)
        self.assertEqual(page_payload["candidates"][0]["candidate_id"], "cand_2")

    def test_job_candidate_page_uses_publishable_email_lookup_artifact(self) -> None:
        snapshot_id = "20260414T084500"
        canonical_candidates = [
            {
                "candidate_id": "cand_overlay",
                "display_name": "Cara Example",
                "name_en": "Cara Example",
                "target_company": "Anthropic",
                "organization": "Anthropic",
                "employment_status": "current",
                "role": "Research Engineer",
                "linkedin_url": "https://www.linkedin.com/in/cara-example/",
                "function_ids": ["24"],
                "role_bucket": "research",
                "functional_facets": ["training"],
            }
        ]
        strict_candidates = [
            {
                **canonical_candidates[0],
                "primary_email": "cara@anthropic.com",
                "primary_email_metadata": {
                    "status": "valid",
                    "quality_score": 98,
                    "found_in_linkedin_profile": True,
                },
            }
        ]
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=canonical_candidates,
            view="canonical_merged",
        )
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=strict_candidates,
            view="strict_roster_only",
        )

        job_id = "job_publishable_overlay"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic的全部成员",
                "query": "给我Anthropic的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 1,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                }
            },
        )

        with mock.patch(
            "sourcing_agent.orchestrator.load_company_snapshot_candidate_documents",
            side_effect=AssertionError("monolith payload should not be loaded"),
        ):
            page_payload = self.orchestrator.get_job_candidate_page(job_id, offset=0, limit=1)

        assert page_payload is not None
        self.assertEqual(page_payload["returned_count"], 1)
        self.assertEqual(page_payload["candidates"][0]["primary_email"], "cara@anthropic.com")
        self.assertEqual(
            dict(page_payload["candidates"][0].get("primary_email_metadata") or {}).get("status"),
            "valid",
        )

    def test_job_candidate_detail_uses_candidate_shard_without_monolith_payload(self) -> None:
        snapshot_id = "20260414T090000"
        candidate_payload = {
            "candidate_id": "cand_detail",
            "display_name": "Dana Example",
            "name_en": "Dana Example",
            "target_company": "Anthropic",
            "organization": "Anthropic",
            "employment_status": "current",
            "role": "Research Engineer",
            "linkedin_url": "https://www.linkedin.com/in/dana-example/",
            "function_ids": ["24"],
            "role_bucket": "research",
            "functional_facets": ["post_train"],
            "experience_lines": ["2023~Present, Anthropic, Research Engineer"],
            "education_lines": ["2018~2022, Bachelor, MIT, Computer Science"],
            "evidence": [
                {
                    "candidate_id": "cand_detail",
                    "source_type": "linkedin_profile_detail",
                    "title": "Dana profile",
                    "summary": "Full profile evidence",
                }
            ],
        }
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[candidate_payload],
            view="canonical_merged",
        )
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[candidate_payload],
            view="strict_roster_only",
        )

        job_id = "job_candidate_detail_shard"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Anthropic的全部成员",
                "query": "给我Anthropic的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
            },
            plan_payload={
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": False,
                    "baseline_candidate_count": 1,
                },
                "organization_execution_profile": {
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                }
            },
        )

        with mock.patch(
            "sourcing_agent.orchestrator.load_company_snapshot_candidate_documents",
            side_effect=AssertionError("monolith payload should not be loaded"),
        ):
            detail_payload = self.orchestrator.get_job_candidate_detail(job_id, "cand_detail")

        assert detail_payload is not None
        self.assertEqual(detail_payload["candidate"]["candidate_id"], "cand_detail")
        self.assertEqual(
            detail_payload["candidate"]["experience_lines"], ["2023~Present, Anthropic, Research Engineer"]
        )
        self.assertEqual(len(detail_payload["evidence"]), 1)
        self.assertEqual(detail_payload["evidence"][0]["title"], "Dana profile")

    def test_job_results_scrub_risky_harvest_email_from_profile_timeline(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "reflectionai" / "20260414T080000"
        profile_dir = snapshot_dir / "harvest_profiles"
        roster_dir = snapshot_dir / "harvest_company_employees"
        profile_dir.mkdir(parents=True, exist_ok=True)
        roster_dir.mkdir(parents=True, exist_ok=True)

        raw_profile_path = profile_dir / "abby.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "emails": [
                            {
                                "email": "abby@reflection.ai",
                                "status": "risky",
                                "qualityScore": 60,
                            }
                        ],
                        "experience": [
                            {
                                "title": "Member of Technical Staff",
                                "companyName": "Reflection AI",
                                "startDate": {"year": 2024},
                                "endDate": {"text": "Present"},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        roster_path = roster_dir / "harvest_company_employees_visible.json"
        roster_path.write_text(json.dumps({"items": []}, ensure_ascii=False, indent=2), encoding="utf-8")

        linkedin_url = "https://www.linkedin.com/in/abby-reflection/"
        candidate = Candidate(
            candidate_id="cand_abby",
            name_en="Abby Example",
            display_name="Abby Example",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Member of Technical Staff",
            linkedin_url=linkedin_url,
            source_dataset="reflectionai_linkedin_company_people",
            source_path=str(roster_path),
            metadata={
                "profile_url": linkedin_url,
                "source_path": str(roster_path),
                "primary_email": "stale@reflection.ai",
            },
        )
        self.store.upsert_candidate(candidate)
        self.store.mark_linkedin_profile_registry_fetched(
            linkedin_url,
            raw_path=str(raw_profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        job_id = "job_results_risky_email_scrub"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Reflection AI做Post-train方向的人",
                "query": "给我Reflection AI做Post-train方向的人",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current"],
                "keywords": ["Post-train"],
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.88,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.88,
                    "confidence_reason": "registry fallback",
                    "explanation": "Recovered from local profile registry raw path.",
                    "matched_fields": ["work_history"],
                    "outreach_layer": 0,
                    "outreach_layer_key": "layer_0_roster",
                    "outreach_layer_source": "deterministic",
                    "metadata": {"primary_email": "stale@reflection.ai"},
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertNotIn("primary_email", payload["results"][0])
        self.assertNotIn("primary_email_metadata", payload["results"][0])

    def test_job_results_scrub_harvest_roster_preview_email_without_profile_detail(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "reflectionai" / "20260414T081500"
        roster_dir = snapshot_dir / "harvest_company_employees"
        roster_dir.mkdir(parents=True, exist_ok=True)

        roster_path = roster_dir / "harvest_company_employees_visible.json"
        roster_path.write_text(json.dumps({"items": []}, ensure_ascii=False, indent=2), encoding="utf-8")

        linkedin_url = "https://www.linkedin.com/in/abby-reflection/"
        candidate = Candidate(
            candidate_id="cand_abby_roster_preview",
            name_en="Abby Example",
            display_name="Abby Example",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Member of Technical Staff",
            linkedin_url=linkedin_url,
            source_dataset="reflectionai_linkedin_company_people",
            source_path=str(roster_path),
            metadata={
                "profile_url": linkedin_url,
                "source_path": str(roster_path),
                "primary_email": "abby@reflection.ai",
                "primary_email_metadata": {
                    "source": "harvestapi",
                    "status": "valid",
                    "qualityScore": 80,
                },
            },
        )
        self.store.upsert_candidate(candidate)

        job_id = "job_results_roster_preview_email_scrub"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Reflection AI做Post-train方向的人",
                "query": "给我Reflection AI做Post-train方向的人",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current"],
                "keywords": ["Post-train"],
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.76,
                    "semantic_score": 0.0,
                    "confidence_label": "medium",
                    "confidence_score": 0.76,
                    "confidence_reason": "roster preview fallback",
                    "explanation": "Recovered from roster preview payload.",
                    "matched_fields": ["work_history"],
                    "outreach_layer": 0,
                    "outreach_layer_key": "layer_0_roster",
                    "outreach_layer_source": "deterministic",
                    "metadata": {
                        "primary_email": "abby@reflection.ai",
                        "primary_email_metadata": {
                            "source": "harvestapi",
                            "status": "valid",
                            "qualityScore": 80,
                        },
                    },
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertNotIn("primary_email", payload["results"][0])
        self.assertNotIn("primary_email_metadata", payload["results"][0])

        detail = self.orchestrator.get_job_candidate_detail(job_id, candidate.candidate_id)
        assert detail is not None
        self.assertNotIn("primary_email", detail["candidate"])
        self.assertNotIn("primary_email_metadata", detail["candidate"])

    def test_job_results_scrub_external_harvest_profile_email_without_linkedin_proof(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "openai" / "20260414T082000"
        profile_dir = snapshot_dir / "harvest_profiles"
        profile_dir.mkdir(parents=True, exist_ok=True)

        raw_profile_path = profile_dir / "rohan.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "emails": [
                            {
                                "email": "rohan@periodic.com",
                                "status": "valid",
                                "qualityScore": 80,
                            }
                        ],
                        "experience": [
                            {
                                "title": "Co-founder",
                                "companyName": "Periodic Labs",
                                "startDate": {"year": 2025},
                                "endDate": {"text": "Present"},
                            },
                            {
                                "title": "Researcher",
                                "companyName": "OpenAI",
                                "startDate": {"year": 2023},
                                "endDate": {"year": 2025},
                            },
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        linkedin_url = "https://www.linkedin.com/in/rohan-pandey/"
        candidate = Candidate(
            candidate_id="cand_rohan_external_email",
            name_en="Rohan Pandey",
            display_name="Rohan Pandey",
            category="employee",
            target_company="OpenAI",
            organization="Periodic Labs",
            employment_status="former",
            role="Researcher",
            linkedin_url=linkedin_url,
            source_dataset="openai_snapshot",
            source_path=str(raw_profile_path),
            metadata={
                "profile_url": linkedin_url,
                "profile_timeline_source_path": str(raw_profile_path),
            },
        )
        self.store.upsert_candidate(candidate)
        self.store.mark_linkedin_profile_registry_fetched(
            linkedin_url,
            raw_path=str(raw_profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        job_id = "job_results_external_harvest_email_keep"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我OpenAI做Reasoning方向的人",
                "query": "给我OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "keywords": ["Reasoning"],
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.77,
                    "semantic_score": 0.0,
                    "confidence_label": "medium",
                    "confidence_score": 0.77,
                    "confidence_reason": "external domain profile detail",
                    "explanation": "Recovered from harvested profile detail.",
                    "matched_fields": ["work_history"],
                    "outreach_layer": 1,
                    "outreach_layer_key": "layer_1_former",
                    "outreach_layer_source": "deterministic",
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertNotIn("primary_email", payload["results"][0])
        self.assertNotIn("primary_email_metadata", payload["results"][0])

    def test_job_results_keep_external_harvest_profile_email_with_linkedin_proof(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "reflectionai" / "20260414T082500"
        profile_dir = snapshot_dir / "harvest_profiles"
        profile_dir.mkdir(parents=True, exist_ok=True)

        raw_profile_path = profile_dir / "raviraj.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "emails": [
                            {
                                "email": "raviraj@lsvp.com",
                                "status": "valid",
                                "qualityScore": 100,
                                "foundInLinkedInProfile": True,
                            }
                        ],
                        "experience": [
                            {
                                "title": "Partner",
                                "companyName": "Lightspeed Venture Partners",
                                "startDate": {"year": 2022},
                                "endDate": {"text": "Present"},
                            },
                            {
                                "title": "Research Scientist",
                                "companyName": "Reflection AI",
                                "startDate": {"year": 2024},
                                "endDate": {"year": 2025},
                            },
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        linkedin_url = "https://www.linkedin.com/in/raviraj-jain/"
        candidate = Candidate(
            candidate_id="cand_external_email_verified",
            name_en="Raviraj Jain",
            display_name="Raviraj Jain",
            category="employee",
            target_company="Reflection AI",
            organization="Lightspeed Venture Partners",
            employment_status="former",
            role="Research Scientist",
            linkedin_url=linkedin_url,
            source_dataset="reflectionai_snapshot",
            source_path=str(raw_profile_path),
            metadata={
                "profile_url": linkedin_url,
                "profile_timeline_source_path": str(raw_profile_path),
            },
        )
        self.store.upsert_candidate(candidate)
        self.store.mark_linkedin_profile_registry_fetched(
            linkedin_url,
            raw_path=str(raw_profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        job_id = "job_results_external_harvest_email_keep_verified"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Reflection AI做Post-train方向的人",
                "query": "给我Reflection AI做Post-train方向的人",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.79,
                    "semantic_score": 0.0,
                    "confidence_label": "medium",
                    "confidence_score": 0.79,
                    "confidence_reason": "external domain profile detail with linkedin proof",
                    "explanation": "Recovered from harvested profile detail.",
                    "matched_fields": ["work_history"],
                    "outreach_layer": 1,
                    "outreach_layer_key": "layer_1_former",
                    "outreach_layer_source": "deterministic",
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertEqual(payload["results"][0]["primary_email"], "raviraj@lsvp.com")
        self.assertEqual(payload["results"][0]["primary_email_metadata"]["source"], "harvestapi")
        self.assertEqual(payload["results"][0]["primary_email_metadata"]["qualityScore"], 100)
        self.assertTrue(payload["results"][0]["primary_email_metadata"]["foundInLinkedInProfile"])

    def test_job_results_recover_school_name_from_school_linkedin_url(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "openai" / "20260413T140350"
        profile_dir = snapshot_dir / "harvest_profiles"
        roster_dir = snapshot_dir / "harvest_company_employees"
        profile_dir.mkdir(parents=True, exist_ok=True)
        roster_dir.mkdir(parents=True, exist_ok=True)

        raw_profile_path = profile_dir / "john_hallman.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "experience": [
                            {
                                "title": "Researcher",
                                "companyName": "OpenAI",
                                "startDate": {"year": 2025},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "education": [
                            {
                                "degree": "BA",
                                "schoolName": "node",
                                "schoolLinkedinUrl": "https://www.linkedin.com/school/princeton-university/",
                                "fieldOfStudy": "null",
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        roster_path = roster_dir / "harvest_company_employees_visible.json"
        roster_path.write_text(json.dumps({"items": []}, ensure_ascii=False, indent=2), encoding="utf-8")

        linkedin_url = "https://www.linkedin.com/in/john-hallman/"
        candidate = Candidate(
            candidate_id="cand_john_hallman",
            name_en="John Hallman",
            display_name="John Hallman",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Researcher",
            linkedin_url=linkedin_url,
            source_dataset="openai_linkedin_company_people",
            source_path=str(roster_path),
            metadata={
                "profile_url": linkedin_url,
                "source_path": str(roster_path),
            },
        )
        self.store.upsert_candidate(candidate)
        self.store.mark_linkedin_profile_registry_fetched(
            linkedin_url,
            raw_path=str(raw_profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        job_id = "job_results_school_slug_recovery"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我OpenAI做Reasoning方向的人",
                "query": "给我OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current"],
                "keywords": ["Reasoning"],
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.88,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.88,
                    "confidence_reason": "registry fallback",
                    "explanation": "Recovered from local profile registry raw path.",
                    "matched_fields": ["work_history"],
                    "outreach_layer": 0,
                    "outreach_layer_key": "layer_0_roster",
                    "outreach_layer_source": "deterministic",
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertEqual(payload["results"][0]["education_lines"], ["BA, Princeton University"])

    def test_job_results_keep_profile_signals_when_experience_is_already_materialized(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "reflectionai" / "20260414T120000"
        profile_dir = snapshot_dir / "harvest_profiles"
        roster_dir = snapshot_dir / "normalized_artifacts"
        profile_dir.mkdir(parents=True, exist_ok=True)
        roster_dir.mkdir(parents=True, exist_ok=True)

        raw_profile_path = profile_dir / "abby.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "profilePicture": {
                            "url": "https://cdn.example.com/abby.jpg",
                        },
                        "emails": [
                            {
                                "email": "abby@reflection.ai",
                            }
                        ],
                        "experience": [
                            {
                                "title": "Head of Business and Technology Communications",
                                "companyName": "Reflection AI",
                                "startDate": {"year": 2024},
                                "endDate": {"text": "Present"},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        materialized_path = roster_dir / "materialized_candidate_documents.json"
        materialized_path.write_text(json.dumps({"candidates": []}, ensure_ascii=False, indent=2), encoding="utf-8")

        candidate = Candidate(
            candidate_id="cand_abby",
            name_en="Abby Reider",
            display_name="Abby Reider",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Head of Business and Technology Communications at Reflection AI",
            linkedin_url="https://www.linkedin.com/in/abby-reider/",
            source_dataset="reflectionai_snapshot",
            source_path=str(materialized_path),
            metadata={
                "experience_lines": ["2024~Present, Reflection AI, Head of Business and Technology Communications"],
                "profile_timeline_source_path": str(raw_profile_path),
            },
        )
        self.store.upsert_candidate(candidate)

        job_id = "job_results_materialized_profile_signals"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找Reflection AI做Post-train方向的人",
                "query": "帮我找Reflection AI做Post-train方向的人",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.82,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.82,
                    "confidence_reason": "materialized candidate artifact",
                    "explanation": "Recovered from materialized candidate artifact.",
                    "matched_fields": ["experience_lines"],
                    "outreach_layer": 0,
                    "outreach_layer_key": "layer_0_roster",
                    "outreach_layer_source": "deterministic",
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertEqual(
            payload["results"][0]["experience_lines"],
            ["2024~Present, Reflection AI, Head of Business and Technology Communications"],
        )
        self.assertEqual(payload["results"][0]["avatar_url"], "https://cdn.example.com/abby.jpg")
        self.assertNotIn("primary_email", payload["results"][0])
        self.assertNotIn("primary_email_metadata", payload["results"][0])

    def test_job_results_and_asset_population_fall_back_to_strict_roster_publishable_email(self) -> None:
        snapshot_id = "20260416T101500"
        linkedin_url = "https://www.linkedin.com/in/alice-example/"
        canonical_candidate = Candidate(
            candidate_id="cand_alice",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Google",
            organization="Google DeepMind",
            employment_status="current",
            role="Research Scientist",
            linkedin_url=linkedin_url,
            source_dataset="google_snapshot",
            metadata={"headline": "Multimodal @ Google DeepMind"},
        )
        strict_candidate = Candidate(
            candidate_id="cand_alice",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Google",
            organization="Google DeepMind",
            employment_status="current",
            role="Research Scientist",
            linkedin_url=linkedin_url,
            source_dataset="google_snapshot",
            metadata={
                "headline": "Multimodal @ Google DeepMind",
                "primary_email": "alice@stanford.edu",
                "primary_email_metadata": {
                    "source": "harvestapi",
                    "status": "valid",
                    "qualityScore": 100,
                    "foundInLinkedInProfile": True,
                },
            },
        )
        self._write_materialized_snapshot_view(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[canonical_candidate.to_record()],
            view="canonical_merged",
        )
        self._write_materialized_snapshot_view(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[strict_candidate.to_record()],
            view="strict_roster_only",
        )
        self.store.upsert_candidate(canonical_candidate)

        job_id = "job_results_strict_roster_email_fallback"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找Google做多模态方向的人",
                "query": "Google multimodal people",
                "target_company": "Google",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "keywords": ["Multimodal"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                }
            },
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": canonical_candidate.candidate_id,
                    "rank": 1,
                    "score": 0.93,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.93,
                    "confidence_reason": "strict roster fallback",
                    "explanation": "Recovered from strict roster publishable email.",
                    "matched_fields": ["focus_areas"],
                    "outreach_layer": 0,
                    "outreach_layer_key": "layer_0_roster",
                    "outreach_layer_source": "deterministic",
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        self.assertEqual(payload["results"], [])
        self.assertEqual(payload["asset_population"]["candidates"][0]["primary_email"], "alice@stanford.edu")
        self.assertTrue(
            payload["asset_population"]["candidates"][0]["primary_email_metadata"]["foundInLinkedInProfile"]
        )

    def test_job_candidate_detail_falls_back_to_strict_roster_publishable_email_for_asset_population(self) -> None:
        snapshot_id = "20260416T102000"
        linkedin_url = "https://www.linkedin.com/in/bob-example/"
        canonical_candidate = Candidate(
            candidate_id="cand_bob",
            name_en="Bob Example",
            display_name="Bob Example",
            category="employee",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Research Engineer",
            linkedin_url=linkedin_url,
            source_dataset="tml_snapshot",
        )
        strict_candidate = Candidate(
            candidate_id="cand_bob",
            name_en="Bob Example",
            display_name="Bob Example",
            category="employee",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Research Engineer",
            linkedin_url=linkedin_url,
            source_dataset="tml_snapshot",
            metadata={
                "primary_email": "bob@mit.edu",
                "primary_email_metadata": {
                    "source": "harvestapi",
                    "status": "valid",
                    "qualityScore": 100,
                    "foundInLinkedInProfile": True,
                },
            },
        )
        self._write_materialized_snapshot_view(
            target_company="Thinking Machines Lab",
            snapshot_id=snapshot_id,
            candidates=[canonical_candidate.to_record()],
            view="canonical_merged",
        )
        self._write_materialized_snapshot_view(
            target_company="Thinking Machines Lab",
            snapshot_id=snapshot_id,
            candidates=[strict_candidate.to_record()],
            view="strict_roster_only",
        )

        job_id = "job_detail_strict_roster_email_fallback"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找Thinking Machines Lab的全部成员",
                "query": "Thinking Machines Lab everyone",
                "target_company": "Thinking Machines Lab",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                }
            },
        )

        detail = self.orchestrator.get_job_candidate_detail(job_id, canonical_candidate.candidate_id)
        assert detail is not None
        self.assertEqual(detail["result"]["primary_email"], "bob@mit.edu")
        self.assertTrue(detail["result"]["primary_email_metadata"]["foundInLinkedInProfile"])
        self.assertEqual(detail["candidate"]["primary_email"], "bob@mit.edu")
        self.assertTrue(detail["candidate"]["primary_email_metadata"]["foundInLinkedInProfile"])

    def test_target_candidates_api_uses_strict_roster_publishable_email_lookup(self) -> None:
        snapshot_id = "20260416T103000"
        linkedin_url = "https://www.linkedin.com/in/carol-example/"
        canonical_candidate = Candidate(
            candidate_id="cand_carol",
            name_en="Carol Example",
            display_name="Carol Example",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Research Engineer",
            linkedin_url=linkedin_url,
            source_dataset="openai_snapshot",
        )
        strict_candidate = Candidate(
            candidate_id="cand_carol",
            name_en="Carol Example",
            display_name="Carol Example",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Research Engineer",
            linkedin_url=linkedin_url,
            source_dataset="openai_snapshot",
            metadata={
                "primary_email": "carol@mit.edu",
                "primary_email_metadata": {
                    "source": "harvestapi",
                    "status": "valid",
                    "qualityScore": 100,
                    "foundInLinkedInProfile": True,
                },
            },
        )
        self._write_materialized_snapshot_view(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            candidates=[canonical_candidate.to_record()],
            view="canonical_merged",
        )
        self._write_materialized_snapshot_view(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            candidates=[strict_candidate.to_record()],
            view="strict_roster_only",
        )

        job_id = "job_target_candidates_strict_roster_email_fallback"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我OpenAI的全部成员",
                "query": "OpenAI everyone",
                "target_company": "OpenAI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                }
            },
        )
        self.orchestrator.upsert_target_candidate(
            {
                "job_id": job_id,
                "candidate_id": canonical_candidate.candidate_id,
                "candidate_name": canonical_candidate.display_name,
                "headline": canonical_candidate.role,
                "current_company": canonical_candidate.organization,
                "linkedin_url": canonical_candidate.linkedin_url,
            }
        )

        payload = self.orchestrator.list_target_candidates(job_id=job_id)
        self.assertEqual(payload["target_candidates"][0]["primary_email"], "carol@mit.edu")
        self.assertTrue(payload["target_candidates"][0]["primary_email_metadata"]["foundInLinkedInProfile"])

    def test_job_results_promote_running_workflow_when_final_stage_summary_is_completed(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "google" / "20260414T090000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        request_payload = {
            "raw_user_request": "帮我找Google里做多模态和Pre-train方向的人",
            "query": "帮我找Google里做多模态和Pre-train方向的人",
            "target_company": "Google",
            "target_scope": "full_company_asset",
            "employment_statuses": ["current", "former"],
            "keywords": ["multimodal", "Pre-train"],
        }
        final_summary_path = self.orchestrator._persist_workflow_stage_summary_file(
            request=JobRequest.from_payload(request_payload),
            stage_name="stage_2_final",
            summary_payload={
                "status": "completed",
                "analysis_stage": "stage_2_final",
                "snapshot_id": "20260414T090000",
                "text": "Workflow completed.",
            },
            snapshot_dir=snapshot_dir,
        )

        candidate = Candidate(
            candidate_id="cand_google",
            name_en="Google Candidate",
            display_name="Google Candidate",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/google-candidate/",
            source_dataset="google_snapshot",
        )
        self.store.upsert_candidate(candidate)

        job_id = "job_reconcile_completed_from_results"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request_payload,
            plan_payload={},
            summary_payload={
                "analysis_stage": "stage_2_final",
                "stage_summary_path": final_summary_path,
                "message": "Stage 2 final artifact is ready but job row is stale.",
                "snapshot_id": "20260414T090000",
            },
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.9,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.9,
                    "confidence_reason": "completed artifact",
                    "explanation": "Recovered from completed stage_2_final artifact.",
                    "matched_fields": ["focus_areas"],
                    "outreach_layer": 0,
                    "outreach_layer_key": "layer_0_roster",
                    "outreach_layer_source": "deterministic",
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        self.assertEqual(payload["job"]["status"], "completed")
        self.assertEqual(payload["job"]["stage"], "completed")
        refreshed = self.store.get_job(job_id)
        assert refreshed is not None
        self.assertEqual(refreshed["status"], "completed")
        self.assertEqual(refreshed["stage"], "completed")

    def test_job_results_expose_asset_population_for_snapshot_reuse_even_when_plan_payload_is_missing(self) -> None:
        company_dir = Path(self.tempdir.name) / "company_assets" / "reflectionai"
        snapshot_dir = company_dir / "20260414T110000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260414T110000",
                    "company_identity": {
                        "requested_name": "Reflection AI",
                        "canonical_name": "Reflection AI",
                        "company_key": "reflectionai",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="cand_reflection",
            name_en="Arnaud Stiegler",
            display_name="Arnaud Stiegler",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Member of Technical Staff",
            linkedin_url="https://www.linkedin.com/in/arnaud-stiegler/",
            source_dataset="reflection_roster",
            metadata={"headline": "Research Engineer | Post-training at Reflection AI"},
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260414T110000",
                        "company_identity": {
                            "requested_name": "Reflection AI",
                            "canonical_name": "Reflection AI",
                            "company_key": "reflectionai",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        job_id = "job_results_asset_population_snapshot_reuse"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "query": "Reflection AI Post-train direction people",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train", "post_train"],
                "must_have_facets": ["post_train"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260414T110000",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "candidate_documents.json"),
                }
            },
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        self.assertTrue(payload["asset_population"]["available"])
        self.assertEqual(payload["asset_population"]["candidate_count"], 1)
        self.assertEqual(payload["asset_population"]["candidates"][0]["display_name"], "Arnaud Stiegler")

    def test_job_results_auto_materialize_snapshot_candidate_documents_for_asset_population(self) -> None:
        company_dir = Path(self.tempdir.name) / "company_assets" / "anthropic"
        snapshot_dir = company_dir / "20260416T225318"
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260416T225318",
                    "company_identity": {
                        "requested_name": "Anthropic",
                        "canonical_name": "Anthropic",
                        "company_key": "anthropic",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        raw_profile_path = harvest_dir / "ada.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "experience": [
                            {
                                "title": "Research Engineer",
                                "companyName": "Anthropic",
                                "startDate": {"year": 2023},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "educations": [
                            {
                                "degreeName": "BS",
                                "schoolName": "MIT",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"year": 2016},
                                "endDate": {"year": 2020},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="cand_ada_materialize",
            name_en="Ada Researcher",
            display_name="Ada Researcher",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/ada-researcher/",
            source_dataset="anthropic_roster",
            source_path=str(raw_profile_path),
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260416T225318",
                        "company_identity": {
                            "requested_name": "Anthropic",
                            "canonical_name": "Anthropic",
                            "company_key": "anthropic",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        job_id = "job_results_asset_population_auto_materialize"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找 Anthropic 做 Pre-training 的人",
                "query": "Anthropic Pre-training",
                "target_company": "Anthropic",
                "target_scope": "scoped_search",
                "employment_statuses": ["current", "former"],
                "categories": ["researcher", "engineer"],
                "keywords": ["Pre-train"],
            },
            plan_payload={
                "organization_execution_profile": {
                    "target_company": "Anthropic",
                    "org_scale_band": "medium",
                    "default_acquisition_mode": "scoped_search_roster",
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": True,
                    "baseline_candidate_count": 3000,
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260416T225318",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "candidate_documents.json"),
                }
            },
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        candidate_payload = payload["asset_population"]["candidates"][0]
        self.assertEqual(payload["asset_population"]["source_kind"], "materialized_candidate_documents_manifest")
        self.assertIn("normalized_artifacts/manifest.json", payload["asset_population"]["source_path"])
        self.assertEqual(candidate_payload["display_name"], "Ada Researcher")
        self.assertEqual(candidate_payload["experience_lines"], ["2023~Present, Anthropic, Research Engineer"])
        self.assertEqual(candidate_payload["education_lines"], ["2016~2020, BS, MIT, Computer Science"])

    def test_job_results_asset_population_applies_snapshot_outreach_layering_metadata(self) -> None:
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="Physical Intelligence",
            snapshot_id="20260416T144859",
            candidates=[
                {
                    "candidate_id": "cand_pi_layered",
                    "display_name": "Aaron Delgado",
                    "name_en": "Aaron Delgado",
                    "category": "former_employee",
                    "target_company": "Physical Intelligence",
                    "organization": "Physical Intelligence",
                    "employment_status": "former",
                    "role": "Robot Specialist at Parametric (YC F25)",
                    "linkedin_url": "https://www.linkedin.com/in/aaron-delgado/",
                    "metadata": {},
                }
            ],
        )
        self._write_outreach_layering_analysis(
            snapshot_dir=snapshot_dir,
            candidates=[
                {
                    "candidate_id": "cand_pi_layered",
                    "display_name": "Aaron Delgado",
                    "final_layer": 2,
                    "final_layer_source": "deterministic",
                }
            ],
        )

        job_id = "job_results_asset_population_layering"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Physical Intelligence的所有成员",
                "query": "Physical Intelligence all members",
                "target_company": "Physical Intelligence",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260416T144859",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"),
                }
            },
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        self.assertTrue(payload["asset_population"]["available"])
        self.assertEqual(payload["asset_population"]["candidate_count"], 1)
        self.assertEqual(payload["asset_population"]["candidates"][0]["outreach_layer"], 2)
        self.assertEqual(
            payload["asset_population"]["candidates"][0]["outreach_layer_key"],
            "layer_2_greater_china_region_experience",
        )
        self.assertEqual(payload["asset_population"]["candidates"][0]["outreach_layer_source"], "deterministic")

    def test_job_results_resolve_asset_population_from_job_result_view_without_summary_candidate_source(self) -> None:
        self._write_materialized_snapshot_view(
            target_company="Anthropic",
            snapshot_id="20260418T010101",
            candidates=[
                {
                    "candidate_id": "cand_result_view_email",
                    "display_name": "Ada Researcher",
                    "name_en": "Ada Researcher",
                    "category": "employee",
                    "target_company": "Anthropic",
                    "organization": "Anthropic",
                    "employment_status": "current",
                    "role": "Research Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/ada-researcher/",
                    "primary_email": "ada@anthropic.com",
                    "primary_email_metadata": {
                        "foundInLinkedInProfile": True,
                    },
                    "metadata": {},
                }
            ],
        )

        job_id = "job_results_result_view_only"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我 Anthropic 的全部成员",
                "query": "Anthropic all members",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={},
            summary_payload={"text": "completed without embedded candidate_source"},
        )
        result_view = self.store.upsert_job_result_view(
            job_id=job_id,
            target_company="Anthropic",
            source_kind="company_snapshot",
            view_kind="asset_population",
            snapshot_id="20260418T010101",
            asset_view="canonical_merged",
            source_path="",
            summary={"candidate_count": 1, "default_results_mode": "asset_population"},
            metadata={},
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        self.assertTrue(payload["asset_population"]["available"])
        self.assertEqual(payload["asset_population"]["candidate_count"], 1)
        self.assertEqual(payload["asset_population"]["candidates"][0]["display_name"], "Ada Researcher")
        self.assertEqual(payload["asset_population"]["candidates"][0]["primary_email"], "ada@anthropic.com")
        self.assertTrue(
            payload["asset_population"]["candidates"][0]["primary_email_metadata"]["foundInLinkedInProfile"]
        )
        self.assertEqual(payload["effective_execution_semantics"]["default_results_mode"], "asset_population")
        self.assertTrue(payload["asset_population"]["source_path"])
        self.assertEqual(result_view["view_kind"], "asset_population")

        detail_payload = self.orchestrator.get_job_candidate_detail(job_id, "cand_result_view_email")
        assert detail_payload is not None
        self.assertEqual(detail_payload["candidate"]["primary_email"], "ada@anthropic.com")
        self.assertEqual(detail_payload["candidate"]["display_name"], "Ada Researcher")

    def test_job_results_resolve_outreach_layering_from_job_result_view_without_source_path(self) -> None:
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="Physical Intelligence",
            snapshot_id="20260418T020202",
            candidates=[
                {
                    "candidate_id": "cand_result_view_layer",
                    "display_name": "Anna Walling",
                    "name_en": "Anna Walling",
                    "category": "employee",
                    "target_company": "Physical Intelligence",
                    "organization": "Physical Intelligence",
                    "employment_status": "current",
                    "role": "Research Scientist",
                    "linkedin_url": "https://www.linkedin.com/in/anna-walling/",
                    "metadata": {},
                }
            ],
        )
        self._write_outreach_layering_analysis(
            snapshot_dir=snapshot_dir,
            candidates=[
                {
                    "candidate_id": "cand_result_view_layer",
                    "display_name": "Anna Walling",
                    "final_layer": 3,
                    "final_layer_source": "ai_verified",
                }
            ],
        )

        job_id = "job_results_result_view_layering_only"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我 Physical Intelligence 的全部成员",
                "query": "Physical Intelligence all members",
                "target_company": "Physical Intelligence",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={},
            summary_payload={"text": "completed without source_path"},
        )
        self.store.upsert_job_result_view(
            job_id=job_id,
            target_company="Physical Intelligence",
            source_kind="company_snapshot",
            view_kind="asset_population",
            snapshot_id="20260418T020202",
            asset_view="canonical_merged",
            source_path="",
            summary={"candidate_count": 1, "default_results_mode": "asset_population"},
            metadata={},
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        self.assertEqual(payload["asset_population"]["candidates"][0]["outreach_layer"], 3)
        self.assertEqual(
            payload["asset_population"]["candidates"][0]["outreach_layer_key"],
            "layer_3_mainland_china_experience_or_chinese_language",
        )
        self.assertEqual(payload["asset_population"]["candidates"][0]["outreach_layer_source"], "ai_verified")

    def test_job_results_asset_population_compacts_snapshot_candidates_for_dashboard_use(self) -> None:
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="xAI",
            snapshot_id="20260416T012752",
            candidates=[
                {
                    "candidate_id": "cand_xai_compact",
                    "display_name": "Abhishek Jha",
                    "name_en": "Abhishek Jha",
                    "category": "employee",
                    "target_company": "xAI",
                    "organization": "xAI",
                    "employment_status": "current",
                    "role": "Founding GTM Enterprise Leader",
                    "headline": "Founding GTM Enterprise Leader at xAI",
                    "summary": "Leads enterprise GTM at xAI.",
                    "linkedin_url": "https://www.linkedin.com/in/abhishek-jha/",
                    "source_dataset": "xai_roster",
                    "source_path": "/tmp/xai-candidate.json",
                    "work_history": "Verbose legacy work history blob that should not be sent in asset population payload.",
                    "education": "Verbose legacy education blob that should not be sent in asset population payload.",
                    "metadata": {
                        "headline": "Founding GTM Enterprise Leader at xAI",
                        "profile_capture_kind": "harvest_profile_detail",
                        "source_path": "/tmp/xai-harvest-profile.json",
                        "experience_lines": [
                            "2026~Present, xAI, Founding GTM Enterprise Leader",
                        ],
                        "education_lines": [
                            "2001~2007, Bachelors, Political Science, The University of Georgia",
                        ],
                        "languages": ["English"],
                        "skills": "Enterprise GTM",
                    },
                }
            ],
        )

        job_id = "job_results_asset_population_compact"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我xAI的全部成员",
                "query": "xAI all members",
                "target_company": "xAI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260416T012752",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"),
                }
            },
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        candidate = payload["asset_population"]["candidates"][0]
        self.assertEqual(candidate["display_name"], "Abhishek Jha")
        self.assertEqual(
            candidate["experience_lines"],
            ["2026~Present, xAI, Founding GTM Enterprise Leader"],
        )
        self.assertEqual(
            candidate["education_lines"],
            ["2001~2007, Bachelors, Political Science, The University of Georgia"],
        )
        self.assertEqual(candidate["linkedin_url"], "https://www.linkedin.com/in/abhishek-jha/")
        self.assertTrue(candidate["has_profile_detail"])
        self.assertFalse(candidate["needs_profile_completion"])
        self.assertNotIn("metadata", candidate)
        self.assertNotIn("source_path", candidate)
        self.assertNotIn("work_history", candidate)
        self.assertNotIn("education", candidate)

    def test_job_results_asset_population_marks_low_profile_richness_without_profile_completion_gap(self) -> None:
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="Physical Intelligence",
            snapshot_id="20260416T150000",
            candidates=[
                {
                    "candidate_id": "cand_sparse_profile",
                    "display_name": "Jonathan Feng",
                    "name_en": "Jonathan Feng",
                    "category": "employee",
                    "employment_status": "current",
                    "target_company": "Physical Intelligence",
                    "organization": "Physical Intelligence",
                    "headline": "Research Engineer at Physical Intelligence",
                    "role": "Research Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/jonathan-feng/",
                    "profile_capture_kind": "harvest_profile_detail",
                    "experience_lines": [
                        "2024~Present, Physical Intelligence, Research Engineer",
                    ],
                }
            ],
        )

        job_id = "job_results_asset_population_low_profile_richness"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我 Physical Intelligence 的全部成员",
                "query": "Physical Intelligence all members",
                "target_company": "Physical Intelligence",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260416T150000",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"),
                }
            },
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        candidate = payload["asset_population"]["candidates"][0]
        self.assertTrue(candidate["has_profile_detail"])
        self.assertFalse(candidate["needs_profile_completion"])
        self.assertTrue(candidate["low_profile_richness"])

    def test_job_results_asset_population_search_seed_preview_requires_profile_completion(self) -> None:
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="OpenAI",
            snapshot_id="20260417T041804",
            candidates=[
                {
                    "candidate_id": "cand_seed_preview_only",
                    "display_name": "Avery Yang",
                    "name_en": "Avery Yang",
                    "category": "employee",
                    "employment_status": "current",
                    "target_company": "OpenAI",
                    "organization": "OpenAI",
                    "headline": "Director of Artificial Intelligence Technology at OpenAI",
                    "role": "Director of Artificial Intelligence Technology",
                    "linkedin_url": "https://www.linkedin.com/in/ACwAADSzRicBHxDjS8piFRijJy1S8kOf9z-3yno",
                    "profile_capture_kind": "search_seed_preview",
                    "experience_lines": [
                        "2024~Present, OpenAI, Director of Artificial Intelligence Technology",
                    ],
                }
            ],
        )

        job_id = "job_results_asset_population_search_seed_preview"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找OpenAI做RL方向的人",
                "query": "OpenAI RL direction researcher or engineer",
                "target_company": "OpenAI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260417T041804",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"),
                }
            },
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        candidate = payload["asset_population"]["candidates"][0]
        self.assertFalse(candidate["has_profile_detail"])
        self.assertTrue(candidate["needs_profile_completion"])
        self.assertFalse(candidate["low_profile_richness"])

    def test_job_results_asset_population_sparse_harvest_profile_detail_requires_profile_completion(self) -> None:
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="Physical Intelligence",
            snapshot_id="20260417T163200",
            candidates=[
                {
                    "candidate_id": "cand_sparse_harvest_profile",
                    "display_name": "Adnan Esmail",
                    "name_en": "Adnan Esmail",
                    "category": "employee",
                    "employment_status": "current",
                    "target_company": "Physical Intelligence",
                    "organization": "Physical Intelligence",
                    "headline": "Software Engineer at Physical Intelligence",
                    "role": "Software Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/adnan-esmail/",
                    "profile_capture_kind": "harvest_profile_detail",
                }
            ],
        )

        job_id = "job_results_asset_population_sparse_harvest_profile"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我 Physical Intelligence 的全部成员",
                "query": "Physical Intelligence all members",
                "target_company": "Physical Intelligence",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260417T163200",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"),
                }
            },
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        candidate = payload["asset_population"]["candidates"][0]
        self.assertFalse(candidate["has_profile_detail"])
        self.assertTrue(candidate["needs_profile_completion"])
        self.assertFalse(candidate["low_profile_richness"])

    def test_job_results_default_to_asset_population_for_full_company_snapshot_even_when_delta_is_required(
        self,
    ) -> None:
        company_dir = Path(self.tempdir.name) / "company_assets" / "openai"
        snapshot_dir = company_dir / "20260416T095325"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260416T095325",
                    "company_identity": {
                        "requested_name": "OpenAI",
                        "canonical_name": "OpenAI",
                        "company_key": "openai",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="cand_openai_pretrain",
            name_en="John Hallman",
            display_name="John Hallman",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Member of Technical Staff at OpenAI",
            linkedin_url="https://www.linkedin.com/in/john-hallman/",
            source_dataset="openai_roster",
            metadata={"headline": "Pretraining @ OpenAI"},
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260416T095325",
                        "company_identity": {
                            "requested_name": "OpenAI",
                            "canonical_name": "OpenAI",
                            "company_key": "openai",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        job_id = "job_results_asset_population_delta_default"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我OpenAI做Pre-train方向的人",
                "query": "OpenAI Pre-train direction",
                "target_company": "OpenAI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "categories": ["researcher", "engineer"],
                "keywords": ["Pre-train"],
                "must_have_facets": ["pre_train"],
            },
            plan_payload={
                "organization_execution_profile": {
                    "target_company": "OpenAI",
                    "org_scale_band": "large",
                    "default_acquisition_mode": "scoped_search_roster",
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": True,
                    "baseline_candidate_count": 153,
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260416T095325",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "candidate_documents.json"),
                }
            },
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.95,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.95,
                    "confidence_reason": "delta result",
                    "explanation": "Pretraining match.",
                    "matched_fields": ["focus_areas"],
                    "outreach_layer": 0,
                    "outreach_layer_key": "layer_0_roster",
                    "outreach_layer_source": "deterministic",
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        self.assertEqual(payload["effective_execution_semantics"]["default_results_mode"], "asset_population")
        self.assertTrue(payload["effective_execution_semantics"]["asset_population_supported"])
        self.assertTrue(payload["asset_population"]["available"])
        self.assertTrue(payload["asset_population"]["default_selected"])
        self.assertEqual(payload["asset_population"]["candidate_count"], 1)

    def test_job_results_default_to_asset_population_for_scoped_snapshot_even_when_delta_is_required(self) -> None:
        company_dir = Path(self.tempdir.name) / "company_assets" / "anthropic"
        snapshot_dir = company_dir / "20260416T123000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260416T123000",
                    "company_identity": {
                        "requested_name": "Anthropic",
                        "canonical_name": "Anthropic",
                        "company_key": "anthropic",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="cand_anthropic_scoped",
            name_en="Ada Researcher",
            display_name="Ada Researcher",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/ada-researcher/",
            source_dataset="anthropic_roster",
            metadata={"headline": "Research Engineer at Anthropic"},
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260416T123000",
                        "company_identity": {
                            "requested_name": "Anthropic",
                            "canonical_name": "Anthropic",
                            "company_key": "anthropic",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        job_id = "job_results_asset_population_scoped_default"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找 Anthropic 做 Pre-training 的人",
                "query": "Anthropic Pre-training",
                "target_company": "Anthropic",
                "target_scope": "scoped_search",
                "employment_statuses": ["current", "former"],
                "categories": ["researcher", "engineer"],
                "keywords": ["Pre-train"],
                "must_have_facets": ["pre_training"],
            },
            plan_payload={
                "organization_execution_profile": {
                    "target_company": "Anthropic",
                    "org_scale_band": "medium",
                    "default_acquisition_mode": "scoped_search_roster",
                    "current_lane_default": "reuse_baseline",
                    "former_lane_default": "reuse_baseline",
                },
                "asset_reuse_plan": {
                    "baseline_reuse_available": True,
                    "requires_delta_acquisition": True,
                    "baseline_candidate_count": 3000,
                },
            },
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260416T123000",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "candidate_documents.json"),
                }
            },
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.95,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.95,
                    "confidence_reason": "scoped delta result",
                    "explanation": "Should be hidden when asset population is the default results view.",
                    "matched_fields": ["headline"],
                    "outreach_layer": 0,
                    "outreach_layer_key": "layer_0_roster",
                    "outreach_layer_source": "deterministic",
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        assert payload is not None
        self.assertEqual(payload["effective_execution_semantics"]["default_results_mode"], "asset_population")
        self.assertTrue(payload["effective_execution_semantics"]["asset_population_supported"])
        self.assertTrue(payload["asset_population"]["available"])
        self.assertTrue(payload["asset_population"]["default_selected"])
        self.assertEqual(payload["asset_population"]["candidate_count"], 1)
        self.assertEqual(payload["ranked_results"], [])

    def test_job_candidate_detail_falls_back_to_asset_population_snapshot_candidate(self) -> None:
        company_dir = Path(self.tempdir.name) / "company_assets" / "reflectionai"
        snapshot_dir = company_dir / "20260414T130000"
        profile_dir = snapshot_dir / "harvest_profiles"
        profile_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260414T130000",
                    "company_identity": {
                        "requested_name": "Reflection AI",
                        "canonical_name": "Reflection AI",
                        "company_key": "reflectionai",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        raw_profile_path = profile_dir / "arnaud.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "profilePicture": {
                            "url": "https://cdn.example.com/arnaud.jpg",
                        },
                        "emails": [
                            {
                                "email": "arnaud@reflection.ai",
                            }
                        ],
                        "experience": [
                            {
                                "title": "Member of Technical Staff",
                                "companyName": "Reflection AI",
                                "startDate": {"year": 2024},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "educations": [
                            {
                                "degreeName": "Master",
                                "schoolName": "EPFL",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"year": 2018},
                                "endDate": {"year": 2020},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        candidate = Candidate(
            candidate_id="cand_asset_only",
            name_en="Arnaud Stiegler",
            display_name="Arnaud Stiegler",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Member of Technical Staff",
            linkedin_url="https://www.linkedin.com/in/arnaud-stiegler/",
            source_dataset="reflection_roster",
            source_path=str(snapshot_dir / "candidate_documents.json"),
            metadata={
                "profile_timeline_source_path": str(raw_profile_path),
            },
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260414T130000",
                        "company_identity": {
                            "requested_name": "Reflection AI",
                            "canonical_name": "Reflection AI",
                            "company_key": "reflectionai",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        job_id = "job_candidate_detail_asset_population_only"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "query": "Reflection AI Post-train direction people",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "20260414T130000",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "candidate_documents.json"),
                }
            },
        )

        detail = self.orchestrator.get_job_candidate_detail(job_id, candidate.candidate_id)
        assert detail is not None
        self.assertEqual(
            detail["candidate"]["experience_lines"],
            ["2024~Present, Reflection AI, Member of Technical Staff"],
        )
        self.assertEqual(
            detail["candidate"]["education_lines"],
            ["2018~2020, Master, EPFL, Computer Science"],
        )
        self.assertEqual(detail["candidate"]["avatar_url"], "https://cdn.example.com/arnaud.jpg")
        self.assertNotIn("primary_email", detail["candidate"])
        self.assertNotIn("primary_email_metadata", detail["candidate"])

    def test_job_candidate_detail_loads_authoritative_snapshot_candidate_when_sqlite_candidate_missing(self) -> None:
        snapshot_id = "20260414T130500"
        snapshot_dir = self._write_materialized_snapshot_view(
            target_company="Reflection AI",
            snapshot_id=snapshot_id,
            candidates=[
                {
                    "candidate_id": "cand_authoritative_only",
                    "name_en": "Avery Snapshot",
                    "display_name": "Avery Snapshot",
                    "category": "employee",
                    "target_company": "Reflection AI",
                    "organization": "Reflection AI",
                    "employment_status": "current",
                    "role": "Research Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/avery-snapshot/",
                    "evidence": [
                        {
                            "evidence_id": "ev-authoritative-1",
                            "candidate_id": "cand_authoritative_only",
                            "source_type": "profile",
                            "title": "Avery Snapshot profile",
                            "url": "https://www.linkedin.com/in/avery-snapshot/",
                            "summary": "Research Engineer at Reflection AI",
                            "source_dataset": "materialized_snapshot",
                            "source_path": "snapshot",
                            "metadata": {},
                        }
                    ],
                }
            ],
        )

        job_id = "job_detail_authoritative_only"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我Reflection AI的全部成员",
                "query": "Reflection AI everyone",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
            },
            plan_payload={},
            summary_payload={
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "source_path": str(snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"),
                }
            },
        )

        detail = self.orchestrator.get_job_candidate_detail(job_id, "cand_authoritative_only")
        assert detail is not None
        self.assertEqual(detail["candidate"]["candidate_id"], "cand_authoritative_only")
        self.assertEqual(detail["candidate"]["display_name"], "Avery Snapshot")
        self.assertEqual(len(detail["evidence"]), 1)
        self.assertEqual(detail["evidence"][0]["title"], "Avery Snapshot profile")

    def test_job_results_recover_seed_candidate_profile_signals_from_search_seed_raw_payload(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "openai" / "20260415T010000"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        raw_dir = discovery_dir / "harvest_profile_search"
        raw_dir.mkdir(parents=True, exist_ok=True)

        raw_search_path = raw_dir / "reasoning.json"
        raw_search_path.write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "id": "franklin",
                            "linkedinUrl": "https://www.linkedin.com/in/franklin-wang/",
                            "firstName": "Franklin",
                            "lastName": "Wang",
                            "pictureUrl": "https://cdn.example.com/franklin.jpg",
                            "location": {"linkedinText": "San Francisco Bay Area"},
                            "currentPositions": [
                                {
                                    "companyName": "OpenAI",
                                    "title": "Researcher",
                                    "current": True,
                                    "startedOn": {"year": 2025},
                                }
                            ],
                        }
                    ]
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        summary_path = discovery_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "query_summaries": [
                        {
                            "query": "Reasoning",
                            "effective_query_text": "Reasoning",
                            "mode": "harvest_profile_search",
                            "raw_path": str(raw_search_path),
                        }
                    ]
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        linkedin_url = "https://www.linkedin.com/in/franklin-wang/"
        candidate = Candidate(
            candidate_id="cand_franklin",
            name_en="Franklin Wang",
            display_name="Franklin Wang",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Researcher at OpenAI",
            linkedin_url=linkedin_url,
            source_dataset="openai_search_seed",
            source_path=str(summary_path),
            metadata={
                "source_path": str(summary_path),
                "profile_url": linkedin_url,
                "seed_query": "Reasoning",
                "seed_source_type": "harvest_profile_search",
            },
        )
        self.store.upsert_candidate(candidate)

        job_id = "job_results_seed_raw_fallback"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "query": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "target_scope": "scoped_roster",
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.9,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.9,
                    "confidence_reason": "search seed raw fallback",
                    "explanation": "Recovered from search seed raw payload.",
                    "matched_fields": ["linkedin_url"],
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertEqual(
            payload["results"][0]["experience_lines"],
            ["2025~Present, OpenAI, Researcher"],
        )
        self.assertEqual(payload["results"][0]["avatar_url"], "https://cdn.example.com/franklin.jpg")
        self.assertEqual(payload["results"][0]["media_url"], "https://cdn.example.com/franklin.jpg")

    def test_job_results_recover_profile_top_education_from_queue_dataset_items_without_registry(self) -> None:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "openai" / "20260415T020000"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        profile_dir = snapshot_dir / "harvest_profiles"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        profile_dir.mkdir(parents=True, exist_ok=True)

        summary_path = discovery_dir / "summary.json"
        summary_path.write_text(json.dumps({}, ensure_ascii=False, indent=2), encoding="utf-8")
        queue_dataset_items_path = profile_dir / "harvest_profile_batch.queue_dataset_items.json"
        queue_dataset_items_path.write_text(
            json.dumps(
                [
                    {
                        "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                        "firstName": "Jane",
                        "lastName": "Doe",
                        "profilePicture": {
                            "url": "https://cdn.example.com/jane.jpg",
                        },
                        "emails": [
                            {
                                "email": "jane@openai.com",
                            }
                        ],
                        "currentPosition": [
                            {
                                "companyName": "OpenAI",
                                "position": "Research Scientist",
                                "startDate": {"year": 2024},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "profileTopEducation": [
                            {
                                "degree": "Bachelor",
                                "schoolName": "Tsinghua University",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"year": 2016},
                                "endDate": {"year": 2020},
                            }
                        ],
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        linkedin_url = "https://www.linkedin.com/in/jane-doe/"
        candidate = Candidate(
            candidate_id="cand_jane",
            name_en="Jane Doe",
            display_name="Jane Doe",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Research Scientist",
            linkedin_url=linkedin_url,
            source_dataset="openai_search_seed",
            source_path=str(summary_path),
            metadata={
                "source_path": str(summary_path),
                "profile_url": linkedin_url,
            },
        )
        self.store.upsert_candidate(candidate)

        job_id = "job_results_queue_dataset_items_fallback"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "query": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.88,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.88,
                    "confidence_reason": "queue dataset items fallback",
                    "explanation": "Recovered from queued harvest profile payload.",
                    "matched_fields": ["linkedin_url"],
                }
            ],
        )

        payload = self.orchestrator.get_job_results(job_id)
        self.assertEqual(
            payload["results"][0]["experience_lines"],
            ["2024~Present, OpenAI, Research Scientist"],
        )
        self.assertEqual(
            payload["results"][0]["education_lines"],
            ["2016~2020, Bachelor, Tsinghua University, Computer Science"],
        )
        self.assertEqual(payload["results"][0]["avatar_url"], "https://cdn.example.com/jane.jpg")
        self.assertNotIn("primary_email", payload["results"][0])
        self.assertNotIn("primary_email_metadata", payload["results"][0])

    def test_serialize_candidate_api_record_keeps_embedded_profile_signals_without_loading_timeline(self) -> None:
        serialized = self.orchestrator._serialize_candidate_api_record(
            {
                "candidate_id": "cand_embedded_signals",
                "display_name": "Embedded Signals",
                "metadata": {
                    "headline": "Research Engineer",
                    "avatar_url": "https://cdn.example.com/embedded.jpg",
                    "primary_email": "embedded@example.com",
                    "experience_lines": ["2023~Present, OpenAI, Research Engineer"],
                    "education_lines": ["2019~2023, Bachelor, MIT, EECS"],
                },
            },
            load_profile_timeline=False,
        )
        self.assertEqual(serialized["headline"], "Research Engineer")
        self.assertEqual(serialized["avatar_url"], "https://cdn.example.com/embedded.jpg")
        self.assertEqual(serialized["primary_email"], "embedded@example.com")
        self.assertEqual(
            serialized["experience_lines"],
            ["2023~Present, OpenAI, Research Engineer"],
        )
        self.assertEqual(
            serialized["education_lines"],
            ["2019~2023, Bachelor, MIT, EECS"],
        )

    def test_serialize_candidate_api_record_hides_harvest_corporate_email_without_loading_timeline(self) -> None:
        serialized = self.orchestrator._serialize_candidate_api_record(
            {
                "candidate_id": "cand_harvest_corporate_email",
                "display_name": "Harvest Corporate Email",
                "target_company": "OpenAI",
                "organization": "OpenAI",
                "metadata": {
                    "primary_email": "jane@openai.com",
                    "primary_email_metadata": {
                        "source": "harvestapi",
                        "status": "valid",
                        "qualityScore": 80,
                    },
                    "profile_capture_kind": "provider_profile_detail",
                    "source_path": "/tmp/company_assets/openai/20260414T090000/harvest_profiles/jane.json",
                },
            },
            load_profile_timeline=False,
        )
        self.assertNotIn("primary_email", serialized)
        self.assertNotIn("primary_email_metadata", serialized)

    def test_serialize_candidate_api_record_keeps_harvest_corporate_email_with_linkedin_proof_without_loading_timeline(
        self,
    ) -> None:
        serialized = self.orchestrator._serialize_candidate_api_record(
            {
                "candidate_id": "cand_harvest_corporate_email_verified",
                "display_name": "Harvest Corporate Email Verified",
                "target_company": "OpenAI",
                "organization": "OpenAI",
                "metadata": {
                    "primary_email": "jane@openai.com",
                    "primary_email_metadata": {
                        "source": "harvestapi",
                        "status": "valid",
                        "qualityScore": 80,
                        "foundInLinkedInProfile": True,
                    },
                    "profile_capture_kind": "provider_profile_detail",
                    "source_path": "/tmp/company_assets/openai/20260414T090000/harvest_profiles/jane.json",
                },
            },
            load_profile_timeline=False,
        )
        self.assertEqual(serialized["primary_email"], "jane@openai.com")
        self.assertTrue(serialized["primary_email_metadata"]["foundInLinkedInProfile"])

    def test_serialize_candidate_api_record_keeps_harvest_external_email_with_linkedin_proof_without_loading_timeline(
        self,
    ) -> None:
        serialized = self.orchestrator._serialize_candidate_api_record(
            {
                "candidate_id": "cand_harvest_external_email",
                "display_name": "Harvest External Email",
                "target_company": "OpenAI",
                "organization": "Periodic Labs",
                "metadata": {
                    "primary_email": "verified@periodic.com",
                    "primary_email_metadata": {
                        "source": "harvestapi",
                        "status": "valid",
                        "qualityScore": 100,
                        "foundInLinkedInProfile": True,
                    },
                    "profile_capture_kind": "provider_profile_detail",
                    "source_path": "/tmp/company_assets/openai/20260414T090000/harvest_profiles/verified.json",
                },
            },
            load_profile_timeline=False,
        )
        self.assertEqual(serialized["primary_email"], "verified@periodic.com")
        self.assertTrue(serialized["primary_email_metadata"]["foundInLinkedInProfile"])

    def test_job_progress_promotes_running_workflow_when_stage2_final_and_results_exist_without_summary_path(
        self,
    ) -> None:
        job_id = "job_progress_promote_stage2_final"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={
                "raw_user_request": "帮我找Google里做多模态和Pre-train方向的人",
                "query": "帮我找Google里做多模态和Pre-train方向的人",
                "target_company": "Google",
            },
            plan_payload={},
            summary_payload={"message": "Stage 1 preview ready. Continuing Public Web Stage 2 acquisition."},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": "cand_google_multimodal",
                    "rank": 1,
                    "score": 0.91,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "confidence_reason": "stage2 final ready",
                    "explanation": "Recovered from completed stage 2 final results.",
                    "matched_fields": ["work_history"],
                }
            ],
        )

        with mock.patch.object(
            self.orchestrator,
            "_load_workflow_stage_summaries",
            return_value={"summaries": {"stage_2_final": {"status": "completed"}}},
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        assert progress is not None
        self.assertEqual(progress["status"], "completed")
        self.assertEqual(progress["stage"], "completed")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        self.assertEqual(refreshed_job["status"], "completed")
        self.assertEqual(refreshed_job["stage"], "completed")

    def test_target_candidate_export_archive_api_returns_zip_with_csv_and_profile(self) -> None:
        job_id = "job_target_export_1"
        candidate_id = "cand_export_1"
        linkedin_url = "https://www.linkedin.com/in/alice-zhang/"
        raw_profile_path = Path(self.tempdir.name) / "runtime" / "profiles" / "alice-zhang.json"
        raw_profile_path.parent.mkdir(parents=True, exist_ok=True)
        raw_profile_path.write_text(
            json.dumps(
                {
                    "profile": {
                        "fullName": "Alice Zhang",
                        "headline": "Research Engineer at OpenAI",
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "给我OpenAI做Coding方向的人",
                "query": "给我OpenAI做Coding方向的人",
                "target_company": "OpenAI",
            },
            plan_payload={},
            summary_payload={},
        )
        self.store.upsert_candidate(
            Candidate(
                candidate_id=candidate_id,
                name_en="Alice Zhang",
                display_name="Alice Zhang",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="current",
                role="Research Engineer",
                linkedin_url=linkedin_url,
                source_path=str(raw_profile_path),
                metadata={
                    "headline": "Research Engineer at OpenAI",
                    "profile_location": "San Francisco Bay Area",
                    "primary_email_metadata": {
                        "source": "harvestapi",
                        "qualityScore": 100,
                        "foundInLinkedInProfile": True,
                    },
                    "public_identifier": "alice-zhang",
                },
            )
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate_id,
                    "rank": 1,
                    "score": 0.95,
                    "confidence_label": "high",
                    "confidence_score": 0.95,
                    "confidence_reason": "matched coding scope",
                    "explanation": "Strong coding alignment.",
                    "matched_fields": ["work_history", "education"],
                }
            ],
        )
        self.store.upsert_target_candidate(
            {
                "record_id": "target-export-record-1",
                "candidate_id": candidate_id,
                "job_id": job_id,
                "history_id": "history-export-1",
                "candidate_name": "Alice Zhang",
                "linkedin_url": linkedin_url,
                "primary_email": "alice@openai.com",
                "metadata": {
                    "primary_email_metadata": {
                        "source": "harvestapi",
                        "qualityScore": 100,
                        "foundInLinkedInProfile": True,
                    }
                },
            }
        )
        self.store.mark_linkedin_profile_registry_fetched(
            profile_url=linkedin_url,
            raw_path=str(raw_profile_path),
            raw_linkedin_url=linkedin_url,
            sanity_linkedin_url=linkedin_url,
        )

        with mock.patch.object(
            self.orchestrator,
            "get_job_candidate_detail",
            return_value={
                "candidate": {
                    "candidate_id": candidate_id,
                    "display_name": "Alice Zhang",
                    "employment_status": "current",
                    "linkedin_url": linkedin_url,
                    "primary_email": "alice@openai.com",
                    "primary_email_metadata": {
                        "source": "harvestapi",
                        "qualityScore": 100,
                        "foundInLinkedInProfile": True,
                    },
                    "experience_lines": [
                        "2022~Present, OpenAI, Research Engineer, Coding Agents",
                    ],
                    "education_lines": [
                        "2020, Stanford University, MS Computer Science",
                    ],
                    "profile_location": "San Francisco Bay Area",
                    "public_identifier": "alice-zhang",
                    "source_path": str(raw_profile_path),
                    "metadata": {
                        "profile_location": "San Francisco Bay Area",
                        "public_identifier": "alice-zhang",
                    },
                },
                "result": {
                    "outreach_layer_key": "layer_3_mainland_china_experience_or_chinese_language",
                    "outreach_layer": 3,
                    "linkedin_url": linkedin_url,
                    "source_path": str(raw_profile_path),
                    "metadata": {
                        "public_identifier": "alice-zhang",
                    },
                },
                "evidence": [],
                "manual_review_items": [],
                "request_preview": {},
            },
        ):
            server = create_server(self.orchestrator, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            host, port = server.server_address
            opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
            try:
                request = urllib_request.Request(
                    f"http://{host}:{port}/api/target-candidates/export",
                    data=json.dumps({"record_ids": ["target-export-record-1"]}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with opener.open(request) as response:
                    archive_body = response.read()
                    content_type = response.headers.get("Content-Type")
                    content_disposition = response.headers.get("Content-Disposition")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

        self.assertEqual(content_type, "application/zip")
        self.assertIn("attachment; filename=", content_disposition or "")
        self.assertRegex(content_disposition or "", r'filename="target-candidates-\d{8}T\d{4}\.zip"')
        self.assertNotIn("Z.zip", content_disposition or "")
        with zipfile.ZipFile(io.BytesIO(archive_body), "r") as archive:
            names = set(archive.namelist())
            csv_name = next(name for name in names if name.endswith("/target_candidates_summary.csv"))
            inventory_name = next(name for name in names if name.endswith("/profiles/profile_inventory.json"))
            profile_name = next(name for name in names if name.endswith("/profiles/Alice_Zhang__alice-zhang.json"))
            csv_payload = archive.read(csv_name).decode("utf-8-sig")
            inventory_payload = json.loads(archive.read(inventory_name).decode("utf-8"))
            profile_payload = json.loads(archive.read(profile_name).decode("utf-8"))

        self.assertIn("姓名,LinkedIn 链接,华人线索信息分层结果,在职状态,工作经历,教育经历,地区,Email", csv_payload)
        self.assertIn("Alice Zhang", csv_payload)
        self.assertIn("Layer 3 · 大陆经历/中文信号", csv_payload)
        self.assertIn("alice@openai.com", csv_payload)
        self.assertTrue(str(inventory_payload["generated_at"]).endswith("+08:00"))
        self.assertEqual(inventory_payload["record_count"], 1)
        self.assertEqual(inventory_payload["packaged_profile_count"], 1)
        self.assertEqual(inventory_payload["profiles"][0]["status"], "packaged")
        self.assertEqual(profile_payload["profile"]["fullName"], "Alice Zhang")

    def test_target_candidate_import_from_job_api_creates_records_from_results(self) -> None:
        job_id = "job_target_import_1"
        candidate_id = "cand_target_import_1"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "Excel 批量导入 OpenAI 候选人",
                "query": "Excel 批量导入 OpenAI 候选人",
                "target_company": "OpenAI",
            },
            plan_payload={},
            summary_payload={"workflow_kind": "excel_intake"},
        )
        self.store.upsert_candidate(
            Candidate(
                candidate_id=candidate_id,
                name_en="Bob Chen",
                display_name="Bob Chen",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="current",
                role="Research Engineer",
                linkedin_url="https://www.linkedin.com/in/bob-chen/",
                metadata={"headline": "Research Engineer at OpenAI"},
            )
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate_id,
                    "rank": 1,
                    "score": 0.91,
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "confidence_reason": "excel intake resolved",
                    "explanation": "Resolved from uploaded workbook.",
                    "matched_fields": ["linkedin"],
                }
            ],
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            request = urllib_request.Request(
                f"http://{host}:{port}/api/target-candidates/import-from-job",
                data=json.dumps({"job_id": job_id, "history_id": "history-import-1"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(payload["status"], "imported")
        self.assertEqual(payload["imported_count"], 1)
        records = self.store.list_target_candidates(history_id="history-import-1")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["candidate_name"], "Bob Chen")

    def test_target_candidate_public_web_api_queues_idempotent_runs(self) -> None:
        record = self.store.upsert_target_candidate(
            {
                "record_id": "target-public-web-1",
                "candidate_id": "cand-public-web-1",
                "candidate_name": "Alice Public",
                "current_company": "Example AI",
                "linkedin_url": "https://www.linkedin.com/in/alice-public/",
            }
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            request = urllib_request.Request(
                f"http://{host}:{port}/api/target-candidates/public-web-search",
                data=json.dumps(
                    {
                        "record_ids": [record["id"]],
                        "options": {
                            "max_queries_per_candidate": 4,
                            "fetch_content": False,
                            "ai_extraction": "off",
                        },
                    }
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(request) as response:
                queued = json.loads(response.read().decode("utf-8"))
            with opener.open(request) as response:
                joined = json.loads(response.read().decode("utf-8"))
            with opener.open(f"http://{host}:{port}/api/target-candidates/public-web-search") as response:
                listed = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(joined["status"], "joined")
        self.assertEqual(queued["batch"]["batch_id"], joined["batch"]["batch_id"])
        self.assertEqual(len(queued["runs"]), 1)
        self.assertEqual(queued["worker_summary"]["queued_worker_count"], 1)
        self.assertEqual(len(listed["runs"]), 1)
        workers = self.store.list_agent_workers(job_id=queued["job"]["job_id"])
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["wait_stage"], "waiting_remote_search")

    def test_target_candidate_public_web_detail_api_returns_model_safe_signals(self) -> None:
        record = self.store.upsert_target_candidate(
            {
                "record_id": "target-public-web-detail-1",
                "candidate_id": "cand-public-web-detail-1",
                "candidate_name": "Alice Detail",
                "current_company": "Example AI",
                "linkedin_url": "https://www.linkedin.com/in/alice-detail/",
            }
        )
        run = self.store.upsert_target_candidate_public_web_run(
            {
                "run_id": "public-web-detail-run-1",
                "batch_id": "public-web-detail-batch-1",
                "record_id": record["id"],
                "candidate_id": record["candidate_id"],
                "candidate_name": record["candidate_name"],
                "current_company": record["current_company"],
                "linkedin_url": record["linkedin_url"],
                "linkedin_url_key": "alice-detail",
                "person_identity_key": "linkedin:alice-detail",
                "idempotency_key": "public-web-detail-run-key-1",
                "status": "completed",
                "phase": "completed",
                "summary": {"email_candidate_count": 1, "entry_link_count": 1},
                "analysis_checkpoint": {"stage": "completed", "status": "completed"},
            }
        )
        asset = self.store.upsert_person_public_web_asset(
            {
                "asset_id": "public-web-detail-asset-1",
                "person_identity_key": "linkedin:alice-detail",
                "linkedin_url_key": "alice-detail",
                "latest_run_id": run["run_id"],
                "target_candidate_record_id": record["id"],
                "summary": {"email_candidate_count": 1},
                "signals": {"fetched_documents": [{"raw_path": "/tmp/raw.html"}]},
                "source_run_ids": [run["run_id"]],
            }
        )
        self.store.replace_person_public_web_signals_for_run(
            run_id=run["run_id"],
            signals=[
                {
                    "signal_id": "public-web-detail-email-1",
                    "run_id": run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:alice-detail",
                    "record_id": record["id"],
                    "candidate_id": record["candidate_id"],
                    "candidate_name": record["candidate_name"],
                    "linkedin_url_key": "alice-detail",
                    "signal_kind": "email_candidate",
                    "signal_type": "academic",
                    "email_type": "academic",
                    "value": "alice@example.edu",
                    "normalized_value": "alice@example.edu",
                    "source_url": "https://alice.example.edu/",
                    "source_domain": "alice.example.edu",
                    "source_family": "profile_web_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.9,
                    "publishable": True,
                    "promotion_status": "promotion_recommended",
                    "artifact_refs": {
                        "evidence_slice_path": "/tmp/evidence.json",
                        "raw_path": "/tmp/raw.html",
                    },
                    "metadata": {
                        "query_id": "homepage",
                        "raw_payload": {"html": "<html></html>"},
                    },
                },
                {
                    "signal_id": "public-web-detail-link-1",
                    "run_id": run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:alice-detail",
                    "record_id": record["id"],
                    "candidate_id": record["candidate_id"],
                    "candidate_name": record["candidate_name"],
                    "linkedin_url_key": "alice-detail",
                    "signal_kind": "profile_link",
                    "signal_type": "github_url",
                    "value": "https://github.com/alice-detail",
                    "normalized_value": "https://github.com/alice-detail",
                    "url": "https://github.com/alice-detail",
                    "source_url": "https://github.com/alice-detail",
                    "source_domain": "github.com",
                    "source_family": "technical_presence",
                    "identity_match_label": "ambiguous_identity",
                    "identity_match_score": 0.42,
                    "publishable": False,
                },
            ],
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            with opener.open(
                f"http://{host}:{port}/api/target-candidates/{record['id']}/public-web-search"
            ) as response:
                detail = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(detail["status"], "ok")
        self.assertEqual(detail["latest_run"]["run_id"], run["run_id"])
        self.assertEqual(detail["person_asset"]["asset_id"], asset["asset_id"])
        self.assertEqual(detail["email_candidates"][0]["promotion_status"], "promotion_recommended")
        self.assertEqual(detail["profile_links"][0]["identity_match_label"], "ambiguous_identity")
        self.assertEqual(detail["grouped_signals"]["email_candidates_by_type"]["academic"][0]["value"], "alice@example.edu")
        payload_text = json.dumps(detail, ensure_ascii=False)
        self.assertNotIn("raw_path", payload_text)
        self.assertNotIn("raw_payload", payload_text)
        self.assertNotIn("<html", payload_text)

    def test_target_candidate_public_web_promotion_updates_primary_email_then_export_uses_promoted_signals(self) -> None:
        record = self.store.upsert_target_candidate(
            {
                "record_id": "target-public-web-promote-1",
                "candidate_id": "cand-public-web-promote-1",
                "candidate_name": "Alice Promote",
                "current_company": "Example AI",
                "linkedin_url": "https://www.linkedin.com/in/alice-promote/",
            }
        )
        run = self.store.upsert_target_candidate_public_web_run(
            {
                "run_id": "public-web-promote-run-1",
                "batch_id": "public-web-promote-batch-1",
                "record_id": record["id"],
                "candidate_id": record["candidate_id"],
                "candidate_name": record["candidate_name"],
                "current_company": record["current_company"],
                "linkedin_url": record["linkedin_url"],
                "linkedin_url_key": "alice-promote",
                "person_identity_key": "linkedin:alice-promote",
                "idempotency_key": "public-web-promote-run-key-1",
                "status": "completed",
                "phase": "completed",
                "summary": {"email_candidate_count": 1, "entry_link_count": 2},
                "analysis_checkpoint": {"stage": "completed", "status": "completed"},
            }
        )
        asset = self.store.upsert_person_public_web_asset(
            {
                "asset_id": "public-web-promote-asset-1",
                "person_identity_key": "linkedin:alice-promote",
                "linkedin_url_key": "alice-promote",
                "latest_run_id": run["run_id"],
                "target_candidate_record_id": record["id"],
                "summary": {"email_candidate_count": 1},
                "source_run_ids": [run["run_id"]],
            }
        )
        self.store.replace_person_public_web_signals_for_run(
            run_id=run["run_id"],
            signals=[
                {
                    "signal_id": "public-web-promote-email-1",
                    "run_id": run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:alice-promote",
                    "record_id": record["id"],
                    "candidate_id": record["candidate_id"],
                    "candidate_name": record["candidate_name"],
                    "linkedin_url_key": "alice-promote",
                    "signal_kind": "email_candidate",
                    "signal_type": "academic",
                    "email_type": "academic",
                    "value": "alice.promote@example.edu",
                    "normalized_value": "alice.promote@example.edu",
                    "source_url": "https://alice-promote.example.edu/",
                    "source_domain": "alice-promote.example.edu",
                    "source_family": "profile_web_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.94,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.92,
                    "publishable": True,
                    "promotion_status": "promotion_recommended",
                    "evidence_excerpt": "Contact Alice at alice.promote@example.edu",
                    "artifact_refs": {
                        "evidence_slice_path": "/tmp/evidence.json",
                        "raw_path": "/tmp/raw.html",
                    },
                    "metadata": {"raw_payload": {"html": "<html></html>"}},
                },
                {
                    "signal_id": "public-web-promote-github-1",
                    "run_id": run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:alice-promote",
                    "record_id": record["id"],
                    "candidate_id": record["candidate_id"],
                    "candidate_name": record["candidate_name"],
                    "linkedin_url_key": "alice-promote",
                    "signal_kind": "profile_link",
                    "signal_type": "github_url",
                    "value": "https://github.com/alice-promote",
                    "normalized_value": "https://github.com/alice-promote",
                    "url": "https://github.com/alice-promote",
                    "source_url": "https://github.com/alice-promote",
                    "source_domain": "github.com",
                    "source_family": "technical_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.9,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.88,
                    "publishable": True,
                    "metadata": {"clean_profile_link": True, "link_shape_warnings": []},
                },
            ],
        )

        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            promote_request = urllib_request.Request(
                f"http://{host}:{port}/api/target-candidates/{record['id']}/public-web-promotions",
                data=json.dumps(
                    {
                        "signal_id": "public-web-promote-email-1",
                        "action": "promote",
                        "operator": "qa-user",
                    }
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(promote_request) as response:
                promoted = json.loads(response.read().decode("utf-8"))
            with opener.open(
                f"http://{host}:{port}/api/target-candidates/{record['id']}/public-web-search"
            ) as response:
                detail = json.loads(response.read().decode("utf-8"))
            export_request = urllib_request.Request(
                f"http://{host}:{port}/api/target-candidates/public-web-export",
                data=json.dumps({"record_ids": [record["id"]]}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(export_request) as response:
                archive_body = response.read()
                content_type = response.headers.get("Content-Type")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(promoted["status"], "promoted")
        self.assertEqual(promoted["promotion"]["previous_value"], "")
        self.assertEqual(promoted["promotion"]["new_value"], "alice.promote@example.edu")
        updated_record = self.store.get_target_candidate(record["id"])
        self.assertEqual(updated_record["primary_email"], "alice.promote@example.edu")
        self.assertEqual(updated_record["metadata"]["primary_email_metadata"]["source"], "public_web_search")
        self.assertEqual(detail["email_candidates"][0]["promotion_status"], "manually_promoted")
        self.assertEqual(detail["promotion_summary"]["promoted_email_count"], 1)
        self.assertEqual(content_type, "application/zip")
        with zipfile.ZipFile(io.BytesIO(archive_body), "r") as archive:
            names = set(archive.namelist())
            summary_name = next(name for name in names if name.endswith("/public_web_summary.csv"))
            signals_name = next(name for name in names if name.endswith("/public_web_signals.csv"))
            promotions_name = next(name for name in names if name.endswith("/public_web_promotions.csv"))
            manifest_name = next(name for name in names if name.endswith("/public_web_manifest.json"))
            summary_csv = archive.read(summary_name).decode("utf-8-sig")
            signals_csv = archive.read(signals_name).decode("utf-8-sig")
            promotions_csv = archive.read(promotions_name).decode("utf-8-sig")
            manifest = json.loads(archive.read(manifest_name).decode("utf-8"))
            archive_text = "\n".join(archive.read(name).decode("utf-8", errors="ignore") for name in names if name.endswith((".csv", ".json")))

        self.assertIn("alice.promote@example.edu", summary_csv)
        self.assertIn("manual_promoted", signals_csv)
        self.assertIn("qa-user", promotions_csv)
        self.assertFalse(manifest["raw_assets_included"])
        self.assertNotIn("raw_path", archive_text)
        self.assertNotIn("raw_payload", archive_text)
        self.assertNotIn("<html", archive_text)

    def test_asset_governance_promote_default_api_persists_pointer(self) -> None:
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            request = urllib_request.Request(
                f"http://{host}:{port}/api/assets/governance/promote-default",
                data=json.dumps(
                    {
                        "company_key": "OpenAI",
                        "snapshot_id": "snap-api-1",
                        "coverage_proof": {"coverage_scope": "company"},
                    }
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(request) as response:
                promoted = json.loads(response.read().decode("utf-8"))
            with opener.open(f"http://{host}:{port}/api/assets/governance/default-pointers?company_key=openai") as response:
                listed = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(promoted["status"], "promoted")
        self.assertEqual(promoted["asset_default_pointer"]["snapshot_id"], "snap-api-1")
        self.assertEqual(len(listed["asset_default_pointers"]), 1)
