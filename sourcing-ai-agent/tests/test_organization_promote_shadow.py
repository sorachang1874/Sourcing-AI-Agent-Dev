"""WS7/W7.3 S3 suite — SHADOW integration of the AI promote judgment.

Provenance: docs/WS7_AI_PROMOTE_DESIGN.md §2.4/§7 S3 (OQ4/OQ5/OQ7 RATIFIED
2026-07-24). S3 records an AI promote decision at the
``upsert_organization_asset_registry_with_guard`` seam (asset_reuse_planning.py:1541)
but NEVER changes the live promote decision — authority stays 100% the rule
ladder (``evaluate``) + the storage lineage guard. This suite pins:

  * a contested scripted-judge decision produces a shadow record (engaged, a
    validated ai_promote_decision.v1 + a ladder-divergence digest);
  * a non-contested decision records a SKIP shadow WITHOUT a model call;
  * THE key regression — the authoritative-row outcome is byte-identical with the
    shadow on vs off (same row authoritative, same registry decision), for both a
    promote and a keep-incumbent scenario;
  * a shadow exception is isolated: the live decision is unaffected and the error
    is recorded as the shadow record;
  * the additive shadow key is NOT persisted into the registry row (so the S0
    characterization oracle's persisted-record shape is structurally untouched).

PLACEMENT (honest calibration vs design §2.4): the ``organization_asset_registry``
table has no ``metadata``/``metadata_json`` column, so — mirroring the proven
divider S3 (shadow on the activity surface, durable registry field deferred to a
later slice) and honoring the task hard rule "no schema_version bump on the
registry — S5's job" — the shadow rides the seam's RETURNED record under the
sibling key ``ai_promote_decision_shadow``; the durable
``metadata.ai_promote_decision`` column is deferred to S5's schema bump.

PG-backed (the guard wrapper runs against the store). Skips when no local
Postgres is available unless SOURCING_REQUIRE_PG_STORE_TESTS=1.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import sourcing_agent.acquisition as acquisition_module
import sourcing_agent.candidate_artifacts as candidate_artifacts_module
import sourcing_agent.company_asset_completion as company_asset_completion_module
import sourcing_agent.company_asset_supplement as company_asset_supplement_module
import sourcing_agent.orchestrator as orchestrator_module
import sourcing_agent.snapshot_materializer as snapshot_materializer_module
from sourcing_agent.asset_reuse_planning import upsert_organization_asset_registry_with_guard
from sourcing_agent.candidate_artifacts import build_company_candidate_artifacts
from sourcing_agent.domain import Candidate, EvidenceRecord, make_evidence_id
from sourcing_agent.model_provider import (
    DeterministicModelClient,
    OfflineModelClient,
    ScriptedOrganizationPromoteJudgeModelClient,
)
from sourcing_agent.organization_promote_judgment import (
    SHADOW_RECORD_KEY,
    SHADOW_STATUS_ERROR,
    model_client_supports_promote_judgment,
    record_organization_promote_shadow,
)
from tests.pg_store_fixture import pg_backed_control_plane_store

_SCRIPTED_JUDGE_ENV = {"SOURCING_SCRIPTED_ORGANIZATION_PROMOTE_JUDGE": "1"}


def _registry_payload(
    snapshot_id: str,
    *,
    candidate_count: int,
    profile_detail_count: int,
    completeness_score: float,
    current_lane: int,
    former_lane: int = 0,
    source_snapshot_count: int = 1,
    generation_key: str = "",
    generation_sequence: int = 1,
    selected: list[str] | None = None,
    status: str = "ready",
    target_company: str = "OpenAI",
    company_key: str = "openai",
) -> dict[str, Any]:
    return {
        "target_company": target_company,
        "company_key": company_key,
        "snapshot_id": snapshot_id,
        "asset_view": "canonical_merged",
        "status": status,
        "candidate_count": candidate_count,
        "evidence_count": profile_detail_count,
        "profile_detail_count": profile_detail_count,
        "missing_linkedin_count": 0,
        "profile_completion_backlog_count": 0,
        "completeness_score": completeness_score,
        "completeness_band": "high" if completeness_score >= 75 else "medium",
        "current_lane_effective_candidate_count": current_lane,
        "former_lane_effective_candidate_count": former_lane,
        "source_snapshot_count": source_snapshot_count,
        "materialization_generation_key": generation_key or f"gen-{snapshot_id}",
        "materialization_generation_sequence": generation_sequence,
        "selected_snapshot_ids": list(selected if selected is not None else [snapshot_id]),
    }


def _incumbent_payload(**overrides: Any) -> dict[str, Any]:
    base = dict(
        snapshot_id="inc",
        candidate_count=332,
        profile_detail_count=332,
        completeness_score=88.0,
        current_lane=252,
        former_lane=80,
        source_snapshot_count=9,
        generation_key="lineage-inc",
        generation_sequence=6,
        selected=["inc"],
    )
    base.update(overrides)
    return _registry_payload(**base)


def _promote_candidate_payload(**overrides: Any) -> dict[str, Any]:
    # The oracle's materially_higher_coverage_with_stable_quality shape: wider
    # lane coverage at equal completeness → ladder promotes.
    base = dict(
        snapshot_id="cand",
        candidate_count=404,
        profile_detail_count=404,
        completeness_score=88.0,
        current_lane=293,
        former_lane=111,
        source_snapshot_count=11,
        generation_key="lineage-cand",
        generation_sequence=7,
        selected=["cand"],
    )
    base.update(overrides)
    return _registry_payload(**base)


def _thinner_candidate_payload(**overrides: Any) -> dict[str, Any]:
    # Strictly thinner + lower score → the ladder's contested guard_rejected.
    base = dict(
        snapshot_id="cand",
        candidate_count=150,
        profile_detail_count=150,
        completeness_score=70.0,
        current_lane=150,
        former_lane=0,
        source_snapshot_count=1,
        generation_key="lineage-cand",
        generation_sequence=7,
        selected=["cand"],
    )
    base.update(overrides)
    return _registry_payload(**base)


def _authoritative_map(store: Any, *, target_company: str = "OpenAI") -> dict[str, int]:
    rows = store.list_organization_asset_registry(target_company=target_company)
    return {str(r["snapshot_id"]): int(bool(r["authoritative"])) for r in rows}


def _scripted_client() -> ScriptedOrganizationPromoteJudgeModelClient:
    return ScriptedOrganizationPromoteJudgeModelClient(mode="simulate")


class _SpyJudgeClient(DeterministicModelClient):
    """Judge-capable spy: counts calls, returns a canned response or raises."""

    def __init__(self, *, response: dict[str, Any] | None = None, raises: Exception | None = None) -> None:
        self.response = response or {}
        self.raises = raises
        self.judge_calls = 0

    def judge_organization_asset_promotion(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.judge_calls += 1
        if self.raises is not None:
            raise self.raises
        return self.response


class CapabilityGateTest(unittest.TestCase):
    """Condition (a): only a judge-capable client engages the shadow hook."""

    def test_deterministic_and_offline_clients_are_not_judge_capable(self) -> None:
        self.assertFalse(model_client_supports_promote_judgment(None))
        self.assertFalse(model_client_supports_promote_judgment(DeterministicModelClient()))
        self.assertFalse(model_client_supports_promote_judgment(OfflineModelClient(mode="simulate")))

    def test_scripted_and_spy_clients_are_judge_capable(self) -> None:
        self.assertTrue(model_client_supports_promote_judgment(_scripted_client()))
        self.assertTrue(model_client_supports_promote_judgment(_SpyJudgeClient()))

    def test_no_client_records_no_shadow(self) -> None:
        record = record_organization_promote_shadow(
            None,
            existing_authoritative={},
            candidate_record=_promote_candidate_payload(),
            ladder_decision={"promote": True, "reason": "materially_higher_coverage_with_stable_quality"},
        )
        self.assertIsNone(record)

    def test_offline_client_records_no_shadow(self) -> None:
        record = record_organization_promote_shadow(
            OfflineModelClient(mode="simulate"),
            existing_authoritative=_incumbent_payload(),
            candidate_record=_promote_candidate_payload(),
            ladder_decision={"promote": True, "reason": "materially_higher_coverage_with_stable_quality"},
        )
        self.assertIsNone(record)


class ContestedShadowRecordTest(unittest.TestCase):
    """A contested scripted-judge decision produces the shadow record."""

    def test_contested_promote_records_engaged_shadow(self) -> None:
        with pg_backed_control_plane_store(schema_label="promote_shadow_contested") as store:
            store.upsert_organization_asset_registry(_incumbent_payload(), authoritative=True)
            with patch.dict(os.environ, _SCRIPTED_JUDGE_ENV, clear=False):
                result = upsert_organization_asset_registry_with_guard(
                    store=store,
                    candidate_record=_promote_candidate_payload(),
                    model_client=_scripted_client(),
                )
            shadow = result[SHADOW_RECORD_KEY]
            self.assertTrue(shadow["engaged"])
            self.assertTrue(shadow["contested"])
            self.assertEqual(shadow["ai_status"], "promoted")
            self.assertEqual(shadow["decision"]["decision"], "promote")
            self.assertEqual(shadow["decision"]["reason_code"], "ai_coverage_superset")
            self.assertIsNone(shadow["audit"])
            self.assertFalse(shadow["guard_predicted_verdict"]["refused"])
            comparison = shadow["ladder_comparison"]
            self.assertTrue(comparison["ladder_promote"])
            self.assertTrue(comparison["ai_promote"])
            self.assertTrue(comparison["agreement"])
            self.assertEqual(comparison["divergence"], "agree")
            # The candidate became authoritative — the live decision (the ladder's)
            # is unchanged by the shadow.
            self.assertEqual(_authoritative_map(store), {"inc": 0, "cand": 1})

    def test_contested_keep_incumbent_records_honest_reject_shadow(self) -> None:
        with pg_backed_control_plane_store(schema_label="promote_shadow_contested_keep") as store:
            store.upsert_organization_asset_registry(_incumbent_payload(), authoritative=True)
            with patch.dict(os.environ, _SCRIPTED_JUDGE_ENV, clear=False):
                result = upsert_organization_asset_registry_with_guard(
                    store=store,
                    candidate_record=_thinner_candidate_payload(),
                    model_client=_scripted_client(),
                )
            shadow = result[SHADOW_RECORD_KEY]
            self.assertTrue(shadow["engaged"])
            self.assertTrue(shadow["contested"])
            self.assertEqual(shadow["ai_status"], "kept_incumbent")
            # Scripted honest reject on strictly-narrower coverage.
            self.assertEqual(shadow["decision"]["decision"], "reject")
            self.assertEqual(shadow["decision"]["reason_code"], "ai_coverage_not_superset")
            self.assertEqual(shadow["ladder_comparison"]["divergence"], "agree")
            self.assertEqual(_authoritative_map(store), {"inc": 1, "cand": 0})


class NonContestedSkipTest(unittest.TestCase):
    """Non-contested (deterministic pre-branch) → skip shadow, no model call."""

    def test_no_incumbent_skips_without_a_model_call(self) -> None:
        spy = _SpyJudgeClient(response={})
        with pg_backed_control_plane_store(schema_label="promote_shadow_no_incumbent") as store:
            result = upsert_organization_asset_registry_with_guard(
                store=store,
                candidate_record=_promote_candidate_payload(),
                model_client=spy,
            )
            shadow = result[SHADOW_RECORD_KEY]
            self.assertEqual(spy.judge_calls, 0)
            self.assertFalse(shadow["engaged"])
            self.assertIsNone(shadow["decision"])
            self.assertIsNone(shadow["audit"])
            self.assertTrue(shadow["skip_reason"])
            self.assertEqual(shadow["ladder_comparison"]["divergence"], "not_engaged")
            self.assertTrue(shadow["ladder_comparison"]["ladder_promote"])
            self.assertTrue(shadow["ladder_comparison"]["ai_promote"])
            # No-incumbent promote is the deterministic pre-branch outcome.
            self.assertEqual(_authoritative_map(store), {"cand": 1})

    def test_lifecycle_not_promotable_skips_without_a_model_call(self) -> None:
        spy = _SpyJudgeClient(response={})
        with pg_backed_control_plane_store(schema_label="promote_shadow_not_promotable") as store:
            store.upsert_organization_asset_registry(_incumbent_payload(), authoritative=True)
            result = upsert_organization_asset_registry_with_guard(
                store=store,
                candidate_record=_promote_candidate_payload(status="archived"),
                model_client=spy,
            )
            shadow = result[SHADOW_RECORD_KEY]
            self.assertEqual(spy.judge_calls, 0)
            self.assertFalse(shadow["engaged"])
            self.assertFalse(shadow["ladder_comparison"]["ladder_promote"])
            self.assertFalse(shadow["ladder_comparison"]["ai_promote"])
            self.assertEqual(_authoritative_map(store), {"inc": 1, "cand": 0})


class ByteIdenticalAuthorityTest(unittest.TestCase):
    """THE key regression: the authoritative-row outcome is byte-identical with
    the shadow on vs off. Runs the promote/upsert path both ways in fresh schemas
    and asserts the same row becomes authoritative and the registry decision is
    unchanged; the shadow only adds a sibling key to the RETURNED record."""

    def _run(
        self, *, candidate_payload: dict[str, Any], model_client: Any, schema_label: str
    ) -> tuple[dict[str, Any], dict[str, int], bool]:
        with pg_backed_control_plane_store(schema_label=schema_label) as store:
            store.upsert_organization_asset_registry(_incumbent_payload(), authoritative=True)
            with patch.dict(os.environ, _SCRIPTED_JUDGE_ENV, clear=False):
                result = upsert_organization_asset_registry_with_guard(
                    store=store,
                    candidate_record=candidate_payload,
                    model_client=model_client,
                )
            return result, _authoritative_map(store), bool(result.get("authoritative"))

    def _assert_identical(self, *, candidate_factory: Any, label: str) -> None:
        off_result, off_map, off_auth = self._run(
            candidate_payload=candidate_factory(), model_client=None, schema_label=f"{label}_off"
        )
        on_result, on_map, on_auth = self._run(
            candidate_payload=candidate_factory(), model_client=_scripted_client(), schema_label=f"{label}_on"
        )
        # Same row authoritative + same candidate-row registry decision.
        self.assertEqual(off_map, on_map)
        self.assertEqual(off_auth, on_auth)
        # Shadow off writes NO sibling key; shadow on writes exactly the one key.
        self.assertNotIn(SHADOW_RECORD_KEY, off_result)
        self.assertIn(SHADOW_RECORD_KEY, on_result)
        # Byte-identity of the persisted registry record: the returned dicts are
        # identical once the additive shadow key and the wall-clock timestamps
        # (which differ per fresh schema) are excluded.
        volatile = {SHADOW_RECORD_KEY, "registry_id", "created_at", "updated_at"}
        off_core = {k: v for k, v in off_result.items() if k not in volatile}
        on_core = {k: v for k, v in on_result.items() if k not in volatile}
        self.assertEqual(off_core, on_core)

    def test_promote_scenario_is_byte_identical(self) -> None:
        self._assert_identical(candidate_factory=_promote_candidate_payload, label="promote_shadow_bytepromote")

    def test_keep_incumbent_scenario_is_byte_identical(self) -> None:
        self._assert_identical(candidate_factory=_thinner_candidate_payload, label="promote_shadow_bytekeep")


class ShadowFailureIsolationTest(unittest.TestCase):
    """A shadow exception NEVER affects the live decision or the registry write."""

    def test_shadow_exception_is_recorded_and_live_decision_unaffected(self) -> None:
        raising = _SpyJudgeClient(raises=RuntimeError("scripted judge boom"))
        with pg_backed_control_plane_store(schema_label="promote_shadow_exception") as store:
            store.upsert_organization_asset_registry(_incumbent_payload(), authoritative=True)
            result = upsert_organization_asset_registry_with_guard(
                store=store,
                candidate_record=_promote_candidate_payload(),
                model_client=raising,
            )
            # The candidate still promoted per the ladder (live decision intact).
            self.assertEqual(_authoritative_map(store), {"inc": 0, "cand": 1})
            self.assertTrue(bool(result.get("authoritative")))
            self.assertEqual(raising.judge_calls, 1)
            shadow = result[SHADOW_RECORD_KEY]
            self.assertEqual(shadow["shadow_status"], SHADOW_STATUS_ERROR)
            self.assertFalse(shadow["engaged"])
            self.assertIn("boom", shadow["shadow_error"])
            self.assertEqual(shadow["shadow_error_type"], "RuntimeError")


class AdditiveKeyDoesNotPollutePersistedRecordTest(unittest.TestCase):
    """The additive shadow key is NOT persisted into the registry row — so the S0
    characterization oracle's persisted-record shape is structurally untouched."""

    def test_shadow_key_absent_from_the_persisted_registry_row(self) -> None:
        with pg_backed_control_plane_store(schema_label="promote_shadow_persist") as store:
            store.upsert_organization_asset_registry(_incumbent_payload(), authoritative=True)
            with patch.dict(os.environ, _SCRIPTED_JUDGE_ENV, clear=False):
                upsert_organization_asset_registry_with_guard(
                    store=store,
                    candidate_record=_promote_candidate_payload(),
                    model_client=_scripted_client(),
                )
            for row in store.list_organization_asset_registry(target_company="OpenAI"):
                self.assertNotIn(SHADOW_RECORD_KEY, row)
                self.assertNotIn("ai_promote_decision", row)
            authoritative = store.get_authoritative_organization_asset_registry(
                target_company="OpenAI", asset_view="canonical_merged"
            )
            self.assertEqual(authoritative["snapshot_id"], "cand")
            self.assertNotIn(SHADOW_RECORD_KEY, authoritative)


def _ratchet_candidate(index: int) -> Candidate:
    return Candidate(
        candidate_id=f"c{index}",
        name_en=f"Person {index}",
        display_name=f"Person {index}",
        category="employee",
        target_company="Acme",
        employment_status="current",
        role="Research Engineer",
        linkedin_url=f"https://www.linkedin.com/in/acme-person-{index:04d}/",
        source_dataset="linkedin_roster",
    )


def _ratchet_evidence(index: int) -> EvidenceRecord:
    url = f"https://www.linkedin.com/in/acme-person-{index:04d}/"
    return EvidenceRecord(
        evidence_id=make_evidence_id(f"c{index}", "linkedin_profile", "LinkedIn", url),
        candidate_id=f"c{index}",
        source_type="linkedin_profile",
        title="LinkedIn",
        url=url,
        summary="Roster evidence.",
        source_dataset="linkedin_roster",
        source_path="/tmp/roster.json",
        metadata={"profile_url": url},
    )


def _write_ratchet_snapshot(runtime_dir: Path, snapshot_id: str, candidate_count: int) -> None:
    snapshot_dir = runtime_dir / "company_assets" / "acme" / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "company_assets" / "acme" / "latest_snapshot.json").write_text(
        json.dumps(
            {
                "snapshot_id": snapshot_id,
                "company_identity": {
                    "requested_name": "Acme",
                    "canonical_name": "Acme",
                    "company_key": "acme",
                    "aliases": [],
                },
            }
        ),
        encoding="utf-8",
    )
    (snapshot_dir / "candidate_documents.json").write_text(
        json.dumps(
            {
                "candidates": [_ratchet_candidate(i).to_record() for i in range(candidate_count)],
                "evidence": [_ratchet_evidence(i).to_record() for i in range(candidate_count)],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


class ProductionEntrypointWiringRatchetTest(unittest.TestCase):
    """ANTI-INERTNESS RATCHET (2026-07-25) — the promote shadow path must stay
    reachable from the PRODUCTION artifact-build entrypoint.

    Until 2026-07-25 ``upsert_organization_asset_registry_with_guard``'s
    ``model_client`` parameter was DEAD in production: ``asset_registration.py``
    threaded it, but every production caller of
    ``sync_company_asset_registration`` left it at its ``None`` default and the
    layer above (``build_company_candidate_artifacts``) had no such parameter at
    all. So ``record_organization_promote_shadow`` was always called with
    ``None`` and returned on its first line — the S3 shadow could only ever be
    observed from tests that call the seam directly (every other test in this
    file does exactly that, so none of them would have caught it).

    This class drives the REAL ``build_company_candidate_artifacts`` entrypoint
    against a PG-backed store and a real on-disk snapshot, and asserts a shadow
    record actually lands on ``sync_status["organization_asset_registry_refresh"]
    ["result"]``. It fails if the thread-through is ever removed or if a caller
    silently reverts to the default.

    Zero provider calls: the client is the deterministic scripted judge.
    """

    def _build(self, model_client: Any, *, schema_label: str) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            with pg_backed_control_plane_store(schema_label=schema_label) as store:
                # First build: no incumbent → the deterministic pre-branch.
                _write_ratchet_snapshot(runtime_dir, "20260101T000000", 40)
                build_company_candidate_artifacts(
                    runtime_dir=runtime_dir,
                    store=store,
                    target_company="Acme",
                    snapshot_id="20260101T000000",
                    model_client=model_client,
                )
                # Second build: a wider competing snapshot → a CONTESTED decision.
                _write_ratchet_snapshot(runtime_dir, "20260202T000000", 80)
                second = build_company_candidate_artifacts(
                    runtime_dir=runtime_dir,
                    store=store,
                    target_company="Acme",
                    snapshot_id="20260202T000000",
                    model_client=model_client,
                )
                authoritative = store.get_authoritative_organization_asset_registry(
                    target_company="Acme", asset_view="canonical_merged"
                )
                return {
                    "refresh": dict(
                        dict(second.get("sync_status") or {}).get("organization_asset_registry_refresh") or {}
                    ),
                    "authoritative_snapshot_id": str(authoritative.get("snapshot_id") or ""),
                }

    def test_judge_capable_client_reaches_the_seam_from_build_company_candidate_artifacts(self) -> None:
        with patch.dict(os.environ, _SCRIPTED_JUDGE_ENV, clear=False):
            outcome = self._build(_scripted_client(), schema_label="promote_shadow_ratchet_on")
        refresh = outcome["refresh"]
        self.assertEqual(refresh.get("status"), "completed")
        result = dict(refresh.get("result") or {})
        # THE ratchet assertion.
        self.assertIn(
            SHADOW_RECORD_KEY,
            result,
            "WS7/W7.3 S3 promote shadow path went INERT at the production entrypoint: "
            "build_company_candidate_artifacts no longer threads model_client down to "
            "upsert_organization_asset_registry_with_guard.",
        )
        shadow = dict(result[SHADOW_RECORD_KEY])
        self.assertEqual(shadow["mode"], "shadow")
        self.assertTrue(shadow["contested"], "the wider second snapshot must produce a CONTESTED decision")
        self.assertTrue(shadow["engaged"])
        self.assertTrue(str(shadow["decision_id"]))
        self.assertIsNotNone(shadow["decision"])
        self.assertIn(
            dict(shadow["ladder_comparison"])["divergence"],
            {"agree", "ai_more_conservative", "ai_more_permissive"},
        )
        # Authority is untouched by the shadow: the ladder promoted the wider row.
        self.assertEqual(outcome["authoritative_snapshot_id"], "20260202T000000")

    def test_default_none_client_stays_structurally_inert(self) -> None:
        outcome = self._build(None, schema_label="promote_shadow_ratchet_off")
        self.assertNotIn(SHADOW_RECORD_KEY, dict(outcome["refresh"].get("result") or {}))
        self.assertEqual(outcome["authoritative_snapshot_id"], "20260202T000000")

    def test_offline_default_client_stays_structurally_inert(self) -> None:
        # The daemon's real simulate-default client: threaded but not judge-capable.
        outcome = self._build(OfflineModelClient(mode="simulate"), schema_label="promote_shadow_ratchet_offline")
        self.assertNotIn(SHADOW_RECORD_KEY, dict(outcome["refresh"].get("result") or {}))
        self.assertEqual(outcome["authoritative_snapshot_id"], "20260202T000000")


class ProductionCallerThreadingPinTest(unittest.TestCase):
    """Source-level pins: the thread-through exists at the entrypoint AND every
    production caller that holds a model client supplies it.

    Without these pins a caller could silently drop back to the default and the
    functional ratchet above (which passes the client explicitly) would stay
    green while production went inert again.
    """

    def test_build_company_candidate_artifacts_passes_model_client_to_the_registration_sync(self) -> None:
        source = Path(candidate_artifacts_module.__file__).read_text(encoding="utf-8")
        self.assertIn("    model_client: Any = None,\n", source)
        self.assertIn("            model_client=model_client,\n", source)

    def test_every_model_client_holding_caller_threads_it(self) -> None:
        expected = {
            acquisition_module: 1,
            company_asset_completion_module: 2,
            company_asset_supplement_module: 2,
            orchestrator_module: 2,
            snapshot_materializer_module: 1,
        }
        for module, count in expected.items():
            with self.subTest(module=module.__name__):
                source = Path(module.__file__).read_text(encoding="utf-8")
                needle = (
                    '"model_client": self.model_client,'
                    if module is acquisition_module
                    else "model_client=self.model_client,"
                )
                # `build_company_candidate_artifacts` callsites that hold a model
                # client must pass it; anything less re-opens the inertness gap.
                self.assertGreaterEqual(
                    source.count(needle),
                    count,
                    f"{module.__name__} stopped threading model_client into "
                    "build_company_candidate_artifacts (WS7/W7.3 S3 shadow goes inert).",
                )


if __name__ == "__main__":
    unittest.main()
