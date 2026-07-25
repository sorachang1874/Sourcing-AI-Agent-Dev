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

import ast
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import sourcing_agent.candidate_artifacts as candidate_artifacts_module
import sourcing_agent.model_provider as model_provider_module
from sourcing_agent.asset_reuse_planning import upsert_organization_asset_registry_with_guard
from sourcing_agent.candidate_artifacts import build_company_candidate_artifacts
from sourcing_agent.domain import Candidate, EvidenceRecord, make_evidence_id
from sourcing_agent.model_provider import (
    WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL_ENV,
    WS7_SHADOW_BILLING_FREE_ATTR,
    DeterministicModelClient,
    OfflineModelClient,
    ScriptedOrganizationPromoteJudgeModelClient,
    ScriptedProfileBatchDividerModelClient,
)
from sourcing_agent.organization_promote_judgment import (
    SHADOW_RECORD_KEY,
    SHADOW_STATUS_ERROR,
    model_client_supports_promote_judgment,
    promote_shadow_model_calls_permitted,
    record_organization_promote_shadow,
)
from tests.pg_store_fixture import pg_backed_control_plane_store

_SCRIPTED_JUDGE_ENV = {"SOURCING_SCRIPTED_ORGANIZATION_PROMOTE_JUDGE": "1"}
_REAL_MODEL_SHADOW_OPT_IN = {WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL_ENV: "1"}


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
    """Judge-capable spy: counts calls, returns a canned response or raises.

    DECLARED billing-free (it is a local object that never touches a network),
    so it stands in for the scripted judge under the 2026-07-25 safety gate.
    ``_RealCapableJudgeClient`` below is the deliberately UNdeclared twin used to
    pin that an ungated real-capable client makes no call.
    """

    ws7_shadow_billing_free = True

    def __init__(self, *, response: dict[str, Any] | None = None, raises: Exception | None = None) -> None:
        self.response = response or {}
        self.raises = raises
        self.judge_calls = 0

    def judge_organization_asset_promotion(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.judge_calls += 1
        if self.raises is not None:
            raise self.raises
        return self.response


class _RealCapableJudgeClient(DeterministicModelClient):
    """Stands in for a REAL provider client at the seam: structurally
    judge-capable (it overrides the deterministic stub) and NOT declared
    billing-free — exactly the shape of ``OpenAICompatibleChatModelClient`` /
    ``QwenResponsesModelClient``.

    It performs no network I/O: it only RECORDS that a call was attempted. A
    recorded call is precisely the defect being pinned, because on a real client
    that same reachability is a billed request.
    """

    def __init__(self) -> None:
        self.judge_calls = 0

    def judge_organization_asset_promotion(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.judge_calls += 1
        return {
            "judgment": {
                "decision": "promote",
                "reason": "a real model would have been billed for this",
                "reason_code": "ai_coverage_superset",
            },
            "provenance": {},
        }


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


class RealModelShadowSafetyGateTest(unittest.TestCase):
    """SAFETY GATE (2026-07-25) — capability is NOT permission.

    The 2026-07-25 wiring made ``self.model_client`` reach
    ``record_organization_promote_shadow`` from eight production callers. The
    engagement gate at that moment was a bare structural type/method check, which
    is True for the REAL provider clients as well — so any live-mode process
    would have issued a BILLED, synchronous provider call from inside
    ``upsert_organization_asset_registry_with_guard``, i.e. on the authoritative
    write path, with no env opt-in and no kill switch.

    These tests pin the fix: a real-capable (non-billing-free) client reaching
    the seam WITHOUT ``SOURCING_WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL`` makes ZERO
    model calls and produces NO record; with the explicit opt-in it engages.
    """

    def _clean_env(self) -> dict[str, str]:
        # Ensure the opt-in is genuinely absent, whatever the ambient env holds.
        return {WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL_ENV: ""}

    def test_real_capable_client_without_opt_in_makes_no_model_call(self) -> None:
        client = _RealCapableJudgeClient()
        self.assertTrue(
            model_client_supports_promote_judgment(client),
            "the probe must still consider it CAPABLE — the gate is permission, not capability",
        )
        with patch.dict(os.environ, self._clean_env(), clear=False):
            self.assertFalse(promote_shadow_model_calls_permitted(client))
            record = record_organization_promote_shadow(
                client,
                existing_authoritative=_incumbent_payload(),
                candidate_record=_promote_candidate_payload(),
                ladder_decision={"promote": True, "reason": "materially_higher_coverage_with_stable_quality"},
            )
        self.assertIsNone(record, "an ungated real-capable client must produce NO shadow record")
        self.assertEqual(client.judge_calls, 0, "a BILLED model call would have been made on the write path")

    def test_real_capable_client_without_opt_in_is_inert_through_the_production_entrypoint(self) -> None:
        """The same pin, driven through the real production entrypoint chain
        (``build_company_candidate_artifacts`` → ``sync_company_asset_registration``
        → the guard seam) rather than by calling the hook directly."""
        client = _RealCapableJudgeClient()
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            with pg_backed_control_plane_store(schema_label="promote_shadow_safety_gate") as store:
                with patch.dict(os.environ, self._clean_env(), clear=False):
                    _write_ratchet_snapshot(runtime_dir, "20260101T000000", 40)
                    build_company_candidate_artifacts(
                        runtime_dir=runtime_dir,
                        store=store,
                        target_company="Acme",
                        snapshot_id="20260101T000000",
                        model_client=client,
                    )
                    _write_ratchet_snapshot(runtime_dir, "20260202T000000", 80)
                    second = build_company_candidate_artifacts(
                        runtime_dir=runtime_dir,
                        store=store,
                        target_company="Acme",
                        snapshot_id="20260202T000000",
                        model_client=client,
                    )
        refresh = dict(dict(second.get("sync_status") or {}).get("organization_asset_registry_refresh") or {})
        self.assertNotIn(SHADOW_RECORD_KEY, dict(refresh.get("result") or {}))
        self.assertEqual(client.judge_calls, 0)

    def test_explicit_opt_in_re_enables_the_real_capable_client(self) -> None:
        client = _RealCapableJudgeClient()
        with patch.dict(os.environ, _REAL_MODEL_SHADOW_OPT_IN, clear=False):
            self.assertTrue(promote_shadow_model_calls_permitted(client))
            record = record_organization_promote_shadow(
                client,
                existing_authoritative=_incumbent_payload(),
                candidate_record=_promote_candidate_payload(),
                ladder_decision={"promote": True, "reason": "materially_higher_coverage_with_stable_quality"},
            )
        self.assertIsNotNone(record)
        self.assertEqual(client.judge_calls, 1)

    def test_scripted_judge_needs_no_opt_in(self) -> None:
        """The billing-free scripted client is unaffected — the gate must not
        make the offline evidence path harder to run than it already is."""
        with patch.dict(os.environ, {**_SCRIPTED_JUDGE_ENV, **self._clean_env()}, clear=False):
            self.assertTrue(promote_shadow_model_calls_permitted(_scripted_client()))
            record = record_organization_promote_shadow(
                _scripted_client(),
                existing_authoritative=_incumbent_payload(),
                candidate_record=_promote_candidate_payload(),
                ladder_decision={"promote": True, "reason": "materially_higher_coverage_with_stable_quality"},
            )
        self.assertIsNotNone(record)
        self.assertTrue(dict(record or {})["engaged"])

    def test_the_opt_in_is_not_implied_by_any_live_provider_gate_var(self) -> None:
        """Turning the system live must NOT turn the shadow's real-model call on:
        the opt-in is a separate, dedicated variable."""
        client = _RealCapableJudgeClient()
        live_ish = {
            "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
            "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
            "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
            WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL_ENV: "",
        }
        with patch.dict(os.environ, live_ish, clear=False):
            self.assertFalse(promote_shadow_model_calls_permitted(client))
            record = record_organization_promote_shadow(
                client,
                existing_authoritative=_incumbent_payload(),
                candidate_record=_promote_candidate_payload(),
                ladder_decision={"promote": True, "reason": "materially_higher_coverage_with_stable_quality"},
            )
        self.assertIsNone(record)
        self.assertEqual(client.judge_calls, 0)

    def test_only_the_declared_scripted_clients_are_billing_free(self) -> None:
        """Pin the exact allowlist. The declaration is a class attribute, so this
        is the test that keeps a real provider client from quietly acquiring it."""
        billing_free = sorted(
            name
            for name, obj in vars(model_provider_module).items()
            if isinstance(obj, type) and bool(getattr(obj, WS7_SHADOW_BILLING_FREE_ATTR, False))
        )
        self.assertEqual(
            billing_free,
            [
                ScriptedOrganizationPromoteJudgeModelClient.__name__,
                ScriptedProfileBatchDividerModelClient.__name__,
            ],
            "a client class newly declared ws7_shadow_billing_free bypasses the WS7 shadow real-model "
            "opt-in — only the scripted (never-billing) clients may carry it",
        )


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


# ---------------------------------------------------------------------------
# WS7/W7.3 S3 PROMOTE-SHADOW WIRING REGISTRY (2026-07-25, hardened same day)
#
# WHY A REGISTRY. The first version of this ratchet asserted
# `source.count("model_client=self.model_client,") >= N` over each whole caller
# module. That is vacuous: those modules contain many unrelated occurrences of
# the same literal, so five of the nine wiring points could be deleted with the
# whole suite still green (adversarial verification 2026-07-25). The pin below
# instead PARSES each production module and matches every
# `build_company_candidate_artifacts` callsite against a declared expectation,
# by (module, enclosing qualname, ordinal) — never by line number.
#
# COVERAGE HONESTY. Wiring point #1 (`candidate_artifacts.py`'s pass-through
# into `sync_company_asset_registration`) has FUNCTIONAL coverage:
# `ProductionEntrypointWiringRatchetTest` drives the real entrypoint against a PG
# store and goes red if the pass-through is removed. The eight CALLER wiring
# points have SOURCE-LEVEL coverage only — this registry. Driving
# `SnapshotMaterializer.synchronize_snapshot_candidate_documents`,
# `SourcingOrchestrator._reconcile_completed_workflow_if_needed`, the two
# supplement and two completion manager entrypoints and the acquisition engine
# end-to-end would each need a full job-request/snapshot/store fixture to verify
# one pass-through kwarg. That is stated here rather than implied, per the review
# finding: this ratchet proves the kwarg is WRITTEN at every callsite, not that
# every callsite executes in a test.
#
# The registry doubles as the residual inventory design §7.1 used to carry as
# prose: every callsite that is deliberately left unthreaded is listed with its
# reason and whether it can still reach the promote seam.
# ---------------------------------------------------------------------------

_WIRING_TARGET = "build_company_candidate_artifacts"

# (module file, enclosing qualname, ordinal) -> expectation
_PROMOTE_SHADOW_WIRING_REGISTRY: dict[tuple[str, str, int], dict[str, Any]] = {
    # -- threaded: the production callers that hold a model client -------------
    ("acquisition.py", "AcquisitionEngine::_build_snapshot_candidate_artifacts", 0): {
        "model_client": "self.model_client",
        "reaches_promote_seam": True,
        "note": "workflow acquisition snapshot build (kwargs dict + ** expansion)",
    },
    ("snapshot_materializer.py", "SnapshotMaterializer::synchronize_snapshot_candidate_documents", 0): {
        "model_client": "self.model_client",
        "reaches_promote_seam": True,
        "note": "the daemon's main materialization path",
    },
    ("orchestrator.py", "SourcingOrchestrator::_process_excel_snapshot_full_materialization_item", 0): {
        "model_client": "self.model_client",
        "reaches_promote_seam": True,
        "note": "Excel deferred artifact materialization",
    },
    ("orchestrator.py", "SourcingOrchestrator::_reconcile_completed_workflow_if_needed", 0): {
        "model_client": "self.model_client",
        "reaches_promote_seam": True,
        "note": "background exploration reconcile rebuild",
    },
    ("company_asset_supplement.py", "CompanyAssetSupplementManager::supplement_snapshot", 0): {
        "model_client": "self.model_client",
        "reaches_promote_seam": True,
        "note": "incremental supplement",
    },
    ("company_asset_supplement.py", "CompanyAssetSupplementManager::merge_candidates_into_snapshot", 0): {
        "model_client": "self.model_client",
        "reaches_promote_seam": True,
        "note": "candidate merge into an existing snapshot",
    },
    ("company_asset_completion.py", "CompanyAssetCompletionManager::complete_company_assets", 0): {
        "model_client": "self.model_client",
        "reaches_promote_seam": True,
        "note": "company asset completion",
    },
    ("company_asset_completion.py", "CompanyAssetCompletionManager::complete_snapshot_profiles", 0): {
        "model_client": "self.model_client",
        "reaches_promote_seam": True,
        "note": "snapshot profile completion",
    },
    # -- deliberately unthreaded, but they DO reach the promote seam ----------
    # (RESIDUAL: recorded here and in docs/WS7_AI_PROMOTE_DESIGN.md §7.1. Left
    # unthreaded on purpose in the 2026-07-25 remediation batch: the batch's
    # first-priority finding was that the shadow's reach into a billed provider
    # call was TOO easy, so widening reach in the same pass is the wrong
    # direction. Wiring them is S5 scope, together with the durable record.)
    ("candidate_artifacts.py", "repair_missing_company_candidate_artifacts", 0): {
        "model_client": None,
        "reaches_promote_seam": True,
        "note": "operator/import repair path (cli.py + cloud_asset_import.py); holds no model client",
    },
    ("candidate_artifacts.py", "load_authoritative_company_snapshot_candidate_documents", 0): {
        "model_client": None,
        "reaches_promote_seam": True,
        "note": (
            "materialization fallback inside a READ path; its acquisition.py caller does hold a client "
            "but the read function has no parameter to carry it — S5 scope"
        ),
    },
    ("runtime_rebuild.py", "_repair_snapshot_artifacts_if_needed", 0): {
        "model_client": None,
        "reaches_promote_seam": True,
        "note": "runtime rebuild repair path; holds no model client",
    },
    ("cli.py", "_cli_cmd_store_command_group_2", 0): {
        "model_client": None,
        "reaches_promote_seam": True,
        "note": (
            "operator CLI `build-company-candidate-artifacts` / `rebuild-company-serving-view`; "
            "has settings in scope and COULD build a client (cli.py does so for the divider) — S5 scope"
        ),
    },
    # -- structurally exempt: they never reach the registration sync ----------
    ("authoritative_serving_repair.py", "repair_authoritative_serving_generation", 0): {
        "model_client": None,
        "reaches_promote_seam": False,
        "note": "sync_registration=False — the guard seam is never entered",
    },
    ("authoritative_serving_repair.py", "repair_authoritative_serving_generation_for_publication", 0): {
        "model_client": None,
        "reaches_promote_seam": False,
        "note": "sync_registration=False — the guard seam is never entered",
    },
}


def _scan_wiring_callsites() -> dict[tuple[str, str, int], dict[str, Any]]:
    """Parse every `src/sourcing_agent/*.py` and return each
    `build_company_candidate_artifacts` callsite keyed by
    (module file, enclosing qualname, ordinal within that qualname).

    Resolves the acquisition-style `**kwargs` callsite by looking for a dict
    literal with a "model_client" key inside the same enclosing function, so a
    deleted entry there is caught exactly like a deleted keyword argument.
    """

    src_dir = Path(candidate_artifacts_module.__file__).parent
    found: dict[tuple[str, str, int], dict[str, Any]] = {}
    for path in sorted(src_dir.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        stack: list[str] = []
        # enclosing FunctionDef node -> dict-literal "model_client" values
        scope_dict_values: dict[str, list[str]] = {}
        ordinals: dict[tuple[str, str], int] = {}

        def _dict_literal_model_client(node: ast.AST) -> str | None:
            for child in ast.walk(node):
                if not isinstance(child, ast.Dict):
                    continue
                for key, value in zip(child.keys, child.values):
                    if isinstance(key, ast.Constant) and key.value == "model_client":
                        return ast.unparse(value)
            return None

        class _Visitor(ast.NodeVisitor):
            def visit_FunctionDef(self, node: Any) -> None:
                stack.append(node.name)
                scope_dict_values[node.name] = [_dict_literal_model_client(node) or ""]
                self.generic_visit(node)
                stack.pop()

            visit_AsyncFunctionDef = visit_FunctionDef

            def visit_ClassDef(self, node: Any) -> None:
                stack.append(node.name)
                self.generic_visit(node)
                stack.pop()

            def visit_Call(self, node: Any) -> None:
                func = node.func
                name = getattr(func, "id", None) or getattr(func, "attr", None)
                if name == _WIRING_TARGET:
                    qual = "::".join(stack)
                    ordinal = ordinals.get((path.name, qual), 0)
                    ordinals[(path.name, qual)] = ordinal + 1
                    keyword = next((kw for kw in node.keywords if kw.arg == "model_client"), None)
                    sync = next((kw for kw in node.keywords if kw.arg == "sync_registration"), None)
                    if keyword is not None:
                        model_client = ast.unparse(keyword.value)
                    elif stack and any(kw.arg is None for kw in node.keywords):
                        # `**kwargs` expansion — resolve from the dict literal.
                        model_client = (scope_dict_values.get(stack[-1]) or [""])[0] or None
                    else:
                        model_client = None
                    found[(path.name, qual, ordinal)] = {
                        "model_client": model_client,
                        "sync_registration": ast.unparse(sync.value) if sync else None,
                        "line": node.lineno,
                    }
                self.generic_visit(node)

        _Visitor().visit(tree)
    return found


class ProductionCallerWiringRegistryRatchetTest(unittest.TestCase):
    """ANTI-INERTNESS RATCHET, source level: EVERY production
    `build_company_candidate_artifacts` callsite matches its declared wiring.

    Deleting `model_client=self.model_client` (or the `"model_client"` entry of
    acquisition's kwargs dict) at ANY of the eight caller wiring points fails
    this test — which is exactly the regression the previous count-based pin
    could not detect. Adding a NEW callsite, or silently wiring/unwiring a
    residual one, also fails until the registry (and design §7.1) is updated.
    """

    def test_no_unregistered_or_missing_callsite(self) -> None:
        scanned = set(_scan_wiring_callsites())
        declared = set(_PROMOTE_SHADOW_WIRING_REGISTRY)
        self.assertEqual(
            sorted(scanned - declared),
            [],
            "NEW build_company_candidate_artifacts callsite(s) with no promote-shadow wiring decision. "
            "Add them to _PROMOTE_SHADOW_WIRING_REGISTRY and to docs/WS7_AI_PROMOTE_DESIGN.md §7.1.",
        )
        self.assertEqual(
            sorted(declared - scanned),
            [],
            "a registered build_company_candidate_artifacts callsite disappeared — if it moved, update the "
            "registry key; if it was deleted, drop the row and the §7.1 residual line with it.",
        )

    def test_every_callsite_threads_exactly_what_it_declares(self) -> None:
        scanned = _scan_wiring_callsites()
        for key, expected in sorted(_PROMOTE_SHADOW_WIRING_REGISTRY.items()):
            with self.subTest(callsite=f"{key[0]}::{key[1]}#{key[2]}"):
                actual = scanned.get(key)
                self.assertIsNotNone(actual, f"{key} vanished")
                assert actual is not None
                self.assertEqual(
                    actual["model_client"],
                    expected["model_client"],
                    f"{key[0]}:{actual['line']} ({key[1]}) changed how it threads model_client into "
                    f"{_WIRING_TARGET}: declared {expected['model_client']!r}, found "
                    f"{actual['model_client']!r}. Threading it is what keeps the WS7/W7.3 S3 promote "
                    "shadow reachable in production; NOT threading it is a recorded residual. Either "
                    "way the registry and docs/WS7_AI_PROMOTE_DESIGN.md §7.1 must move with the code.",
                )

    def test_exempt_callsites_really_are_exempt(self) -> None:
        """The two `authoritative_serving_repair` callsites are exempt only
        because they pass `sync_registration=False`; if that ever changes they
        silently become unthreaded seam-reachers."""
        scanned = _scan_wiring_callsites()
        for key, expected in sorted(_PROMOTE_SHADOW_WIRING_REGISTRY.items()):
            if expected["reaches_promote_seam"]:
                continue
            with self.subTest(callsite=f"{key[0]}::{key[1]}#{key[2]}"):
                self.assertEqual((scanned.get(key) or {}).get("sync_registration"), "False")

    def test_at_least_the_known_caller_wiring_points_are_threaded(self) -> None:
        """A floor, so an update that relaxes every row to None cannot pass."""
        threaded = [
            key
            for key, expected in _PROMOTE_SHADOW_WIRING_REGISTRY.items()
            if expected["model_client"] == "self.model_client"
        ]
        self.assertGreaterEqual(len(threaded), 8, "the eight production caller wiring points must stay threaded")


class ProductionCallerThreadingPinTest(unittest.TestCase):
    """The entrypoint's own pass-through (wiring point #1), pinned at source level
    in addition to the functional ratchet above."""

    def test_build_company_candidate_artifacts_passes_model_client_to_the_registration_sync(self) -> None:
        source = Path(candidate_artifacts_module.__file__).read_text(encoding="utf-8")
        self.assertIn("    model_client: Any = None,\n", source)
        self.assertIn("            model_client=model_client,\n", source)


if __name__ == "__main__":
    unittest.main()
