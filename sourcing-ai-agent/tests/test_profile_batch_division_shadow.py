"""WS7/W7.2 S3 offline suite: SHADOW integration of the AI batch divider.

Provenance: docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §7 slice S3 (+ §5.2 shadow
ladder, §4.5 completion-path exclusion, discrepancy D1), OQ5/OQ6/OQ7 RATIFIED
2026-07-23. Pins `record_profile_prefetch_division_shadow`
(src/sourcing_agent/profile_batch_division.py) and its enrichment.py mint-seam
call: the shadow hook RECORDS a division proposal (or ruling-④ fallback audit)
on the `refill_plan_items` activity surface and NEVER drives dispatch — the
ladder-built `dispatch_item_specs` stay byte-identical with the shadow on or
off, the oracle-pinned plan record keeps `schema_version=1` untouched
(test_fetch_profile_batch_characterization.py whole-dict goldens; the additive
`ai_batch_division` plan-record key is the S5 flip's job), the completion fast
path (ruling ②) never invokes the hook, and any shadow exception is swallowed
into a `shadow_error` audit record.

Fixture patterns mirror the characterization suite (pure plan-builder inputs
under provider mode "simulate"); the scripted divider client rides the OQ7 env
gate `SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER`.
"""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

import sourcing_agent.enrichment as enrichment_module
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import Candidate
from sourcing_agent.enrichment import (
    MultiSourceEnricher,
    ProfilePrefetchBatchPlan,
    _build_profile_prefetch_batch_plan,
    _build_profile_prefetch_queue_items,
)
from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key
from sourcing_agent.model_provider import (
    WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL_ENV,
    DeterministicModelClient,
    OfflineModelClient,
    ScriptedProfileBatchDividerModelClient,
)
from sourcing_agent.profile_batch_division import (
    DURABLE_WAVE_BATCH_SIZE_REASON,
    SHADOW_RECORD_KIND,
    SHADOW_STATUS_ERROR,
    SKIP_REASON_BELOW_ENGAGEMENT_THRESHOLD,
    divider_shadow_model_calls_permitted,
    model_client_supports_batch_division,
    record_profile_prefetch_division_shadow,
)
from sourcing_agent.profile_batch_division_contract import (
    DIVISION_SOURCE_AI_DIVIDER,
    FALLBACK_REASON_MODEL_UNAVAILABLE,
    SCHEMA_ID_V1,
    VALIDATOR_RESULT_STATUS_PASS,
    VALIDATOR_V5_WORKER_BUDGET,
    VALIDATOR_V6_WAVE_MINT_ONLY,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin

_SIMULATE_ENV = {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}
_SCRIPTED_DIVIDER_ENV = {
    "SOURCING_EXTERNAL_PROVIDER_MODE": "simulate",
    "SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER": "1",
}


def _urls(prefix: str, count: int) -> list[str]:
    return [f"https://www.linkedin.com/in/{prefix}-{i:04d}/" for i in range(count)]


def _asset_catalog(root: Path) -> AssetCatalog:
    """Minimal on-disk catalog for the end-to-end seam ratchet (mirrors
    tests/test_profile_prefetch_scheduler_contract.py)."""
    return AssetCatalog(
        project_root=root,
        dev_root=root,
        anthropic_root=root,
        anthropic_workbook=root / "anthropic.xlsx",
        anthropic_readme=root / "README.md",
        anthropic_progress=root / "PROGRESS.md",
        legacy_api_accounts=root / "api_accounts.json",
        legacy_company_ids=root / "company_ids.json",
        anthropic_publications=root / "publications.json",
        scholar_scan_results=root / "scholar.json",
        investor_members_json=root / "investor.json",
        employee_scan_skill=root / "employee_skill.md",
        investor_scan_skill=root / "investor_skill.md",
        onepager_skill=root / "onepager_skill.md",
    )


class _StubHarvestProfileConnector:
    """Enabled-but-never-called connector: the ratchet runs with
    ``execute_profile_refill_submit_commands=False``, so no submit ever reaches
    it (ZERO provider calls)."""

    settings = type("_Settings", (), {"enabled": True})()


def _budget(available: int, *, active: int = 0, reserved: int = 0, actor: int = 4, submit: int = 4) -> dict[str, int]:
    return {
        "submit_budget": submit,
        "actor_budget": actor,
        "active_worker_count": active,
        "scheduler_reserved_worker_count": reserved,
        "effective_active_worker_count": max(active, reserved),
        "available_new_worker_count": available,
    }


def _plan(
    url_list: list[str],
    *,
    window: dict[str, Any] | None = None,
    wbudget: dict[str, int] | None = None,
    registry_entries: dict[str, dict[str, Any]] | None = None,
    allow_tail: bool = False,
) -> ProfilePrefetchBatchPlan:
    items = _build_profile_prefetch_queue_items(
        url_list,
        source_shards_by_url={},
        priority=False,
        queue_state="ready",
        registry_entries=registry_entries or {},
    )
    return _build_profile_prefetch_batch_plan(
        dispatch_urls=url_list,
        requested_url_count=len(url_list),
        candidate_count=len(url_list),
        priority=False,
        source_shards_by_url={},
        worker_budget=wbudget or _budget(4),
        dispatch_window=window,
        queue_items=items,
        allow_under_target_final_tail_dispatch=allow_tail,
    )


def _plan_dispatch_snapshot(plan: ProfilePrefetchBatchPlan) -> str:
    """Canonical JSON snapshot of everything dispatch consumes from the plan."""
    return json.dumps(
        {
            "dispatch_specs": plan.dispatch_specs,
            "dispatch_item_specs": [
                (index, [item.profile_url for item in chunk]) for index, chunk in plan.dispatch_item_specs
            ],
            "deferred_urls": plan.deferred_urls,
            "record": plan.to_record(),
        },
        sort_keys=True,
    )


class _SpyDividerClient(DeterministicModelClient):
    """Divider-capable spy: canned response + call counter (OQ5/OQ6 pins).

    DECLARED billing-free (a local object that never touches a network), so it
    stands in for the scripted divider under the 2026-07-25 safety gate.
    ``_RealCapableDividerClient`` below is the deliberately UNdeclared twin.
    """

    ws7_shadow_billing_free = True

    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.response = dict(response or {})
        self.divide_calls = 0
        self.payloads: list[dict[str, Any]] = []

    def divide_profile_prefetch_batches(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.divide_calls += 1
        self.payloads.append(payload)
        return dict(self.response)


class _RaisingDividerClient(DeterministicModelClient):
    ws7_shadow_billing_free = True

    def divide_profile_prefetch_batches(self, payload: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        raise RuntimeError("shadow divider client exploded")


class _RealCapableDividerClient(DeterministicModelClient):
    """Stands in for a REAL provider client at the mint seam: structurally
    divider-capable and NOT declared billing-free — the exact shape of
    ``OpenAICompatibleChatModelClient`` / ``QwenResponsesModelClient``. It does
    no network I/O; it only records that a (billable) call was attempted."""

    def __init__(self) -> None:
        self.divide_calls = 0

    def divide_profile_prefetch_batches(self, payload: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        self.divide_calls += 1
        return {}


class RealModelShadowSafetyGateTest(unittest.TestCase):
    """SAFETY GATE (2026-07-25) — capability is NOT permission (mirror of the
    promote seam's gate; see profile_batch_division.divider_shadow_model_calls_permitted).

    EXPOSURE, stated precisely: unlike the promote seam this hook is on the
    REFILL/mint path, not the authoritative write path — nothing it returns can
    change dispatch, and the ladder plan is byte-identical with it on or off.
    What an ungated real client would have cost is a BILLED provider call plus up
    to the 20 s divider timeout per wave mint >300 eligible urls, taken while the
    scheduler lock is held. Cost/latency on a scheduling hot spot rather than a
    correctness risk to authority — gated identically anyway.
    """

    def setUp(self) -> None:
        patcher = mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_real_capable_client_without_opt_in_makes_no_model_call(self) -> None:
        client = _RealCapableDividerClient()
        self.assertTrue(
            model_client_supports_batch_division(client),
            "the probe must still consider it CAPABLE — the gate is permission, not capability",
        )
        with mock.patch.dict("os.environ", {WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL_ENV: ""}, clear=False):
            self.assertFalse(divider_shadow_model_calls_permitted(client))
            record = record_profile_prefetch_division_shadow(
                client, plan=_plan(_urls("gate", 600)), wave_mint_provider_submit=True
            )
        self.assertIsNone(record, "an ungated real-capable client must produce NO shadow record")
        self.assertEqual(client.divide_calls, 0, "a BILLED model call would have been made at the mint seam")

    def test_no_live_provider_gate_var_implies_the_opt_in(self) -> None:
        client = _RealCapableDividerClient()
        live_ish = {
            "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
            "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
            "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
            WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL_ENV: "",
        }
        with mock.patch.dict("os.environ", live_ish, clear=False):
            self.assertFalse(divider_shadow_model_calls_permitted(client))
            self.assertIsNone(
                record_profile_prefetch_division_shadow(
                    client, plan=_plan(_urls("gate_live", 600)), wave_mint_provider_submit=True
                )
            )
        self.assertEqual(client.divide_calls, 0)

    def test_explicit_opt_in_re_enables_the_real_capable_client(self) -> None:
        client = _RealCapableDividerClient()
        with mock.patch.dict("os.environ", {WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL_ENV: "1"}, clear=False):
            self.assertTrue(divider_shadow_model_calls_permitted(client))
            record = record_profile_prefetch_division_shadow(
                client, plan=_plan(_urls("gate_on", 600)), wave_mint_provider_submit=True
            )
        assert record is not None
        self.assertEqual(client.divide_calls, 1)
        # It returned {} → the ruling-④ F1 audit, i.e. the call really happened.
        self.assertEqual(record["fallback_audit"]["fallback_reason"], FALLBACK_REASON_MODEL_UNAVAILABLE)

    def test_scripted_divider_needs_no_opt_in(self) -> None:
        with mock.patch.dict("os.environ", {WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL_ENV: ""}, clear=False):
            client = ScriptedProfileBatchDividerModelClient(mode="simulate")
            self.assertTrue(divider_shadow_model_calls_permitted(client))
            record = record_profile_prefetch_division_shadow(
                client, plan=_plan(_urls("gate_scripted", 600)), wave_mint_provider_submit=True
            )
        assert record is not None
        self.assertTrue(record["engaged"])


class ShadowEngagementTest(unittest.TestCase):
    """>300 eligible + scripted/live client → one recorded shadow proposal."""

    def setUp(self) -> None:
        patcher = mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_shadow_record_proposed_for_large_wave_with_scripted_divider(self) -> None:
        plan = _plan(_urls("shadow", 600))
        record = record_profile_prefetch_division_shadow(
            ScriptedProfileBatchDividerModelClient(mode="simulate"),
            plan=plan,
            registry_entries={},
            runtime_tuning_context={},
            wave_mint_provider_submit=True,
        )
        assert record is not None
        self.assertEqual(record["kind"], SHADOW_RECORD_KIND)
        self.assertEqual(record["mode"], "shadow")
        self.assertEqual(record["shadow_status"], "proposed")
        self.assertTrue(record["engaged"])
        self.assertEqual(record["skip_reason"], "")
        self.assertEqual(record["eligible_member_count"], 600)
        self.assertIsNone(record["fallback_audit"])
        division = record["division"]
        self.assertEqual(division["schema_id"], SCHEMA_ID_V1)
        self.assertEqual(division["division_source"], DIVISION_SOURCE_AI_DIVIDER)
        # Scripted divider: clamp(ceil(600/300), 4, 8) = 4 near-equal batches.
        self.assertEqual(division["batch_count"], 4)
        self.assertEqual([batch["member_count"] for batch in division["batches"]], [150, 150, 150, 150])
        self.assertTrue(division["validator_results"])
        # V5/V6 apply-time wiring at the seam (S1 left them `skipped`).
        apply_time = {entry["validator"]: entry for entry in record["apply_time_validator_results"]}
        self.assertEqual(apply_time[VALIDATOR_V5_WORKER_BUDGET]["status"], VALIDATOR_RESULT_STATUS_PASS)
        self.assertEqual(apply_time[VALIDATOR_V6_WAVE_MINT_ONLY]["status"], VALIDATOR_RESULT_STATUS_PASS)
        # S4 landed the refill_plan_division_id writer (2026-07-23): the empty
        # live set now means "no live divided wave", not "no writer exists".
        self.assertIn("no live durable wave carries a division id", apply_time[VALIDATOR_V6_WAVE_MINT_ONLY]["reason"])
        # Divergence digest vs the ladder's ACTUAL division.
        comparison = record["ladder_comparison"]
        self.assertEqual(comparison["ladder_dispatched_batch_count"], len(plan.dispatch_item_specs))
        self.assertEqual(
            comparison["ladder_dispatched_batch_sizes"],
            [len(chunk) for _, chunk in plan.dispatch_item_specs],
        )
        self.assertEqual(comparison["ladder_deferred_item_count"], len(plan.deferred_items))
        self.assertEqual(comparison["ai_batch_count"], 4)
        self.assertEqual(comparison["batch_count_delta"], 4 - len(plan.dispatch_item_specs))
        self.assertEqual(len(comparison["ladder_dispatched_membership_sha256"]), 64)
        self.assertEqual(division["membership_sha256"], comparison["ai_membership_sha256"])
        self.assertIsInstance(comparison["dispatched_membership_identical"], bool)

    def test_scripted_client_without_env_optin_records_f1_fallback_audit(self) -> None:
        # OQ7 belt-and-braces: the scripted client re-checks its env per call, so
        # with the opt-in absent the call returns {} — the structural F1 marker —
        # and the shadow record carries the ruling-④ fallback audit.
        with mock.patch.dict("os.environ", {"SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER": ""}, clear=False):
            record = record_profile_prefetch_division_shadow(
                ScriptedProfileBatchDividerModelClient(mode="simulate"),
                plan=_plan(_urls("f1", 400)),
                wave_mint_provider_submit=True,
            )
        assert record is not None
        self.assertEqual(record["shadow_status"], "fallback")
        self.assertTrue(record["engaged"])
        self.assertIsNone(record["division"])
        self.assertEqual(record["fallback_audit"]["fallback_reason"], FALLBACK_REASON_MODEL_UNAVAILABLE)
        # Fallback records still carry the ladder-side divergence digest.
        self.assertIsNone(record["ladder_comparison"]["ai_batch_count"])
        self.assertEqual(record["apply_time_validator_results"], [])

    def test_at_or_below_threshold_is_a_structural_skip_without_model_call(self) -> None:
        spy = _SpyDividerClient()
        record = record_profile_prefetch_division_shadow(
            spy,
            plan=_plan(_urls("threshold", 300)),
            wave_mint_provider_submit=True,
        )
        assert record is not None
        self.assertEqual(record["shadow_status"], "fallback")
        self.assertFalse(record["engaged"])
        self.assertEqual(record["skip_reason"], SKIP_REASON_BELOW_ENGAGEMENT_THRESHOLD)
        self.assertIsNone(record["division"])
        self.assertIsNone(record["fallback_audit"])
        self.assertEqual(spy.divide_calls, 0)

    def test_registry_failure_history_and_budget_flow_into_the_divider_payload(self) -> None:
        # OQ1 full-input ruling at the seam: attempt/failure state from the
        # registry entries reaches the model payload (aggregate, no URL echo).
        urls = _urls("history", 400)
        first_key = normalize_linkedin_profile_url_key(urls[0])
        registry_entries = {
            first_key: {
                "refill_queue_state": "ready",
                "last_refill_attempt_count": 3,
                "last_refill_deferred_reason": "worker_budget_deferred",
            }
        }
        spy = _SpyDividerClient()
        record = record_profile_prefetch_division_shadow(
            spy,
            plan=_plan(urls, registry_entries=registry_entries),
            registry_entries=registry_entries,
            wave_mint_provider_submit=True,
        )
        assert record is not None
        self.assertEqual(spy.divide_calls, 1)
        payload = spy.payloads[0]
        self.assertEqual(payload["inventory"]["size"], 400)
        self.assertEqual(payload["failure_history"]["attempted_item_count"], 1)
        self.assertEqual(payload["failure_history"]["max_attempt_count"], 3)
        self.assertEqual(payload["failure_history"]["failure_class_counts"], {"worker_budget_deferred": 1})
        self.assertEqual(payload["budget"]["actor_global_inflight"], 4)
        self.assertEqual(record["actor_global_inflight"], 4)


class ShadowNonInvocationTest(unittest.TestCase):
    """Structural non-invocation: no record, no model call (OQ6/§4.5/V8/D2)."""

    def setUp(self) -> None:
        patcher = mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_non_divider_capable_clients_never_engage(self) -> None:
        plan = _plan(_urls("capability", 600))
        for client in (None, DeterministicModelClient(), OfflineModelClient(mode="simulate")):
            with self.subTest(client=type(client).__name__ if client else "none"):
                self.assertFalse(model_client_supports_batch_division(client))
                self.assertIsNone(
                    record_profile_prefetch_division_shadow(client, plan=plan, wave_mint_provider_submit=True)
                )
        self.assertTrue(model_client_supports_batch_division(ScriptedProfileBatchDividerModelClient(mode="simulate")))
        self.assertTrue(model_client_supports_batch_division(_SpyDividerClient()))

    def test_completion_fast_path_never_invokes_the_shadow_hook(self) -> None:
        # Ruling ② + OQ6: deferred-submit completion callbacks are signal-only;
        # the divider runs once per wave mint (submit_provider=True path).
        spy = _SpyDividerClient()
        self.assertIsNone(
            record_profile_prefetch_division_shadow(
                spy,
                plan=_plan(_urls("completion", 600)),
                wave_mint_provider_submit=False,
            )
        )
        self.assertEqual(spy.divide_calls, 0)

    def test_retry_isolated_wave_never_invokes_the_shadow_hook(self) -> None:
        # V8/§4.2: the retry wave is never AI-divided.
        urls = _urls("retry", 7)
        registry = {
            normalize_linkedin_profile_url_key(url): {"status": "queued", "refill_queue_state": "retry_wait"}
            for url in urls
        }
        plan = _plan(urls, registry_entries=registry)
        self.assertEqual(plan.plan_reason, "retry_wait_isolated_dispatch")
        spy = _SpyDividerClient()
        self.assertIsNone(record_profile_prefetch_division_shadow(spy, plan=plan, wave_mint_provider_submit=True))
        self.assertEqual(spy.divide_calls, 0)

    def test_durable_wave_inherited_window_never_invokes_the_shadow_hook(self) -> None:
        # §4.5: an R6-claimed window belongs to an in-flight wave — no re-division.
        plan = _plan(
            _urls("durable", 600),
            window={
                "batch_size": 300,
                "max_workers": 2,
                "batch_count": 2,
                "source_mix": {"other": 600},
                "batch_size_reason": DURABLE_WAVE_BATCH_SIZE_REASON,
            },
        )
        spy = _SpyDividerClient()
        self.assertIsNone(record_profile_prefetch_division_shadow(spy, plan=plan, wave_mint_provider_submit=True))
        self.assertEqual(spy.divide_calls, 0)

    def test_empty_plan_never_invokes_the_shadow_hook(self) -> None:
        spy = _SpyDividerClient()
        self.assertIsNone(record_profile_prefetch_division_shadow(spy, plan=_plan([]), wave_mint_provider_submit=True))
        self.assertEqual(spy.divide_calls, 0)


class ShadowDispatchByteIdentityTest(unittest.TestCase):
    """THE S3 regression: dispatch output is byte-identical with shadow on/off."""

    def test_dispatch_output_byte_identical_with_shadow_on_vs_off(self) -> None:
        urls = _urls("identical", 600)
        with mock.patch.dict("os.environ", _SIMULATE_ENV, clear=False):
            plan_shadow_off = _plan(urls)
            snapshot_off = _plan_dispatch_snapshot(plan_shadow_off)
        with mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False):
            plan_shadow_on = _plan(urls)
            before_hook = _plan_dispatch_snapshot(plan_shadow_on)
            record = record_profile_prefetch_division_shadow(
                ScriptedProfileBatchDividerModelClient(mode="simulate"),
                plan=plan_shadow_on,
                wave_mint_provider_submit=True,
            )
            after_hook = _plan_dispatch_snapshot(plan_shadow_on)
        assert record is not None
        self.assertEqual(record["shadow_status"], "proposed")
        # The hook mutated nothing on the plan it observed...
        self.assertEqual(before_hook, after_hook)
        # ...and the ladder output is byte-identical to a shadow-off build,
        # including the oracle-pinned plan record (schema_version stays 1;
        # the additive `ai_batch_division` key is the S5 flip's job).
        self.assertEqual(snapshot_off, after_hook)
        self.assertEqual(plan_shadow_on.to_record()["schema_version"], 1)
        self.assertNotIn("ai_batch_division", plan_shadow_on.to_record())

    def test_shadow_record_is_a_plain_payload_detached_from_the_plan(self) -> None:
        # Structural isolation: deep-copying / serializing the record touches no
        # plan internals — dispatch could never observe it even if it tried.
        with mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False):
            plan = _plan(_urls("detached", 600))
            record = record_profile_prefetch_division_shadow(
                ScriptedProfileBatchDividerModelClient(mode="simulate"),
                plan=plan,
                wave_mint_provider_submit=True,
            )
        assert record is not None
        clone = copy.deepcopy(record)
        clone["division"]["batches"][0]["member_index_ranges"][0][0] = 999999
        self.assertEqual(json.dumps(plan.dispatch_specs), json.dumps(_plan(_urls("detached", 600)).dispatch_specs))
        json.dumps(record, sort_keys=True)  # JSON-serializable activity payload


class ShadowExceptionIsolationTest(unittest.TestCase):
    """Any shadow exception → audit record, dispatch untouched (S3 hard rule)."""

    def test_helper_exception_becomes_shadow_error_audit_record(self) -> None:
        with mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False):
            plan = _plan(_urls("boom", 600))
            before = _plan_dispatch_snapshot(plan)
            with mock.patch(
                "sourcing_agent.profile_batch_division.propose_and_validate_division",
                side_effect=RuntimeError("synthetic shadow failure"),
            ):
                record = record_profile_prefetch_division_shadow(
                    _SpyDividerClient(),
                    plan=plan,
                    wave_mint_provider_submit=True,
                )
            after = _plan_dispatch_snapshot(plan)
        assert record is not None
        self.assertEqual(record["kind"], SHADOW_RECORD_KIND)
        self.assertEqual(record["shadow_status"], SHADOW_STATUS_ERROR)
        self.assertIn("synthetic shadow failure", record["shadow_error"])
        self.assertEqual(record["shadow_error_type"], "RuntimeError")
        self.assertEqual(before, after)

    def test_raising_model_client_becomes_shadow_error_audit_record(self) -> None:
        with mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False):
            plan = _plan(_urls("clientboom", 600))
            before = _plan_dispatch_snapshot(plan)
            record = record_profile_prefetch_division_shadow(
                _RaisingDividerClient(),
                plan=plan,
                wave_mint_provider_submit=True,
            )
            after = _plan_dispatch_snapshot(plan)
        assert record is not None
        self.assertEqual(record["shadow_status"], SHADOW_STATUS_ERROR)
        self.assertIn("shadow divider client exploded", record["shadow_error"])
        self.assertEqual(before, after)


class ShadowSeamStructuralPinTest(unittest.TestCase):
    """Source-level pins on the enrichment.py mint seam (meta-guard style)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.source = Path(enrichment_module.__file__).read_text(encoding="utf-8")

    def test_shadow_hook_is_called_exactly_once_from_the_mint_seam(self) -> None:
        # One import + one call — the hook has exactly one seat (OQ6: once per
        # wave mint), inside queue_background_profile_prefetch's lock block.
        self.assertEqual(self.source.count("record_profile_prefetch_division_shadow"), 2)
        self.assertEqual(self.source.count("record_profile_prefetch_division_shadow("), 1)
        # OQ6/ruling-② gate: the completion fast path (submit_provider=False)
        # is structurally excluded at the call site.
        self.assertIn("wave_mint_provider_submit=bool(submit_provider)", self.source)

    def test_shadow_record_is_write_only_on_the_activity_surface(self) -> None:
        # The key is assigned once onto refill_plan_items and NEVER read back:
        # dispatch has no way to consume the shadow division. Since S4 the seam
        # has exactly ONE sanctioned read of the shadow record — the
        # `shadow_plan_division_id` mint threading into the plan-record write
        # (design §4.3 wave identity); dispatch shapes still never consume it.
        self.assertEqual(self.source.count("ai_batch_division_shadow"), 1)
        self.assertEqual(self.source.count("shadow_division_record"), 4)
        self.assertEqual(self.source.count("shadow_plan_division_id(shadow_division_record)"), 1)
        self.assertEqual(self.source.count('refill_plan_items["ai_batch_division_shadow"] = shadow_division_record'), 1)

    def test_plan_record_gains_no_ai_batch_division_key_before_the_flip(self) -> None:
        # D1: the additive plan-record key + schema_version bump land ONLY at S5.
        self.assertNotIn('"ai_batch_division"', self.source)
        self.assertIn('"schema_version": 1,', self.source)


class EndToEndSeamEngagementRatchetTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    """ANTI-INERTNESS RATCHET (2026-07-25) — the divider shadow path must stay
    reachable through the PRODUCTION entrypoint, not just through a direct call
    to the hook.

    Every test above drives ``record_profile_prefetch_division_shadow`` directly,
    so all of them would stay green even if the enrichment mint seam stopped
    threading ``self.model_client`` (or if some new early-return were added ahead
    of it). This class closes that hole: it runs the real
    ``MultiSourceEnricher.queue_background_profile_prefetch`` against a real
    PG-backed store with a divider-capable client and asserts a shadow record
    actually lands on the returned ``refill_plan_items`` activity surface — i.e.
    the gate ladder A0 (connector/worker_runtime/job_id) → A1 (post-lock
    revalidation) → A2 (capability probe) → A3 (``wave_mint_provider_submit``) →
    A4-A7 all clear end to end.

    Zero provider calls: ``execute_profile_refill_submit_commands=False`` stops
    at the planned-command boundary, and the client is the deterministic scripted
    divider (pure arithmetic). The paired negative test pins the default-inert
    contract: the simulate-default ``OfflineModelClient`` produces NO record.
    """

    def _run_wave(self, model_client: Any, *, url_count: int, label: str) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(
                _asset_catalog(root),
                accounts=[],
                harvest_profile_connector=_StubHarvestProfileConnector(),
                model_client=model_client,
                store=store,
            )
            # A0: the seam early-returns "worker_prefetch_unavailable" without a
            # worker runtime; the sentinel is never called on this code path.
            enricher.worker_runtime = object()
            candidates = [
                Candidate(
                    candidate_id=f"{label}_{index:04d}",
                    name_en=f"Ratchet {index}",
                    display_name=f"Ratchet {index}",
                    linkedin_url=f"https://www.linkedin.com/in/{label}-{index:04d}/",
                )
                for index in range(url_count)
            ]
            return enricher.queue_background_profile_prefetch(
                candidates=candidates,
                snapshot_dir=root,
                job_id=f"job_{label}",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                priority=True,
                load_cached_profile_payloads=False,
                submit_provider=True,
                execute_profile_refill_submit_commands=False,
            )

    def test_divider_capable_client_produces_a_shadow_record_through_the_real_seam(self) -> None:
        with mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False):
            result = self._run_wave(
                ScriptedProfileBatchDividerModelClient(mode="simulate"),
                url_count=600,
                label="ratcheton",
            )
        refill_plan_items = dict(result.get("refill_plan_items") or {})
        # THE ratchet assertion: the production seam produced a shadow record.
        self.assertIn(
            "ai_batch_division_shadow",
            refill_plan_items,
            "WS7/W7.2 S3 divider shadow path went INERT at the production mint seam: "
            "queue_background_profile_prefetch no longer reaches "
            "record_profile_prefetch_division_shadow with a divider-capable client.",
        )
        record = dict(refill_plan_items["ai_batch_division_shadow"])
        self.assertEqual(record["kind"], SHADOW_RECORD_KIND)
        self.assertEqual(record["mode"], "shadow")
        self.assertTrue(record["engaged"])
        self.assertEqual(record["shadow_status"], "proposed")
        self.assertEqual(record["skip_reason"], "")
        self.assertGreater(int(record["eligible_member_count"]), 300)
        division = dict(record["division"] or {})
        self.assertEqual(division["schema_id"], SCHEMA_ID_V1)
        self.assertEqual(division["division_source"], DIVISION_SOURCE_AI_DIVIDER)
        self.assertGreaterEqual(int(division["batch_count"]), 4)
        # The divergence digest is populated against the ladder's REAL plan.
        self.assertGreater(int(dict(record["ladder_comparison"] or {})["ladder_dispatched_batch_count"]), 0)
        # Authority is untouched: the shadow key rides the activity surface only.
        self.assertNotIn("ai_batch_division", dict(result.get("batch_plan") or {}))

    def test_offline_default_client_stays_structurally_inert_through_the_real_seam(self) -> None:
        # The default-inert contract: no opt-in env, simulate-default client →
        # the capability probe (A2) is False → no record, no model call.
        with mock.patch.dict("os.environ", _SIMULATE_ENV, clear=False):
            result = self._run_wave(OfflineModelClient(mode="simulate"), url_count=600, label="ratchetoff")
        self.assertNotIn("ai_batch_division_shadow", dict(result.get("refill_plan_items") or {}))


if __name__ == "__main__":
    unittest.main()
