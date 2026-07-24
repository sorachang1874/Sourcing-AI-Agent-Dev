# WS7/W7.4 — Pipeline Compensation / Recovery Mechanism Design (operator directive #4)

> Status: DESIGN DRAFT 2026-07-24 — **awaiting operator rulings on OQ1–OQ8 (§8) + independent review gate**. No implementation slice lands before the ruling batch is answered and the review verdict for the contract slices is recorded (contract-heavy + durable-runtime-touching → §9 protocol). Authority: operator directive #4 RATIFIED 2026-07-22 ([REFACTOR_MASTER_PLAN.md](REFACTOR_MASTER_PLAN.md) §6.5, verbatim: "原子化后的完整补偿机制：各环节充分抽象与原子化之后，建立完整的重试/recovery 机制——补充 acquire、补充 fetch、补充 materialize、补充 promote、对执行失败的记录 recovery"); factual base: [WS7_STRONG_AGENT_RECON_2026-07-22.md](WS7_STRONG_AGENT_RECON_2026-07-22.md) §1.3/§2/§5-Q17. Design shape mirrors the proven [WS7_AI_BATCH_DIVIDER_DESIGN.md](WS7_AI_BATCH_DIVIDER_DESIGN.md) (议案①) and [WS7_AI_PROMOTE_DESIGN.md](WS7_AI_PROMOTE_DESIGN.md) (议案③).

```
status: design-draft    owner: operator (rulings pending)
canonical-path: sourcing-ai-agent/docs/WS7_COMPENSATION_DESIGN.md
drafted: 2026-07-24     oracle-pin: tests/test_recovery_tick_characterization.py (EXISTS — the tick oracle is the floor; §6)
```

Unprefixed `file:line` anchors refer to `sourcing-ai-agent/src/sourcing_agent/`; test anchors to `sourcing-ai-agent/tests/`. Anchors verified on this tree (branch `governance-phase0-ttl-20260611`).

---

## 0. Ratified constraint + the thing that makes this proposal DIFFERENT

Directive #4 is a **durable retry/recovery MECHANISM**, not an AI-native decision. Unlike 议案① (AI divider, an efficiency judgment) and 议案③ (AI promote, an asset-correctness judgment), compensation adds **no `ModelClient` Protocol method, no model call, no `sourcing.*.decision` schema authored by a model.** It is the plumbing that, when a pipeline stage's execution is incomplete, re-runs *exactly the missing sub-unit* (补充 acquire / 补充 fetch / 补充 materialize / 补充 promote) and records the failure for recovery.

**The load-bearing finding (read this before anything else): the compensation substrate already largely exists as the worker recovery daemon.** `run_worker_recovery_once` (orchestrator.py:40217; tick body :38028–40063) already drains a per-stage typed-command-owner registry every ≤5 s poll tick (service_daemon.py:832/:913). The recovery tick's characterized phase sequence (test_recovery_tick_characterization.py:66–143) **already contains a typed owner for every acquire sub-unit** and for fetch/materialize:

| directive #4 sub-unit | existing recovery-tick phase(s) that already re-run it | owner label |
|---|---|---|
| **补 acquire** | `acquisition_run_create_command_owner`, `acquisition_intent_resolve_command_owner`, `acquisition_plan_build_command_owner`, `acquisition_plan_review_request_command_owner`, `acquisition_plan_commit_command_owner`, `acquisition_probe_command_owner`, `acquisition_scale_plan_command_owner`, `operation_native_discovery_activity_owner` | `acquisition_*` / `linkedin_acquisition_owner` |
| **补 fetch** | `pre_worker_profile_prefetch_refill`, `profile_prefetch_refill`, `post_event_level_profile_prefetch_refill`, `post_followup_profile_prefetch_refill`, `profile_refill_command_owner`, `profile_url_terminal_record_command_owner`, `operation_native_profile_fetch_activity_owner` | `profile_refill_daemon` / `linkedin_profile_*_owner` |
| **补 materialize** | `event_level_materialization_followup`, `profile_refill_event_level_materialization_followup`, `local_apply_backlog`, `board_visible_apply`, `snapshot_full_materialization`, `collection_authoritative_merge`, `legacy_materialization_adapter` | `event_level_local_apply_*` / `snapshot_full_materialization_queue` / `collection_writer_owner` |
| **补 promote** | **— NO recovery-tick owner exists —** promote runs inline in the registration flow (`upsert_organization_asset_registry_with_guard`, asset_reuse_planning.py:1541), NOT drained by the tick | (none — see §1.4 / OQ6, the honest gap) |

Therefore directive #4 is **not** "build a new recovery system." It is: (a) formalize a single **compensation contract** — a typed gap record + idempotent compensation action — that names, for any stage, the detected gap and the missing sub-unit; (b) add ONE **completeness-audit** seam that turns the stages' *already-emitted* completeness signals into compensation intents; (c) route each intent to the stage's **existing** idempotent command owner. The whole proposal's discipline is: **compose with the recovery daemon; never build a second loop that races it** (task hard rule; and R-019 forbids a parallel dispatch path outright, §3.3).

**Atomization dependency, stated honestly (task hard rule):** directive #4's premise is "各环节充分抽象与原子化之后". Status of that premise per stage:
- **acquire — atomized.** Seven typed durable command owners with idempotency keys + already-succeeded short-circuits (§1.1). Ready.
- **fetch — atomized.** The `retry_wait` state machine + `_profile_refill_retry_gate` IS the 补 fetch mechanism today (§1.2). Ready — but its *sub-unit shape* (batch membership) is mid-flip under 议案① (divider at S4-shadow, flip S5 not landed). Compensation must key on the divider's `refill_plan_division_id` (migration 0015, already landed) so it survives the flip (§1.2, D-C1).
- **materialize — atomized.** Per-shard completion honesty (`resolve_segmented_roster_completion`, company_shard_planning.py:341) + snapshot/board recovery phases (§1.3). Ready.
- **promote — NOT yet atomized into a compensable sub-unit.** It is an inline registration decision, not a recovery-drained command owner, and its judgment shape is mid-flip under 议案③ (AI promote at S3-shadow, flip S5 not landed). **补 promote is therefore BLOCKED** on directive #3's S5 + a promote command-owner seat; until then the existing keep-incumbent fail-closed IS the compensation (§1.4, OQ6). Recorded, not papered over.

---

## 1. The atomized stages + their completeness contracts (from code)

For each stage: what "incomplete" means, how it is detected TODAY, and what a 补充 (top-up) re-run of the missing sub-unit looks like.

### 1.1 acquire — the acquisition command spine + shard-registry completeness

**Sub-units & idempotency.** The acquisition pipeline is already decomposed into typed durable commands, each with a deterministic idempotency key and an `already_succeeded` short-circuit:
- `acquisition_command_owner.py`: `run.create` (idempotency root :427), `intent.resolve` (key `…:parent:{parent_command_id}` :715), `plan.build` (:784), `plan.review.request` (key `…:plan:{plan_id}:parent:{parent_command_id}` :855), `plan.commit` (key `…:review:{review_id}` :1869).
- Re-run safety is built in: a replay returns `acquisition_run_create_command_already_succeeded` (:473), `acquisition_intent_resolve_command_already_succeeded` (:998), `acquisition_plan_build_command_already_succeeded` (:1366), `acquisition_plan_review_request_command_already_succeeded` (:1744) — **the compensation "never double-dispatch" property is already a per-owner invariant, not something this design invents.**

**What "incomplete" means / how detected.** The acquire coverage honesty contract is `resolve_segmented_roster_completion` (company_shard_planning.py:341–374): coverage is `completed` **only** when every expected shard is present AND no shard carries truncation evidence (`shard_summary_is_truncated`); a missing or truncated shard yields `completion_status="partial"` + `missing_shard_ids` + `truncated_shard_ids` (:365–373). Both the direct segmented fetch (acquisition.py:5435/:7551) and background reconcile (snapshot_materializer.py:332) route through it — so a *partial* acquire is already a first-class, self-reported signal. The per-shard evidence face is `build_acquisition_shard_registry_record` (asset_reuse_planning.py:1709; the W7.1 shard-recording first cut wrote the full request-filter face into `metadata.request_filters`).

**What 补 acquire looks like.** For each `missing_shard_id` / `truncated_shard_id`, re-dispatch **that shard only** through its existing `acquisition_*` command owner under the owner's existing idempotency key. Already-present shards are skipped by the `already_succeeded` short-circuit — **the top-up is delta-only by construction** (§5). No new dispatch code path; the compensation intent is a *pointer* to an existing owner + sub-unit id.

### 1.2 fetch — the profile-prefetch refill spine + `retry_wait` (this IS 补 fetch today)

**Sub-units.** The durable unit is a `linkedin_profile_registry` item (enrichment.py:1281). Refill queue states: normal `("deferred_budget","deferred_coalescing","dispatch_reserved","dispatch_claimed")` (:520–525), retry `("retry_wait",)` (:526), provider-owned `"planned_dispatch"` (:527).

**What "incomplete" means / how detected.** A URL whose worker summary reports `worker_status ∈ {backpressure, failed}` is written to `status="retry_wait"` (enrichment.py:3447); `failed_urls` are extracted at :3396–3416; per-URL terminal fetched/failed is recorded by the `linkedin.profile_url.terminal_record` command family (:2930–2995). The saturation/idle audit face (`refill_saturation` ∈ {no_ready_items, worker_budget_saturated, underfilled_with_deferred_items, tail_coalescing_wait, …}, `unfilled_available_slot_count`, enrichment.py:1253–1277) is the "gap" signal for slot-level incompleteness.

**What 补 fetch looks like.** It is the operator-mandated **retained mechanism** (directive #2: "保留…url 级失败记录 + 最后统一发起一次重试"): `_profile_refill_retry_gate` (enrichment.py:3900–4020, "Retry is a separate wave. It may start only after the normal wave is closed") releases a single isolated `retry_wait_isolated_dispatch` wave (:1537) after the normal wave closes, with `retry_isolation` as the legal tiny-batch reason (:1959). **补 fetch = this retry wave.** Compensation does NOT reimplement it; it records the retry_wait tail as a compensation intent and lets the existing gate own dispatch.
- **D-C1 (honest coupling to 议案①):** the fetch sub-unit's *membership shape* is being reworked by the AI divider (flip S5 pending). Compensation must key the fetch gap on the divider's already-landed `refill_plan_division_id` (registry field, migration `0015`, 议案① S4) + the per-URL registry item, NOT on a batch ordinal, so a top-up after the divider flip re-runs the right sub-unit. Until 议案① S5 lands, `refill_plan_division_id` is written only for validated shadow proposals — compensation reads it defensively (absent → fall back to per-URL item identity).

### 1.3 materialize — snapshot materialization + per-shard completion honesty

**Sub-units.** Materialization phases already drained by the tick: `event_level_materialization_followup` / `profile_refill_event_level_materialization_followup` (event → board-visible local apply), `local_apply_backlog`, `board_visible_apply`, `snapshot_full_materialization`, `collection_authoritative_merge`, plus the `legacy_materialization_adapter` bridge (test_recovery_tick_characterization.py:81/86/93 etc.).

**What "incomplete" means / how detected.** The snapshot manifest carries `completion_status` + `expected_shard_count`/`available_shard_count`/`available_shard_ids` written by the same `resolve_segmented_roster_completion` contract (snapshot_materializer.py:332–360); a truncated shard forces `partial` even without a durable expected-shard plan on disk (:355–358). Board-visible / full-materialization backlog is visible as un-drained items on the respective recovery phases.

**What 补 materialize looks like.** Re-run the specific materialization phase for the specific durable unit (one snapshot / one board-visible unit) — these phases are already idempotent "one-durable-unit-per-tick" drains (the projection串, orchestrator.py:39264–39597). Compensation records a `partial` manifest or an un-drained backlog item as an intent that points at the existing phase; the phase's own idempotency (consumed markers, e.g. `inline_incremental_ingest`) prevents double-apply.

### 1.4 promote — the guard + AI judgment (the NOT-yet-compensable stage)

**Current shape.** Single inline decision `evaluate_organization_asset_registry_promotion` (asset_reuse_planning.py:1217) inside `upsert_organization_asset_registry_with_guard` (:1518/:1541), backstopped by the fail-closed storage lineage guard (storage.py:8633–8695, two refusal shapes `stale_generation_sequence_replay` / `source_snapshot_coverage_regression`). Directive #3 (议案③) is replacing the completeness-threshold family with an AI judgment; it is at **S3 shadow (record-only), flip S5 not landed.**

**What "incomplete"/"failed" means.** A promote that the guard refuses lands the row **non-authoritative** and returns `authoritative_promotion_refused` (storage.py:8683–8695). Under 议案③ ruling ④, an AI-judge failure keeps the incumbent (F1–F6 → keep-incumbent, no promote). **A refused/kept promote is already fail-closed and already self-reported** — it is not an "incomplete execution" needing a top-up dispatch; it is a *correctness decision*.

**Why 补 promote is BLOCKED (honest scope cut).** Two reasons: (1) promote is not drained by the recovery tick — there is no `promote_command_owner` phase in the characterized sequence, so there is no idempotent seat to compensate onto (unlike acquire/fetch/materialize); (2) the promote sub-unit's decision shape is mid-flip under 议案③ S5. Building 补 promote now would overfit to a shape that is changing. **Scope: 补 promote is deferred to after 议案③ S5 + a promote command-owner seat; until then the existing keep-incumbent fail-closed IS the compensation, and "补 promote" means at most *re-evaluating a kept candidate on the next tick* once its evidence completes — which is already what the next registration attempt does.** OQ6 ratifies this cut.

---

## 2. The compensation contract

### 2.1 Schema — `sourcing.pipeline.compensation_intent.v1`

One record per detected gap. Versioned dotted-contract id (repo convention, mirrors `sourcing.profile_prefetch.ai_batch_division.v1` / `sourcing.organization_asset.ai_promote_decision.v1`). **This is a durable record, not a model output** — every field is caller-authored fact.

```jsonc
{
  "schema_id": "sourcing.pipeline.compensation_intent.v1",
  "intent_id": "<ULID>",                        // audit id
  "stage": "acquire",                           // "acquire" | "fetch" | "materialize"  (NOT "promote" pre-议案③-S5, §1.4)
  "sub_unit": {                                 // WHICH missing atom — the delta-only anchor
    "kind": "acquisition_shard",                // "acquisition_shard" | "profile_registry_item" | "snapshot_durable_unit"
    "sub_unit_id": "profile_search|former|…|Google Multimodal Researcher|…",  // shard_id / normalized url-key / snapshot id
    "source_lineage": {                         // the lineage this gap belongs to — never cross-lineage
      "operation_id": "…", "job_id": "…", "materialization_generation_key": "…",
      "refill_plan_division_id": "…"            // fetch only; 议案① migration 0015 (D-C1). Nullable.
    }
  },
  "gap": {
    "detected_by": "completeness_audit",        // "completeness_audit" (§3.2) — never a second reconciler
    "signal": "roster_partial",                 // "roster_partial" | "roster_truncated" | "retry_wait_tail" |
                                                //   "manifest_partial" | "board_visible_backlog"
    "evidence_ref": {                           // pointer to the self-reported signal, NOT a copy
      "missing_shard_ids": ["…"], "truncated_shard_ids": ["…"],  // acquire/materialize
      "retry_wait_url_count": 0                  // fetch
    }
  },
  "compensation_action": {
    "target_command_owner": "acquisition_probe_command_owner",  // an EXISTING tick phase owner (§0 table) — never a new one
    "owner_idempotency_key": "acquisition.plan.commit:review:1234",  // the owner's OWN key — the never-double-pay fence (§5)
    "delta_only": true                          // HARD: only the named missing sub-unit is dispatched
  },
  "attempt": {                                  // recovery-of-recovery (§4)
    "count": 0, "max": 3, "backoff_owner": "recovery_tick_poll",
    "terminal_status": null                     // "compensated" | "compensation_exhausted_needs_human" | "superseded"
  },
  "provenance": { "created_at": "…", "created_by_phase": "compensation_completeness_audit", "input_snapshot_sha256": "…" }
}
```

Key design rules:
1. **The compensation intent NEVER carries a paid payload.** It carries a *pointer* to an existing command owner + that owner's own idempotency key. Re-dispatch is the owner's job, under the owner's fence. The intent cannot itself pay.
2. **`sub_unit_id` + `source_lineage` = the delta anchor.** A top-up re-runs exactly one atom of one lineage; it can never widen scope or cross a lineage boundary (mirrors the promote guard's strict-subset refusal, storage.py:8676–8681).
3. **`owner_idempotency_key` is the never-double-pay guarantee.** It is the *same* key the stage owner already uses (§1.1 keys / `terminal_record` / snapshot generation key), so a replay hits the owner's existing `already_succeeded` short-circuit and pays nothing (§5).

### 2.2 Who creates, who consumes

- **Created by** a single **completeness-audit** step that reads the stages' *already-emitted* self-reported signals (`completion_status="partial"`, `retry_wait` items, `refill_saturation`, `manifest partial`). It does not compute correctness afresh; it composes existing honest signals into intents. This is the OQ2 answer: **owner self-report, composed by one audit — not a rival reconciler.**
- **Consumed by** the existing per-stage command-owner recovery phases (§0 table). The audit does not dispatch; it enqueues an intent that the *next* tick's relevant owner phase drains under its own fence.

### 2.3 Placement (durable record location)

The compensation intent is a durable row keyed by `(stage, sub_unit_id, source_lineage)`. It rides the existing durable-command substrate — it is itself a lightweight typed command targeting an existing owner, NOT a new table with a new dispatch runtime (OQ1). Its persistence follows the `DurableRuntimeWriter` event→commands→outbox→state discipline (durable_runtime.py) so the audit's write and the owner's drain share one causal spine — **critical, because a compensation intent living outside that spine is exactly the R-019 last-writer-wins race (§3.3).**

---

## 3. Composition with the EXISTING recovery daemon (the seam, honestly)

### 3.1 Does compensation live inside `run_worker_recovery_once` / `recovery_phases`, or as a sibling?

**Inside — as additive recovery phases, never a sibling loop.** The recovery tick is already the daemon that re-runs incomplete work; `recovery_phases.py` already models self-contained phases as `RecoveryPhase` objects driven by `build_recovery_phase_registry` (:164) with a `TickContext` (:119). Compensation adds:
- **one new phase** `compensation_completeness_audit` (the §2.2 producer), inserted at a pinned position in the ordered sequence;
- **zero new dispatch phases** — the 补 acquire/fetch/materialize *executors* are the command-owner phases that already exist in the sequence (§0 table). The audit phase only *enqueues intents*; existing owner phases *drain* them on subsequent ticks.

A sibling daemon is rejected outright: it would double-drive the same command owners, racing the tick's per-phase budget (`phase_budget_exhausted`, service_daemon.py:160–166) and the R-019 dispatch atomicity gap (§3.3).

### 3.2 The seam is a phase, and its guard is a pure predicate over settled state

`recovery_phases.py` deliberately models only phases "whose gating is a pure predicate over already-settled context state" and that "do NOT need a non-phase threading statement to run between them and the next phase" (module docstring, PARTIAL ADOPTION note). The completeness-audit phase fits this exactly: its `wants_to_run` is a pure predicate (are there un-consumed partial/retry_wait/backlog signals in `ctx`?), its owner is constant (`compensation_completeness_audit`), and it writes intents into a durable queue — it threads nothing into the *next* phase. This is why it can be a first-class `RecoveryPhase` and not an inline cascade cluster.

### 3.3 R-019 is a HARD composition constraint (the reason this is a mechanism, not a rewrite)

**R-019 (workflow_runtime operation state-sync / dispatch atomicity, RESIDUAL_LEDGER.md:38, status `pending remediation`)** states: 24 production `update_action_state`/`update_operation_state` calls are not yet unified into an in-lock JSON-merge + operation+action+event/command UoW; command-plan can leave an executable command under a cancel race; `DurableRuntimeWriter` still commits event→commands→outbox→state in multiple steps and concurrent reducers can last-writer-wins the counts. Its remediation clause is explicit: **"下一次触碰 operation retry/dispatch/command completion、workflow commands 分子批、任何新增 `_connect_with_transaction_lock` caller … 之前必须修；调用点不得新增（当前 24 为上限）"**.

Compensation's 补 acquire/fetch *are* operation retry/dispatch/command-completion touches. Therefore:
- **HARD RULE (OQ5):** compensation MUST ride **only** the existing idempotent command-owner replays. It adds **no new dispatch path, no new `update_*_state` call site, no new `_connect_with_transaction_lock` caller.** The audit phase writes an intent (a typed command targeting an existing owner) and the existing owner drains it through its *existing* UoW. This keeps the R-019 call-site count at 24 and the blast radius zero.
- Any compensation design that *mints a new paid dispatch path* for 补 acquire/fetch is **blocked by R-019** until R-019 is remediated. This is recorded as the governing dependency, not a footnote.
- Cite: the recovery-band ownership forensics (`docs/governance/RECOVERY_BAND_OWNERSHIP_2026-07-22.md`) already established that resume/readiness, completion-event pipeline, and queue-dispatch strategy have live modern homes — compensation extends those homes, it does not resurrect a retired parallel path.

### 3.4 Discrepancies recorded (assumptions the code refines)

- **D-C1** — fetch sub-unit membership is mid-flip under 议案① (§1.2). Compensation keys on `refill_plan_division_id` (migration 0015, landed) + per-URL item, defensively (absent → per-URL identity).
- **D-C2** — promote has no recovery-tick owner phase (§1.4). 补 promote is scoped OUT until 议案③ S5. The §0 table's "补 promote" row is intentionally empty.
- **D-C3** — the recon Q17 phrasing "各自成为 typed durable command" reads as *new* commands; the code shows the acquire commands already exist and are idempotent. Following the code, compensation REUSES them (OQ1) rather than minting parallel `补_acquire`-typed commands, which would double the owner surface and re-open R-019.
- **D-C4** — the tick oracle pins a byte-identical phase sequence (§6). Inserting the audit phase is an *additive* oracle extension (mirrors the group-3 "tick 让位梯 ×8 港进 characterization oracle" precedent, master plan B2), NOT an oracle weakening.

---

## 4. Failure / recovery-of-recovery semantics

What happens when a compensation action itself fails — reusing the established fail-closed discipline (议案③ ruling ④ direction: a failure never widens scope, never re-pays, escalates).

| Situation | Behavior |
|---|---|
| Compensation intent's owner replay returns `already_succeeded` | Intent terminalizes `compensated` (the gap was already closed by a concurrent tick — the intended idempotent outcome). |
| Owner replay fails transiently (backpressure/transport) | Bounded retry: `attempt.count += 1` on the next tick's audit pass, up to `attempt.max` (default 3, OQ7). Backoff owner = the tick poll cadence itself (≤5 s), never a busy loop. |
| `attempt.count` reaches `attempt.max` | Intent terminalizes `compensation_exhausted_needs_human` — a durable, board-visible needs-human record (reuse the existing `needs_human` / `blocked` terminal + board visibility, mirroring R-019's `awaiting_budget→pending`, `needs_human→applied` outcome map). **Never silently re-loops; never fails open.** |
| The underlying gap disappears (lineage superseded, snapshot promoted, URL terminalized elsewhere) | Intent terminalizes `superseded` on the next audit pass (the audit re-reads live signals; a closed gap yields no live signal → the intent is retired, not re-dispatched). |
| The audit phase itself raises | Caught + recorded (`shadow_status`-style isolation, mirroring 议案① S3 / 议案③ S3 exception isolation); the tick's other phases are untouched. The audit is best-effort and never blocks the tick. |

**Recovery-of-recovery is bounded and fail-closed:** a compensation intent is a *finite* durable object with a hard attempt ceiling that terminalizes to a human-visible state — it cannot become a self-perpetuating dispatch source. This is the same conservative posture as the paid-dispatch red line: when in doubt, stop and surface, never re-pay.

---

## 5. Paid-dispatch safety (NON-NEGOTIABLE)

补 acquire / 补 fetch re-dispatch paid HarvestAPI actor calls. The CLAUDE.md paid-dispatch rules are HARD constraints: "inventory local + remote history first, delta-only, never retry/resume a terminalized paid command." The design satisfies each **structurally**, not by convention:

1. **Never double-pay for already-acquired data (idempotency).** The compensation intent carries the stage owner's *own* idempotency key (§2.1). A top-up re-dispatch hits the owner's existing `already_succeeded` short-circuit (`acquisition_*_command_already_succeeded`, acquisition_command_owner.py:473/:998/:1366/:1744) or the per-URL `terminal_record` (already-fetched URLs are terminal, never re-fetched, enrichment.py:2930–2995). **The compensation layer has no payment authority of its own — payment can only happen through an owner that already refuses to re-pay.**
2. **Delta-only.** The `sub_unit_id` anchors the top-up to exactly one missing atom of one lineage (`missing_shard_ids` / `retry_wait` URL keys / one snapshot). Already-present shards and terminalized URLs produce no live gap signal, so the audit never enqueues an intent for them (§2.2). A partial roster tops up only its `missing_shard_ids`; a completed roster produces zero intents.
3. **Never retry/resume a terminalized paid command.** The audit reads *live* completeness signals each pass (§4 `superseded`); a terminalized command has no live gap, so no intent is minted. The intent's `owner_idempotency_key` targets the owner's replay, which is itself terminal-aware (议案①/R-019 terminal-record discipline). A terminalized command replay returns `already_succeeded`, not a re-dispatch.
4. **Inventory-first.** The completeness-audit *is* the inventory step: it reconciles expected-vs-available shards (`resolve_segmented_roster_completion`) and registry item states before any top-up. No blind re-dispatch.
5. **Live env unchanged.** The triple-gate (`SOURCING_EXTERNAL_PROVIDER_MODE=live` + confirm + isolated) and committed `scripts/live_*.py`-only rules are untouched; compensation runs in the same fail-closed provider posture as every other stage. Under simulate/replay the owner phases use offline clients, so the audit→owner path is exercisable offline without a billed call (§7).

---

## 6. Characterization prerequisite (does an oracle need pinning first?)

**Finding: the characterization floor ALREADY EXISTS — unlike 议案③'s promote (which had NO oracle and forced an S0).** `tests/test_recovery_tick_characterization.py` (the tick oracle, 10/10 byte-identical per master plan §7 / B2) pins the *whole* recovery tick: the exact ordered phase sequence + per-phase owner/status/skip-reason/`max_sync_work` observables (test file :66–143 `CHARACTERIZED_PHASE_SEQUENCE`, :242–257), the summary-key mapping, and the budget-exhaustion epilogue. This is precisely the surface a compensation phase perturbs.

Consequences:
1. **No new whole-tick oracle is needed.** The tick oracle is the hard floor. Inserting `compensation_completeness_audit` is an **additive** extension of `CHARACTERIZED_PHASE_SEQUENCE` (a new pinned row at a pinned position), exactly as the group-3 "tick 让位梯 ×8 港进 characterization oracle (加法扩 oracle 合法)" precedent (master plan B2). The oracle stays byte-identical for every existing phase; only the additive row is new (D-C4).
2. **The stage completeness contracts already have oracles:** `resolve_segmented_roster_completion` is pinned by `test_former_shard_merge_completion.py` / `test_request_scoped_roster_shards.py` / `test_strategy_contract_preflight.py`; the retry-wait spine by the fetch batch characterization + `_profile_refill_retry_gate` PG lane. Compensation *consumes* these; it does not re-pin them.
3. **What DOES need a new offline suite:** the compensation-contract module itself — the `sourcing.pipeline.compensation_intent.v1` schema (strict parser + key allowlist), the gap-signal → intent mapping (partial/retry_wait/backlog → intent), the attempt-ladder terminalization (§4), and the delta-only/idempotency-key invariants (§5). This is a pure-function offline suite, mirroring 议案① S1 / 议案③ S1.

**Answer: no NEW whole-tick characterization oracle is required (the tick oracle is the pre-existing floor); a compensation-contract offline suite is the only new pin.** This is a materially cheaper prerequisite than 议案③'s mandatory S0 — recorded as a genuine advantage of building on the already-characterized recovery substrate.

---

## 7. Simulate-first validation ladder (live deferred to quota + operator)

Every slice keeps the tick oracle byte-identical (additive only) until the single gated flip; compensation is additive throughout.

1. **S1 — compensation contract module.** `sourcing.pipeline.compensation_intent.v1` schema (strict fail-closed parser, key allowlist), gap-signal→intent pure mapping, attempt-ladder terminalization, delta-only + owner-idempotency-key invariants. Offline pure-function suite. Tick oracle untouched. ADDITIVE.
2. **S2 — completeness-audit read surface (no enqueue).** A pure reader that turns live stage signals (`resolve_segmented_roster_completion` results, `retry_wait` counts, `refill_saturation`, manifest `completion_status`) into *proposed* intents, recorded on an activity surface only (shadow — never enqueued, never dispatched). Divergence counter: "gaps the audit would compensate vs gaps already closed by the next natural tick" — free before/after evidence for whether compensation adds coverage over the daemon's existing drains. Tick oracle byte-identical.
3. **S3 — audit phase inserted (enqueue, still no new dispatch).** `compensation_completeness_audit` added as a `RecoveryPhase` at a pinned position; it enqueues intents into the durable queue. Existing owner phases drain them under their existing fences (zero new dispatch code). Tick oracle **additively extended** (one new pinned row per D-C4/§6). This is the "flip" — gated by review GO (durable-runtime + recovery seam = trigger 3).
4. **Simulate e2e** — full-chain smoke under simulate: inject a partial roster (missing shard) + a retry_wait tail + a partial manifest; assert the audit mints the right intents, the existing owners drain them delta-only, terminalization is correct (compensated / exhausted-needs-human / superseded), and **no double-dispatch** occurs (assert owner `already_succeeded` short-circuits fire). Both R-019-safe (no new call site) and paid-safe (offline clients, no billed call).
5. **Live validation — explicitly deferred** to HarvestAPI quota restoration + explicit operator go **AND R-019 remediation** (§3.3): a live 补 acquire/fetch is an operation-dispatch touch, which R-019 gates. Red lines unchanged (triple-gate env, committed `scripts/live_*.py` only, delta-only).

---

## 8. Operator questions (the AskUserQuestion batch)

Mapping against the recon's E-section (recon §5): directive #4's compensation question is **recon Q17**; recon Q14 (idempotent execution marker placement) and Q18 (characterization order) also touch this scope. Each restated as a decidable question with a recommendation.

- **OQ1 (= recon Q17, first half) — compensation atomic unit + command family.** Do 补 acquire/fetch/materialize become **NEW** typed durable commands, or does compensation **reuse the existing per-stage command owners** already drained by the recovery tick (§0 table: 7 `acquisition_*` owners + profile_refill/`retry_wait` + snapshot/board phases), routing a thin gap-intent to them?
  **Recommended**: **reuse** (compensation = a thin gap→existing-owner router). The acquire owners already have idempotency keys + `already_succeeded` short-circuits (§1.1); fetch 补 = the existing `retry_wait` gate (§1.2); materialize 补 = existing snapshot/board phases (§1.3). Minting parallel `补_*`-typed commands would double the owner surface and re-open R-019 (D-C3).
  *Alternatives*: (b) new typed commands per sub-unit — cleaner naming, but doubles owners, adds dispatch paths (R-019-blocked), and duplicates idempotency logic; (c) new commands only where no owner exists (i.e. promote) — but 补 promote is deferred anyway (OQ6).

- **OQ2 (= recon Q17, second half) — who produces the gap list.** A single **completeness-audit** recovery phase that composes the stages' *already self-reported* signals (`completion_status="partial"`, `retry_wait` items, `refill_saturation`, manifest `partial`) into intents — versus a separate reconciler that recomputes coverage?
  **Recommended**: **owner self-report, composed by one audit phase** (§2.2). The honesty contracts already emit the gaps; the audit reads them, it does not re-derive correctness. A rival reconciler would race the daemon and the owners (§3.1).
  *Alternatives*: (b) each owner also self-enqueues its own compensation — more distributed but harder to bound/observe globally; (c) a standalone reconciler daemon — rejected (races the tick + R-019).

- **OQ3 — compensation intent record + idempotency key.** Adopt `sourcing.pipeline.compensation_intent.v1` (§2.1), keyed by `(stage, sub_unit_id, source_lineage)`, carrying the **stage owner's own idempotency key** as the never-double-pay fence (§2.1 rule 3 / §5), and living on the `DurableRuntimeWriter` causal spine (§2.3)?
  **Recommended**: yes — the intent carries a *pointer + the owner's key*, never a paid payload; it cannot pay on its own. Living on the durable spine avoids the R-019 last-writer-wins race.
  *Alternatives*: (b) a new standalone `compensation_intents` table with its own dispatch runtime — a migration + reader/writer/dispatch surface for records that are 1:1 with existing owner replays; defer unless volume demands it.

- **OQ4 — the seam (inside the tick vs sibling).** Confirm compensation lives **inside** `run_worker_recovery_once` as an additive `compensation_completeness_audit` `RecoveryPhase` (§3.1/§3.2), with the 补 executors being the **existing** command-owner phases — and NOT a sibling recovery loop?
  **Recommended**: yes — one additive audit phase, zero new dispatch phases. A sibling loop double-drives the owners and races the per-phase budget + R-019 (§3.1).
  *Alternatives*: sibling daemon — rejected outright (task hard rule: must not duplicate/race the existing daemon).

- **OQ5 — R-019 gating (HARD).** Confirm compensation rides **only** existing idempotent command-owner replays — **no new dispatch path, no new `update_*_state` call site, no new `_connect_with_transaction_lock` caller** — keeping the R-019 call-site count at 24, and that a compensation design minting a NEW paid dispatch path is **blocked until R-019 is remediated** (§3.3)?
  **Recommended**: yes — this is the non-negotiable composition constraint. The audit enqueues; existing owners drain through their existing UoW.
  *Alternatives*: none acceptable — a new dispatch path violates R-019's explicit remediation clause.

- **OQ6 — 补 promote deferral (honest scope cut).** Accept that **补 promote is scoped OUT** of this proposal because (1) promote has no recovery-tick command-owner seat (it runs inline in registration) and (2) its judgment shape is mid-flip under 议案③ (S5 not landed) — so 补 promote is deferred to **after 议案③ S5 + a promote command-owner seat**, and until then the existing keep-incumbent fail-closed (storage.py:8683 / 议案③ ruling ④) IS the promote compensation (§1.4, D-C2)?
  **Recommended**: yes — do not overfit 补 promote to a shape that is still changing; a kept candidate is simply re-evaluated on the next registration once its evidence completes.
  *Alternatives*: (b) build a promote command-owner seat now as part of this proposal — rejected: couples directive #4 to directive #3's unlanded flip and enlarges scope past the atomized stages.

- **OQ7 — recovery-of-recovery bound.** Confirm the failure ladder (§4): bounded retries (`attempt.max` default 3, backoff = the ≤5 s poll cadence), then terminalize to a durable board-visible `compensation_exhausted_needs_human` state — never a silent re-loop, never fail-open; and audit-phase exceptions are caught/isolated and never block the tick?
  **Recommended**: yes — a compensation intent is a finite object with a hard ceiling that escalates to a human, reusing the established `needs_human`/`blocked` terminal + board visibility.
  *Alternatives*: (b) unbounded retry with exponential backoff — rejected: a self-perpetuating dispatch source is exactly the failure mode the paid-dispatch red line forbids.

- **OQ8 (= recon Q18, compensation half) — characterization.** Accept that **no new whole-tick oracle is needed** — the existing `test_recovery_tick_characterization.py` (10/10 byte-identical) is the hard floor, and the audit phase is an **additive** oracle extension (one pinned row, per the group-3 precedent, D-C4) — with the only new pin being a **compensation-contract offline suite** (§6)?
  **Recommended**: yes — the recovery substrate is already characterized (a genuine advantage over 议案③'s mandatory S0); add the contract suite, extend the tick oracle additively at the S3 flip, weaken nothing.
  *Alternatives*: (b) build a fresh compensation-specific whole-tick oracle before S1 — redundant with the existing tick oracle; wasteful.

---

## 9. Implementation slices + review-gate routing

Each slice is independently green; §7-protocol applies (implement + targeted tests + pin commit + record review request, then continue; NO-GO freezes only the affected scope's promotion). **No slice lands before the OQ1–OQ8 ruling batch is answered.**

| Slice | Content | Gate/oracle state | Review gate |
|---|---|---|---|
| **S1** | Compensation contract module: `sourcing.pipeline.compensation_intent.v1` strict fail-closed schema (exact-version, key allowlist), gap-signal→intent pure mapping, attempt-ladder terminalization (§4), delta-only + owner-idempotency-key invariants (§5). New `src/sourcing_agent/pipeline_compensation_contract.py` + offline suite. ADDITIVE ONLY — orchestrator/enrichment/acquisition untouched; tick oracle green untouched. | tick oracle untouched (green) | request recorded (trigger 1: new contract schema) — verdict blocks nothing before S3 |
| **S2** | Completeness-audit **read surface (shadow, no enqueue)**: pure reader from live stage signals (`resolve_segmented_roster_completion` / `retry_wait` / `refill_saturation` / manifest) → proposed intents recorded on an activity surface only + divergence counter (gaps compensated vs gaps the next natural tick already closes). No dispatch, no enqueue. | tick oracle byte-identical | rides S1 |
| **S3** | **Flip**: `compensation_completeness_audit` inserted as a `RecoveryPhase` at a pinned tick position; enqueues intents into the durable queue; existing command-owner phases drain them under their existing fences (ZERO new dispatch code, ZERO new `update_*_state`/`_connect_with_transaction_lock` caller — R-019 count stays 24). Tick oracle **additively extended** (one new pinned row, D-C4). Simulate e2e both compensation-on/off. | tick oracle additively extended in-slice (no existing row changes) | **GO verdict required before landing** (trigger 3: durable-runtime + recovery seam; trigger 5: claimed mechanism) + operator confirmation of OQ1–OQ8 |
| **S4** | **BLOCKED / deferred** — 补 promote (needs 议案③ S5 + a promote command-owner seat, OQ6) AND any live 补 acquire/fetch dispatch beyond existing-owner replay (needs R-019 remediation, OQ5). Not scheduled until those land. | n/a | n/a — dependency-gated |
| **S5** | Live validation wave (compensated top-up on a real partial roster / retry_wait tail). | n/a | operator explicit go + quota + R-019 remediated + live red lines |

Ordering: S1→S2 may proceed immediately after the OQ ruling batch; S3 strictly after the pinned review of the audit-phase + tick-oracle-extension diff (GO). S4/S5 are dependency-gated (议案③ S5, R-019 remediation, quota) and indefinitely deferred.

---

*Design method: read-only over the current tree (orchestrator.py:40217, recovery_phases.py:119/164, test_recovery_tick_characterization.py:66–143, acquisition_command_owner.py:427/715/1869/473/998/1366/1744, company_shard_planning.py:341, snapshot_materializer.py:332, enrichment.py:526/1253/2930/3396/3447/3900, asset_reuse_planning.py:1217/1541/1709, storage.py:8633, durable_runtime.py) + the recon §1.3/§2/§5-Q17 forensics + R-019 (RESIDUAL_LEDGER.md:38) + RECOVERY_BAND_OWNERSHIP_2026-07-22.md + the two proven WS7 议案 as the shape. No src/tests changed; no live env; no PG mutation. Anchors verified on branch `governance-phase0-ttl-20260611`.*
