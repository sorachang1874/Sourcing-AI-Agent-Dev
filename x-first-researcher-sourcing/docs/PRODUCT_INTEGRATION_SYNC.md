# X-First → product integration sync manifest

> Scope: sync payload for the 2026-07-18 X-First review-fix batch (state machine / schema / asset
> registry). Audience: the `sourcing-ai-agent` lane. Format follows the ai-assisted-engineering
> playbook's cross-project integration envelope and interruption-safe handoff card. All digests
> below were computed live from the checked-in X-First assets at generation time; the product
> lane's byte-parity test (`tests/test_x_first_portable_package.py::…byte_identical_to_x_first_owner`)
> is the fail-closed verifier of this sync.

## What changed on the X-First side

Pinned-review (double NO-GO) fixes, all landed in `x-first-researcher-sourcing`:

1. `research_in_progress` now requires a per-account live frontier (unconsumed
   `continuation_available` tip or retryable `failed` tip); zero-attempt / all-exhausted /
   between-chain parking is rejected (`portable_result_research_in_progress_frontier_invalid`).
2. Typed negative handle resolution: new required result arrays `handle_resolution_attempts` and
   `handle_resolution_outcomes`; `no_verified_account` only from an exhausted `no_match` chain,
   `failed` only with a bound `failed` attempt carrying an error receipt; inline content-addressed
   attempt receipts.
3. Semantic-recall `failed` requires at least one bound `failed` attempt.
4. Optional-channel outcome gains `research_in_progress` (persistable partial frontier; blocks
   `complete` and `failed` derivations).
5. `x_account` seeds may only be `analyzed` / `research_in_progress` (see `docs/RESIDUAL_LEDGER.md`
   R-001).
6. Orchestration registry now covers the package-manifest and semantic-receipt schemas plus the
   selected-subject package fixture; `validate_checked_in_assets` rebuilds the package and compares
   it byte-for-byte.

## Pinned artifacts (byte digests)

| artifact | path (X-First) | sha256 of bytes |
| --- | --- | --- |
| result schema | `contracts/x.portable.research_campaign.result.v1.schema.json` | `31dbfc3d9f31a026df9fbe1d66fad38d00f91ff47f071533837adaa4f4f31403` |
| result fixture | `fixtures/portable_research_campaign_result_fixture_v1.json` | `19ae64034ddee5392b77cf863311f7aacd5f0a8c00d30381fb8457294674e925` |
| selected-subject package fixture | `fixtures/selected_subject_fixture_simulate_package_v1.json` | `074244972d0945a8caaa658592c7554d56f9c547d5dec822bfc826a73186914e` |

All other vendored schemas (request, plan, package manifest, semantic receipt, request binding,
orchestration policy, scope catalog) are unchanged and still byte-identical.

Package trust values embedded in the package fixture:

- `manifest_sha256`: `f2ac74a74bfc21c105b52e07167afddcff48b2c7408a809d08211cbc8e033407`
- `receipt_sha256`: `615e9c068c3ba9fe12ad86f9b955ab3f5c41f8a668a81f43872e5fa40621537e`
- `validator_revision_sha256`: `f44f61134d689efc5115fff538fd6149150847a24dc4ab1acccd570c92ffb10d`
  (revision string unchanged: `x_first.portable_campaign.semantic_validator.v1`; the digest moved
  because the validator implementation sources changed)
- receipt effects: `simulated_external_attempt_count=4`, `simulated_external_evidence_count=6`
  (resolution arrays are empty in the selected-subject package; base-fixture resolution rows are
  not part of this package)

## Product-lane sync checklist

1. Copy `contracts/x.portable.research_campaign.result.v1.schema.json` →
   `contracts/external/x_first/` (byte copy), then update the result pin in
   `src/sourcing_agent/x_first_portable_package.py` (`_ARTIFACT_CONTRACTS`, currently line ~126) to
   `31dbfc3d9f31a026df9fbe1d66fad38d00f91ff47f071533837adaa4f4f31403`.
2. Copy `fixtures/selected_subject_fixture_simulate_package_v1.json` →
   `tests/fixtures/x_first/` (byte copy).
3. Replace `configs/x_first_fixture_semantic_validation_registry.v1.json` with the content below
   (byte-exact, trailing newline included), then update the hard-coded `FIXTURE_REGISTRY_SHA256`
   in `src/sourcing_agent/x_first_portable_package.py` (currently line ~39) to
   `a4901921f7a142c85cd9a392fa1fc974df4e3c45ba8cf9ddc141abb527fba7c9`:

```json
{
  "schema_version": "sourcing.x_first.fixture_semantic_validation_registry.v1",
  "registry_version": "x_first_fixture_semantic_validation_registry.v1",
  "fixtures": [
    {
      "fixture_id": "x_first_selected_people_fixture_v1",
      "manifest_sha256": "f2ac74a74bfc21c105b52e07167afddcff48b2c7408a809d08211cbc8e033407",
      "receipt_sha256": "615e9c068c3ba9fe12ad86f9b955ab3f5c41f8a668a81f43872e5fa40621537e",
      "validator_revision_sha256": "f44f61134d689efc5115fff538fd6149150847a24dc4ab1acccd570c92ffb10d"
    }
  ],
  "authority": {
    "caller_supplied_pin_allowed": false,
    "live_authority": false,
    "product_writes_allowed": false
  }
}
```

4. Mirror the fixture-only scan: X-First's `_validate_fixture_execution` now also scans
   `handle_resolution_attempts` (nested `retrieval_receipt` must be `fixture_synthetic` with
   `receipt_locator=None`) and counts them in `simulated_external_attempt_count`. The product-side
   mirror in `x_first_portable_package.py` should match, or the next non-empty resolution fixture
   will diverge on effect-count reconciliation. (Inert for this fixture: both arrays are empty.)
5. Product decision (not required for sync): whether `handle_resolution_outcomes`
   reason/error/attempt bindings should surface in the verification import preview; X-First keeps
   them in the result artifact and the preview schema would need a versioned change.
6. Re-run the product byte-parity, registry-drift, capability-seal, and preview tests
   (`tests/test_x_first_portable_package.py`, `test_x_first_portable_adapter.py`,
   `test_x_first_simulate_owner.py`), then the Independent Review Gate per product rules.

## X-First validation evidence

All commands from `x-first-researcher-sourcing/`:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest discover -s tests
# Ran 608 tests — OK
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.research_orchestration
# {"errors": [], "status": "valid"}
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.contracts
# "status": "valid"
../sourcing-ai-agent/.venv/bin/python scripts/check_residual_ledger.py
# {"errors": [], "status": "valid"}
../sourcing-ai-agent/.venv/bin/python -m ruff check src/ tests/
# All checks passed (scoped: changed files)
```

## Resume card

- lane: `x-first-researcher-sourcing`
- goal: close the 2026-07-18 double-NO-GO X-First findings and ship a turn-key product sync
- base state: worktree on branch `governance-phase0-ttl-20260611`; X-First review-fix batch
  uncommitted, all validation green
- touched paths: `contracts/x.portable.research_campaign.result.v1.schema.json`,
  `contracts/research_orchestration_contract_registry.v1.json`,
  `src/x_first/research_orchestration.py`, `src/x_first/portable_campaign_package.py`,
  `fixtures/portable_research_campaign_result_fixture_v1.json`,
  `fixtures/selected_subject_fixture_simulate_package_v1.json`,
  `tests/test_research_orchestration.py`, `tests/test_portable_campaign_package.py`,
  `tests/test_residual_ledger.py`, `scripts/check_residual_ledger.py`,
  `docs/GENERALIZED_RESEARCH_ORCHESTRATION.md`, `docs/RESIDUAL_LEDGER.md`, this file
- last validation: `PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest discover -s tests` → `Ran 608 tests — OK`
- state: X-First side complete and green; product-lane sync not yet applied (checklist above)
- next step: product lane executes the checklist, then both lanes re-run the pinned cross-project
  review; residuals tracked in `docs/RESIDUAL_LEDGER.md` (R-001…R-004, N-001)
