# Track D D0f — Durable model invocation envelope evidence

> Status: author implementation candidate (2026-07-15); non-live validation and fresh pinned independent review are
> required before this scope can be promoted. This batch is PG-only terminal-envelope evidence. It is not logical
> result-slot acceptance/consumption, an AgentAction/effect path, a provider transport, a cost ledger, a live gate, or
> product activation.

## 1. Engineering outcome

D0f gives the existing immutable `ModelInvocationEnvelopeV1` exactly one durable owner without introducing a second
envelope schema:

- `ModelInvocationEnvelopeV1.to_canonical_json()` and `from_canonical_json()` own the exact canonical JSON round trip;
- `ModelInvocationEnvelopeRepository` is the sole reference issuer, persistence/lookup owner, and retention entrypoint;
- `ControlPlaneRepositories.model_invocation_envelopes` is the only store repository population;
- migration `0007_model_invocation_envelopes.sql` adds one immutable PG-only evidence table;
- the live PG adapter uses a specialized advisory-lock + `SELECT ... FOR UPDATE` + plain `INSERT` path, never the
  generic replace-all upsert or `_PRIMARY_KEY_COLUMNS`;
- `Kind.TIMESTAMPTZ` adds the exact nullable timezone-aware codec required by this table. Decimal remains deferred.

All validation and reference derivation complete before the first database call. `replay` and incomplete strict
evidence therefore fail with zero reads/writes.

## 2. Owner and identity contract

| Contract | Sole owner / source of truth | Exact rule | Forbidden behavior |
|---|---|---|---|
| Envelope schema and digest | `ModelInvocationEnvelopeV1` | Existing v1 record and computed SHA-256; canonical JSON must round-trip byte-for-byte | Repository-owned duplicate schema, caller-trusted digest, normalization on read |
| Durable reference | `ModelInvocationEnvelopeRepository` | `mie:v1:<full-PFX-digest>:<envelope-digest>` | Caller-minted refs, short/partial tenant identity, placeholder URIs |
| Full PFX | Repository input plus exact envelope mirror | `(runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id)` | `scope_digest` alone, automatic tenant/mode fallback, cross-PFX lookup |
| Immutable persistence | Specialized live PG adapter | Transaction advisory lock, both digest/ref `FOR UPDATE` probes, exact replay comparison, plain `INSERT` | Generic `upsert_row`, `ON CONFLICT DO UPDATE`, revival after purge |
| Lookup | Repository + specialized adapter | Full PFX + owner ref + envelope digest | Ref-only lookup or digest-only lookup |
| Retention | DB clock and fixed policy `model_invocation_retention_30d_v1` | `retained` for exactly 30 days, then one CAS to `purged_tombstone` | Caller timestamps, policy ladder, privacy-engine expansion |

The full-PFX digest is SHA-256 over canonical JSON containing the fixed schema tag
`model_invocation_envelope_pfx_v1` and all five PFX values. Both the reference and lookup recompute it; an inconsistent
PFX/ref/digest tuple rejects before PG.

## 3. Exact physical manifest

The ordered `model_invocation_envelopes` manifest is fixed at fifteen columns:

| # | Column | Type | Nullable | Default |
|---:|---|---|---:|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `model_invocation_envelope_ref` | `TEXT` | no | none |
| 7 | `envelope_schema_version` | `TEXT` | no | none |
| 8 | `envelope_digest` | `TEXT` | no | none |
| 9 | `envelope_record_json` | `TEXT` | yes | none |
| 10 | `retention_policy_version` | `TEXT` | no | none |
| 11 | `retention_state` | `TEXT` | no | `retained` |
| 12 | `retained_until` | `TIMESTAMPTZ` | no | owner INSERT expression |
| 13 | `created_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 14 | `purged_at` | `TIMESTAMPTZ` | yes | none |
| 15 | `state_version` | `BIGINT` | no | `0` |

Keys are exactly:

```text
PRIMARY KEY (PFX, model_invocation_envelope_ref)
UNIQUE      (PFX, model_invocation_envelope_ref, envelope_digest)
UNIQUE      (PFX, envelope_digest)
```

DDL constraints close eligible modes, SHA/ref shapes, the exact v1 schema/policy, positive review lineage, the
30-day deadline, and the two valid lifecycle shapes. No activity, result-slot, cost, receipt, approval, action, or
effect column is added.

## 4. Eligibility and zero-write boundary

Durable persistence is eligible only for `live|simulate|scripted`. Every eligible mode requires all six causality
identities:

```text
operation_run_id
turn_id
step_id
workflow_command_id
activity_run_id
activity_attempt_id
```

Every mode also requires non-null `cost_exposure_ref`; `live` additionally requires non-null
`effective_route_snapshot_ref`. The envelope's runtime namespace, provider mode, and workspace must exactly mirror the
PFX. `replay`, missing/partial causality, missing cost identity, live without route-snapshot identity, and any mirror
mismatch reject before PG authority or adapter calls.

This batch persists completed envelope evidence only. It does not decide whether a logical result slot is current,
accept or consume a result, create an AgentAction, apply an effect, authorize a provider call, or reconcile money.

## 5. Exact replay, collision, and retention

For one reference transaction the specialized adapter:

1. takes the transaction-scoped advisory lock derived from the owner-issued ref;
2. selects by full PFX + digest and by full PFX + ref with `FOR UPDATE`;
3. returns only an exact immutable replay, rejects split/ref/digest/canonical-record collision, or performs one plain
   `INSERT`;
4. lets the repository revalidate the complete fifteen-column row and canonical envelope before returning it.

Concurrent identical writers therefore converge on one row. A retained row has canonical JSON, `state_version=0`, and
no purge timestamp. The DB-clock purge entrypoint uses `FOR UPDATE SKIP LOCKED` plus state-version CAS to erase only the
canonical JSON and transition once to `purged_tombstone`, retaining immutable PFX/ref/schema/digest/policy/time identity
with `state_version=1`. A retry after purge returns typed `ModelInvocationEnvelopePurgedError`; it never revives or
rewrites the tombstone.

## 6. Ten-invariant closure for this bounded batch

| Invariant | D0f mechanism |
|---|---|
| Single writer / aggregate ownership | One repository and one specialized PG adapter surface; no generic descriptor registration |
| Tenant key | Every key, ref derivation, probe, and lookup contains the full five-field PFX |
| Generation / physical fencing | Positive pinned coordination-review lineage is part of physical identity; D0f does not claim result-slot generation authority |
| Lifecycle completeness | Exactly `retained -> purged_tombstone`, version `0 -> 1`; no reverse edge |
| Late / partial results | D0f stores terminal envelope evidence only and grants zero late-result acceptance authority |
| Cost honesty | Non-null exposure ref required; no amount, pricing, reservation, or reconciliation inference |
| Physical identity binding | Owner ref binds canonical full-PFX digest and canonical envelope digest |
| Provenance / trust boundary | Exact schema roundtrip and envelope/PFX mirrors are revalidated before and after storage |
| Self-contained / cross-document consistency | Existing `ModelInvocationEnvelopeV1` remains schema owner; D3c2g/D3c2h0 successor observations record the narrowed closure |
| Runtime / mode isolation | Only live/simulate/scripted; replay zero-write; live route-snapshot evidence required |

## 7. Explicit non-closure

D0f does not implement or authorize:

- logical result-slot accept/consume, freshness/generation/control-epoch decisions, or AgentAction/effects;
- response/failure receipts, late quarantine, verification intent, cost reservation/exposure tables, Decimal codecs,
  settlement, provider-search/Harvest transport variants, or strict runtime writers;
- provider/model/HTTP calls, route activation, credentials, paid/live canaries, W6/nightly, manual signoff, or served
  Agent tool population;
- a generic privacy/retention engine, per-tenant policies, deletion workflows beyond the fixed audit tombstone, or a
  second envelope version/schema;
- formal independent review or any broader Track D milestone closure.

## 8. Local validation

Run from `sourcing-ai-agent/` without provider/model credentials:

```bash
make local-pg-up
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d0f_model_invocation_envelope_repository.py \
  tests/test_model_invocation_contract.py
SOURCING_REQUIRE_PG_STORE_TESTS=1 SOURCING_REQUIRE_PG_DURABLE_RUNTIME_TESTS=1 \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d0f_model_invocation_envelope_postgres.py
SOURCING_REQUIRE_PG_STORE_TESTS=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
  .venv/bin/python -m pytest -q tests/test_migration_runner.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_storage_surface_guardrails.py tests/test_pg_onconflict_guard.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d3c2g_cost_ledger_decision_lock.py \
  tests/test_d3c2h0_evidence_cross_contract_ratification.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_control_plane_postgres.py
```

Final author evidence on the candidate tree: D0f unit + existing invocation contract `83 passed`; real-PG migration,
three-mode exact replay, full-PFX isolation, 16-writer convergence, canonical tamper, DB constraints, and tombstone
non-revival `1 passed`; full migration chain `15 passed + 65 subtests`; storage/`ON CONFLICT` guards `64 passed`;
D3c2g/D3c2h0 successor ratchets `22 passed`; adjacent control-plane PG unit `17 passed`; scoped Ruff lint/format and
focused mypy over three source files clean. Project mypy remains exactly at the accepted baseline `81 errors / 4 files`
with no D0f file in the error set. These are implementation evidence only. They cannot substitute for a fresh pinned
non-author review or be reported as formal `GO`.
