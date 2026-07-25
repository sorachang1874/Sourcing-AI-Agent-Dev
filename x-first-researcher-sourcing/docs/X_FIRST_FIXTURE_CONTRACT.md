# X-first Fixture Contract

> Status: fixture-only vertical slice. No live provider/model/network access and no canonical writes.

## Goal

Prove the data, identity, state, coverage, safety, and artifact boundaries required for an X-first professional-evidence
lane before any Grok/X capability call.

## Population

The discovery population is `lab + current affiliation proposal + PRETRAIN_CORE|PRETRAIN_ADJACENT`. The fixture may
include negative and unknown accounts so precision and recall are meaningful. Protected traits and proxy signals are
not fields, query inputs, evidence classes, outputs, or ranking features.

## Ownership matrix

| Contract | Owner/source of truth | Fixture rule | Future adapter rule |
| --- | --- | --- | --- |
| External X account | Stable platform user ID | Handle changes do not change identity | Preserve ID and handle history |
| Provisional person | X-first sibling | Opaque `pp_x_<ULID>` | Never auto-merge by name/model |
| Current affiliation | Evidence proposal | May be `current_proposed`, `conflicted`, or `unknown` | Requires adjudication |
| Pretrain relevance | Fixture taxonomy | Core, adjacent, out-of-scope, or unknown | Unknown is quarantined |
| Observation | `x.grok.collection.v1` | Bounded excerpt, public URL, time, task provenance | Verify before materialization |
| Assertion | Canonical assertion owner | Always empty | Separate owner confirmation |
| Coverage | Query-family ledger | Explicit stop reason, `exhaustive=false` | Never claim exhaustive X coverage |
| Query registry | `configs/query_families.v1.json` plus executable validator | Exact eight families, `2/20` per-family caps, protected-signal scan | Owner-reviewed version bump before expansion |
| Gold labels/thresholds | `openai_pretrain_gold_v1.json` plus executable validator | Packet labels must match exactly; precision `0.95`, recall `0.90` | Never lower thresholds in-place |
| CRM/export/outreach | Existing product owners | No calls or fields | Explicit future gate only |

## State registries

Run status is one of `queued`, `running`, `blocked`, `completed`, `failed`, or `cancelled`. Task status is one of
`pending`, `queued`, `running`, `succeeded`, `failed`, `cancelled`, `expired`, or `quarantined`. Unknown values fail
validation; no unknown status is treated as active.

The canonical fixture is terminal: run `completed`, every task `succeeded`, no errors. Negative and unknown candidates
are represented by relevance/review state rather than hidden task failure.

`contracts/x.grok.collection.v1.schema.json` is the declarative interchange shape;
`src/x_first/contracts.py` is the zero-dependency executable Stage 0 gate. Both must move together. The executable gate
uses strict JSON types (booleans never masquerade as integers), full provisional ULID syntax, timezone-aware timestamps,
exact fixture stop reasons/budgets, bidirectional references, and a fail-closed selection truth table.

## Identity boundary

- `platform_user_id` owns the external-account identity.
- `current_handle` is a mutable attribute; `handle_history` is append-only evidence in the artifact.
- `provisional_person_id` is a separate opaque subject reference.
- `identity_link_proposals=[]` in the first slice.
- A future proposal must be reversible and human reviewed; it cannot create a canonical person or assertion directly.

## Evidence boundary

Every observation includes a stable fixture object ID, account ID, kind, `.invalid` public URL, authored/observed
times, bounded excerpt, task, query family, and technical scope. Candidate packets reference only observations owned by
the same external account. Account profile URLs and observation canonical URLs are each globally unique in the
artifact. Account and observation `observed_at` values must fall within the run window, and the active handle must
start no later than the account snapshot time.

The artifact keeps `assertions=[]` and contains no PersonAsset, PersonEvidence, CRM, projection, export, or outreach
shape. A future adapter must preserve `search -> fetch/verify -> adjudicate -> materialize` and remain a separate owner.

## Query and coverage contract

The eight query families are versioned in `configs/query_families.v1.json`. Each has one deterministic fixture task and
12 observations. The coverage ledger records the lab, family, task, count, stop reason, and `exhaustive=false`.

Fixture hard budgets are exactly eight tasks, eight pages, 96 observations, and 24 candidate packets. External calls
and cost are zero.

## Acceptance metrics

- precision at least 95%;
- fixture recall at least 90%;
- false merges exactly zero;
- protected-trait/proxy output exactly zero;
- 24 accounts, 96 observations, eight tasks/families, and complete reference integrity;
- all URLs `.invalid`, no real X URL;
- deterministic regeneration and validation under 60 seconds.

These metrics validate the synthetic contract only. They make no claim about live X coverage or provider quality.
