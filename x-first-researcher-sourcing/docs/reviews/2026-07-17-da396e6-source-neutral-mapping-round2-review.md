# Source-neutral mapping round-2 pinned review

> This artifact reviews pinned commit `da396e62151bcb824036574a6433c845993049a6` only. It does not review, validate, or characterize the current fixed-forward working tree.

Date: 2026-07-17

## Evidence header

- Reviewed commit: `da396e62151bcb824036574a6433c845993049a6`
- Exact parent: `ce3ffecc98ca39ac476fc6d008298da5922ee52a`
- Pinned tree: `040fa4d819afd4505493e331a0fd4ae19ff0b87b`
- Commit subject: `fix(x-first): make mapping lineage fail closed`
- Relationship: the reviewed commit has exactly the requested parent.
- Exact scope: the 11 changed files under `x-first-researcher-sourcing`; no sibling-repository changes were reviewed.
- Scope size: 11 files, 1,057 insertions, 267 deletions.
- Scoped binary-diff SHA-256: `5dfdf9ceeb1ee6f9c8ff681cef2002a45b04cef6bf7466435ce3a88c875a7ee3`
- Scoped name/status SHA-256: `05257e4842d017c9b62a6223ecc838f09f8fd12c402165aa88027627d0df5296`
- Scoped numstat SHA-256: `8c2bdc30b31d919582cd10de24907561b8024c04e64f59a36600bb4ee938adf9`
- Inspection source: clean detached worktree `/private/tmp/x-first-da396e6-review.SOXk29` at the exact pinned commit plus pinned Git objects.
- Review boundary: no network, provider, model, credential, private-artifact, or live-X call was made. Mutable working-tree implementation files were not used as review evidence, and the review did not modify the pinned worktree.

## Validation

| Check | Pinned result |
|---|---|
| `PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_source_neutral_mapping -v` | 24/24 passed in 5.384s |
| `PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v` | 505/505 passed in 129.154s |
| `PYTHONPATH=src .../.venv/bin/python -m x_first.source_neutral_mapping` | exit 0; `errors=[]`; `status=valid` |
| `PYTHONPATH=src .../.venv/bin/python -m x_first.contracts` | exit 0; `errors=[]`; precision/recall both 1.0 |
| Ruff check for changed Python files | passed |
| Ruff format for changed Python files | passed; 2/2 already formatted |
| Scoped `git diff --check` | passed |
| Duplicate-hydration-task probe | two queued IDs, first task hydrated twice, second omitted; exact hydration and Luna coverage both accepted as 2/2, reducer `complete`, remaining Luna queue empty |
| Campaign-fork probe | two ordinal-1 siblings accepted from one predecessor; nonzero sibling added six IDs, selected zero branch remained adjacent and continued |
| Cross-wave owner probe | six stable IDs rotated across six candidate owners; fresh mapping/hydration/Luna replay passed, cumulative frontier unchanged, marginal count zero |
| Strategy-normalization probe | reversing a commutative OR alias list changed the semantic strategy signature |
| Stable-ID canonicalization probe | both `1` and `01` accepted; 12 string-unique IDs represented 11 numeric identities |
| Luna evidence-reduction probe | complete `current` plus `unsupported` reviews reduced both axes to `ambiguous` |
| Recursive lineage probes | explicit cycle and depth overflow both failed closed with stable typed errors |

The data-quality review treated queued task identity and candidate/evidence grain, rather than projection row counts, as the authoritative coverage denominator.

## Findings totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 3 |
| P2 | 4 |
| P3 | 0 |

## Findings

### P1-1 — Duplicate hydration can replace a missing Post while reporting complete coverage

`_validated_hydration_outputs` validates projection, session, and request uniqueness but does not enforce unique `task_sha256` values or exact equality with the queued-task set (`src/x_first/source_neutral_mapping.py:1498-1520`). Wave completion compares only list lengths (`src/x_first/source_neutral_mapping.py:1684-1685`), and aggregate arithmetic counts hydration projection rows rather than distinct completed tasks (`src/x_first/source_neutral_mapping.py:2087-2117`).

The negative probe queued stable IDs `8000010` and `8000011`, then hydrated `8000010` twice with distinct session, request, and projection identities while omitting `8000011`. The aggregate accepted exact hydration `2/2 = 1.0`, accepted Luna diagnostic coverage `2/2 = 1.0`, reported semantic coverage `complete`, and the executed-wave fact cleared the Luna queue.

One Post can therefore disappear while all downstream coverage surfaces claim 100 percent. Require exactly one projection per unique queued task SHA, compare exact task-SHA sets where completion is claimed, and derive hydration numerators from distinct completed task identities.

### P1-2 — Campaign lineage is forkable and its proof registry is not campaign-global

The builder recursively replays only the caller-selected predecessor and derives the next ordinal from that value (`src/x_first/source_neutral_mapping.py:1641-1653`). The public builder has no canonical campaign head, append-only journal, compare-and-swap successor token, root registry, or unique successor owner (`src/x_first/source_neutral_mapping.py:1862-1884`). Stopping checks only the supplied trailing branch (`src/x_first/source_neutral_mapping.py:1938-1995`).

Two legal ordinal-1 siblings were built from one predecessor. One sibling added six Posts; the other was zero-yield and continued into another zero-yield wave. Both siblings passed construction, and the selected zero branch remained `lineage_adjacent=true`. The same execution bundle was also accepted in two sibling facts of one campaign and as ordinal-zero roots of two different campaign IDs.

Diagnostic Luna authority currently prevents structural stopping, but cumulative campaign truth and the documented no-silent-cache rule are already unenforced. Add a durable canonical campaign head, append-only successor CAS, global proof-identity registry, and an explicit source/consumer-bound cache projection.

### P1-3 — The cumulative frontier drops candidate and author ownership

Within one wave, stable-Post ownership is checked (`src/x_first/source_neutral_mapping.py:1014,1101-1103`). The predecessor frontier, however, retains only a tuple of bare stable IDs (`src/x_first/source_neutral_mapping.py:1654-1666`), and marginal yield is derived from a string-ID set difference (`src/x_first/source_neutral_mapping.py:1716-1735`).

The negative probe rotated the same six stable IDs across six candidates using fresh mapping, exact-hydration, and Luna execution identities. Candidate bindings changed, the cumulative frontier remained unchanged, and the successor reported zero new stable IDs.

This accepts cross-wave owner conflicts and can swallow corrected candidate evidence as zero marginal yield. Persist and validate `stable_post_id -> candidate_ref + author/platform identity` across the campaign instead of retaining only an ID set.

### P2-1 — Temporal diagnostics lack temporal evidence and use an invalid evidence lattice

The exact-hydration contract contains no publication timestamp, observation time, or adjudication `as_of` (`contracts/x.source_neutral.mapping.exact_post_hydration.v1.schema.json:6-26`). Luna input emits candidate, author, Post ID, URL, and text but no temporal anchor (`src/x_first/source_neutral_mapping.py:1523-1534`). Nevertheless, each review must propose `current|historical|ambiguous|unsupported`, while the state-version digest binds only an undated manifest prior (`src/x_first/source_neutral_mapping.py:1435-1441`).

The reducer also maps every heterogeneous proposal set to `ambiguous` (`src/x_first/source_neutral_mapping.py:1417-1424`). A complete review containing one `current` signal and one unrelated `unsupported` Post reduced both axes to `ambiguous`. Unrelated no-signal evidence can therefore erase a useful signal, and current versus historical cannot be grounded consistently.

Use timestamped evidence-level claims such as `supports_current`, `supports_historical`, `no_signal`, and `conflict`, bind an adjudication `as_of`, and apply deterministic evidence precedence rather than unanimity over per-Post candidate-state labels.

### P2-2 — Commutative OR aliases create false strategy distinctness

The semantic strategy payload preserves alias order and uses that order in its query template (`src/x_first/source_neutral_mapping.py:1594-1615`). The syntactic payload is hashed into each fact (`src/x_first/source_neutral_mapping.py:1808,1841`), and a differing hash is sufficient for the strategy-distinctness term (`src/x_first/source_neutral_mapping.py:2024-2025`).

Reversing one OR alias list, without changing the alias set, native tool, mode, limit, candidate coverage, or topology, changed the strategy signature. Thus `a OR b` and `b OR a` can be counted as materially distinct.

Canonicalize OR terms, including documented Unicode, case, and whitespace handling, unless order sensitivity is an explicit empirically validated strategy dimension.

### P2-3 — Stable Post IDs accept non-canonical decimal aliases

`_STATUS_ID_RE` accepts any one-to-32-digit string, including leading-zero aliases (`src/x_first/source_neutral_mapping.py:69`). URL and hydration validation reuse that rule (`src/x_first/source_neutral_mapping.py:712,1174`), while frontier deduplication uses strings and sorting uses `int` (`src/x_first/source_neutral_mapping.py:1716-1727`).

A complete mapping, hydration, and Luna fact accepted both `/status/1` and `/status/01`. It reported 12 string-unique IDs representing only 11 numeric identities. Equal integer sort keys over a set can also make the cumulative digest order process-dependent.

Require canonical positive decimal representation at ingress, for example `^[1-9][0-9]*$`, and verify `str(int(value)) == value` within the supported length bound.

### P2-4 — The strategy contract cannot represent saturation or challenger execution

The semantic strategy payload hardcodes `strategy_family=wave_p`, `query_surface=candidate_authored`, `relationship_topology=self_authored`, and `time_window=None` (`src/x_first/source_neutral_mapping.py:1594-1615`). `expand_saturation_item` emits topic, mode, and time children (`src/x_first/source_neutral_mapping.py:959-997`), but no executable and replayable `ExecutedWaveFacts` path consumes those children or challenger strategies. The public facts builder accepts only a validated Wave P plan (`src/x_first/source_neutral_mapping.py:1618-1635,1862-1884`).

Consequently, the strategy digest cannot describe the actual saturation or challenger work required by the six-queue stopping contract. Tests obtain distinctness by changing the entire Wave P policy mode rather than replaying a real child strategy.

Add a source-neutral strategy plan and retained execution projection capable of expressing every query family before using the digest as global convergence evidence.

## Prior-finding disposition

1. Caller-created Luna authority is fixed narrowly. The result contract is diagnostic-only, caller upgrade Booleans and IDs are gone, resolved state remains the frozen prior, authorized transitions remain zero, and diagnostic completion cannot authorize stopping.
2. Caller-owned `prior_stable_post_ids` is removed and direct `[F1,F3]` adjacency forgery is fixed, but the core lineage finding is re-raised because campaign branches, omitted siblings, missing canonical head authority, and lost evidence ownership remain possible.
3. Changing only plan or run identity no longer changes the strategy signature. The complete semantic-distinctness finding is re-raised because commutative alias ordering remains syntactic and actual saturation/challenger strategies cannot be represented.
4. Cross-lane execution identity reuse is fixed within one wave and one linear predecessor chain. Sibling-branch and independent-root reuse remain accepted without a cache projection.
5. Caller-owned upgrade multiplicity is removed, and the reducer emits one diagnostic row per candidate, axis, and manifest-bound state version. Exact task-set coverage and temporal evidence reduction remain incorrect as described above.

## Confirmed controls and accepted residuals

- Exact Wave P plan reconstruction continues to reject policy-owned call and batch drift.
- Mapping and hydration projection replay remains source-retaining and fail-closed at its immediate boundary.
- Same-wave mapping/hydration session and request identity collisions are rejected.
- Recursive predecessor replay rejects explicit cycles and excessive depth with stable typed errors.
- Diagnostic Luna output cannot authorize a state transition or structural stop; receipt-first semantic execution remains unimplemented, and structural stopping is deliberately unavailable.
- No explicit cache projection exists. Reuse inside one linear chain is rejected, while the broader registry gap is a finding rather than an accepted exception.
- Owner-private calibration files were not opened or revalidated.
- Historical exploratory sessions remain non-promotable because they lack the typed precommit required by this controller.

## Final verdict

NO-GO
