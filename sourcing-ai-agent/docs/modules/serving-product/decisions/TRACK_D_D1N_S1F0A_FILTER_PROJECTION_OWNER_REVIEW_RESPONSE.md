# Track D D1n S1f0a — pinned Ultra review response

> Candidate: `64a7dfc8f84f416a166a6ded69c666c7e50e523e`
> Review: `runtime/reviews/20260718T103535Z_Track_D_D1n_S1f0a_filter_projection_publication_owner_decision.md`
> Verdict: `NO-GO`, new findings `P0/P1/P2/P3 = 0/10/2/2`; residual R-019/R-029.
> This fixed-forward response is author evidence, not an independent-review verdict.

## Outcome

The response retracts the candidate's product-owner and terminal-owner claims. The current status is
`foundation_only_unbound`; `serving_projection_owner` remains the only physical writer. S1f0b may harden reservation,
invalidation, collection strip, and the existing atomic UoW, but no current carrier is Agent-readable or adoptable as
the future product owner.

The closed fixed-forward source is
[`filter_projection_foundation_boundary_v1.json`](../contracts/filter_projection_foundation_boundary_v1.json). The
human-readable boundary is
[`TRACK_D_D1N_S1F0_FILTER_PROJECTION_OWNER_DECISION.md`](TRACK_D_D1N_S1F0_FILTER_PROJECTION_OWNER_DECISION.md).

## Finding reconciliation

| # | Review finding | Fixed-forward disposition |
| --- | --- | --- |
| 1 | second physical owner | closed: physical writer is `serving_projection_owner`; filter component is a field-validator candidate only |
| 2 | receipt occurrence not bound | product claim removed; exact receipt/Action/Operation/Workflow/root join is a required S1f0c0 decision and S1f0c1 propagation |
| 3 | planning/execution parity incomplete | product claim removed; S1f0c0 must freeze retained planning-manifest load and exact capability-bearing recompile |
| 4 | incomplete candidate population commitment | product claim removed; S1f0c0 must define nonzero bounded source equality, counts, exclusions, and canonical member-set digest |
| 5 | freshness/readiness owner missing | product/reader claim removed; both owners are explicit S1f0c0 prerequisites |
| 6 | nested lane shapes open | product/reader claim removed; exact private-to-model-safe lane transformation is an S1f0c0 prerequisite |
| 7 | runtime path incompatible with model identifier | product/reader claim removed; S1f0c0 must ratify a versioned opaque namespace ref with private path binding |
| 8 | wrong terminal owner and incomplete result-slot ref | S1f1 blocked; Cohort result is audit provenance, final projection publication will be terminal authority after S1f0d |
| 9 | mutation surface incomplete | machine manifest enumerates every current writer; S1f0b owns dedicated registries plus atomic all-member invalidation |
| 10 | lexical decision oracle | replaced by a closed JSON manifest and unknown-key structural tests |
| 11 | zero-DDL/atomic oracle weak | migration chain is scanned in S1f0a tests; behavioral fault/PG evidence belongs to S1f0b |
| 12 | writer characterization incomplete | machine manifest contains the explicit facade/repository/native/orchestrator/migration/repair inventory |
| 13 | route synchronization weak | all five serving-product routers carry the same `foundation_only_unbound` status and exact links |
| 14 | S1f0b/S1f1 ownership contradiction | closed: S1f0b foundation, S1f0c0-c2 lineage, S1f0d product owner, S1f1 reader/result |

## Exact dependency graph

```text
S1f0b-foundation --------------------+
                                      v
S1f0c0-lineage-decision -> S1f0c1-start-propagation -> S1f0c2-cohort-terminal -> S1f0d-owner-v2
                                                                                       |
                                                                                       v
                                                                              S1f1-reader-result
```

S1f0b and S1f0c0 are dependency-ready and have disjoint primary write sets. Their reviews may run asynchronously.
S1f0d is the first batch allowed to claim a product-eligible owner candidate. S1f1 is the first batch allowed to
prepare a projection terminal result. `served=0`, provider/model/live=0, and R-019/R-029 remain unchanged.
