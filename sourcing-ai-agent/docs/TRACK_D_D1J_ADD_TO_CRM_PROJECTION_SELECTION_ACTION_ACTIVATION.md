> Status: bounded non-live implementation candidate after D1i. This activates
> `add_to_crm` as the sixth schema-defined production action. The D1j checkpoint
> partition was **6 schema-defined / 9 schema-less / served=0**; D1l later reached its **9/6/0** checkpoint, and the
> D1m candidate now makes the current candidate partition **10 schema-defined / 5 schema-less / served=0**. It does
> not authorize live provider/model calls, product signoff, or closure of R-028/R-029.

# Track D D1j Add-to-CRM Projection Selection Action Activation

## Scope

D1j moves the Operation `add_to_crm` path from schema-less projection selection
inputs to an owner-bound projection selection target:

- caller selector: exactly one `projection_id`, one membership revision alias,
  and one selected candidate key list;
- owner-bound target: server workspace, projection id, membership revision,
  source candidate count, and sorted selected `candidate_identity_keys`;
- closed input: CRM destination parameters only (`pipeline_id`, `stage`,
  `source_reason`).

Authenticated submit mints the target from the canonical serving projection
reader. Dispatch revalidates the persisted request and re-reads the selected
projection members before planning the `crm.record.add_from_projection` command.
The command payload carries the exact owner-bound projection target, and the CRM
writer command owner revalidates the command/action/run binding plus the current
projection snapshot before any CRM record, engagement, event, Activity, Attempt,
or EntityDelta write.

## Residuals

This slice does not close R-028. The existing projection-to-CRM fixed UoW still
lives behind the temporary `ControlPlaneStore.apply_projection_crm_selection`
facade, legacy CRM mutation callers remain outside one shared identity-lock
repository, and command terminal/effect/linked Operation synchronization are not
one global exactly-once transaction.

At the D1j checkpoint, R-029 dropped from 10 to **9** schema-less actions, but it remains open until every
API-submittable action has a reviewed schema/binder decision and the release
window records zero compatibility hits. Served Agent tool population remains
zero.

## Evidence

Author evidence for the local candidate:

- `tests.test_operation_runtime` = **137 passed**;
- D1j targeted nodes = **3 passed**;
- D1 action request surface characterization = **6 passed**.

This is author evidence only. The combined D1j/D1k pinned `354e979` runner-backed advisory completed as
`NO-GO 0/9/7/1`; it is not formal `GO`. Its findings require fixed-forward plus a fresh pinned re-review before any
live/W6/manual/product signoff for this scope.
