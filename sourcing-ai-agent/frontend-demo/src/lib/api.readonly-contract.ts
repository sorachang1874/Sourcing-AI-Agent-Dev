import type {
  OperationRunProvenance,
  OperationRunRecord,
  WorkflowActivityRecord,
  listOperationRuns,
  listWorkflowActivities,
} from "./api";

// This file is a compile-time contract test. The public adapter returns deeply frozen snapshots,
// so its exported TypeScript surface must reject the same mutations before they reach runtime.
declare const operationRun: OperationRunRecord;
declare const provenance: OperationRunProvenance;
declare const activity: WorkflowActivityRecord;
declare const operationRuns: Awaited<ReturnType<typeof listOperationRuns>>;
declare const activities: Awaited<ReturnType<typeof listWorkflowActivities>>;
declare const ordinaryUnknownValue: unknown;

// @ts-expect-error Public DTO roots are immutable snapshots.
operationRun.status = "cancelled";
// @ts-expect-error Nested DTO records are immutable snapshots.
operationRun.progress.phase = "running";
// @ts-expect-error Compatibility raw records are immutable references.
operationRun.raw.status = "forged";
// @ts-expect-error Endpoint list results are readonly.
operationRuns.push(operationRun);
// @ts-expect-error Nested endpoint lists are readonly.
provenance.workflowCommands.splice(0, 1);
// @ts-expect-error JSON array members inside DTOs are readonly.
activity.artifactRefs.push("forged");
// @ts-expect-error Every list endpoint exposes a readonly result.
activities[0] = activity;

const nestedProgress = operationRun.progress.nested;
if (
  nestedProgress !== null &&
  typeof nestedProgress === "object" &&
  !Array.isArray(nestedProgress)
) {
  // @ts-expect-error Ordinary-object narrowing must preserve second-level immutability.
  nestedProgress.status = "forged";
}
if (Array.isArray(nestedProgress)) {
  // @ts-expect-error Array.isArray narrowing must preserve readonly indices.
  nestedProgress[0] = "forged";
  // @ts-expect-error Array.isArray narrowing must not restore mutating methods.
  nestedProgress.push("forged");
  // @ts-expect-error Array.isArray narrowing must not restore structural mutation.
  nestedProgress.splice(0, 1);
}

const optionalNestedControl = operationRun.controlState?.raw.nested;
if (Array.isArray(optionalNestedControl)) {
  // @ts-expect-error Optional JSON narrowing must preserve readonly indices.
  optionalNestedControl[0] = "forged";
  // @ts-expect-error Optional JSON narrowing must preserve readonly length.
  optionalNestedControl.length = 0;
  // @ts-expect-error Optional JSON narrowing must not restore mutating methods.
  optionalNestedControl.push("forged");
  // @ts-expect-error Optional JSON narrowing must not restore structural mutation.
  optionalNestedControl.splice(0, 1);
}

if (Array.isArray(operationRuns)) {
  // @ts-expect-error Endpoint DTO narrowing must preserve readonly indices.
  operationRuns[0] = operationRun;
  // @ts-expect-error Endpoint DTO narrowing must preserve readonly length.
  operationRuns.length = 0;
  // @ts-expect-error Endpoint DTO narrowing must not restore mutating methods.
  operationRuns.push(operationRun);
  // @ts-expect-error Endpoint DTO narrowing must not restore structural mutation.
  operationRuns.splice(0, 1);
}

if (Array.isArray(provenance.workflowCommands)) {
  // @ts-expect-error Provenance DTO narrowing must preserve readonly indices.
  provenance.workflowCommands[0] = provenance.workflowCommands[0];
  // @ts-expect-error Provenance DTO narrowing must preserve readonly length.
  provenance.workflowCommands.length = 0;
  // @ts-expect-error Provenance DTO narrowing must not restore mutating methods.
  provenance.workflowCommands.push(provenance.workflowCommands[0]);
  // @ts-expect-error Provenance DTO narrowing must not restore structural mutation.
  provenance.workflowCommands.splice(0, 1);
}

declare const controlPolicy: NonNullable<
  (typeof provenance.workflowCommands)[number]["controlPolicy"]
>;

if (Array.isArray(controlPolicy.providerAfterStartControlUpgradeRequirements)) {
  // @ts-expect-error Control-upgrade requirements must preserve readonly indices.
  controlPolicy.providerAfterStartControlUpgradeRequirements[0] = "forged";
  // @ts-expect-error Control-upgrade requirements must preserve readonly length.
  controlPolicy.providerAfterStartControlUpgradeRequirements.length = 0;
  // @ts-expect-error Control-upgrade requirements must not restore mutating methods.
  controlPolicy.providerAfterStartControlUpgradeRequirements.push("forged");
  // @ts-expect-error Control-upgrade requirements must not restore structural mutation.
  controlPolicy.providerAfterStartControlUpgradeRequirements.splice(0, 1);
}

if (Array.isArray(controlPolicy.runningControlCategories)) {
  // @ts-expect-error Running-control categories must preserve readonly indices.
  controlPolicy.runningControlCategories[0] = "forged";
  // @ts-expect-error Running-control categories must preserve readonly length.
  controlPolicy.runningControlCategories.length = 0;
  // @ts-expect-error Running-control categories must not restore mutating methods.
  controlPolicy.runningControlCategories.push("forged");
  // @ts-expect-error Running-control categories must not restore structural mutation.
  controlPolicy.runningControlCategories.splice(0, 1);
}

declare const controlState: NonNullable<OperationRunRecord["controlState"]>;
if (Array.isArray(controlState.allowedActions)) {
  // @ts-expect-error Allowed actions must preserve readonly indices.
  controlState.allowedActions[0] = "forged";
  // @ts-expect-error Allowed actions must preserve readonly length.
  controlState.allowedActions.length = 0;
  // @ts-expect-error Allowed actions must not restore mutating methods.
  controlState.allowedActions.push("forged");
  // @ts-expect-error Allowed actions must not restore structural mutation.
  controlState.allowedActions.splice(0, 1);
}

if (Array.isArray(ordinaryUnknownValue)) {
  // The constrained overload must not change ordinary mutable Array.isArray narrowing.
  ordinaryUnknownValue[0] = "mutable";
  ordinaryUnknownValue.length = 1;
  ordinaryUnknownValue.push("mutable");
  ordinaryUnknownValue.splice(0, 1);
}

export {};
