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

export {};
