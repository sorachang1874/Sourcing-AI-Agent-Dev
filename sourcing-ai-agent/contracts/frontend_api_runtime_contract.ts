export const OPERATION_ACTION_QUERY_SUCCESS_STATUSES = ["ok"] as const;
export const OPERATION_ACTION_SUBMIT_APPLIED_OUTCOMES = [
  "queued",
  "approval_required",
] as const;
export const OPERATION_ACTION_DECISION_APPLIED_OUTCOMES = {
  approve: ["queued"],
  reject: ["rejected"],
} as const;

export const OPERATION_ACTION_DETAIL_SUCCESS_STATUSES = [
  ...OPERATION_ACTION_QUERY_SUCCESS_STATUSES,
  ...OPERATION_ACTION_SUBMIT_APPLIED_OUTCOMES,
  ...OPERATION_ACTION_DECISION_APPLIED_OUTCOMES.reject,
] as const;
export type OperationActionDetailSuccessStatus =
  (typeof OPERATION_ACTION_DETAIL_SUCCESS_STATUSES)[number];
export type OperationActionDecision = keyof typeof OPERATION_ACTION_DECISION_APPLIED_OUTCOMES;
export type OperationActionDecisionAppliedOutcome =
  (typeof OPERATION_ACTION_DECISION_APPLIED_OUTCOMES)[OperationActionDecision][number];

export const OPERATION_RUN_PROVENANCE_SUCCESS_STATUSES = ["ok"] as const;
export type OperationRunProvenanceSuccessStatus =
  (typeof OPERATION_RUN_PROVENANCE_SUCCESS_STATUSES)[number];

export const OPERATION_RUN_CONTROL_APPLIED_OUTCOMES = {
  cancel: ["cancelled"],
  retry: ["queued"],
  resume: ["queued"],
  dispatch: ["planned"],
} as const;
export type OperationRunControlAction = keyof typeof OPERATION_RUN_CONTROL_APPLIED_OUTCOMES;
export type OperationRunControlAppliedOutcome =
  (typeof OPERATION_RUN_CONTROL_APPLIED_OUTCOMES)[OperationRunControlAction][number];

export const WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES = {
  cancel: ["cancelled"],
  retry: ["queued"],
  resume: ["queued"],
} as const;
export type WorkflowCommandControlAction = keyof typeof WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES;
export type WorkflowCommandControlAppliedOutcome =
  (typeof WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES)[WorkflowCommandControlAction][number];

export const WORKFLOW_PUBLIC_PROJECTION_LIMITS = Object.freeze({
  maxDepth: 32,
  maxNodes: 4096,
  maxCollectionEntries: 256,
});
