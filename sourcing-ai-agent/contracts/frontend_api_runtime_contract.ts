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
  maxKeyBytes: 1024,
  maxStringBytes: 65_536,
  maxOccurrenceBytes: 1_048_576,
  maxTransportBodyBytes: 4_194_304,
});

const WORKFLOW_PUBLIC_UTF8_ENCODER = new TextEncoder();

export function workflowPublicUtf8ByteLength(value: string): number {
  return WORKFLOW_PUBLIC_UTF8_ENCODER.encode(value).byteLength;
}

export function workflowPublicJsonStringByteLength(value: string): number {
  return workflowPublicUtf8ByteLength(JSON.stringify(value));
}

export function workflowPublicTransportContentLengthIsOverLimit(
  contentLength: string | null | undefined,
): boolean {
  if (contentLength === null || contentLength === undefined) {
    return false;
  }
  const normalized = contentLength.trim();
  if (!/^\d+$/.test(normalized)) {
    return false;
  }
  const canonical = normalized.replace(/^0+(?=\d)/, "");
  const limit = String(WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxTransportBodyBytes);
  return canonical.length > limit.length ||
    (canonical.length === limit.length && canonical > limit);
}

export function workflowPublicTransportBodyIsOverLimit(body: string): boolean {
  return workflowPublicUtf8ByteLength(body) > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxTransportBodyBytes;
}
