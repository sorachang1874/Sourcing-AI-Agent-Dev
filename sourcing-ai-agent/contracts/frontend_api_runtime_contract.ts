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

export interface WorkflowPublicFinalJsonFootprint {
  readonly nodes: number;
  readonly bytes: number;
}

interface WorkflowPublicMeasuredJsonFootprint extends WorkflowPublicFinalJsonFootprint {
  readonly maxRelativeDepth: number;
}

interface WorkflowPublicJsonMeasurementContext {
  readonly activeContainers: WeakSet<object>;
  readonly memo: WeakMap<object, WorkflowPublicMeasuredJsonFootprint>;
}

const WORKFLOW_PUBLIC_JSON_OMITTED = Symbol("workflow-public-json-omitted");

type WorkflowPublicJsonMeasurement =
  | WorkflowPublicMeasuredJsonFootprint
  | typeof WORKFLOW_PUBLIC_JSON_OMITTED
  | undefined;

function workflowPublicMeasuredJsonFootprintFits(
  footprint: WorkflowPublicFinalJsonFootprint,
): boolean {
  return footprint.nodes <= WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxNodes &&
    footprint.bytes <= WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxOccurrenceBytes;
}

function measureWorkflowPublicFinalJsonValue(
  value: unknown,
  context: WorkflowPublicJsonMeasurementContext,
  depth: number,
  containerKind: "root" | "object" | "array",
): WorkflowPublicJsonMeasurement {
  if (depth > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxDepth) {
    return undefined;
  }
  if (value === undefined || typeof value === "function" || typeof value === "symbol") {
    return containerKind === "array"
      ? { nodes: 1, bytes: 4, maxRelativeDepth: 0 }
      : WORKFLOW_PUBLIC_JSON_OMITTED;
  }
  if (value === null) {
    return { nodes: 1, bytes: 4, maxRelativeDepth: 0 };
  }
  if (typeof value === "string") {
    if (workflowPublicUtf8ByteLength(value) > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxStringBytes) {
      return undefined;
    }
    return {
      nodes: 1,
      bytes: workflowPublicJsonStringByteLength(value),
      maxRelativeDepth: 0,
    };
  }
  if (typeof value === "boolean") {
    return { nodes: 1, bytes: value ? 4 : 5, maxRelativeDepth: 0 };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return undefined;
    }
    return {
      nodes: 1,
      bytes: workflowPublicUtf8ByteLength(Object.is(value, -0) ? "0" : String(value)),
      maxRelativeDepth: 0,
    };
  }
  if (typeof value === "bigint" || !value || typeof value !== "object") {
    return undefined;
  }
  if (context.activeContainers.has(value)) {
    return undefined;
  }
  const memoized = context.memo.get(value);
  if (memoized) {
    return depth + memoized.maxRelativeDepth <= WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxDepth
      ? memoized
      : undefined;
  }

  let ownKeys: readonly PropertyKey[];
  try {
    ownKeys = Reflect.ownKeys(value);
  } catch {
    return undefined;
  }
  if (ownKeys.length > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxCollectionEntries + 1) {
    return undefined;
  }

  context.activeContainers.add(value);
  try {
    let measured: WorkflowPublicMeasuredJsonFootprint;
    if (Array.isArray(value)) {
      let lengthDescriptor: PropertyDescriptor | undefined;
      try {
        lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length");
      } catch {
        return undefined;
      }
      if (
        !lengthDescriptor ||
        !("value" in lengthDescriptor) ||
        !Number.isSafeInteger(lengthDescriptor.value) ||
        lengthDescriptor.value < 0 ||
        lengthDescriptor.value > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxCollectionEntries
      ) {
        return undefined;
      }
      let nodes = 1;
      let bytes = 2;
      let maxRelativeDepth = 0;
      for (let index = 0; index < lengthDescriptor.value; index += 1) {
        let descriptor: PropertyDescriptor | undefined;
        try {
          descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        } catch {
          return undefined;
        }
        if (descriptor && !("value" in descriptor)) {
          return undefined;
        }
        const item = measureWorkflowPublicFinalJsonValue(
          descriptor && "value" in descriptor ? descriptor.value : undefined,
          context,
          depth + 1,
          "array",
        );
        if (!item || item === WORKFLOW_PUBLIC_JSON_OMITTED) {
          return undefined;
        }
        nodes += item.nodes;
        bytes += item.bytes + (index > 0 ? 1 : 0);
        maxRelativeDepth = Math.max(maxRelativeDepth, item.maxRelativeDepth + 1);
        if (!workflowPublicMeasuredJsonFootprintFits({ nodes, bytes })) {
          return undefined;
        }
      }
      measured = { nodes, bytes, maxRelativeDepth };
    } else {
      let prototype: object | null;
      try {
        prototype = Object.getPrototypeOf(value);
      } catch {
        return undefined;
      }
      if (prototype !== Object.prototype && prototype !== null) {
        return undefined;
      }
      let nodes = 1;
      let bytes = 2;
      let serializedEntryCount = 0;
      let maxRelativeDepth = 0;
      for (const key of ownKeys) {
        if (typeof key !== "string") {
          continue;
        }
        let descriptor: PropertyDescriptor | undefined;
        try {
          descriptor = Object.getOwnPropertyDescriptor(value, key);
        } catch {
          return undefined;
        }
        if (!descriptor || !descriptor.enumerable) {
          continue;
        }
        if (!("value" in descriptor) || key === "toJSON") {
          return undefined;
        }
        if (workflowPublicUtf8ByteLength(key) > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxKeyBytes) {
          return undefined;
        }
        const item = measureWorkflowPublicFinalJsonValue(
          descriptor.value,
          context,
          depth + 1,
          "object",
        );
        if (!item) {
          return undefined;
        }
        if (item === WORKFLOW_PUBLIC_JSON_OMITTED) {
          continue;
        }
        nodes += item.nodes;
        bytes += workflowPublicJsonStringByteLength(key) + 1 + item.bytes +
          (serializedEntryCount > 0 ? 1 : 0);
        serializedEntryCount += 1;
        maxRelativeDepth = Math.max(maxRelativeDepth, item.maxRelativeDepth + 1);
        if (!workflowPublicMeasuredJsonFootprintFits({ nodes, bytes })) {
          return undefined;
        }
      }
      measured = { nodes, bytes, maxRelativeDepth };
    }
    context.memo.set(value, measured);
    return measured;
  } finally {
    context.activeContainers.delete(value);
  }
}

/**
 * Measures the JSON-visible shape of a locally constructed public DTO without invoking getters,
 * Proxy `get` traps, or `toJSON`. Non-enumerable compatibility properties such as `raw` are ignored.
 */
export function workflowPublicFinalJsonFootprint(
  value: unknown,
): WorkflowPublicFinalJsonFootprint | undefined {
  const measured = measureWorkflowPublicFinalJsonValue(
    value,
    {
      activeContainers: new WeakSet<object>(),
      memo: new WeakMap<object, WorkflowPublicMeasuredJsonFootprint>(),
    },
    0,
    "root",
  );
  return measured && measured !== WORKFLOW_PUBLIC_JSON_OMITTED
    ? { nodes: measured.nodes, bytes: measured.bytes }
    : undefined;
}
