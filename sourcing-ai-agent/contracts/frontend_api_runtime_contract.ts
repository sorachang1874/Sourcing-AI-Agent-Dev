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

export interface WorkflowPublicFinalJsonSnapshot extends WorkflowPublicFinalJsonFootprint {
  /**
   * Fresh, frozen, accessor-free JSON value.  Callers must serialize this value rather than the
   * descriptor source from which it was captured.
   */
  readonly value: unknown;
}

interface WorkflowPublicCapturedJsonValue extends WorkflowPublicFinalJsonFootprint {
  readonly maxRelativeDepth: number;
  readonly value: unknown;
}

interface WorkflowPublicJsonCaptureContext {
  readonly activeContainers: WeakSet<object>;
  readonly memo: WeakMap<object, WorkflowPublicCapturedJsonValue>;
}

const WORKFLOW_PUBLIC_JSON_OMITTED = Symbol("workflow-public-json-omitted");

type WorkflowPublicJsonCapture =
  | WorkflowPublicCapturedJsonValue
  | typeof WORKFLOW_PUBLIC_JSON_OMITTED
  | undefined;

function workflowPublicMeasuredJsonFootprintFits(
  footprint: WorkflowPublicFinalJsonFootprint,
): boolean {
  return footprint.nodes <= WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxNodes &&
    footprint.bytes <= WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxOccurrenceBytes;
}

function defineWorkflowPublicSnapshotField(
  target: object,
  key: string,
  value: unknown,
  enumerable: boolean,
): void {
  Object.defineProperty(target, key, {
    value,
    enumerable,
    configurable: false,
    writable: false,
  });
}

function sealWorkflowPublicSnapshotContainer(value: object): void {
  // JSON.stringify performs [[Get]]("toJSON") even when the source did not own the key.  A safe
  // own shadow keeps the captured value independent from later Object/Array prototype pollution.
  defineWorkflowPublicSnapshotField(value, "toJSON", undefined, false);
  Object.freeze(value);
}

function captureWorkflowPublicFinalJsonValue(
  value: unknown,
  context: WorkflowPublicJsonCaptureContext,
  depth: number,
  containerKind: "root" | "object" | "array",
  preserveCompatibilityRaw: boolean,
): WorkflowPublicJsonCapture {
  if (depth > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxDepth) {
    return undefined;
  }
  // Functions are never treated as JSON omission/null. JSON.stringify looks up and may execute a
  // callable's own or inherited toJSON before applying those normal omission rules.
  if (typeof value === "function") {
    return undefined;
  }
  if (value === undefined || typeof value === "symbol") {
    return containerKind === "array"
      ? { nodes: 1, bytes: 4, maxRelativeDepth: 0, value: null }
      : WORKFLOW_PUBLIC_JSON_OMITTED;
  }
  if (value === null) {
    return { nodes: 1, bytes: 4, maxRelativeDepth: 0, value: null };
  }
  if (typeof value === "string") {
    if (workflowPublicUtf8ByteLength(value) > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxStringBytes) {
      return undefined;
    }
    return {
      nodes: 1,
      bytes: workflowPublicJsonStringByteLength(value),
      maxRelativeDepth: 0,
      value,
    };
  }
  if (typeof value === "boolean") {
    return { nodes: 1, bytes: value ? 4 : 5, maxRelativeDepth: 0, value };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return undefined;
    }
    return {
      nodes: 1,
      bytes: workflowPublicUtf8ByteLength(Object.is(value, -0) ? "0" : String(value)),
      maxRelativeDepth: 0,
      value,
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

  context.activeContainers.add(value);
  try {
    let captured: WorkflowPublicCapturedJsonValue;
    if (Array.isArray(value)) {
      let ownToJsonDescriptor: PropertyDescriptor | undefined;
      let compatibilityRawDescriptor: PropertyDescriptor | undefined;
      let lengthDescriptor: PropertyDescriptor | undefined;
      try {
        ownToJsonDescriptor = Object.getOwnPropertyDescriptor(value, "toJSON");
        if (preserveCompatibilityRaw) {
          compatibilityRawDescriptor = Object.getOwnPropertyDescriptor(value, "raw");
        }
        lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length");
      } catch {
        return undefined;
      }
      if (
        ownToJsonDescriptor &&
        (ownToJsonDescriptor.enumerable ||
          !("value" in ownToJsonDescriptor) ||
          ownToJsonDescriptor.value !== undefined)
      ) {
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
      const snapshot: unknown[] = [];
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
        const item = captureWorkflowPublicFinalJsonValue(
          descriptor && "value" in descriptor ? descriptor.value : undefined,
          context,
          depth + 1,
          "array",
          preserveCompatibilityRaw,
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
        defineWorkflowPublicSnapshotField(snapshot, String(index), item.value, true);
      }
      if (compatibilityRawDescriptor) {
        if (
          compatibilityRawDescriptor.enumerable ||
          !("value" in compatibilityRawDescriptor)
        ) {
          return undefined;
        }
        defineWorkflowPublicSnapshotField(
          snapshot,
          "raw",
          compatibilityRawDescriptor.value,
          false,
        );
      }
      sealWorkflowPublicSnapshotContainer(snapshot);
      captured = { nodes, bytes, maxRelativeDepth, value: snapshot };
    } else {
      let ownKeys: readonly PropertyKey[];
      try {
        ownKeys = Reflect.ownKeys(value);
      } catch {
        return undefined;
      }
      // At most two keys may sit outside the ordinary collection cap, and those keys are
      // admitted below only when they are the exact non-enumerable compatibility fields owned by
      // this serializer (`toJSON` and, for demo DTOs, `raw`).  Do not let the +2 allowance become
      // an implicit 258-entry JSON collection limit.
      if (ownKeys.length > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxCollectionEntries + 2) {
        return undefined;
      }
      let nodes = 1;
      let bytes = 2;
      let ordinaryOwnKeyCount = 0;
      let serializedEntryCount = 0;
      let maxRelativeDepth = 0;
      const snapshot: Record<string, unknown> = {};
      let compatibilityRaw: unknown | undefined;
      for (const key of ownKeys) {
        if (typeof key !== "string") {
          ordinaryOwnKeyCount += 1;
          if (ordinaryOwnKeyCount > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxCollectionEntries) {
            return undefined;
          }
          continue;
        }
        let descriptor: PropertyDescriptor | undefined;
        try {
          descriptor = Object.getOwnPropertyDescriptor(value, key);
        } catch {
          return undefined;
        }
        if (!descriptor) {
          return undefined;
        }
        if (key === "toJSON") {
          if (
            descriptor.enumerable ||
            !("value" in descriptor) ||
            descriptor.value !== undefined
          ) {
            return undefined;
          }
          continue;
        }
        if (key === "raw" && !descriptor.enumerable && preserveCompatibilityRaw) {
          if (!("value" in descriptor)) {
            return undefined;
          }
          compatibilityRaw = descriptor.value;
          continue;
        }
        ordinaryOwnKeyCount += 1;
        if (ordinaryOwnKeyCount > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxCollectionEntries) {
          return undefined;
        }
        if (!descriptor.enumerable) {
          continue;
        }
        if (!("value" in descriptor)) {
          return undefined;
        }
        if (workflowPublicUtf8ByteLength(key) > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxKeyBytes) {
          return undefined;
        }
        const item = captureWorkflowPublicFinalJsonValue(
          descriptor.value,
          context,
          depth + 1,
          "object",
          preserveCompatibilityRaw,
        );
        if (!item) {
          return undefined;
        }
        if (item === WORKFLOW_PUBLIC_JSON_OMITTED) {
          continue;
        }
        if (serializedEntryCount >= WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxCollectionEntries) {
          return undefined;
        }
        nodes += item.nodes;
        bytes += workflowPublicJsonStringByteLength(key) + 1 + item.bytes +
          (serializedEntryCount > 0 ? 1 : 0);
        serializedEntryCount += 1;
        maxRelativeDepth = Math.max(maxRelativeDepth, item.maxRelativeDepth + 1);
        if (!workflowPublicMeasuredJsonFootprintFits({ nodes, bytes })) {
          return undefined;
        }
        defineWorkflowPublicSnapshotField(snapshot, key, item.value, true);
      }
      if (compatibilityRaw !== undefined) {
        defineWorkflowPublicSnapshotField(snapshot, "raw", compatibilityRaw, false);
      }
      sealWorkflowPublicSnapshotContainer(snapshot);
      captured = { nodes, bytes, maxRelativeDepth, value: snapshot };
    }
    context.memo.set(value, captured);
    return captured;
  } finally {
    context.activeContainers.delete(value);
  }
}

/**
 * Captures and measures a public DTO without invoking getters, Proxy `get` traps, or `toJSON`.
 *
 * The source is consulted only through one descriptor pass.  The returned value is a fresh,
 * accessor-free ordinary-object/dense-array graph with a safe own `toJSON` shadow, and is the only
 * value callers may serialize.  Non-enumerable `raw` compatibility views remain non-enumerable
 * references and do not contribute to the JSON footprint; they are outside this JSON seal.
 */
export function captureWorkflowPublicFinalJsonSnapshot(
  value: unknown,
): WorkflowPublicFinalJsonSnapshot | undefined {
  const captured = captureWorkflowPublicFinalJsonValue(
    value,
    {
      activeContainers: new WeakSet<object>(),
      memo: new WeakMap<object, WorkflowPublicCapturedJsonValue>(),
    },
    0,
    "root",
    true,
  );
  return captured && captured !== WORKFLOW_PUBLIC_JSON_OMITTED
    ? Object.freeze({
        value: captured.value,
        nodes: captured.nodes,
        bytes: captured.bytes,
      })
    : undefined;
}
