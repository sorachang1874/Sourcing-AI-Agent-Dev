import type {
  CohortLocationSelection,
  CohortSelection,
  CohortSelectionOption,
  CohortSelectionOptions,
} from "../types";

const COHORT_SELECTION_KEYS = new Set([
  "schema_version",
  "role_bucket_ids",
  "employment_statuses",
  "role_match",
  "source",
]);
const COHORT_SELECTION_SCHEMA_VERSION = "cohort_selection.v1";

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function requiredString(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`Cohort options response has an invalid ${key}.`);
  }
  return value.trim();
}

function parseOptionList(value: unknown, field: string, allowEmpty = false): CohortSelectionOption[] {
  if (!Array.isArray(value) || (!allowEmpty && value.length === 0)) {
    throw new Error(`Cohort options response has an invalid ${field}.`);
  }
  const seenIds = new Set<string>();
  const options = value.map((item) => {
    if (!isRecord(item)) {
      throw new Error(`Cohort options response has an invalid ${field} item.`);
    }
    const id = requiredString(item, "id");
    const label = requiredString(item, "label");
    const order = item.order;
    if (!Number.isInteger(order)) {
      throw new Error(`Cohort options response has an invalid ${field} order.`);
    }
    if (seenIds.has(id)) {
      throw new Error(`Cohort options response has a duplicate ${field} id.`);
    }
    seenIds.add(id);
    return { id, label, order: Number(order) };
  });
  return options.sort((left, right) => left.order - right.order || left.id.localeCompare(right.id));
}

export function parseCohortSelectionOptionsPayload(payload: unknown): CohortSelectionOptions {
  if (!isRecord(payload)) {
    throw new Error("Cohort options response must be an object.");
  }
  const roleBuckets = parseOptionList(payload.role_buckets, "role_buckets", true);
  const employmentStatuses = parseOptionList(payload.employment_statuses, "employment_statuses");
  const roleMatchOptions = parseOptionList(payload.role_match_options, "role_match_options");
  if (!isRecord(payload.defaults)) {
    throw new Error("Cohort options response has invalid defaults.");
  }
  const defaultRoleMatch = requiredString(payload.defaults, "role_match");
  if (!roleMatchOptions.some((option) => option.id === defaultRoleMatch)) {
    throw new Error("Cohort options response default role_match is not selectable.");
  }
  const schemaVersion = requiredString(payload, "schema_version");
  if (schemaVersion !== COHORT_SELECTION_SCHEMA_VERSION) {
    throw new Error("Cohort options response uses an unsupported schema_version.");
  }
  return {
    schemaVersion,
    registryVersion: requiredString(payload, "registry_version"),
    registryDigest: requiredString(payload, "registry_digest"),
    roleBuckets,
    employmentStatuses,
    roleMatchOptions,
    defaultRoleMatch,
  };
}

function parseStringArray(record: Record<string, unknown>, key: string, allowEmpty: boolean): string[] {
  const value = record[key];
  if (!Array.isArray(value) || (!allowEmpty && value.length === 0)) {
    throw new Error(`Cohort selection has an invalid ${key}.`);
  }
  const normalized = value.map((item) => {
    if (typeof item !== "string" || !item.trim()) {
      throw new Error(`Cohort selection has an invalid ${key} value.`);
    }
    return item.trim();
  });
  if (new Set(normalized).size !== normalized.length) {
    throw new Error(`Cohort selection has duplicate ${key} values.`);
  }
  return normalized;
}

export function parseCohortSelectionPayload(value: unknown): CohortSelection {
  if (!isRecord(value)) {
    throw new Error("Cohort selection must be an object.");
  }
  const unknownKeys = Object.keys(value).filter((key) => !COHORT_SELECTION_KEYS.has(key));
  if (unknownKeys.length > 0 || Object.keys(value).length !== COHORT_SELECTION_KEYS.size) {
    throw new Error("Cohort selection fields do not match the v1 contract.");
  }
  const source = requiredString(value, "source");
  if (source !== "user_explicit") {
    throw new Error("Frontend cohort selection must be user_explicit.");
  }
  const schemaVersion = requiredString(value, "schema_version");
  if (schemaVersion !== COHORT_SELECTION_SCHEMA_VERSION) {
    throw new Error("Cohort selection uses an unsupported schema_version.");
  }
  return {
    schema_version: schemaVersion,
    role_bucket_ids: parseStringArray(value, "role_bucket_ids", true),
    employment_statuses: parseStringArray(value, "employment_statuses", false),
    role_match: requiredString(value, "role_match"),
    source,
  };
}

export function cloneCohortSelection(value: CohortSelection): CohortSelection {
  return {
    schema_version: value.schema_version,
    role_bucket_ids: [...value.role_bucket_ids],
    employment_statuses: [...value.employment_statuses],
    role_match: value.role_match,
    source: "user_explicit",
  };
}

export function createDefaultCohortSelection(options: CohortSelectionOptions): CohortSelection {
  if (options.employmentStatuses.length === 0) {
    throw new Error("At least one employment status is required to enable cohort selection.");
  }
  return {
    schema_version: options.schemaVersion,
    role_bucket_ids: [],
    employment_statuses: options.employmentStatuses.map((option) => option.id),
    role_match: options.defaultRoleMatch,
    source: "user_explicit",
  };
}

export function toggleOrderedOption(
  selectedIds: string[],
  optionId: string,
  checked: boolean,
  options: CohortSelectionOption[],
): string[] {
  const selected = new Set(selectedIds);
  if (checked) {
    selected.add(optionId);
  } else {
    selected.delete(optionId);
  }
  return options.filter((option) => selected.has(option.id)).map((option) => option.id);
}

export function equalCohortSelection(left: CohortSelection, right: CohortSelection): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

export function summarizeCohortSelection(
  selection: CohortSelection,
  options: CohortSelectionOptions | null,
): string {
  const roleLabels = selection.role_bucket_ids.length === 0
    ? ["All roles"]
    : selection.role_bucket_ids.map(
        (id) => options?.roleBuckets.find((option) => option.id === id)?.label || id,
      );
  const statusLabels = selection.employment_statuses.map(
    (id) => options?.employmentStatuses.find((option) => option.id === id)?.label || id,
  );
  const matchLabel =
    options?.roleMatchOptions.find((option) => option.id === selection.role_match)?.label
    || selection.role_match;
  return `${roleLabels.join(", ")} · ${statusLabels.join(", ")} · ${matchLabel}`;
}

/**
 * Location targeting (FT0 D7): the `target_locations` /
 * `exclude_target_locations` request fields are siblings of the closed
 * cohort_selection.v1 object, never members of it. The v1 parser above stays
 * byte-compatible; everything location-related lives below this line.
 */

/** Server-side default injected for explicit-Cohort requests without the field. */
export const DEFAULT_TARGET_LOCATIONS = ["United States"];

export function createDefaultCohortLocationSelection(): CohortLocationSelection {
  return {
    targetLocations: [...DEFAULT_TARGET_LOCATIONS],
    excludeTargetLocations: [],
  };
}

/**
 * Trim, collapse inner whitespace, dedupe, and preserve order (FT0 §7.2).
 * Absent (null/undefined) normalizes to the empty list; wrong shapes fail
 * closed, mirroring the backend ingress posture.
 */
export function normalizeLocationList(value: unknown, field: string): string[] {
  if (value === undefined || value === null) {
    return [];
  }
  if (!Array.isArray(value)) {
    throw new Error(`Location field ${field} must be a list of names.`);
  }
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string" || !item.trim()) {
      throw new Error(`Location field ${field} must contain non-empty names.`);
    }
    const name = item.trim().replace(/\s+/g, " ");
    if (!seen.has(name)) {
      seen.add(name);
      normalized.push(name);
    }
  }
  return normalized;
}

export function cloneCohortLocationSelection(value: CohortLocationSelection): CohortLocationSelection {
  return {
    targetLocations: [...value.targetLocations],
    excludeTargetLocations: [...value.excludeTargetLocations],
  };
}

export function equalCohortLocationSelection(
  left: CohortLocationSelection,
  right: CohortLocationSelection,
): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

/**
 * Read the sibling location fields from one request mirror record. Returns
 * null when the record carries neither key (legacy requests); invalid shapes
 * fail closed.
 */
export function parseCohortLocationMirror(record: Record<string, unknown>): CohortLocationSelection | null {
  const hasTarget = Object.prototype.hasOwnProperty.call(record, "target_locations");
  const hasExclude = Object.prototype.hasOwnProperty.call(record, "exclude_target_locations");
  if (!hasTarget && !hasExclude) {
    return null;
  }
  return {
    targetLocations: normalizeLocationList(record.target_locations, "target_locations"),
    excludeTargetLocations: normalizeLocationList(record.exclude_target_locations, "exclude_target_locations"),
  };
}

/**
 * Build the request-payload fragment for the location fields. Empty lists are
 * omitted: an absent `target_locations` means the server default
 * (["United States"] for explicit-Cohort requests) applies.
 */
export function buildCohortLocationApiPayload(
  targetLocations?: string[],
  excludeTargetLocations?: string[],
): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  const target = normalizeLocationList(targetLocations, "target_locations");
  const exclude = normalizeLocationList(excludeTargetLocations, "exclude_target_locations");
  if (target.length > 0) {
    payload.target_locations = target;
  }
  if (exclude.length > 0) {
    payload.exclude_target_locations = exclude;
  }
  return payload;
}

export function appendLocationValue(list: string[], value: string): string[] {
  const name = value.trim().replace(/\s+/g, " ");
  if (!name || list.includes(name)) {
    return list;
  }
  return [...list, name];
}

export function removeLocationValue(list: string[], value: string): string[] {
  return list.filter((item) => item !== value);
}

export function summarizeCohortLocations(
  targetLocations: string[] | undefined,
  excludeTargetLocations: string[] | undefined,
): string {
  const targetLabel =
    targetLocations && targetLocations.length > 0
      ? targetLocations.join("、")
      : `${DEFAULT_TARGET_LOCATIONS[0]}（默认）`;
  const excludeLabel =
    excludeTargetLocations && excludeTargetLocations.length > 0
      ? ` · 排除: ${excludeTargetLocations.join("、")}`
      : "";
  return `目标地区: ${targetLabel}${excludeLabel}`;
}

export interface CohortShardPreviewItem {
  shardId: string;
  statusId: string;
  statusLabel: string;
  roleId: string | null;
  roleLabel: string;
}

export interface CohortShardPreview {
  /** Planned shard count: S * max(1, R) (FT0 §8.1). */
  shardCount: number;
  shards: CohortShardPreviewItem[];
  /** Roles empty + every server-provided status selected => full-population recall. */
  isFullRecall: boolean;
}

/**
 * Client-side preview of the compiler's shard expansion. The
 * CohortProviderCompiler remains the SOLE expansion layer; this projection
 * only explains the pending selection before confirmation.
 */
export function buildCohortShardPreview(
  selection: CohortSelection,
  options: CohortSelectionOptions,
): CohortShardPreview {
  const selectedStatuses = options.employmentStatuses.filter((option) =>
    selection.employment_statuses.includes(option.id),
  );
  const selectedRoles = options.roleBuckets.filter((option) =>
    selection.role_bucket_ids.includes(option.id),
  );
  const effectiveRoles: Array<{ id: string | null; label: string }> =
    selectedRoles.length > 0
      ? selectedRoles.map((option) => ({ id: option.id, label: option.label }))
      : [{ id: null, label: "All roles" }];
  const shards: CohortShardPreviewItem[] = [];
  for (const status of selectedStatuses) {
    for (const role of effectiveRoles) {
      shards.push({
        shardId: `${status.id}:${role.id || "all_roles"}`,
        statusId: status.id,
        statusLabel: status.label,
        roleId: role.id,
        roleLabel: role.label,
      });
    }
  }
  const allStatusesSelected =
    options.employmentStatuses.length > 0 &&
    options.employmentStatuses.every((option) => selection.employment_statuses.includes(option.id));
  return {
    shardCount: shards.length,
    shards,
    isFullRecall: selection.role_bucket_ids.length === 0 && allStatusesSelected,
  };
}
