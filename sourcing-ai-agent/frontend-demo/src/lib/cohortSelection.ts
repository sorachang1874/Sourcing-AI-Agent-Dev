import type {
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
