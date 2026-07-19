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

/**
 * Byte-exact registry pin validation (FT2 fixed-forward r4, review finding
 * 3): registry version/digest are server identity bytes, never display text.
 * They must NOT be trimmed or otherwise repaired into a match — a padded or
 * mis-shaped pin is invalid pin evidence and fails closed. The documented
 * shapes come from the backend registry owner (`cohort_selection.py`):
 * `registry_version` is a dotted version token (e.g.
 * `cohort_selection.registry.v1`) and `registry_digest` is the sha256 hex of
 * the canonical registry payload.
 */
export const COHORT_REGISTRY_VERSION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
export const COHORT_REGISTRY_DIGEST_PATTERN = /^[0-9a-f]{64}$/;

export function isByteExactRegistryVersion(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value !== "" &&
    value === value.trim() &&
    COHORT_REGISTRY_VERSION_PATTERN.test(value)
  );
}

export function isByteExactRegistryDigest(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value !== "" &&
    value === value.trim() &&
    COHORT_REGISTRY_DIGEST_PATTERN.test(value)
  );
}

function requiredRegistryPinValue(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  const valid =
    key === "registry_version" ? isByteExactRegistryVersion(value) : isByteExactRegistryDigest(value);
  if (!valid) {
    throw new Error(`Cohort options response has an invalid ${key}.`);
  }
  return value as string;
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
    // Pin fields use the dedicated byte-exact validator (never the trimming
    // requiredString): padded/mis-shaped pin bytes are invalid evidence, not
    // a repairable near-match.
    registryVersion: requiredRegistryPinValue(payload, "registry_version"),
    registryDigest: requiredRegistryPinValue(payload, "registry_digest"),
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
  // Unavailable selected ids are preserved inertly (FT2 fixed-forward r4,
  // review finding 5/rerun3 finding 7): toggling one AVAILABLE option must
  // never silently drop selected ids the current server options no longer
  // offer. They keep their original relative order after the available ones
  // until the user explicitly removes them (the unchecked branch above) or
  // replaces the selection.
  const availableOrdered = options.filter((option) => selected.has(option.id)).map((option) => option.id);
  const availableIds = new Set(options.map((option) => option.id));
  const preservedUnavailable = selectedIds.filter((id) => selected.has(id) && !availableIds.has(id));
  return [...availableOrdered, ...preservedUnavailable];
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
 *
 * Presence-aware tri-state, mirroring `domain._normalize_location_list`
 * exactly (FT0 §7.2 / COHORT_SELECTION_CONTRACT.md "Location sibling
 * fields"): absent (`undefined`) selects the server default for explicit
 * Cohort requests, an explicit `[]` opts out of location filtering, and a
 * present `null` is invalid and rejected.
 */

/** Bounded-list convention shared with the backend request normalization. */
export const REQUEST_LOCATION_MAX_ITEMS = 16;
export const REQUEST_LOCATION_ITEM_MAX_LENGTH = 240;

/**
 * Display-only mirror of the backend `acquisition_strategy.DEFAULT_PRIMARY_LOCATION`
 * used to render the absent (server-default) state truthfully. It is NEVER
 * serialized into a payload by default: an absent field stays absent so the
 * server remains the sole writer of the default. A server-owned default
 * metadata channel does not exist on the options endpoint yet; when one
 * lands, this display constant must be replaced by it.
 */
export const SERVER_DEFAULT_TARGET_LOCATION_DISPLAY = "United States";

/**
 * Trim, dedupe (first occurrence wins, case-insensitive), preserve order,
 * and enforce the bounded-list convention — byte-parity with the backend
 * `domain._normalize_location_list` (FT0 §7.2). Absent (`undefined`)
 * normalizes to `undefined`; a present `null`, wrong container, non-string
 * or out-of-bounds items, and over-bound lists fail closed.
 */
export function normalizeLocationList(value: unknown, field: string): string[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (value === null || !Array.isArray(value)) {
    throw new Error(`Location field ${field} must be a list of names.`);
  }
  if (value.length > REQUEST_LOCATION_MAX_ITEMS) {
    throw new Error(
      `Location field ${field} accepts at most ${REQUEST_LOCATION_MAX_ITEMS} items.`,
    );
  }
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string") {
      throw new Error(`Location field ${field} must contain non-empty names.`);
    }
    const name = item.trim();
    if (!name || name.length > REQUEST_LOCATION_ITEM_MAX_LENGTH) {
      throw new Error(
        `Location field ${field} items must be 1-${REQUEST_LOCATION_ITEM_MAX_LENGTH} characters.`,
      );
    }
    const key = name.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      normalized.push(name);
    }
  }
  return normalized;
}

export function createDefaultCohortLocationSelection(): CohortLocationSelection {
  // Absent on both axes: the server default (["United States"] for explicit
  // Cohort requests) applies and the picker renders it as a display seed.
  return {
    targetLocations: undefined,
    excludeTargetLocations: undefined,
  };
}

export function cloneCohortLocationSelection(value: CohortLocationSelection): CohortLocationSelection {
  return {
    ...(value.targetLocations !== undefined
      ? { targetLocations: [...value.targetLocations] }
      : {}),
    ...(value.excludeTargetLocations !== undefined
      ? { excludeTargetLocations: [...value.excludeTargetLocations] }
      : {}),
  };
}

export function equalCohortLocationSelection(
  left: CohortLocationSelection,
  right: CohortLocationSelection,
): boolean {
  return (
    JSON.stringify(left.targetLocations ?? null) === JSON.stringify(right.targetLocations ?? null) &&
    JSON.stringify(left.excludeTargetLocations ?? null) === JSON.stringify(right.excludeTargetLocations ?? null)
  );
}

/**
 * Read the sibling location fields from one request mirror record. Returns
 * null when the record carries neither key (legacy requests); a key present
 * with a `null` value or any invalid shape fails closed.
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
 * Build the request-payload fragment for the location fields, preserving the
 * contract tri-state: `undefined` omits the key (server default applies for
 * explicit Cohort requests), an explicit `[]` serializes as `[]` (opt-out of
 * location filtering), and present values serialize normalized. A present
 * `null` or any invalid shape fails closed.
 */
export function buildCohortLocationApiPayload(
  targetLocations?: string[] | null,
  excludeTargetLocations?: string[] | null,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  const target = normalizeLocationList(targetLocations, "target_locations");
  const exclude = normalizeLocationList(excludeTargetLocations, "exclude_target_locations");
  if (target !== undefined) {
    payload.target_locations = target;
  }
  if (exclude !== undefined) {
    payload.exclude_target_locations = exclude;
  }
  return payload;
}

/**
 * Tagged plan-REVIEW wire contract for the location fields (FT2
 * fixed-forward r4, rerun3 review finding 6). The review-decision payload
 * must distinguish three states per authorized axis:
 * - values / explicit `[]`  -> replace the canonical request axis
 *   (serialized as the normalized list, unchanged);
 * - restored ABSENCE        -> an explicit clear operation
 *   (`{ op: "clear" }`) so a backend application owner can merge the
 *   restore-absence decision into the canonical request instead of reading
 *   an omitted key as "field not part of this decision";
 * - unauthorized / uninitialized axis -> the key is omitted entirely.
 * The backend review gate + application owner for these fields is still
 * pending (see the r4 handoff): until the gate lists a location field as
 * editable the frontend never serializes either operation, so the tagged
 * contract cannot reach the wire unauthorized.
 */
export const LOCATION_REVIEW_CLEAR_OPERATION = "clear";

export function buildLocationReviewClearPayload(): Record<string, string> {
  return { op: LOCATION_REVIEW_CLEAR_OPERATION };
}

/**
 * Location state is REQUEST-OWNED (FT2 fixed-forward r2, review finding 1):
 * the presence-aware selection lives in the request owner (SearchPage for the
 * composer, the review decision for plan review) and is passed explicitly
 * through SearchComposer → SourcingBackendClient → the payload builders.
 * There is deliberately NO module-global draft registry: the round-1 draft
 * was keyed by cohort content, so editing any cohort option silently dropped
 * the user's locations from the real request, and recovered flows had no
 * draft to rehydrate. The payload builders now accept the explicit location
 * arguments only; an absent argument means the server default applies.
 */

export function appendLocationValue(list: string[], value: string): string[] {
  const name = value.trim();
  if (!name || list.includes(name)) {
    return list;
  }
  return [...list, name];
}

export function removeLocationValue(list: string[], value: string): string[] {
  return list.filter((item) => item !== value);
}

/**
 * Render the effective location state. The absent state renders the
 * server-owned default distinctly from an explicit opt-out, so a stored
 * `[]` never collapses into the default display and vice versa.
 */
export function summarizeCohortLocations(
  targetLocations: string[] | undefined,
  excludeTargetLocations: string[] | undefined,
): string {
  const targetLabel =
    targetLocations === undefined
      ? `${SERVER_DEFAULT_TARGET_LOCATION_DISPLAY}（服务端默认）`
      : targetLocations.length === 0
        ? "不限地区（已显式退出地区筛选）"
        : targetLocations.join("、");
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
  /** Planned shard count over the validated selection: S * max(1, R) (FT0 §8.1). */
  shardCount: number;
  shards: CohortShardPreviewItem[];
  /** Roles empty + every server-provided status selected => full-population recall. */
  isFullRecall: boolean;
  /**
   * Selected ids missing from the current server options (registry drift,
   * stale plan, incomplete options response). Surfaced explicitly — never
   * silently dropped — and they invalidate the preview for confirmation.
   */
  unavailableRoleIds: string[];
  unavailableStatusIds: string[];
  /** The selection's role_match is not selectable under the current options. */
  roleMatchUnavailable: boolean;
  /** True whenever any selected value is unavailable; confirmation must block. */
  hasUnavailableSelections: boolean;
  /** Registry pin of the options this preview was computed against. */
  registryVersion: string;
  registryDigest: string;
}

/**
 * Client-side preview of the compiler's shard expansion. The
 * CohortProviderCompiler remains the SOLE expansion layer; this projection
 * only explains the pending selection before confirmation.
 *
 * Membership is validated exactly (FT2 fixed-forward, review finding 5): a
 * selected status/role/role-match missing from the current server options is
 * surfaced as unavailable instead of being silently intersected away, and a
 * nonempty role selection can never collapse into a false "All roles" (or a
 * false full-recall) preview.
 */
export function buildCohortShardPreview(
  selection: CohortSelection,
  options: CohortSelectionOptions,
): CohortShardPreview {
  const unavailableRoleIds = selection.role_bucket_ids.filter(
    (id) => !options.roleBuckets.some((option) => option.id === id),
  );
  const unavailableStatusIds = selection.employment_statuses.filter(
    (id) => !options.employmentStatuses.some((option) => option.id === id),
  );
  const roleMatchUnavailable = !options.roleMatchOptions.some(
    (option) => option.id === selection.role_match,
  );
  const hasUnavailableSelections =
    unavailableRoleIds.length > 0 || unavailableStatusIds.length > 0 || roleMatchUnavailable;

  const selectedStatuses = options.employmentStatuses.filter((option) =>
    selection.employment_statuses.includes(option.id),
  );
  const selectedRoles = options.roleBuckets.filter((option) =>
    selection.role_bucket_ids.includes(option.id),
  );
  // "All roles" is the display for a GENUINELY empty role selection only; a
  // nonempty selection with no available roles yields no shards rather than a
  // fabricated all-roles expansion.
  const effectiveRoles: Array<{ id: string | null; label: string }> =
    selection.role_bucket_ids.length === 0
      ? [{ id: null, label: "All roles" }]
      : selectedRoles.map((option) => ({ id: option.id, label: option.label }));
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
    isFullRecall:
      selection.role_bucket_ids.length === 0 && allStatusesSelected && !hasUnavailableSelections,
    unavailableRoleIds,
    unavailableStatusIds,
    roleMatchUnavailable,
    hasUnavailableSelections,
    registryVersion: options.registryVersion,
    registryDigest: options.registryDigest,
  };
}
