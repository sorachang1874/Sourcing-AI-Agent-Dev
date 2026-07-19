import { useState } from "react";
import {
  appendLocationValue,
  buildCohortShardPreview,
  canonicalCohortSelectionKey,
  cloneCohortSelection,
  createDefaultCohortLocationSelection,
  createDefaultCohortSelection,
  publishCohortLocationDraft,
  removeLocationValue,
  SERVER_DEFAULT_TARGET_LOCATION_DISPLAY,
  summarizeCohortSelection,
  toggleOrderedOption,
} from "../lib/cohortSelection";
import type {
  CohortLocationSelection,
  CohortSelection,
  CohortSelectionOptions,
} from "../types";

interface CohortSelectionPickerProps {
  idPrefix: string;
  value: CohortSelection | null;
  options: CohortSelectionOptions | null;
  isLoading?: boolean;
  errorMessage?: string;
  disabled?: boolean;
  locked?: boolean;
  compact?: boolean;
  /**
   * Controlled location selection (sibling of the cohort object, never part
   * of it). When omitted, the picker owns the location state locally AND
   * publishes it into the explicit draft registry consumed by the pinned
   * request payload builders, so unleased parents that only transport the
   * cohort object still submit the user's real locations (FT2 fixed-forward,
   * review finding 1).
   */
  locationValue?: CohortLocationSelection | null;
  onLocationChange?: (value: CohortLocationSelection) => void;
  /**
   * Disable ONLY the location controls (review finding 2): location editing
   * is exposed exclusively when the backend review gate authorizes the
   * location editable fields; otherwise the effective values render
   * read-only.
   */
  locationDisabled?: boolean;
  /** Set false when the embedding surface renders its own shard preview. */
  showShardPreview?: boolean;
  onChange: (value: CohortSelection | null) => void;
  onRetryOptions?: () => void;
}

function LocationTagInput({
  idPrefix,
  legend,
  hint,
  values,
  placeholder,
  disabled,
  onChange,
}: {
  idPrefix: string;
  legend: string;
  hint: string;
  values: string[];
  placeholder: string;
  disabled: boolean;
  onChange: (values: string[]) => void;
}) {
  const [draft, setDraft] = useState("");
  const commitDraft = () => {
    const next = appendLocationValue(values, draft);
    setDraft("");
    if (next !== values) {
      onChange(next);
    }
  };
  return (
    <fieldset disabled={disabled} data-testid={idPrefix}>
      <legend>{legend}</legend>
      <p className="cohort-picker-hint">{hint}</p>
      <div className="cohort-location-tags">
        {values.map((name) => (
          <span key={name} className="cohort-location-tag" data-testid={`${idPrefix}-tag`}>
            <span>{name}</span>
            <button
              type="button"
              className="link-chip"
              aria-label={`移除 ${name}`}
              disabled={disabled}
              onClick={() => onChange(removeLocationValue(values, name))}
            >
              ×
            </button>
          </span>
        ))}
        <input
          type="text"
          className="text-input cohort-location-input"
          data-testid={`${idPrefix}-input`}
          value={draft}
          placeholder={placeholder}
          disabled={disabled}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              commitDraft();
            }
          }}
        />
        <button
          type="button"
          className="link-chip"
          data-testid={`${idPrefix}-add`}
          disabled={disabled || !draft.trim()}
          onClick={commitDraft}
        >
          添加
        </button>
      </div>
    </fieldset>
  );
}

export function CohortSelectionPicker({
  idPrefix,
  value,
  options,
  isLoading = false,
  errorMessage = "",
  disabled = false,
  locked = false,
  compact = false,
  locationValue = null,
  onLocationChange,
  locationDisabled = false,
  showShardPreview = true,
  onChange,
  onRetryOptions,
}: CohortSelectionPickerProps) {
  const controlsDisabled = disabled || locked || !options;
  const canEnable = Boolean(options) && !disabled && !locked;
  const locationsReadOnly = controlsDisabled || locationDisabled;
  // Local location state is bound to the exact cohort object it belongs to:
  // when the controlled cohort identity changes (new search, recovered
  // flow), the local copy resets to the absent/server-default seed instead
  // of leaking a previous flow's locations into display or submission.
  const cohortKey = value ? canonicalCohortSelectionKey(value) : "";
  const [localLocations, setLocalLocations] = useState<{
    cohortKey: string;
    selection: CohortLocationSelection;
  }>(() => ({
    cohortKey,
    selection: createDefaultCohortLocationSelection(),
  }));
  if (!locationValue && localLocations.cohortKey !== cohortKey) {
    setLocalLocations({ cohortKey, selection: createDefaultCohortLocationSelection() });
  }
  const locations = locationValue || localLocations.selection;

  const updateSelection = (patch: Partial<CohortSelection>) => {
    if (!value || controlsDisabled) {
      return;
    }
    onChange({
      ...cloneCohortSelection(value),
      ...patch,
    });
  };

  const updateLocations = (next: CohortLocationSelection) => {
    if (!locationValue) {
      setLocalLocations({ cohortKey, selection: next });
      if (value) {
        publishCohortLocationDraft(next, value);
      }
    }
    onLocationChange?.(next);
  };

  const shardPreview = value && options ? buildCohortShardPreview(value, options) : null;
  const unavailableSelections: string[] = shardPreview
    ? [
        ...shardPreview.unavailableRoleIds.map((id) => `角色 ${id}`),
        ...shardPreview.unavailableStatusIds.map((id) => `在职状态 ${id}`),
        ...(shardPreview.roleMatchUnavailable ? [`匹配方式 ${value?.role_match || "role_match"}`] : []),
      ]
    : [];

  const targetOptedOut =
    locations.targetLocations !== undefined && locations.targetLocations.length === 0;

  return (
    <section
      className={`cohort-picker${compact ? " compact" : ""}${locked ? " locked" : ""}`}
      data-testid={`${idPrefix}-cohort-picker`}
    >
      <div className="cohort-picker-heading">
        <label className="cohort-picker-enable" htmlFor={`${idPrefix}-cohort-enabled`}>
          <input
            id={`${idPrefix}-cohort-enabled`}
            data-testid={`${idPrefix}-cohort-enabled`}
            type="checkbox"
            checked={Boolean(value)}
            disabled={value ? disabled || locked : !canEnable}
            onChange={(event) => {
              if (event.target.checked && options) {
                const nextCohort = createDefaultCohortSelection(options);
                onChange(nextCohort);
                // Location is a sibling request field: seed the absent
                // (server-default) state alongside the cohort object and
                // publish it for the request payload builders.
                const seed = createDefaultCohortLocationSelection();
                if (!locationValue) {
                  setLocalLocations({
                    cohortKey: canonicalCohortSelectionKey(nextCohort),
                    selection: seed,
                  });
                  publishCohortLocationDraft(seed, nextCohort);
                }
                onLocationChange?.(seed);
              } else if (!event.target.checked) {
                onChange(null);
                // Clear the sibling location state atomically with the
                // cohort object (local copy, draft registry, and any
                // controlled parent alike).
                const cleared = createDefaultCohortLocationSelection();
                if (!locationValue) {
                  setLocalLocations({ cohortKey: "", selection: cleared });
                  publishCohortLocationDraft(null, null);
                }
                onLocationChange?.(cleared);
              }
            }}
          />
          <span>
            <strong>限定目标人群</strong>
            <small>按角色与在职状态精确选择；关闭时保留原有自然语言路径。</small>
          </span>
        </label>
        {locked && value ? <span className="cohort-picker-lock">已随方案锁定</span> : null}
      </div>

      {isLoading && !options ? <p className="cohort-picker-state">正在加载可选人群…</p> : null}
      {errorMessage && !options ? (
        <div className="cohort-picker-state error" role="status">
          <span>{errorMessage}</span>
          {onRetryOptions ? (
            <button type="button" className="link-chip" onClick={onRetryOptions}>
              重试
            </button>
          ) : null}
        </div>
      ) : null}

      {value && !options ? (
        <p className="cohort-picker-state">{summarizeCohortSelection(value, null)}</p>
      ) : null}

      {value && options ? (
        <div className="cohort-picker-fields">
          <fieldset disabled={controlsDisabled}>
            <legend>角色（可多选）</legend>
            <p className="cohort-picker-hint">不勾选具体角色即代表全部角色。</p>
            <div className="cohort-option-grid" data-testid={`${idPrefix}-cohort-roles`}>
              <button
                type="button"
                className={`cohort-all-roles${value.role_bucket_ids.length === 0 ? " selected" : ""}`}
                disabled={controlsDisabled}
                aria-pressed={value.role_bucket_ids.length === 0}
                onClick={() => updateSelection({ role_bucket_ids: [] })}
              >
                All roles
              </button>
              {options.roleBuckets.map((option) => (
                <label key={option.id} className="checkbox-option">
                  <input
                    type="checkbox"
                    value={option.id}
                    checked={value.role_bucket_ids.includes(option.id)}
                    onChange={(event) =>
                      updateSelection({
                        role_bucket_ids: toggleOrderedOption(
                          value.role_bucket_ids,
                          option.id,
                          event.target.checked,
                          options.roleBuckets,
                        ),
                      })
                    }
                  />
                  <span>{option.label}</span>
                </label>
              ))}
            </div>
          </fieldset>

          <fieldset disabled={controlsDisabled}>
            <legend>在职状态（可多选）</legend>
            <div className="cohort-option-grid" data-testid={`${idPrefix}-cohort-statuses`}>
              {options.employmentStatuses.map((option) => {
                const checked = value.employment_statuses.includes(option.id);
                const isLastSelected = checked && value.employment_statuses.length === 1;
                return (
                  <label key={option.id} className="checkbox-option">
                    <input
                      type="checkbox"
                      value={option.id}
                      checked={checked}
                      disabled={controlsDisabled || isLastSelected}
                      onChange={(event) => {
                        const employmentStatuses = toggleOrderedOption(
                          value.employment_statuses,
                          option.id,
                          event.target.checked,
                          options.employmentStatuses,
                        );
                        if (employmentStatuses.length > 0) {
                          updateSelection({ employment_statuses: employmentStatuses });
                        }
                      }}
                    />
                    <span>{option.label}</span>
                  </label>
                );
              })}
            </div>
          </fieldset>

          <fieldset disabled={controlsDisabled}>
            <legend>多角色匹配方式</legend>
            <div className="cohort-option-grid" data-testid={`${idPrefix}-cohort-role-match`}>
              {options.roleMatchOptions.map((option) => (
                <label key={option.id} className="checkbox-option">
                  <input
                    type="radio"
                    name={`${idPrefix}-role-match`}
                    value={option.id}
                    checked={value.role_match === option.id}
                    onChange={() => updateSelection({ role_match: option.id })}
                  />
                  <span>{option.label}</span>
                </label>
              ))}
            </div>
          </fieldset>

          <fieldset disabled={locationsReadOnly} data-testid={`${idPrefix}-target-locations`}>
            <legend>目标地区（可多值）</legend>
            <p className="cohort-picker-hint">
              自由文本，由后端校验；未选择时服务端按默认 {SERVER_DEFAULT_TARGET_LOCATION_DISPLAY} 执行。
            </p>
            {locations.targetLocations === undefined ? (
              <p
                className="cohort-location-default"
                data-testid={`${idPrefix}-target-locations-default`}
              >
                服务端默认: {SERVER_DEFAULT_TARGET_LOCATION_DISPLAY}
              </p>
            ) : null}
            <label className="checkbox-option cohort-location-optout">
              <input
                type="checkbox"
                data-testid={`${idPrefix}-target-locations-optout`}
                checked={targetOptedOut}
                disabled={locationsReadOnly}
                onChange={(event) =>
                  updateLocations({
                    ...locations,
                    targetLocations: event.target.checked ? [] : undefined,
                  })
                }
              />
              <span>不限地区（显式退出地区筛选）</span>
            </label>
            {targetOptedOut ? (
              <p
                className="cohort-location-opted-out"
                data-testid={`${idPrefix}-target-locations-opted-out`}
              >
                不限地区（已显式退出地区筛选）
              </p>
            ) : (
              <LocationTagInput
                idPrefix={`${idPrefix}-target-locations-input`}
                legend="自定义目标地区"
                hint="添加后按所选地区执行（用户选择完整覆盖服务端默认，不做合并）。"
                values={locations.targetLocations ?? []}
                placeholder={SERVER_DEFAULT_TARGET_LOCATION_DISPLAY}
                disabled={locationsReadOnly}
                onChange={(targetValues) =>
                  updateLocations({
                    ...locations,
                    targetLocations: targetValues.length > 0 ? targetValues : undefined,
                  })
                }
              />
            )}
          </fieldset>

          <LocationTagInput
            idPrefix={`${idPrefix}-exclude-locations`}
            legend="排除地区（可选）"
            hint="自由文本，命中排除地区的成员不纳入召回。"
            values={locations.excludeTargetLocations ?? []}
            placeholder="例如：European Union"
            disabled={locationsReadOnly}
            onChange={(excludeValues) =>
              updateLocations({
                ...locations,
                excludeTargetLocations: excludeValues.length > 0 ? excludeValues : undefined,
              })
            }
          />

          {locationDisabled ? (
            <p className="cohort-picker-hint" data-testid={`${idPrefix}-locations-locked`}>
              地区边界当前未获编辑授权，仅展示生效值。
            </p>
          ) : null}

          {showShardPreview && shardPreview ? (
            <div className="cohort-shard-preview" data-testid={`${idPrefix}-cohort-shard-preview`}>
              {shardPreview.hasUnavailableSelections ? (
                <p
                  className="cohort-shard-stale-warning"
                  role="alert"
                  data-testid={`${idPrefix}-shard-stale-warning`}
                >
                  ⚠ 当前选项（registry {shardPreview.registryVersion}）已不包含所选值：
                  {unavailableSelections.join("、")}。请刷新选项后重新选择；预览与确认已被阻止。
                </p>
              ) : null}
              <p className="cohort-picker-hint" data-testid={`${idPrefix}-cohort-shard-count`}>
                当前选择将展开为 {shardPreview.shardCount} 个执行分片（在职状态 × 角色）：
              </p>
              <ul className="cohort-shard-list">
                {shardPreview.shards.map((shard) => (
                  <li
                    key={shard.shardId}
                    data-testid={`${idPrefix}-cohort-shard-${shard.shardId}`}
                  >
                    {shard.statusLabel} · {shard.roleLabel}
                  </li>
                ))}
              </ul>
              {shardPreview.isFullRecall ? (
                <p
                  className="cohort-full-recall-warning"
                  role="alert"
                  data-testid={`${idPrefix}-full-recall-warning`}
                >
                  ⚠ 未限定角色且覆盖全部在职状态：将对目标公司全量成员发起全量召回，结果量与执行耗时显著增加；每个分片仍受服务端预算上限与
                  provider 限额约束。
                </p>
              ) : null}
            </div>
          ) : null}

          {locked ? (
            <p className="cohort-picker-hint">
              此方案已绑定精确人群边界；如需更改，请返回新建搜索，避免 review 阶段静默改写请求。
            </p>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
