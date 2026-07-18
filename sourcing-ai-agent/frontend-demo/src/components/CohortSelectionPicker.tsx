import { useState } from "react";
import {
  appendLocationValue,
  buildCohortShardPreview,
  cloneCohortSelection,
  createDefaultCohortLocationSelection,
  createDefaultCohortSelection,
  removeLocationValue,
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
   * of it). When omitted, the picker keeps a local copy seeded with the
   * server default so unwired parents still render the default truthfully.
   */
  locationValue?: CohortLocationSelection | null;
  onLocationChange?: (value: CohortLocationSelection) => void;
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
  showShardPreview = true,
  onChange,
  onRetryOptions,
}: CohortSelectionPickerProps) {
  const controlsDisabled = disabled || locked || !options;
  const canEnable = Boolean(options) && !disabled && !locked;
  const [localLocations, setLocalLocations] = useState<CohortLocationSelection>(() =>
    createDefaultCohortLocationSelection(),
  );
  const locations = locationValue || localLocations;

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
      setLocalLocations(next);
    }
    onLocationChange?.(next);
  };

  const shardPreview = value && options ? buildCohortShardPreview(value, options) : null;

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
                onChange(createDefaultCohortSelection(options));
                // Location is a sibling request field: seed the server
                // default (["United States"]) alongside the cohort object.
                updateLocations(createDefaultCohortLocationSelection());
              } else if (!event.target.checked) {
                onChange(null);
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

          <LocationTagInput
            idPrefix={`${idPrefix}-target-locations`}
            legend="目标地区（可多值）"
            hint="自由文本，由后端校验；全部移除后服务端按默认 United States 执行。"
            values={locations.targetLocations}
            placeholder="United States"
            disabled={controlsDisabled}
            onChange={(targetLocations) => updateLocations({ ...locations, targetLocations })}
          />

          <LocationTagInput
            idPrefix={`${idPrefix}-exclude-locations`}
            legend="排除地区（可选）"
            hint="自由文本，命中排除地区的成员不纳入召回。"
            values={locations.excludeTargetLocations}
            placeholder="例如：European Union"
            disabled={controlsDisabled}
            onChange={(excludeTargetLocations) =>
              updateLocations({ ...locations, excludeTargetLocations })
            }
          />

          {showShardPreview && shardPreview ? (
            <div className="cohort-shard-preview" data-testid={`${idPrefix}-cohort-shard-preview`}>
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
