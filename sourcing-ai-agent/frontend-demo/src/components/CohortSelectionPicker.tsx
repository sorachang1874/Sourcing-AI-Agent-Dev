import {
  cloneCohortSelection,
  createDefaultCohortSelection,
  summarizeCohortSelection,
  toggleOrderedOption,
} from "../lib/cohortSelection";
import type { CohortSelection, CohortSelectionOptions } from "../types";

interface CohortSelectionPickerProps {
  idPrefix: string;
  value: CohortSelection | null;
  options: CohortSelectionOptions | null;
  isLoading?: boolean;
  errorMessage?: string;
  disabled?: boolean;
  locked?: boolean;
  compact?: boolean;
  onChange: (value: CohortSelection | null) => void;
  onRetryOptions?: () => void;
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
  onChange,
  onRetryOptions,
}: CohortSelectionPickerProps) {
  const controlsDisabled = disabled || locked || !options;
  const canEnable = Boolean(options) && !disabled && !locked;

  const updateSelection = (patch: Partial<CohortSelection>) => {
    if (!value || controlsDisabled) {
      return;
    }
    onChange({
      ...cloneCohortSelection(value),
      ...patch,
    });
  };

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
