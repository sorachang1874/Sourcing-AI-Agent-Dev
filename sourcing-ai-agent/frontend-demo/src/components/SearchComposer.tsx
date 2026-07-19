import type { ReactNode } from "react";
import type { CohortLocationSelection, CohortSelection, CohortSelectionOptions } from "../types";
import { CohortSelectionPicker } from "./CohortSelectionPicker";

interface SearchComposerProps {
  value: string;
  isSubmitting: boolean;
  compact?: boolean;
  showWelcome?: boolean;
  promptExamples: string[];
  afterPrompts?: ReactNode;
  cohortSelection?: CohortSelection | null;
  cohortOptions?: CohortSelectionOptions | null;
  isLoadingCohortOptions?: boolean;
  cohortOptionsError?: string;
  /**
   * Request-owned location state (sibling of the cohort object): owned by
   * SearchPage, passed explicitly so the real request carries the user's
   * locations and cohort option edits never reset them (review finding 1).
   */
  cohortLocations?: CohortLocationSelection | null;
  onChange: (value: string) => void;
  onCohortSelectionChange?: (value: CohortSelection | null) => void;
  onCohortLocationChange?: (value: CohortLocationSelection) => void;
  onRetryCohortOptions?: () => void;
  onSubmit: (value: string) => void;
  onPickPrompt?: (value: string) => void;
}

export function SearchComposer({
  value,
  isSubmitting,
  compact = false,
  showWelcome = true,
  promptExamples,
  afterPrompts,
  cohortSelection = null,
  cohortOptions = null,
  isLoadingCohortOptions = false,
  cohortOptionsError = "",
  cohortLocations = null,
  onChange,
  onCohortSelectionChange,
  onCohortLocationChange,
  onRetryCohortOptions,
  onSubmit,
  onPickPrompt,
}: SearchComposerProps) {
  const placeholder = "告诉我你想找什么样的人...";

  return (
    <section className={`composer-shell${compact ? " compact" : ""}`} data-testid="search-composer">
      <div className="composer-card">
        {!compact && showWelcome ? (
          <div className="welcome-block">
            <span className="welcome-kicker">Welcome Back</span>
          </div>
        ) : null}
        <div className="composer-input-wrap">
          <textarea
            className="composer-input"
            data-testid="search-composer-input"
            value={value}
            placeholder={placeholder}
            onChange={(event) => onChange(event.target.value)}
            rows={compact ? 3 : 4}
          />
          <button
            type="button"
            className="primary-button composer-submit"
            data-testid="search-composer-submit"
            disabled={isSubmitting || !value.trim()}
            onClick={() => onSubmit(value)}
          >
            {isSubmitting ? "生成中..." : "开始搜索"}
          </button>
        </div>

        {onCohortSelectionChange ? (
          <CohortSelectionPicker
            idPrefix="search"
            value={cohortSelection}
            options={cohortOptions}
            isLoading={isLoadingCohortOptions}
            errorMessage={cohortOptionsError}
            disabled={isSubmitting}
            compact
            locationValue={cohortLocations}
            onChange={onCohortSelectionChange}
            onLocationChange={onCohortLocationChange}
            onRetryOptions={onRetryCohortOptions}
          />
        ) : null}

        <div className="prompt-grid">
          {promptExamples.map((prompt) => (
            <button
              key={prompt}
              type="button"
              className="prompt-card"
              data-testid="search-composer-prompt"
              onClick={() => (onPickPrompt || onChange)(prompt)}
            >
              {prompt}
            </button>
          ))}
        </div>

        {afterPrompts ? <div className="composer-after-prompts">{afterPrompts}</div> : null}
      </div>
    </section>
  );
}
