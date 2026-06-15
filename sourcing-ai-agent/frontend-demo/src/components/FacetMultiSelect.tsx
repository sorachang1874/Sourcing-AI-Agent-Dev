import type { CandidateFacetOption } from "../lib/candidateFilters";

interface FacetMultiSelectProps {
  label: string;
  summary: string;
  options: CandidateFacetOption[];
  selectedIds: string[];
  onToggle: (optionId: string) => void;
  showCounts?: boolean;
  emptyMessage?: string;
  disabled?: boolean;
  disabledSummary?: string;
}

export function FacetMultiSelect({
  label,
  summary,
  options,
  selectedIds,
  onToggle,
  showCounts = true,
  emptyMessage = "当前没有可用筛选项。",
  disabled = false,
  disabledSummary,
}: FacetMultiSelectProps) {
  return (
    <details className={`facet-dropdown${disabled ? " disabled" : ""}`}>
      <summary className="facet-dropdown-trigger">
        <span className="field-label">{label}</span>
        <strong>{disabled ? disabledSummary || summary : summary}</strong>
      </summary>
      <div className="facet-dropdown-menu">
        {options.length === 0 ? <p className="facet-empty-message">{emptyMessage}</p> : null}
        {options.map((option) => (
          <label key={option.id} className="facet-option">
            <input
              type="checkbox"
              disabled={disabled}
              checked={selectedIds.includes(option.id)}
              onChange={() => onToggle(option.id)}
            />
            <span>{option.label}</span>
            {showCounts ? <strong>{option.count}</strong> : null}
          </label>
        ))}
      </div>
    </details>
  );
}
