import type { CandidateFacetOption } from "../lib/candidateFilters";

interface FacetMultiSelectProps {
  label: string;
  summary: string;
  options: CandidateFacetOption[];
  selectedIds: string[];
  onToggle: (optionId: string) => void;
}

export function FacetMultiSelect({
  label,
  summary,
  options,
  selectedIds,
  onToggle,
}: FacetMultiSelectProps) {
  return (
    <details className="facet-dropdown">
      <summary className="facet-dropdown-trigger">
        <span className="field-label">{label}</span>
        <strong>{summary}</strong>
      </summary>
      <div className="facet-dropdown-menu">
        {options.map((option) => (
          <label key={option.id} className="facet-option">
            <input
              type="checkbox"
              checked={selectedIds.includes(option.id)}
              onChange={() => onToggle(option.id)}
            />
            <span>{option.label}</span>
            <strong>{option.count}</strong>
          </label>
        ))}
      </div>
    </details>
  );
}
