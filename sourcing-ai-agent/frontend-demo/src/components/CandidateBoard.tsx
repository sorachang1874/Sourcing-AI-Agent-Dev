import { useEffect, useMemo, useState } from "react";
import { CandidateCard } from "./CandidateCard";
import { FacetMultiSelect } from "./FacetMultiSelect";
import {
  buildEmploymentOptions,
  buildLayerOptions,
  buildRecallBucketOptions,
  defaultEmploymentSelection,
  defaultLayerSelection,
  defaultRecallSelection,
  filterCandidatesByFacets,
  normalizeFacetSelection,
  summarizeSelectedFacet,
  toggleFacetSelection,
} from "../lib/candidateFilters";
import type { Candidate, DashboardData } from "../types";

interface CandidateBoardProps {
  dashboard: DashboardData;
  onOpenOnePage: (candidate: Candidate) => void;
}

export function CandidateBoard({ dashboard, onOpenOnePage }: CandidateBoardProps) {
  const layerOptions = useMemo(() => buildLayerOptions(dashboard), [dashboard]);
  const recallOptions = useMemo(
    () => buildRecallBucketOptions(dashboard.candidates, dashboard.intentKeywords),
    [dashboard.candidates, dashboard.intentKeywords],
  );
  const employmentOptions = useMemo(
    () => buildEmploymentOptions(dashboard.candidates),
    [dashboard.candidates],
  );

  const [selectedLayers, setSelectedLayers] = useState<string[]>(defaultLayerSelection(layerOptions));
  const [selectedRecallBuckets, setSelectedRecallBuckets] = useState<string[]>(defaultRecallSelection(recallOptions));
  const [selectedEmploymentStatuses, setSelectedEmploymentStatuses] = useState<string[]>(
    defaultEmploymentSelection(employmentOptions),
  );

  useEffect(() => {
    setSelectedLayers((current) =>
      normalizeFacetSelection(current, layerOptions, defaultLayerSelection(layerOptions)),
    );
  }, [layerOptions]);

  useEffect(() => {
    setSelectedRecallBuckets((current) =>
      normalizeFacetSelection(current, recallOptions, defaultRecallSelection(recallOptions)),
    );
  }, [recallOptions]);

  useEffect(() => {
    setSelectedEmploymentStatuses((current) =>
      normalizeFacetSelection(current, employmentOptions, defaultEmploymentSelection(employmentOptions)),
    );
  }, [employmentOptions]);

  const visibleCandidates = useMemo(
    () =>
      filterCandidatesByFacets(
        dashboard.candidates,
        {
          layers: selectedLayers,
          recallBuckets: selectedRecallBuckets,
          employmentStatuses: selectedEmploymentStatuses,
          locations: [],
          functionBuckets: [],
          searchKeyword: "",
        },
        dashboard.intentKeywords,
      ),
    [dashboard.candidates, dashboard.intentKeywords, selectedEmploymentStatuses, selectedLayers, selectedRecallBuckets],
  );

  return (
    <section className="conversation-card candidate-board">
      <div className="section-heading section-heading-spread">
        <div>
          <h3>候选人看板</h3>
        </div>
      </div>

      <div className="facet-dropdown-row">
        <FacetMultiSelect
          label="华人线索信息分层"
          summary={summarizeSelectedFacet(selectedLayers, layerOptions, "Layer 0")}
          options={layerOptions}
          selectedIds={selectedLayers}
          onToggle={(optionId) =>
            setSelectedLayers((current) =>
              toggleFacetSelection(current, optionId, {
                fallback: defaultLayerSelection(layerOptions),
              }),
            )
          }
        />
        <FacetMultiSelect
          label="召回排序"
          summary={summarizeSelectedFacet(selectedRecallBuckets, recallOptions, "全量")}
          options={recallOptions}
          selectedIds={selectedRecallBuckets}
          onToggle={(optionId) =>
            setSelectedRecallBuckets((current) =>
              toggleFacetSelection(current, optionId, {
                allId: "all",
                fallback: defaultRecallSelection(recallOptions),
              }),
            )
          }
        />
        <FacetMultiSelect
          label="在职状态"
          summary={summarizeSelectedFacet(selectedEmploymentStatuses, employmentOptions, "在职、已离职")}
          options={employmentOptions}
          selectedIds={selectedEmploymentStatuses}
          onToggle={(optionId) =>
            setSelectedEmploymentStatuses((current) =>
              toggleFacetSelection(current, optionId, {
                fallback: defaultEmploymentSelection(employmentOptions),
              }),
            )
          }
        />
      </div>

      <div className="candidate-board-grid">
        {visibleCandidates.map((candidate) => (
          <CandidateCard key={candidate.id} candidate={candidate} onOpenOnePage={onOpenOnePage} />
        ))}
      </div>

      {visibleCandidates.length === 0 ? (
        <div className="empty-state">
          <p>当前筛选条件下还没有候选人。</p>
          <span>可以放宽 Layer、召回排序或在职状态筛选后再查看结果。</span>
        </div>
      ) : null}
    </section>
  );
}
