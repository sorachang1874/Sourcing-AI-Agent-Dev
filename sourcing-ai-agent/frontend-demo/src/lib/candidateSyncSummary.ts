import type { CandidateFacetOption } from "./candidateFilters";
import { lifecycleCurrentSnapshotServingComplete } from "./resultViewLifecycle";
import type { BoardRuntimeState, EffectiveExecutionSemantics, LinkedinStage1Progress, ResultViewLifecycle } from "../types";

export interface CandidateSyncSummaryInput {
  loadedCandidateCount: number;
  expectedCandidateCount: number;
  resultViewLifecycle?: ResultViewLifecycle;
  boardRuntimeState?: BoardRuntimeState;
  linkedinStage1Progress?: LinkedinStage1Progress;
  effectiveExecutionSemantics?: EffectiveExecutionSemantics;
  recallOptions?: CandidateFacetOption[];
}

export interface CandidateSyncSummary {
  syncedCandidateCount: number;
  loadedCandidateCount: number;
  hydratedCandidateCount: number;
  expectedCandidateCount: number;
  intentMatchStatusText: string;
  candidateDiscoveryStatusText: string;
  profileFetchStatusText: string;
  cardMaterializationStatusText: string;
  noteText: string;
}

function positiveInteger(value: number | undefined | null): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.floor(value));
}

function explicitDeltaMaterializedCount(resultViewLifecycle: ResultViewLifecycle | undefined): number | null {
  if (!resultViewLifecycle) {
    return null;
  }
  if (lifecycleCurrentSnapshotServingComplete(resultViewLifecycle)) {
    return positiveInteger(resultViewLifecycle.deltaProfileMaterializedCount);
  }
  const boardVisibleValue = resultViewLifecycle.deltaProfileBoardVisibleCount;
  if (typeof boardVisibleValue === "number" && Number.isFinite(boardVisibleValue)) {
    return positiveInteger(boardVisibleValue);
  }
  const materializedValue = resultViewLifecycle.deltaProfileMaterializedCount;
  return typeof materializedValue === "number" && Number.isFinite(materializedValue)
    ? positiveInteger(materializedValue)
    : null;
}

function buildIntentMatchStatusText(recallOptions: CandidateFacetOption[] = []): string {
  const intentOptions = recallOptions.filter((option) => option.id !== "all" && positiveInteger(option.count) > 0);
  if (intentOptions.length === 0) {
    return "";
  }
  const formatted = intentOptions.slice(0, 3).map((option) => `${option.label} ${positiveInteger(option.count)} 人`);
  const suffix = intentOptions.length > formatted.length ? " 等" : "";
  return `当前意图匹配 ${formatted.join("、")}${suffix}`;
}

function backendSyncNoteText(board: BoardRuntimeState): string {
  const backendLines = (board.syncNoteLines || [])
    .map((line) => String(line?.text || "").trim())
    .filter(Boolean);
  if (backendLines.length > 0) {
    return backendLines.join("；");
  }
  if (board.noteText) {
    return board.noteText;
  }
  return [
    board.candidateDiscoveryStatusText || "",
    board.profileFetchStatusText || "",
    board.cardMaterializationStatusText || "",
  ]
    .filter(Boolean)
    .join("；");
}

function parseBoardSyncStatusText(value: string | undefined): { syncedCandidateCount: number; expectedCandidateCount: number } | null {
  const text = String(value || "").trim();
  const match = text.match(/(\d+)\s*\/\s*(\d+)/);
  if (!match) {
    return null;
  }
  const syncedCandidateCount = positiveInteger(Number(match[1]));
  const expectedCandidateCount = positiveInteger(Number(match[2]));
  if (expectedCandidateCount <= 0) {
    return null;
  }
  return {
    syncedCandidateCount: Math.min(syncedCandidateCount, expectedCandidateCount),
    expectedCandidateCount,
  };
}

function lifecycleServedCount(resultViewLifecycle: ResultViewLifecycle | undefined): number {
  return positiveInteger(resultViewLifecycle?.servedCandidateCount);
}

function lifecycleExpectedCount(resultViewLifecycle: ResultViewLifecycle | undefined): number {
  return positiveInteger(resultViewLifecycle?.expectedCandidateCount);
}

function buildProfileFetchStatusText(
  resultViewLifecycle: ResultViewLifecycle | undefined,
  linkedinStage1Progress: LinkedinStage1Progress | undefined,
  effectiveExecutionSemantics: EffectiveExecutionSemantics | undefined,
): string {
  const fullLocalAssetReuseWithoutDelta =
    Boolean(effectiveExecutionSemantics?.fullLocalAssetReuse) &&
    !Boolean(effectiveExecutionSemantics?.requiresDeltaAcquisition);
  const deltaProfileProgressApplicable = resultViewLifecycle?.deltaProfileProgressApplicable !== false;
  if (!deltaProfileProgressApplicable || fullLocalAssetReuseWithoutDelta) {
    return "";
  }
  const requiredCount = Math.max(
    positiveInteger(linkedinStage1Progress?.profileFetchRequiredCount),
    positiveInteger(resultViewLifecycle?.deltaProfileRequiredCount),
  );
  if (requiredCount === 0) {
    return "";
  }
  const fetchedCount = Math.min(
    requiredCount,
    Math.max(
      positiveInteger(linkedinStage1Progress?.profileFetchedCount),
      positiveInteger(resultViewLifecycle?.deltaProfileFetchedCount),
    ),
  );
  const retryableCount = Math.max(
    positiveInteger(linkedinStage1Progress?.profileFailedRetryableCount),
    positiveInteger(resultViewLifecycle?.deltaProfileRetryableCount),
  );
  const retryableText = retryableCount > 0 ? `，可重试 ${retryableCount}` : "";
  const requiresDeltaAcquisition = Boolean(effectiveExecutionSemantics?.requiresDeltaAcquisition);
  const hasLifecycleDeltaBaseline = Boolean(
    deltaProfileProgressApplicable &&
      resultViewLifecycle?.baselineSnapshotId &&
      positiveInteger(resultViewLifecycle?.baselineCandidateCount) > 0,
  );
  const hasDeltaServingBaseline = Boolean(
    (requiresDeltaAcquisition || hasLifecycleDeltaBaseline) &&
      (resultViewLifecycle?.baselineSnapshotId ||
        positiveInteger(resultViewLifecycle?.baselineCandidateCount) > 0 ||
        ["baseline_serving", "delta_applying", "current_snapshot_materializing", "post_result_layering"].includes(
          resultViewLifecycle?.state || "",
        )),
  );
  const servedCount = positiveInteger(resultViewLifecycle?.servedCandidateCount);
  if (!hasDeltaServingBaseline) {
    return `本次 LinkedIn Profile 已取回 ${fetchedCount}/${requiredCount}${retryableText}`;
  }
  return `新增 LinkedIn Profile 已取回 ${fetchedCount}/${requiredCount}${retryableText}`;
}

function buildCardMaterializationStatusText(
  resultViewLifecycle: ResultViewLifecycle | undefined,
  linkedinStage1Progress: LinkedinStage1Progress | undefined,
  effectiveExecutionSemantics: EffectiveExecutionSemantics | undefined,
): string {
  const fullLocalAssetReuseWithoutDelta =
    Boolean(effectiveExecutionSemantics?.fullLocalAssetReuse) &&
    !Boolean(effectiveExecutionSemantics?.requiresDeltaAcquisition);
  const deltaProfileProgressApplicable = resultViewLifecycle?.deltaProfileProgressApplicable !== false;
  if (!deltaProfileProgressApplicable || fullLocalAssetReuseWithoutDelta) {
    return "";
  }
  const requiredCount = Math.max(
    positiveInteger(linkedinStage1Progress?.profileFetchRequiredCount),
    positiveInteger(resultViewLifecycle?.deltaProfileRequiredCount),
  );
  if (requiredCount === 0) {
    return "";
  }
  const fetchedCount = Math.min(
    requiredCount,
    Math.max(
      positiveInteger(linkedinStage1Progress?.profileFetchedCount),
      positiveInteger(resultViewLifecycle?.deltaProfileFetchedCount),
    ),
  );
  const servedCount = positiveInteger(resultViewLifecycle?.servedCandidateCount);
  const requiresDeltaAcquisition = Boolean(effectiveExecutionSemantics?.requiresDeltaAcquisition);
  const hasLifecycleDeltaBaseline = Boolean(
    deltaProfileProgressApplicable &&
      resultViewLifecycle?.baselineSnapshotId &&
      positiveInteger(resultViewLifecycle?.baselineCandidateCount) > 0,
  );
  const hasDeltaServingBaseline = Boolean(
    (requiresDeltaAcquisition || hasLifecycleDeltaBaseline) &&
      (resultViewLifecycle?.baselineSnapshotId ||
        positiveInteger(resultViewLifecycle?.baselineCandidateCount) > 0 ||
        ["baseline_serving", "delta_applying", "current_snapshot_materializing", "post_result_layering"].includes(
          resultViewLifecycle?.state || "",
        )),
  );
  if (!hasDeltaServingBaseline) {
    const explicitMaterializedCount = explicitDeltaMaterializedCount(resultViewLifecycle);
    const inferredMaterializedCount = Math.max(0, Math.min(requiredCount, servedCount || fetchedCount, fetchedCount));
    const materializedCount = Math.min(requiredCount, explicitMaterializedCount ?? inferredMaterializedCount);
    return `卡片详情已合入看板 ${materializedCount}/${requiredCount}`;
  }
  const expectedCount = positiveInteger(resultViewLifecycle?.expectedCandidateCount);
  const baselineCount =
    positiveInteger(resultViewLifecycle?.baselineCandidateCount) ||
    Math.max(0, Math.max(expectedCount, servedCount) - requiredCount);
  const explicitMaterializedCount = explicitDeltaMaterializedCount(resultViewLifecycle);
  const inferredMaterializedCount = Math.max(0, Math.min(requiredCount, servedCount - baselineCount, fetchedCount));
  const materializedCount = Math.min(requiredCount, explicitMaterializedCount ?? inferredMaterializedCount);
  return `卡片详情已合入看板 ${materializedCount}/${requiredCount}`;
}

function buildCandidateSyncCounts(
  loadedCandidateCount: number,
  inputExpectedCandidateCount: number,
  resultViewLifecycle: ResultViewLifecycle | undefined,
): { syncedCandidateCount: number; hydratedCandidateCount: number; expectedCandidateCount: number } {
  const lifecycle = resultViewLifecycle;
  const servedCandidateCount = lifecycleServedCount(lifecycle);
  const hydratedCandidateCount = positiveInteger(loadedCandidateCount);
  const baseSyncedCandidateCount = servedCandidateCount || hydratedCandidateCount;
  const expectedCandidateCount = Math.max(
    positiveInteger(inputExpectedCandidateCount),
    lifecycleExpectedCount(lifecycle),
    servedCandidateCount,
    baseSyncedCandidateCount,
  );
  return {
    syncedCandidateCount: expectedCandidateCount > 0
      ? Math.min(baseSyncedCandidateCount, expectedCandidateCount)
      : baseSyncedCandidateCount,
    hydratedCandidateCount,
    expectedCandidateCount,
  };
}

export function buildCandidateSyncSummary(input: CandidateSyncSummaryInput): CandidateSyncSummary {
  if (input.boardRuntimeState) {
    const board = input.boardRuntimeState;
    // Once boardRuntimeState exists, syncStatusText is the canonical
    // row-publication progress. Profile/card readiness is rendered only through
    // syncNoteLines, not by recomputing another frontend denominator.
    const parsedSyncStatus = parseBoardSyncStatusText(board.syncStatusText);
    const expectedCandidateCount = parsedSyncStatus?.expectedCandidateCount || positiveInteger(board.expectedCandidateCount);
    const fallbackSyncedCandidateCount =
      board.publicationStatus === "complete"
        ? expectedCandidateCount
        : Math.max(
            positiveInteger(board.publishedCandidateCount),
            positiveInteger(board.servedCandidateCount),
            positiveInteger(board.displayReadyCandidateCount),
          );
    const syncedCandidateCount =
      parsedSyncStatus?.syncedCandidateCount || Math.min(fallbackSyncedCandidateCount, expectedCandidateCount || fallbackSyncedCandidateCount);
    const hydratedCandidateCount = positiveInteger(input.loadedCandidateCount);
    const frontendHydrationComplete =
      syncedCandidateCount > 0
        ? hydratedCandidateCount >= Math.min(syncedCandidateCount, expectedCandidateCount || syncedCandidateCount)
        : hydratedCandidateCount >= expectedCandidateCount;
    const intentMatchStatusText = frontendHydrationComplete ? buildIntentMatchStatusText(input.recallOptions) : "";
    const candidateDiscoveryStatusText = board.candidateDiscoveryStatusText || "";
    const profileFetchStatusText = board.profileFetchStatusText || "";
    const cardMaterializationStatusText = board.cardMaterializationStatusText || "";
    const noteText = backendSyncNoteText(board);
    return {
      syncedCandidateCount,
      loadedCandidateCount: syncedCandidateCount,
      hydratedCandidateCount,
      expectedCandidateCount,
      intentMatchStatusText,
      candidateDiscoveryStatusText,
      profileFetchStatusText,
      cardMaterializationStatusText,
      noteText,
    };
  }
  const lifecycle = input.resultViewLifecycle;
  const { syncedCandidateCount, hydratedCandidateCount, expectedCandidateCount } = buildCandidateSyncCounts(
    input.loadedCandidateCount,
    input.expectedCandidateCount,
    lifecycle,
  );
  const frontendHydrationComplete =
    expectedCandidateCount === 0 || hydratedCandidateCount >= expectedCandidateCount;
  const intentMatchStatusText = frontendHydrationComplete ? buildIntentMatchStatusText(input.recallOptions) : "";
  const candidateDiscoveryStatusText = "";
  const profileFetchStatusText = buildProfileFetchStatusText(
    lifecycle,
    input.linkedinStage1Progress,
    input.effectiveExecutionSemantics,
  );
  const cardMaterializationStatusText = buildCardMaterializationStatusText(
    lifecycle,
    input.linkedinStage1Progress,
    input.effectiveExecutionSemantics,
  );
  const noteText = [profileFetchStatusText, cardMaterializationStatusText]
    .filter(Boolean)
    .join("；");
  return {
    syncedCandidateCount,
    // Backward-compatible alias for older callers/tests. This is canonical
    // business sync progress, not the frontend page hydration window.
    loadedCandidateCount: syncedCandidateCount,
    hydratedCandidateCount,
    expectedCandidateCount,
    intentMatchStatusText,
    candidateDiscoveryStatusText,
    profileFetchStatusText,
    cardMaterializationStatusText,
    noteText,
  };
}
