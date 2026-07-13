import type { DashboardData, ResultViewLifecycle } from "../types";

function positiveInteger(value: number | undefined | null): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.floor(value));
}

function lifecycleServedCandidateCount(lifecycle: ResultViewLifecycle | undefined): number {
  return positiveInteger(lifecycle?.servedCandidateCount);
}

function lifecycleExpectedCandidateCount(lifecycle: ResultViewLifecycle | undefined): number {
  return positiveInteger(lifecycle?.expectedCandidateCount);
}

function boardExpectedCandidateCount(dashboard: DashboardData | null | undefined): number {
  return positiveInteger(dashboard?.boardRuntimeState?.expectedCandidateCount);
}

export function dashboardRowHydrationTargetCount(dashboard: DashboardData | null | undefined): number {
  if (!dashboard?.boardRuntimeState) {
    return 0;
  }
  return positiveInteger(dashboard?.boardRuntimeState?.rowHydrationTargetCount);
}

export function dashboardLoadedCandidateRowCount(dashboard: DashboardData | null | undefined): number {
  return positiveInteger(dashboard?.candidates.length);
}

export function dashboardServedCandidateCount(dashboard: DashboardData | null | undefined): number {
  if (dashboard?.boardRuntimeState) {
    return positiveInteger(dashboard.boardRuntimeState.servedCandidateCount);
  }
  return lifecycleServedCandidateCount(dashboard?.resultViewLifecycle);
}

export function dashboardExpectedCandidateCount(dashboard: DashboardData | null | undefined): number {
  if (!dashboard) {
    return 0;
  }
  if (dashboard.boardRuntimeState) {
    // Board runtime state is the canonical business total. Published rows are
    // hydration watermarks only and must not inflate the denominator.
    return boardExpectedCandidateCount(dashboard);
  }
  return Math.max(
    positiveInteger(dashboard.totalCandidates),
    positiveInteger(dashboard.assetPopulationCount),
    lifecycleExpectedCandidateCount(dashboard.resultViewLifecycle),
    lifecycleServedCandidateCount(dashboard.resultViewLifecycle),
    dashboardLoadedCandidateRowCount(dashboard),
  );
}

export function dashboardHasPublicCandidateCount(dashboard: DashboardData | null | undefined): boolean {
  return dashboardExpectedCandidateCount(dashboard) > 0;
}

export function dashboardHasRenderableCandidates(dashboard: DashboardData | null | undefined): boolean {
  if (dashboard?.boardRuntimeState) {
    return (
      positiveInteger(dashboard.boardRuntimeState.displayReadyCandidateCount) > 0 ||
      dashboardLoadedCandidateRowCount(dashboard) > 0
    );
  }
  return dashboardLoadedCandidateRowCount(dashboard) > 0;
}

export function dashboardBoardRuntimePublicationComplete(dashboard: DashboardData | null | undefined): boolean {
  if (dashboard?.boardRuntimeState) {
    const board = dashboard.boardRuntimeState;
    const expectedCount = boardExpectedCandidateCount(dashboard);
    const servedCount = positiveInteger(board.servedCandidateCount);
    return (
      board.publicationStatus === "complete" &&
      ["current_snapshot_serving", "canonical_projection_serving"].includes(board.phase) &&
      (expectedCount <= 0 || servedCount >= expectedCount)
    );
  }
  const lifecycle = dashboard?.resultViewLifecycle;
  const expectedCount = dashboardExpectedCandidateCount(dashboard);
  const servedCount = lifecycleServedCandidateCount(lifecycle);
  return (
    Boolean(lifecycle) &&
    ["current_snapshot_serving", "post_result_layering"].includes(lifecycle?.state || "") &&
    (expectedCount <= 0 || servedCount >= expectedCount)
  );
}

export function dashboardCandidateHydrationPending(dashboard: DashboardData | null | undefined): boolean {
  const targetCount = dashboard?.boardRuntimeState
    ? dashboardRowHydrationTargetCount(dashboard)
    : dashboardExpectedCandidateCount(dashboard);
  return targetCount > 0 && dashboardLoadedCandidateRowCount(dashboard) < targetCount;
}

export function dashboardCandidateHydrationBannerVisible(
  dashboard: DashboardData | null | undefined,
  isHydratingCandidates = false,
): boolean {
  if (!dashboardCandidateHydrationPending(dashboard)) {
    return false;
  }
  const publicationStatus = dashboard?.boardRuntimeState?.publicationStatus || "";
  if (dashboard?.boardRuntimeState) {
    if (publicationStatus === "complete") {
      return false;
    }
    return Boolean(isHydratingCandidates && publicationStatus === "partial");
  }
  const lifecycle = dashboard?.resultViewLifecycle;
  const terminalServingState =
    lifecycle?.state === "current_snapshot_serving" ||
    lifecycle?.state === "post_result_layering";
  const servedCount = lifecycleServedCandidateCount(lifecycle);
  const expectedCount = dashboardExpectedCandidateCount(dashboard);
  if (terminalServingState && servedCount >= expectedCount) {
    return false;
  }
  if (isHydratingCandidates) {
    return true;
  }
  return !terminalServingState || servedCount < expectedCount;
}

export function dashboardCandidateBoardBootstrapping(
  dashboard: DashboardData | null | undefined,
  hydrationError = "",
): boolean {
  return dashboardHasPublicCandidateCount(dashboard) && !dashboardHasRenderableCandidates(dashboard) && !hydrationError;
}
