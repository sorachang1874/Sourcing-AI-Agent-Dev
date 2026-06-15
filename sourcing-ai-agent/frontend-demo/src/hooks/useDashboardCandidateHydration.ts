import { useEffect, useRef, useState } from "react";
import {
  getDashboard,
  getDashboardCandidatePage,
  getProjectionCandidatePage,
  mergeDashboardCandidatePage,
  storeProjectionDashboardCache,
  storeDashboardCache,
} from "../lib/api";
import {
  dashboardCandidateHydrationPending,
  dashboardExpectedCandidateCount,
  dashboardLoadedCandidateRowCount,
  dashboardRowHydrationTargetCount,
} from "../lib/dashboardHydration";
import { lifecycleEffectiveDeltaMaterializedCount } from "../lib/resultViewLifecycle";
import type { DashboardData } from "../types";

const DASHBOARD_INITIAL_REQUIRED_CANDIDATE_COUNT = 96;
const DASHBOARD_BACKGROUND_HYDRATION_LIMIT = 96;
const DASHBOARD_BACKGROUND_HYDRATION_CONCURRENCY = 3;
const DASHBOARD_VIEWPORT_PREFETCH_CANDIDATE_COUNT = 48;
const DASHBOARD_ASSET_REFRESH_INTERVAL_MS = 5000;
const DASHBOARD_ASSET_ACTIVE_REFRESH_INTERVAL_MS = 1000;
const DASHBOARD_ASSET_REFRESH_MIN_ROUNDS = 12;
const DASHBOARD_ASSET_REFRESH_MAX_ROUNDS = 60;

interface UseDashboardCandidateHydrationOptions {
  jobId?: string;
  projectionId?: string;
  dashboard: DashboardData | null;
  onDashboardChange: (dashboard: DashboardData) => void;
  requiredCandidateCount?: number;
  backgroundCandidateCount?: number;
}

export function useDashboardCandidateHydration({
  jobId = "",
  projectionId = "",
  dashboard,
  onDashboardChange,
  requiredCandidateCount = DASHBOARD_INITIAL_REQUIRED_CANDIDATE_COUNT,
  backgroundCandidateCount = DASHBOARD_INITIAL_REQUIRED_CANDIDATE_COUNT + DASHBOARD_BACKGROUND_HYDRATION_LIMIT,
}: UseDashboardCandidateHydrationOptions) {
  const dashboardRef = useRef<DashboardData | null>(dashboard);
  const [isHydratingCandidates, setIsHydratingCandidates] = useState(false);
  const [candidateHydrationError, setCandidateHydrationError] = useState("");

  useEffect(() => {
    dashboardRef.current = dashboard;
  }, [dashboard]);

  useEffect(() => {
    if ((!jobId && !projectionId) || !dashboard || dashboard.resultMode !== "asset_population") {
      return undefined;
    }
    let cancelled = false;
    let timeoutId: number | null = null;
    let refreshRound = 0;

    const schedule = () => {
      if (cancelled || refreshRound >= DASHBOARD_ASSET_REFRESH_MAX_ROUNDS) {
        return;
      }
      const delayMs = dashboardHasActiveAssetLifecycle(dashboardRef.current)
        ? DASHBOARD_ASSET_ACTIVE_REFRESH_INTERVAL_MS
        : DASHBOARD_ASSET_REFRESH_INTERVAL_MS;
      timeoutId = globalThis.setTimeout(() => {
        timeoutId = null;
        void refresh();
      }, delayMs);
    };

    const shouldContinuePolling = (dashboardValue: DashboardData | null, round: number): boolean => {
      if (!dashboardValue || dashboardValue.resultMode !== "asset_population") {
        return false;
      }
      const lifecycle = dashboardValue.resultViewLifecycle;
      const boardRuntimeState = dashboardValue.boardRuntimeState;
      const boardRuntimePending = Boolean(
        boardRuntimeState &&
          ["awaiting_publication", "partial_serving", "post_result_layering"].includes(boardRuntimeState.phase),
      );
      const legacyLifecyclePending = Boolean(
        !boardRuntimeState &&
          lifecycle &&
          ["baseline_serving", "delta_applying", "current_snapshot_materializing", "post_result_layering"].includes(
            lifecycle.state,
          ),
      );
      const legacyProfileTailPending = !boardRuntimeState && resultViewLifecycleHasPendingDeltaProfileWork(lifecycle);
      const legacyProfileWorkPending =
        !boardRuntimeState && profileFetchProgressHasPendingWork(dashboardValue.profileFetchProgress);
      const candidateHydrationPending = dashboardCandidateHydrationPending(dashboardValue);
      return (
        boardRuntimePending ||
        legacyLifecyclePending ||
        legacyProfileTailPending ||
        legacyProfileWorkPending ||
        candidateHydrationPending ||
        round < DASHBOARD_ASSET_REFRESH_MIN_ROUNDS
      );
    };

    const refresh = async () => {
      const currentDashboard = dashboardRef.current;
      if (!shouldContinuePolling(currentDashboard, refreshRound)) {
        return;
      }
      refreshRound += 1;
      try {
        const refreshedDashboard = projectionId
          ? dashboardRef.current
          : await getDashboard(jobId, { forceRefresh: true });
        if (!refreshedDashboard) {
          return;
        }
        if (cancelled) {
          return;
        }
        const latestDashboard = dashboardRef.current || refreshedDashboard;
        const populationShapeChanged = dashboardPopulationShapeChanged(latestDashboard, refreshedDashboard);
        const refreshedTargetCount = refreshedDashboard.boardRuntimeState
          ? dashboardRowHydrationTargetCount(refreshedDashboard)
          : dashboardExpectedCandidateCount(refreshedDashboard);
        const mergedDashboard = populationShapeChanged
          ? refreshedDashboard
          : mergeDashboardCandidatePage(latestDashboard, {
              jobId,
              resultMode: refreshedDashboard.resultMode,
              offset: 0,
              limit: refreshedDashboard.candidates.length,
              returnedCount: refreshedDashboard.candidates.length,
              totalCandidates: refreshedTargetCount,
              filteredCandidateCount: refreshedTargetCount,
              hasMore: dashboardCandidateHydrationPending(refreshedDashboard),
              nextOffset: refreshedDashboard.candidates.length,
              candidates: refreshedDashboard.candidates,
              profileFetchProgress: refreshedDashboard.profileFetchProgress,
              linkedinStage1Progress: refreshedDashboard.linkedinStage1Progress,
              resultViewLifecycle: refreshedDashboard.resultViewLifecycle,
              boardRuntimeState: refreshedDashboard.boardRuntimeState,
              candidateFacetSummary: refreshedDashboard.candidateFacetSummary,
              candidateFacetSummaryScope: refreshedDashboard.candidateFacetSummaryScope,
            });
        const changed = dashboardRefreshChanged(latestDashboard, mergedDashboard);
        if (changed) {
          dashboardRef.current = mergedDashboard;
          storeDashboardCache(jobId, mergedDashboard);
          onDashboardChange(mergedDashboard);
        }
        if (shouldContinuePolling(mergedDashboard, refreshRound)) {
          schedule();
        }
      } catch {
        if (!cancelled && shouldContinuePolling(dashboardRef.current, refreshRound)) {
          schedule();
        }
      }
    };

    schedule();
    return () => {
      cancelled = true;
      if (timeoutId !== null) {
        globalThis.clearTimeout(timeoutId);
      }
    };
  }, [dashboard?.resultMode, jobId, onDashboardChange, projectionId]);

  useEffect(() => {
    if ((!jobId && !projectionId) || !dashboard) {
      setIsHydratingCandidates(false);
      setCandidateHydrationError("");
      return;
    }
    const totalCandidates = dashboard.boardRuntimeState
      ? dashboardRowHydrationTargetCount(dashboard)
      : dashboardExpectedCandidateCount(dashboard);
    const backendCanonicalPagingAvailable = Boolean(
      dashboard.boardRuntimeState?.filterContract?.backendFilteredPagingSupported,
    );
    if (totalCandidates === 0 || dashboard.candidates.length >= totalCandidates) {
      setIsHydratingCandidates(false);
      setCandidateHydrationError("");
      return;
    }
    const requiredTargetCount = Math.min(
      totalCandidates,
      Math.max(requiredCandidateCount, DASHBOARD_INITIAL_REQUIRED_CANDIDATE_COUNT),
    );
    const backgroundTargetCount = Math.min(
      totalCandidates,
      Math.max(requiredTargetCount, backgroundCandidateCount),
    );

    let cancelled = false;
    let idleCallbackId: number | null = null;
    let timeoutId: number | null = null;
    setIsHydratingCandidates(true);
    setCandidateHydrationError("");

    const waitForIdleWindow = async () =>
      new Promise<void>((resolve) => {
        if (typeof window === "undefined") {
          resolve();
          return;
        }
        const idleWindow = window as Window & {
          requestIdleCallback?: (callback: IdleRequestCallback, options?: IdleRequestOptions) => number;
          cancelIdleCallback?: (handle: number) => void;
        };
        if (typeof idleWindow.requestIdleCallback === "function") {
          idleCallbackId = idleWindow.requestIdleCallback(
            () => {
              idleCallbackId = null;
              resolve();
            },
            { timeout: 800 },
          );
          return;
        }
        timeoutId = globalThis.setTimeout(() => {
          timeoutId = null;
          resolve();
        }, 0);
      });

    const hydrateUntil = async (targetCount: number, options?: { yieldBetweenRounds?: boolean }) => {
      while (!cancelled) {
        const currentDashboard = dashboardRef.current;
        if (!currentDashboard) {
          return;
        }
        const loadedCount = currentDashboard.candidates.length;
        const expectedTotal = currentDashboard.boardRuntimeState
          ? dashboardRowHydrationTargetCount(currentDashboard)
          : dashboardExpectedCandidateCount(currentDashboard);
        if (expectedTotal === 0 || loadedCount >= expectedTotal || loadedCount >= targetCount) {
          return;
        }
        const requestedOffsets: number[] = [];
        for (let index = 0; index < DASHBOARD_BACKGROUND_HYDRATION_CONCURRENCY; index += 1) {
          const offset = loadedCount + index * DASHBOARD_BACKGROUND_HYDRATION_LIMIT;
          if (offset >= expectedTotal || offset >= targetCount) {
            break;
          }
          requestedOffsets.push(offset);
        }
        if (requestedOffsets.length === 0) {
          return;
        }
        const pages = await Promise.all(
          requestedOffsets.map((offset) =>
            projectionId
              ? getProjectionCandidatePage(projectionId, {
                  offset,
                  limit: Math.max(
                    1,
                    Math.min(DASHBOARD_BACKGROUND_HYDRATION_LIMIT, targetCount - offset + DASHBOARD_VIEWPORT_PREFETCH_CANDIDATE_COUNT),
                  ),
                  forceRefresh: true,
                })
              : getDashboardCandidatePage(jobId, {
                  offset,
                  limit: Math.max(
                    1,
                    Math.min(DASHBOARD_BACKGROUND_HYDRATION_LIMIT, targetCount - offset + DASHBOARD_VIEWPORT_PREFETCH_CANDIDATE_COUNT),
                  ),
                  forceRefresh: true,
                }),
          ),
        );
        if (cancelled) {
          return;
        }
        let mergedDashboard = currentDashboard;
        for (const page of [...pages].sort((left, right) => left.offset - right.offset)) {
          mergedDashboard = mergeDashboardCandidatePage(mergedDashboard, page);
        }
        dashboardRef.current = mergedDashboard;
        if (projectionId) {
          storeProjectionDashboardCache(projectionId, mergedDashboard);
        } else {
          storeDashboardCache(jobId, mergedDashboard);
        }
        onDashboardChange(mergedDashboard);
        if (mergedDashboard.candidates.length >= expectedTotal || pages.some((page) => !page.hasMore)) {
          return;
        }
        if (options?.yieldBetweenRounds) {
          await waitForIdleWindow();
        }
      }
    };

    const hydrate = async () => {
      await hydrateUntil(requiredTargetCount);
      if (cancelled) {
        return;
      }
      if (
        !backendCanonicalPagingAvailable &&
        backgroundTargetCount > requiredTargetCount &&
        (typeof document === "undefined" || document.visibilityState === "visible")
      ) {
        await waitForIdleWindow();
        if (!cancelled) {
          await hydrateUntil(backgroundTargetCount, { yieldBetweenRounds: true });
        }
      }
      const finalDashboard = dashboardRef.current;
      const finalTotalCandidates = finalDashboard?.boardRuntimeState
        ? dashboardRowHydrationTargetCount(finalDashboard)
        : dashboardExpectedCandidateCount(finalDashboard);
      if (!cancelled && !backendCanonicalPagingAvailable && finalTotalCandidates > 0) {
        await waitForIdleWindow();
        if (!cancelled) {
          await hydrateUntil(finalTotalCandidates, { yieldBetweenRounds: true });
        }
      }
      if (!cancelled) {
        setIsHydratingCandidates(false);
      }
    };

    void hydrate().catch((error) => {
      if (cancelled) {
        return;
      }
      setCandidateHydrationError(error instanceof Error ? error.message : "候选人分块加载失败。");
      setIsHydratingCandidates(false);
    });

    return () => {
      cancelled = true;
      const idleWindow =
        typeof window === "undefined"
          ? null
          : (window as Window & {
              cancelIdleCallback?: (handle: number) => void;
            });
      if (idleCallbackId !== null && typeof idleWindow?.cancelIdleCallback === "function") {
        idleWindow.cancelIdleCallback(idleCallbackId);
      }
      if (timeoutId !== null) {
        globalThis.clearTimeout(timeoutId);
      }
    };
  }, [backgroundCandidateCount, dashboard, jobId, onDashboardChange, projectionId, requiredCandidateCount]);

  return {
    isHydratingCandidates,
    candidateHydrationError,
    loadedCandidateCount: dashboardLoadedCandidateRowCount(dashboard),
  };
}

function dashboardRefreshChanged(current: DashboardData, next: DashboardData): boolean {
  const currentProgress = current.profileFetchProgress;
  const nextProgress = next.profileFetchProgress;
  const currentLifecycle = current.resultViewLifecycle;
  const nextLifecycle = next.resultViewLifecycle;
  const currentLinkedinProgress = current.linkedinStage1Progress;
  const nextLinkedinProgress = next.linkedinStage1Progress;
  return (
    current.totalCandidates !== next.totalCandidates ||
    current.assetPopulationCount !== next.assetPopulationCount ||
    current.candidates.length !== next.candidates.length ||
    candidateRenderSignature(current) !== candidateRenderSignature(next) ||
    candidateFacetSummarySignature(current) !== candidateFacetSummarySignature(next) ||
    (currentProgress?.totalUrlCount || 0) !== (nextProgress?.totalUrlCount || 0) ||
    (currentProgress?.fetchedUrlCount || 0) !== (nextProgress?.fetchedUrlCount || 0) ||
    (currentProgress?.queuedUrlCount || 0) !== (nextProgress?.queuedUrlCount || 0) ||
    (currentProgress?.failedRetryableUrlCount || 0) !== (nextProgress?.failedRetryableUrlCount || 0) ||
    (currentProgress?.unrecoverableUrlCount || 0) !== (nextProgress?.unrecoverableUrlCount || 0) ||
    (currentProgress?.missingRegistryUrlCount || 0) !== (nextProgress?.missingRegistryUrlCount || 0) ||
    (currentProgress?.deferredUrlCount || 0) !== (nextProgress?.deferredUrlCount || 0) ||
    (currentProgress?.pendingUrlCount || 0) !== (nextProgress?.pendingUrlCount || 0) ||
    (currentLifecycle?.state || "") !== (nextLifecycle?.state || "") ||
    (currentLifecycle?.outreachLayeringStatus || "") !== (nextLifecycle?.outreachLayeringStatus || "") ||
    (currentLifecycle?.servedCandidateCount || 0) !== (nextLifecycle?.servedCandidateCount || 0) ||
    (currentLifecycle?.expectedCandidateCount || 0) !== (nextLifecycle?.expectedCandidateCount || 0) ||
    (currentLifecycle?.deltaProfileRequiredCount || 0) !== (nextLifecycle?.deltaProfileRequiredCount || 0) ||
    (currentLifecycle?.deltaProfileFetchedCount || 0) !== (nextLifecycle?.deltaProfileFetchedCount || 0) ||
    (currentLifecycle?.deltaProfileBoardVisibleCount || 0) !==
      (nextLifecycle?.deltaProfileBoardVisibleCount || 0) ||
    (currentLifecycle?.deltaProfileMaterializedCount || 0) !==
      (nextLifecycle?.deltaProfileMaterializedCount || 0) ||
    (currentLifecycle?.deltaProfilePendingCount || 0) !== (nextLifecycle?.deltaProfilePendingCount || 0) ||
    (current.boardRuntimeState?.rowPublicationWatermark || "") !==
      (next.boardRuntimeState?.rowPublicationWatermark || "") ||
    (current.boardRuntimeState?.rowHydrationTargetCount || 0) !==
      (next.boardRuntimeState?.rowHydrationTargetCount || 0) ||
    (current.boardRuntimeState?.displayReadyCandidateCount || 0) !==
      (next.boardRuntimeState?.displayReadyCandidateCount || 0) ||
    (current.boardRuntimeState?.previewCandidateCount || 0) !==
      (next.boardRuntimeState?.previewCandidateCount || 0) ||
    (current.boardRuntimeState?.profileDetailCandidateCount || 0) !==
      (next.boardRuntimeState?.profileDetailCandidateCount || 0) ||
    (current.boardRuntimeState?.explicitProfileCaptureCandidateCount || 0) !==
      (next.boardRuntimeState?.explicitProfileCaptureCandidateCount || 0) ||
    (current.boardRuntimeState?.expectedCandidateCount || 0) !==
      (next.boardRuntimeState?.expectedCandidateCount || 0) ||
    (current.boardRuntimeState?.layeringStatus || "") !==
      (next.boardRuntimeState?.layeringStatus || "") ||
    (currentLinkedinProgress?.profileFetchedCount || 0) !== (nextLinkedinProgress?.profileFetchedCount || 0) ||
    (currentLinkedinProgress?.profileFetchRequiredCount || 0) !==
      (nextLinkedinProgress?.profileFetchRequiredCount || 0)
  );
}

function candidateFacetSummarySignature(dashboard: DashboardData): string {
  const summary = dashboard.candidateFacetSummary;
  if (!summary) {
    return "";
  }
  const sections = [summary.layers, summary.recall, summary.employment, summary.locations, summary.functions];
  return sections
    .map((options) => (options || []).map((option) => `${option.id}:${option.count}`).join(","))
    .join("|");
}

function dashboardPopulationShapeChanged(current: DashboardData, next: DashboardData): boolean {
  return (
    current.resultMode !== next.resultMode ||
    current.snapshotId !== next.snapshotId ||
    dashboardExpectedCandidateCount(current) !== dashboardExpectedCandidateCount(next)
  );
}

function candidateRenderSignature(dashboard: DashboardData): string {
  return dashboard.candidates
    .map((candidate) =>
      [
        candidate.id,
        candidate.outreachLayer ?? "",
        candidate.outreachLayerKey || "",
        candidate.employmentStatus,
        candidate.location || "",
        (candidate.functionIds || []).join(","),
        candidate.roleBucket || "",
        candidate.lowProfileRichness === true ? "low" : "",
        candidate.needsProfileCompletion === true ? "needs_profile_completion" : "",
      ].join(":"),
    )
    .join("|");
}

function dashboardHasActiveAssetLifecycle(dashboardValue: DashboardData | null): boolean {
  if (!dashboardValue || dashboardValue.resultMode !== "asset_population") {
    return false;
  }
  const progress = dashboardValue.profileFetchProgress;
  const lifecycle = dashboardValue.resultViewLifecycle;
  const boardRuntimeState = dashboardValue.boardRuntimeState;
  if (boardRuntimeState) {
    return ["awaiting_publication", "partial_serving", "post_result_layering"].includes(boardRuntimeState.phase);
  }
  const pendingProfileWork = profileFetchProgressHasPendingWork(progress);
  const deltaLifecyclePending = Boolean(
    lifecycle &&
      ["baseline_serving", "delta_applying", "current_snapshot_materializing", "post_result_layering"].includes(
        lifecycle.state,
      ),
  );
  return pendingProfileWork || deltaLifecyclePending || resultViewLifecycleHasPendingDeltaProfileWork(lifecycle);
}

function profileFetchProgressHasPendingWork(progress: DashboardData["profileFetchProgress"]): boolean {
  if (!progress) {
    return false;
  }
  const explicitPendingCount =
    progress.pendingUrlCount +
    progress.queuedUrlCount +
    progress.failedRetryableUrlCount +
    progress.missingRegistryUrlCount +
    progress.deferredUrlCount;
  if (explicitPendingCount > 0) {
    return true;
  }
  const unresolvedCount =
    progress.totalUrlCount - progress.fetchedUrlCount - progress.unrecoverableUrlCount;
  return unresolvedCount > 0;
}

function resultViewLifecycleHasPendingDeltaProfileWork(lifecycle: DashboardData["resultViewLifecycle"]): boolean {
  if (!lifecycle || lifecycle.deltaProfileProgressApplicable === false) {
    return false;
  }
  const requiredCount = Math.max(0, lifecycle.deltaProfileRequiredCount || 0);
  if (requiredCount <= 0) {
    return false;
  }
  const servedCount = Math.max(0, lifecycle.servedCandidateCount || 0);
  const expectedCount = Math.max(0, lifecycle.expectedCandidateCount || 0);
  const servingPhase = lifecycle.servingProjectionPhase || lifecycle.state || "";
  if (
    lifecycle.servedSnapshotId &&
    lifecycle.currentSnapshotId &&
    lifecycle.servedSnapshotId === lifecycle.currentSnapshotId &&
    (servingPhase === "current_snapshot_serving" ||
      lifecycle.state === "current_snapshot_serving" ||
      lifecycle.state === "post_result_layering") &&
    expectedCount > 0 &&
    servedCount >= expectedCount
  ) {
    return false;
  }
  const fetchedCount = Math.max(0, lifecycle.deltaProfileFetchedCount || 0);
  const materializedCount = lifecycleEffectiveDeltaMaterializedCount(lifecycle);
  return fetchedCount < requiredCount || materializedCount < requiredCount;
}
