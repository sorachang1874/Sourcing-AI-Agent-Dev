import { useEffect, useRef, useState } from "react";
import {
  getDashboardCandidatePage,
  mergeDashboardCandidatePage,
  storeDashboardCache,
} from "../lib/api";
import type { DashboardData } from "../types";

const DASHBOARD_INITIAL_REQUIRED_CANDIDATE_COUNT = 96;
const DASHBOARD_BACKGROUND_HYDRATION_LIMIT = 160;
const DASHBOARD_BACKGROUND_HYDRATION_CONCURRENCY = 3;
const DASHBOARD_VIEWPORT_PREFETCH_CANDIDATE_COUNT = 48;

interface UseDashboardCandidateHydrationOptions {
  jobId?: string;
  dashboard: DashboardData | null;
  onDashboardChange: (dashboard: DashboardData) => void;
  requiredCandidateCount?: number;
  backgroundCandidateCount?: number;
}

export function useDashboardCandidateHydration({
  jobId = "",
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
    if (!jobId || !dashboard) {
      setIsHydratingCandidates(false);
      setCandidateHydrationError("");
      return;
    }
    const totalCandidates = Math.max(dashboard.totalCandidates || 0, dashboard.candidates.length);
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
        const expectedTotal = Math.max(currentDashboard.totalCandidates || 0, loadedCount);
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
            getDashboardCandidatePage(jobId, {
              offset,
              limit: Math.max(
                1,
                Math.min(DASHBOARD_BACKGROUND_HYDRATION_LIMIT, targetCount - offset + DASHBOARD_VIEWPORT_PREFETCH_CANDIDATE_COUNT),
              ),
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
        storeDashboardCache(jobId, mergedDashboard);
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
        backgroundTargetCount > requiredTargetCount &&
        (typeof document === "undefined" || document.visibilityState === "visible")
      ) {
        await waitForIdleWindow();
        if (!cancelled) {
          await hydrateUntil(backgroundTargetCount, { yieldBetweenRounds: true });
        }
      }
      const finalDashboard = dashboardRef.current;
      const finalTotalCandidates = Math.max(
        finalDashboard?.totalCandidates || 0,
        finalDashboard?.candidates.length || 0,
      );
      if (!cancelled && finalTotalCandidates > 0) {
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
  }, [backgroundCandidateCount, dashboard, jobId, onDashboardChange, requiredCandidateCount]);

  return {
    isHydratingCandidates,
    candidateHydrationError,
    loadedCandidateCount: dashboard?.candidates.length || 0,
    totalCandidateCount: Math.max(dashboard?.totalCandidates || 0, dashboard?.candidates.length || 0),
  };
}
