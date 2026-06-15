import type { ResultViewLifecycle } from "../types";

function positiveInteger(value: number | undefined | null): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.floor(value));
}

export function lifecycleCurrentSnapshotServingComplete(
  lifecycle: ResultViewLifecycle | undefined,
): boolean {
  if (!lifecycle || lifecycle.deltaProfileProgressApplicable === false) {
    return false;
  }
  const state = lifecycle.state || "";
  const terminalServingState =
    state === "current_snapshot_serving" ||
    state === "post_result_layering" ||
    state === "current_serving" ||
    lifecycle.servingProjectionPhase === "current_snapshot_serving";
  const currentSnapshotId = lifecycle.currentSnapshotId || "";
  const servedSnapshotId = lifecycle.servedSnapshotId || "";
  const requiredCount = positiveInteger(lifecycle.deltaProfileRequiredCount);
  const materializedCount = positiveInteger(lifecycle.deltaProfileMaterializedCount);
  const servedCount = positiveInteger(lifecycle.servedCandidateCount);
  const expectedCount = positiveInteger(lifecycle.expectedCandidateCount);
  return Boolean(
    terminalServingState &&
      currentSnapshotId &&
      servedSnapshotId === currentSnapshotId &&
      expectedCount > 0 &&
      servedCount >= expectedCount &&
      requiredCount > 0 &&
      materializedCount >= requiredCount,
  );
}

export function lifecycleEffectiveDeltaMaterializedCount(
  lifecycle: ResultViewLifecycle | undefined,
): number {
  if (!lifecycle || lifecycle.deltaProfileProgressApplicable === false) {
    return 0;
  }
  const materializedCount = positiveInteger(lifecycle.deltaProfileMaterializedCount);
  const boardVisibleCount = positiveInteger(lifecycle.deltaProfileBoardVisibleCount);
  if (lifecycleCurrentSnapshotServingComplete(lifecycle)) {
    return materializedCount;
  }
  return typeof lifecycle.deltaProfileBoardVisibleCount === "number" ? boardVisibleCount : materializedCount;
}
