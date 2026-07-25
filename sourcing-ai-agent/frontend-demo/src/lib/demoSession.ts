import type { DemoPlan, SearchTimelineStep, WorkflowPhase } from "../types";

const STORAGE_KEY = "sourcing-ai-agent-demo-session";

export interface DemoSessionState {
  queryText: string;
  plan: DemoPlan | null;
  reviewApproved: boolean;
  lastVisitedStage: "search" | "review" | "run" | "results" | "manual-review" | "outreach" | "candidate";
  activeHistoryId: string;
  phase: WorkflowPhase;
  revisionText: string;
  timelineSteps: SearchTimelineStep[];
  selectedCandidateId: string;
}

const defaultSessionState: DemoSessionState = {
  queryText: "",
  plan: null,
  reviewApproved: false,
  lastVisitedStage: "search",
  activeHistoryId: "",
  phase: "idle",
  revisionText: "",
  timelineSteps: [],
  selectedCandidateId: "",
};

function persistableState(state: DemoSessionState): DemoSessionState {
  return {
    queryText: state.queryText,
    plan: state.plan,
    reviewApproved: state.reviewApproved,
    lastVisitedStage: state.lastVisitedStage,
    activeHistoryId: state.activeHistoryId,
    phase: state.phase,
    revisionText: state.revisionText,
    timelineSteps: state.timelineSteps,
    selectedCandidateId: state.selectedCandidateId,
  };
}

function canUseStorage(): boolean {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

export function readDemoSession(): DemoSessionState {
  if (!canUseStorage()) {
    return defaultSessionState;
  }
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return defaultSessionState;
    }
    const payload = JSON.parse(raw) as Partial<DemoSessionState>;
    const nextState = persistableState({
      ...defaultSessionState,
      ...payload,
    });
    if (Object.prototype.hasOwnProperty.call(payload, "dashboard")) {
      try {
        window.localStorage.setItem(STORAGE_KEY, JSON.stringify(nextState));
      } catch {
        window.localStorage.removeItem(STORAGE_KEY);
      }
    }
    return nextState;
  } catch {
    return defaultSessionState;
  }
}

export function writeDemoSession(patch: Partial<DemoSessionState>): DemoSessionState {
  const nextState = {
    ...readDemoSession(),
    ...patch,
  };
  if (canUseStorage()) {
    const serialized = JSON.stringify(persistableState(nextState));
    try {
      window.localStorage.setItem(STORAGE_KEY, serialized);
    } catch {
      try {
        window.localStorage.removeItem(STORAGE_KEY);
        window.localStorage.setItem(STORAGE_KEY, serialized);
      } catch {
        return defaultSessionState;
      }
    }
  }
  return nextState;
}
