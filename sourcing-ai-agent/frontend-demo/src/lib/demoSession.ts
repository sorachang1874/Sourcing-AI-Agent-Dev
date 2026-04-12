import type { DashboardData, DemoPlan, SearchTimelineStep, WorkflowPhase } from "../types";

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
  dashboard: DashboardData | null;
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
  dashboard: null,
};

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
    return {
      ...defaultSessionState,
      ...payload,
    };
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
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(nextState));
  }
  return nextState;
}
