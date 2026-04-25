import type { SearchHistoryItem } from "../types";
import { readDemoSession } from "./demoSession";
import { readSearchHistoryItem } from "./searchHistory";

export interface WorkflowPageContext {
  historyId: string;
  jobId: string;
  candidateId: string;
  historyItem: SearchHistoryItem | null;
}

export function resolveWorkflowPageContext(searchParams: URLSearchParams): WorkflowPageContext {
  const session = readDemoSession();
  const requestedHistoryId = (searchParams.get("history") || "").trim();
  const requestedJobId = (searchParams.get("job") || "").trim();
  const requestedCandidateId = (searchParams.get("candidate") || "").trim();
  const fallbackHistoryId = session.activeHistoryId || "";
  const historyId = requestedHistoryId || fallbackHistoryId;
  const historyItem = historyId ? readSearchHistoryItem(historyId) : null;
  return {
    historyId,
    jobId: requestedJobId || historyItem?.jobId || "",
    candidateId: requestedCandidateId || historyItem?.selectedCandidateId || "",
    historyItem,
  };
}

export function buildWorkflowRoute(
  pathname: string,
  context: {
    historyId?: string;
    jobId?: string;
    candidateId?: string;
  },
): string {
  const params = new URLSearchParams();
  if (context.historyId) {
    params.set("history", context.historyId);
  }
  if (context.jobId) {
    params.set("job", context.jobId);
  }
  if (context.candidateId) {
    params.set("candidate", context.candidateId);
  }
  const query = params.toString();
  return query ? `${pathname}?${query}` : pathname;
}
