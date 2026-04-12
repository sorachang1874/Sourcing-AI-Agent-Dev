import { useEffect, useRef, useState } from "react";

import {
  SourcingAgentApiError,
  createSourcingAgentApiClient,
  type SourcingAgentApiClient,
  type SourcingAgentApiClientOptions,
} from "./frontend_api_adapter";
import type {
  JobProgressResponse,
  JobResultsResponse,
  JsonObject,
  PlanResponse,
  ReviewInstructionCompileResponse,
  RuntimeHealthResponse,
  RuntimeMetricsResponse,
  SystemProgressResponse,
  WorkflowStartResponse,
  WorkflowStageSummariesPayload,
  WorkflowStageSummaryItem,
} from "./frontend_api_contract";

export interface HookClientOptions extends SourcingAgentApiClientOptions {
  client?: SourcingAgentApiClient;
}

export interface HookState<T> {
  data?: T;
  loading: boolean;
  refreshing: boolean;
  error?: Error;
  lastUpdatedAt?: number;
}

export interface MutationHookState<T> extends HookState<T> {
  run: (payload: JsonObject) => Promise<T | undefined>;
  reset: () => void;
}

export interface QueryHookState<T> extends HookState<T> {
  refresh: () => Promise<T | undefined>;
  reset: () => void;
}

export interface JobProgressHookOptions extends HookClientOptions {
  enabled?: boolean;
  pollIntervalMs?: number;
  stopWhenTerminal?: boolean;
}

export interface JobResultsHookOptions extends HookClientOptions {
  enabled?: boolean;
}

export interface SystemProgressHookOptions extends HookClientOptions {
  enabled?: boolean;
  pollIntervalMs?: number;
  filters?: JsonObject;
}

export interface RuntimeStatusHookOptions extends HookClientOptions {
  enabled?: boolean;
  pollIntervalMs?: number;
  filters?: JsonObject;
}

export interface WorkflowRunHookOptions extends JobProgressHookOptions {
  autoLoadResults?: boolean;
}

export interface WorkflowRunState {
  jobId?: string;
  start: (payload: JsonObject) => Promise<WorkflowStartResponse | undefined>;
  startState: MutationHookState<WorkflowStartResponse>;
  progress: QueryHookState<JobProgressResponse>;
  results: QueryHookState<JobResultsResponse>;
  stageSummaries: WorkflowStageSummaryItem[];
  reset: () => void;
}

export const WORKFLOW_STAGE_DISPLAY_LABELS: Record<string, string> = {
  linkedin_stage_1: "LinkedIn Stage 1 completed",
  stage_1_preview: "Stage 1 preview completed",
  public_web_stage_2: "Public Web Stage 2 completed",
  stage_2_final: "Stage 2 final analysis completed",
};

export function useSourcingPlan(options: HookClientOptions = {}): MutationHookState<PlanResponse> {
  return useJsonMutation((client, payload) => client.plan(payload), options);
}

export function useReviewInstructionPreview(
  options: HookClientOptions = {},
): MutationHookState<ReviewInstructionCompileResponse> {
  return useJsonMutation((client, payload) => client.compilePlanReviewInstruction(payload), options);
}

export function useStartWorkflow(
  options: HookClientOptions = {},
): MutationHookState<WorkflowStartResponse> {
  return useJsonMutation((client, payload) => client.startWorkflow(payload), options);
}

export function useJobProgress(
  jobId?: string,
  options: JobProgressHookOptions = {},
): QueryHookState<JobProgressResponse> {
  const [data, setData] = useState<JobProgressResponse | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | undefined>(undefined);
  const dataRef = useRef<JobProgressResponse | undefined>(undefined);
  const requestIdRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const {
    enabled = true,
    pollIntervalMs = 5000,
    stopWhenTerminal = true,
    ...clientOptions
  } = options;

  function clearTimer() {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = undefined;
    }
  }

  function reset() {
    clearTimer();
    requestIdRef.current += 1;
    dataRef.current = undefined;
    setData(undefined);
    setLoading(false);
    setRefreshing(false);
    setError(undefined);
    setLastUpdatedAt(undefined);
  }

  async function refresh(): Promise<JobProgressResponse | undefined> {
    if (!jobId) {
      return undefined;
    }
    const requestId = ++requestIdRef.current;
    const previousData = dataRef.current;
    setError(undefined);
    setLoading((previous) => previous || !previousData);
    setRefreshing(Boolean(previousData));
    try {
      const response = await getApiClient(clientOptions).getJobProgress(jobId);
      if (requestId !== requestIdRef.current) {
        return response;
      }
      dataRef.current = response;
      setData(response);
      setLoading(false);
      setRefreshing(false);
      setLastUpdatedAt(Date.now());
      return response;
    } catch (requestError) {
      if (requestId !== requestIdRef.current) {
        return undefined;
      }
      setError(asError(requestError));
      setLoading(false);
      setRefreshing(false);
      return undefined;
    }
  }

  useEffect(() => {
    clearTimer();
    requestIdRef.current += 1;
    dataRef.current = undefined;
    setData(undefined);
    setLoading(false);
    setRefreshing(false);
    setError(undefined);
    setLastUpdatedAt(undefined);
  }, [jobId]);

  useEffect(() => {
    if (!jobId || !enabled) {
      clearTimer();
      return;
    }

    let disposed = false;

    async function tick() {
      const response = await refresh();
      if (disposed || !jobId) {
        return;
      }
      if (!response) {
        timerRef.current = setTimeout(() => {
          void tick();
        }, pollIntervalMs);
        return;
      }
      if (stopWhenTerminal && isTerminalStatus(response.status)) {
        clearTimer();
        return;
      }
      timerRef.current = setTimeout(() => {
        void tick();
      }, pollIntervalMs);
    }

    void tick();

    return () => {
      disposed = true;
      clearTimer();
    };
  }, [jobId, enabled, pollIntervalMs, stopWhenTerminal, options.client, options.baseUrl, options.fetchImpl]);

  return {
    data,
    loading,
    refreshing,
    error,
    lastUpdatedAt,
    refresh,
    reset,
  };
}

export function useJobResults(
  jobId?: string,
  options: JobResultsHookOptions = {},
): QueryHookState<JobResultsResponse> {
  const [data, setData] = useState<JobResultsResponse | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | undefined>(undefined);
  const dataRef = useRef<JobResultsResponse | undefined>(undefined);
  const requestIdRef = useRef(0);
  const { enabled = true, ...clientOptions } = options;

  function reset() {
    requestIdRef.current += 1;
    dataRef.current = undefined;
    setData(undefined);
    setLoading(false);
    setRefreshing(false);
    setError(undefined);
    setLastUpdatedAt(undefined);
  }

  async function refresh(): Promise<JobResultsResponse | undefined> {
    if (!jobId) {
      return undefined;
    }
    const requestId = ++requestIdRef.current;
    const previousData = dataRef.current;
    setError(undefined);
    setLoading((previous) => previous || !previousData);
    setRefreshing(Boolean(previousData));
    try {
      const response = await getApiClient(clientOptions).getJobResults(jobId);
      if (requestId !== requestIdRef.current) {
        return response;
      }
      dataRef.current = response;
      setData(response);
      setLoading(false);
      setRefreshing(false);
      setLastUpdatedAt(Date.now());
      return response;
    } catch (requestError) {
      if (requestId !== requestIdRef.current) {
        return undefined;
      }
      setError(asError(requestError));
      setLoading(false);
      setRefreshing(false);
      return undefined;
    }
  }

  useEffect(() => {
    reset();
  }, [jobId]);

  useEffect(() => {
    if (!jobId || !enabled) {
      return;
    }
    void refresh();
  }, [jobId, enabled, options.client, options.baseUrl, options.fetchImpl]);

  return {
    data,
    loading,
    refreshing,
    error,
    lastUpdatedAt,
    refresh,
    reset,
  };
}

export function useSystemProgress(
  options: SystemProgressHookOptions = {},
): QueryHookState<SystemProgressResponse> {
  const { enabled = true, pollIntervalMs = 5000, filters = {}, ...clientOptions } = options;
  return usePollingJsonQuery(
    (client) => client.getSystemProgress(filters),
    {
      ...clientOptions,
      enabled,
      pollIntervalMs,
      identityKey: JSON.stringify(filters),
    },
  );
}

export function useRuntimeMetrics(
  options: RuntimeStatusHookOptions = {},
): QueryHookState<RuntimeMetricsResponse> {
  const { enabled = true, pollIntervalMs = 5000, filters = {}, ...clientOptions } = options;
  return usePollingJsonQuery(
    (client) => client.getRuntimeMetrics(filters),
    {
      ...clientOptions,
      enabled,
      pollIntervalMs,
      identityKey: JSON.stringify(filters),
    },
  );
}

export function useRuntimeHealth(
  options: RuntimeStatusHookOptions = {},
): QueryHookState<RuntimeHealthResponse> {
  const { enabled = true, pollIntervalMs = 5000, filters = {}, ...clientOptions } = options;
  return usePollingJsonQuery(
    (client) => client.getRuntimeHealth(filters),
    {
      ...clientOptions,
      enabled,
      pollIntervalMs,
      identityKey: JSON.stringify(filters),
    },
  );
}

export function useWorkflowRun(options: WorkflowRunHookOptions = {}): WorkflowRunState {
  const startState = useStartWorkflow(options);
  const [jobId, setJobId] = useState<string | undefined>(undefined);
  const progress = useJobProgress(jobId, options);
  const results = useJobResults(jobId, {
    ...options,
    enabled: Boolean(jobId) && Boolean(options.autoLoadResults ?? true) && progress.data?.status === "completed",
  });
  const stageSummaries = getOrderedWorkflowStageSummaries(results.data ?? progress.data);

  async function start(payload: JsonObject): Promise<WorkflowStartResponse | undefined> {
    results.reset();
    progress.reset();
    setJobId(undefined);
    const response = await startState.run(payload);
    if (response?.job_id) {
      setJobId(response.job_id);
    }
    return response;
  }

  function reset() {
    setJobId(undefined);
    startState.reset();
    progress.reset();
    results.reset();
  }

  return {
    jobId,
    start,
    startState,
    progress,
    results,
    stageSummaries,
    reset,
  };
}

export function getOrderedWorkflowStageSummaries(
  payload?: { workflow_stage_summaries?: WorkflowStageSummariesPayload },
): WorkflowStageSummaryItem[] {
  const stagePayload = payload?.workflow_stage_summaries;
  if (!stagePayload) {
    return [];
  }
  const orderedEntries = stagePayload.stage_order
    .map((stageName) => {
      const summary = stagePayload.summaries?.[stageName];
      if (!summary) {
        return undefined;
      }
      return summary.stage ? summary : { ...summary, stage: stageName };
    })
    .filter((item): item is WorkflowStageSummaryItem => Boolean(item));
  return orderedEntries;
}

export function getWorkflowStageDisplayLabel(stageName?: string): string {
  if (!stageName) {
    return "Unknown workflow stage";
  }
  return WORKFLOW_STAGE_DISPLAY_LABELS[stageName] ?? stageName;
}

function useJsonMutation<T>(
  runner: (client: SourcingAgentApiClient, payload: JsonObject) => Promise<T>,
  options: HookClientOptions,
): MutationHookState<T> {
  const [data, setData] = useState<T | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | undefined>(undefined);
  const dataRef = useRef<T | undefined>(undefined);
  const requestIdRef = useRef(0);

  function reset() {
    requestIdRef.current += 1;
    dataRef.current = undefined;
    setData(undefined);
    setLoading(false);
    setRefreshing(false);
    setError(undefined);
    setLastUpdatedAt(undefined);
  }

  async function run(payload: JsonObject): Promise<T | undefined> {
    const requestId = ++requestIdRef.current;
    const previousData = dataRef.current;
    setError(undefined);
    setLoading((previous) => previous || !previousData);
    setRefreshing(Boolean(previousData));
    try {
      const response = await runner(getApiClient(options), payload);
      if (requestId !== requestIdRef.current) {
        return response;
      }
      dataRef.current = response;
      setData(response);
      setLoading(false);
      setRefreshing(false);
      setLastUpdatedAt(Date.now());
      return response;
    } catch (requestError) {
      if (requestId !== requestIdRef.current) {
        return undefined;
      }
      setError(asError(requestError));
      setLoading(false);
      setRefreshing(false);
      return undefined;
    }
  }

  return {
    data,
    loading,
    refreshing,
    error,
    lastUpdatedAt,
    run,
    reset,
  };
}

function usePollingJsonQuery<T>(
  runner: (client: SourcingAgentApiClient) => Promise<T>,
  options: HookClientOptions & {
    enabled?: boolean;
    pollIntervalMs?: number;
    identityKey?: string;
  },
): QueryHookState<T> {
  const [data, setData] = useState<T | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | undefined>(undefined);
  const dataRef = useRef<T | undefined>(undefined);
  const requestIdRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const { enabled = true, pollIntervalMs = 5000, identityKey = "", ...clientOptions } = options;

  function clearTimer() {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = undefined;
    }
  }

  function reset() {
    clearTimer();
    requestIdRef.current += 1;
    dataRef.current = undefined;
    setData(undefined);
    setLoading(false);
    setRefreshing(false);
    setError(undefined);
    setLastUpdatedAt(undefined);
  }

  async function refresh(): Promise<T | undefined> {
    const requestId = ++requestIdRef.current;
    const previousData = dataRef.current;
    setError(undefined);
    setLoading((previous) => previous || !previousData);
    setRefreshing(Boolean(previousData));
    try {
      const response = await runner(getApiClient(clientOptions));
      if (requestId !== requestIdRef.current) {
        return response;
      }
      dataRef.current = response;
      setData(response);
      setLoading(false);
      setRefreshing(false);
      setLastUpdatedAt(Date.now());
      return response;
    } catch (requestError) {
      if (requestId !== requestIdRef.current) {
        return undefined;
      }
      setError(asError(requestError));
      setLoading(false);
      setRefreshing(false);
      return undefined;
    }
  }

  useEffect(() => {
    reset();
  }, [options.client, options.baseUrl, options.fetchImpl, identityKey]);

  useEffect(() => {
    if (!enabled) {
      clearTimer();
      return;
    }

    let disposed = false;

    async function tick() {
      const response = await refresh();
      if (disposed) {
        return;
      }
      if (!response) {
        timerRef.current = setTimeout(() => {
          void tick();
        }, pollIntervalMs);
        return;
      }
      timerRef.current = setTimeout(() => {
        void tick();
      }, pollIntervalMs);
    }

    void tick();

    return () => {
      disposed = true;
      clearTimer();
    };
  }, [enabled, pollIntervalMs, options.client, options.baseUrl, options.fetchImpl, identityKey]);

  return {
    data,
    loading,
    refreshing,
    error,
    lastUpdatedAt,
    refresh,
    reset,
  };
}

function getApiClient(options: HookClientOptions): SourcingAgentApiClient {
  if (options.client) {
    return options.client;
  }
  const { client, ...clientOptions } = options;
  return createSourcingAgentApiClient(clientOptions);
}

function isTerminalStatus(status?: string): boolean {
  return status === "completed" || status === "failed" || status === "cancelled";
}

function asError(error: unknown): Error {
  if (error instanceof SourcingAgentApiError) {
    return error;
  }
  if (error instanceof Error) {
    return error;
  }
  return new Error("Unknown SourcingAgent hook error");
}
