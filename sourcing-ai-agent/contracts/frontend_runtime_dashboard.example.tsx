import type { CSSProperties } from "react";

import type {
  JsonObject,
  ObjectSyncTransferProgressItem,
  RuntimeRecoverableWorkerItem,
  RuntimeStaleJobItem,
  ServiceStatusPayload,
  SystemProgressWorkflowItem,
} from "./frontend_api_contract";
import {
  type HookClientOptions,
  useRuntimeHealth,
  useRuntimeMetrics,
  useSystemProgress,
} from "./frontend_react_hooks.example";

export interface RuntimeDashboardExampleProps extends HookClientOptions {
  pollIntervalMs?: number;
  runtimeFilters?: JsonObject;
  systemFilters?: JsonObject;
  maxWorkflowRows?: number;
  maxStalledJobRows?: number;
  maxRecoverableWorkerRows?: number;
  maxTransferRows?: number;
}

// Reference-only dashboard wiring for runtime observability pages.
export function RuntimeDashboardExample(props: RuntimeDashboardExampleProps) {
  const {
    pollIntervalMs = 5000,
    runtimeFilters = {},
    systemFilters = {},
    maxWorkflowRows = 8,
    maxStalledJobRows = 6,
    maxRecoverableWorkerRows = 6,
    maxTransferRows = 6,
    ...clientOptions
  } = props;

  const runtimeMetrics = useRuntimeMetrics({
    ...clientOptions,
    filters: runtimeFilters,
    pollIntervalMs,
  });
  const runtimeHealth = useRuntimeHealth({
    ...clientOptions,
    filters: runtimeFilters,
    pollIntervalMs,
  });
  const systemProgress = useSystemProgress({
    ...clientOptions,
    filters: systemFilters,
    pollIntervalMs,
  });

  const workflows = (systemProgress.data?.workflow_jobs?.items ?? []).slice(0, maxWorkflowRows);
  const stalledJobs = (runtimeHealth.data?.stalled_jobs ?? []).slice(0, maxStalledJobRows);
  const recoverableWorkers = (runtimeHealth.data?.recoverable_workers?.sample ?? []).slice(
    0,
    maxRecoverableWorkerRows,
  );
  const recentTransfers = (systemProgress.data?.object_sync?.recent_transfers ?? []).slice(0, maxTransferRows);
  const sharedRecovery =
    runtimeHealth.data?.services?.shared_recovery ?? runtimeMetrics.data?.services?.shared_recovery;
  const trackedJobRecoveryCount =
    runtimeMetrics.data?.services?.tracked_job_recovery_count ??
    runtimeHealth.data?.services?.job_recoveries?.length ??
    0;
  const providerStatuses = objectEntries(runtimeHealth.data?.providers);
  const errorList = [runtimeMetrics.error, runtimeHealth.error, systemProgress.error].filter(Boolean) as Error[];

  return (
    <div style={styles.page}>
      <div style={styles.headerRow}>
        <div>
          <h2 style={styles.title}>Runtime Dashboard Example</h2>
          <p style={styles.subtitle}>
            Runtime health, refresh metrics, stalled jobs, worker recovery, workflow progress, and object sync in one
            polling view.
          </p>
        </div>
        <div style={styles.buttonRow}>
          <button style={styles.button} onClick={() => void runtimeMetrics.refresh()}>
            Refresh Metrics
          </button>
          <button style={styles.button} onClick={() => void runtimeHealth.refresh()}>
            Refresh Health
          </button>
          <button style={styles.button} onClick={() => void systemProgress.refresh()}>
            Refresh Progress
          </button>
        </div>
      </div>

      {errorList.length > 0 ? (
        <section style={styles.errorPanel}>
          {errorList.map((error, index) => (
            <div key={`${error.name}-${index}`}>{error.message}</div>
          ))}
        </section>
      ) : null}

      <div style={styles.summaryGrid}>
        <SummaryCard
          label="Runtime Status"
          value={runtimeHealth.data?.status ?? runtimeMetrics.data?.status ?? "loading"}
          detail={compactTimestamp(runtimeHealth.data?.observed_at ?? runtimeMetrics.data?.observed_at)}
        />
        <SummaryCard
          label="Shared Recovery"
          value={sharedRecovery?.status ?? "unknown"}
          detail={sharedRecovery?.service_name ?? sharedRecovery?.detail ?? "not reported"}
        />
        <SummaryCard
          label="Tracked Job Recoveries"
          value={String(trackedJobRecoveryCount)}
          detail={`${runtimeHealth.data?.services?.job_recoveries?.length ?? 0} listed in runtime health`}
        />
        <SummaryCard
          label="Stalled Jobs"
          value={String(runtimeHealth.data?.stalled_jobs?.length ?? 0)}
          detail={`${countStaleJobs(runtimeHealth.data)} stale jobs in acquiring/queued buckets`}
        />
        <SummaryCard
          label="Recoverable Workers"
          value={String(runtimeHealth.data?.recoverable_workers?.count ?? 0)}
          detail={`${recoverableWorkers.length} sample rows`}
        />
        <SummaryCard
          label="Active Object Sync"
          value={String(systemProgress.data?.object_sync?.active_transfer_count ?? 0)}
          detail={`${systemProgress.data?.object_sync?.tracked_bundle_count ?? 0} tracked bundles`}
        />
      </div>

      <div style={styles.twoColumnGrid}>
        <section style={styles.panel}>
          <SectionHeader
            title="Recovery Services"
            detail={`metrics ${compactTimestamp(runtimeMetrics.lastUpdatedAt)} | health ${compactTimestamp(
              runtimeHealth.lastUpdatedAt,
            )}`}
          />
          <KeyValueRow label="Shared daemon" value={sharedRecovery?.service_name ?? "n/a"} />
          <KeyValueRow label="Shared status" value={formatServiceStatus(sharedRecovery)} />
          <KeyValueRow
            label="Shared heartbeat"
            value={compactTimestamp(sharedRecovery?.heartbeat_at ?? sharedRecovery?.updated_at)}
          />
          <KeyValueRow
            label="Tracked job daemons"
            value={String(runtimeMetrics.data?.services?.tracked_job_recovery_count ?? 0)}
          />
          {providerStatuses.length > 0 ? (
            <>
              <div style={styles.subsectionTitle}>Provider Health</div>
              <div style={styles.tagWrap}>
                {providerStatuses.map(([name, value]) => (
                  <span key={name} style={styles.tag}>
                    {name}: {stringifyValue(value)}
                  </span>
                ))}
              </div>
            </>
          ) : (
            <div style={styles.emptyState}>No provider health payload reported yet.</div>
          )}
        </section>

        <section style={styles.panel}>
          <SectionHeader
            title="Refresh Metrics"
            detail={compactTimestamp(runtimeMetrics.data?.observed_at ?? runtimeMetrics.lastUpdatedAt)}
          />
          <KeyValueRow
            label="Pre-retrieval refresh jobs"
            value={stringifyValue(runtimeMetrics.data?.refresh_metrics?.pre_retrieval_refresh_job_count)}
          />
          <KeyValueRow
            label="Inline search workers"
            value={stringifyValue(runtimeMetrics.data?.refresh_metrics?.inline_search_seed_worker_count)}
          />
          <KeyValueRow
            label="Inline harvest workers"
            value={stringifyValue(runtimeMetrics.data?.refresh_metrics?.inline_harvest_prefetch_worker_count)}
          />
          <KeyValueRow
            label="Background search reconciles"
            value={stringifyValue(runtimeMetrics.data?.refresh_metrics?.background_search_seed_reconcile_job_count)}
          />
          <KeyValueRow
            label="Background harvest reconciles"
            value={stringifyValue(runtimeMetrics.data?.refresh_metrics?.background_harvest_prefetch_reconcile_job_count)}
          />
          <KeyValueRow
            label="Background reconcile jobs"
            value={stringifyValue(runtimeMetrics.data?.refresh_metrics?.background_reconcile_job_count)}
          />
        </section>
      </div>

      <div style={styles.twoColumnGrid}>
        <section style={styles.panel}>
          <SectionHeader
            title="Stalled Jobs"
            detail={`${runtimeHealth.data?.stalled_jobs?.length ?? 0} runtime flagged`}
          />
          {stalledJobs.length === 0 ? (
            <div style={styles.emptyState}>No stalled jobs.</div>
          ) : (
            <table style={styles.table}>
              <thead>
                <tr>
                  <th style={styles.tableHeader}>Job</th>
                  <th style={styles.tableHeader}>Stage</th>
                  <th style={styles.tableHeader}>Health</th>
                  <th style={styles.tableHeader}>Updated</th>
                </tr>
              </thead>
              <tbody>
                {stalledJobs.map((item) => (
                  <RuntimeJobRow key={item.job_id ?? `${item.stage}-${item.updated_at}`} item={item} />
                ))}
              </tbody>
            </table>
          )}
        </section>

        <section style={styles.panel}>
          <SectionHeader
            title="Recoverable Workers"
            detail={`${runtimeHealth.data?.recoverable_workers?.count ?? 0} recoverable`}
          />
          {recoverableWorkers.length === 0 ? (
            <div style={styles.emptyState}>No recoverable worker sample rows.</div>
          ) : (
            <table style={styles.table}>
              <thead>
                <tr>
                  <th style={styles.tableHeader}>Worker</th>
                  <th style={styles.tableHeader}>Job</th>
                  <th style={styles.tableHeader}>Lane</th>
                  <th style={styles.tableHeader}>Status</th>
                </tr>
              </thead>
              <tbody>
                {recoverableWorkers.map((worker) => (
                  <RecoverableWorkerRow
                    key={`${worker.worker_id ?? "worker"}-${worker.job_id ?? "job"}`}
                    worker={worker}
                  />
                ))}
              </tbody>
            </table>
          )}
        </section>
      </div>

      <section style={styles.panel}>
        <SectionHeader
          title="Workflow System Progress"
          detail={`${systemProgress.data?.workflow_jobs?.count ?? 0} tracked jobs`}
        />
        {workflows.length === 0 ? (
          <div style={styles.emptyState}>No workflow jobs returned by system progress.</div>
        ) : (
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.tableHeader}>Job</th>
                <th style={styles.tableHeader}>Company</th>
                <th style={styles.tableHeader}>Stage</th>
                <th style={styles.tableHeader}>Runtime Health</th>
                <th style={styles.tableHeader}>Updated</th>
              </tr>
            </thead>
            <tbody>
              {workflows.map((workflow) => (
                <WorkflowRow key={workflow.job_id} workflow={workflow} />
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section style={styles.panel}>
        <SectionHeader
          title="Object Sync"
          detail={`${systemProgress.data?.object_sync?.active_transfer_count ?? 0} active transfers`}
        />
        {recentTransfers.length === 0 ? (
          <div style={styles.emptyState}>No recent object sync transfers.</div>
        ) : (
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.tableHeader}>Bundle</th>
                <th style={styles.tableHeader}>Status</th>
                <th style={styles.tableHeader}>Progress</th>
                <th style={styles.tableHeader}>Updated</th>
              </tr>
            </thead>
            <tbody>
              {recentTransfers.map((transfer) => (
                <ObjectSyncRow
                  key={`${transfer.bundle_id ?? "bundle"}-${transfer.progress_path ?? transfer.updated_at ?? "row"}`}
                  transfer={transfer}
                />
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}

function SummaryCard(props: { label: string; value: string; detail: string }) {
  return (
    <div style={styles.summaryCard}>
      <div style={styles.summaryLabel}>{props.label}</div>
      <div style={styles.summaryValue}>{props.value}</div>
      <div style={styles.summaryDetail}>{props.detail}</div>
    </div>
  );
}

function SectionHeader(props: { title: string; detail?: string }) {
  return (
    <div style={styles.sectionHeader}>
      <h3 style={styles.sectionTitle}>{props.title}</h3>
      <span style={styles.sectionDetail}>{props.detail ?? ""}</span>
    </div>
  );
}

function KeyValueRow(props: { label: string; value: string }) {
  return (
    <div style={styles.keyValueRow}>
      <span style={styles.keyLabel}>{props.label}</span>
      <span style={styles.keyValue}>{props.value}</span>
    </div>
  );
}

function RuntimeJobRow(props: { item: RuntimeStaleJobItem & { runtime_health?: JsonObject } }) {
  return (
    <tr>
      <td style={styles.tableCell}>{props.item.job_id ?? "n/a"}</td>
      <td style={styles.tableCell}>{props.item.stage ?? props.item.status ?? "n/a"}</td>
      <td style={styles.tableCell}>{formatRuntimeHealth(props.item.runtime_health)}</td>
      <td style={styles.tableCell}>{compactTimestamp(props.item.updated_at)}</td>
    </tr>
  );
}

function RecoverableWorkerRow(props: { worker: RuntimeRecoverableWorkerItem }) {
  return (
    <tr>
      <td style={styles.tableCell}>{props.worker.worker_id ?? "n/a"}</td>
      <td style={styles.tableCell}>{props.worker.job_id ?? "n/a"}</td>
      <td style={styles.tableCell}>{props.worker.lane_id ?? "n/a"}</td>
      <td style={styles.tableCell}>{props.worker.status ?? "n/a"}</td>
    </tr>
  );
}

function WorkflowRow(props: { workflow: SystemProgressWorkflowItem }) {
  return (
    <tr>
      <td style={styles.tableCell}>{props.workflow.job_id}</td>
      <td style={styles.tableCell}>{props.workflow.target_company ?? "n/a"}</td>
      <td style={styles.tableCell}>{props.workflow.stage ?? props.workflow.status ?? "n/a"}</td>
      <td style={styles.tableCell}>{formatRuntimeHealth(props.workflow.runtime_health as JsonObject | undefined)}</td>
      <td style={styles.tableCell}>{compactTimestamp(props.workflow.updated_at)}</td>
    </tr>
  );
}

function ObjectSyncRow(props: { transfer: ObjectSyncTransferProgressItem }) {
  return (
    <tr>
      <td style={styles.tableCell}>{props.transfer.bundle_id ?? props.transfer.bundle_kind ?? "n/a"}</td>
      <td style={styles.tableCell}>{props.transfer.status ?? "n/a"}</td>
      <td style={styles.tableCell}>
        {formatPercent(props.transfer.completion_ratio)} ({props.transfer.completed_file_count ?? 0}/
        {props.transfer.requested_file_count ?? 0})
      </td>
      <td style={styles.tableCell}>{compactTimestamp(props.transfer.updated_at)}</td>
    </tr>
  );
}

function formatRuntimeHealth(value: JsonObject | undefined): string {
  if (!value) {
    return "n/a";
  }
  const classification = stringifyValue(value.classification);
  const state = stringifyValue(value.state);
  const detail = stringifyValue(value.detail);
  if (detail !== "n/a") {
    return `${classification} / ${state} - ${detail}`;
  }
  return `${classification} / ${state}`;
}

function formatServiceStatus(service: ServiceStatusPayload | undefined): string {
  if (!service) {
    return "n/a";
  }
  const pieces = [service.status, service.lock_status].filter(Boolean);
  return pieces.length > 0 ? pieces.join(" / ") : "n/a";
}

function formatPercent(value?: number | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Math.round(value * 100)}%`;
}

function compactTimestamp(value?: string | number): string {
  if (!value && value !== 0) {
    return "n/a";
  }
  const parsed = typeof value === "number" ? new Date(value) : new Date(String(value));
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toLocaleString();
}

function countStaleJobs(runtimeHealth: { stale_jobs?: JsonObject } | undefined): number {
  const stale = runtimeHealth?.stale_jobs as
    | {
        acquiring?: RuntimeStaleJobItem[];
        queued?: RuntimeStaleJobItem[];
      }
    | undefined;
  return (stale?.acquiring?.length ?? 0) + (stale?.queued?.length ?? 0);
}

function stringifyValue(value: unknown): string {
  if (typeof value === "string" && value.trim()) {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "n/a";
}

function objectEntries(value: unknown): Array<[string, unknown]> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return [];
  }
  return Object.entries(value as Record<string, unknown>);
}

const styles: Record<string, CSSProperties> = {
  page: {
    display: "grid",
    gap: "16px",
    padding: "20px",
    background: "#f7f8fa",
    color: "#18212f",
    fontFamily: `"IBM Plex Sans", "Segoe UI", sans-serif`,
  },
  headerRow: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "flex-start",
    gap: "16px",
    flexWrap: "wrap",
  },
  title: {
    margin: 0,
    fontSize: "22px",
    lineHeight: 1.2,
  },
  subtitle: {
    margin: "8px 0 0",
    maxWidth: "760px",
    color: "#516074",
    fontSize: "14px",
  },
  buttonRow: {
    display: "flex",
    gap: "8px",
    flexWrap: "wrap",
  },
  button: {
    border: "1px solid #d4dae5",
    borderRadius: "10px",
    background: "#ffffff",
    padding: "8px 12px",
    cursor: "pointer",
    color: "#18212f",
    fontWeight: 600,
  },
  errorPanel: {
    border: "1px solid #f0b3b3",
    background: "#fff0f0",
    color: "#8a1f1f",
    borderRadius: "12px",
    padding: "12px 14px",
    display: "grid",
    gap: "6px",
  },
  summaryGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
    gap: "12px",
  },
  summaryCard: {
    borderRadius: "16px",
    background: "#ffffff",
    border: "1px solid #e3e8ef",
    padding: "14px",
    boxShadow: "0 6px 18px rgba(24,33,47,0.05)",
  },
  summaryLabel: {
    color: "#516074",
    fontSize: "12px",
    textTransform: "uppercase",
    letterSpacing: "0.04em",
  },
  summaryValue: {
    marginTop: "8px",
    fontSize: "28px",
    fontWeight: 700,
  },
  summaryDetail: {
    marginTop: "6px",
    fontSize: "12px",
    color: "#66768a",
  },
  twoColumnGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
    gap: "16px",
  },
  panel: {
    borderRadius: "16px",
    background: "#ffffff",
    border: "1px solid #e3e8ef",
    padding: "16px",
    boxShadow: "0 6px 18px rgba(24,33,47,0.05)",
  },
  sectionHeader: {
    display: "flex",
    justifyContent: "space-between",
    gap: "12px",
    alignItems: "baseline",
    flexWrap: "wrap",
    marginBottom: "12px",
  },
  sectionTitle: {
    margin: 0,
    fontSize: "16px",
  },
  sectionDetail: {
    color: "#66768a",
    fontSize: "12px",
  },
  subsectionTitle: {
    marginTop: "14px",
    marginBottom: "8px",
    fontWeight: 700,
    fontSize: "13px",
  },
  tagWrap: {
    display: "flex",
    gap: "8px",
    flexWrap: "wrap",
  },
  tag: {
    borderRadius: "999px",
    background: "#eef3f9",
    color: "#274164",
    padding: "6px 10px",
    fontSize: "12px",
  },
  keyValueRow: {
    display: "flex",
    justifyContent: "space-between",
    gap: "12px",
    padding: "8px 0",
    borderBottom: "1px solid #eef2f7",
  },
  keyLabel: {
    color: "#516074",
  },
  keyValue: {
    fontWeight: 600,
    textAlign: "right",
  },
  emptyState: {
    color: "#66768a",
    fontSize: "13px",
  },
  table: {
    width: "100%",
    borderCollapse: "collapse",
  },
  tableHeader: {
    textAlign: "left",
    padding: "10px 8px",
    fontSize: "12px",
    color: "#516074",
    borderBottom: "1px solid #dde4ee",
  },
  tableCell: {
    padding: "10px 8px",
    borderBottom: "1px solid #eef2f7",
    fontSize: "13px",
    verticalAlign: "top",
  },
};
