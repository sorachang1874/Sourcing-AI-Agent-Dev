import { useEffect, useMemo, useState } from "react";

interface ExcelWorkflowBatchGroup {
  company: string;
  historyId: string;
  jobId: string;
  queryText: string;
  rowCount: number;
  sourceCompanies: string[];
  status: string;
  currentMessage: string;
}

interface ExcelWorkflowBatchLaunch {
  batchId: string;
  inputFilename: string;
  totalRowCount: number;
  createdJobCount: number;
  groupCount: number;
  unassignedRowCount: number;
  unassignedRows: Array<{
    rowKey: string;
    name: string;
    company: string;
    title: string;
  }>;
  groups: ExcelWorkflowBatchGroup[];
}

interface ExcelWorkflowIntakePanelProps {
  isSubmitting: boolean;
  onLaunch: (payload: {
    file: File;
    filename: string;
  }) => Promise<ExcelWorkflowBatchLaunch>;
  onPollGroupProgress: (jobId: string) => Promise<{
    status: string;
    currentMessage: string;
  }>;
  onOpenHistory: (historyId: string, jobId: string) => void;
  onImportTargetCandidates: (jobId: string, historyId: string) => Promise<number>;
  onExportTargetCandidates: (jobId: string, historyId: string) => Promise<{
    blob: Blob;
    filename: string;
  }>;
}

function isTerminalStatus(status: string): boolean {
  return ["completed", "failed"].includes(status.trim().toLowerCase());
}

function isCompletedStatus(status: string): boolean {
  return status.trim().toLowerCase() === "completed";
}

function batchStatusSummary(groups: ExcelWorkflowBatchGroup[]): string {
  const completedCount = groups.filter((group) => group.status.trim().toLowerCase() === "completed").length;
  if (completedCount === groups.length && groups.length > 0) {
    return "全部子工作流已完成。";
  }
  return `已完成 ${completedCount} / ${groups.length} 个子工作流。`;
}

export function ExcelWorkflowIntakePanel({
  isSubmitting,
  onLaunch,
  onPollGroupProgress,
  onOpenHistory,
  onImportTargetCandidates,
  onExportTargetCandidates,
}: ExcelWorkflowIntakePanelProps) {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [selectedFileMessage, setSelectedFileMessage] = useState("");
  const [batchLaunch, setBatchLaunch] = useState<ExcelWorkflowBatchLaunch | null>(null);
  const [groupActionMessageByJobId, setGroupActionMessageByJobId] = useState<Record<string, string>>({});
  const [runningGroupActionByJobId, setRunningGroupActionByJobId] = useState<Record<string, boolean>>({});

  const handleLaunch = async () => {
    if (!selectedFile) {
      setErrorMessage("请先上传 Excel 文件。");
      return;
    }
      setErrorMessage("");
      try {
      const launched = await onLaunch({
        file: selectedFile,
        filename: selectedFile.name,
      });
      setBatchLaunch(launched);
      setGroupActionMessageByJobId({});
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Excel 批量导入启动失败。");
    }
  };

  const setGroupActionState = (jobId: string, running: boolean) => {
    setRunningGroupActionByJobId((current) => ({
      ...current,
      [jobId]: running,
    }));
  };

  const setGroupActionMessage = (jobId: string, message: string) => {
    setGroupActionMessageByJobId((current) => ({
      ...current,
      [jobId]: message,
    }));
  };

  const handleImportTargets = async (group: ExcelWorkflowBatchGroup) => {
    if (!group.jobId) {
      return;
    }
    setGroupActionState(group.jobId, true);
    setGroupActionMessage(group.jobId, "");
    try {
      const importedCount = await onImportTargetCandidates(group.jobId, group.historyId);
      setGroupActionMessage(group.jobId, `已导入 ${importedCount} 位目标候选人。`);
    } catch (error) {
      setGroupActionMessage(group.jobId, error instanceof Error ? error.message : "导入目标候选人失败。");
    } finally {
      setGroupActionState(group.jobId, false);
    }
  };

  const handleExportTargets = async (group: ExcelWorkflowBatchGroup) => {
    if (!group.jobId) {
      return;
    }
    setGroupActionState(group.jobId, true);
    setGroupActionMessage(group.jobId, "");
    try {
      const download = await onExportTargetCandidates(group.jobId, group.historyId);
      const url = URL.createObjectURL(download.blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = download.filename || "target-candidates.zip";
      anchor.click();
      URL.revokeObjectURL(url);
      setGroupActionMessage(group.jobId, "目标候选人包已开始下载。");
    } catch (error) {
      setGroupActionMessage(group.jobId, error instanceof Error ? error.message : "导出目标候选人包失败。");
    } finally {
      setGroupActionState(group.jobId, false);
    }
  };

  useEffect(() => {
    if (!batchLaunch) {
      return;
    }
    const pendingGroups = batchLaunch.groups.filter((group) => group.jobId && !isTerminalStatus(group.status));
    if (pendingGroups.length === 0) {
      return;
    }
    let cancelled = false;
    const timer = window.setTimeout(() => {
      void Promise.all(
        pendingGroups.map(async (group) => {
          const nextProgress = await onPollGroupProgress(group.jobId).catch(() => null);
          return {
            jobId: group.jobId,
            status: String(nextProgress?.status || group.status || ""),
            currentMessage: String(nextProgress?.currentMessage || group.currentMessage || ""),
          };
        }),
      ).then((updates) => {
        if (cancelled) {
          return;
        }
        setBatchLaunch((current) => {
          if (!current) {
            return current;
          }
          const progressByJobId = new Map(updates.map((item) => [item.jobId, item]));
          return {
            ...current,
            groups: current.groups.map((group) => {
              const next = progressByJobId.get(group.jobId);
              return next
                ? {
                    ...group,
                    status: next.status,
                    currentMessage: next.currentMessage,
                  }
                : group;
            }),
          };
        });
      });
    }, 2500);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [batchLaunch, onPollGroupProgress]);

  const batchSummary = useMemo(
    () => (batchLaunch ? batchStatusSummary(batchLaunch.groups) : ""),
    [batchLaunch],
  );

  return (
    <section className="excel-workflow-launcher" data-testid="excel-intake-panel">
      <div className="excel-workflow-launcher__header">
        <p className="eyebrow">Excel Intake</p>
        <h3>从 Excel 批量导入候选人</h3>
        <p>系统会先解析 Excel，再按 company 自动拆成多个工作流，并持续回填每个公司的导入进度。</p>
      </div>

      <div className="excel-workflow-launcher__controls">
        <div className="excel-workflow-launcher__upload">
          <p className="field-label">上传 Excel</p>
          <input
            className="supplement-file-input"
            data-testid="excel-intake-file-input"
            type="file"
            accept=".xlsx,.xls,.csv"
            onChange={(event) => {
              const file = event.target.files?.[0] || null;
              setSelectedFile(file);
              setErrorMessage("");
              setSelectedFileMessage(file ? `已选择 ${file.name}` : "");
              setBatchLaunch(null);
            }}
          />
          <p className="excel-workflow-launcher__hint">
            {selectedFileMessage || "支持按 company 列自动拆分多公司多 job。"}
          </p>
        </div>

        <div className="excel-workflow-launcher__cta">
          <button
            type="button"
            className="primary-button excel-workflow-launcher__submit"
            data-testid="excel-intake-submit"
            disabled={isSubmitting}
            onClick={() => {
              void handleLaunch();
            }}
          >
            {isSubmitting ? "拆分中..." : "批量拉取候选人信息"}
          </button>
          <p className="excel-workflow-launcher__hint">
            导入后会为每个公司创建独立 history / workflow，检索方案页默认不适用。
          </p>
        </div>
      </div>

      {batchLaunch ? (
        <div className="excel-workflow-launcher__result" data-testid="excel-intake-result">
          <div className="excel-workflow-launcher__result-head">
            <div>
              <strong>{batchLaunch.inputFilename || "Excel 批量导入"}</strong>
              <p>
                共识别 {batchLaunch.totalRowCount} 行，拆成 {batchLaunch.createdJobCount} 个公司工作流。
              </p>
            </div>
            <span className="phase-pill phase-running">{batchSummary}</span>
          </div>

          {batchLaunch.unassignedRowCount > 0 ? (
            <div className="excel-workflow-launcher__unassigned">
              <strong>未自动拆分的行</strong>
              <p>{batchLaunch.unassignedRowCount} 行缺少可识别 company，暂未创建子工作流。</p>
            </div>
          ) : null}

          <div className="excel-workflow-launcher__groups">
            {batchLaunch.groups.map((group) => (
              <div
                key={group.jobId || `${group.company}-${group.historyId}`}
                className="excel-workflow-launcher__group"
                data-testid="excel-intake-group"
              >
                <div>
                  <strong>{group.company}</strong>
                  <p>
                    {group.rowCount} 行
                    {group.sourceCompanies.length > 0 ? ` · 来源: ${group.sourceCompanies.join(" / ")}` : ""}
                  </p>
                  <p>{group.currentMessage || "子工作流已创建，等待执行。"}</p>
                </div>
                <div className="excel-workflow-launcher__group-actions">
                  <span className={`phase-pill phase-${group.status.trim().toLowerCase() || "idle"}`}>
                    {group.status || "queued"}
                  </span>
                  <button
                    type="button"
                    className="ghost-button"
                    onClick={() => onOpenHistory(group.historyId, group.jobId)}
                  >
                    打开工作流
                  </button>
                  <button
                    type="button"
                    className="ghost-button"
                    disabled={!isCompletedStatus(group.status) || Boolean(runningGroupActionByJobId[group.jobId])}
                    onClick={() => {
                      void handleImportTargets(group);
                    }}
                  >
                    导入目标候选人
                  </button>
                  <button
                    type="button"
                    className="ghost-button"
                    disabled={!isCompletedStatus(group.status) || Boolean(runningGroupActionByJobId[group.jobId])}
                    onClick={() => {
                      void handleExportTargets(group);
                    }}
                  >
                    导出候选人包
                  </button>
                  {groupActionMessageByJobId[group.jobId] ? (
                    <p className="excel-workflow-launcher__hint">{groupActionMessageByJobId[group.jobId]}</p>
                  ) : null}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {errorMessage ? (
        <div className="warning-card error-card excel-workflow-launcher__error" data-testid="excel-intake-error">
          <strong>执行失败</strong>
          <p>{errorMessage}</p>
        </div>
      ) : null}
    </section>
  );
}
