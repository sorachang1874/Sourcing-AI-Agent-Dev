import { useState } from "react";
import {
  continueExcelIntakeReview,
  ingestExcelContacts,
  supplementCompanyAssets,
} from "../lib/api";
import type {
  ExcelIntakeResponse,
  ExcelIntakeReviewOption,
  ExcelIntakeRowResult,
  SupplementOperationResult,
} from "../types";

function mergeIntakeResult(
  previous: ExcelIntakeResponse | null,
  next: ExcelIntakeResponse,
): ExcelIntakeResponse {
  if (!previous) {
    return next;
  }
  const rowsByKey = new Map<string, ExcelIntakeRowResult>();
  for (const row of previous.results || []) {
    rowsByKey.set(row.row_key, row);
  }
  for (const row of next.results || []) {
    rowsByKey.set(row.row_key, row);
  }
  return {
    ...previous,
    ...next,
    intake_id: previous.intake_id || next.intake_id,
    results: Array.from(rowsByKey.values()),
    attachment: next.attachment || previous.attachment,
    attachment_summary: next.attachment_summary || previous.attachment_summary,
  };
}

function buildLocalCandidateChoice(option: ExcelIntakeReviewOption): {
  candidateId: string;
  label: string;
} | null {
  const candidate = option.candidate && typeof option.candidate === "object" ? option.candidate : null;
  const candidateId = String(
    (candidate && "candidate_id" in candidate ? candidate.candidate_id : option.candidate_id) || "",
  ).trim();
  if (!candidateId) {
    return null;
  }
  const displayName = String(
    (candidate && ("display_name" in candidate ? candidate.display_name : candidate.name_en)) || "本地候选人",
  ).trim();
  const role = String((candidate && "role" in candidate ? candidate.role : "") || "").trim();
  return {
    candidateId,
    label: role ? `${displayName} · ${role}` : displayName,
  };
}

function buildSearchProfileChoice(option: ExcelIntakeReviewOption): {
  profileUrl: string;
  label: string;
} | null {
  const profileUrl = String(option.profile_url || "").trim();
  if (!profileUrl) {
    return null;
  }
  const displayName = String(option.full_name || "候选主页").trim();
  const headline = String(option.headline || "").trim();
  return {
    profileUrl,
    label: headline ? `${displayName} · ${headline}` : displayName,
  };
}

function countStatuses(rows: ExcelIntakeRowResult[]): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const row of rows) {
    const status = String(row.status || "").trim();
    if (!status) {
      continue;
    }
    counts[status] = (counts[status] || 0) + 1;
  }
  return counts;
}

export function SupplementIntakePanel() {
  const [targetCompany, setTargetCompany] = useState("Anthropic");
  const [snapshotId, setSnapshotId] = useState("");
  const [attachToSnapshot, setAttachToSnapshot] = useState(true);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [packageResult, setPackageResult] = useState<SupplementOperationResult | null>(null);
  const [intakeResult, setIntakeResult] = useState<ExcelIntakeResponse | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [activeAction, setActiveAction] = useState("");

  const rows = intakeResult?.results || [];
  const statusCounts = countStatuses(rows);
  const pendingRows = rows.filter((row) =>
    ["manual_review_local", "manual_review_search", "unresolved"].includes(String(row.status || "").trim()),
  );
  const attachmentSummary = intakeResult?.attachment_summary || null;

  const handlePackageImport = async () => {
    setActiveAction("package-import");
    setErrorMessage("");
    try {
      const response = await supplementCompanyAssets({
        targetCompany,
        snapshotId,
        importLocalBootstrapPackage: true,
        syncProjectLocalPackage: true,
        buildArtifacts: true,
      });
      setPackageResult(response);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Anthropic 本地资产包导入失败。");
    } finally {
      setActiveAction("");
    }
  };

  const handleExcelUpload = async () => {
    if (!selectedFile) {
      setErrorMessage("请选择一个 Excel 文件。");
      return;
    }
    setActiveAction("excel-upload");
    setErrorMessage("");
    try {
      const response = await ingestExcelContacts({
        file: selectedFile,
        filename: selectedFile.name,
        targetCompany,
        snapshotId,
        attachToSnapshot,
        buildArtifacts: true,
      });
      setIntakeResult(response);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Excel 导入失败。");
    } finally {
      setActiveAction("");
    }
  };

  const handleDecision = async (row: ExcelIntakeRowResult, decision: Record<string, unknown>) => {
    if (!intakeResult?.intake_id) {
      return;
    }
    setActiveAction(`decision:${row.row_key}`);
    setErrorMessage("");
    try {
      const response = await continueExcelIntakeReview({
        intakeId: intakeResult.intake_id,
        decisions: [
          {
            row_key: row.row_key,
            ...decision,
          },
        ],
        targetCompany,
        snapshotId,
        attachToSnapshot,
        buildArtifacts: true,
      });
      setIntakeResult((previous) => mergeIntakeResult(previous, response));
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "补充审核续跑失败。");
    } finally {
      setActiveAction("");
    }
  };

  return (
    <section className="panel supplement-intake-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Supplement</p>
          <h3>项目内资产收编与 Excel 补充</h3>
          <p>把 Anthropic 本地包收编到项目内 package，并把补充成员直接并回 snapshot / artifact / registry。</p>
        </div>
      </div>

      {errorMessage ? (
        <div className="warning-card error-card">
          <strong>操作失败</strong>
          <p>{errorMessage}</p>
        </div>
      ) : null}

      <div className="supplement-intake-grid">
        <section className="supplement-intake-card">
          <p className="field-label">目标公司</p>
          <input
            className="text-input"
            value={targetCompany}
            onChange={(event) => setTargetCompany(event.target.value)}
            placeholder="例如 Anthropic"
          />

          <p className="field-label">Snapshot ID</p>
          <input
            className="text-input"
            value={snapshotId}
            onChange={(event) => setSnapshotId(event.target.value)}
            placeholder="留空则复用最新 snapshot，不存在时自动创建"
          />

          <div className="action-row supplement-intake-actions">
            <button
              className="primary-button"
              type="button"
              onClick={handlePackageImport}
              disabled={activeAction !== ""}
            >
              {activeAction === "package-import" ? "导入中..." : "导入 Anthropic 本地资产包"}
            </button>
          </div>

          {packageResult ? (
            <dl className="plan-grid supplement-intake-summary-grid">
              <div>
                <dt>状态</dt>
                <dd>{String(packageResult.status || "-")}</dd>
              </div>
              <div>
                <dt>Snapshot</dt>
                <dd>{String(packageResult.snapshot_id || "-")}</dd>
              </div>
              <div>
                <dt>候选人数</dt>
                <dd>{String(packageResult.candidate_count || "-")}</dd>
              </div>
              <div>
                <dt>包同步</dt>
                <dd>{String((packageResult.package_sync as Record<string, unknown> | undefined)?.mode || "-")}</dd>
              </div>
            </dl>
          ) : null}
        </section>

        <section className="supplement-intake-card">
          <p className="field-label">Excel / XLSX 补充名单</p>
          <input
            className="supplement-file-input"
            type="file"
            accept=".xlsx,.xlsm,.xls"
            onChange={(event) => setSelectedFile(event.target.files?.[0] || null)}
          />
          <p className="supplement-file-hint">
            {selectedFile ? `已选择 ${selectedFile.name}` : "支持导入人工整理的成员名单并直接写回 snapshot。"}
          </p>

          <label className="supplement-checkbox-row">
            <input
              type="checkbox"
              checked={attachToSnapshot}
              onChange={(event) => setAttachToSnapshot(event.target.checked)}
            />
            <span>导入后直接并回当前目标公司的 snapshot / artifact / registry</span>
          </label>

          <div className="action-row supplement-intake-actions">
            <button
              className="primary-button"
              type="button"
              onClick={handleExcelUpload}
              disabled={activeAction !== "" || !selectedFile}
            >
              {activeAction === "excel-upload" ? "导入中..." : "导入 Excel 补充名单"}
            </button>
          </div>

          {intakeResult ? (
            <dl className="plan-grid supplement-intake-summary-grid">
              <div>
                <dt>Intake ID</dt>
                <dd>{String(intakeResult.intake_id || "-")}</dd>
              </div>
              <div>
                <dt>已解析行数</dt>
                <dd>{String(intakeResult.summary?.total_rows || rows.length || "-")}</dd>
              </div>
              <div>
                <dt>已并回 Snapshot</dt>
                <dd>{String((attachmentSummary && attachmentSummary.status) || "skipped")}</dd>
              </div>
              <div>
                <dt>待人工处理</dt>
                <dd>{String(pendingRows.length)}</dd>
              </div>
            </dl>
          ) : null}
        </section>
      </div>

      {intakeResult ? (
        <section className="supplement-results-stack">
          <div className="stat-pill-row">
            {Object.entries(statusCounts).map(([status, count]) => (
              <span className="badge status-badge" key={status}>
                {status}: {count}
              </span>
            ))}
          </div>

          {pendingRows.length > 0 ? (
            <div className="supplement-pending-grid">
              {pendingRows.map((row) => (
                <article className="manual-review-card supplement-review-card" key={row.row_key}>
                  <div className="candidate-card-top-block">
                    <div>
                      <p className="eyebrow">{row.status}</p>
                      <h4>{row.name || row.row_key}</h4>
                      <p>
                        {row.company || "未知公司"}
                        {row.title ? ` · ${row.title}` : ""}
                      </p>
                    </div>
                    {row.reason ? <p className="muted">{row.reason}</p> : null}
                  </div>

                  <div className="candidate-card-content">
                    {(row.manual_review_candidates || []).map((option, index) => {
                      const localChoice = buildLocalCandidateChoice(option);
                      const searchChoice = buildSearchProfileChoice(option);
                      const buttonDisabled = activeAction !== "";
                      return (
                        <div className="supplement-option-card" key={`${row.row_key}:${index}`}>
                          <p>{localChoice?.label || searchChoice?.label || "待确认候选项"}</p>
                          <div className="supplement-option-actions">
                            {localChoice ? (
                              <button
                                className="ghost-button small-button"
                                type="button"
                                disabled={buttonDisabled}
                                onClick={() =>
                                  handleDecision(row, {
                                    action: "select_local_candidate",
                                    selected_candidate_id: localChoice.candidateId,
                                  })
                                }
                              >
                                选用本地候选人
                              </button>
                            ) : null}
                            {searchChoice ? (
                              <button
                                className="ghost-button small-button"
                                type="button"
                                disabled={buttonDisabled}
                                onClick={() =>
                                  handleDecision(row, {
                                    action: "select_search_profile",
                                    selected_profile_url: searchChoice.profileUrl,
                                  })
                                }
                              >
                                选用该 LinkedIn 主页
                              </button>
                            ) : null}
                          </div>
                        </div>
                      );
                    })}
                  </div>

                  <div className="candidate-actions-bottom supplement-option-actions">
                    <button
                      className="ghost-button small-button"
                      type="button"
                      disabled={activeAction !== ""}
                      onClick={() => handleDecision(row, { action: "continue_search" })}
                    >
                      继续搜索
                    </button>
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <div className="warning-card">
              <strong>当前没有待处理的 intake 审核项</strong>
              <p>本次导入中已能自动复用、抓取或并回 snapshot 的人员已经完成处理。</p>
            </div>
          )}
        </section>
      ) : null}
    </section>
  );
}
