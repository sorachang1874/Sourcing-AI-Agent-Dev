import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Avatar } from "./Avatar";
import {
  exportTargetCandidatesArchive,
  getCandidateDetailsBatch,
  peekTargetCandidatesCache,
} from "../lib/api";
import {
  followUpStatusLabel,
  readTargetCandidates,
  targetCandidatesUpdatedEventName,
  updateTargetCandidate,
} from "../lib/targetCandidatesStore";
import { pickCandidateRoleLine } from "../lib/candidatePresentation";
import type { TargetCandidateFollowUpStatus, TargetCandidateRecord } from "../types";

const FOLLOW_UP_OPTIONS: Array<{ id: TargetCandidateFollowUpStatus; label: string }> = [
  { id: "pending_outreach", label: "待沟通" },
  { id: "contacted_waiting", label: "已沟通待回复" },
  { id: "rejected", label: "已拒绝邀约" },
  { id: "accepted", label: "已接受邀约" },
  { id: "interview_completed", label: "已完成访谈" },
];

function downloadBlobFile(filename: string, blob: Blob): void {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

export function TargetCandidatesPanel() {
  const [records, setRecords] = useState<TargetCandidateRecord[]>(() => peekTargetCandidatesCache());
  const [exportingArchive, setExportingArchive] = useState(false);
  const [exportError, setExportError] = useState("");
  const attemptedHydrationIds = useRef<Set<string>>(new Set());

  useEffect(() => {
    const sync = () => {
      void readTargetCandidates()
        .then((items) => {
          setRecords(items);
        })
        .catch(() => {
          setRecords([]);
        });
    };
    sync();
    window.addEventListener(targetCandidatesUpdatedEventName(), sync);
    window.addEventListener("storage", sync);
    return () => {
      window.removeEventListener(targetCandidatesUpdatedEventName(), sync);
      window.removeEventListener("storage", sync);
    };
  }, []);

  useEffect(() => {
    const hydrationTargets = records.filter(
      (record) =>
        record.jobId &&
        record.candidateId &&
        (!record.avatarUrl || !record.headline || !record.currentCompany || !record.primaryEmail || !record.linkedinUrl) &&
        !attemptedHydrationIds.current.has(record.id),
    );
    if (hydrationTargets.length === 0) {
      return;
    }
    hydrationTargets.forEach((record) => attemptedHydrationIds.current.add(record.id));
    const targetsByJobId = hydrationTargets.reduce<Record<string, TargetCandidateRecord[]>>((accumulator, record) => {
      const jobId = record.jobId || "";
      if (!jobId) {
        return accumulator;
      }
      accumulator[jobId] = accumulator[jobId] || [];
      accumulator[jobId].push(record);
      return accumulator;
    }, {});
    void Promise.allSettled(
      Object.entries(targetsByJobId).map(async ([jobId, jobRecords]) => {
        const detailsByCandidateId = await getCandidateDetailsBatch(
          jobRecords.map((record) => record.candidateId),
          jobId,
        );
        await Promise.allSettled(
          jobRecords.map(async (record) => {
            const detail = detailsByCandidateId[record.candidateId];
            if (!detail) {
              return;
            }
            const patch: Partial<TargetCandidateRecord> & { metadata?: Record<string, unknown> } = {};
            if (!record.headline && detail.headline) {
              patch.headline = detail.headline;
            }
            if (!record.currentCompany && detail.currentCompany) {
              patch.currentCompany = detail.currentCompany;
            }
            if (!record.avatarUrl && detail.avatarUrl) {
              patch.avatarUrl = detail.avatarUrl;
            }
            if (!record.primaryEmail && detail.primaryEmail) {
              patch.primaryEmail = detail.primaryEmail || "";
              patch.primaryEmailMetadata = detail.primaryEmailMetadata;
              patch.metadata = detail.primaryEmailMetadata
                ? { primary_email_metadata: detail.primaryEmailMetadata }
                : {};
            }
            if (!record.linkedinUrl && detail.linkedinUrl) {
              patch.linkedinUrl = detail.linkedinUrl;
            }
            if (Object.keys(patch).length > 0) {
              await updateTargetCandidate(record.id, patch);
            }
          }),
        );
      }),
    );
  }, [records]);

  const exportRecordIds = useMemo(
    () => Array.from(new Set(records.map((record) => record.id).filter(Boolean))),
    [records],
  );

  const handleExportArchive = async () => {
    if (exportRecordIds.length === 0 || exportingArchive) {
      return;
    }
    setExportingArchive(true);
    setExportError("");
    try {
      const download = await exportTargetCandidatesArchive({
        recordIds: exportRecordIds,
      });
      downloadBlobFile(download.filename || "target-candidates.zip", download.blob);
    } catch (error) {
      setExportError(error instanceof Error ? error.message : "导出失败，请稍后重试。");
    } finally {
      setExportingArchive(false);
    }
  };

  return (
    <div className="target-candidates-panel">
      <section className="panel target-candidates-toolbar">
        <div>
          <h3>目标候选人</h3>
          <p className="muted">用于沉淀后续跟进、质量评价和备注的候选人管理页。</p>
        </div>
        <div className="action-row target-candidates-actions">
          <div className="metric-card">
            <span className="muted">候选人数</span>
            <strong>{records.length}</strong>
          </div>
          <div className="target-candidates-export-block">
            <button
              type="button"
              className="ghost-button"
              onClick={() => void handleExportArchive()}
              disabled={exportRecordIds.length === 0 || exportingArchive}
            >
              {exportRecordIds.length === 0
                ? "暂无可导出候选人"
                : exportingArchive
                  ? `正在打包 ${exportRecordIds.length} 位候选人...`
                  : `批量导出 ${exportRecordIds.length} 位候选人信息`}
            </button>
            <span className="target-candidates-export-note">
              导出 zip 内含 CSV 摘要与可用的 LinkedIn Profile 原始文件，Email 仅包含高置信/可发布结果。
            </span>
            {exportError ? <span className="target-candidates-export-note">{exportError}</span> : null}
          </div>
        </div>
      </section>

      {records.length > 0 ? (
        <section className="target-candidate-grid">
          {records.map((record) => (
            <article key={record.id} className="candidate-card target-candidate-card">
              <div className="candidate-header candidate-header-with-avatar">
                <Avatar name={record.candidateName} src={record.avatarUrl} size="small" />
                <div className="candidate-copy">
                  <div className="candidate-title-row">
                    <div>
                      <h4>{record.candidateName}</h4>
                      <p className="candidate-meta-line">
                        {pickCandidateRoleLine({
                          id: record.candidateId,
                          name: record.candidateName,
                          headline: record.headline || "",
                          avatarUrl: record.avatarUrl || "",
                          team: "",
                          employmentStatus: "lead",
                          confidence: "lead_only",
                          summary: "",
                          outreachLayer: 0,
                          matchedKeywords: [],
                          currentCompany: record.currentCompany || "",
                          focusAreas: [],
                          matchReasons: [],
                          education: [],
                          experience: [],
                          evidence: [],
                          primaryEmail: record.primaryEmail || "",
                        })}
                      </p>
                      {record.primaryEmail ? <p className="candidate-email-line">{record.primaryEmail}</p> : null}
                    </div>
                  </div>
                </div>
              </div>

              <div className="target-candidate-fields">
                <label className="review-control-block">
                  <span className="field-label">当前跟进状态</span>
                  <select
                    value={record.followUpStatus}
                    onChange={(event) =>
                      void updateTargetCandidate(record.id, {
                        followUpStatus: event.target.value as TargetCandidateFollowUpStatus,
                      })
                    }
                  >
                    {FOLLOW_UP_OPTIONS.map((option) => (
                      <option key={option.id} value={option.id}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="review-control-block">
                  <span className="field-label">质量评价分数</span>
                  <input
                    className="text-input"
                    type="number"
                    min={1}
                    max={10}
                    value={record.qualityScore ?? ""}
                    placeholder="1-10"
                    onChange={(event) =>
                      void updateTargetCandidate(record.id, {
                        qualityScore: event.target.value ? Number(event.target.value) : null,
                      })
                    }
                  />
                </label>

                <label className="review-control-block target-candidate-comment">
                  <span className="field-label">备注 Comment</span>
                  <textarea
                    className="message-editor"
                    rows={4}
                    value={record.comment}
                    placeholder="记录跟进信息、沟通反馈或后续判断。"
                    onChange={(event) =>
                      void updateTargetCandidate(record.id, {
                        comment: event.target.value,
                      })
                    }
                  />
                </label>
              </div>

              <div className="candidate-actions candidate-actions-compact">
                <span className="keyword-chip">{followUpStatusLabel(record.followUpStatus)}</span>
                {record.linkedinUrl ? (
                  <a className="ghost-button candidate-action-button" href={record.linkedinUrl} target="_blank" rel="noreferrer">
                    打开 LinkedIn
                  </a>
                ) : null}
                {record.jobId && record.candidateId ? (
                  <Link
                    className="ghost-button candidate-action-button"
                    to={`/candidate/${encodeURIComponent(record.candidateId)}?job=${encodeURIComponent(record.jobId)}${record.historyId ? `&history=${encodeURIComponent(record.historyId)}` : ""}`}
                  >
                    查看历史记录
                  </Link>
                ) : null}
              </div>
            </article>
          ))}
        </section>
      ) : (
        <section className="panel">
          <div className="empty-state">
            <p>当前还没有加入目标候选人的记录。</p>
            <span>你可以在候选人结果看板里把值得持续跟进的人加入这里。</span>
          </div>
        </section>
      )}
    </div>
  );
}
