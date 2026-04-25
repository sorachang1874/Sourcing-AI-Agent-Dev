import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { Avatar } from "./Avatar";
import {
  getCandidateDetailsBatch,
  getManualReviewItems,
  peekCandidateReviewRecordsCache,
  peekManualReviewItemsCache,
  reviewManualReviewItem,
} from "../lib/api";
import {
  candidateAutoReviewStatus,
  candidateHasLowProfileRichness,
  candidateNeedsProfileCompletion,
  extractPrimaryEmail,
  isOpenReviewStatus,
  pickCandidateRoleLine,
  reviewStatusLabel,
  resolveEffectiveReviewStatus,
  sanitizeCandidateText,
} from "../lib/candidatePresentation";
import { getFormattedEducationExperience, getFormattedWorkExperience } from "../lib/profileFormatting";
import {
  listCandidateReviewRecords,
  setCandidateReviewStatus,
  candidateReviewRegistryUpdatedEventName,
} from "../lib/reviewRegistry";
import { buildWorkflowRoute } from "../lib/workflowContext";
import type { Candidate, CandidateDetail, CandidateReviewRecord, CandidateReviewStatus, ManualReviewItem } from "../types";

interface ManualReviewQueuePanelProps {
  jobId?: string;
  historyId?: string;
  preferredCandidateId?: string;
  backendItems?: ManualReviewItem[];
  supplementalCandidates?: Candidate[];
  onReviewMutated?: () => void;
}

interface ReviewCardGroup {
  id: string;
  candidateId: string;
  candidateName: string;
  backendItems: ManualReviewItem[];
  backendCandidate: Candidate | null;
  localRecord: CandidateReviewRecord | null;
  reviewItemIds: number[];
  supplementalCandidate: Candidate | null;
}

const REVIEW_STATUS_OPTIONS: Array<{ id: CandidateReviewStatus; label: string }> = [
  { id: "needs_profile_completion", label: "信息不完整待补全" },
  { id: "low_profile_richness", label: "LinkedIn信息丰富度低" },
  { id: "needs_review", label: "待审核" },
  { id: "no_review_needed", label: "无需审核" },
  { id: "verified_keep", label: "已核实候选人" },
  { id: "verified_exclude", label: "已核实可排除候选人" },
];

function mapReviewStatusToManualReviewAction(status: CandidateReviewStatus): "open" | "resolve" | "dismiss" {
  if (status === "needs_review" || status === "needs_profile_completion" || status === "low_profile_richness") {
    return "open";
  }
  if (status === "verified_exclude") {
    return "dismiss";
  }
  return "resolve";
}

function buildReviewKey(item: ManualReviewItem): string {
  return item.candidateId || item.candidateName || item.id;
}

function groupReviewCandidates(
  backendItems: ManualReviewItem[],
  localRecords: CandidateReviewRecord[],
  supplementalCandidates: Candidate[],
  preferredCandidateId = "",
): ReviewCardGroup[] {
  const groups = new Map<string, ReviewCardGroup>();
  const localRecordByCandidateId = new Map<string, CandidateReviewRecord>();
  const localRecordByFallbackKey = new Map<string, CandidateReviewRecord>();
  const supplementalCandidateById = new Map<string, Candidate>();

  for (const record of localRecords) {
    if (record.candidateId) {
      localRecordByCandidateId.set(record.candidateId, record);
    }
    localRecordByFallbackKey.set(record.candidateId || record.candidateName || record.id, record);
  }
  for (const candidate of supplementalCandidates) {
    if (candidate.id) {
      supplementalCandidateById.set(candidate.id, candidate);
    }
  }

  for (const item of backendItems) {
    const key = buildReviewKey(item);
    const localRecord = localRecordByCandidateId.get(item.candidateId) || localRecordByFallbackKey.get(key) || null;
    if (localRecord && !isOpenReviewStatus(localRecord.status)) {
      continue;
    }
    const existing = groups.get(key);
    if (existing) {
      existing.backendItems.push(item);
      if (!existing.backendCandidate && item.candidate) {
        existing.backendCandidate = item.candidate;
      }
      if (item.reviewItemId) {
        existing.reviewItemIds.push(item.reviewItemId);
      }
      continue;
    }
    groups.set(key, {
      id: key,
      candidateId: item.candidateId,
      candidateName: item.candidateName,
      backendItems: [item],
      backendCandidate: item.candidate || null,
      localRecord,
      reviewItemIds: item.reviewItemId ? [item.reviewItemId] : [],
      supplementalCandidate: supplementalCandidateById.get(item.candidateId) || null,
    });
  }

  for (const record of localRecords) {
    if (!isOpenReviewStatus(record.status)) {
      continue;
    }
    const key = record.candidateId || record.candidateName || record.id;
    const existing = groups.get(key);
    if (existing) {
      existing.localRecord = record;
      if (!existing.supplementalCandidate && record.candidateId) {
        existing.supplementalCandidate = supplementalCandidateById.get(record.candidateId) || null;
      }
      continue;
    }
    groups.set(key, {
      id: key,
      candidateId: record.candidateId,
      candidateName: record.candidateName,
      backendItems: [],
      backendCandidate: null,
      localRecord: record,
      reviewItemIds: [],
      supplementalCandidate: supplementalCandidateById.get(record.candidateId) || null,
    });
  }

  for (const candidate of supplementalCandidates) {
    const autoStatus = candidateAutoReviewStatus(candidate);
    if (!candidate.id || !autoStatus) {
      continue;
    }
    const localRecord = localRecordByCandidateId.get(candidate.id) || null;
    if (localRecord && !isOpenReviewStatus(localRecord.status)) {
      continue;
    }
    const key = candidate.id;
    const existing = groups.get(key);
    if (existing) {
      existing.supplementalCandidate = candidate;
      continue;
    }
    groups.set(key, {
      id: key,
      candidateId: candidate.id,
      candidateName: candidate.name,
      backendItems: [],
      backendCandidate: null,
      localRecord,
      reviewItemIds: [],
      supplementalCandidate: candidate,
    });
  }

  return Array.from(groups.values()).sort((left, right) => {
    if (preferredCandidateId && left.candidateId === preferredCandidateId) {
      return -1;
    }
    if (preferredCandidateId && right.candidateId === preferredCandidateId) {
      return 1;
    }
    return left.candidateName.localeCompare(right.candidateName);
  });
}

function reviewSignalText(group: ReviewCardGroup): string {
  return [
    ...(group.localRecord ? [group.localRecord.comment || ""] : []),
    ...group.backendItems.flatMap((item) => [
      item.reviewType,
      item.summary,
      item.recommendedAction,
      item.notes || "",
      item.synthesisSummary || "",
      ...item.evidenceLabels,
    ]),
  ]
    .join(" ")
    .toLowerCase();
}

function buildMissingInfoBullets(
  group: ReviewCardGroup,
  detail: CandidateDetail | null,
  supplementalCandidate: Candidate | null,
): string[] {
  const lower = reviewSignalText(group);
  const effectiveCandidate = detail || supplementalCandidate;
  const workExperience = effectiveCandidate ? getFormattedWorkExperience(effectiveCandidate, 4) : [];
  const educationExperience = effectiveCandidate ? getFormattedEducationExperience(effectiveCandidate, 3) : [];
  const bullets: string[] = [];

  if (/linkedin|profile/.test(lower) || !effectiveCandidate?.linkedinUrl) {
    bullets.push("无 LinkedIn 链接");
  }
  if (supplementalCandidate && candidateNeedsProfileCompletion(supplementalCandidate)) {
    bullets.push("LinkedIn 详情抓取未完成");
  }
  if (supplementalCandidate && candidateHasLowProfileRichness(supplementalCandidate)) {
    bullets.push("LinkedIn 资料较少，部分字段未填写");
  }
  if (/profile_detail_gap|work|employment|经历/.test(lower) || workExperience.length === 0) {
    bullets.push("工作经历证据不足");
  }
  if (/education|school|degree|学历|教育/.test(lower) || educationExperience.length === 0) {
    bullets.push("教育信息缺失");
  }
  if (/identity|unresolved|company归属|current company/.test(lower) || !effectiveCandidate?.currentCompany) {
    bullets.push("当前公司归属不明确");
  }
  if (
    /keyword|focus|topic|方向/.test(lower) ||
    ((!effectiveCandidate?.matchedKeywords.length) && (!effectiveCandidate?.focusAreas.length))
  ) {
    bullets.push("无明确关键词命中");
  }

  return Array.from(new Set(bullets)).slice(0, 5);
}

export function ManualReviewQueuePanel({
  jobId = "",
  historyId = "",
  preferredCandidateId = "",
  backendItems,
  supplementalCandidates,
  onReviewMutated,
}: ManualReviewQueuePanelProps) {
  const [fetchedBackendItems, setFetchedBackendItems] = useState<ManualReviewItem[]>(() =>
    backendItems ?? (jobId ? peekManualReviewItemsCache({ jobId, status: "open" }) : []),
  );
  const [localRecords, setLocalRecords] = useState<CandidateReviewRecord[]>(() =>
    jobId ? peekCandidateReviewRecordsCache({ jobId }) : [],
  );
  const [detailsByCandidateId, setDetailsByCandidateId] = useState<Record<string, CandidateDetail | null>>({});
  const [isLoading, setIsLoading] = useState(() => !(backendItems || (jobId && peekManualReviewItemsCache({ jobId, status: "open" }).length > 0)));
  const [errorMessage, setErrorMessage] = useState("");
  const [actingKey, setActingKey] = useState("");

  const effectiveBackendItems = backendItems ?? fetchedBackendItems;
  const effectiveSupplementalCandidates = supplementalCandidates ?? [];

  const loadBackendItems = async () => {
    if (backendItems) {
      return;
    }
    if (!jobId) {
      setFetchedBackendItems([]);
      return;
    }
    const payload = await getManualReviewItems({ jobId, status: "open" });
    setFetchedBackendItems(payload);
  };

  useEffect(() => {
    const syncLocalRecords = () => {
      if (!jobId) {
        setLocalRecords([]);
        return;
      }
      setLocalRecords(peekCandidateReviewRecordsCache({ jobId }));
      void listCandidateReviewRecords(jobId)
        .then((records) => {
          setLocalRecords(records);
        })
        .catch(() => {
          setLocalRecords([]);
        });
    };
    syncLocalRecords();
    window.addEventListener(candidateReviewRegistryUpdatedEventName(), syncLocalRecords);
    window.addEventListener("storage", syncLocalRecords);
    return () => {
      window.removeEventListener(candidateReviewRegistryUpdatedEventName(), syncLocalRecords);
      window.removeEventListener("storage", syncLocalRecords);
    };
  }, [jobId]);

  useEffect(() => {
    let isMounted = true;
    if (!jobId) {
      setFetchedBackendItems([]);
      setDetailsByCandidateId({});
      setErrorMessage("当前没有可恢复的 workflow job，请先在搜索页完成一次真实执行。");
      setIsLoading(false);
      return () => {
        isMounted = false;
      };
    }
    const cachedBackendItems = backendItems ?? peekManualReviewItemsCache({ jobId, status: "open" });
    if (!backendItems) {
      setFetchedBackendItems(cachedBackendItems);
    }
    setIsLoading(cachedBackendItems.length === 0);
    setErrorMessage("");
    setDetailsByCandidateId({});
    void loadBackendItems()
      .catch((error) => {
        if (!isMounted) {
          return;
        }
        setErrorMessage(error instanceof Error ? error.message : "人工审核队列加载失败。");
      })
      .finally(() => {
        if (isMounted) {
          setIsLoading(false);
        }
      });
    return () => {
      isMounted = false;
    };
  }, [backendItems, jobId]);

  const groupedItems = useMemo(
    () =>
      groupReviewCandidates(
        effectiveBackendItems,
        localRecords,
        effectiveSupplementalCandidates,
        preferredCandidateId,
      ),
    [effectiveBackendItems, effectiveSupplementalCandidates, localRecords, preferredCandidateId],
  );

  const detailCandidateIds = useMemo(
    () =>
      Array.from(
        new Set(
          groupedItems
            .filter((group) => {
              if (!group.candidateId) {
                return false;
              }
              if (Object.prototype.hasOwnProperty.call(detailsByCandidateId, group.candidateId)) {
                return false;
              }
              const previewCandidate = group.backendCandidate || group.supplementalCandidate;
              if (!previewCandidate) {
                return true;
              }
              return (
                previewCandidate.experience.length === 0 ||
                previewCandidate.education.length === 0 ||
                !previewCandidate.avatarUrl
              );
            })
            .slice(0, 6)
            .map((group) => group.candidateId),
        ),
      ),
    [detailsByCandidateId, groupedItems],
  );

  useEffect(() => {
    let isMounted = true;
    if (!jobId || detailCandidateIds.length === 0) {
      return () => {
        isMounted = false;
      };
    }
    void getCandidateDetailsBatch(detailCandidateIds, jobId)
      .then((nextMap) => {
        if (!isMounted) {
          return;
        }
        setDetailsByCandidateId((current) => ({ ...current, ...nextMap }));
      })
      .catch(() => {
        if (!isMounted) {
          return;
        }
        const nextMap = Object.fromEntries(detailCandidateIds.map((candidateId) => [candidateId, null]));
        setDetailsByCandidateId((current) => ({ ...current, ...nextMap }));
      });
    return () => {
      isMounted = false;
    };
  }, [detailCandidateIds, jobId]);

  const handleReviewStatusChange = async (
    group: ReviewCardGroup,
    status: CandidateReviewStatus,
  ) => {
    setActingKey(`${group.id}:${status}`);
    setErrorMessage("");
    try {
      if (group.reviewItemIds.length > 0) {
        await Promise.all(
          group.reviewItemIds.map((reviewItemId) =>
            reviewManualReviewItem(reviewItemId, mapReviewStatusToManualReviewAction(status), {
              reviewer: "frontend-demo",
              metadataMerge: {
                candidate_review_status: status,
              },
            }),
          ),
        );
      }
      const detail = group.candidateId ? detailsByCandidateId[group.candidateId] || null : null;
      const candidate = detail ||
        group.supplementalCandidate || {
          id: group.candidateId,
          name: group.candidateName,
          headline: group.localRecord?.headline || "",
          currentCompany: group.localRecord?.currentCompany,
          avatarUrl: group.localRecord?.avatarUrl || "",
          linkedinUrl: group.localRecord?.linkedinUrl,
          externalLinks: [],
          summary: "",
          notesSnippet: "",
          matchReasons: [],
          experience: [],
          education: [],
        };
      await setCandidateReviewStatus(
        jobId,
        candidate,
        status,
        {
          historyId,
          source: group.reviewItemIds.length > 0 ? "backend_override" : "manual_review",
        },
      );
      await loadBackendItems();
      onReviewMutated?.();
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "人工审核操作失败。");
    } finally {
      setActingKey("");
    }
  };

  if (isLoading) {
    return (
      <section className="panel">
        <div className="results-skeleton">
          <div className="skeleton-line short" />
          <div className="skeleton-line" />
          <div className="skeleton-line" />
        </div>
      </section>
    );
  }

  return (
    <div className="manual-review-panel" data-testid="manual-review-panel">
      {errorMessage ? (
        <section className="warning-card error-card">
          <strong>人工审核队列异常</strong>
          <p>{errorMessage}</p>
        </section>
      ) : null}

      {groupedItems.length > 0 ? (
        <section className="manual-review-card-grid" data-testid="manual-review-card-grid">
          {groupedItems.map((group) => {
            const detail = group.candidateId ? detailsByCandidateId[group.candidateId] || null : null;
            const fallbackCandidate = group.backendCandidate || group.supplementalCandidate;
            const effectiveCandidate = detail || fallbackCandidate;
            const workExperience = effectiveCandidate ? getFormattedWorkExperience(effectiveCandidate, 4) : [];
            const educationExperience = effectiveCandidate ? getFormattedEducationExperience(effectiveCandidate, 3) : [];
            const missingInfoBullets = buildMissingInfoBullets(group, detail, fallbackCandidate);
            const rawProfileRoute = group.candidateId
              ? buildWorkflowRoute(`/candidate/${group.candidateId}`, {
                  historyId,
                  jobId,
                  candidateId: group.candidateId,
                })
              : "";
            const rawProfileUrl =
              detail?.linkedinUrl ||
              fallbackCandidate?.linkedinUrl ||
              group.localRecord?.linkedinUrl ||
              rawProfileRoute;
            const emailMetadata =
              detail?.primaryEmailMetadata ||
              fallbackCandidate?.primaryEmailMetadata ||
              group.localRecord?.primaryEmailMetadata;
            const email = detail
              ? extractPrimaryEmail(detail)
              : fallbackCandidate
                ? extractPrimaryEmail(fallbackCandidate) || group.localRecord?.primaryEmail || ""
                : emailMetadata
                  ? group.localRecord?.primaryEmail || ""
                  : "";
            const autoReviewStatus = fallbackCandidate ? candidateAutoReviewStatus(fallbackCandidate) : null;
            const currentReviewStatus =
              resolveEffectiveReviewStatus({
                localStatus: group.localRecord?.status || null,
                backendReviewTypes: group.backendItems.map((item) => item.reviewType),
                autoStatus: autoReviewStatus,
              }) || "needs_review";

            return (
              <article key={group.id} className="candidate-card manual-review-card">
                <div className="candidate-header candidate-header-with-avatar">
                  <Avatar
                    name={group.candidateName}
                    src={detail?.avatarUrl || fallbackCandidate?.avatarUrl || group.localRecord?.avatarUrl}
                    size="small"
                  />
                  <div className="candidate-copy">
                    <div className="candidate-title-row">
                      <div>
                        <h4>{group.candidateName}</h4>
                        <p className="candidate-meta-line">
                          {effectiveCandidate
                            ? pickCandidateRoleLine(effectiveCandidate)
                            : group.localRecord?.headline || "待补充候选资料"}
                        </p>
                        {email ? (
                          <div className="candidate-email-line candidate-email-line-with-help">
                            <span>{email}</span>
                            {emailMetadata ? (
                              <span className="help-badge" aria-label="查看邮箱来源说明">
                                ?
                                <span className="help-tooltip">
                                  <span>来源: {emailMetadata.source || "unknown"}</span>
                                  <span>状态: {emailMetadata.status || "unknown"}</span>
                                  <span>
                                    质量分:{" "}
                                    {typeof emailMetadata.qualityScore === "number"
                                      ? emailMetadata.qualityScore
                                      : "unknown"}
                                  </span>
                                  <span>
                                    LinkedIn Profile 标记:{" "}
                                    {emailMetadata.foundInLinkedInProfile === true
                                      ? "是"
                                      : emailMetadata.foundInLinkedInProfile === false
                                        ? "否"
                                        : "未提供"}
                                  </span>
                                </span>
                              </span>
                            ) : null}
                          </div>
                        ) : null}
                      </div>
                    </div>
                  </div>
                </div>

                <div className="candidate-keywords">
                  <span className="keyword-chip">{reviewStatusLabel(currentReviewStatus)}</span>
                  {group.backendItems.length > 1 ? <span className="keyword-chip">合并 {group.backendItems.length} 条审核项</span> : null}
                </div>

                <div className="candidate-experience-grid">
                  <div className="candidate-experience-section">
                    <h5>工作经历</h5>
                    <div className="candidate-experience-scroll">
                      {workExperience.length > 0 ? (
                        <ul className="flat-list compact experience-preview-list">
                          {workExperience.map((line) => (
                            <li key={`${group.id}-work-${line}`}>{sanitizeCandidateText(line)}</li>
                          ))}
                        </ul>
                      ) : (
                        <p>暂无可结构化提取的工作经历。</p>
                      )}
                    </div>
                  </div>
                  <div className="candidate-experience-section">
                    <h5>教育经历</h5>
                    <div className="candidate-experience-scroll">
                      {educationExperience.length > 0 ? (
                        <ul className="flat-list compact experience-preview-list">
                          {educationExperience.map((line) => (
                            <li key={`${group.id}-edu-${line}`}>{sanitizeCandidateText(line)}</li>
                          ))}
                        </ul>
                      ) : (
                        <p>暂无可结构化提取的教育经历。</p>
                      )}
                    </div>
                  </div>
                </div>

                <div className="manual-review-missing-block">
                  <h5>待确认 / 缺失的信息点</h5>
                  {missingInfoBullets.length > 0 ? (
                    <ul className="flat-list compact experience-preview-list">
                      {missingInfoBullets.map((item) => (
                        <li key={`${group.id}-${item}`}>{item}</li>
                      ))}
                    </ul>
                  ) : (
                    <p>当前主要需要人工确认候选身份与证据完整性。</p>
                  )}
                </div>

                <div className="candidate-actions candidate-actions-compact candidate-actions-bottom">
                  <label className="review-status-control">
                    <span className="field-label">审核状态</span>
                    <select
                      value={currentReviewStatus}
                      onChange={(event) =>
                        void handleReviewStatusChange(group, event.target.value as CandidateReviewStatus)
                      }
                      disabled={actingKey.startsWith(`${group.id}:`)}
                    >
                      {REVIEW_STATUS_OPTIONS.map((option) => (
                        <option key={option.id} value={option.id}>
                          {actingKey === `${group.id}:${option.id}` ? `${option.label}（处理中...）` : option.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  {rawProfileUrl ? (
                    rawProfileUrl.startsWith("http") ? (
                      <a className="ghost-button candidate-action-button" href={rawProfileUrl} target="_blank" rel="noreferrer">
                        查看原始资料
                      </a>
                    ) : (
                      <Link className="ghost-button candidate-action-button" to={rawProfileUrl}>
                        查看原始资料
                      </Link>
                    )
                  ) : null}
                </div>
              </article>
            );
          })}
        </section>
      ) : (
        <section className="panel" data-testid="manual-review-empty-state">
          <div className="empty-state">
            <p>当前没有待处理的人工审核候选人。</p>
            <span>后续如果有缺失证据或需人工确认的候选人，会在这里集中展示。</span>
          </div>
        </section>
      )}
    </div>
  );
}
