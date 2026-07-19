import type { ReactNode } from "react";
import {
  buildCohortShardPreview,
  summarizeCohortLocations,
  summarizeCohortSelection,
} from "../lib/cohortSelection";
import type {
  CohortLocationSelection,
  CohortSelectionOptions,
  DemoPlan,
  PlanReviewDecision,
  PlanReviewEditableField,
  ProviderExecutionLanePreview,
} from "../types";
import { CohortSelectionPicker } from "./CohortSelectionPicker";

interface PlanCardProps {
  plan: DemoPlan;
  revisionText: string;
  reviewDecision: PlanReviewDecision;
  cohortOptions: CohortSelectionOptions | null;
  isLoadingCohortOptions: boolean;
  cohortOptionsError: string;
  reviewChecklistConfirmed: boolean;
  isApplyingRevision: boolean;
  isConfirming: boolean;
  onRevisionChange: (value: string) => void;
  onReviewDecisionChange: (patch: Partial<PlanReviewDecision>) => void;
  onRetryCohortOptions: () => void;
  onReviewChecklistChange: (value: boolean) => void;
  onApplyRevision: () => void;
  onConfirm: () => void;
}

function supportsField(plan: DemoPlan, field: PlanReviewEditableField): boolean {
  return Boolean(plan.reviewGate?.editableFields.includes(field));
}

function toggleScopeValue(values: string[], value: string, checked: boolean): string[] {
  if (checked) {
    return values.includes(value) ? values : [...values, value];
  }
  return values.filter((item) => item !== value);
}

function decisionBooleanValue(value: boolean | undefined): string {
  if (value === true) {
    return "true";
  }
  if (value === false) {
    return "false";
  }
  return "";
}

function referenceBaselineSnapshot(plan: DemoPlan): string {
  if (plan.baselineSnapshotId?.trim()) {
    return plan.baselineSnapshotId.trim();
  }
  const matched = (plan.executionNotes || []).find((item) => /baseline snapshot/i.test(item));
  return matched?.split(":").slice(1).join(":").trim() || "";
}

function AdvancedField({
  title,
  children,
  value,
  description,
  subtle = false,
}: {
  title: string;
  children?: ReactNode;
  value?: string;
  description?: string;
  subtle?: boolean;
}) {
  return (
    <div className={`advanced-review-item${subtle ? " subtle" : ""}`}>
      <div className="advanced-review-item-head">
        <span className="advanced-review-item-title">{title}</span>
        {description ? <span className="advanced-review-item-description">{description}</span> : null}
        {value ? <span className="advanced-review-item-value">{value}</span> : null}
      </div>
      {children ? <div className="advanced-review-item-body">{children}</div> : null}
    </div>
  );
}

function renderPlanKeywords(keywords: string[]): string {
  return keywords.length > 0 ? keywords.join("、") : "无定向关键词";
}

function translateProviderLaneEmploymentStatus(value: string): string {
  switch ((value || "").trim().toLowerCase()) {
    case "current":
      return "在职";
    case "former":
      return "已离职";
    default:
      return value?.trim() || "全部";
  }
}

function renderProviderLaneFilters(lane: ProviderExecutionLanePreview): string {
  const filters = Object.entries(lane.companyFilters || {})
    .filter(([, values]) => values.length > 0)
    .map(([key, values]) => `${key}=${values.slice(0, 3).join(", ")}${values.length > 3 ? "..." : ""}`);
  if (filters.length === 0) {
    return "无额外 filter";
  }
  return filters.join(" · ");
}

function renderProviderLaneQuery(lane: ProviderExecutionLanePreview): string {
  if (lane.queryTexts.length > 0) {
    return lane.queryTexts.join("、");
  }
  if (lane.provider === "harvest_company_employees") {
    return "无 search keyword，使用 company-employees 公司过滤";
  }
  if (lane.provider === "harvest_profile_search" && lane.companyFilters.past_companies?.length) {
    return "无 search keyword，使用 pastCompanies 过滤";
  }
  return "无 search keyword";
}

function renderProviderExecutionLanes(lanes: ProviderExecutionLanePreview[]): string {
  if (lanes.length === 0) {
    return "后端未返回实际 provider 参数";
  }
  return lanes
    .map((lane) => {
      const laneLabel = translateProviderLaneEmploymentStatus(lane.employmentStatus);
      const provider = lane.displayLabel || lane.provider || lane.operation || "provider";
      return `${laneLabel}: ${provider}; ${renderProviderLaneQuery(lane)}; ${renderProviderLaneFilters(lane)}`;
    })
    .join(" / ");
}

function translateCompanyIdentityResolver(value?: string): string {
  switch ((value || "").trim()) {
    case "builtin":
      return "内置映射";
    case "manual_review_override":
      return "人工修正";
    case "legacy_mapping":
      return "历史映射";
    case "heuristic":
      return "启发式识别";
    default:
      return value?.trim() || "未标注";
  }
}

function translateCompanyIdentityConfidence(value?: string): string {
  switch ((value || "").trim().toLowerCase()) {
    case "high":
      return "高";
    case "medium":
      return "中";
    case "low":
      return "低";
    default:
      return value?.trim() || "未标注";
  }
}

function describeTargetCompanyIdentity(plan: DemoPlan): string {
  const identity = plan.targetCompanyIdentity;
  if (!identity) {
    return "后端未返回公司 LinkedIn 识别结果，可手动输入公司主页 URL。";
  }
  const parts = [
    `识别来源: ${translateCompanyIdentityResolver(identity.resolver)}`,
    `置信度: ${translateCompanyIdentityConfidence(identity.confidence)}`,
  ];
  if (identity.localAssetAvailable) {
    parts.push("本地资产可复用");
  }
  return parts.join(" · ");
}

function shouldShowTargetCompanyLinkedin(plan: DemoPlan): boolean {
  return Boolean(plan.targetCompanyIdentity) || supportsField(plan, "target_company_linkedin_url");
}

export function PlanCard({
  plan,
  revisionText,
  reviewDecision,
  cohortOptions,
  isLoadingCohortOptions,
  cohortOptionsError,
  isApplyingRevision,
  isConfirming,
  onRevisionChange,
  onReviewDecisionChange,
  onRetryCohortOptions,
  onApplyRevision,
  onConfirm,
}: PlanCardProps) {
  const reviewGate = plan.reviewGate;
  const scopeHints = reviewGate?.scopeHints || [];
  const baselineSnapshot = referenceBaselineSnapshot(plan);
  const showProjectScope = Boolean(
    plan.projectScope?.trim() && plan.projectScope.trim() !== "目标公司全量范围",
  );
  const hasAdvancedContent = Boolean(
    baselineSnapshot ||
      plan.providerExecutionLanes?.length ||
      reviewGate?.editableFields.length,
  );
  // Effective (editable) cohort state: the review decision overrides the
  // frozen plan mirror; locations are sibling request fields (FT0 §7.2) and
  // keep their presence tri-state (absent / explicit opt-out / explicit
  // values) instead of collapsing to an empty list.
  const effectiveCohortSelection = reviewDecision.cohortSelection || plan.cohortSelection || null;
  const effectiveLocations: CohortLocationSelection = {
    targetLocations: reviewDecision.targetLocations ?? plan.targetLocations,
    excludeTargetLocations: reviewDecision.excludeTargetLocations ?? plan.excludeTargetLocations,
  };
  // Location review edits are exposed ONLY when the backend review gate
  // authorizes BOTH location editable fields; otherwise the review decision
  // has no authorized application path and the controls stay read-only
  // (FT2 fixed-forward, review finding 2).
  const locationEditingAuthorized =
    supportsField(plan, "target_locations") && supportsField(plan, "exclude_target_locations");
  const planShardPreview =
    effectiveCohortSelection && cohortOptions
      ? buildCohortShardPreview(effectiveCohortSelection, cohortOptions)
      : null;
  // Confirmation gate (review finding 4): an effective Cohort may not enter
  // execution without a VALIDATED shard preview + bounded-budget note. The
  // preview exists only over loaded, non-stale server options, so options
  // loading/failure and registry drift all block here with a retryable
  // state instead of silently enabling confirmation.
  const cohortPreviewBlocker: { message: string; retryable: boolean } | null = (() => {
    if (!effectiveCohortSelection) {
      return null;
    }
    if (isLoadingCohortOptions && !cohortOptions) {
      return { message: "正在加载可选人群，分片预览与预算说明尚未验证…", retryable: false };
    }
    if (cohortOptionsError && !cohortOptions) {
      return {
        message: `可选人群加载失败，无法验证分片预览与预算说明：${cohortOptionsError}`,
        retryable: true,
      };
    }
    if (!cohortOptions) {
      return { message: "缺少服务端可选人群，无法验证分片预览与预算说明。", retryable: true };
    }
    if (planShardPreview?.hasUnavailableSelections) {
      const unavailable = [
        ...planShardPreview.unavailableRoleIds.map((id) => `角色 ${id}`),
        ...planShardPreview.unavailableStatusIds.map((id) => `在职状态 ${id}`),
        ...(planShardPreview.roleMatchUnavailable ? ["多角色匹配方式"] : []),
      ];
      return {
        message: `当前选项（registry ${planShardPreview.registryVersion}）已不包含所选值：${unavailable.join("、")}。请刷新选项后重新选择。`,
        retryable: true,
      };
    }
    return null;
  })();
  // Fail closed in the handler as well as on the button: a blocked
  // confirmation can never reach the approve/start path.
  const handleConfirm = () => {
    if (cohortPreviewBlocker) {
      return;
    }
    onConfirm();
  };

  return (
    <section
      className="conversation-card plan-card"
      data-testid="plan-card"
      data-plan-dispatch-strategy={plan.dispatchStrategy || ""}
      data-plan-planner-mode={plan.plannerMode || ""}
      data-plan-requires-delta-acquisition={plan.requiresDeltaAcquisition ? "true" : "false"}
      data-plan-current-lane-behavior={plan.currentLaneBehavior || ""}
      data-plan-former-lane-behavior={plan.formerLaneBehavior || ""}
      data-plan-default-acquisition-mode={plan.defaultAcquisitionMode || ""}
      data-plan-organization-scale-band={plan.organizationScaleBand || ""}
      data-plan-baseline-snapshot-id={plan.baselineSnapshotId || ""}
    >
      <div className="section-heading">
        <h3>检索方案</h3>
      </div>

      <dl className="plan-grid">
        <div>
          <dt>目标公司</dt>
          <dd>{plan.targetCompany}</dd>
        </div>
        <div>
          <dt>目标人群</dt>
          <dd>{plan.targetPopulation}</dd>
        </div>
        {plan.cohortSelection ? (
          <div className="plan-grid-wide" data-testid="plan-cohort-summary">
            <dt>精确人群筛选</dt>
            <dd>{summarizeCohortSelection(plan.cohortSelection, cohortOptions)}</dd>
            <dd data-testid="plan-cohort-locations">
              {summarizeCohortLocations(plan.targetLocations, plan.excludeTargetLocations)}
            </dd>
          </div>
        ) : null}
        {showProjectScope ? (
          <div>
            <dt>项目范围</dt>
            <dd>{plan.projectScope}</dd>
          </div>
        ) : null}
        <div>
          <dt>检索关键词</dt>
          <dd>{renderPlanKeywords(plan.keywords)}</dd>
        </div>
        <div>
          <dt>检索策略</dt>
          <dd>{plan.acquisitionStrategy}</dd>
        </div>
        {shouldShowTargetCompanyLinkedin(plan) ? (
          <div className="plan-grid-wide plan-grid-editable-item">
            <dt className="plan-grid-inline-label">
              <span>Target company LinkedIn</span>
              <span className="help-badge" aria-label={describeTargetCompanyIdentity(plan)}>
                ?
                <span className="help-tooltip">
                  {describeTargetCompanyIdentity(plan)}
                  <span>留空则继续使用后端当前识别结果；只有这里填入新 URL 时，执行阶段才会覆盖默认 slug 解析。</span>
                </span>
              </span>
            </dt>
            <dd>{plan.targetCompanyIdentity?.linkedinCompanyUrl || "未识别"}</dd>
            {supportsField(plan, "target_company_linkedin_url") ? (
              <div className="plan-grid-inline-editor">
                <label className="field-label" htmlFor="target-company-linkedin-url">
                  手动修正公司 LinkedIn URL
                </label>
                <input
                  id="target-company-linkedin-url"
                  className="text-input"
                  type="text"
                  value={reviewDecision.targetCompanyLinkedinUrl || ""}
                  placeholder={
                    plan.targetCompanyIdentity?.linkedinCompanyUrl || "https://www.linkedin.com/company/ssi-ai/"
                  }
                  onChange={(event) =>
                    onReviewDecisionChange({
                      targetCompanyLinkedinUrl: event.target.value,
                    })
                  }
                />
              </div>
            ) : null}
          </div>
        ) : null}
      </dl>

      <section className="plan-review-section">
        <div className="section-heading plan-review-heading">
          <h3>Review</h3>
        </div>

        {plan.reviewRequired ? <p className="muted plan-review-note">本次方案需要 review gate 放行后才会真正进入执行。</p> : null}

        <div className="revision-panel plan-review-revision-panel">
          <label className="field-label" htmlFor="plan-revision">
            继续补充或修改要求
          </label>
          <textarea
            id="plan-revision"
            className="revision-input"
            value={revisionText}
            placeholder="优先复用本地资产，只做Scoped search，要多模态方向（Nano Banana和Veo组）的人"
            onChange={(event) => onRevisionChange(event.target.value)}
            rows={3}
          />
        </div>

        <CohortSelectionPicker
          idPrefix="plan-review"
          value={reviewDecision.cohortSelection || plan.cohortSelection || null}
          options={cohortOptions}
          isLoading={isLoadingCohortOptions}
          errorMessage={cohortOptionsError}
          locked={Boolean(plan.cohortSelection)}
          locationValue={effectiveLocations}
          locationDisabled={!locationEditingAuthorized}
          showShardPreview={false}
          onChange={(value) => onReviewDecisionChange({ cohortSelection: value || undefined })}
          onLocationChange={
            locationEditingAuthorized
              ? (next) =>
                  onReviewDecisionChange({
                    targetLocations: next.targetLocations,
                    excludeTargetLocations: next.excludeTargetLocations,
                  })
              : undefined
          }
          onRetryOptions={onRetryCohortOptions}
        />

        {planShardPreview && !cohortPreviewBlocker ? (
          <div className="plan-shard-preview" data-testid="plan-cohort-shard-preview">
            <p className="plan-shard-preview-count" data-testid="plan-cohort-shard-count">
              本次方案计划执行 {planShardPreview.shardCount} 个召回分片（在职状态 × 角色）：
            </p>
            <ul className="cohort-shard-list">
              {planShardPreview.shards.map((shard) => (
                <li key={shard.shardId} data-testid={`plan-cohort-shard-${shard.shardId}`}>
                  {shard.statusLabel} · {shard.roleLabel}
                </li>
              ))}
            </ul>
            {planShardPreview.isFullRecall ? (
              <p
                className="cohort-full-recall-warning"
                role="alert"
                data-testid="plan-full-recall-warning"
              >
                ⚠ 未限定角色且覆盖全部在职状态：将对目标公司全量成员发起全量召回，结果量与执行耗时显著增加。
              </p>
            ) : null}
            <p className="muted plan-shard-budget-note" data-testid="plan-shard-budget-note">
              每个分片执行全量成员召回，实际消耗受服务端分片预算上限与 provider
              限额约束（上限数值由运营方配置，确认前不展示预估值）。
            </p>
          </div>
        ) : null}

        {cohortPreviewBlocker ? (
          <div
            className="plan-shard-preview-blocked"
            role="alert"
            data-testid="plan-cohort-preview-blocked"
          >
            <span>{cohortPreviewBlocker.message}</span>
            {cohortPreviewBlocker.retryable ? (
              <button type="button" className="link-chip" onClick={onRetryCohortOptions}>
                重试
              </button>
            ) : null}
          </div>
        ) : null}

        {hasAdvancedContent ? (
          <details className="advanced-review-panel">
            <summary className="advanced-review-summary">
              <div className="advanced-review-summary-copy">
                <span className="advanced-review-summary-label">Advanced Mode</span>
                <strong className="advanced-review-summary-title">可选执行参数</strong>
              </div>
              <span className="advanced-review-summary-icon" aria-hidden="true">⌄</span>
            </summary>

            {baselineSnapshot ? (
              <AdvancedField
                title="Baseline snapshot"
                description="参考信息，仅用于说明当前默认复用的 baseline。"
                value={baselineSnapshot}
                subtle
              />
            ) : null}

            {plan.providerExecutionLanes?.length ? (
              <AdvancedField
                title="实际 Provider 参数"
                description="开发/排障参考；用户可见策略以检索关键词与检索策略为准。"
                value={renderProviderExecutionLanes(plan.providerExecutionLanes)}
                subtle
              />
            ) : null}

            {supportsField(plan, "company_scope") && scopeHints.length > 0 ? (
              <AdvancedField title="确认范围" description="确认本次执行需要覆盖的组织范围。">
                <div className="checkbox-grid">
                  {Array.from(new Set([plan.targetCompany, ...scopeHints])).map((scope) => (
                    <label key={scope} className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={reviewDecision.confirmedCompanyScope.includes(scope)}
                        onChange={(event) =>
                          onReviewDecisionChange({
                            confirmedCompanyScope: toggleScopeValue(
                              reviewDecision.confirmedCompanyScope,
                              scope,
                              event.target.checked,
                            ),
                          })
                        }
                      />
                      <span>{scope}</span>
                    </label>
                  ))}
                </div>
              </AdvancedField>
            ) : null}

            {supportsField(plan, "reuse_existing_roster") ? (
              <AdvancedField title="本地 baseline / snapshot 复用" description="优先利用已有本地资产。">
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={Boolean(reviewDecision.reuseExistingRoster)}
                    onChange={(event) =>
                      onReviewDecisionChange({
                        reuseExistingRoster: event.target.checked,
                      })
                    }
                  />
                  <span>优先复用本地 baseline / snapshot</span>
                </label>
              </AdvancedField>
            ) : null}

            {supportsField(plan, "run_former_search_seed") ? (
              <AdvancedField title="Former lane 增量" description="允许补充离职成员相关的增量召回。">
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={Boolean(reviewDecision.runFormerSearchSeed)}
                    onChange={(event) =>
                      onReviewDecisionChange({
                        runFormerSearchSeed: event.target.checked,
                      })
                    }
                  />
                  <span>允许补 former lane 增量</span>
                </label>
              </AdvancedField>
            ) : null}

            {supportsField(plan, "use_company_employees_lane") ? (
              <AdvancedField title="Current roster lane" description="控制是否启用公司成员采集 lane。">
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={Boolean(reviewDecision.useCompanyEmployeesLane)}
                    onChange={(event) =>
                      onReviewDecisionChange({
                        useCompanyEmployeesLane: event.target.checked,
                      })
                    }
                  />
                  <span>启用 current roster / company-employees lane</span>
                </label>
              </AdvancedField>
            ) : null}

            {supportsField(plan, "keyword_priority_only") ? (
              <AdvancedField title="Current lane 检索模式" description="优先使用 scoped keyword search。">
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={Boolean(reviewDecision.keywordPriorityOnly)}
                    onChange={(event) =>
                      onReviewDecisionChange({
                        keywordPriorityOnly: event.target.checked,
                      })
                    }
                  />
                  <span>Current lane 以 scoped keyword search 为主</span>
                </label>
              </AdvancedField>
            ) : null}

            {supportsField(plan, "former_keyword_queries_only") ? (
              <AdvancedField title="Former lane 检索模式" description="控制 former lane 的关键词检索策略。">
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={Boolean(reviewDecision.formerKeywordQueriesOnly)}
                    onChange={(event) =>
                      onReviewDecisionChange({
                        formerKeywordQueriesOnly: event.target.checked,
                      })
                    }
                  />
                  <span>Former lane 仅跑关键词 search</span>
                </label>
              </AdvancedField>
            ) : null}

            {supportsField(plan, "force_fresh_run") ? (
              <AdvancedField title="强制 fresh run" description="跳过历史资产，按 fresh run 执行。">
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={Boolean(reviewDecision.forceFreshRun)}
                    onChange={(event) =>
                      onReviewDecisionChange({
                        forceFreshRun: event.target.checked,
                      })
                    }
                  />
                  <span>本次强制 fresh run，不复用历史资产</span>
                </label>
              </AdvancedField>
            ) : null}

            {supportsField(plan, "provider_people_search_query_strategy") ? (
              <AdvancedField title="Provider search query strategy" description="控制 provider query 的合并策略。">
                <select
                  value={reviewDecision.providerPeopleSearchQueryStrategy || "all_queries_union"}
                  onChange={(event) =>
                    onReviewDecisionChange({
                      providerPeopleSearchQueryStrategy: event.target.value,
                    })
                  }
                >
                  <option value="all_queries_union">all_queries_union</option>
                  <option value="primary_only">primary_only</option>
                </select>
              </AdvancedField>
            ) : null}

            {supportsField(plan, "provider_people_search_max_queries") ? (
              <AdvancedField title="Provider max queries" description="限定 provider query 的最大轮数。">
                <input
                  className="text-input"
                  type="number"
                  min={1}
                  max={24}
                  value={reviewDecision.providerPeopleSearchMaxQueries ?? ""}
                  onChange={(event) =>
                    onReviewDecisionChange({
                      providerPeopleSearchMaxQueries: event.target.value ? Number(event.target.value) : null,
                    })
                  }
                />
              </AdvancedField>
            ) : null}

            {supportsField(plan, "acquisition_strategy_override") ? (
              <AdvancedField title="Acquisition strategy override" description="必要时手动覆盖执行策略。">
                <select
                  value={reviewDecision.acquisitionStrategyOverride || ""}
                  onChange={(event) =>
                    onReviewDecisionChange({
                      acquisitionStrategyOverride: event.target.value,
                    })
                  }
                >
                  <option value="">保持当前策略</option>
                  <option value="full_company_roster">full_company_roster</option>
                  <option value="scoped_search_roster">scoped_search_roster</option>
                  <option value="former_employee_search">former_employee_search</option>
                </select>
              </AdvancedField>
            ) : null}

            {supportsField(plan, "precision_recall_bias") ? (
              <AdvancedField title="Precision / recall bias" description="控制精度与召回倾向。">
                <select
                  value={reviewDecision.precisionRecallBias || ""}
                  onChange={(event) =>
                    onReviewDecisionChange({
                      precisionRecallBias: event.target.value,
                    })
                  }
                >
                  <option value="">保持默认</option>
                  <option value="precision">precision</option>
                  <option value="balanced">balanced</option>
                  <option value="recall">recall</option>
                </select>
              </AdvancedField>
            ) : null}
          </details>
        ) : null}
      </section>

      <div className="action-row">
        <button type="button" className="ghost-button" onClick={onApplyRevision} disabled={isApplyingRevision}>
          {isApplyingRevision ? "更新中..." : "修改方案"}
        </button>
        <button
          type="button"
          className="primary-button"
          data-testid="plan-confirm-button"
          onClick={handleConfirm}
          disabled={isConfirming || Boolean(cohortPreviewBlocker)}
        >
          {isConfirming ? "准备执行..." : "确认执行"}
        </button>
      </div>
    </section>
  );
}
