import type { DemoPlan } from "../types";

interface PlanCardProps {
  plan: DemoPlan;
  revisionText: string;
  isApplyingRevision: boolean;
  isConfirming: boolean;
  onRevisionChange: (value: string) => void;
  onApplyRevision: () => void;
  onConfirm: () => void;
}

export function PlanCard({
  plan,
  revisionText,
  isApplyingRevision,
  isConfirming,
  onRevisionChange,
  onApplyRevision,
  onConfirm,
}: PlanCardProps) {
  return (
    <section className="conversation-card plan-card">
      <div className="section-heading">
        <span className="section-step">Step 1</span>
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
        <div>
          <dt>项目范围</dt>
          <dd>{plan.projectScope}</dd>
        </div>
        <div>
          <dt>检索关键词</dt>
          <dd>{plan.keywords.join("、")}</dd>
        </div>
        <div>
          <dt>检索策略</dt>
          <dd>{plan.acquisitionStrategy}</dd>
        </div>
        <div>
          <dt>检索路径</dt>
          <dd>{plan.searchStrategy.join(" / ")}</dd>
        </div>
      </dl>

      {plan.reviewRequired ? (
        <div className="warning-card">
          <strong>注意</strong>
          <p>本次方案涉及全公司扫描，属于高成本操作，需要你先确认后才能进入执行阶段。</p>
        </div>
      ) : null}

      <div className="revision-panel">
        <label className="field-label" htmlFor="plan-revision">
          继续补充或修改要求
        </label>
        <textarea
          id="plan-revision"
          className="revision-input"
          value={revisionText}
          placeholder="例如：只看 Nano Banana 和 Veo 组，优先华人研究员，排除纯 academia。"
          onChange={(event) => onRevisionChange(event.target.value)}
          rows={3}
        />
      </div>

      <div className="action-row">
        <button type="button" className="ghost-button" onClick={onApplyRevision} disabled={isApplyingRevision}>
          {isApplyingRevision ? "更新中..." : "修改方案"}
        </button>
        <button type="button" className="primary-button" onClick={onConfirm} disabled={isConfirming}>
          {isConfirming ? "准备执行..." : "确认执行"}
        </button>
      </div>
    </section>
  );
}
