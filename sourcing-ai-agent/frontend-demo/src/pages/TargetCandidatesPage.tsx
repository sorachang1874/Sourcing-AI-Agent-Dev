import { TargetCandidatesPanel } from "../components/TargetCandidatesPanel";

export function TargetCandidatesPage() {
  return (
    <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">目标候选人</p>
          <h2>持续跟进与状态管理</h2>
        </div>
      </header>

      <TargetCandidatesPanel />
    </section>
  );
}
