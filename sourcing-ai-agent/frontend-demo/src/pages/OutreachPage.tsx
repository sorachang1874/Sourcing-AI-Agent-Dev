import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { StatusBadge } from "../components/Badges";
import { getDashboard } from "../lib/api";
import { writeDemoSession } from "../lib/demoSession";
import type { Candidate } from "../types";

type OutreachStatus = "未回复" | "婉拒" | "同意见面" | "已见面" | "访谈归档";

interface OutreachRecord {
  candidate: Candidate;
  status: OutreachStatus;
}

const statusFlow: OutreachStatus[] = ["未回复", "婉拒", "同意见面", "已见面", "访谈归档"];

function buildMessage(candidate: Candidate): string {
  const highlights = [
    candidate.headline,
    candidate.focusAreas.slice(0, 2).join(" / "),
    candidate.currentCompany || "",
  ]
    .filter(Boolean)
    .join("，");
  return `Hi ${candidate.name}，我们正在围绕多模态与模型系统方向搭建核心团队。看到你在${highlights}方面的背景非常匹配，想邀请你聊 20 分钟，看看是否有合作机会。`;
}

export function OutreachPage() {
  const [records, setRecords] = useState<OutreachRecord[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    setIsLoading(true);
    void getDashboard()
      .then((dashboard) => {
        if (!mounted) {
          return;
        }
        const seeded = dashboard.candidates.slice(0, 24).map((candidate, index) => ({
          candidate,
          status: statusFlow[index % statusFlow.length],
        }));
        setRecords(seeded);
        setSelectedId(seeded[0]?.candidate.id || "");
        writeDemoSession({ lastVisitedStage: "outreach" });
      })
      .finally(() => {
        if (mounted) {
          setIsLoading(false);
        }
      });
    return () => {
      mounted = false;
    };
  }, []);

  const selected = useMemo(
    () => records.find((item) => item.candidate.id === selectedId) || records[0],
    [records, selectedId],
  );

  if (isLoading) {
    return (
      <section className="page">
        <header className="page-header">
          <p className="eyebrow">Step 5 · 触达跟进</p>
          <h2>生成 Message / Email 话术并追踪沟通状态</h2>
        </header>
        <section className="panel">
          <div className="results-skeleton">
            <div className="skeleton-line short" />
            <div className="skeleton-line" />
            <div className="skeleton-line" />
          </div>
        </section>
      </section>
    );
  }

  return (
    <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">Step 5 · 触达跟进</p>
          <h2>基于候选背景自动生成触达文案，并持续推进状态流转</h2>
        </div>
        <div className="stat-pill-row">
          <StatusBadge label={`${records.length} 位触达对象`} />
          <Link className="ghost-button" to="/results">
            返回结果看板
          </Link>
        </div>
      </header>

      <div className="dashboard-layout">
        <section className="panel">
          <div className="panel-header">
            <h3>候选触达队列</h3>
          </div>
          <div className="stack">
            {records.map((record) => (
              <button
                key={record.candidate.id}
                type="button"
                className={`review-item${selected?.candidate.id === record.candidate.id ? " selected" : ""}`}
                onClick={() => setSelectedId(record.candidate.id)}
              >
                <div className="timeline-row">
                  <strong>{record.candidate.name}</strong>
                  <StatusBadge label={record.status} />
                </div>
                <p className="muted">{record.candidate.currentCompany || "Unknown company"}</p>
                <p className="muted">{record.candidate.headline}</p>
              </button>
            ))}
          </div>
        </section>

        <section className="panel candidate-panel">
          {selected ? (
            <>
              <div className="panel-header">
                <h3>{selected.candidate.name}</h3>
                <StatusBadge label={selected.status} />
              </div>
              <p className="muted">{selected.candidate.summary}</p>
              <div className="divider" />
              <h4>推荐触达文案</h4>
              <p className="muted">{buildMessage(selected.candidate)}</p>
              <div className="action-row">
                <button className="primary-button" type="button">
                  标记已发送
                </button>
                <button className="ghost-button" type="button">
                  记录反馈
                </button>
                {selected.candidate.linkedinUrl ? (
                  <a className="ghost-button" href={selected.candidate.linkedinUrl} target="_blank" rel="noreferrer">
                    LinkedIn 私信
                  </a>
                ) : null}
              </div>
            </>
          ) : null}
        </section>

        <aside className="panel grouping-panel">
          <div className="panel-header">
            <h3>状态追踪</h3>
          </div>
          <ul className="flat-list compact">
            <li>未回复：已发送消息，等待回应</li>
            <li>婉拒 / 同意见面：记录决策并触发下一动作</li>
            <li>已见面：沉淀访谈纪要</li>
            <li>访谈归档：形成可复用联系资产</li>
          </ul>
        </aside>
      </div>
    </section>
  );
}
