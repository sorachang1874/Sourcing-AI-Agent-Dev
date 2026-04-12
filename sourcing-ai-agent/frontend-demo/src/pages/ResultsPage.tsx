import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { ConfidenceBadge, StatusBadge } from "../components/Badges";
import { Avatar } from "../components/Avatar";
import { getDashboard } from "../lib/api";
import { writeDemoSession } from "../lib/demoSession";
import type { Candidate, DashboardData } from "../types";

interface ContactLink {
  label: string;
  url: string;
  kind: "linkedin" | "email" | "github" | "twitter" | "scholar" | "website";
}

const URL_REGEX = /(https?:\/\/[^\s)]+)|(www\.[^\s)]+)/gi;
const EMAIL_REGEX = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi;

function cleanEducationLine(value: string): string {
  return value
    .replace(/\s+/g, " ")
    .replace(/\s*·\s*/g, " · ")
    .trim();
}

function sanitizeContactText(value: string): string {
  return value
    .replace(URL_REGEX, " ")
    .replace(EMAIL_REGEX, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function pickSchoolLine(candidate: Candidate): string {
  const first = candidate.education[0] || "";
  if (!first) {
    return "Education details unavailable";
  }
  return cleanEducationLine(first);
}

function pickRoleLine(candidate: Candidate): string {
  const company = candidate.currentCompany || "Unknown company";
  const role = candidate.headline || "Candidate profile";
  return `${company} · ${role}`;
}

function buildWhyCandidateText(candidate: Candidate): string {
  const reason = sanitizeContactText(candidate.matchReasons[0] || "");
  const summary = sanitizeContactText(candidate.summary || "");
  const text = [reason, summary].filter(Boolean).join(" ");
  return text || "Profile evidence indicates strong role fit for this query.";
}

function normalizeUrl(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return "";
  }
  if (/^https?:\/\//i.test(trimmed) || /^mailto:/i.test(trimmed)) {
    return trimmed;
  }
  if (/^www\./i.test(trimmed)) {
    return `https://${trimmed}`;
  }
  return "";
}

function extractContactLinks(candidate: Candidate): ContactLink[] {
  const results: ContactLink[] = [];
  const seen = new Set<string>();

  const inferKind = (url: string): ContactLink["kind"] => {
    const lower = url.toLowerCase();
    if (lower.startsWith("mailto:")) {
      return "email";
    }
    if (lower.includes("linkedin.com")) {
      return "linkedin";
    }
    if (lower.includes("github.com")) {
      return "github";
    }
    if (lower.includes("x.com") || lower.includes("twitter.com")) {
      return "twitter";
    }
    if (lower.includes("scholar.google")) {
      return "scholar";
    }
    return "website";
  };

  const defaultLabel = (url: string, kind: ContactLink["kind"]): string => {
    if (kind === "email") {
      return "邮箱";
    }
    if (kind === "github") {
      return "GitHub";
    }
    if (kind === "twitter") {
      return "X / Twitter";
    }
    if (kind === "scholar") {
      return "Google Scholar";
    }
    if (kind === "linkedin") {
      return "LinkedIn";
    }
    try {
      const host = new URL(url).hostname.replace(/^www\./, "");
      return host || "个人网站";
    } catch {
      return "外部链接";
    }
  };

  const pushLink = (rawUrl: string, preferredLabel = "") => {
    const url = normalizeUrl(rawUrl);
    if (!url || seen.has(url)) {
      return;
    }
    const kind = inferKind(url);
    if (kind === "linkedin") {
      return;
    }
    const label = preferredLabel || defaultLabel(url, kind);
    seen.add(url);
    results.push({ label, url, kind });
  };

  for (const link of candidate.externalLinks || []) {
    pushLink(link.url, link.label || "");
  }

  const fromText = [
    candidate.summary,
    candidate.notesSnippet || "",
    ...candidate.matchReasons,
    candidate.headline,
    ...candidate.education,
    ...candidate.experience,
  ].join(" ");

  for (const match of fromText.matchAll(URL_REGEX)) {
    const raw = match[0] || "";
    pushLink(raw);
  }
  for (const match of fromText.matchAll(EMAIL_REGEX)) {
    const email = match[0] || "";
    if (email) {
      pushLink(`mailto:${email}`, "邮箱");
    }
  }

  for (const evidence of candidate.evidence) {
    if (!evidence.url || evidence.url === "#") {
      continue;
    }
    if (evidence.type === "github") {
      pushLink(evidence.url, "GitHub");
      continue;
    }
    if (evidence.type === "homepage") {
      pushLink(evidence.url, "个人网站");
      continue;
    }
    if (evidence.type === "cv") {
      pushLink(evidence.url, "简历");
      continue;
    }
    pushLink(evidence.url);
  }

  return results.slice(0, 4);
}

function linkIcon(kind: ContactLink["kind"]): string {
  if (kind === "linkedin") {
    return "https://www.linkedin.com/favicon.ico";
  }
  if (kind === "github") {
    return "https://github.githubassets.com/favicons/favicon.svg";
  }
  if (kind === "twitter") {
    return "https://x.com/favicon.ico";
  }
  if (kind === "scholar") {
    return "https://scholar.google.com/favicon.ico";
  }
  if (kind === "email") {
    return "https://ssl.gstatic.com/ui/v1/icons/mail/rfr/gmail.ico";
  }
  return "https://www.google.com/s2/favicons?sz=64&domain=example.com";
}

function pickKeywords(candidate: Candidate): string[] {
  const corpus = [
    candidate.headline,
    candidate.summary,
    candidate.notesSnippet || "",
    candidate.currentCompany || "",
    ...candidate.focusAreas,
    ...candidate.matchReasons,
    ...candidate.education,
    ...candidate.experience,
  ]
    .join(" ")
    .toLowerCase();

  const taxonomy: Array<{ label: string; patterns: string[] }> = [
    { label: "Multimodal", patterns: ["multimodal", "multi-modal", "vision-language", "vlm"] },
    { label: "LLM Inference", patterns: ["inference", "serving", "state space model", "token", "latency"] },
    { label: "Distributed Systems", patterns: ["distributed", "microservice", "kubernetes", "cluster", "scalab"] },
    { label: "Machine Learning", patterns: ["machine learning", "ml ", "ml/", "ml,"] },
    { label: "Deep Learning", patterns: ["deep learning", "neural network", "transformer"] },
    { label: "Generative AI", patterns: ["generative ai", "genai", "diffusion", "text-to", "image generation"] },
    { label: "Computer Vision", patterns: ["computer vision", "vision", "image", "video"] },
    { label: "NLP", patterns: ["nlp", "language model", "natural language"] },
    { label: "Reinforcement Learning", patterns: ["reinforcement learning", "rl ", "policy"] },
    { label: "AI Infrastructure", patterns: ["mlops", "ai infra", "training pipeline", "gpu", "cuda"] },
    { label: "Model Optimization", patterns: ["quantization", "pruning", "lora", "distillation"] },
  ];

  const matched = taxonomy
    .filter((item) => item.patterns.some((pattern) => corpus.includes(pattern)))
    .map((item) => item.label);
  return Array.from(new Set(matched)).slice(0, 3);
}

function filterCandidates(candidates: Candidate[], layer: string, group: string) {
  return candidates.filter((candidate) => {
    const layerMatch =
      layer === "all" ||
      (layer === "high" && candidate.confidence === "high") ||
      (layer === "lead_only" && candidate.confidence === "lead_only") ||
      (layer === "confirmed" && candidate.confidence !== "lead_only");
    const groupMatch = group === "All" || candidate.team === group;
    return layerMatch && groupMatch;
  });
}

const emptyDashboard: DashboardData = {
  title: "Thinking Machines Lab Talent Asset View",
  snapshotId: "--",
  queryLabel: "",
  totalCandidates: 0,
  totalEvidence: 0,
  manualReviewCount: 0,
  layers: [
    { id: "all", label: "Layer 0 · All Candidates", count: 0 },
    { id: "lead_only", label: "Layer 1 · Lead / Low Confidence", count: 0 },
    { id: "high", label: "Layer 2 · High Confidence", count: 0 },
    { id: "confirmed", label: "Layer 3 · Confirmed Direction", count: 0 },
  ],
  groups: ["All"],
  candidates: [],
};

export function ResultsPage() {
  const [dashboard, setDashboard] = useState<DashboardData>(emptyDashboard);
  const [isLoading, setIsLoading] = useState(true);
  const [selectedLayer, setSelectedLayer] = useState("all");
  const [selectedGroup, setSelectedGroup] = useState("All");
  const [keyword, setKeyword] = useState("");
  const [selectedCandidateId, setSelectedCandidateId] = useState("");
  const [confidenceFilter, setConfidenceFilter] = useState("全部");
  const [employmentFilter, setEmploymentFilter] = useState("全部");

  useEffect(() => {
    let isMounted = true;
    void getDashboard()
      .then((payload) => {
        if (!isMounted) {
          return;
        }
        setDashboard(payload);
        writeDemoSession({ lastVisitedStage: "results" });
      })
      .finally(() => {
        if (isMounted) {
          setIsLoading(false);
        }
      });
    return () => {
      isMounted = false;
    };
  }, []);

  const visibleCandidates = useMemo(
    () =>
      filterCandidates(dashboard.candidates, selectedLayer, selectedGroup).filter((candidate) => {
        const haystack = [
          candidate.name,
          candidate.headline,
          candidate.summary,
          candidate.team,
          ...candidate.focusAreas,
          ...candidate.matchReasons,
        ]
          .join(" ")
          .toLowerCase();
        const keywordMatch = haystack.includes(keyword.toLowerCase());
        const confidenceMatch =
          confidenceFilter === "全部" ||
          (confidenceFilter === "高置信度" && candidate.confidence === "high") ||
          (confidenceFilter === "中置信度" && candidate.confidence === "medium") ||
          (confidenceFilter === "线索" && candidate.confidence === "lead_only");
        const employmentMatch =
          employmentFilter === "全部" ||
          (employmentFilter === "在职" && candidate.employmentStatus === "current") ||
          (employmentFilter === "前员工" && candidate.employmentStatus === "former") ||
          (employmentFilter === "线索" && candidate.employmentStatus === "lead");
        return keywordMatch && confidenceMatch && employmentMatch;
      }),
    [confidenceFilter, dashboard.candidates, employmentFilter, keyword, selectedGroup, selectedLayer],
  );

  useEffect(() => {
    if (visibleCandidates.length > 0 && !visibleCandidates.some((candidate) => candidate.id === selectedCandidateId)) {
      setSelectedCandidateId(visibleCandidates[0].id);
    }
    if (visibleCandidates.length === 0) {
      setSelectedCandidateId("");
    }
  }, [selectedCandidateId, visibleCandidates]);

  const selectedCandidate = useMemo(
    () => visibleCandidates.find((candidate) => candidate.id === selectedCandidateId) ?? visibleCandidates[0],
    [selectedCandidateId, visibleCandidates],
  );

  if (isLoading) {
    return (
      <section className="page">
        <header className="page-header split-header">
          <div>
            <p className="eyebrow">结果看板</p>
            <h2>{emptyDashboard.title}</h2>
          </div>
          <div className="stat-pill-row">
            <StatusBadge label="正在加载本地资产..." />
          </div>
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
          <p className="eyebrow">Step 4 · 结果看板</p>
          <h2>{dashboard.title}</h2>
        </div>
        <div className="stat-pill-row">
          <StatusBadge label={`快照 ${dashboard.snapshotId}`} />
          <StatusBadge label={`${dashboard.totalCandidates} 位候选人`} />
          <StatusBadge label={`${dashboard.totalEvidence} 条证据`} />
          <StatusBadge label={isLoading ? "正在加载本地资产..." : "本地资产已就绪"} />
        </div>
      </header>

      <section className="panel">
        <div className="results-toolbar">
          <div className="results-search">
            <label className="field-label" htmlFor="results-keyword">
              结果内搜索
            </label>
            <input
              id="results-keyword"
              className="text-input"
              value={keyword}
              onChange={(event) => setKeyword(event.target.value)}
              placeholder="按姓名、方向、团队、命中原因筛选"
            />
          </div>
          <div className="toolbar-stats">
            <div className="metric-card">
              <span className="muted">当前分层</span>
              <strong>{dashboard.layers.find((layer) => layer.id === selectedLayer)?.label || "全部"}</strong>
            </div>
            <div className="metric-card">
              <span className="muted">当前分组</span>
              <strong>{selectedGroup}</strong>
            </div>
            <div className="metric-card">
              <span className="muted">可见候选人</span>
              <strong>{visibleCandidates.length}</strong>
            </div>
          </div>
        </div>
        <div className="results-filter-row">
          <label className="results-filter">
            <span className="field-label">置信度</span>
            <select value={confidenceFilter} onChange={(event) => setConfidenceFilter(event.target.value)}>
              <option>全部</option>
              <option>高置信度</option>
              <option>中置信度</option>
              <option>线索</option>
            </select>
          </label>
          <label className="results-filter">
            <span className="field-label">状态</span>
            <select value={employmentFilter} onChange={(event) => setEmploymentFilter(event.target.value)}>
              <option>全部</option>
              <option>在职</option>
              <option>前员工</option>
              <option>线索</option>
            </select>
          </label>
        </div>
      </section>

      <div className="dashboard-layout">
        <aside className="panel funnel-panel">
          <div className="panel-header">
            <h3>置信度漏斗</h3>
          </div>
          <div className="stack">
            {dashboard.layers.map((layer) => (
              <button
                key={layer.id}
                type="button"
                className={`funnel-item${selectedLayer === layer.id ? " selected" : ""}`}
                onClick={() => setSelectedLayer(layer.id)}
              >
                <span>{layer.label}</span>
                <strong>{layer.count}</strong>
              </button>
            ))}
          </div>
        </aside>

        <section className="panel candidate-panel">
          <div className="panel-header">
            <h3>候选人列表</h3>
            <p className="muted">当前显示 {visibleCandidates.length} 位</p>
          </div>
          <div className="candidate-list">
            {visibleCandidates.map((candidate) => {
              const keywords = pickKeywords(candidate);
              const contactLinks = extractContactLinks(candidate);
              return (
                <article
                  key={candidate.id}
                  className={`candidate-card${selectedCandidate?.id === candidate.id ? " selected-card" : ""}`}
                  onClick={() => setSelectedCandidateId(candidate.id)}
                >
                <div className="candidate-header">
                  <Avatar name={candidate.name} src={candidate.avatarUrl} size="small" />
                  <div className="candidate-copy">
                    <div className="candidate-title-row">
                      <div>
                        <h4>{candidate.name}</h4>
                        <p className="candidate-meta-line">{pickRoleLine(candidate)}</p>
                        <p className="candidate-meta-line">{pickSchoolLine(candidate)}</p>
                      </div>
                      <ConfidenceBadge confidence={candidate.confidence} />
                    </div>
                  </div>
                </div>

                <div className="candidate-why-block">
                  <h5>Why this candidate?</h5>
                  <p>{buildWhyCandidateText(candidate)}</p>
                </div>

                {keywords.length > 0 ? (
                  <div className="candidate-keywords">
                    {keywords.map((keyword) => (
                      <span key={keyword} className="keyword-chip">
                        {keyword}
                      </span>
                    ))}
                  </div>
                ) : null}

                <div className="candidate-actions">
                  <StatusBadge label={candidate.team} />
                  <StatusBadge label={candidate.employmentStatus} />
                  {candidate.linkedinUrl ? (
                    <a className="ghost-button" href={candidate.linkedinUrl} target="_blank" rel="noreferrer">
                      <img className="link-icon-image" src={linkIcon("linkedin")} alt="" aria-hidden="true" />
                      <span>打开 LinkedIn</span>
                    </a>
                  ) : null}
                  {contactLinks.length > 0 ? (
                    <div className="candidate-contact-links">
                      {contactLinks.map((link) => (
                        <a key={`${candidate.id}-${link.url}`} className="ghost-button" href={link.url} target="_blank" rel="noreferrer">
                          <img className="link-icon-image" src={linkIcon(link.kind)} alt="" aria-hidden="true" />
                          <span>{link.label}</span>
                        </a>
                      ))}
                    </div>
                  ) : null}
                  <Link className="ghost-button" to={`/candidate/${candidate.id}`}>
                    打开 One-Pager
                  </Link>
                  <Link className="ghost-button" to="/manual-review">
                    加入审核视图
                  </Link>
                </div>
                </article>
              );
            })}
          </div>
        </section>

        <aside className="panel grouping-panel">
          <div className="panel-header">
            <h3>按组查看</h3>
          </div>
          <div className="stack">
            {dashboard.groups.map((group) => (
              <button
                key={group}
                type="button"
                className={`group-item${selectedGroup === group ? " selected" : ""}`}
                onClick={() => setSelectedGroup(group)}
              >
                {group}
              </button>
            ))}
          </div>
          <div className="divider" />
          {selectedCandidate ? (
            <div className="stack">
              <h4>右侧摘要</h4>
              <strong>{selectedCandidate.name}</strong>
              <p className="muted">{selectedCandidate.headline}</p>
              <div className="meta-row">
                {selectedCandidate.currentCompany ? <span>{selectedCandidate.currentCompany}</span> : null}
                {selectedCandidate.location ? <span>{selectedCandidate.location}</span> : null}
              </div>
              <p className="muted">{selectedCandidate.summary}</p>
              {selectedCandidate.notesSnippet ? <p className="candidate-note">{selectedCandidate.notesSnippet}</p> : null}
              <div className="tag-row">
                {selectedCandidate.focusAreas.slice(0, 4).map((tag) => (
                  <span key={tag} className="tag">
                    {tag}
                  </span>
                ))}
              </div>
              <div className="divider" />
              <h4>命中原因</h4>
              <ul className="flat-list compact">
                {selectedCandidate.matchReasons.slice(0, 3).map((reason) => (
                  <li key={reason}>{reason}</li>
                ))}
              </ul>
              <div className="divider" />
              <h4>证据概览</h4>
              <div className="stack">
                {selectedCandidate.evidence.slice(0, 3).map((item) => (
                  <div key={item.label} className="worker-card">
                    <strong>{item.label}</strong>
                    <p className="muted">{item.excerpt}</p>
                  </div>
                ))}
              </div>
              {selectedCandidate.linkedinUrl ? (
                <>
                  <div className="divider" />
                  <a className="ghost-button" href={selectedCandidate.linkedinUrl} target="_blank" rel="noreferrer">
                    在 LinkedIn 中查看原始资料
                  </a>
                </>
              ) : null}
              <div className="action-row">
                <Link className="primary-button" to={`/candidate/${selectedCandidate.id}`}>
                  查看完整详情
                </Link>
                <Link className="ghost-button" to="/manual-review">
                  转到人工审核
                </Link>
              </div>
            </div>
          ) : (
            <p className="muted">当前没有可展示的候选人。</p>
          )}
        </aside>
      </div>
    </section>
  );
}
