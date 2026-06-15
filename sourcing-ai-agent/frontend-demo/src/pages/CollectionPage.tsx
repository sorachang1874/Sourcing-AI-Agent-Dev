import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { LocalAssetTabs } from "../components/LocalAssetTabs";
import {
  getCollectionAssetEntry,
  getCompanyAssetFacts,
  type CollectionAssetEntry,
  type CompanyAssetFacts,
  type CompanyAssetRecord,
} from "../lib/api";
import {
  assetStatusLabel,
  collectionDisplayName,
  collectionLogoImageUrl,
  collectionLogoLabel,
  collectionLogoText,
  formatAssetNumber,
  indexReadinessLabel,
  profileProgressPercent,
  profileProgressText,
} from "../lib/localAssetPresentation";

export function CollectionPage() {
  const navigate = useNavigate();
  const routeParams = useParams();
  const collectionId = (routeParams.collectionId || "").trim();
  const [assetEntry, setAssetEntry] = useState<CollectionAssetEntry | null>(null);
  const [companyFacts, setCompanyFacts] = useState<CompanyAssetFacts | null>(null);
  const [errorMessage, setErrorMessage] = useState("");

  useEffect(() => {
    let isMounted = true;
    if (!collectionId) {
      setErrorMessage("缺少 collection_id，无法打开本地资产入口。");
      return () => {
        isMounted = false;
      };
    }
    getCollectionAssetEntry(collectionId)
      .then((entry) => {
        if (!isMounted) {
          return;
        }
        setAssetEntry(entry);
      })
      .catch((error) => {
        if (!isMounted) {
          return;
        }
        setErrorMessage(error instanceof Error ? error.message : "本地资产 projection 尚未生成。");
      });
    return () => {
      isMounted = false;
    };
  }, [collectionId, navigate]);

  useEffect(() => {
    let isMounted = true;
    if (!assetEntry) {
      setCompanyFacts(null);
      return () => {
        isMounted = false;
      };
    }
    getCompanyAssetFacts({
      companyKey: companyKeyFromCollectionId(assetEntry.collectionId),
      targetCompany: collectionDisplayName(assetEntry),
      limit: 100,
    }).then((facts) => {
      if (isMounted) {
        setCompanyFacts(facts);
      }
    });
    return () => {
      isMounted = false;
    };
  }, [assetEntry]);

  return (
    <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">本地资产</p>
          <h2>{assetEntry ? `${collectionDisplayName(assetEntry)} 本地资产` : "公司本地资产"}</h2>
        </div>
      </header>
      <LocalAssetTabs
        active="company"
        collectionId={collectionId}
        companyName={assetEntry ? collectionDisplayName(assetEntry) : ""}
        projectionId={assetEntry?.projectionId || ""}
      />
      <section className="panel">
        {errorMessage ? (
          <div className="warning-card error-card">
            <strong>本地资产入口不可用</strong>
            <p>{errorMessage}</p>
          </div>
        ) : assetEntry ? (
          <div className="collection-entry-panel">
            <div className="collection-entry-hero">
              <span className="company-logo-mark company-logo-mark-large" aria-label={collectionLogoLabel(assetEntry)}>
                {collectionLogoImageUrl(assetEntry) ? (
                  <img src={collectionLogoImageUrl(assetEntry)} alt="" className="company-logo-image" loading="lazy" />
                ) : (
                  collectionLogoText(assetEntry)
                )}
              </span>
              <div className="collection-entry-hero-copy">
                <div className="collection-entry-title-row">
                  <h3>{collectionDisplayName(assetEntry)}</h3>
                  <span className={`phase-pill phase-${assetEntry.status || "idle"}`}>
                    {assetStatusLabel(assetEntry.status)}
                  </span>
                </div>
                <p className="muted">
                  {formatAssetNumber(assetEntry.candidateCount)} 位候选人资产，Profile {profileProgressText(assetEntry)}
                </p>
                <span className="asset-progress-bar" aria-label={`Profile 完成度 ${profileProgressPercent(assetEntry)}%`}>
                  <span style={{ width: `${profileProgressPercent(assetEntry)}%` }} />
                </span>
              </div>
            </div>
            <div className="collection-entry-summary">
              <div>
                <span className="metric-label">候选人资产</span>
                <strong>{formatAssetNumber(assetEntry.candidateCount)}</strong>
              </div>
              <div>
                <span className="metric-label">Profile</span>
                <strong>{profileProgressText(assetEntry)}</strong>
              </div>
              <div>
                <span className="metric-label">候选人卡片</span>
                <strong>{formatAssetNumber(assetEntry.cardMaterializedCount)}</strong>
              </div>
            </div>
            <div className="company-fact-panel">
              <div className="company-fact-header">
                <div>
                  <span className="metric-label">公司资料资产</span>
                  <strong>{formatAssetNumber(companyFacts?.assets.length || 0)}</strong>
                </div>
                <span className="muted">官网、Research、Engineering、论文等只读资产</span>
              </div>
              {companyFacts?.assets.length ? (
                <div className="company-fact-grid">
                  {companyFacts.assets.slice(0, 6).map((asset) => (
                    <a
                      key={asset.assetId || asset.contentRef}
                      className="company-fact-card"
                      href={asset.sourceUrl || asset.contentRef}
                      target="_blank"
                      rel="noreferrer"
                    >
                      <span>{companyAssetTypeLabel(asset.assetType)}</span>
                      <strong>{companyAssetTitle(asset)}</strong>
                      <small>{asset.sourceUrl || asset.contentRef || "无公开链接"}</small>
                    </a>
                  ))}
                </div>
              ) : (
                <p className="muted compact-copy">暂无已同步的公司资料资产。完成公司 Public Web 刷新或历史回填后会显示在这里。</p>
              )}
            </div>
            <div className="collection-entry-actions">
              <button
                type="button"
                className="primary-button"
                onClick={() => {
                  navigate(`/projections/${encodeURIComponent(assetEntry.projectionId)}?collection=${encodeURIComponent(collectionId)}`);
                }}
              >
                打开候选人看板
              </button>
              <button
                type="button"
                className="ghost-button"
                onClick={() => {
                  navigate(`/targets?collection=${encodeURIComponent(collectionId)}`);
                }}
              >
                查看目标候选人
              </button>
            </div>
            <details className="collection-entry-diagnostics">
              <summary>资产状态</summary>
              <div className="collection-entry-meta">
                <span>版本：{assetEntry.activeCollectionVersion || "未提供"}</span>
                <span>筛选索引：{indexReadinessLabel(assetEntry.rawProfileIndexWatermark)}</span>
                <span>公开线索索引：{indexReadinessLabel(assetEntry.evidenceIndexWatermark)}</span>
                <span>计数：{assetEntry.countScope || "exact projection"}</span>
              </div>
            </details>
          </div>
        ) : (
          <div className="results-skeleton">
            <div className="skeleton-line short" />
            <div className="skeleton-line" />
            <div className="skeleton-line" />
          </div>
        )}
      </section>
    </section>
  );
}

function companyKeyFromCollectionId(collectionId: string): string {
  return String(collectionId || "").replace(/^company:/i, "").trim().toLowerCase();
}

function companyAssetTypeLabel(assetType: string): string {
  const normalized = String(assetType || "").trim();
  const labels: Record<string, string> = {
    company_homepage: "官网",
    company_research: "Research",
    company_engineering: "Engineering",
    company_blog: "Blog",
    company_news: "News",
    company_docs: "Docs",
    company_rss: "RSS",
    company_arxiv: "arXiv",
    company_openreview: "OpenReview",
    company_crawl: "Crawl",
  };
  return labels[normalized] || normalized || "公司资料";
}

function companyAssetTitle(asset: CompanyAssetRecord): string {
  const payload = (asset.metadata.model_safe_payload || {}) as Record<string, unknown>;
  const title = typeof payload.title === "string" ? payload.title.trim() : "";
  if (title) {
    return title;
  }
  const url = asset.sourceUrl || asset.contentRef;
  return url.replace(/^https?:\/\//i, "").replace(/\/$/, "") || companyAssetTypeLabel(asset.assetType);
}
