import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { LocalAssetTabs } from "../components/LocalAssetTabs";
import {
  getCollectionAssetOverview,
  type CollectionAssetOverview,
} from "../lib/api";
import {
  assetStatusLabel,
  collectionDisplayName,
  collectionLogoImageUrl,
  collectionLogoLabel,
  collectionLogoText,
  formatAssetNumber,
  profileProgressPercent,
  profileProgressText,
} from "../lib/localAssetPresentation";

export function CollectionsPage() {
  const navigate = useNavigate();
  const [overview, setOverview] = useState<CollectionAssetOverview | null>(null);
  const [errorMessage, setErrorMessage] = useState("");

  useEffect(() => {
    let isMounted = true;
    getCollectionAssetOverview()
      .then((payload) => {
        if (!isMounted) {
          return;
        }
        setOverview(payload);
      })
      .catch((error) => {
        if (!isMounted) {
          return;
        }
        setErrorMessage(error instanceof Error ? error.message : "本地资产 overview 暂不可用。");
      });
    return () => {
      isMounted = false;
    };
  }, []);

  return (
    <section className="page">
      <LocalAssetTabs active="overview" />
      <section className="panel">
        {errorMessage ? (
          <div className="warning-card error-card">
            <strong>本地资产 overview 不可用</strong>
            <p>{errorMessage}</p>
          </div>
        ) : overview ? (
          <div className="collection-overview-panel">
            <div className="collection-overview-header">
              <div>
                <span className="metric-label">已发布公司资产</span>
                <strong>{formatAssetNumber(overview.collectionCount)}</strong>
              </div>
              <p className="muted">这里只展示可浏览的公司资产。</p>
            </div>
            {overview.collections.length ? (
              <div className="collection-overview-grid">
                {overview.collections.map((item) => {
                  const displayName = collectionDisplayName(item);
                  const profilePercent = profileProgressPercent(item);
                  const logoUrl = collectionLogoImageUrl(item);
                  return (
                    <article key={item.collectionId} className="collection-overview-card">
                      <button
                        type="button"
                        className="collection-overview-card-button"
                        disabled={item.status !== "ready" || !item.collectionId}
                        onClick={() => {
                          navigate(`/collections/${encodeURIComponent(item.collectionId)}`);
                        }}
                      >
                        <span className="company-logo-mark" aria-label={collectionLogoLabel(item)}>
                          {logoUrl ? (
                            <img src={logoUrl} alt="" className="company-logo-image" loading="lazy" />
                          ) : (
                            collectionLogoText(item)
                          )}
                        </span>
                        <span className="collection-overview-card-main">
                          <span className="collection-overview-card-title-row">
                            <strong>{displayName}</strong>
                            <span className={`phase-pill phase-${item.status || "idle"}`}>{assetStatusLabel(item.status)}</span>
                          </span>
                          <span className="collection-overview-card-stats">
                            <span>{formatAssetNumber(item.candidateCount)} 位候选人</span>
                            <span>Profile {profileProgressText(item)}</span>
                          </span>
                          <span className="asset-progress-bar" aria-label={`Profile 完成度 ${profilePercent}%`}>
                            <span style={{ width: `${profilePercent}%` }} />
                          </span>
                        </span>
                      </button>
                    </article>
                  );
                })}
              </div>
            ) : (
              <div className="warning-card">
                <strong>暂无可服务公司资产</strong>
                <p>当前没有已发布的公司资产。完成一次公司资产导入或回填后会出现在这里。</p>
              </div>
            )}
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
