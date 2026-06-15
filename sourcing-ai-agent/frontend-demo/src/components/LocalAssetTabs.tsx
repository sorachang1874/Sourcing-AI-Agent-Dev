import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getCollectionAssetEntry } from "../lib/api";
import { collectionDisplayName } from "../lib/localAssetPresentation";

type LocalAssetTabId = "overview" | "company" | "board" | "review" | "targets";

interface LocalAssetTabsProps {
  active: LocalAssetTabId;
  collectionId?: string;
  companyName?: string;
  projectionId?: string;
  jobId?: string;
  historyId?: string;
  candidateId?: string;
}

function appendContext(path: string, params: Record<string, string | undefined>): string {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    const normalized = String(value || "").trim();
    if (normalized) {
      query.set(key, normalized);
    }
  });
  const serialized = query.toString();
  return serialized ? `${path}?${serialized}` : path;
}

export function LocalAssetTabs({
  active,
  collectionId = "",
  companyName = "",
  projectionId = "",
  jobId = "",
  historyId = "",
  candidateId = "",
}: LocalAssetTabsProps) {
  const normalizedCollectionId = collectionId.trim();
  const [resolvedAsset, setResolvedAsset] = useState<{
    companyName: string;
    projectionId: string;
  } | null>(null);

  useEffect(() => {
    let isMounted = true;
    const needsAssetLookup = normalizedCollectionId && (!projectionId.trim() || !companyName.trim());
    if (!needsAssetLookup) {
      setResolvedAsset(null);
      return () => {
        isMounted = false;
      };
    }
    getCollectionAssetEntry(normalizedCollectionId)
      .then((entry) => {
        if (!isMounted) {
          return;
        }
        setResolvedAsset({
          companyName: collectionDisplayName(entry),
          projectionId: entry.projectionId,
        });
      })
      .catch(() => {
        if (!isMounted) {
          return;
        }
        setResolvedAsset(null);
      });
    return () => {
      isMounted = false;
    };
  }, [companyName, normalizedCollectionId, projectionId]);

  const normalizedProjectionId = projectionId.trim() || resolvedAsset?.projectionId.trim() || "";
  const normalizedJobId = jobId.trim();
  const displayName = normalizedCollectionId
    ? collectionDisplayName({
        collectionId: normalizedCollectionId,
        displayName: companyName || resolvedAsset?.companyName || "",
      })
    : "公司主页";
  const collectionContext = normalizedCollectionId || undefined;
  const tabs: Array<{
    id: LocalAssetTabId;
    label: string;
    to: string;
    enabled: boolean;
    disabledLabel?: string;
  }> = [
    {
      id: "overview",
      label: "公司资产 Overview",
      to: "/collections",
      enabled: true,
    },
    {
      id: "company",
      label: displayName,
      to: normalizedCollectionId ? `/collections/${encodeURIComponent(normalizedCollectionId)}` : "",
      enabled: Boolean(normalizedCollectionId),
      disabledLabel: "先选择公司",
    },
    {
      id: "board",
      label: "候选人看板",
      to: normalizedProjectionId
        ? appendContext(`/projections/${encodeURIComponent(normalizedProjectionId)}`, {
            collection: collectionContext,
          })
        : "",
      enabled: Boolean(normalizedProjectionId),
      disabledLabel: "资产未发布",
    },
    {
      id: "review",
      label: "人工审核",
      to: normalizedJobId
        ? appendContext("/manual-review", {
            collection: collectionContext,
            job: normalizedJobId,
            history: historyId,
            candidate: candidateId,
          })
        : "",
      enabled: Boolean(normalizedJobId),
      disabledLabel: "需要任务上下文",
    },
    {
      id: "targets",
      label: "目标候选人",
      to: appendContext("/targets", {
        collection: collectionContext,
      }),
      enabled: true,
    },
  ];

  return (
    <nav className="workflow-step-shell local-asset-tab-shell" aria-label="本地资产导航">
      <div className="workflow-step-tabs local-asset-tabs" role="tablist">
        {tabs.map((tab) => {
          const className = `workflow-step-tab local-asset-tab${active === tab.id ? " active" : ""}${tab.enabled ? "" : " disabled"}`;
          if (!tab.enabled) {
            return (
              <span key={tab.id} className={className} aria-disabled="true" title={tab.disabledLabel}>
                <strong>{tab.label}</strong>
              </span>
            );
          }
          return (
            <Link key={tab.id} className={className} to={tab.to} aria-current={active === tab.id ? "page" : undefined}>
              <strong>{tab.label}</strong>
            </Link>
          );
        })}
      </div>
    </nav>
  );
}
