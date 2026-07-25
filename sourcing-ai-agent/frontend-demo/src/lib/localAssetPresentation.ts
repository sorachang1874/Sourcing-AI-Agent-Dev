import type { CollectionAssetEntry, CollectionAssetOverviewItem } from "./api";

type LocalAssetLike = Pick<
  CollectionAssetEntry | CollectionAssetOverviewItem,
  "collectionId" | "displayName" | "companyMedia" | "status" | "profileFetchedCount" | "profileFetchRequiredCount"
>;

export function formatAssetNumber(value: number | undefined): string {
  const normalized = Math.max(0, Number(value || 0));
  return new Intl.NumberFormat("zh-CN").format(normalized);
}

export function collectionDisplayName(item: Pick<LocalAssetLike, "collectionId" | "displayName">): string {
  const explicitName = String(item.displayName || "").trim();
  const fallback = String(item.collectionId || "").replace(/^company:/i, "").trim();
  const name = explicitName || fallback || "Unknown company";
  const brandName = name.toLowerCase();
  if (brandName === "openai") {
    return "OpenAI";
  }
  if (brandName === "google") {
    return "Google";
  }
  if (brandName === "anthropic") {
    return "Anthropic";
  }
  if (name.length <= 3) {
    return name.toUpperCase();
  }
  return name.charAt(0).toUpperCase() + name.slice(1);
}

export function collectionInitials(name: string): string {
  const tokens = name
    .replace(/[^A-Za-z0-9\u4e00-\u9fa5 ]/g, " ")
    .split(/\s+/)
    .map((item) => item.trim())
    .filter(Boolean);
  if (tokens.length >= 2) {
    return `${tokens[0][0] || ""}${tokens[1][0] || ""}`.toUpperCase();
  }
  return (tokens[0] || name || "A").slice(0, 2).toUpperCase();
}

export function collectionLogoText(item: Pick<LocalAssetLike, "displayName" | "companyMedia">): string {
  const placeholderText = String(item.companyMedia?.placeholderText || "").trim();
  if (placeholderText) {
    return placeholderText;
  }
  return "??";
}

export function collectionLogoImageUrl(item: Pick<LocalAssetLike, "companyMedia">): string {
  const logoStatus = String(item.companyMedia?.logoStatus || "").trim();
  const logoUrl = String(item.companyMedia?.logoUrl || "").trim();
  return logoStatus === "available" && logoUrl ? logoUrl : "";
}

export function collectionLogoLabel(item: Pick<LocalAssetLike, "displayName" | "companyMedia">): string {
  const logoStatus = String(item.companyMedia?.logoStatus || "company_media_missing").trim();
  if (logoStatus === "logo_unavailable") {
    return `${collectionDisplayName({ collectionId: "", displayName: item.displayName })} logo unavailable`;
  }
  return String(item.companyMedia?.logoAlt || item.displayName || "Company logo").trim();
}

export function assetStatusLabel(status: string | undefined): string {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "ready") {
    return "可浏览";
  }
  if (normalized === "not_ready" || normalized === "pending") {
    return "准备中";
  }
  if (normalized === "failed" || normalized === "error") {
    return "不可用";
  }
  return normalized || "未知";
}

export function profileProgressText(item: Pick<LocalAssetLike, "profileFetchedCount" | "profileFetchRequiredCount">): string {
  return `${formatAssetNumber(item.profileFetchedCount)}/${formatAssetNumber(item.profileFetchRequiredCount)}`;
}

export function profileProgressPercent(item: Pick<LocalAssetLike, "profileFetchedCount" | "profileFetchRequiredCount">): number {
  const denominator = Math.max(0, Number(item.profileFetchRequiredCount || 0));
  if (denominator <= 0) {
    return 0;
  }
  return Math.max(0, Math.min(100, Math.round((Number(item.profileFetchedCount || 0) / denominator) * 100)));
}

export function indexReadinessLabel(watermark: string | undefined): string {
  return String(watermark || "").trim() ? "已生成" : "待构建";
}
