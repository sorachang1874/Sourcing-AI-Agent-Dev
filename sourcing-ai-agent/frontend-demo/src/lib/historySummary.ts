export function summarizeSearchQuery(queryText: string): string {
  const normalized = queryText.replace(/\s+/g, " ").trim();
  if (normalized.length <= 30) {
    return normalized;
  }
  return `${normalized.slice(0, 30)}...`;
}
