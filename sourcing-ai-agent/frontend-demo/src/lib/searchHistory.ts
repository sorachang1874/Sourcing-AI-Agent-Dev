import type { SearchHistoryItem } from "../types";

const HISTORY_STORAGE_KEY = "sourcing_history";
const HISTORY_UPDATED_EVENT = "sourcing-history-updated";
const START_NEW_SEARCH_EVENT = "sourcing-start-new-search";

function canUseStorage(): boolean {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

function emitHistoryUpdated(): void {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event(HISTORY_UPDATED_EVENT));
  }
}

export function searchHistoryUpdatedEventName(): string {
  return HISTORY_UPDATED_EVENT;
}

export function startNewSearchEventName(): string {
  return START_NEW_SEARCH_EVENT;
}

export function summarizeSearchQuery(queryText: string): string {
  const normalized = queryText.replace(/\s+/g, " ").trim();
  if (normalized.length <= 30) {
    return normalized;
  }
  return `${normalized.slice(0, 30)}...`;
}

export function readSearchHistory(): SearchHistoryItem[] {
  if (!canUseStorage()) {
    return [];
  }
  try {
    const raw = window.localStorage.getItem(HISTORY_STORAGE_KEY);
    if (!raw) {
      return [];
    }
    const payload = JSON.parse(raw) as SearchHistoryItem[];
    if (!Array.isArray(payload)) {
      return [];
    }
    return payload
      .filter((item) => item && typeof item === "object")
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt));
  } catch {
    return [];
  }
}

export function readSearchHistoryItem(historyId: string): SearchHistoryItem | null {
  return readSearchHistory().find((item) => item.id === historyId) || null;
}

export function upsertSearchHistoryItem(item: SearchHistoryItem): SearchHistoryItem[] {
  const nextItems = [item, ...readSearchHistory().filter((entry) => entry.id !== item.id)].slice(0, 24);
  if (canUseStorage()) {
    window.localStorage.setItem(HISTORY_STORAGE_KEY, JSON.stringify(nextItems));
  }
  emitHistoryUpdated();
  return nextItems;
}

export function deleteSearchHistoryItem(historyId: string): SearchHistoryItem[] {
  const nextItems = readSearchHistory().filter((item) => item.id !== historyId);
  if (canUseStorage()) {
    window.localStorage.setItem(HISTORY_STORAGE_KEY, JSON.stringify(nextItems));
  }
  emitHistoryUpdated();
  return nextItems;
}
