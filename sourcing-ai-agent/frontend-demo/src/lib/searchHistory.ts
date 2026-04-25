import { deleteFrontendHistory, listFrontendHistory } from "./api";
import {
  historyItemFromRecoveryEnvelope,
} from "./historyRecovery";
import type { SearchHistoryItem } from "../types";

const HISTORY_STORAGE_KEY = "sourcing_history";
const HISTORY_UPDATED_EVENT = "sourcing-history-updated";
const START_NEW_SEARCH_EVENT = "sourcing-start-new-search";
const HISTORY_LIMIT = 24;

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

function writeSearchHistory(items: SearchHistoryItem[]): SearchHistoryItem[] {
  const nextItems = items
    .filter((item) => item && typeof item === "object" && item.id)
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
    .slice(0, HISTORY_LIMIT);
  if (canUseStorage()) {
    window.localStorage.setItem(HISTORY_STORAGE_KEY, JSON.stringify(nextItems));
  }
  emitHistoryUpdated();
  return nextItems;
}

function mergeSearchHistoryItems(
  preferredItems: SearchHistoryItem[],
  fallbackItems: SearchHistoryItem[],
): SearchHistoryItem[] {
  const merged = new Map<string, SearchHistoryItem>();
  for (const item of [...preferredItems, ...fallbackItems]) {
    if (!item?.id || merged.has(item.id)) {
      continue;
    }
    merged.set(item.id, item);
  }
  return Array.from(merged.values())
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
    .slice(0, HISTORY_LIMIT);
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
  return writeSearchHistory([item, ...readSearchHistory().filter((entry) => entry.id !== item.id)]);
}

export function deleteSearchHistoryItem(historyId: string): SearchHistoryItem[] {
  return writeSearchHistory(readSearchHistory().filter((item) => item.id !== historyId));
}

export async function syncSearchHistoryFromBackend(limit = HISTORY_LIMIT): Promise<SearchHistoryItem[]> {
  const remoteItems = (await listFrontendHistory(limit)).map(historyItemFromRecoveryEnvelope);
  return writeSearchHistory(mergeSearchHistoryItems(remoteItems, readSearchHistory()));
}

export async function deleteSearchHistoryItemShared(historyId: string): Promise<SearchHistoryItem[]> {
  try {
    await deleteFrontendHistory(historyId);
  } catch {
    return deleteSearchHistoryItem(historyId);
  }
  return deleteSearchHistoryItem(historyId);
}
