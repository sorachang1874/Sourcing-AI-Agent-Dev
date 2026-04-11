import { useEffect, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import {
  deleteSearchHistoryItem,
  readSearchHistory,
  searchHistoryUpdatedEventName,
  startNewSearchEventName,
} from "../lib/searchHistory";
import type { SearchHistoryItem } from "../types";
import { BrandLogo } from "./BrandLogo";

interface SearchHistorySidebarProps {
  collapsed: boolean;
  onToggleCollapse: () => void;
}

function formatHistoryTime(value: string): string {
  try {
    return new Intl.DateTimeFormat("zh-CN", {
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).format(new Date(value));
  } catch {
    return value;
  }
}

function phaseLabel(item: SearchHistoryItem): string {
  if (item.errorMessage) {
    return "failed";
  }
  return item.phase;
}

export function SearchHistorySidebar({ collapsed, onToggleCollapse }: SearchHistorySidebarProps) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const activeHistoryId = searchParams.get("history") || "";
  const [items, setItems] = useState<SearchHistoryItem[]>(() => readSearchHistory());

  useEffect(() => {
    const syncItems = () => setItems(readSearchHistory());
    window.addEventListener(searchHistoryUpdatedEventName(), syncItems);
    window.addEventListener("storage", syncItems);
    return () => {
      window.removeEventListener(searchHistoryUpdatedEventName(), syncItems);
      window.removeEventListener("storage", syncItems);
    };
  }, []);

  const groupedItems = useMemo(() => items.slice(0, 12), [items]);

  return (
    <aside className={`sidebar${collapsed ? " collapsed" : ""}`}>
      <div className="sidebar-topbar">
        <div className="brand-block">
          <div className="brand-lockup">
            <BrandLogo />
            {!collapsed ? (
              <div className="brand-copy">
                <strong>RedMatch</strong>
                <span>Talent search, matched with intent</span>
              </div>
            ) : null}
          </div>
        </div>

        <button
          type="button"
          className="sidebar-toggle"
          aria-label={collapsed ? "展开侧边栏" : "收起侧边栏"}
          aria-pressed={collapsed}
          onClick={onToggleCollapse}
        >
          <span>{collapsed ? ">" : "<"}</span>
        </button>
      </div>

      <button
        type="button"
        className={`primary-button sidebar-action${collapsed ? " icon-only" : ""}`}
        aria-label="新建搜索"
        title="新建搜索"
        onClick={() => {
          navigate("/");
          window.dispatchEvent(new Event(startNewSearchEventName()));
        }}
      >
        <span>{collapsed ? "+" : "新建搜索"}</span>
      </button>

      {collapsed ? (
        <div className="sidebar-collapsed-state">
          <span className="collapsed-count">{groupedItems.length}</span>
          <small>History</small>
        </div>
      ) : null}

      {!collapsed ? (
        <div className="sidebar-section">
          <div className="sidebar-section-header">
            <h2>历史搜索记录</h2>
            <span>{groupedItems.length}</span>
          </div>

          {groupedItems.length === 0 ? (
            <div className="history-empty">
              <p>还没有历史记录。</p>
              <span>提交一次搜索后，这里会自动保存最近记录。</span>
            </div>
          ) : (
            <div className="history-list">
              {groupedItems.map((item) => (
                <article key={item.id} className={`history-item${item.id === activeHistoryId ? " active" : ""}`}>
                  <button type="button" className="history-select" onClick={() => navigate(`/?history=${item.id}`)}>
                    <div className="history-item-head">
                      <strong>{formatHistoryTime(item.createdAt)}</strong>
                      <span className={`phase-pill phase-${phaseLabel(item)}`}>{phaseLabel(item)}</span>
                    </div>
                    <p>{item.summary}</p>
                  </button>
                  <div className="history-item-foot">
                    <span>{item.errorMessage || item.plan?.targetCompany || "未生成方案"}</span>
                    <button
                      type="button"
                      className="history-delete"
                      onClick={() => {
                        deleteSearchHistoryItem(item.id);
                        if (item.id === activeHistoryId) {
                          navigate("/");
                        } else {
                          setItems(readSearchHistory());
                        }
                      }}
                    >
                      删除
                    </button>
                  </div>
                </article>
              ))}
            </div>
          )}
        </div>
      ) : null}
    </aside>
  );
}
