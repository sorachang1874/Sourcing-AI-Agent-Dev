import { useEffect, useState, type ReactNode } from "react";
import { SearchHistorySidebar } from "./SearchHistorySidebar";

interface AppShellProps {
  children: ReactNode;
}

const sidebarPreferenceKey = "redmatch_sidebar_collapsed";

export function AppShell({ children }: AppShellProps) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  useEffect(() => {
    try {
      const saved = window.localStorage.getItem(sidebarPreferenceKey);
      setSidebarCollapsed(saved === "true");
    } catch {
      setSidebarCollapsed(false);
    }
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(sidebarPreferenceKey, String(sidebarCollapsed));
    } catch {
      return;
    }
  }, [sidebarCollapsed]);

  return (
    <div className={`app-shell${sidebarCollapsed ? " sidebar-collapsed" : ""}`}>
      <SearchHistorySidebar collapsed={sidebarCollapsed} onToggleCollapse={() => setSidebarCollapsed((current) => !current)} />
      <main className={`main-content${sidebarCollapsed ? " main-content-wide" : ""}`}>
        <div className="page-frame">{children}</div>
      </main>
    </div>
  );
}
