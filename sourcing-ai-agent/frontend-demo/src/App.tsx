import { Navigate, Route, Routes } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { CandidatePage } from "./pages/CandidatePage";
import { ManualReviewPage } from "./pages/ManualReviewPage";
import { ResultsPage } from "./pages/ResultsPage";
import { SearchPage } from "./pages/SearchPage";
import { TargetCandidatesPage } from "./pages/TargetCandidatesPage";

export function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<SearchPage />} />
        <Route path="/results" element={<ResultsPage />} />
        <Route path="/manual-review" element={<ManualReviewPage />} />
        <Route path="/targets" element={<TargetCandidatesPage />} />
        <Route path="/candidate/:candidateId" element={<CandidatePage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}
