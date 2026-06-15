from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class FrontendRunStatusContractTest(unittest.TestCase):
    def test_run_status_candidate_total_prefers_lifecycle_expected_count(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        expected_snippets = [
            "const lifecycleExpectedCount = resultViewLifecycle?.expectedCandidateCount || 0;",
            "const stage1ExpectedCount = baselineCount > 0 && stage1RequiredCount > 0 ? baselineCount + stage1RequiredCount : 0;",
            "lifecycleExpectedCount,",
            "stage1ExpectedCount,",
        ]
        for snippet in expected_snippets:
            with self.subTest(snippet=snippet):
                self.assertIn(snippet, source)
        self.assertIn("const candidateCountMetric = boardRuntimeState\n    ? Math.max(0, boardExpectedCount)\n    : Math.max(", source)
        self.assertNotIn("Math.max(boardExpectedCount || 0, boardPublishedCount || 0)", source)

    def test_run_status_mapping_keeps_stage1_metric_card_when_present(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/api.ts"),
              "utf8",
            );
            const labels = [
              "新发现在职候选人",
              "新发现离职候选人",
              "经去重得到",
              "需补取 LinkedIn Profile",
              "已取回 LinkedIn Profile",
            ];
            console.log(JSON.stringify({
              labelsPresent: labels.every((label) => source.includes(label)),
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["labelsPresent"])

    def test_run_status_manual_review_metric_uses_canonical_job_count_not_stage1_preview(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        self.assertIn("asNumber(payload.manual_review_count)", source)
        self.assertIn("asNumber(payload.progress?.counters?.manual_review_count)", source)
        self.assertNotIn("stage1PreviewSummary.manual_review_queue_count", source)
        self.assertNotIn("stage2FinalSummary.manual_review_queue_count", source)

    def test_dashboard_manual_review_metric_is_hidden_during_board_or_profile_work(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        self.assertIn("const rawManualReviewCount =", source)
        self.assertIn("const linkedinStage1ProviderWorkVisible = Boolean(", source)
        self.assertIn("rawManualReviewCount > 0 && !boardRuntimeState && !linkedinStage1ProviderWorkVisible", source)
        self.assertNotIn("manualReviewCount =\n    typeof payload.manual_review_count", source)

    def test_avatar_component_does_not_show_expired_linkedin_media_as_blank(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/Avatar.tsx").read_text(encoding="utf-8")
        self.assertIn("function isExpiredLinkedInMediaUrl", source)
        self.assertIn('url.hostname.endsWith("media.licdn.com")', source)
        self.assertIn('url.searchParams.get("e")', source)
        self.assertIn("providerImageExpired", source)
        self.assertIn("avatar-generated", source)
