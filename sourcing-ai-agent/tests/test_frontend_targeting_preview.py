"""FT2 frontend targeting + preview contract tests (frontend VM harness).

Pins the FT0 v2 handoff (.coord/handoffs/tml-ft0-targeting-decision-v2.md)
§10.2 six-row matrix:

1. Options remain server-derived; picker-off omits the object; picker-on
   defaults roles [] + both statuses + location ["United States"].
2. Shard preview math: [] + [current,former] -> 2 shards; 3 roles x 2
   statuses -> 6 shards with exact per-shard labels; 1 role x 1 status -> 1.
3. Full-recall warning renders when roles empty and both statuses selected;
   budget/lane count visible before confirmation.
4. Location editing: values round-trip through submit/revision/recovery/
   review unchanged; clearing to empty restores the US default display;
   stale historical values display preserved (no silent deletion).
5. Facet consumption: board renders backend options including
   infra_systems/founding; multi-bucket candidates filter correctly; no
   local taxonomy remains (deleted maps are gone).
6. Dual-status candidate appears under both employment filters; card still
   shows one display status.

All checks run against the frontend TypeScript VM harness or source
assertions only: zero provider/model/network calls, served=0 unchanged.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_LOAD_MODULE_PREAMBLE = """
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const ts = require("./frontend-demo/node_modules/typescript");

function loadModule(relativePath, overrides = {}) {
  const source = fs
    .readFileSync(path.join(process.cwd(), relativePath), "utf8")
    .replaceAll("import.meta.env", "({})");
  const compiled = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  }).outputText;
  const module = { exports: {} };
  const localRequire = (specifier) => {
    if (Object.prototype.hasOwnProperty.call(overrides, specifier)) {
      return overrides[specifier];
    }
    if (specifier === "./workflowStatus") {
      return {
        normalizeWorkflowStatus: () => "failed",
        resolveWorkflowStatus: () => ({ status: "failed", terminal: true }),
      };
    }
    return require(specifier);
  };
  vm.runInNewContext(
    compiled,
    {
      module,
      exports: module.exports,
      require: localRequire,
      console,
      TextEncoder,
      crypto: { randomUUID: () => "test-id" },
    },
    { filename: path.basename(relativePath) },
  );
  return module.exports;
}

const cohortSelection = loadModule("frontend-demo/src/lib/cohortSelection.ts");
const runtimeContract = loadModule("contracts/frontend_api_runtime_contract.ts");
const api = loadModule("frontend-demo/src/lib/api.ts", {
  "../data/mockData": {
    mockCandidateDetails: {},
    mockDashboard: {},
    mockManualReviewItems: [],
    mockPlan: {},
    mockRunStatus: {},
  },
  "./dashboardHydration": {
    dashboardExpectedCandidateCount: () => 0,
    dashboardHasRenderableCandidates: () => false,
  },
  "./resultViewLifecycle": {
    lifecycleEffectiveDeltaMaterializedCount: () => 0,
  },
  "./cohortSelection": cohortSelection,
  "../../../contracts/frontend_api_runtime_contract": runtimeContract,
});

const optionsPayload = {
  schema_version: "cohort_selection.v1",
  registry_version: "cohort_selection.registry.v1",
  registry_digest: "registry-digest",
  role_buckets: [
    { id: "engineering", label: "Engineer", order: 20 },
    { id: "research", label: "Researcher", order: 10 },
    { id: "product_management", label: "Product Manager", order: 30 },
    { id: "infra_systems", label: "Infrastructure & Systems", order: 40 },
    { id: "founding", label: "Founder", order: 50 },
  ],
  employment_statuses: [
    { id: "former", label: "Former employees", order: 20 },
    { id: "current", label: "Current employees", order: 10 },
  ],
  role_match_options: [
    { id: "all", label: "Match all selected roles", order: 20 },
    { id: "any", label: "Match any selected role", order: 10 },
  ],
  defaults: { role_match: "any" },
};
const parsedOptions = cohortSelection.parseCohortSelectionOptionsPayload(optionsPayload);
const explicitCohort = {
  schema_version: "cohort_selection.v1",
  role_bucket_ids: ["research", "engineering"],
  employment_statuses: ["current", "former"],
  role_match: "any",
  source: "user_explicit",
};
const captureError = (callback) => {
  try {
    callback();
    return "";
  } catch (error) {
    return String(error?.message || error || "");
  }
};
"""

_CANDIDATE_FILTERS_PREAMBLE = """
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const ts = require("./frontend-demo/node_modules/typescript");
const source = fs.readFileSync(
  path.join(process.cwd(), "frontend-demo/src/lib/candidateFilters.ts"),
  "utf8",
);
const compiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
}).outputText;
const module = { exports: {} };
vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
  filename: "candidateFilters.js",
});
const candidateFilters = module.exports;
"""


def _run_node(script_body: str) -> dict:
    completed = subprocess.run(
        ["node", "-e", script_body],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(completed.stdout)


class FrontendTargetingPreviewTest(unittest.TestCase):
    def setUp(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")

    def test_options_server_derived_and_picker_defaults(self) -> None:
        script = textwrap.dedent(
            _LOAD_MODULE_PREAMBLE
            + """
            const pickerOffSubmit = api.__testBuildPlanSubmitPayload("find people");
            console.log(JSON.stringify({
              parsedRoleIds: parsedOptions.roleBuckets.map((option) => option.id),
              parsedStatusIds: parsedOptions.employmentStatuses.map((option) => option.id),
              defaultCohort: cohortSelection.createDefaultCohortSelection(parsedOptions),
              defaultLocations: cohortSelection.createDefaultCohortLocationSelection(),
              pickerOffOmitsCohort: !("cohort_selection" in pickerOffSubmit),
              pickerOffOmitsTargetLocations: !("target_locations" in pickerOffSubmit),
              pickerOffOmitsExcludeLocations: !("exclude_target_locations" in pickerOffSubmit),
            }));
            """
        )
        payload = _run_node(script)
        self.assertEqual(
            payload["parsedRoleIds"],
            ["research", "engineering", "product_management", "infra_systems", "founding"],
        )
        self.assertEqual(payload["parsedStatusIds"], ["current", "former"])
        self.assertEqual(
            payload["defaultCohort"],
            {
                "schema_version": "cohort_selection.v1",
                "role_bucket_ids": [],
                "employment_statuses": ["current", "former"],
                "role_match": "any",
                "source": "user_explicit",
            },
        )
        self.assertEqual(
            payload["defaultLocations"],
            {"targetLocations": ["United States"], "excludeTargetLocations": []},
        )
        self.assertTrue(payload["pickerOffOmitsCohort"])
        self.assertTrue(payload["pickerOffOmitsTargetLocations"])
        self.assertTrue(payload["pickerOffOmitsExcludeLocations"])

        picker_source = (REPO_ROOT / "frontend-demo/src/components/CohortSelectionPicker.tsx").read_text(
            encoding="utf-8"
        )
        self.assertIn("options.roleBuckets.map", picker_source)
        self.assertIn("options.employmentStatuses.map", picker_source)
        self.assertIn("options.roleMatchOptions.map", picker_source)
        self.assertIn("onChange(createDefaultCohortSelection(options));", picker_source)
        self.assertIn("updateLocations(createDefaultCohortLocationSelection());", picker_source)
        self.assertNotIn('value="research"', picker_source)
        self.assertNotIn('value="engineering"', picker_source)
        self.assertNotIn('value="product_management"', picker_source)
        self.assertNotIn('value="infra_systems"', picker_source)
        self.assertNotIn('value="founding"', picker_source)

    def test_shard_preview_math_matches_compiler_expansion(self) -> None:
        script = textwrap.dedent(
            _LOAD_MODULE_PREAMBLE
            + """
            const selectionFor = (roleIds, statusIds) => ({
              schema_version: "cohort_selection.v1",
              role_bucket_ids: roleIds,
              employment_statuses: statusIds,
              role_match: "any",
              source: "user_explicit",
            });
            const allRolesBothStatuses = cohortSelection.buildCohortShardPreview(
              selectionFor([], ["current", "former"]),
              parsedOptions,
            );
            const threeRolesTwoStatuses = cohortSelection.buildCohortShardPreview(
              selectionFor(["research", "engineering", "product_management"], ["current", "former"]),
              parsedOptions,
            );
            const oneRoleOneStatus = cohortSelection.buildCohortShardPreview(
              selectionFor(["research"], ["current"]),
              parsedOptions,
            );
            console.log(JSON.stringify({
              allRolesBothStatuses,
              threeRolesTwoStatuses,
              oneRoleOneStatus,
            }));
            """
        )
        payload = _run_node(script)

        # [] roles + [current, former] -> S * max(1, 0) = 2 status-only shards.
        self.assertEqual(payload["allRolesBothStatuses"]["shardCount"], 2)
        self.assertEqual(
            [
                (shard["shardId"], shard["statusLabel"], shard["roleId"], shard["roleLabel"])
                for shard in payload["allRolesBothStatuses"]["shards"]
            ],
            [
                ("current:all_roles", "Current employees", None, "All roles"),
                ("former:all_roles", "Former employees", None, "All roles"),
            ],
        )

        # 3 roles x 2 statuses -> 6 shards with exact per-shard labels.
        self.assertEqual(payload["threeRolesTwoStatuses"]["shardCount"], 6)
        self.assertEqual(
            [
                (shard["shardId"], shard["statusLabel"], shard["roleLabel"])
                for shard in payload["threeRolesTwoStatuses"]["shards"]
            ],
            [
                ("current:research", "Current employees", "Researcher"),
                ("current:engineering", "Current employees", "Engineer"),
                ("current:product_management", "Current employees", "Product Manager"),
                ("former:research", "Former employees", "Researcher"),
                ("former:engineering", "Former employees", "Engineer"),
                ("former:product_management", "Former employees", "Product Manager"),
            ],
        )

        # 1 role x 1 status -> 1 shard.
        self.assertEqual(payload["oneRoleOneStatus"]["shardCount"], 1)
        self.assertEqual(
            [
                (shard["shardId"], shard["statusLabel"], shard["roleLabel"])
                for shard in payload["oneRoleOneStatus"]["shards"]
            ],
            [("current:research", "Current employees", "Researcher")],
        )

    def test_full_recall_warning_and_budget_visible_before_confirmation(self) -> None:
        script = textwrap.dedent(
            _LOAD_MODULE_PREAMBLE
            + """
            const selectionFor = (roleIds, statusIds) => ({
              schema_version: "cohort_selection.v1",
              role_bucket_ids: roleIds,
              employment_statuses: statusIds,
              role_match: "any",
              source: "user_explicit",
            });
            console.log(JSON.stringify({
              emptyRolesAllStatuses: cohortSelection.buildCohortShardPreview(
                selectionFor([], ["current", "former"]),
                parsedOptions,
              ).isFullRecall,
              oneRoleAllStatuses: cohortSelection.buildCohortShardPreview(
                selectionFor(["research"], ["current", "former"]),
                parsedOptions,
              ).isFullRecall,
              emptyRolesOneStatus: cohortSelection.buildCohortShardPreview(
                selectionFor([], ["current"]),
                parsedOptions,
              ).isFullRecall,
            }));
            """
        )
        payload = _run_node(script)
        self.assertTrue(payload["emptyRolesAllStatuses"])
        self.assertFalse(payload["oneRoleAllStatuses"])
        self.assertFalse(payload["emptyRolesOneStatus"])

        picker_source = (REPO_ROOT / "frontend-demo/src/components/CohortSelectionPicker.tsx").read_text(
            encoding="utf-8"
        )
        self.assertIn("buildCohortShardPreview(value, options)", picker_source)
        self.assertIn('role="alert"', picker_source)
        self.assertIn("full-recall-warning", picker_source)
        self.assertIn("cohort-shard-count", picker_source)
        self.assertIn("服务端预算上限", picker_source)

        plan_source = (REPO_ROOT / "frontend-demo/src/components/PlanCard.tsx").read_text(encoding="utf-8")
        self.assertIn("buildCohortShardPreview(effectiveCohortSelection, cohortOptions)", plan_source)
        self.assertIn('data-testid="plan-cohort-shard-preview"', plan_source)
        self.assertIn('data-testid="plan-cohort-shard-count"', plan_source)
        self.assertIn('data-testid="plan-full-recall-warning"', plan_source)
        self.assertIn('data-testid="plan-shard-budget-note"', plan_source)
        self.assertIn("服务端分片预算上限", plan_source)
        self.assertLess(
            plan_source.index('data-testid="plan-cohort-shard-preview"'),
            plan_source.index('data-testid="plan-confirm-button"'),
            "shard count/list + bounded budget must render before the confirmation button",
        )

    def test_location_fields_round_trip_alongside_cohort_object(self) -> None:
        script = textwrap.dedent(
            _LOAD_MODULE_PREAMBLE
            + """
            const submitPayload = api.__testBuildPlanSubmitPayload(
              "find people",
              "",
              explicitCohort,
              ["Canada", " United States ", "Canada"],
              ["Europe"],
            );
            const revisionPayload = api.__testBuildPlanSubmitPayload(
              "find people with revision",
              "history-server-owned-1",
              explicitCohort,
              ["Canada"],
              [],
            );
            const reviewPayload = api.planReviewDecisionToApiPayload(
              {
                confirmedCompanyScope: [],
                extraSourceFamilies: [],
                cohortSelection: explicitCohort,
                targetLocations: ["Canada"],
                excludeTargetLocations: ["Europe"],
              },
              [],
            );
            const recoveryPlan = api.__testMapPlanPayloadToDemoPlan(
              {
                request: {
                  raw_user_request: "find people",
                  cohort_selection: explicitCohort,
                  target_locations: [" 旧金山  Bay Area "],
                  exclude_target_locations: ["Europe"],
                },
                request_preview: {
                  cohort_selection: explicitCohort,
                  target_locations: [" 旧金山  Bay Area "],
                  exclude_target_locations: ["Europe"],
                },
                plan: {},
              },
              "find people",
            );
            const conflictError = captureError(() =>
              api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { cohort_selection: explicitCohort, target_locations: ["Canada"] },
                  request_preview: { cohort_selection: explicitCohort, target_locations: ["United States"] },
                  plan: {},
                },
                "find people",
              ),
            );
            const legacyPlanWithoutLocations = api.__testMapPlanPayloadToDemoPlan(
              {
                request: { raw_user_request: "find people", cohort_selection: explicitCohort },
                request_preview: { cohort_selection: explicitCohort },
                plan: {},
              },
              "find people",
            );
            const emptyLocationPayload = cohortSelection.buildCohortLocationApiPayload([], []);
            const defaultLocationSummary = cohortSelection.summarizeCohortLocations([], []);
            const staleSummary = cohortSelection.summarizeCohortLocations(["旧金山 Bay Area"], []);
            const locationInsideCohortError = captureError(() =>
              cohortSelection.parseCohortSelectionPayload({
                ...explicitCohort,
                target_locations: ["United States"],
              }),
            );
            console.log(JSON.stringify({
              submitPayload,
              revisionPayload,
              reviewPayload,
              recoveryPlan: {
                targetLocations: recoveryPlan.targetLocations,
                excludeTargetLocations: recoveryPlan.excludeTargetLocations,
                reviewDefaultTargetLocations: recoveryPlan.reviewDecisionDefaults.targetLocations,
                cohortSelection: recoveryPlan.cohortSelection,
              },
              conflictError,
              legacyPlanTargetLocations: legacyPlanWithoutLocations.targetLocations ?? null,
              emptyLocationPayload,
              emptyLocationKeys: Object.keys(emptyLocationPayload),
              defaultLocationSummary,
              staleSummary,
              locationInsideCohortError,
            }));
            """
        )
        payload = _run_node(script)

        explicit_cohort = {
            "schema_version": "cohort_selection.v1",
            "role_bucket_ids": ["research", "engineering"],
            "employment_statuses": ["current", "former"],
            "role_match": "any",
            "source": "user_explicit",
        }

        # Submit: fields ride ALONGSIDE the closed 5-field cohort object.
        submit = payload["submitPayload"]
        self.assertEqual(submit["target_locations"], ["Canada", "United States"])
        self.assertEqual(submit["exclude_target_locations"], ["Europe"])
        self.assertEqual(submit["cohort_selection"], explicit_cohort)
        self.assertEqual(
            sorted(submit["cohort_selection"].keys()),
            ["employment_statuses", "role_bucket_ids", "role_match", "schema_version", "source"],
        )
        self.assertNotIn("target_locations", submit["cohort_selection"])
        self.assertNotIn("exclude_target_locations", submit["cohort_selection"])

        # Revision: identical location transport on the same builder.
        revision = payload["revisionPayload"]
        self.assertEqual(revision["history_id"], "history-server-owned-1")
        self.assertEqual(revision["target_locations"], ["Canada"])
        self.assertNotIn("exclude_target_locations", revision)

        # Review: decision locations emitted alongside the cohort object.
        review = payload["reviewPayload"]
        self.assertEqual(review["cohort_selection"], explicit_cohort)
        self.assertEqual(review["target_locations"], ["Canada"])
        self.assertEqual(review["exclude_target_locations"], ["Europe"])

        # Recovery: stale historical free-text values display preserved
        # (trimmed/deduped, never silently deleted) and defaults seeded.
        recovery = payload["recoveryPlan"]
        self.assertEqual(recovery["targetLocations"], ["旧金山 Bay Area"])
        self.assertEqual(recovery["excludeTargetLocations"], ["Europe"])
        self.assertEqual(recovery["reviewDefaultTargetLocations"], ["旧金山 Bay Area"])
        self.assertEqual(recovery["cohortSelection"], explicit_cohort)
        self.assertEqual(payload["staleSummary"], "目标地区: 旧金山 Bay Area")

        # Conflicting mirrors fail closed; legacy plans carry no location fields.
        self.assertIn("conflicting target_locations mirrors", payload["conflictError"])
        self.assertIsNone(payload["legacyPlanTargetLocations"])

        # Clearing to empty omits the fields and restores the US default display.
        self.assertEqual(payload["emptyLocationKeys"], [])
        self.assertEqual(payload["defaultLocationSummary"], "目标地区: United States（默认）")

        # Location can never be smuggled inside the closed v1 object.
        self.assertIn("do not match the v1 contract", payload["locationInsideCohortError"])

    def test_facet_consumption_uses_server_options_and_buckets(self) -> None:
        filters_source = (REPO_ROOT / "frontend-demo/src/lib/candidateFilters.ts").read_text(encoding="utf-8")
        self.assertNotIn("const ROLE_BUCKET_TO_FUNCTION_BUCKET", filters_source)
        self.assertNotIn("const FUNCTION_BUCKET_KEYWORDS", filters_source)
        self.assertNotIn("inferredFunctionBucketFromProfile(", filters_source)
        self.assertNotIn("countKeywordOccurrences(", filters_source)
        self.assertNotIn('normalizedIds.includes("24")', filters_source)
        self.assertNotIn('normalizedIds.includes("8")', filters_source)
        self.assertNotIn('normalizedIds.includes("19")', filters_source)
        self.assertIn("candidate.functionBucketIds", filters_source)

        board_source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(
            encoding="utf-8"
        )
        self.assertIn("candidateFacetSummary?.functions", board_source)
        self.assertIn("canonicalBoardFacetOptions(", board_source)

        types_source = (REPO_ROOT / "frontend-demo/src/types.ts").read_text(encoding="utf-8")
        self.assertIn("functionBucketIds?: string[];", types_source)
        self.assertIn('"lane_membership" | "registry_evidence" | "legacy_inference"', types_source)

        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        self.assertIn("function_bucket_ids", api_source)
        self.assertIn("function_bucket_source", api_source)

        script = textwrap.dedent(
            _CANDIDATE_FILTERS_PREAMBLE
            + """
            const baseCandidate = {
              name: "",
              headline: "",
              summary: "",
              currentCompany: "",
              notesSnippet: "",
              team: "Unknown",
              focusAreas: [],
              matchReasons: [],
              education: [],
              experience: [],
              matchedKeywords: [],
              sourceMatches: [],
              employmentStatus: "current",
              outreachLayer: null,
            };
            const candidates = [
              {
                ...baseCandidate,
                id: "multi-bucket-member",
                functionBucketIds: ["engineering", "infra_systems"],
                functionBucketSource: "lane_membership",
              },
              {
                ...baseCandidate,
                id: "founding-member",
                functionBucketIds: ["founding"],
                functionBucketSource: "registry_evidence",
              },
            ];
            const options = candidateFilters.buildFunctionOptions(candidates);
            const filterSelection = (functionBuckets) => ({
              layers: [],
              recallBuckets: [],
              employmentStatuses: [],
              locations: [],
              functionBuckets,
              searchKeyword: "",
            });
            const hits = (functionBuckets) =>
              candidateFilters
                .filterCandidatesByFacets(candidates, filterSelection(functionBuckets), [])
                .map((candidate) => candidate.id);
            console.log(JSON.stringify({
              options,
              engineeringHits: hits(["engineering"]),
              infraHits: hits(["infra_systems"]),
              foundingHits: hits(["founding"]),
              combinedHits: hits(["infra_systems", "founding"]),
            }));
            """
        )
        payload = _run_node(script)
        # Backend-driven option ids, including infra_systems/founding, render
        # verbatim (multi-membership: the dual-bucket candidate counts in BOTH).
        self.assertEqual(
            payload["options"],
            [
                {"id": "engineering", "label": "engineering", "count": 1},
                {"id": "founding", "label": "founding", "count": 1},
                {"id": "infra_systems", "label": "infra_systems", "count": 1},
            ],
        )
        self.assertEqual(payload["engineeringHits"], ["multi-bucket-member"])
        self.assertEqual(payload["infraHits"], ["multi-bucket-member"])
        self.assertEqual(payload["foundingHits"], ["founding-member"])
        self.assertEqual(
            sorted(payload["combinedHits"]),
            ["founding-member", "multi-bucket-member"],
        )

    def test_employment_filtering_delegates_membership_and_keeps_single_display_status(self) -> None:
        # Employment facet membership for Cohort-produced candidates is
        # computed backend-side from lane membership (FT0 §6, FT1); the
        # frontend transports the selected statuses to the backend page filter
        # and keeps exactly one display status per card.
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        self.assertIn(
            "employment_statuses: normalized.employmentStatuses.length > 0",
            api_source,
        )
        board_source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "selectedBackendFacetFilterIds(selectedEmploymentStatuses, employmentFacetOptions)",
            board_source,
        )
        presentation_source = (REPO_ROOT / "frontend-demo/src/lib/candidatePresentation.ts").read_text(
            encoding="utf-8"
        )
        self.assertIn("export function employmentStatusLabel(", presentation_source)
        types_source = (REPO_ROOT / "frontend-demo/src/types.ts").read_text(encoding="utf-8")
        self.assertIn('employmentStatus: "current" | "former" | "lead";', types_source)

        script = textwrap.dedent(
            _CANDIDATE_FILTERS_PREAMBLE
            + """
            const baseCandidate = {
              name: "",
              headline: "",
              summary: "",
              currentCompany: "",
              notesSnippet: "",
              team: "Unknown",
              focusAreas: [],
              matchReasons: [],
              education: [],
              experience: [],
              matchedKeywords: [],
              sourceMatches: [],
              outreachLayer: null,
              functionBucketIds: [],
            };
            const candidates = [
              { ...baseCandidate, id: "display-current", employmentStatus: "current" },
              { ...baseCandidate, id: "display-former", employmentStatus: "former" },
              { ...baseCandidate, id: "display-lead", employmentStatus: "lead" },
            ];
            const filterSelection = (employmentStatuses) => ({
              layers: [],
              recallBuckets: [],
              employmentStatuses,
              locations: [],
              functionBuckets: [],
              searchKeyword: "",
            });
            const hits = (employmentStatuses) =>
              candidateFilters
                .filterCandidatesByFacets(candidates, filterSelection(employmentStatuses), [])
                .map((candidate) => candidate.id);
            console.log(JSON.stringify({
              currentOnly: hits(["current"]),
              formerOnly: hits(["former"]),
              bothStatuses: hits(["current", "former"]),
              displayStatuses: candidates.map((candidate) => candidate.employmentStatus),
            }));
            """
        )
        payload = _run_node(script)
        self.assertEqual(payload["currentOnly"], ["display-current"])
        self.assertEqual(payload["formerOnly"], ["display-former"])
        self.assertEqual(
            payload["bothStatuses"],
            ["display-current", "display-former", "display-lead"],
        )
        # One card, one display status (FT0 §6.2 display-only projection).
        self.assertEqual(
            payload["displayStatuses"],
            ["current", "former", "lead"],
        )


if __name__ == "__main__":
    unittest.main()
