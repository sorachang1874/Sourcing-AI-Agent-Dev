"""FT2 frontend targeting + preview contract tests (frontend VM harness).

Pins the FT0 v2 handoff (.coord/handoffs/tml-ft0-targeting-decision-v2.md)
§10.2 six-row matrix, reworked per the FT2 fixed-forward review so every row
exercises the PRODUCTION paths (real payload builders, real rendered
components, real projection functions) instead of reimplemented mirror logic:

1. Options remain server-derived; picker-off omits the object; picker-on
   defaults roles [] + both statuses with the server-default location
   DISPLAY seed (absent on the wire, so the server stays the default owner).
2. Shard preview math: [] + [current,former] -> 2 shards; 3 roles x 2
   statuses -> 6 shards with exact per-shard labels; 1 role x 1 status -> 1;
   stale/removed selected ids are surfaced as unavailable, never dropped.
3. Full-recall warning renders when roles empty and both statuses selected;
   shard count/list + bounded-budget note render before confirmation, and
   confirmation is DISABLED without a validated preview (options loading /
   failure / stale registry), with a retryable blocking state.
4. Location editing: values flow through the real initial submit, revision,
   review, and recovery paths; absent -> server default (omitted), explicit
   [] -> opt-out (serialized), present null / invalid bounds fail closed;
   stale historical values display preserved (no silent deletion).
5. Facet consumption: the atomic server pair
   {function_bucket_ids, function_bucket_source} is consumed verbatim from
   one authoritative layer; partial/conflicting/malformed pairs fail closed;
   missing pairs disable the facet instead of synthesizing membership.
6. Dual-status candidate (server membership truth) appears under BOTH
   employment filters; card still shows one display status.

All checks run against the frontend TypeScript VM harness (real modules,
real React server renders): zero provider/model/network calls, served=0
unchanged.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_LOAD_MODULE_PREAMBLE = r"""
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const ts = require("./frontend-demo/node_modules/typescript");
const React = require(path.join(process.cwd(), "frontend-demo/node_modules/react"));
const ReactDOMServer = require(path.join(process.cwd(), "frontend-demo/node_modules/react-dom/server"));
const ReactJsxRuntime = require(path.join(process.cwd(), "frontend-demo/node_modules/react/jsx-runtime"));

function loadModule(relativePath, overrides = {}) {
  const source = fs
    .readFileSync(path.join(process.cwd(), relativePath), "utf8")
    .replaceAll("import.meta.env", "({})");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2020,
      jsx: ts.JsxEmit.ReactJSX,
    },
  }).outputText;
  const module = { exports: {} };
  const localRequire = (specifier) => {
    if (specifier === "react") {
      return React;
    }
    if (specifier === "react/jsx-runtime") {
      return ReactJsxRuntime;
    }
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
const picker = loadModule("frontend-demo/src/components/CohortSelectionPicker.tsx", {
  "../lib/cohortSelection": cohortSelection,
});
const planCard = loadModule("frontend-demo/src/components/PlanCard.tsx", {
  "../lib/cohortSelection": cohortSelection,
  "./CohortSelectionPicker": picker,
});
const historyRecovery = loadModule("frontend-demo/src/lib/historyRecovery.ts", {
  "./cohortSelection": cohortSelection,
  "./historySummary": { summarizeSearchQuery: () => "query summary" },
  "./workflow": {
    buildReusedCompletedTimelineSteps: () => [],
    isReusedCompletedHistory: () => false,
  },
});
const candidateFilters = loadModule("frontend-demo/src/lib/candidateFilters.ts");

const el = React.createElement;
const render = (element) => ReactDOMServer.renderToStaticMarkup(element);

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
const selectionFor = (roleIds, statusIds, roleMatch = "any") => ({
  schema_version: "cohort_selection.v1",
  role_bucket_ids: roleIds,
  employment_statuses: statusIds,
  role_match: roleMatch,
  source: "user_explicit",
});
const captureError = (callback) => {
  try {
    callback();
    return "";
  } catch (error) {
    return String(error?.message || error || "");
  }
};

const makePlan = (overrides = {}) => ({
  planId: "plan-1",
  rawUserRequest: "find people",
  targetCompany: "ACME",
  targetPopulation: "researchers",
  projectScope: "",
  keywords: [],
  acquisitionStrategy: "strategy",
  searchStrategy: [],
  estimatedCostLevel: "low",
  reviewRequired: true,
  status: "pending_review",
  reviewGate: {
    status: "pending",
    requiredBeforeExecution: true,
    riskLevel: "low",
    reasons: [],
    confirmationItems: [],
    editableFields: [],
    suggestedActions: [],
    scopeHints: [],
    executionModeHints: [],
  },
  ...overrides,
});
const makeDecision = (overrides = {}) => ({
  confirmedCompanyScope: [],
  extraSourceFamilies: [],
  ...overrides,
});
const renderPlanCard = (props = {}) =>
  render(
    el(planCard.PlanCard, {
      plan: makePlan(),
      revisionText: "",
      reviewDecision: makeDecision(),
      cohortOptions: parsedOptions,
      isLoadingCohortOptions: false,
      cohortOptionsError: "",
      reviewChecklistConfirmed: true,
      isApplyingRevision: false,
      isConfirming: false,
      onRevisionChange: () => {},
      onReviewDecisionChange: () => {},
      onRetryCohortOptions: () => {},
      onReviewChecklistChange: () => {},
      onApplyRevision: () => {},
      onConfirm: () => {},
      ...props,
    }),
  );
const buttonTag = (markup, testid) => {
  const match = markup.match(new RegExp(`<button[^>]*data-testid="${testid}"[^>]*>`));
  return match ? match[0] : "";
};
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
};
const filterSelection = (patch) => ({
  layers: [],
  recallBuckets: [],
  employmentStatuses: [],
  locations: [],
  functionBuckets: [],
  searchKeyword: "",
  ...patch,
});
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
            const defaultCohort = cohortSelection.createDefaultCohortSelection(parsedOptions);
            const defaultLocations = cohortSelection.createDefaultCohortLocationSelection();
            // Real render of the production picker (uncontrolled locations):
            // enabled with the default cohort -> the server-default location
            // display seed renders; the opt-out affordance stays unchecked.
            const enabledMarkup = render(el(picker.CohortSelectionPicker, {
              idPrefix: "search",
              value: defaultCohort,
              options: parsedOptions,
              onChange: () => {},
            }));
            const disabledMarkup = render(el(picker.CohortSelectionPicker, {
              idPrefix: "search",
              value: null,
              options: parsedOptions,
              onChange: () => {},
            }));
            console.log(JSON.stringify({
              parsedRoleIds: parsedOptions.roleBuckets.map((option) => option.id),
              parsedStatusIds: parsedOptions.employmentStatuses.map((option) => option.id),
              defaultCohort,
              defaultLocations,
              pickerOffOmitsCohort: !("cohort_selection" in pickerOffSubmit),
              pickerOffOmitsTargetLocations: !("target_locations" in pickerOffSubmit),
              pickerOffOmitsExcludeLocations: !("exclude_target_locations" in pickerOffSubmit),
              enabledShowsServerDefault:
                enabledMarkup.includes('data-testid="search-target-locations-default"')
                && enabledMarkup.includes("United States"),
              enabledOptOutUnchecked: (() => {
                const match = enabledMarkup.match(
                  /<input[^>]*data-testid="search-target-locations-optout"[^>]*>/,
                );
                return Boolean(match) && !match[0].includes("checked");
              })(),
              enabledRendersOptionLabels:
                enabledMarkup.includes("Researcher")
                && enabledMarkup.includes("Engineer")
                && enabledMarkup.includes("Infrastructure &amp; Systems")
                && enabledMarkup.includes("Founder")
                && enabledMarkup.includes("Current employees")
                && enabledMarkup.includes("Former employees"),
              disabledHidesLocationFields:
                !disabledMarkup.includes("search-target-locations-default")
                && !disabledMarkup.includes("search-target-locations-optout"),
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
        # Absent location state: the server default applies on the wire; the
        # picker renders it as a display seed (FT0 §7.2 tri-state).
        self.assertEqual(payload["defaultLocations"], {})
        self.assertTrue(payload["pickerOffOmitsCohort"])
        self.assertTrue(payload["pickerOffOmitsTargetLocations"])
        self.assertTrue(payload["pickerOffOmitsExcludeLocations"])
        self.assertTrue(payload["enabledShowsServerDefault"])
        self.assertTrue(payload["enabledOptOutUnchecked"])
        self.assertTrue(payload["enabledRendersOptionLabels"])
        self.assertTrue(payload["disabledHidesLocationFields"])

    def test_shard_preview_math_and_stale_selection_surfacing(self) -> None:
        script = textwrap.dedent(
            _LOAD_MODULE_PREAMBLE
            + """
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
            // Registry drift: a selected role no longer exists in the current
            // options. It must surface as unavailable, never silently drop.
            const staleRole = cohortSelection.buildCohortShardPreview(
              selectionFor(["research", "removed_role"], ["current", "former"]),
              parsedOptions,
            );
            // Every selected role is unavailable: no fabricated All-roles.
            const allRolesStale = cohortSelection.buildCohortShardPreview(
              selectionFor(["removed_role"], ["current"]),
              parsedOptions,
            );
            // Unknown status ids must not produce a false full-recall call.
            const staleStatus = cohortSelection.buildCohortShardPreview(
              selectionFor([], ["current", "former", "ghost_status"]),
              parsedOptions,
            );
            const staleRoleMatch = cohortSelection.buildCohortShardPreview(
              selectionFor(["research"], ["current"], "bogus_match"),
              parsedOptions,
            );
            console.log(JSON.stringify({
              allRolesBothStatuses,
              threeRolesTwoStatuses,
              oneRoleOneStatus,
              staleRole,
              allRolesStale,
              staleStatus,
              staleRoleMatch,
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
        self.assertTrue(payload["allRolesBothStatuses"]["isFullRecall"])
        self.assertFalse(payload["allRolesBothStatuses"]["hasUnavailableSelections"])
        self.assertEqual(payload["allRolesBothStatuses"]["registryDigest"], "registry-digest")

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

        # Stale role: surfaced as unavailable; the available part still
        # previews honestly (research x 2 statuses), never 2x2=4 and never
        # "All roles" for a nonempty selection.
        stale_role = payload["staleRole"]
        self.assertEqual(stale_role["unavailableRoleIds"], ["removed_role"])
        self.assertEqual(stale_role["unavailableStatusIds"], [])
        self.assertTrue(stale_role["hasUnavailableSelections"])
        self.assertEqual(
            [shard["shardId"] for shard in stale_role["shards"]],
            ["current:research", "former:research"],
        )
        self.assertFalse(stale_role["isFullRecall"])

        # All roles stale: zero shards, no fabricated all-roles expansion.
        all_roles_stale = payload["allRolesStale"]
        self.assertEqual(all_roles_stale["shardCount"], 0)
        self.assertEqual(all_roles_stale["shards"], [])
        self.assertEqual(all_roles_stale["unavailableRoleIds"], ["removed_role"])
        self.assertTrue(all_roles_stale["hasUnavailableSelections"])

        # Unknown status id: no false full-recall classification.
        stale_status = payload["staleStatus"]
        self.assertEqual(stale_status["unavailableStatusIds"], ["ghost_status"])
        self.assertTrue(stale_status["hasUnavailableSelections"])
        self.assertFalse(stale_status["isFullRecall"])

        # Unknown role_match: flagged unavailable as well.
        stale_role_match = payload["staleRoleMatch"]
        self.assertTrue(stale_role_match["roleMatchUnavailable"])
        self.assertTrue(stale_role_match["hasUnavailableSelections"])

    def test_full_recall_warning_budget_and_confirmation_gate(self) -> None:
        script = textwrap.dedent(
            _LOAD_MODULE_PREAMBLE
            + """
            const fullRecallCohort = selectionFor([], ["current", "former"]);
            const scopedCohort = selectionFor(["research"], ["current"]);
            const staleCohort = selectionFor(["removed_role"], ["current"]);

            // Validated preview: shard count/list + bounded-budget note render
            // BEFORE the confirmation button; the button is enabled.
            const validMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: fullRecallCohort }),
              reviewDecision: makeDecision({ cohortSelection: fullRecallCohort }),
            });
            const validPreviewIndex = validMarkup.indexOf('data-testid="plan-cohort-shard-preview"');
            const validBudgetIndex = validMarkup.indexOf('data-testid="plan-shard-budget-note"');
            const validConfirmIndex = validMarkup.indexOf('data-testid="plan-confirm-button"');

            // Options loading: no validated preview -> confirmation disabled.
            const loadingMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: scopedCohort }),
              reviewDecision: makeDecision({ cohortSelection: scopedCohort }),
              cohortOptions: null,
              isLoadingCohortOptions: true,
            });

            // Options failure: retryable blocking state -> confirmation disabled.
            const failureMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: scopedCohort }),
              reviewDecision: makeDecision({ cohortSelection: scopedCohort }),
              cohortOptions: null,
              cohortOptionsError: "options fetch failed",
            });

            // Registry drift (stale selected role): blocked as well.
            const staleMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: staleCohort }),
              reviewDecision: makeDecision({ cohortSelection: staleCohort }),
            });

            // Legacy plan without a cohort: no gate, confirmation enabled.
            const legacyMarkup = renderPlanCard({});

            // Scoped (non full-recall) cohort: preview without the warning.
            const scopedMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: scopedCohort }),
              reviewDecision: makeDecision({ cohortSelection: scopedCohort }),
            });

            console.log(JSON.stringify({
              valid: {
                hasPreview: validPreviewIndex >= 0,
                hasFullRecallWarning: validMarkup.includes('data-testid="plan-full-recall-warning"'),
                hasBudgetNote: validBudgetIndex >= 0,
                previewBeforeConfirm:
                  validPreviewIndex >= 0 && validConfirmIndex >= 0 && validPreviewIndex < validConfirmIndex,
                budgetBeforeConfirm:
                  validBudgetIndex >= 0 && validConfirmIndex >= 0 && validBudgetIndex < validConfirmIndex,
                confirmDisabled: buttonTag(validMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: validMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
              },
              loading: {
                confirmDisabled: buttonTag(loadingMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: loadingMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                hasPreview: loadingMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
              failure: {
                confirmDisabled: buttonTag(failureMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: failureMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                blockerMentionsError: failureMarkup.includes("options fetch failed"),
                hasRetry:
                  failureMarkup.indexOf('data-testid="plan-cohort-preview-blocked"') >= 0
                  && failureMarkup.indexOf(
                    "重试",
                    failureMarkup.indexOf('data-testid="plan-cohort-preview-blocked"'),
                  ) >= 0,
                hasPreview: failureMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
              stale: {
                confirmDisabled: buttonTag(staleMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: staleMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                blockerMentionsRole: staleMarkup.includes("removed_role"),
                hasPreview: staleMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
              legacy: {
                confirmDisabled: buttonTag(legacyMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: legacyMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                hasPreview: legacyMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
              scoped: {
                hasPreview: scopedMarkup.includes('data-testid="plan-cohort-shard-preview"'),
                hasFullRecallWarning: scopedMarkup.includes('data-testid="plan-full-recall-warning"'),
                confirmDisabled: buttonTag(scopedMarkup, "plan-confirm-button").includes("disabled"),
              },
            }));
            """
        )
        payload = _run_node(script)

        valid = payload["valid"]
        self.assertTrue(valid["hasPreview"])
        self.assertTrue(valid["hasFullRecallWarning"])
        self.assertTrue(valid["hasBudgetNote"])
        self.assertTrue(valid["previewBeforeConfirm"])
        self.assertTrue(valid["budgetBeforeConfirm"])
        self.assertFalse(valid["confirmDisabled"])
        self.assertFalse(valid["hasBlocker"])

        loading = payload["loading"]
        self.assertTrue(loading["confirmDisabled"])
        self.assertTrue(loading["hasBlocker"])
        self.assertFalse(loading["hasPreview"])

        failure = payload["failure"]
        self.assertTrue(failure["confirmDisabled"])
        self.assertTrue(failure["hasBlocker"])
        self.assertTrue(failure["blockerMentionsError"])
        self.assertTrue(failure["hasRetry"])
        self.assertFalse(failure["hasPreview"])

        stale = payload["stale"]
        self.assertTrue(stale["confirmDisabled"])
        self.assertTrue(stale["hasBlocker"])
        self.assertTrue(stale["blockerMentionsRole"])
        self.assertFalse(stale["hasPreview"])

        legacy = payload["legacy"]
        self.assertFalse(legacy["confirmDisabled"])
        self.assertFalse(legacy["hasBlocker"])
        self.assertFalse(legacy["hasPreview"])

        scoped = payload["scoped"]
        self.assertTrue(scoped["hasPreview"])
        self.assertFalse(scoped["hasFullRecallWarning"])
        self.assertFalse(scoped["confirmDisabled"])

        # The fail-closed handler is bound on the production button, and the
        # disabled state is the same cohortPreviewBlocker that gates it.
        plan_source = (REPO_ROOT / "frontend-demo/src/components/PlanCard.tsx").read_text(encoding="utf-8")
        self.assertIn("onClick={handleConfirm}", plan_source)
        self.assertIn("disabled={isConfirming || Boolean(cohortPreviewBlocker)}", plan_source)
        self.assertIn("if (cohortPreviewBlocker) {", plan_source)

    def test_location_fields_round_trip_through_real_request_paths(self) -> None:
        script = textwrap.dedent(
            _LOAD_MODULE_PREAMBLE
            + """
            const defaultCohort = cohortSelection.createDefaultCohortSelection(parsedOptions);

            // --- Initial submit + revision through the REAL draft-registry ->
            // payload-builder path (the exact functions the picker binds). ---
            cohortSelection.publishCohortLocationDraft(
              cohortSelection.createDefaultCohortLocationSelection(),
              defaultCohort,
            );
            const seedSubmit = api.__testBuildPlanSubmitPayload("find people", "", defaultCohort);

            cohortSelection.publishCohortLocationDraft(
              { targetLocations: ["Canada"], excludeTargetLocations: ["Europe"] },
              defaultCohort,
            );
            const editedSubmit = api.__testBuildPlanSubmitPayload("find people", "", defaultCohort);
            const editedRevision = api.__testBuildPlanSubmitPayload(
              "find people with revision",
              "history-server-owned-1",
              defaultCohort,
            );

            cohortSelection.publishCohortLocationDraft({ targetLocations: [] }, defaultCohort);
            const optOutSubmit = api.__testBuildPlanSubmitPayload("find people", "", defaultCohort);

            // Stale draft bound to a different cohort must fail safe.
            const otherCohort = selectionFor(["research"], ["current", "former"]);
            const staleSubmit = api.__testBuildPlanSubmitPayload("find people", "", otherCohort);

            // Disable clears atomically; non-cohort submits never gain locations.
            cohortSelection.publishCohortLocationDraft(null, null);
            const clearedSubmit = api.__testBuildPlanSubmitPayload("find people", "", defaultCohort);
            cohortSelection.publishCohortLocationDraft({ targetLocations: ["Canada"] }, defaultCohort);
            const nonCohortSubmit = api.__testBuildPlanSubmitPayload("find people");
            cohortSelection.publishCohortLocationDraft(null, null);

            // Explicit caller arguments always beat the draft.
            cohortSelection.publishCohortLocationDraft({ targetLocations: ["Canada"] }, defaultCohort);
            const explicitArgsSubmit = api.__testBuildPlanSubmitPayload(
              "find people",
              "",
              defaultCohort,
              ["Mexico"],
              undefined,
            );
            cohortSelection.publishCohortLocationDraft(null, null);

            // --- Review path: locations only through the authorized channel. ---
            const unauthorizedReview = api.planReviewDecisionToApiPayload(
              makeDecision({
                cohortSelection: explicitCohort,
                targetLocations: ["Canada"],
                excludeTargetLocations: ["Europe"],
              }),
              [],
            );
            const authorizedReview = api.planReviewDecisionToApiPayload(
              makeDecision({
                cohortSelection: explicitCohort,
                targetLocations: ["Canada"],
                excludeTargetLocations: [],
              }),
              ["target_locations", "exclude_target_locations"],
            );
            const nullReviewError = captureError(() =>
              api.planReviewDecisionToApiPayload(
                makeDecision({ cohortSelection: explicitCohort, targetLocations: null }),
                ["target_locations"],
              ),
            );

            // --- cloneReviewDecision must not silently drop the fields. ---
            const clonedDecision = historyRecovery.cloneReviewDecision(
              makePlan({
                reviewDecisionDefaults: makeDecision({
                  cohortSelection: explicitCohort,
                  targetLocations: ["Canada"],
                  excludeTargetLocations: [],
                }),
              }),
            );

            // --- Recovery mirrors: tri-state + stale values + fail closed. ---
            const recoveredPlan = api.__testMapPlanPayloadToDemoPlan(
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
            const recoveredOptOut = api.__testMapPlanPayloadToDemoPlan(
              {
                request: { cohort_selection: explicitCohort, target_locations: [] },
                request_preview: { cohort_selection: explicitCohort, target_locations: [] },
                plan: {},
              },
              "find people",
            );
            const legacyPlan = api.__testMapPlanPayloadToDemoPlan(
              {
                request: { raw_user_request: "find people", cohort_selection: explicitCohort },
                request_preview: { cohort_selection: explicitCohort },
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
            const nullMirrorError = captureError(() =>
              api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { cohort_selection: explicitCohort, target_locations: null },
                  plan: {},
                },
                "find people",
              ),
            );
            const tooManyError = captureError(() =>
              cohortSelection.buildCohortLocationApiPayload(Array(17).fill("x"), undefined),
            );
            const tooLongError = captureError(() =>
              cohortSelection.buildCohortLocationApiPayload(["x".repeat(241)], undefined),
            );
            const nullItemError = captureError(() =>
              cohortSelection.buildCohortLocationApiPayload(["United States", null], undefined),
            );
            const nullFieldError = captureError(() =>
              cohortSelection.buildCohortLocationApiPayload(null, undefined),
            );
            const locationInsideCohortError = captureError(() =>
              cohortSelection.parseCohortSelectionPayload({
                ...explicitCohort,
                target_locations: ["United States"],
              }),
            );
            const defaultSummary = cohortSelection.summarizeCohortLocations(undefined, undefined);
            const optOutSummary = cohortSelection.summarizeCohortLocations([], []);
            const staleSummary = cohortSelection.summarizeCohortLocations(["旧金山  Bay Area"], []);

            console.log(JSON.stringify({
              seedSubmit,
              editedSubmit,
              editedRevision,
              optOutSubmit,
              staleSubmit,
              clearedSubmit,
              nonCohortSubmit,
              explicitArgsSubmit,
              unauthorizedReview,
              authorizedReview,
              nullReviewError,
              clonedDecision: {
                targetLocations: clonedDecision.targetLocations ?? null,
                excludeTargetLocations: clonedDecision.excludeTargetLocations ?? null,
                hasTargetKey: Object.prototype.hasOwnProperty.call(clonedDecision, "targetLocations"),
                hasExcludeKey: Object.prototype.hasOwnProperty.call(clonedDecision, "excludeTargetLocations"),
              },
              recovered: {
                targetLocations: recoveredPlan.targetLocations ?? null,
                excludeTargetLocations: recoveredPlan.excludeTargetLocations ?? null,
                reviewDefaultTargetLocations: recoveredPlan.reviewDecisionDefaults.targetLocations ?? null,
                cohortSelection: recoveredPlan.cohortSelection,
              },
              recoveredOptOut: {
                targetLocations: recoveredOptOut.targetLocations ?? null,
                hasTargetKey: Object.prototype.hasOwnProperty.call(recoveredOptOut, "targetLocations"),
                optOutValue: recoveredOptOut.targetLocations,
              },
              legacyTargetLocations: legacyPlan.targetLocations ?? null,
              conflictError,
              nullMirrorError,
              tooManyError,
              tooLongError,
              nullItemError,
              nullFieldError,
              locationInsideCohortError,
              defaultSummary,
              optOutSummary,
              staleSummary,
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

        # Seeded (absent) state: omitted -> server default applies.
        self.assertNotIn("target_locations", payload["seedSubmit"])
        self.assertNotIn("exclude_target_locations", payload["seedSubmit"])

        # Edited locations enter BOTH the real initial and revision payloads,
        # alongside (never inside) the closed 5-field cohort object.
        edited = payload["editedSubmit"]
        self.assertEqual(edited["target_locations"], ["Canada"])
        self.assertEqual(edited["exclude_target_locations"], ["Europe"])
        self.assertEqual(
            sorted(edited["cohort_selection"].keys()),
            ["employment_statuses", "role_bucket_ids", "role_match", "schema_version", "source"],
        )
        revision = payload["editedRevision"]
        self.assertEqual(revision["history_id"], "history-server-owned-1")
        self.assertEqual(revision["target_locations"], ["Canada"])
        self.assertEqual(revision["exclude_target_locations"], ["Europe"])

        # Explicit [] = opt-out: serialized, not omitted.
        self.assertEqual(payload["optOutSubmit"]["target_locations"], [])

        # Stale draft for a different cohort fails safe (omitted).
        self.assertNotIn("target_locations", payload["staleSubmit"])

        # Disable clears atomically; non-cohort submits never gain locations.
        self.assertNotIn("target_locations", payload["clearedSubmit"])
        self.assertNotIn("target_locations", payload["nonCohortSubmit"])
        self.assertNotIn("cohort_selection", payload["nonCohortSubmit"])

        # Explicit caller arguments win over the draft.
        self.assertEqual(payload["explicitArgsSubmit"]["target_locations"], ["Mexico"])

        # Review: unauthorized decisions carry NO location fields; the
        # authorized channel carries them presence-intact (explicit [] kept).
        self.assertNotIn("target_locations", payload["unauthorizedReview"])
        self.assertNotIn("exclude_target_locations", payload["unauthorizedReview"])
        self.assertEqual(payload["unauthorizedReview"]["cohort_selection"], explicit_cohort)
        self.assertEqual(payload["authorizedReview"]["target_locations"], ["Canada"])
        self.assertEqual(payload["authorizedReview"]["exclude_target_locations"], [])
        self.assertIn("target_locations must be a list of names", payload["nullReviewError"])

        # cloneReviewDecision preserves the fields presence-intact.
        cloned = payload["clonedDecision"]
        self.assertEqual(cloned["targetLocations"], ["Canada"])
        self.assertEqual(cloned["excludeTargetLocations"], [])
        self.assertTrue(cloned["hasTargetKey"])
        self.assertTrue(cloned["hasExcludeKey"])

        # Recovery: stale historical free-text preserved (trimmed only, never
        # silently deleted); explicit opt-out and legacy absence round-trip
        # distinctly.
        recovered = payload["recovered"]
        self.assertEqual(recovered["targetLocations"], ["旧金山  Bay Area"])
        self.assertEqual(recovered["excludeTargetLocations"], ["Europe"])
        self.assertEqual(recovered["reviewDefaultTargetLocations"], ["旧金山  Bay Area"])
        self.assertEqual(recovered["cohortSelection"], explicit_cohort)
        self.assertEqual(payload["recoveredOptOut"]["optOutValue"], [])
        self.assertTrue(payload["recoveredOptOut"]["hasTargetKey"])
        self.assertIsNone(payload["legacyTargetLocations"])

        # Fail closed: conflicting mirrors, present null, bounds violations,
        # null items, and smuggling locations inside the closed object.
        self.assertIn("conflicting target_locations mirrors", payload["conflictError"])
        self.assertIn("target_locations must be a list of names", payload["nullMirrorError"])
        self.assertIn("at most 16 items", payload["tooManyError"])
        self.assertIn("1-240 characters", payload["tooLongError"])
        self.assertIn("non-empty names", payload["nullItemError"])
        self.assertIn("target_locations must be a list of names", payload["nullFieldError"])
        self.assertIn("do not match the v1 contract", payload["locationInsideCohortError"])

        # Display tri-state: server default vs explicit opt-out vs stale values.
        self.assertEqual(payload["defaultSummary"], "目标地区: United States（服务端默认）")
        self.assertEqual(payload["optOutSummary"], "目标地区: 不限地区（已显式退出地区筛选）")
        self.assertEqual(payload["staleSummary"], "目标地区: 旧金山  Bay Area")

    def test_facet_consumption_atomic_pair_and_fail_closed(self) -> None:
        script = textwrap.dedent(
            _LOAD_MODULE_PREAMBLE
            + """
            // --- Real projection path: the atomic pair from one layer. ---
            const topLevel = api.__testDeriveCandidate({
              candidate_id: "served-top",
              function_bucket_ids: ["research", "engineering"],
              function_bucket_source: "lane_membership",
            });
            const mirrorOnly = api.__testDeriveCandidate({
              candidate_id: "served-mirror",
              metadata: {
                function_bucket_ids: ["founding"],
                function_bucket_source: "registry_evidence",
              },
            });
            const matchingMirrors = api.__testDeriveCandidate({
              candidate_id: "served-agree",
              function_bucket_ids: ["research"],
              function_bucket_source: "registry_evidence",
              metadata: {
                function_bucket_ids: ["research"],
                function_bucket_source: "registry_evidence",
              },
            });
            const verbatimCase = api.__testDeriveCandidate({
              candidate_id: "served-verbatim",
              function_bucket_ids: ["Research"],
              function_bucket_source: "registry_evidence",
            });
            const conflictError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-conflict",
                function_bucket_ids: ["research"],
                function_bucket_source: "registry_evidence",
                metadata: {
                  function_bucket_ids: ["engineering"],
                  function_bucket_source: "registry_evidence",
                },
              }),
            );
            const sourceConflictError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-source-conflict",
                function_bucket_ids: ["research"],
                function_bucket_source: "lane_membership",
                metadata: {
                  function_bucket_ids: ["research"],
                  function_bucket_source: "legacy_inference",
                },
              }),
            );
            const missingPartnerError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-missing-partner",
                function_bucket_ids: ["research"],
              }),
            );
            const sourceOnlyError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-source-only",
                function_bucket_source: "registry_evidence",
              }),
            );
            const wrongShapeError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-shape",
                function_bucket_ids: "research",
                function_bucket_source: "registry_evidence",
              }),
            );
            const emptyListError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-empty",
                function_bucket_ids: [],
                function_bucket_source: "registry_evidence",
              }),
            );
            const badSourceError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-source",
                function_bucket_ids: ["research"],
                function_bucket_source: "client_guess",
              }),
            );
            // Legacy record without the pair (functionIds ["24"] would have
            // been projected as research by the backend): no facet membership
            // is synthesized client-side.
            const legacyNoPair = api.__testDeriveCandidate({
              candidate_id: "legacy-no-pair",
              function_ids: ["24"],
            });
            // Materialized/base overlay: the pair overlays as one atomic unit.
            const overlay = api.__testDeriveCandidateFromNormalizedRecord(
              {
                candidate_id: "overlay-base",
                function_bucket_ids: ["research"],
                function_bucket_source: "registry_evidence",
              },
              {
                function_bucket_ids: ["engineering"],
                function_bucket_source: "lane_membership",
              },
            );
            const overlayKeepsBase = api.__testDeriveCandidateFromNormalizedRecord(
              {
                candidate_id: "overlay-keep",
                function_bucket_ids: ["research"],
                function_bucket_source: "registry_evidence",
              },
              {},
            );

            // --- Real filter helpers over the derived candidates. ---
            const candidates = [
              {
                ...baseCandidate,
                id: "dual-bucket-member",
                functionBucketIds: ["engineering", "infra_systems"],
                functionBucketSource: "lane_membership",
              },
              {
                ...baseCandidate,
                id: "founding-member",
                functionBucketIds: ["founding"],
                functionBucketSource: "registry_evidence",
              },
              {
                ...baseCandidate,
                id: "server-unknown",
                functionBucketIds: ["unknown"],
                functionBucketSource: "legacy_inference",
              },
              {
                ...baseCandidate,
                id: "facet-unavailable",
                functionIds: ["24"],
              },
            ];
            const options = candidateFilters.buildFunctionOptions(candidates);
            const hits = (functionBuckets) =>
              candidateFilters
                .filterCandidatesByFacets(candidates, filterSelection({ functionBuckets }), [])
                .map((candidate) => candidate.id);
            console.log(JSON.stringify({
              topLevel: {
                functionBucketIds: topLevel.functionBucketIds,
                functionBucketSource: topLevel.functionBucketSource,
              },
              mirrorOnly: {
                functionBucketIds: mirrorOnly.functionBucketIds,
                functionBucketSource: mirrorOnly.functionBucketSource,
              },
              matchingMirrors: {
                functionBucketIds: matchingMirrors.functionBucketIds,
                functionBucketSource: matchingMirrors.functionBucketSource,
              },
              verbatimIds: verbatimCase.functionBucketIds,
              conflictError,
              sourceConflictError,
              missingPartnerError,
              sourceOnlyError,
              wrongShapeError,
              emptyListError,
              badSourceError,
              legacyNoPair: {
                functionBucketIds: legacyNoPair.functionBucketIds ?? null,
                functionBucketSource: legacyNoPair.functionBucketSource ?? null,
              },
              overlay: {
                functionBucketIds: overlay.functionBucketIds,
                functionBucketSource: overlay.functionBucketSource,
              },
              overlayKeepsBase: {
                functionBucketIds: overlayKeepsBase.functionBucketIds,
                functionBucketSource: overlayKeepsBase.functionBucketSource,
              },
              options,
              engineeringHits: hits(["engineering"]),
              infraHits: hits(["infra_systems"]),
              foundingHits: hits(["founding"]),
              unknownHits: hits(["unknown"]),
              unfilteredHits: hits([]),
            }));
            """
        )
        payload = _run_node(script)

        # Atomic pair, verbatim from the authoritative top-level layer.
        self.assertEqual(payload["topLevel"]["functionBucketIds"], ["research", "engineering"])
        self.assertEqual(payload["topLevel"]["functionBucketSource"], "lane_membership")
        self.assertEqual(payload["mirrorOnly"]["functionBucketIds"], ["founding"])
        self.assertEqual(payload["mirrorOnly"]["functionBucketSource"], "registry_evidence")
        self.assertEqual(payload["matchingMirrors"]["functionBucketIds"], ["research"])
        # Ids are consumed verbatim (trim-only), never case-normalized.
        self.assertEqual(payload["verbatimIds"], ["Research"])

        # Fail closed on every malformed/conflicting shape.
        self.assertIn("conflicting function_bucket mirrors", payload["conflictError"])
        self.assertIn("conflicting function_bucket mirrors", payload["sourceConflictError"])
        self.assertIn("invalid function_bucket_source", payload["missingPartnerError"])
        self.assertIn("incomplete function_bucket pair", payload["sourceOnlyError"])
        self.assertIn("incomplete function_bucket pair", payload["wrongShapeError"])
        self.assertIn("incomplete function_bucket pair", payload["emptyListError"])
        self.assertIn("invalid function_bucket_source", payload["badSourceError"])

        # Missing pair: facet unavailable, never repaired into membership.
        self.assertIsNone(payload["legacyNoPair"]["functionBucketIds"])
        self.assertIsNone(payload["legacyNoPair"]["functionBucketSource"])

        # Overlay: whole pair from the materialized layer; never mixed.
        self.assertEqual(payload["overlay"]["functionBucketIds"], ["engineering"])
        self.assertEqual(payload["overlay"]["functionBucketSource"], "lane_membership")
        self.assertEqual(payload["overlayKeepsBase"]["functionBucketIds"], ["research"])
        self.assertEqual(payload["overlayKeepsBase"]["functionBucketSource"], "registry_evidence")

        # Options/filtering: server ids only, including infra_systems/founding;
        # the unavailable-facet row contributes nothing and matches no bucket.
        self.assertEqual(
            payload["options"],
            [
                {"id": "engineering", "label": "engineering", "count": 1},
                {"id": "founding", "label": "founding", "count": 1},
                {"id": "infra_systems", "label": "infra_systems", "count": 1},
                {"id": "unknown", "label": "unknown", "count": 1},
            ],
        )
        self.assertEqual(payload["engineeringHits"], ["dual-bucket-member"])
        self.assertEqual(payload["infraHits"], ["dual-bucket-member"])
        self.assertEqual(payload["foundingHits"], ["founding-member"])
        self.assertEqual(payload["unknownHits"], ["server-unknown"])
        self.assertEqual(
            sorted(payload["unfilteredHits"]),
            ["dual-bucket-member", "facet-unavailable", "founding-member", "server-unknown"],
        )

        # The deleted local taxonomy may not return.
        filters_source = (REPO_ROOT / "frontend-demo/src/lib/candidateFilters.ts").read_text(encoding="utf-8")
        self.assertNotIn("const ROLE_BUCKET_TO_FUNCTION_BUCKET", filters_source)
        self.assertNotIn("const FUNCTION_BUCKET_KEYWORDS", filters_source)
        self.assertNotIn("inferredFunctionBucketFromProfile(", filters_source)
        self.assertNotIn("countKeywordOccurrences(", filters_source)
        self.assertNotIn('normalizedIds.includes("24")', filters_source)
        self.assertNotIn('normalizedIds.includes("8")', filters_source)
        self.assertNotIn('normalizedIds.includes("19")', filters_source)
        self.assertIn("candidate.functionBucketIds", filters_source)
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        self.assertNotIn("pickNonEmptyStringList", api_source)
        self.assertNotIn("pickFunctionBucketSource", api_source)

    def test_employment_membership_dual_status_and_single_display(self) -> None:
        script = textwrap.dedent(
            _LOAD_MODULE_PREAMBLE
            + """
            // Real projection: server membership truth mapped verbatim while
            // the top-level display status stays single (FT0 §6).
            const dual = api.__testDeriveCandidate({
              candidate_id: "dual-member",
              employment_status: "current",
              metadata: { cohort_employment_statuses: ["current", "former"] },
            });
            const legacy = api.__testDeriveCandidate({
              candidate_id: "legacy-lead",
              employment_status: "lead",
            });
            const malformedError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-membership",
                metadata: { cohort_employment_statuses: "current" },
              }),
            );
            const emptyMembershipError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "empty-membership",
                metadata: { cohort_employment_statuses: [] },
              }),
            );
            const invalidMembershipError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "invalid-membership",
                metadata: { cohort_employment_statuses: ["contractor"] },
              }),
            );

            const candidates = [
              {
                ...baseCandidate,
                id: "dual-display-current",
                employmentStatus: "current",
                cohortEmploymentStatuses: ["current", "former"],
              },
              {
                ...baseCandidate,
                id: "single-former",
                employmentStatus: "former",
                cohortEmploymentStatuses: ["former"],
              },
              {
                ...baseCandidate,
                id: "legacy-display-current",
                employmentStatus: "current",
              },
              {
                ...baseCandidate,
                id: "legacy-lead",
                employmentStatus: "lead",
              },
            ];
            const employmentOptions = candidateFilters.buildEmploymentOptions(candidates);
            const hits = (employmentStatuses) =>
              candidateFilters
                .filterCandidatesByFacets(candidates, filterSelection({ employmentStatuses }), [])
                .map((candidate) => candidate.id);
            console.log(JSON.stringify({
              dual: {
                employmentStatus: dual.employmentStatus,
                cohortEmploymentStatuses: dual.cohortEmploymentStatuses,
              },
              legacyMembership: legacy.cohortEmploymentStatuses ?? null,
              malformedError,
              emptyMembershipError,
              invalidMembershipError,
              employmentOptions,
              currentOnly: hits(["current"]),
              formerOnly: hits(["former"]),
              bothStatuses: hits(["current", "former"]),
              displayStatuses: candidates.map((candidate) => candidate.employmentStatus),
            }));
            """
        )
        payload = _run_node(script)

        # Membership truth transported verbatim; display stays single-valued.
        self.assertEqual(payload["dual"]["employmentStatus"], "current")
        self.assertEqual(payload["dual"]["cohortEmploymentStatuses"], ["current", "former"])
        self.assertIsNone(payload["legacyMembership"])
        self.assertIn("malformed cohort_employment_statuses", payload["malformedError"])
        self.assertIn("empty cohort_employment_statuses", payload["emptyMembershipError"])
        self.assertIn("invalid cohort_employment_statuses", payload["invalidMembershipError"])

        # The dual-status candidate counts in BOTH buckets.
        self.assertEqual(
            payload["employmentOptions"],
            [
                {"id": "current", "label": "在职", "count": 2},
                {"id": "former", "label": "已离职", "count": 2},
            ],
        )

        # ... and matches BOTH filters; legacy display-status compat and the
        # lead semantic are unchanged.
        self.assertEqual(
            sorted(payload["currentOnly"]),
            ["dual-display-current", "legacy-display-current"],
        )
        self.assertEqual(sorted(payload["formerOnly"]), ["dual-display-current", "single-former"])
        self.assertEqual(
            sorted(payload["bothStatuses"]),
            ["dual-display-current", "legacy-display-current", "legacy-lead", "single-former"],
        )

        # One card, one display status (FT0 §6.2 display-only projection).
        self.assertEqual(
            payload["displayStatuses"],
            ["current", "former", "current", "lead"],
        )


if __name__ == "__main__":
    unittest.main()
