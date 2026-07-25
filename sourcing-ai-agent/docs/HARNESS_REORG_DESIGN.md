# Harness Reorganization Design v1 — directory organization + routing mechanism

> Status: Executed (R0-R5 complete 2026-07-22: 26bcff4/60fa305/bc8b5d4/e4a5562/ccfeb2f/7960588). Owner: operator. Retained as the reorg record; remaining follow-ups (per-module doc migration passes, ARCHITECTURE/MODULES refresh) are tracked in the migration registry and router gap table.

```
status: proposed        owner: operator (via takeover session)
canonical-path: sourcing-ai-agent/docs/HARNESS_REORG_DESIGN.md
last-verified: 2026-07-22
review-triggers: any phase completes; playbook doc 06/18 changes
retirement-condition: superseded by the executed migration registry rows
```

Research basis: 4-lane workflow 2026-07-22 (playbook doc 06/18/templates extraction;
full workspace audit; external harness survey ×2) + 22-item ranked gap analysis.
Raw reports: session task output `wtwg6vsjr` (operator-side).

## 1. What the best external harnesses do (the user's research question)

| Mechanism | Source | The transferable practice |
|---|---|---|
| AGENTS.md open standard | Linux Foundation; 60k+ repos; OpenAI dogfoods 88 nested files in its monorepo | ONE thin always-read root contract; nested closest-file-wins scoping per subdir |
| Measured payoff | arXiv 2601.20404 (124 PRs, 10 repos) | repo instruction files cut median agent runtime 28.6%, output tokens 16.6%, equal completion |
| CLAUDE.md memory | Anthropic docs | hierarchy (project>user), subdir files load on entry, @path imports (≤4 hops); keep root ~200 lines imperative |
| Skills / commands | Anthropic `.claude/skills`, `.claude/commands` | three-tier progressive disclosure: ~100-token trigger always loaded → <5k body on activation → references on demand; slash commands = named task entry points |
| Cursor rules | `.cursor/rules/*.mdc` | four activation modes: always / glob-auto / description-triggered / manual — the most explicit conditional-loading model |
| Aider repo map | tree-sitter + PageRank | clean symbol graphs are an agent affordance; an 82K-line orchestrator is invisible-by-dilution to ranked maps |
| SWE-agent ACI | paper result | immediate machine feedback (a linter rejecting bad edits) ~tripled solve rates — committed verify commands are first-class harness parts |
| OpenHands / Devin | microagents; Knowledge+Playbooks | committed, keyword/condition-gated knowledge files; repeated task shapes become playbooks |
| Google g3doc | SWE at Google ch.10 | docs beside code; machine-readable freshness stanza (owner + reviewed date, ~3-month nag); landing pages are pure "traffic cops" |
| GitLab / k8s / dotnet | runbooks repo; OWNERS; docs index | alert-keyed runbooks; per-directory OWNERS driving review routing; generated indexes that cannot rot independently of their registry |
| Diátaxis | Canonical, Django | tutorial / how-to / reference / explanation are different documents; do not blend modes |

Convergent principle: **small always-loaded contract + explicit conditional routing to
everything else, enforced by CI feedback** — precisely the playbook's doc 06/18 three-hop
routing contract, plus a tool-native loading layer the playbook does not yet cover.

## 2. Gap verdict (against playbook doc 06/18 + templates)

The machinery exists in DECLARED form (docs/INDEX.md with governance rules, docs/modules/,
docs/governance/, docs/archive/, .coord/ per template, RESIDUAL_LEDGER) but is UNENFORCED
and inverted in practice: the real current truth is untracked (HANDOFF/PROMPT/CI workflow)
or gitignored (.coord/BOARD.md declared SoT); 8+ competing "start here" surfaces span three
eras (April README chain → June INDEX → July handoff); snapshot budgets are blown 2–5×
(PROGRESS 696 lines vs its own 300 rule; NEXT_TODO 1013; READMEs 61K/27K); ~60 TRACK_D
increment docs sit flat in docs/; final deliverables (6 lab CSVs) live in gitignored
scratch without a manifest; scripts/configs mix durable tooling with dated one-offs; the
live-script registry hides inside HARVESTAPI_PLAYBOOK.md; no link-check or routing lint
runs anywhere (Principle 28: prose-only = not implemented).

## 3. Target structure (workspace)

```
<workspace root>
  AGENTS.md            # thin workspace contract (exists; refresh; canonical rules live here)
  CLAUDE.md            # NEW thin tool-native layer: references AGENTS.md, never duplicates
  README.md            # thin product/setup pointer → docs router (replaces April chain)
  PROGRESS.md          # bounded snapshot (≤200 lines, owner+refresh+cleanup dates)
  NEXT_TODO.md         # bounded snapshot (same budget contract)
  .claude/skills/      # NEW repeated ops as skills (live-wave, review-gate, handoff, daemon-ops)
  .claude/commands/    # NEW slash entry points (e.g. /takeover, /live-preflight)
  archive/legacy-research-2026-04/   # April Anthropic华人专项 assets moved out of root
  sourcing-ai-agent/
    AGENTS.md          # package scope rules (exists; trim to scope-only)
    docs/README.md     # THE router (replaces INDEX.md): problem→module table, module
                       # routes+status, snapshot links, routing-gaps table, audit dates
    docs/modules/<m>/  # module indexes own their contracts/testing/operations/decisions
    docs/archive/track-d/   # ~60 TRACK_D increment docs move here (registry rows added)
    scripts/README.md  # live-script registry moves here from HARVESTAPI_PLAYBOOK.md
    deliverables/      # NEW governed tier: MANIFEST.md tracked (checksums, provenance,
                       # judge/batch lineage); CSV payloads NOT in git (people data)
  x-first-researcher-sourcing/
    AGENTS.md (exists) # README 27K narrative → thin README + bounded PROGRESS
.gitignore adds: .worktrees/, ai-assisted-engineering-playbook/
```

Routing contract (three hops, enforced): AGENTS.md/README → docs/README.md router →
module index → canonical artifact. `.coord/` stays the gitignored LIVE channel but
authority reverts to "git wins": durable decisions promote into tracked snapshots/docs,
BOARD keeps only live lane state.

## 4. Phases (each = reviewable slice; R0 cheapest-first per gap leverage ranking)

- **R0 hygiene** — ignore .worktrees/ + playbook repo; delete zero-risk strays (6 empty
  stub dirs, self-nested output/, 0-byte .codex; list shown before deletion); decide
  tracked fate of containerized-pre-release.yml; archive HANDOFF/PROMPT into
  docs/archive/ after extracting their live content into snapshots.
- **R1 entry chain** — new thin root README + refreshed AGENTS.md + new CLAUDE.md;
  rebuild docs/README.md as the router (problem-routing + gaps table); April-era
  README/ONBOARDING/GITHUB_SYNC_PREP archived with redirect stubs; PROGRESS/NEXT_TODO
  rewritten within budget.
- **R2 tool-native layer** — .claude/skills + commands for the 4 repeated ops flows;
  scripts/README.md registry (HARVESTAPI_PLAYBOOK keeps provider knowledge, loses the
  registry role); x-first README slimmed.
- **R3 docs lifecycle** — TRACK_D backlog → archive with migration-registry rows;
  metadata blocks (status/owner/last-verified) on active docs; stale ARCHITECTURE/
  MODULES banners.
- **R4 deliverables tier** — deliverables/MANIFEST.md with checksums + lineage for the
  6 lab CSVs (+ export regeneration commands); artifacts commit/publish policy doc.
- **R5 enforcement** — vendor playbook check_markdown_links.py + routing lints
  (index-exists, owner+path-exists, no-duplicate-canonical, snapshot budgets) into the
  curated CI lane.

Explicitly OUT of scope here: code decomposition (Track A owns; symbol-graph legibility
noted as motivation), runtime-root unification (Phase C leftover — user sequenced it
AFTER this mechanism work), CODEOWNERS (single-operator repo today; revisit at team
scale), Diátaxis re-mode-ing of existing docs (fold into artifact classes gradually).

## 5. Decisions needed before execution

D1 April legacy assets: move under archive/ (recommended) vs leave tracked at root.
D2 CLAUDE.md: thin referencing layer (recommended) vs full standalone content.
D3 Deliverables payloads: out-of-git + tracked manifest (recommended, people data) vs tracked.
D4 Phase approval: execute R0→R5 sequentially with per-phase review, or approve R0–R2 now.
