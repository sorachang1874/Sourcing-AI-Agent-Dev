# Google DeepMind wave2-v4 discovery-only remediation pinned rereview

## Evidence header

- Reviewer: independent non-author subagent `/root/gdm_v4_da8ef69_pinned_rereview`; adversarial rereview against
  pinned Git objects only.
- Reviewed head: `da8ef69307e2b3570dbee5b7cad82ecdd3f46942`.
- First parent: `edd2489b8c5394372d7ddae3bddaface8417fe60`.
- Reviewed scope: exactly the six `x-first-researcher-sourcing/` files in the parent-to-head diff (`313 insertions`,
  `45 deletions`), with binary-diff SHA-256
  `ae639d17efec91a301628518fa612cb82b53fb1315336de1ed7dc4cf91884b14`.
- Prior finding source:
  `docs/reviews/2026-07-15-36238ab-gdm-v4-pinned-rereview.md`; this rereview re-tested its P1-1, P1-2, and P2-1
  rather than relying on the author summary.
- Reproduction environment: detached clean worktree `/private/tmp/x-first-da8ef69-rereview.rPE48N` at the exact full
  SHA. `git status --short --branch` remained `## HEAD (no branch)` after validation. The ambient working tree,
  OAuth state, private live artifacts, and later changes were not read as review evidence.
- No Grok/X/model/network call, real credential read, grant issuance against a real account, provider execution,
  staging, commit, product write, or repository promotion occurred. Mutation probes used only synthetic binaries,
  synthetic auth bytes, fake sessions, automatically deleted temporary run roots, and fixture data. This artifact is
  the only shared-working-tree write.

## Pinned validation

- `PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v` — exit `0`, `65/65`
  tests passed in `15.060s`.
- `PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v` — exit `0`, `354/354` tests passed in
  `36.919s`.
- `PYTHONPATH=src .../.venv/bin/python -m x_first.contracts` — exit `0`, status `valid`, precision `1.0`, recall
  `1.0`, predicted/relevant `20/20`, false merges `0`.
- `.../.venv/bin/ruff check src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py` — exit
  `0`, `All checks passed!`.
- The production Google DeepMind discovery-only row records session-query semantic digest
  `94d32396f147d924835c8212a39a05f5e16c105d20d811c6368589be6169e094`; runtime recomputation produced the exact
  same digest for `discovery_only_no_person_hydration_v1` and its v2 projection semantics.
- No sealed runtime bundle is tracked in the pinned Git tree, so no historical private runtime path was read. The
  committed synthetic compatibility lanes for normalization-only result-v3, pre-normalization result-v3, structured
  result-v2, and legacy fixture bundles all remained green.

Severity totals: `P0=0`, `P1=1`, `P2=0`.

## Findings

### P1-1 `[residual]` — Unicode punctuation still converts a bare-handle hydration query into an accepted discovery query

The ASCII examples from the prior review are now rejected, and `x_user_search` applies NFKC plus a closed ASCII
grammar. The keyword/semantic path does not apply the same normalization. It strips only Python
`string.punctuation` from the raw query and then returns `True` for every non-user-search query that is not an ASCII
bare-handle match (`src/x_first/adaptive_grok_wave_runner.py:3602-3610`). NFKC is applied only after that early return,
inside the user-search branch (`:3613-3618`).

A direct pinned probe showed all of the following discovery-only `x_keyword_search` arguments were accepted:
`（TargetPerson）`, `“TargetPerson”`, `＠TargetPerson`, and `TargetPerson。`. An end-to-end synthetic live probe then
substituted each value as the only native-X keyword query. Every case sealed `receipt.status=completed`, recorded
`session_proof.status=verified`, published `X_SEARCH_PARTIAL`, and passed full bundle replay with `[]`. These are four
Unicode spellings of one exact account subject, not broad population discovery.

The updated contract now explicitly says that only surrounding ASCII punctuation is removed
(`docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md:90-94`). That documents the implementation but does not close the prior
finding's required full Unicode-normalization boundary, and it conflicts with the selected policy's operational claim
that this is a `discovery_only_no_person_hydration_v1` session. A provider/model can still move into exact-person
hydration while the operator attests that the phase boundary was verified.

Required remediation: normalize every query-bearing native-X tool with one versioned NFKC/full-consumption lexer
before `from:` and single-subject detection. Strip or classify surrounding Unicode punctuation consistently, then
reject the normalized single handle/name token before branching by tool. Preserve the documented multiword keyword/
semantic residual and typed post-ID thread expansion. Add direct and fake-live regressions for at least fullwidth
parentheses or `＠`, curly quotation marks, and a non-ASCII terminal punctuation mark; bind the revised semantics digest
into the production entry.

## Prior-finding closure ledger

| Prior finding | Rereview state | Evidence |
|---|---|---|
| P1-1 punctuation/Unicode query bypass | `residual` | ASCII quotes/parentheses/period and non-ASCII user-search prefixes now fail, but Unicode-wrapped bare handles still seal completed, verified keyword sessions and replay cleanly |
| P1-2 partial convergence reason | `closed` | For every nonblocked discovery-only model `OK` or `PARTIAL`, the projector sets `X_SEARCH_PARTIAL`, overwrites the model reason with `_DISCOVERY_CONVERGENCE_UNPROVEN_REASON`, and appends the same operator limitation (`src/x_first/adaptive_grok_wave_runner.py:1507-1517`); a pinned partial-with-convergence probe validated this exact output |
| P2-1 transcript-terminal exact technical-limit replay | `closed` | Current command policy binds `post-transform-json-envelope-and-terminal-limit-replay-v1`; current `technical_limit_exceeded` bundles rerun terminal recovery and compare JSON limit kinds in both directions (`:6024-6072`). The concatenated-terminal forged `json_structure` to `json_bytes` mutation and process `stdout_bytes` to `json_bytes` mutation both produce `structured_technical_limit_mismatch` |

## Additional verified non-findings

- Current operator-result semantics have a distinct command-policy digest that includes both normalization and
  `OPERATOR_RESULT_ARTIFACT_POLICY_VERSION`; the prior normalization-only digest is a separate replay-only helper
  (`src/x_first/adaptive_grok_wave_runner.py:1953-1975`). Receipt/grant/bundle replay recognizes the compatibility
  lineage without treating the old digest as the current issuance policy.
- The normalization-only result-v3 compatibility test produces a bundle under the old digest, preserves its
  relationship downgrade, and replays with `[]`. Pre-normalization result-v3 remains sealed under its old rejected
  interpretation. No current-policy relabel of a process limit as a JSON limit survived replay.
- P1-2's operator reason is uniform for model-authored `OK` and `PARTIAL`; `X_SEARCH_BLOCKED` remains blocked. No
  hydration-specific Post/Reply limitation is injected into discovery-only output.
- The production configuration and runtime agree on the v2 session projection/query semantic digest. Mutating the
  stored digest remains fail-closed through effective-prompt policy loading.
- No canonical identity, CRM, ranking, export, outreach, provider fallback, protected-identity authority, or live X
  access was added by this remediation.

## Gate consequence

P1-2 and P2-1 are closed, and the artifact-policy cutover preserves the tested replay lineage. P1-1 remains a direct
phase-boundary bypass: exact-person hydration can still be performed and attested as a verified discovery-only
session by changing only Unicode punctuation. This blocks provider-costing Google DeepMind wave2-v4 live execution,
performance/convergence claims, and milestone promotion at this pinned commit. It requires a new pinned fix, targeted
and full validation, and a fresh non-author rereview. Unrelated fixture-only work remains outside this verdict.

NO-GO
