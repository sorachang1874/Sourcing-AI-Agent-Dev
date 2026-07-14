# Query-policy remediation pinned rereview

## Evidence header

- Reviewer: non-author, adversarial, read-only.
- Base: `b79df16654ec22ac4dd3bcd4ca5a49871696fb2c`
- Reviewed: `0bfe7c49d2251ee8fdf9106ce8290f2d1e313a61`
- Reviewed commit has the stated base as its sole parent.
- Tree: `bac242f09603b0d7c3e385ab0ccc80f747674827`
- Binary diff SHA-256: `e50bb36eb6960a57de58f383303e1411dbd380d1c670c0a573b51d8d314b11b2`
- Scope: 12 files, 1,155 insertions, 143 deletions.
- Detached clean worktree: `/private/tmp/x-first-query-rereview-0bfe7c4`
- Baseline worktree: `/private/tmp/x-first-query-base-b79`
- Current dirty adaptive/Stage 2 work was not read. Only pinned caller/tests at the reviewed commit were inspected to diagnose a downstream regression.
- No live/provider/model call, network access, credential use, tracked edit, staging, or commit occurred.

## Validation

- Python 3.12.13 query-policy/migration targeted suite: 44/44 passed.
- Python 3.14.4 query-policy/migration targeted suite: 44/44 passed.
- Six focused lineage, descriptor-binding, issuance-ID, protected-boundary, secondary-argument, and migration tests: 6/6 passed on both Python versions.
- Custom protected/neutral/secondary matrix: 34/34 passed:
  - production registry, history head, descriptor, manifest, and issuance hashes reconciled;
  - 13 direct multilingual protected operands rejected;
  - all four neutral collocations accepted;
  - all four neutral-plus-protected near misses rejected;
  - China/Asia professional-experience query remained allowed;
  - three hidden secondary fields and one thread secondary operand rejected;
  - valid keyword, semantic, user, and thread shapes retained.
- Custom snapshot-chain mutations: 6/6 behaved as required:
  - complete chain accepted;
  - old snapshot replay accepted only with the complete latest chain;
  - inherited policy-row rewrite rejected;
  - incorrect predecessor hash rejected;
  - truncated canonical history rejected;
  - history extension without a matching latest snapshot rejected.
- Deterministic issuance-ID probe returned identical `qci_9eeda535b4d5df7738ea5602` under Python 3.12.13 and 3.14.4.
- Contract validator: valid under both Python versions, with 20/20 expected rows and precision/recall 1.0.
- Six changed JSON config/schema files parsed successfully.
- Ruff lint passed.
- Ruff format check passed for all four changed Python files.
- Compileall passed for all four changed Python files under both Python versions.
- `git diff --check` passed.
- Privacy tests covering query plaintext, key/nonce material, recursive forbidden values, and CLI failure output passed within the 44-test targeted suite.
- Final detached worktree remained Git-clean.

### Full-suite baseline comparison

- Base under Python 3.12.13: 317/317 passed.
- Base under Python 3.14.4: 317/317 passed.
- Reviewed head under Python 3.12.13: 320 tests, 3 failures and 2 errors.
- Reviewed head under Python 3.14.4: 320 tests, the same 3 failures and 2 errors.
- After restoring only the former subject-only validator semantics in process, the exact five failing nodes passed 5/5 under both Python versions.

Severity totals: P0=0, P1=1, P2=0.

## P1 findings

### P1-1 — New: the “backward-compatible” subject-validator alias breaks adaptive native-X transcript verification

At `src/x_first/grok_cli_exploration.py:964-967`, `base_discovery_tool_subject_allowed` now delegates to `base_discovery_tool_arguments_allowed`. The latter requires the exploration-receipt envelope for `x_keyword_search`: exactly `query`, `limit`, and `mode`.

The unchanged pinned adaptive runner imports the public subject validator at line 38 and calls it at `src/x_first/adaptive_grok_wave_runner.py:2988`. Its Grok session transcript carries the actual provider input `{"query": "synthetic query"}`; transport shape ownership is separate from its protected-subject check.

The semantic change therefore rejects an otherwise valid native-X transcript before evidence reconciliation. The observable regressions are:

1. A valid live run becomes `provider_evidence_invalid` instead of `completed`.
2. Two campaign-bridge tests stop at `adaptive_source_not_live_completed`.
3. The same-path bundle-swap test never reaches its intended lease-integrity check.
4. The grant-window replay test loses its expected `process_release_outside_grant_window` finding.

This is not an inherited baseline failure: both base full suites pass 317/317, while both reviewed-head suites fail the same five nodes. Replacing only the alias target with the prior subject-text predicate makes all five pass under both Python versions.

The fix should preserve two explicit contracts:

- A stable public subject-only predicate for consumers that own their own native-tool envelopes.
- The new complete-envelope predicate for the query explorer and migration paths that own `limit`, `count`, `mode`, and closed argument shapes.

If adaptive transcripts also need full secondary-field closure, define an explicit adaptive transcript-envelope profile matching actual Grok raw inputs; do not silently reuse the exploration receipt envelope. Add the five adaptive nodes, or an equivalent direct shared-contract test, to the query-policy regression lane.

## Verified non-findings

- Code admission binds every registry version to canonical snapshot and head hashes.
- The complete admitted snapshot directory, positions, predecessors, and latest history are validated before selecting an old policy.
- History truncation, unrepresented history extension, incorrect predecessor linkage, and inherited policy-content rewriting fail closed.
- Every history row binds policy path/hash, protected-boundary version, semantic-manifest hash, run, key, and nonce identity.
- A coherently forged issuance ID is rejected by runtime recomputation.
- Direct `LGBTQ`, autism, wheelchair-user, and reviewed Chinese, French, Spanish, Japanese, and Korean operands fail closed.
- The four neutral collocations are span-local exceptions; adding a second protected operand still fails closed.
- Unknown, nested, and secondary fields fail closed in the exploration receipt envelope.
- Public config/schema artifacts expose no private query operands, HMAC key, or nonce.
- The prior formatting failure is resolved.

## Promotion boundary

The original snapshot-chain, immutable-content, protected-value, neutral-collocation, deterministic-ID, schema, format, and privacy findings are closed. Promotion of this query-policy slice remains blocked solely by the new shared-validator compatibility regression. Adaptive implementation outside this pinned diff is not otherwise reviewed here, and no live or milestone claim is authorized.

NO-GO
