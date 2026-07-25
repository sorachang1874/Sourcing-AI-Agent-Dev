# Google DeepMind wave2-v4 Unicode discovery-wrapper final pinned rereview

## Evidence header

- Recovery reviewer: independent non-author subagent `/root/gdm_v4_go_artifact_recovery`; narrow artifact-recovery
  rereview against pinned Git objects only.
- Reviewed head: `c26089649ccf0cb8ec031e7072a2a1eef015c05d`.
- First parent: `0dd21f5400c55be9168c72df57ee16bf5aa9f30c`.
- Reviewed scope: exactly the six `x-first-researcher-sourcing/` files in the parent-to-head diff (`58 insertions`,
  `14 deletions`), with binary-diff SHA-256
  `f627adb2bf831cf82b69550307c644314fed40833ceddd8becb7f6330c87a9c1`.
- Prior finding source:
  `docs/reviews/2026-07-15-da8ef69-gdm-v4-pinned-rereview.md`; this recovery review checked only its remaining
  P1-1 Unicode-wrapper boundary and the requested production policy binding.
- Reproduction environment for the recovery review: detached clean worktree
  `/private/tmp/xfirst-c260896-artifact-recovery.Q9i9wC` at the exact full SHA. `git status --short --branch`
  remained `## HEAD (no branch)` after validation. The ambient dirty working tree, OAuth state, private runtime
  artifacts, and later commits were not read as review evidence.
- Recovered full-suite evidence is explicitly attributed below to the preceding independent non-author reviewer
  `/root/gdm_v4_c260896_final_rereview`. That reviewer completed the full validation and adversarial fake-live probes
  in detached clean worktree `/private/tmp/xfirst-c260896-review.Su2bWe`, but its sub-session reached its usage limit
  while publishing the durable artifact. The recovery reviewer did not rerun or claim authorship of those full-suite
  results.
- Recovery-review evidence is separately identified below. It consists of independent SHA/diff verification, three
  targeted tests, Ruff, `git diff --check`, runtime-versus-config semantic-digest reconciliation, twelve direct
  negative helper probes, and four direct positive controls.
- Neither reviewer made a network, Grok/X/model call, read a real credential, issued a live grant, executed a
  provider, wrote product state, staged, committed, or promoted this scope. This artifact is the only
  shared-working-tree write made by the recovery reviewer.

## Recovered full-suite evidence from the preceding reviewer

The following results are recovered from and attributed to
`/root/gdm_v4_c260896_final_rereview`; they are not represented as reruns by the recovery reviewer:

- Runner suite: exit `0`, `65/65` tests passed in `17.547s`.
- Full standard-library suite: exit `0`, `354/354` tests passed in `46.319s`.
- Contract preflight: exit `0`, status `valid`, precision `1.0`, recall `1.0`, predicted/relevant `20/20`, false
  merges `0`.
- Ruff and pinned `git diff --check`: clean.
- Twelve synthetic fake-live wrapped-handle evaluations were rejected as `provider_evidence_invalid`, recorded
  `session_proof.status=invalid`, and replayed with `[]`; broad keyword, broad semantic, closed target-bound user,
  and typed thread positive controls remained green.

## Independently repeated recovery-review evidence

- Exact object verification produced head `c26089649ccf0cb8ec031e7072a2a1eef015c05d`, parent
  `0dd21f5400c55be9168c72df57ee16bf5aa9f30c`, and binary-diff SHA-256
  `f627adb2bf831cf82b69550307c644314fed40833ceddd8becb7f6330c87a9c1`.
- The three requested targeted tests passed independently (`3/3` in `2.099s`):
  `test_discovery_only_session_query_policy_rejects_mechanical_person_hydration`,
  `test_discovery_only_effective_policy_fails_closed_on_person_scoped_session_query`, and
  `test_production_effective_prompt_policy_owns_exact_openai_and_google_deepmind_waves`.
- `ruff check src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py` — exit `0`,
  `All checks passed!`.
- `git diff --check 0dd21f5400c55be9168c72df57ee16bf5aa9f30c
  c26089649ccf0cb8ec031e7072a2a1eef015c05d` — exit `0`.
- Runtime recomputation for `discovery_only_no_person_hydration_v1` produced
  `e528e99abe6b5ae19fe47a3b4520630459f91444682a786c57a59f1dc54e0270`, exactly matching the production
  `google_deepmind_pretraining_recall_wave2_discovery_only.v4` entry.

Severity totals: `P0=0`, `P1=0`, `P2=0`.

## Findings

No blocking or nonblocking finding was identified in the pinned six-file scope.

## Direct phase-boundary probes repeated by the recovery reviewer

Each required payload was passed directly to `_session_query_phase_arguments_allowed` under
`discovery_only_no_person_hydration_v1` for both `x_keyword_search` and `x_semantic_search`:

- `（TargetPerson）`
- `“TargetPerson”`
- `＠TargetPerson`
- `TargetPerson。`
- `Target\u200bPerson`
- `ｆｒｏｍ：TargetPerson`

All twelve evaluations returned `False`. This independently verifies NFKC normalization, fullwidth `from:`,
fullwidth at-sign, curly/CJK wrappers, and Unicode `Cf` removal in the common query boundary.

The four requested positive controls each returned `True`:

- broad keyword: `Google DeepMind pretraining researchers`;
- broad semantic: `large scale model training engineer`;
- closed target-bound user query: `Google DeepMind researcher`;
- typed thread expansion: `x_thread_fetch` with `post_id=123456`.

The implementation applies the common normalization and single-subject rejection before its tool-specific branch
(`src/x_first/adaptive_grok_wave_runner.py:3600-3621`), then applies full-consumption target/professional grammar to
`x_user_search` (`:3622-3645`). The semantics registry names the revised v3 wrapper policy (`:96-103`), and the
production entry binds its exact recomputed digest.

## Closure ledger

| Boundary | Final state | Evidence |
|---|---|---|
| Prior P1-1 Unicode wrapper bypass | `closed` | All six required Unicode/fullwidth/format-control payloads fail closed under both keyword and semantic tools; preceding-review fake-live/replay evidence independently reports the same twelve paths fail closed |
| Broad discovery availability | `closed / no regression` | Broad keyword and semantic controls remain allowed in the recovery probe and green in the attributed fake-live evidence |
| Closed target-bound user search | `closed / no regression` | `Google DeepMind researcher` remains allowed by the full-consumption target/professional grammar |
| Typed thread expansion | `closed / no regression` | `x_thread_fetch` with typed `post_id` remains allowed |
| Production effective-entry binding | `closed` | Committed and runtime-recomputed digests both equal `e528e99abe6b5ae19fe47a3b4520630459f91444682a786c57a59f1dc54e0270` |

## Gate consequence

The remaining P1 from the prior rereview is closed without blocking broad discovery, closed target-bound user
search, or typed thread expansion. The recovered full-suite/fake-live evidence and the recovery review's independent
narrow probes agree. This pinned commit is acceptable for the separately authorized, one-shot, no-fallback Google
DeepMind wave2-v4 discovery-only live gate. The verdict does not claim live success, population convergence,
source-bound payloads, hydration readiness, product promotion, canonical writes, or outreach authority.

GO
