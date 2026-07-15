# Google DeepMind wave2-v3 Boolean-scope pinned rereview

## Evidence header

- Reviewer: independent non-author subagent `/root/gdm_v3_go_artifact`; adversarial and read-only against pinned Git
  objects.
- Reviewed head: `7d54a23421dda07bb3089dbc83d58102e7371ffe`.
- Parent: `9b2dfaff67e3afdcdec3ed7d44a3be211f39bfa3`.
- Reviewed scope: the six-file `x-first-researcher-sourcing/` parent-to-head remediation diff (`82 insertions`,
  `8 deletions`), binary-diff SHA-256
  `60a47a7d27349d6374ce24e0019401c52a115259725e5ff1fa9958dc6b0014ec`.
- Reproduction environment: a detached clean worktree at the exact reviewed commit. No ambient working-tree
  implementation was inspected.
- No Grok/X/model/network call, OAuth or credential read, grant issuance, provider execution, broad test suite,
  staging, commit, promotion, or product write occurred. This artifact is the only working-tree write.

## Targeted validation

- This reviewer independently ran only the two prior Boolean-scope mutations plus one valid conjunctive Post/Reply
  classifier-and-projection probe against the pinned objects.
- `from:TargetPerson pretraining -filter:replies OR pretraining` and
  `from:TargetPerson pretraining filter:replies OR pretraining` both classified as unattributed. With an unresolved
  candidate, neither produced Post/Reply coverage and the operator projection monotonically downgraded the model's
  `X_SEARCH_OK` to `X_SEARCH_PARTIAL`.
- The valid pair `from:TargetPerson pretraining -filter:replies` and
  `from:TargetPerson pretraining filter:replies` classified respectively as `authored_post` and `authored_reply`,
  projected both exact query hashes into candidate coverage, and preserved `X_SEARCH_OK`.
- The closed classifier rejects standalone `OR` and pipe alternatives before single-handle attribution
  (`src/x_first/native_x_evidence_contract.py:36,87-103`); operator projection still owns the unresolved-surface
  downgrade (`src/x_first/adaptive_grok_wave_runner.py:1353-1379`). The live prompt now requires a conjunctive pair
  and explicitly excludes `OR` and `|` from coverage credit
  (`prompts/live-exploration/2026-07-15-google-deepmind-pretraining-recall-wave2-v3.md:82-93`).
- Prior-reviewer evidence, not rerun or claimed by this reviewer: adaptive runner `57/57` passed; complete X-first
  suite exited zero with `346` tests. This rereview intentionally did not run a broad suite.

## Findings and gate consequence

Severity totals: `P0=0`, `P1=0`, `P2=0`.

No finding remains in the assigned Boolean-scope remediation. The two exact escape mutations cannot forge the
mandatory per-handle Post/Reply pair, while valid conjunctive queries retain the intended classifier and projection
behavior. The pinned commit clears this scoped gate for one fresh, operator-triggered Google DeepMind wave2-v3 live
execution subject to the existing fresh request, one-shot grant, deadline, budget, retention, and no-fallback
controls. Model-mediated rows remain non-source-bound and this verdict does not promote them to campaign or product
truth.

GO
