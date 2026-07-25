# Grok v5.1 excerpt-preflight pinned independent review

## Evidence header

- Reviewer: independent non-author subagent `/root/v5_1_pinned_review`.
- Reviewed commit: `6bfca4afb897b3c297473b7b2fcce52836a21a92`.
- Exact first parent: `dce094e71c2f6085b517f96e665be25335e0a416`.
- Reviewed scope: the exact first-parent diff for eight files under `x-first-researcher-sourcing`; `541 insertions`,
  `22 deletions`. Binary-diff SHA-256:
  `bd2ba0919460dc733d088c9cac7fc06d8622d74e3349288b0ca55549a29ae3de`.
- Reproduction tree: detached pinned worktree
  `/private/tmp/xfirst-review-6bfca4a-20260716` at the reviewed commit. Ambient working-tree changes were not review
  inputs.
- The private retained bundles were read only for de-identified aggregate replay. No live call, grant, OAuth mutation,
  candidate text/handle output, provider execution, implementation edit, staging, commit, or push occurred. This
  artifact is the reviewer's only repository write.

## Exact validation

- Registry append-only probe: `16 -> 18` entries; every pre-existing row remained object-identical and in the same
  relative order. The only new IDs are the OpenAI and Google DeepMind v5.1 excerpt-preflight successors.
- Prompt byte probe: each v5.1 prompt differs from its own v5 predecessor only by the six-line excerpt-preflight
  paragraph. Query coverage, lab vocabulary, stopping heuristic, and the absence of business candidate/query/call caps
  are unchanged.
- Prompt SHA-256 and selected-entry binding:

  | Entry | Prompt SHA-256 | Selected-entry SHA-256 | Session policy | Prior-input policy |
  | --- | --- | --- | --- | --- |
  | OpenAI v5.1 | `2fcf57a05cf788a564cf17f6ab84194a391d7322133503afebc9ddad7cb3e180` | `3e2346e80368a52e5ed0570787da92cbc945c780cbe2f23410912f8fbc34b2d5` | official discovery v2; `OpenAI` only | `require_empty_prior_waves_v1` |
  | GDM v5.1 | `1ba6e53538ac851f1f160a80e63aec3787f31ce7c2711541450800a3cd2836d5` | `a7bf88daf19f0d6213225d1c493da272ca7db18dd8292757a489c9d5d1b99e18` | official discovery v2; `GoogleDeepMind`, `DeepMind` only | absent, preserving v5's optional-prior semantics |

  The shared session-policy digest recomputed to
  `26305c1bd1529e51bb35e9ce1533b64f09b2ed87f99981f14be5dde0718101fc`; OpenAI's prior-policy digest recomputed to
  `8b2e6ed95ad3574903a2d459c0bcafb92cfa293020fd716ad311e115144ec47c`. Target plus prompt SHA selection rejected
  cross-lab use. Old v5 prompt SHAs remained exactly
  `5318d0baedd1b7fe6ec0057c01dfceccff5296d0e1f85dbf5642a094ec85164b` and
  `09991dbb9420309628f36794a79268a3ecb1aa67439f5e3d42169cb9795c8978`.
- Excerpt boundary: v5.1 instructs one source-faithful contiguous span of at most 240 Unicode code points and forbids
  paraphrase, span concatenation, and ellipsis insertion. The runtime/schema hard maximum remains 280. No operator
  truncation or result-rewrite path was added; a 280-code-point value passes and 281 fails.
- Targeted runner suite: `92/92` passed.
- Full standard-library suite: `381/381` passed.
- Contract preflight: `status=valid`, `errors=[]`, precision/recall `1.0/1.0`, predicted/relevant `20/20`, false merges
  `0`.
- Ruff: `All checks passed!`; Python compile: `45/45`; `git diff --check`: exit `0`, no output.
- Retained bundle replay on pinned code:
  - v4 `grok_wave_live_f41ad0d4206f4435baceb4bfe0b0e924`: `[]`;
  - first v5 `grok_wave_live_c4ec7140840942b2b1e62b5d078562f1`: `[]`;
  - replacement v5 `grok_wave_live_4324e8bce20c4e04b1a3b6e8a1a69418`: `[]`.

## Private diagnostic reconciliation

The replacement bundle independently reproduced: provider exit `0`; elapsed `166,160 ms`; `43/43` native-X calls
(`29` keyword, `6` semantic, `4` user, `4` thread); `95` events; one model turn; token counts
`150,242 input + 768,256 cache-read + 18,178 output = 936,676 total`; conservative cost `$1.946064`; terminal
diagnostic `20` casefold-unique candidates, `37` evidence rows, and `295` reported observations. Query arguments were
`43/43` unique, with `21 Latest`, `8 Top`, five positive Reply queries, four explicit time shards, and five allowed
official-account `from:GoogleDeepMind` queries.

The complete validator output was exactly `evidence_value_invalid:7:1`. Excerpt lengths were min `38`, max `319`,
`6/37` over the new 240 operating target, and `1/37` over the 280 hard maximum. A reviewer-only in-memory counterfactual
that replaced only that row with its first 240 code points reduced the validator errors to `[]`; persisted evidence was
not modified. This supports the documented single-field failure without introducing an operator repair behavior.

The v4 private diagnostic independently reproduced `111,982 ms`, 35 calls (`24/5/3/3` keyword/semantic/user/thread),
`592,900` total tokens, `$1.239336`, 13 unique candidates, and 20 evidence rows. The documented v4-to-v5 changes then
recompute correctly: candidates `+53.8%`, evidence `+85.0%`, candidates/call `+25.2%`, calls/candidate `-20.1%`,
seconds/candidate `-3.6%`, and cost/candidate `+2.1%`. Both inputs are contract-invalid diagnostics, so these are method
selection aggregates, not recall, precision, or formal quality claims.

## Findings and accepted residuals

Severity totals: `P0=0 / P1=0 / P2=1 / P3=0`.

### [P2][residual] Thread-fetch causal origin remains unproven

The inherited transport records typed thread-fetch arguments but not the native-X result body that caused a thread to
enter the turn. It therefore proves a valid thread call, not official/project causal origin or per-thread yield. This
is unchanged from the accepted v5 review residual and is nonblocking because discovery projection remains unverified
and cannot authorize hydration, campaign admission, canonical writes, ranking, export, or outreach.

No new P0, P1, P2, or P3 finding was identified. The 240-code-point prompt target is risk reduction, not proof that a
future model will comply; any future over-limit output continues to fail closed under the unchanged 280 hard maximum.
This review authorizes only a fresh one-shot, no-fallback, separately bounded live retry. It does not claim live
success, source-bound native-X payloads, convergence, recall, precision, or product readiness.

GO
