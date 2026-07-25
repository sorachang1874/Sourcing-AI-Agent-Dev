# Google DeepMind wave2-v5 official-discovery pinned review

## Evidence header

- Reviewer: independent non-author subagent `/root/gdm_v5_5005c75_pinned_review`.
- Reviewed head: `5005c75caacb12f9e59e901af67e31b1f08c3c28`.
- Logical X-First baseline: `e5260498cde6e2c98f9abc37e22727d9383f5914`.
- The commit's literal first parent is `2b160d19a105d9f3d805ca35894af3e103b6c778`; that parent contains two
  unrelated `sourcing-ai-agent` Track D commits after the logical X-First baseline. This review used only
  `e5260498..5005c75c -- x-first-researcher-sourcing` and did not inspect or review those unrelated files.
- Reviewed scope: exactly eight X-First files, `833 insertions`, `83 deletions`:
  `README.md`, the effective-prompt policy config and schema, the adaptive-runner contract, the GDM performance
  evidence note, the v5 prompt, the adaptive runner, and its test module.
- Exact binary-diff SHA-256:
  `007f230b179874fd00c6055dc659bfdd6bc68c47baa1e2e9b74debdc4e558100`.
- Reproduction tree: clean `git archive` export at
  `/private/tmp/xfirst-5005c75-review.QAArRG/x-first-researcher-sourcing`. Tests and probes imported only the pinned
  exported source. The ambient dirty working tree and unrelated `sourcing-ai-agent` files were not review inputs.
- No network, Grok/X, model, connector, credential mutation, grant issuance, provider execution, product write,
  staging, commit, or push was performed. The retained v4 bundle was read only for offline replay.
- This artifact is the reviewer's only repository write.

## Exact validation evidence

1. Object and scope verification

   ```text
   git cat-file -t 5005c75caacb12f9e59e901af67e31b1f08c3c28
   # commit

   git diff --binary e526049 5005c75caacb12f9e59e901af67e31b1f08c3c28 \
     -- x-first-researcher-sourcing | shasum -a 256
   # 007f230b179874fd00c6055dc659bfdd6bc68c47baa1e2e9b74debdc4e558100

   git diff --shortstat e526049 5005c75caacb12f9e59e901af67e31b1f08c3c28 \
     -- x-first-researcher-sourcing
   # 8 files changed, 833 insertions(+), 83 deletions(-)
   ```

2. Targeted adaptive-runner suite

   ```text
   PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v
   # Ran 72 tests in 18.099s — OK
   ```

3. Full standard-library suite

   ```text
   PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v
   # exit 0; the pinned loader enumerated and executed 361 tests
   ```

4. Contract preflight

   ```text
   PYTHONPATH=src .../.venv/bin/python -m x_first.contracts
   # status=valid, errors=[]
   # precision=1.0, recall=1.0, predicted/relevant=20/20, false_merge_count=0
   ```

5. Static checks

   ```text
   .../.venv/bin/python -m ruff check src tests scripts
   # All checks passed!

   git diff --check e526049 5005c75caacb12f9e59e901af67e31b1f08c3c28 \
     -- x-first-researcher-sourcing
   # exit 0, no output
   ```

6. Retained v4 bundle replay

   ```text
   validate_operator_bundle(
     runtime/adaptive-grok-waves/grok_wave_live_f41ad0d4206f4435baceb4bfe0b0e924
   )
   # []
   ```

7. Production v5 binding

   - Committed v5 prompt bytes SHA-256:
     `09991dbb9420309628f36794a79268a3ecb1aa67439f5e3d42169cb9795c8978`.
   - Selected entry:
     `google_deepmind_pretraining_recall_wave2_official_discovery.v5`.
   - Selected-entry semantic digest:
     `274d19281824fdcf973134e64f198f497eb9d88c9599d40a4abc92d5e2196616`.
   - Session policy:
     `discovery_only_official_accounts_no_person_hydration_v2`.
   - Recomputed session-policy semantics SHA-256:
     `26305c1bd1529e51bb35e9ce1533b64f09b2ed87f99981f14be5dde0718101fc`.
   - Entry-bound handles: `GoogleDeepMind`, `DeepMind`.

8. Replay-policy separation, recomputed from the retained v4 request

   | Route | SHA-256 |
   |---|---|
   | current result-v3 normalization v2 + artifact policy | `5484ca9387b6101dac23d6db13846bb962f199fcb7ea2942b385e30e1839a4ee` |
   | frozen normalization v1 + artifact policy | `93668af6950f1cd4e767b9f5fd577120ce6fb31073f3cc33f674b0b782de01d4` |
   | normalization-only result-v3 v1 | `7f01e9eafda1864a0e0d336f2b4394d301e65a08bb561997470bd8a28f9ba01a` |
   | pre-normalization result-v3 | `1918b833b46495926137bbe51bab97f604a7f046ec0426c406191b9c4ab253b5` |
   | legacy plain result-v2 | `78617aeb6daa98509acea0cb9e7f8e516f5c1aa7cb688cc63354328176f81975` |
   | legacy structured result-v2 | `1308d53a279d4baec9264726b34a5fc3ae06fc3a1cebf530cc088d0bdddc31aa` |

   All six values are distinct. Grant issuance uses only the current digest; receipt, bundle, grant-ledger replay, and
   crash recovery select the recorded route rather than reinterpreting old bytes under v2.

## Findings

Severity totals: `P0=0`, `P1=0`, `P2=1`.

### [P2][residual] Typed thread expansion does not prove why a thread ID/URL entered the discovery turn

`_session_query_phase_arguments_allowed` deliberately keeps typed `x_thread_fetch` expansion available. A `post_id`,
`tweet_id`, or structurally valid X status URL has no replayable causal link to an earlier official/project discovery
result, and the retained Grok transcript contains arguments but not native-X result bodies. Therefore the operator can
prove only that a well-formed thread fetch occurred; it cannot prove that the thread was an official launch/report
thread rather than a person-selected thread.

This is residual rather than new: the prior v4 pinned review explicitly treated typed thread expansion as a positive
control, and v5 does not widen its shape. It is nonblocking because discovery-only output is always projected to
`X_SEARCH_PARTIAL`, evidence stays `model_mediated_unverified`, and no hydration, campaign, product, identity, or
outreach authority follows from completion.

Precise follow-up: add one sentence to the contract's session-query residual boundary stating that typed thread causal
origin is unproven in transport v1, and add a test that preserves this wording/behavior. A future provider surface that
persists native-X result payloads could bind a thread ID to the call that returned it; until then, do not describe
thread origin or per-thread yield as mechanically verified.

No P0/P1 issue and no other P2 issue was identified in the pinned eight-file scope.

## Adversarial policy analysis

### Official-account `from:` boundary

The parser NFKC-normalizes the complete query, removes every Unicode `Cf` format character, counts all ASCII
case-insensitive `from:` markers, and separately requires exactly one boundary-complete operator/handle match. Under
the v2 policy it then requires:

- `x_keyword_search` only;
- exactly one marker and one parsed operator at the same offset;
- no ASCII/fullwidth-NFKC negation;
- a casefold match to the selected entry's valid, non-empty, casefold-unique handle list.

Five positive controls passed: ordinary and uppercase official handles, fullwidth `ｆｒｏｍ：` plus fullwidth handle,
an embedded zero-width format character, and a parenthesized official operator. Eleven negative controls failed:
ASCII/fullwidth/format-separated negation, unknown/candidate and suffix-extended handles, word-prefix embedding,
ASCII/fullwidth multiple operators, empty allowlist behavior, and `from:` use in semantic, user, or thread query tools.
The old discovery-only policy continued to reject official `from:` queries.

Quoted/Boolean multiword keyword semantics are not treated as proof of provider-side result restriction. They remain
within the already documented multiword keyword/semantic intent residual; completion proves the retained argument and
lexical policy, not how X parsed or yielded that query.

### Effective-policy/grant/replay binding

The loader accepts only three closed row shapes, requires the policy ID/semantics digest pair, requires official
handles only for v2, rejects them for every other policy, validates X handle grammar, and rejects casefold duplicates.
The exact selected row—including official handles—is included in the selected-entry digest. That digest and entry ID
are checked at grant issue/consumption, command binding, intent, receipt, offline bundle replay, and crash recovery.
Appending an unrelated valid row does not invalidate an old selected entry; changing the selected row does.

### Mechanical normalization v2

`_canonicalize_native_x_rfc2822_timestamp` accepts only a timezone-aware UTC value whose `format_datetime(...,
usegmt=True)` output reproduces the input byte-for-byte. The probes rejected a wrong weekday, `+0000`, `UTC`, missing
seconds, invalid calendar date, leading whitespace, and lowercase weekday. Exact
`Wed, 01 Jul 2026 00:00:00 GMT` converted to `2026-07-01T00:00:00Z`.

Normalization operates on a deep JSON copy, retains raw stdout unchanged, appends deterministic audit text, and
returns the transformed copy only when complete runtime validation succeeds. Duplicate handles/evidence, malformed
Bio/status topology, invalid support bindings, and incompatible rows therefore keep the original diagnostic and fail
closed; no merge or semantic relabel is attempted.

The pre-existing result-v3 validator accepts parseable UTC ISO strings ending in `Z`, including some ISO spellings
that are not the one canonical display form. This change intentionally did not tighten that replay surface. The v2
claim is narrower and correct: an admitted RFC `GMT` conversion itself emits the canonical `YYYY-MM-DDTHH:MM:SSZ`
form. Any future requirement that *all* already-ISO inputs be rewritten or rejected needs a new versioned result policy
and legacy replay route; it must not be inferred from this review.

### Prompt/performance contract

The v5 prompt is discovery-first and Post/Reply-aware rather than Bio-led. It requires organization/era, official and
professional graph, project/model, training-function, keyword/semantic/user/thread, Top/Latest, historical-shard, and
four distinct positive-Reply strategy cells. It requires casefold deduplication, Bio-versus-status topology checks,
timestamp conversion, typed support, and final count reconciliation before one terminal JSON.

It contains no candidate, observation, query, answer-length, or native-X-call business cap. The three zero-yield
expansions are explicitly a model-side stopping heuristic; the operator does not promote them to mechanically proven
population convergence. Deadline, token/cost, JSON/session, and process ceilings remain technical emergency limits.

## Closure and residual ledger

| Boundary | Review state |
|---|---|
| Official `from:` false rejection seen in v4 | closed by entry-bound keyword-only exception |
| Candidate/unknown/negated/multiple/non-keyword `from:` | closed by normalized transcript replay |
| Selected official handle authority | closed by selected-entry digest through grant/intent/receipt/replay/recovery |
| Exact native-X IMF-fixdate conversion | closed by strict byte round-trip and atomic admission |
| Duplicate and malformed Bio/status repair | intentionally fail-closed; prompt preflight only |
| Old result-policy meaning | closed by six distinct replay routes; retained v4 bundle returns `[]` |
| Thread-fetch causal origin | P2 residual; typed call proven, origin/yield unproven |
| Multiword keyword/semantic person intent | accepted existing audit residual |
| Native-X result bodies and per-query marginal yield | unavailable in transport v1; model-mediated |
| Discovery convergence | unproven; projection remains `X_SEARCH_PARTIAL` |
| Live performance/success | not established by this offline review |

## Gate consequence

The pinned implementation closes the v4 official-account rejection and RFC timestamp failure without weakening
duplicate/topology fail-closed behavior or rewriting retained artifacts. The one P2 residual does not create downstream
authority and does not block the separately authorized, fresh one-shot, no-fallback v5 Google DeepMind live experiment.
This verdict does not claim live success, population convergence, source-bound X payloads, hydration readiness,
campaign admission, product promotion, identity resolution, canonical writes, ranking, export, or outreach authority.

GO
