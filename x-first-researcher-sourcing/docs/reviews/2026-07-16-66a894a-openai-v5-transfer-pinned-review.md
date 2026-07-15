# OpenAI zero-prior discovery v5 transfer pinned review

## Evidence header

- Reviewer: independent non-author subagent `/root/openai_v5_66a894a_pinned_review`.
- Reviewed head: `66a894a4d1f77c0a55f69d5274bbd755ad57c34b`.
- Reviewed base and literal first parent: `d0878ce64c8485b6c7479571681fd342478a85f7`.
- Reviewed scope: exactly four X-First files, `278 insertions`, `19 deletions`: the effective-prompt policy registry,
  the OpenAI zero-prior transfer note, the OpenAI wave0-v5 prompt, and the adaptive-runner test module.
- Exact binary-diff SHA-256:
  `a78cbe8ba09302d8afb9aaaa0d6d014a24278d01b8c66fb3521fdf4243332adc`.
- Reproduction tree: clean detached worktree
  `/private/tmp/x-first-openai-v5-review-66a894a` at the reviewed head. Review reads and validation used only pinned Git
  objects and that clean tree; the ambient working tree was not a review input.
- No network, Grok/X, model, connector, real OAuth, provider, credential-backed/provider-capable grant, product write,
  staging, commit, or push was performed. The two adversarial probes used only temporary synthetic files and the
  test-owned fake binary/auth material; one deliberately exercised the offline grant-construction function. This
  artifact is the reviewer's only repository write.

## Exact validation evidence

1. Object, ancestry, scope, and digest

   ```text
   git rev-parse 66a894a4d1f77c0a55f69d5274bbd755ad57c34b^
   # d0878ce64c8485b6c7479571681fd342478a85f7

   git diff --binary d0878ce 66a894a4d1f77c0a55f69d5274bbd755ad57c34b | shasum -a 256
   # a78cbe8ba09302d8afb9aaaa0d6d014a24278d01b8c66fb3521fdf4243332adc

   git diff --shortstat d0878ce 66a894a4d1f77c0a55f69d5274bbd755ad57c34b
   # 4 files changed, 278 insertions(+), 19 deletions(-)
   ```

2. Targeted adaptive-runner suite

   ```text
   PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v
   # Ran 75 tests in 12.708s — OK
   ```

3. Full standard-library suite

   ```text
   PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v
   # Ran 364 tests in 35.378s — OK
   ```

4. Contract and static checks

   ```text
   PYTHONPATH=src .../.venv/bin/python -m x_first.contracts
   # status=valid, errors=[]
   # precision=1.0, recall=1.0, predicted/relevant=20/20, false_merge_count=0

   .../.venv/bin/ruff check src tests scripts
   # All checks passed!

   python -m json.tool configs/adaptive_grok_wave_effective_prompt_policy.v1.json
   # exit 0

   git diff --check d0878ce 66a894a4d1f77c0a55f69d5274bbd755ad57c34b
   # exit 0, no output
   ```

5. Production OpenAI binding

   - Prompt bytes SHA-256:
     `d6717a385cd2d5d7309f0e4a0abccb6dbcf03f67d0aef2b79e8017364478cbe3`.
   - Selected entry: `openai_pretraining_zero_prior_official_discovery.v5`.
   - Selected-entry semantic digest:
     `32eaa48be05ebc45db32a91ce9b52417ca7c59a1467692f8996a48e07da26c7b`.
   - Target: `lab_id=openai`, `research_focus_id=pretraining`, with independent current/historical lab and
     pretraining relevance scope.
   - Session policy: `discovery_only_official_accounts_no_person_hydration_v2`.
   - Recomputed session-policy semantics SHA-256:
     `26305c1bd1529e51bb35e9ce1533b64f09b2ed87f99981f14be5dde0718101fc`.
   - Entry-bound official handles: exactly `OpenAI`.

6. Adversarial zero-prior probe

   A temporary otherwise-valid OpenAI request was given one private SHA-bound prior wave containing handle
   `KnownPrior`. The production policy still issued a synthetic preissued grant and selected the zero-prior entry;
   `load_prior_context` returned `KnownPrior`, and `compile_prompt` embedded `["KnownPrior"]` in the operator-owned
   exclusion block:

   ```text
   entry openai_pretraining_zero_prior_official_discovery.v5
   prior_handles ['KnownPrior']
   compiled_contains_prior True
   ```

7. Adversarial OpenAI query-policy probe

   Each term explicitly offered by the prompt's broad `x_user_search` clause was tested through the selected v2
   session policy with target `openai` and official allowlist `OpenAI`. Thirteen terms passed; `data` alone failed.
   Representative controls:

   ```text
   x_user_search  OpenAI researcher                    True
   x_user_search  OpenAI data engineer                 False
   x_user_search  OpenAI distributed training engineer False
   x_keyword_search from:OpenAI pretraining filter:replies True
   ```

## Findings

Severity totals: `P0=0`, `P1=2`, `P2=3`.

### [P1][new] The selected zero-prior entry authorizes non-empty prior waves

The transfer note says the experiment input is `prior_waves=[]`, and the model prompt forbids importing or excluding
prior handles. That is not an effective-policy or grant invariant. The new registry row has target, prompt, session
policy, and official-handle fields but no prior-input policy. `_approved_effective_prompt_binding` selects a row using
only target and prompt digests; generic request validation accepts any well-shaped `prior_waves` array. During
execution, `load_prior_context` reads those prior candidates and `compile_prompt` injects their handles as exclusions.

The adversarial probe proves the consequence: a request containing `KnownPrior` received the zero-prior entry's
preissued grant and would expose that handle to the model. The request-scope digest makes such a grant self-consistent;
it does not make the request zero-prior. Reusing the old 98-handle union would suppress rediscovery and invalidate the
claimed independent frozen-union comparison while every existing new test remained green.

Required closure: make the prior-input rule entry-owned and digest-bound, for example
`prior_input_policy_id=require_empty_prior_waves_v1`; validate it in the policy schema/loader, selected-entry binding,
grant issuance, execution, replay, and recovery. Add a negative production-policy test proving that this exact entry
cannot issue a grant when `prior_waves` is non-empty, plus a positive empty-prior test. Do not rely on the future
operator constructing the request correctly.

### [P1][new] The OpenAI prompt authorizes an `x_user_search` term that the selected session policy rejects

The OpenAI prompt calls the user-search grammar closed, then explicitly offers `data` as a valid professional term.
The frozen v2 professional allowlist does not contain `data`, and the runtime requires every user-query token to be a
subset of target, professional, and connector terms. Consequently a natural prompt-compliant call such as
`x_user_search("OpenAI data engineer")` is rejected during session replay. The live provider work and cost would have
already occurred before the operator turns the entire run into a query-policy failure.

The new shared assertion helper checks only that broad wording fragments exist; it never executes the OpenAI prompt's
enumerated user-search vocabulary through the selected target/policy/allowlist. This is why all 75 targeted tests pass.

Required closure: choose one canonical vocabulary. Either remove/disallow `data` in the user-search clause or add it
to the versioned runtime policy and update every affected policy digest and replay contract. Add an OpenAI-specific,
table-driven compatibility test for every enumerated user-search term. Also state that the coverage-matrix literals
`OpenAI` and `@OpenAI` must be combined with professional terms: both bare forms are mechanically rejected while
`OpenAI pretraining` and `@OpenAI pretraining` pass.

### [P2][new] The model-facing zero-prior prompt discloses the historical denominator

The transfer note says the frozen union is withheld from the prompt, but prompt lines 13–15 tell the model that an
earlier OpenAI union contains 98 handles. No historical handle identity is present—the scan found only the official
`OpenAI` handle—so this is not direct candidate seeding. It is nevertheless unnecessary prior-campaign metadata that
can anchor answer length or search effort in an experiment described as zero-prior.

Keep the union size, denominator, and frozen-union conditional-coverage formula in the operator-side post-hoc note
only. The model needs only the no-prior/no-quota instruction. The operator-side label itself is appropriately
conditional: the 98 denominator is not a complete/adjudicated population, overlap is not called recall, and outside
leads are not called precision.

### [P2][new] Registry coverage tests are hard-coded to a two-lab total

The production-owner test hard-codes OpenAI `8`, Google DeepMind `7`, and total live entries `15`. Adding a correctly
registered Anthropic, xAI, Meta, or other large-lab transfer will fail this OpenAI/GDM test and require another manual
count edit. The digest-set checks per known family are useful, but the global literal makes the test an extension
choke point rather than a data-driven registry invariant.

Use an explicit lab-to-prompt-family registry/table and derive counts and target checks from it, while retaining
per-entry digest uniqueness and orphan-prompt detection. This is nonblocking for the current OpenAI experiment but is
directly contrary to the stated next-lab scalability goal.

### [P2][residual] Thread-fetch causal origin remains unproven

The transferred prompt asks for official or attributable thread expansion, while the retained transport proves only
the typed `x_thread_fetch` argument. It does not retain native-X result bodies that causally bind the fetched thread ID
to a preceding official/project search result. This is the same accepted v5 transport residual, not a new widening.
It remains nonblocking only while output is `X_SEARCH_PARTIAL`, evidence stays model-mediated/unverified, and no
source-bound origin or per-thread yield is claimed.

## Confirmed non-findings

- Registry digest, exact OpenAI target, selected-entry digest, v2 session-policy digest, and the single `OpenAI`
  official allowlist reconcile.
- The prompt is Posts/Replies/mentions/thread-led rather than Bio-first. It requires four positive Reply cells,
  Top/Latest, current/recent/older time shards, semantic/user diversity, and typed temporal evidence.
- Lab-affiliation temporality and pretraining temporality remain independent, including historical combinations.
- No candidate, observation, query, native-X-call, or answer-length business cap was introduced; deadline and resource
  ceilings remain external technical controls.
- The prompt contains no individual from the historical 98-handle union and preserves the protected-identity
  boundary.
- The operator-side frozen-union comparison is correctly labeled conditional coverage rather than population recall;
  novel leads are not described as proof of precision.
- The four-file commit does not call a provider, issue a real grant, read OAuth state, or claim live success.

## Gate consequence

The two P1 findings block a provider-costing OpenAI wave under this pinned scope. A live run could be silently
contaminated by prior exclusions or could consume provider cost and then fail session-policy replay for a query the
prompt itself authorized. Offline fixture work and unrelated implementation may continue. Promotion requires a new
pinned implementation that mechanically enforces empty prior input and reconciles the model-facing user-search
vocabulary with the runtime policy, followed by a fresh non-author review. This artifact does not assess live recall,
precision, population convergence, source-bound X payloads, hydration readiness, product promotion, canonical writes,
ranking, export, or outreach authority.

NO-GO
