# Post/Reply evidence and configurable candidate-gate pinned review

## Evidence header

- Reviewer: independent non-author subagent; adversarial and read-only against pinned Git objects.
- Base: `b54ef9c735612c228a0b803a892be0f6ba7b64d0`.
- Reviewed intermediate X-first commit: `978eca0bfeaaf30f5a93425639796c1f31cf435c`.
- Reviewed head: `1d3e2ebaaa825878bd2cecb3514212c81be24acf`.
- Ancestry: the base is an ancestor of the intermediate commit, and the reviewed head's sole parent is the intermediate
  commit. The full workspace range also contains two unrelated `sourcing-ai-agent` commits; they were excluded by
  reviewing only the `x-first-researcher-sourcing/` subtree.
- Reviewed X-first scoped binary-diff SHA-256:
  `42f6b76c70ace9c19d72d844168abe2f343615507ca888795103eee7f4e72c34` (`17 files`, `3,209 insertions`, `182 deletions`).
- Detached clean worktree: `/private/tmp/x-first-post-reply-review-1d3e2eb/x-first-researcher-sourcing`.
- The current main working-tree Campaign diff was not read or assessed. The only main-worktree write was this review
  artifact.
- Visible operator config at review time: top-level `model=gpt-5.6-sol`, `model_reasoning_effort=medium`,
  `service_tier=priority`; the visible `openai` profile records `gpt-5.6-sol / ultra / priority`. This is config
  visibility, not independently hash-verified effective-rollout evidence for the subagent.
- No Grok/X/model/network call, credential read, provider execution, staging, commit, or product write occurred.

## Validation

- Python `3.12.13` targeted adaptive/exploration suites: `87/87` passed in `16.030s`.
- Python `3.12.13` complete X-first suite: `331/331` passed in `60.833s`.
- Ruff `0.15.11` whole pinned X-first tree: passed.
- `PYTHONPATH=src ... -m x_first.contracts`: valid; `errors=[]`, synthetic fixture precision/recall both `1.0`.
- All seven new/changed JSON policy/schema files parsed; `git diff --check` passed; detached status remained clean.
- The three Google DeepMind prompt bytes hash to their exact policy rows, and their entry-scoped effective-policy
  digests were recomputed from the pinned runtime.
- Mutation probes covered coherent receipt coverage tampering versus bundle replay and an unconstrained Boolean-query
  branch. The former failed closed at bundle replay; the latter reproduced P2-1 below.

Severity totals: `P0=0`, `P1=0`, `P2=2`.

## Findings

### P2-1 `[new]` — single-`from:` counting does not prove every Boolean branch is handle-scoped

`src/x_first/native_x_evidence_contract.py:85-111` counts a query as one candidate's authored Post/Reply attempt when
there is exactly one positive `from:<handle>` and a consistent reply filter, but it does not parse Boolean scope. For
example, each of the following was credited to `targetperson/authored_reply`:

```text
from:TargetPerson pretraining filter:replies OR pretraining
(from:TargetPerson pretraining filter:replies) OR pretraining
pretraining OR from:TargetPerson pretraining filter:replies
```

The unconstrained branch can return unrelated authors, so the receipt may overstate that the complete query was a
candidate-scoped attempt. This does not upgrade evidence, prove exhaustive results, or authorize promotion; the exact
tool arguments and query hash remain replay-bound. It therefore does not block one controlled discovery wave, but the
coverage field must not become a hard quality/KPI gate until a closed grammar or branch-scope validator rejects this
shape. Add the mutations above as regressions.

### P2-2 `[new]` — the canonical evaluation contract still names v1 as the configured/default owner

The runtime defaults are v2 (`src/x_first/grok_cli_exploration.py:27-43`) and binding validation deliberately accepts
both v2 and retained v1 artifacts (`:941-959`, `:2641-2653`). However,
`docs/X_FIRST_EVALUATION_CONTRACT.md:246-250` still says persisted evaluation/hydration output is closed by the v1
schemas, and `:334-340` still names `candidate_value_segment_policy.v1.json` as the configured owner. `README.md:136`
also describes only “Evaluation/hydration v1.” Update these source-of-truth statements to say v2 is the default and v1
is replay-only. The mismatch is non-blocking here because schema versions and policy hashes are runtime-bound, v1
replay is tested, and this offline evaluator is not a live adaptive-wave consumer.

## Live adaptive / Google DeepMind scope

The pinned implementation is sufficient for one operator-triggered, controlled Google DeepMind adaptive wave1:

- result v2 and operator receipt v3 schema/runtime shapes reconcile; typed support claims bind both dimension and
  asserted temporal value;
- Bio evidence requires `thread_relation=null`; every non-Bio row requires `self_post|reply|quote|thread_root|thread_reply`;
- exact candidate subject, author, Post ID and URL bindings are validated, while model-mediated Post bodies remain
  explicitly non-replayable;
- completed keyword-query attempts are derived from the retained transcript, projected into separate authored-Post and
  authored-Reply coverage, and recomputed during bundle replay; coherently editing receipt copies alone fails;
- retained result-v1 prior waves still load without inventing temporal values, while newly emitted supports are typed;
- the three prompts contain complementary broad-team, historical/publication/project, and residual Reply/thread
  strategies. They impose no business candidate, observation, answer-length, or native-X call cap; only the reviewed
  technical deadline/turn/budget/session ceilings apply;
- the exact effective target is `google_deepmind / pretraining / Public professional evidence of current or historical
  Google DeepMind affiliation and current or historical pre-training or base-model training relevance.`;
- prompt SHA-256 values are wave1
  `b8073ded1675dbb96801caa36247f8cbe152d921b02dd7a18a2824cb4099d544`, wave2
  `a195728be9c6ed239d3e9912417513634c31d8ab5b01d635f541622f40cfd677`, and wave3
  `0ff2d205592dccebdd2a5ccaccf73296bc7702cc299c46d1020ed07b18a22ab9`;
- their recomputed entry-scoped effective-policy digests are respectively
  `5bf4326ecab5b694c69f3b27213e071a60aaed29fc1cb3cbedf0fcd1e58476cb`,
  `989f8f4877926224e5252a5c3cb9c324d90b73618aab4ec6cea2838a9e3a4e66`, and
  `7077bdd90ee57b680a230e06c1d7e7b9d045e5cb2c4609dab03345d70bd455ce`;
- wrong target/prompt combinations fail before execution; OAuth isolation/deletion, single-use grant expiry, bounded
  process-group cleanup, session-tree validation, and bundle tamper replay remain fail-closed.

`[residual closed]` The earlier effective-prompt authority, post-link grant-expiry, OAuth cleanup, session-tree ceiling,
and same-path bundle-swap concerns now have pinned regression coverage and passed the full replay suite.

This clearance applies to wave1 using the exact reviewed target and prompt through a fresh request-scoped grant. It is
not an automatic instruction to chain wave2/wave3, and it does not establish source-bound X payloads, candidate truth,
exhaustion, product promotion, canonical identity, ranking, outreach, or campaign admission.

## Offline candidate-gate v2 scope

The candidate-gate implementation is internally closed as an offline, model-mediated diagnostic:

- all four `current|historical` lab × pretraining combinations remain Recall-eligible; only evidence-complete,
  high-confidence `current/current` is Precision-eligible;
- a missing Bio can satisfy the v2 affiliation-profile gate only through high-authority target-lab evidence from
  `self|official_lab|colleague_or_team`; an ordinary third-party mention fails closed;
- historical states do not spuriously force hydration, while ambiguous/unsupported dimensions, missing stable ID, or
  missing high-authority support do;
- evaluation v1, hydration-task v1, and mandatory-Bio v1 semantics replay under their original policy; v2 is the new
  default without rewriting retained artifacts.

This gate does not consume adaptive result v2. The existing adaptive-to-campaign bridge intentionally emits a blocked
artifact because native-X response payload bytes are not retained. That is a verified authority boundary, not a gap
papered over by this review: live adaptive output remains model-mediated research input and cannot be promoted into a
campaign `WaveInput` or treated as source-bound candidate evidence.

## Gate consequence

The two P2 items should be fixed before authored-surface coverage becomes a hard performance metric or the candidate
gate documentation is declared closed. They do not block the exact, controlled Google DeepMind adaptive wave1 because
neither field can upgrade evidence or cross the fail-closed campaign boundary. All provider execution must still use
the reviewed prompt bytes, target tuple, preissued single-use grant, private retention/deletion lifecycle, zero
fallback, and the existing emergency kill path.

GO
