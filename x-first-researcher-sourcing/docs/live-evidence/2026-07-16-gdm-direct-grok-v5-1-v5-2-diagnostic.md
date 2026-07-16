# Google DeepMind Direct Grok Diagnostic, v5.1/v5.2

Date: 2026-07-16

Status: diagnostic only. This is not a formal live-gate artifact and not a candidate handoff.

## Scope

Goal: test whether Grok CLI can use native X search at a useful scale for a large lab recall wave, and whether the
current prompt uses Posts, Replies, mentions, and thread evidence rather than over-indexing on Bio.

Target: public professional evidence for current or historical Google DeepMind/DeepMind affiliation and current,
historical, adjacent, or ambiguous pretraining/base-model-training relevance.

Raw stdout/stderr and receipts are owner-only private artifacts under:

- `/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm-v5-1-20260716T1030Z/`
- `/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm-v5-2-20260716T1040Z/`

No candidate handles, profile text, Post text, or raw evidence are reproduced here.

## Preceding runner result

The reviewed adaptive runner request
`xwave_req_cb382501c0c2791386705c7cb7985ce4` was issued and consumed, but the live process had to be recovered:

- run id: `grok_wave_live_6ed0c7f913994f1f8db190a9bdf10896`
- recovery status: `crash_recovered`
- bundle validator: `[]`
- stdout/stderr: 0 bytes
- process spawned: true
- session proof: missing
- auth digest: tainted with `post_consumption_execution_not_clean`

This does not prove the GDM search method failed. It proves the current reviewed runner path did not produce a
recoverable terminal session proof for that run. Grok CLI itself was separately smoke-tested successfully with:

- normal auth and strict JSON schema;
- isolated copied auth, leader socket, read-only sandbox, fixed session id, `--prompt-file`, `--verbatim`, and
  `--json-schema`;
- `ProcessGroupExecutor` capture around an equivalent minimal command.

## Direct diagnostic A/B

These two direct diagnostics bypassed the reviewed live-gate runner because the current auth digest was tainted after
the recovered run. They used Grok CLI with strict schema, no generic web search, read-only sandbox, no subagents, no
memory, no plan mode, and a 15-minute subprocess timeout. They are evidence for prompt and transport behavior only.

| Metric | v5.1 direct | v5.2 direct |
|---|---:|---:|
| Exit code | 0 | 0 |
| Elapsed ms | 199,513 | 190,740 |
| Wrapper `structuredOutput` | null | null |
| Wrapper error | trailing chars at col 516 | trailing chars at col 579 |
| Concatenated JSON objects in `text` | 6 | 2 |
| Last-object candidates | 23 | 15 |
| Last-object evidence rows | 46 | 35 |
| Observations reported | 320 | 412 |
| Native X queries | 45 | 53 |
| Tool counts | keyword 34, semantic 6, user 3, thread 3 | keyword 37, semantic 7, user 4, thread 5 |
| Evidence kind counts | bio 17, mention 8, post 21 | bio 11, mention 9, post 15 |
| Thread relation counts | self_post 22, reply 3, quote 4, null 17 | self_post 18, reply 2, quote 2, thread_root 1, thread_reply 1, null 11 |
| State pairs | current/current 6, current/ambiguous 4, current/historical 1, historical/historical 5, historical/ambiguous 7 | current/current 4, current/ambiguous 2, ambiguous/current 2, historical/historical 6, historical/ambiguous 1 |
| Excerpt max | 297 | 268 |
| Excerpts over 280 | 1 | 0 |
| Last-object validation | excerpt/support/reconciliation invalid | reconciliation invalid |

## Interpretation

Grok did use native X search broadly. The diagnostic result is not Bio-only:

- v5.1 evidence rows were 46% post, 17% mention, and 37% bio.
- v5.2 evidence rows were 43% post, 26% mention, and 31% bio.
- Both runs used keyword, semantic, user, and thread fetch families.
- Reply/quote/thread evidence appeared in both runs, but remains lower-volume than self-post and Bio evidence.

v5.2 improved format discipline but hurt recall:

- concatenated JSON objects dropped from 6 to 2;
- excerpt-over-280 dropped from 1 to 0;
- observations and query count increased;
- retained candidates dropped from 23 to 15.

Both prompts still failed strict terminal validity because Grok's headless wrapper placed concatenated JSON objects in
`text` while `structuredOutput` remained null. The last JSON object was useful for human diagnostic analysis, but it
must not be promoted as a formal runner result without transcript-bound terminal-message recovery.

The remaining model-side count drift is small and mechanical:

- v5.1 reported 48 evidence/post-url validations for 46 actual evidence rows.
- v5.2 reported 36 evidence/post-url validations for 35 actual evidence rows.

Prompt-only fixes are unlikely to be enough for production-grade strictness. The operator should mechanically derive
diagnostic counts from the final accepted object where those counts are non-authoritative telemetry.

## Method conclusions

1. Large-lab recall is feasible with Grok native X search. A single GDM wave found a nontrivial 15-23 candidate range
   with 35-46 evidence rows and hundreds of observations, without business caps.
2. Posts, Replies, mentions, and threads are necessary. Bio alone would miss much of the signal, including technical
   work that appears only in authored Posts or professional mentions.
3. Lab-affiliation temporality and pretraining-experience temporality should remain independent configured dimensions.
   The useful recall pool includes current/current, current/historical, current/ambiguous, historical/current,
   historical/historical, and historical/ambiguous combinations.
4. For strict automation, keep the reviewed runner path, but repair the live finalization/session-proof issue before
   treating direct Grok output as a gate artifact.
5. For prompt evolution, prefer v5.1-style recall pressure plus narrower mechanical output checks. The v5.2 terminal
   override reduced formatting errors but over-compressed the candidate pool.

## OpenAI scale check

An additional direct diagnostic used the prepared OpenAI v5.1 prompt:

- private receipt:
  `/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/openai-v5-1-20260716T1050Z/receipt.json`
- exit code: 0
- elapsed ms: 350,321
- stdout bytes: 94,116
- stderr bytes: 861
- wrapper `structuredOutput`: null
- wrapper error: trailing characters at line 1 column 510
- strict prefix JSON objects before parse failure: 4
- last strict prefix object: schema-valid `X_SEARCH_PARTIAL` with 0 candidates, 0 observations, and reported tool counts
  keyword 20, semantic 2, user 8, thread 0
- malformed suffix size: 82,958 bytes
- malformed suffix structural key counts: `handle` 43, `evidence` 36, X status/profile URL keys 67

No malformed suffix content is treated as accepted evidence. The useful signal is that OpenAI's larger search space
does appear to produce a much larger candidate-bearing body, but single-shot full candidate emission becomes
format-fragile.

## Updated workflow implication

For large labs, the next Grok strategy should split discovery from detail:

1. Discovery shard: request a compact, strict object containing only handles, source surface type, coarse state pair,
   and source URL/post id references. This maximizes recall and keeps the terminal JSON small.
2. Detail shard: run follow-up bounded batches over selected handles or source threads to collect evidence excerpts,
   Bio/location/link/category/profile fields, and typed support claims.
3. Merge shard: operator-side merge by handle/platform id, mechanical count projection, excerpt bounding, and duplicate
   evidence hashing.

This is still AI-native because Grok chooses and executes the native X search strategy, but the transport no longer
asks one model turn to both discover a large population and serialize every detail row perfectly.

## Compact discovery check

A compact OpenAI discovery shard tested that split directly. It used a smaller response schema with lead handles,
coarse state pairs, source kinds, source URLs, reason codes, query list, and tool counts, without full evidence
excerpts.

Private receipt:
`/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/openai-compact-discovery-20260716T1100Z/receipt.json`

Observed diagnostic metrics:

- exit code: 0
- elapsed ms: 115,872
- stdout bytes: 14,955
- wrapper `structuredOutput`: null
- wrapper error: trailing characters at line 1 column 178
- concatenated JSON objects in `text`: 3
- last object status: `X_DISCOVERY_OK`
- last object leads: 21
- last object queries: 27
- last object tool counts: keyword 28, semantic 4, user 22, thread 1
- last object source kind counts: post 20, mention 13, bio 16, official 2
- source URLs per lead: min 2, max 4
- state pairs: current/current 6, current/historical 2, current/ambiguous 4, historical/current 2,
  historical/historical 4, historical/ambiguous 3

This run was faster and much smaller than the full OpenAI evidence run while still returning a recall-useful lead pool.
It still emitted concatenated JSON in the wrapper `text`, so the formal runner must continue using transcript-bound
terminal extraction or an explicit final-object protocol. Locally, no `jsonschema` package was available, so this
compact check is not a formal schema-validation artifact.

## Next changes suggested

- Reuse the existing operator-side diagnostic-count projection after selecting a valid transcript-bound terminal
  object; do not let model-authored non-authoritative `local_reconciliation` counts decide promotion.
- Preserve the existing transcript-terminal recovery principle. Do not scan arbitrary concatenated headless `text` in
  a formal live lane unless the selected object is bound to the terminal assistant message after the last native-X tool.
- Promote the compact discovery response shape into a reviewed fixture/schema lane, then add a detail shard that
  hydrates selected handles/source threads into bounded evidence rows.
- Investigate why the reviewed runner produced `crash_recovered` with zero stdout/stderr while the same CLI and
  `ProcessGroupExecutor` smoke tests succeeded.
- Run the next large-lab experiment on OpenAI or another larger target only after either refreshing OAuth for a new
  reviewed runner grant or explicitly labeling the run as direct diagnostic.
