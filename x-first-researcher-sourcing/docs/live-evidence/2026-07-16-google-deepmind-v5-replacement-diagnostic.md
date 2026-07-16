# Google DeepMind v5 replacement live diagnostic — 2026-07-16

> Author diagnostic evidence. This is not an independent-review artifact, a valid campaign result, a precision/recall
> measurement, or a product/canonical write authorization. Candidate handles and evidence text remain private.

## Decision

The reviewed v5 discovery strategy materially improved breadth and call efficiency over the earlier v4 diagnostic,
but this run is still `result_contract_invalid`. Its terminal result had exactly one invalid value: one of 37 evidence
excerpts contained 319 Unicode code points while the authoritative result-v3 limit is 280. The whole candidate result
therefore remains excluded from campaign import and formal quality/performance claims.

The failure is narrow enough to justify an append-only prompt successor. The v5 prompt and registry rows remain
unchanged for replay. New Google DeepMind and OpenAI v5.1 prompt rows add a 240-code-point operational target, retain
the 280-code-point hard acceptance boundary, and require a final all-excerpt audit. The operator does not truncate or
rewrite model evidence. A new paid run still requires a pinned non-author review `GO`, a new request, and a new
one-shot grant.

## Exact run evidence

| Field | Observed value |
| --- | --- |
| run | `grok_wave_live_4324e8bce20c4e04b1a3b6e8a1a69418` |
| request | `xwave_req_357a89caa777a4a1fe6d2f80107bd0f5` |
| selected entry | `google_deepmind_pretraining_recall_wave2_official_discovery.v5` |
| source prompt SHA-256 | `09991dbb9420309628f36794a79268a3ecb1aa67439f5e3d42169cb9795c8978` |
| status | `result_contract_invalid` |
| provider process | exit `0`, `166,160 ms`, stderr empty |
| emergency behavior | no timeout, fallback, TERM/KILL, or technical-limit transition |
| native-X calls | `43/43` completed: keyword `29`, semantic `6`, user `4`, thread `4` |
| provider trace | `95` events, one model turn |
| tokens | input `150,242`, cache-read `768,256`, output `18,178`, total `936,676` |
| conservative estimated cost | `$1.946064` |
| terminal diagnostic payload | `20` candidates, `37` evidence rows, `295` observations reported |
| exact validator error | `evidence_value_invalid:7:1` |
| excerpt profile | min `38`, max `319`, `1/37` over 280, `6/37` over 240 |
| retained bundle replay | `validate_operator_bundle(...) == []` |
| cleanup | process group gone, ephemeral home deleted, private 24-hour purge pending |

The Grok headless `text` contained six interim structured messages followed by the transcript-proven terminal
assistant result. This is an already-supported Grok CLI 0.2.101 transport behavior: the operator did not scan the
concatenation for a convenient object, and instead selected only the contiguous terminal assistant message after the
last completed native-X tool. The terminal object then failed the normal result validator on the single excerpt.

## Data-quality profile

The intended grains are one case-insensitive candidate handle per candidate row and one source-bound professional
evidence proposal per evidence row. The terminal result met the duplicate-handle, evidence-shape, URL/author/Post,
timestamp, topology, typed-support, temporal-state, and current-prior rules sufficiently for the validator to emit no
other error.

| Dimension | Aggregate diagnostic |
| --- | ---: |
| Evidence kind | Bio `11`, mention `11`, Post `15` |
| Relationship | self `26`, third-party `10`, colleague/team `1` |
| Post/Reply-like evidence | `26/37`; 19 of 20 candidates had at least one Post-like row |
| Reply-like topology | direct reply `1`, thread reply `1` |
| Confidence | high `8`, medium `10`, low `2` |
| Current lab / current pretraining | `4` |
| Current lab / historical pretraining | `2` |
| Current lab / ambiguous pretraining | `3` |
| Historical lab / current pretraining | `1` |
| Historical lab / historical pretraining | `7` |
| Historical lab / ambiguous pretraining | `3` |

This confirms that discovery was not Bio-led and that keeping lab affiliation and pretraining temporality as
independent axes recovers valuable historical/current and historical/historical combinations. It does not verify the
truth of model-mediated candidate facts because Grok CLI 0.2.101 does not retain native-X result bodies.

## Query-strategy coverage

The trace contained 43 unique tool-argument digests and 39 query-bearing calls. Keyword search used `Latest=21` and
`Top=8`; four queries carried explicit time shards; five used positive `filter:replies`; five used the entry-bound
official `from:GoogleDeepMind` surface; and four attributable threads were fetched. No generic web tool, fallback, or
candidate-scoped person-hydration surface was admitted.

These facts prove attempted strategy coverage, not per-query yield or population convergence. The current CLI trace
does not bind each returned Post/user to its originating query, so the run cannot support true recall, precision, or
zero-marginal-yield claims.

## v4-to-v5 diagnostic comparison

Both rows are invalid diagnostics, so this table guides method selection only.

| Metric | v4 | v5 | Change |
| --- | ---: | ---: | ---: |
| Unique candidate handles | `13` | `20` | `+53.8%` |
| Evidence rows | `20` | `37` | `+85.0%` |
| Evidence rows per call | `0.571` | `0.860` | `+50.6%` |
| Native-X calls | `35` | `43` | `+22.9%` |
| Positive Reply queries | `1` | `5` | `+400.0%` |
| Elapsed seconds | `111.982` | `166.160` | `+48.4%` |
| Estimated cost | `$1.239336` | `$1.946064` | `+57.0%` |
| Candidates per call | `0.371` | `0.465` | `+25.2%` |
| Calls per candidate | `2.692` | `2.150` | `-20.1%` |
| Seconds per candidate | `8.614` | `8.308` | `-3.6%` |
| Cost per candidate | `$0.095334` | `$0.097303` | `+2.1%` |
| Cost per call | `$0.035410` | `$0.045257` | `+27.8%` |

The v5 strategy is the better discovery challenger: it expanded Reply, official-account, semantic, historical, and
thread surfaces while improving candidates per call and holding cost per candidate nearly flat. The next experiment
should preserve this query strategy and change only terminal excerpt preflight. It should not introduce a candidate,
observation, or native-X-call business cap.

## OAuth and next-run eligibility

The one-shot grant is consumed and cannot be reused. Grant and consumption bindings match the receipt. After clean
provider exit, the canonical OAuth digest remained unchanged at
`7c12a9255d0de8cb0dee028b39181358b3754a1553063d18be433c7b67746118`, mode `0600`; neither an active-use claim nor a
taint marker exists. Its selected OIDC row expires at `2026-07-16T05:43:18.021623Z`.

The OAuth/process lifecycle therefore permits a later live run while the freshness horizon remains sufficient. It
does not authorize one by itself: the v5.1 prompt SHA and registry row need pinned non-author `GO`, and every retry
needs a fresh request ID, grant ID, request artifact, and one-shot grant.
