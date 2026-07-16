# Google DeepMind v5.1 excerpt-preflight live diagnostic — 2026-07-16

> Author diagnostic evidence. This is not an independent-review artifact, a valid campaign result, a precision/recall
> measurement, or product/canonical write authorization. Candidate handles and evidence text remain private.

## Decision

The exact v5.1 single-variable retry completed its native-X work normally, but prompt-only excerpt preflight was not
reliable enough to make the terminal transport valid. Grok completed `40/40` native-X calls and returned a terminal
diagnostic with `17` candidates and `32` evidence rows. The only validator error was one `297`-code-point excerpt
against the result-v3 hard maximum of `280`; the run therefore correctly remained `result_contract_invalid`.

The next bounded change preserves the v5.1 query strategy and result-v3 schema. A versioned operator normalization v3
may replace only a `281..560`-code-point, strict-UTF-8 model-reported excerpt with its exact first `280` code points.
It does not strip, normalize, paraphrase, insert an ellipsis, select a keyword window, change typed supports, or alter
candidate state/confidence/source identity. The full model output remains hash-bound in private `raw.stdout`; affected
candidates and the result receive deterministic audit text stating that prefix semantic completeness is not guaranteed
and linked-X-source review is still required. The complete transformed result and technical envelope must validate
atomically, so collisions or any other error retain the original invalid result.

This repair is a display/transport projection for `model_mediated_unverified` evidence, not source verification. A
new live run still requires a pinned non-author review `GO`, a new request, and a fresh one-shot grant.

## Exact run evidence

| Field | Observed value |
| --- | --- |
| run | `grok_wave_live_2ad660aa20e34e93a92ef7c247ac8417` |
| request | `xwave_req_72895d35e75b494d160b6e08a6d6ff61` |
| selected entry | `google_deepmind_pretraining_recall_wave2_official_discovery_excerpt_preflight.v5_1` |
| source prompt SHA-256 | `1ba6e53538ac851f1f160a80e63aec3787f31ce7c2711541450800a3cd2836d5` |
| status | `result_contract_invalid` |
| provider process | exit `0`, `167,567 ms`, stderr empty |
| emergency behavior | no timeout, fallback, TERM/KILL, or technical-limit transition |
| native-X calls | `40/40`: keyword `26`, semantic `5`, user `4`, thread `5` |
| provider trace | `89` events, one model turn |
| tokens | input `131,913`, cache-read `495,104`, output `20,100`, total `647,117` |
| conservative estimated cost | `$1.374634` |
| terminal diagnostic payload | `17` candidates, `32` evidence rows, `132` observations reported |
| exact validator error | `evidence_value_invalid:13:1` |
| excerpt profile | min `43`, max `297`, `1/32` over `280`, `3/32` over `240` |
| retained bundle replay | `validate_operator_bundle(...) == []` under its sealed v2 policy |
| cleanup | process group gone, ephemeral home deleted, private 24-hour purge pending |

The Grok headless `text` again contained multiple interim structured messages followed by the transcript-proven
terminal assistant result. `structuredOutput` was null because the outer text contained trailing documents; the
runner selected only the contiguous terminal assistant message after the final completed native-X tool. The terminal
object then failed solely on the one over-limit excerpt.

## Evidence and temporal coverage

| Dimension | Terminal diagnostic |
| --- | ---: |
| Evidence kind | Bio `10`, mention `7`, Post `15` |
| Relationship | self `25`, third-party `5`, colleague/team `2` |
| Topology | Bio/null `10`, self Post `14`, quote `2`, direct reply `1`, thread reply `2`, thread root `3` |
| Confidence | high `4`, medium `11`, low `2` |
| Current lab / current pretraining | `3` |
| Current lab / ambiguous pretraining | `2` |
| Historical lab / current pretraining | `1` |
| Historical lab / historical pretraining | `7` |
| Historical lab / ambiguous pretraining | `4` |

The terminal result was not Bio-first: `22/32` evidence rows were Post/mention rows, including Reply/thread topology.
This continues to support independent lab-affiliation and pretraining-temporality axes, including valuable historical
lab plus current/historical pretraining combinations. It does not establish truth, precision, recall, or source-bound
Post content because native-X result bodies remain unavailable for replay.

## Prompt-only comparison

Both rows are invalid diagnostics, so the table evaluates transport/query behavior rather than business quality.

| Metric | v5 | v5.1 | Direction |
| --- | ---: | ---: | ---: |
| Candidates | `20` | `17` | `-15.0%` |
| Evidence rows | `37` | `32` | `-13.5%` |
| Native-X calls | `43` | `40` | `-7.0%` |
| Candidates per call | `0.465` | `0.425` | `-8.6%` |
| Evidence per call | `0.860` | `0.800` | `-7.0%` |
| Elapsed seconds per candidate | `8.308` | `9.857` | `+18.6%` |
| Estimated cost per candidate | `$0.097303` | `$0.080861` | `-16.9%` |
| Rows over the 280 hard maximum | `1/37` | `1/32` | unchanged failure count |
| Rows over the 240 prompt target | `6/37` | `3/32` | improved, not reliable |

The 240-target prompt reduced but did not eliminate noncompliance, while stochastic yield also fell. This supports
moving the narrow formatting boundary to deterministic operator normalization rather than spending another live run
on a stricter prompt or changing the discovery strategy.

## Local counterfactual replay of the terminal object

The proposed v3 implementation was applied read-only to the retained terminal JSON without modifying the sealed run.
It changed exactly one row from `297` to the exact first `280` code points, added one candidate caveat plus one result
limitation, and produced `validate_model_result(...) == []`; raw maximum remained `297`. The historical receipt is not
retroactively promoted: its command-policy digest binds normalization v2, and the original bundle still replays as an
invalid result with zero integrity errors.

The next paid experiment should change only the command-policy normalization version, keep the reviewed v5.1 prompt
and query strategy, and require a valid completed GDM bundle before starting the prepared OpenAI large-lab run.
