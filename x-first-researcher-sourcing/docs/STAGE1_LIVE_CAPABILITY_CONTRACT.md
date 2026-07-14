# Stage 1 bounded live Grok/X capability contract

> Status: Author hardening in progress; independent review and first execution remain pending. The user approved a
> bounded live Grok/X test on 2026-07-14. This does not approve researcher mapping, Stage 2, identity merging,
> product writes, outreach, or scale-up. Grok CLI 0.2.99 cannot enforce a hosted-tool call count before transport;
> the runner therefore detects and kills a second call but must not describe that monitor as a provider-side cap.

## Decision and scope

The first live action is still an account-level capability handshake against the official `@OpenAI` X account. It is
designed to answer two separate questions:

1. did the Grok CLI execute its hosted native `x_search` tool and return canonical public X post evidence; and
2. did that evidence bind every observation to one stable numeric author `platform_user_id`?

The result distinguishes `post_retrieval_only` from `x_native_identity_ready`. A handle and canonical post URL can
prove bounded post retrieval, but they cannot authorize researcher identity work. `stage2_eligible_for_owner_review`
is true only for `x_native_identity_ready`, and it is still an owner-review input rather than an automatic Stage 2
transition.

## Owner and source-of-truth matrix

| Concern | Owner/source of truth | V2 rule | Forbidden fallback |
| --- | --- | --- | --- |
| User trigger | closed request + checkout-independent user-state ledger | Exact 2026-07-14 approval is atomically consumed once before spawn under `~/.local/state/x-first-researcher-sourcing/live-approvals/` | Per-checkout/runtime ledgers, OAuth/login inference, or replaying the flag |
| Legal/privacy basis | request `owner_decisions.legal_privacy` | Public-professional, minimized official-account evidence | Private content, protected identity, full-body retention |
| Access mode | runner command + structured Grok session updates | Pinned Grok CLI digest, `grok-4.5`, OAuth, successful artifact requires exactly one completed `x_search` call | `web_search`, `web_fetch`, model memory, Apify, another provider |
| Request identity | canonical sorted-JSON SHA-256 | Result binds exact closed request | Mutable prompt-only approval |
| Tool proof | ephemeral `updates.jsonl` parser + minimized provider-evidence receipt | Exact Grok 0.2.99 envelope/session, one final terminal event, reconciled usage, structured call/update, exact tool/model, and co-located post/author bindings only from the closed `rawOutput.posts[*]` registry | Recursive object/string/key search, diagnostic/request-echo objects, global URL/author unions, prompt prose, or model-declared provenance |
| External account | X platform | Numeric `platform_user_id` plus handle history | Handle/name as canonical person identity |
| Post identity | X platform | Numeric post id and exact `https://x.com/OpenAI/status/{id}` | Snippet URL, search-result redirect, reconstructed id |
| Usage | reconciled headless envelope + structured terminal + monotonic/wall clock + live process monitor | Session ids, normal stop reasons, token totals and turns must agree; success is at most four turns/180s, while a failed receipt may retain up to eight observed violation turns and use at most 200s only to truthfully include bounded process-group cleanup | Missing cost represented as `$0`, an over-budget reported cost/turn count erased, `MaxTurns` accepted as success, or a detected second call treated as success |
| Retention | exact private ignored atomic artifact bundle | Directory name equals run id; exact 3/4-file inventory; real UTC time and exact 24h expiry; deletion receipt only after verified removal | Renamed/extra-file bundle, impossible calendar time, null expiry, or prewritten deletion receipt |
| Product state | existing product owners | All candidate/link/assertion/write arrays empty | PersonAsset, CRM, projection, export, outreach |

The executable owners are `src/x_first/live_probe.py`, the v2 request/result JSON schemas, and
`scripts/run_live_capability_probe.py`. The declarative schemas cannot weaken executable validation.

## Execution topology

```text
closed v2 request -- global atomic one-shot approval consumption
      |
      v
verified CLI binary copy + fd-bound OAuth copy + isolated env/cwd
      |
      v
temporary GROK_HOME -- grok-4.5 -- generic web/local tools disabled
      |                                      |
      |                                      +-- one hosted X Search call maximum
      v
bounded stdio/process-group monitor + strict session/tool/terminal proof
      |
      +-- invalid/ambiguous --------> capability_unavailable, observations=[]
      |
      +-- canonical posts, no user id -> post_retrieval_only
      |
      +-- canonical posts + one stable user id -> x_native_identity_ready
      v
atomic private bundle: request/result/approval/tool receipts; no raw transcript or product writer
```

The runner uses an explicit `--execute-live` gate and atomically consumes the single approval in one mode-`0600`
user-state ledger before spawning the provider process. That owner is outside every repository checkout, so two
checkouts or copied runtime directories contend on the same `O_EXCL` record. Replays and concurrent contenders fail
before another external process starts. It copies a current-user-owned, non-writable reviewed Grok binary into the
temporary home and verifies SHA-256
`01bcacec...9ff81`; the OAuth cache is opened with no-follow semantics, checked by file descriptor, and copied without
rendering its contents. The child gets a closed environment, isolated non-project working directory, disabled updater,
generic web disabled, and all known local tools removed. It does not inherit API keys, proxy variables, project
instructions, or repository access.

For pinned Grok CLI 0.2.99 (`b1b49ccb71a7`), headless `--tools` is documented as a built-in internal-id allowlist,
while public streaming JSON documents only text/thought/end/error records. The installed binary contains `x_search`,
`x_user_search`, `x_thread_fetch`, and `tool_overrides.x_search`, but no exact `x_keyword_search`. That local string
inventory neither disproves hosted keyword search nor proves or denies any CLI `--tools` mapping. The runner therefore
does not pass `--tools x_search` and treats
that mapping as unproven until the one Stage 1 execution. It instead watches the private structured session artifact
and kills the entire dedicated process group when it observes an unknown/non-X tool, a
second X call, malformed completed evidence, oversized output/evidence, or the deadline. The cleanup runs even when
the direct parent reports a clean exit. From the instant `Popen` succeeds, a non-skippable `finally` owns kill, reap,
and process-group verification; monitor/stat/parser and cleanup exceptions become typed failed outcomes rather than
bypassing cleanup. This prevents a same-process-group child from surviving the runner. This limits
further work but cannot prove that a second
server-side call was stopped before transport. A successful artifact still requires exactly one completed call and
exact raw-result-to-observation reconciliation; any ambiguity is a failed probe. The temporary transcript is deleted.
Only a minimized provider-evidence receipt is retained. Before evaluating return code, stop reason, stderr, inner JSON,
or terminal success, the failure path independently snapshots bounded partial updates and structurally parses the
outer envelope. Therefore an already observed request id, call/completed-result count, raw id, turn count, or reported
cost is not rewritten to zero/null merely because inner validation or process-group verification failed. All JSON
entrypoints translate decoder recursion into a typed parse failure, then apply an iterative maximum depth of 64 and
maximum node count of 50,000 before any recursive content inspection. A valid outer envelope still produces a bound
outer-only tool receipt when updates are missing or invalid: its update digest is null, update bytes/calls are zero,
and request/session/token usage/turn/cost fields remain auditable without inventing an X call. A raw post is
accepted only from the direct `rawOutput.posts[*]` path when one closed reviewed record co-locates exactly one numeric
post-id field and the exactly matching canonical URL. A stable author id additionally requires every present registered
`author_info`/`author`/`user` child path to be complete and yield one consistent target handle/numeric id. Unread
provider fields beside `posts`, beside registered post fields, or beside registered author child paths are tolerated;
they never become evidence.

Every provider-owned bounded collection uses one closed projection in the result and tool receipt:
`{observed, relation, retained, truncated}`. `relation=exact` means the parser saw the complete distinct count;
`relation=at_least` means `observed` is a proved lower bound after the single overflow sentinel. Retained samples are
deterministic and bounded (completed calls first, then other calls, preserving first-observed order within each group;
first-observed order for posts, authors, models, unexpected tools, and evidence errors). Calls/models/authors/tools and
errors retain at most eight values; raw posts retain at most 25. The parser stores at most one additional sentinel and
never relabels the retained count as the total. Usage call/result-set counters bind the same projections, and raw
post/author/model projections bind result provenance to the tool receipt (result post ids use a canonical sort of the
same retained set). A receiptless failure must carry exact-zero,
untruncated provider projections. The complete staged 3/4-file bundle is re-read and validated before its directory is
atomically published; an invalid staged bundle is removed.
If an X call itself falls beyond the single retained sentinel, post and author projections become
`at_least`/`truncated` even when their retained arrays are empty; the parser cannot claim exact zero for evidence an
omitted call might have returned. Non-canonical model identities and oversized tool identities are retained only as
bounded SHA-256 labels, and an unrecognized terminal stop reason becomes null plus a fixed evidence error.
Only then does the
parser bind the command session to the headless envelope and every Grok 0.2.99 `session/update` wrapper. It requires
one final `_x.ai/session/update`/`turn_completed`, reconciles both
usage views, and stores author ids only on the exact source records that carried their author dictionaries. Retained
Stage 1 observations remain minimized model output mechanically bound to those source ids/URLs and the call receipt;
they are not represented as verbatim raw provider output. A URL in one subtree and an author id elsewhere can prove at
most post retrieval; it cannot prove stable account identity.

The v1 raw-record registry reads only a direct root `posts` list, and each post accepts `canonical_url` co-located with
exactly one matching `id`, `id_str`, or `rest_id`. Author
identity is accepted only from `author_info.rest_id + author_info.legacy.screen_name`,
`author.id_str + author.screen_name`, `author.id + author.username`, or the corresponding two exact `user` shapes.
Incomplete or malformed registered paths, competing author containers, repeated author identities, duplicate post
records, and conflicting handles/ids fail the probe. Unread extra fields are ignored. The parser never searches
arbitrary prose or recursively adopts another nested object's generic `id`, `posts`, or author dictionary.

## Budgets and kill switch

- one Grok execution;
- exactly one completed `x_search` call for success; a second observed call trips the kill switch and fails the run;
- one completed native-search result set (derived from the structured call receipt, not model prose);
- at most five observations;
- at most four model turns;
- a failed audit receipt may retain up to eight actually observed turns solely to prove a turn-budget violation;
- 180-second subprocess deadline;
- at most 20 additional seconds in a failed wall-clock receipt for bounded kill/wait verification; never for success;
- reported monetary cost at most `$0.25`;
- no fallback, retry, graph expansion, or researcher task.

OAuth often omits monetary cost. The result must then record `cost_status=unreported` and `cost_usd=null`; capability
proof may still be measured with calls, turns, tokens and elapsed time, but cost efficiency remains unproven and cannot
be reported as free.

Any generic-web/local tool, second X call, budget overrun, invalid/duplicate post identity, missing structured provenance,
unsafe OAuth file, unbounded output, protected/proxy content, full-body retention, or product-writer field fails closed.
Failed results discard all observations.
When a call has already occurred, a failure receipt still preserves and reconciles the observed provider request id,
call/completed-result counts, terminal and outer turn counts, raw ids, model ids, and reported or unreported cost. An
over-budget reported cost remains visible in the failed receipt instead of being rewritten as unreported.

Request/result/approval/tool files are written to a private staging directory and renamed as one atomic bundle. A
successful bundle has exactly four files. A failure has four when either bounded updates or an outer envelope can be
retained, and three only when neither evidence surface parsed. Validation
rejects extra transcripts, renamed directories, non-private files, a run-id mismatch, an absent global ledger, invalid
calendar timestamps, and any expiry other than exactly completion plus 24 hours. `--purge-expired` detects unexpected
or renamed paths instead of silently skipping them, deletes a validated expired bundle, verifies it is gone, fsyncs
the runtime owner, and only then writes a non-sensitive deletion receipt. `runtime/` is ignored by Git.

## Adversarial regression closure

The post-`fee3699` reviews reproduced fifteen false-green or non-terminal classes before this hardening:

1. two independent runtime roots could each consume the same approval;
2. a wrapped update with a post URL and an unrelated author dictionary could claim stable identity;
3. `MaxTurns` plus a different outer session id could validate;
4. a locally forged, renamed bundle with an extra transcript and an impossible February 31/null-expiry timeline could validate;
5. rename caused purge to skip the bundle, while a failed `rmtree` still left a `deleted` receipt;
6. a 30-second same-process-group child survived after its direct parent exited;
7. malformed JSONL hid two observed X calls from the monitor;
8. a monitor/read exception escaped before process-group cleanup;
9. a cleanup-verification exception erased an already completed X call from the failure receipt;
10. an invalid inner JSON body erased an already observed outer request id, turn count, and reported cost;
11. a nested diagnostic/request-echo object with `id + canonical_url` produced a false post;
12. a target author container plus a conflicting account container still produced stable identity;
13. duplicate raw post records were set-collapsed before runtime/schema validation;
14. a 10,000-level stdout or updates JSON value escaped with `RecursionError` after consuming approval and wrote no bundle; and
15. a valid outer envelope without updates lost its session and token-usage audit fields;
16. the ninth distinct call/model/author/unexpected tool, more than 25 aggregate posts, or more than eight evidence
    errors could consume approval and leave no valid failure bundle; and
17. JSON booleans/floats/strings equal to numeric zero could forge an outer-only update-byte receipt.

Each now has a deterministic concurrency, mutation, artifact, or subprocess regression. These tests prove the local
fail-closed contract only; they are not a live X capability result or an independent-review `GO`.

The fixed-forward checkout discovers 24 focused live-contract tests and 90 repository tests. The reviewed `a6d9e07`
baseline was 20 focused and 85 repository tests, not 86; the additional repository test before this fixed-forward
slice belongs to the separately committed profile/Bio lane.

An additional read-only compatibility check parsed one already-existing, completed, no-tool local session from pinned
Grok CLI 0.2.99: command/session metadata, `grok-4.5`, `end_turn`, and terminal usage reconciled with zero evidence
errors. That check proves only the wrapper/terminal parser shape. Because the session contained no X call, it supplies
no evidence that hosted `x_search` is available or that the Stage 1 source-binding parser matches a real X result.

## Commands

Offline validation and tests:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest \
  tests.test_x_first_live_probe_and_evaluation -v
../sourcing-ai-agent/.venv/bin/ruff check .
```

The only live entrypoint is:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_live_capability_probe.py --execute-live
```

Existing private artifacts can be revalidated without external execution:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_live_capability_probe.py \
  --validate-directory runtime/live-probes/<run_id>
```

Delete expired artifacts and record the purge:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_live_capability_probe.py --purge-expired
```

## Stage 2 remains separate

Stage 1 cannot identify or rank researchers. Even `x_native_identity_ready` only makes a bounded Stage 2 design
eligible for an owner decision. Stage 2 requires its own request/result contract, query-family registry, independent
golden set, adjudication workflow, cost/rate budget, retention/deletion evidence, and independent review.

For a later scalable transport, the official xAI Responses API exposes only `x_search`, handle allowlists (up to 20),
date filters, structured X tool-call records, and sources. The official Batch API can queue many independently keyed
Responses requests and supports cancellation, per-request status, result pagination and cost tracking. Both require a
supported xAI API credential; the current Grok OAuth cache is not repurposed as an API key. Official `max_turns` limits
assistant/tool turns, not individual parallel calls, so even the API design must reconcile actual call records rather
than claiming a nonexistent `max_tool_calls` control.

Official references: [X Search](https://docs.x.ai/developers/tools/x-search),
[tool-call turns](https://docs.x.ai/developers/tools/tool-usage-details), and
[Batch API](https://docs.x.ai/developers/advanced-api-usage/batch-api).
