# Adaptive Grok Native-X Recall-Wave Contract

Status: offline and fake-live contract tests complete; real Grok/X diagnostics exist, while every new execution remains
explicitly operator-triggered and review-gated.

## Product outcome

`src/x_first/adaptive_grok_wave_runner.py` runs one target-neutral Grok research wave using only the four native X
tools exposed by the local Grok CLI. Its goal is broad, evidence-bearing discovery. It does **not** cap the number of
candidates, observations, search queries, evidence items, excluded examples, or native-X calls as a business rule.

The model should continue varying X-native search strategies while they appear to produce materially new evidence.
Overall wave yield and model-reported saturation are evaluated after the wave; per-query marginal yield is not native-
result-bound in the current transport. None is encoded as an arbitrary `25 candidates`, `200 observations`, or
`16 calls` cutoff.

The operator still has non-business safety boundaries:

- a configurable, deliberately high model-turn emergency ceiling;
- one monotonic wall deadline;
- high stdout, stderr, JSON-byte, JSON-depth, and JSON-node ceilings;
- high session-entry, depth, per-file, aggregate, update-log, and update-line ceilings;
- request-bound token and estimated-cost ceilings plus a private retention TTL;
- bounded process-group `TERM`, then `KILL` cleanup.

Those limits protect the workstation from runaway execution or malformed output. They are not recall targets and do
not make model-reported provider counts mechanically true.

The resulting files are private research artifacts only. They cannot write product, canonical identity, ranking,
CRM, export, billing, permission, or outreach state. Protected-identity inference and provider fallback remain false.

## Authority and source-of-truth matrix

| Concern | Owner/source of truth | Fail-closed rule |
|---|---|---|
| Live target and prompt | Versioned module-owned effective-prompt policy plus SHA-bound request | The exact target tuple and source-prompt digest must be one reviewed `live_authorized` row; arbitrary scope text or a new prompt fails before any run root is retained |
| Fixture target and prompt | SHA-bound request plus private `0600` source | Fixture requests remain generic and offline; a `fixture_only` policy row never grants live authority |
| Prior waves | Private `0600` files plus SHA-256, optionally constrained by the selected effective-prompt entry | Casefold duplicates inside a prior wave fail; `require_empty_prior_waves_v1` rejects any non-empty array before auth, grant issuance/consumption, run-root creation, replay, or recovery |
| Prior completion | Local validator | An old handle may reappear only with new evidence digest or a new temporal state |
| Grok executable | Request digest plus canonical local locator | Stable owner-controlled target is descriptor-copied to a private staged executable |
| OAuth state | Canonical private auth file, current Grok 0.2.101 xAI OIDC/JWT contract, request-pinned digest/account reference, replay-independent active-use claim, and auth-digest taint registry | Exactly one issuer/client/identity-consistent credential is descriptor-copied into isolated `GROK_HOME`; one run/recovery owns the digest until audit plus durable deletion, and provider mutation/deletion/unreadability or an abnormal provider-capable exit taints that digest for future grants |
| Live authority | Preissued request-scoped grant | Account/auth, model, command/tool/schema/environment policies, emergency, budget, retention, and request scope are bound; expiry is checked before consumption, after the exclusive link, and at the gated target release |
| Native-X tool surface | Operator command policy | Exact closed allowlist of four tools; generic web and local/agent tools denied |
| Run ownership | Random durable `run.lock` token plus nonblocking `flock` | Execution owns the lease for the whole run; recovery/purge mutate nothing while an active owner exists |
| Process identity | Gated launcher, kernel birth identity, inherited random token, process-group ledger, and run lease | Target exec is released only after durable ledger; recovery never kills a numeric PGID alone |
| Process cleanup | Monotonic operator | All normal, timeout, output-limit, callback-error, and exception paths are bounded |
| Output transport | Strict Grok headless envelope plus raw private bytes | Exact outer object, `EndTurn`, command session, request ID, turns, token totals, inner JSON, duplicate keys, nonfinite numbers, prefix, and suffix are replayed; the inner result alone cannot claim terminal success |
| Model diagnostics | Raw outer `text`, plus diagnostic provenance retained in the normalized inner result | Model-reported calls, queries, observations, and original `local_reconciliation` are never tool-ledger truth; the unmodified original remains in `raw.stdout` |
| Evidence excerpt output | Result-v3 schema and validator retain the 280-code-point publication maximum; append-only v5.1 discovery prompts target 240 and require a final all-row audit | Normalization v3 may replace only a strict-UTF-8 model-reported excerpt of `281..560` code points with its exact first 280-code-point display prefix. It does not strip, NFKC-normalize, paraphrase, insert an ellipsis, or select a semantic window. Raw output remains hash-bound; candidate and result audit text says semantic completeness is not guaranteed. `561+`, non-scalar, collision-producing, or otherwise invalid rows still reject the complete result |
| Operator projections | Inner candidate/evidence arrays plus verified session proof | `sanitized.json` rewrites candidate, evidence, post-URL, tool-call, and per-tool counts from local structure/transcript facts; it preserves model provenance only as diagnostics |
| Mechanical result normalization | `mechanical-evidence-relationship-x-rfc2822-timestamp-and-model-reported-excerpt-prefix-v3`, bound into the command-policy digest | The current policy may downgrade only a non-Bio `self` row whose author differs from the candidate to `third_party`, convert only an exact round-trippable native-X IMF-fixdate `GMT` timestamp to canonical UTC ISO-8601 `Z`, and apply the narrow excerpt display projection above. Raw bytes stay unchanged, deterministic audit text is appended, typed supports/state/confidence/source identity are unchanged, and all changes are admitted atomically only when the complete result and technical envelope pass. Normalization v2, v1 relationship-only, normalization-only result-v3, pre-normalization result-v3, and result-v2 remain replay-only under their recorded digests; collisions, duplicates, and malformed Bio/Post topology still fail closed |
| Tool-call facts | Raw Grok session `updates.jsonl` | Effective model, native-X starts/completions, names, and exact arguments are replayed. On Grok 0.2.101 the outer envelope owns terminal/usage; a legacy transcript `turn_completed` is additionally reconciled when present |
| Session query phase | Selected effective-prompt entry plus raw native-X arguments | Entries default to `mixed_discovery_hydration_v1`; `discovery_only_no_person_hydration_v1` rejects every `from:` form. `discovery_only_official_accounts_no_person_hydration_v2` instead accepts exactly one positive `from:` only in keyword search and only when its handle casefold-matches the selected entry's non-empty `official_account_handles`; negated, multiple, semantic, unknown, and candidate handles fail. Both discovery policies NFKC-normalize queries, remove invisible format controls, reject outer ASCII/Unicode-wrapped handle-like single-token subjects, and apply the same closed target/professional `x_user_search` grammar; multiword keyword/semantic person intent remains a named residual |
| Post-transform technical envelope | Request-frozen `max_json_bytes`, `max_json_depth`, and `max_json_nodes`, plus `post-transform-json-envelope-and-terminal-limit-replay-v1` in the current command-policy digest | Normalization, transcript-terminal recovery, and phase-aware projection are serialized and structurally rechecked before publication. An expanded result that crosses a bound becomes a truthful `technical_limit_exceeded` bundle with no oversized `sanitized.json`; current-policy replay independently rederives the exact transcript-terminal limit kind rather than trusting the receipt label |
| Candidate authored-surface attempts | Completed `x_keyword_search` arguments in the raw session transcript | Only one exact positive `from:<handle>` can be attributed; positive `filter:replies` means `authored_reply`, absent/negated reply filter means `authored_post`, and global, multi-handle, semantic, user, or thread calls remain unattributed |
| Retention/deletion | Request TTL, terminal receipt, external deletion journal and receipt | Expired bundle is validated and journaled before recursive deletion; a crash between delete and receipt is reconcilable |

## Request contract

The closed schema is `contracts/x.grok.adaptive_recall_wave.request.v2.schema.json`. Required sections are:

- `target`: configurable `lab_id`, `research_focus_id`, and scope in fixture mode; live grant issuance/execution additionally require the exact tuple to be approved with the prompt digest in the module-owned effective-prompt policy;
- `prompt_source`: private path and exact digest;
- `prior_waves`: any number of private wave locators and digests;
- `transport`: fixed provider owner, configurable model/effort, exact live binary/auth digests, and opaque operator account reference;
- `emergency`: turn ceiling, deadline, and TERM/KILL grace periods;
- `technical_limits`: high byte, JSON-complexity, and complete session-tree ceilings;
- `budget`: pricing-policy ID, token ceiling, estimated-cost ceiling, and pinned input/output rates;
- `retention`: policy ID, TTL, and mandatory deletion receipt;
- `approval.grant_id`: opaque locator for a separately preissued, single-use grant;
- closed negative authority flags.

The runtime additionally enforces `max_json_bytes <= max_stdout_bytes`, owner-only files, cross-field profile URLs, and
the state relationships that plain JSON Schema cannot express.

The command-policy digest owns the exact provider argv template,
`mechanical-evidence-relationship-x-rfc2822-timestamp-and-model-reported-excerpt-prefix-v3`, and
`post-transform-json-envelope-and-terminal-limit-replay-v1`. This prevents a retained result-v3 bundle sealed before
either policy from being reinterpreted after the fact. The former normalization-v2-plus-artifact,
v1-plus-artifact, and older normalization-only digests are accepted solely for retained replay and cannot authorize a
new grant. The current normalization repairs only the mechanically impossible authority label `self` to the generic
lower-authority `third_party`; converts a timestamp only when parsing and formatting it back as IMF-fixdate reproduces
the input byte-for-byte with literal `GMT`; and replaces only a strict-UTF-8 `281..560`-code-point model-reported excerpt
with its verbatim first 280-code-point display prefix. Prefix projection does not claim semantic completeness or
source fidelity, and leaves typed supports, state, confidence, overlap, and immutable source identity untouched. It
preserves raw provider bytes, appends deterministic candidate/result audit text, reapplies the technical envelope, and
accepts the whole transformed copy only if no other runtime contract error remains. Bio relationship/topology and
case-insensitive self-author labels are never reclassified; malformed subject/URL/Post ID/time/thread/support bindings,
duplicate handles/evidence, and every other invalid result still fail closed.

The effective-prompt entry may also own the pair `session_query_policy_id` plus
`session_query_policy_sha256`. The official-account discovery policy additionally requires a non-empty, casefold-unique
`official_account_handles` array of valid X handles; no other policy may carry that field. Existing rows omit these
fields and retain mixed discovery/hydration behavior under unchanged entry digests. A discovery-only row binds the
stricter phase/query/projection semantic digest and any official handles into the exact selected-entry digest used by
the grant, intent, receipt, and replay. The loader recomputes the semantic digest from a
versioned registry and fails closed on disagreement, preventing an existing ID from silently acquiring new behavior.
An entry may independently own the paired `prior_input_policy_id` and `prior_input_policy_sha256`. The first policy,
`require_empty_prior_waves_v1`, accepts only the exact empty request array. Both fields are optional so entries that do
not opt in retain their byte-identical selected-entry digests; adding, removing, or altering the pair on a selected row
changes that row's digest. Selection enforces the rule before live auth or grant work and before run-root creation, and
the same selection is rerun during grant validation, retained-bundle replay, and crash recovery.
The session parser NFKC-normalizes every native-X query and removes invisible Unicode format controls before checking positive or negated `from:` forms. The v2 official path requires one exact positive operator, a keyword tool, and an entry-bound official handle; every other `from:` path is rejected. It removes
outer characters that cannot belong to an ASCII X handle, including ASCII punctuation, curly quotes, fullwidth
punctuation, and other Unicode wrappers, then rejects a remaining handle-like single-token subject before branching
by tool. User search is narrower: it fully consumes the normalized query, rejects non-ASCII or unrecognized syntax,
requires a target-lab token and a professional-context token, and accepts only the closed
target/professional/connector grammar. A model prompt must enumerate terms from that exact runtime vocabulary; bare
organization literals and topic-only `data` queries belong to keyword/semantic discovery rather than user search.
Arbitrary multiword person intent in keyword or semantic search
remains a post-run audit residual because rejecting all such phrases would also remove useful project/topic discovery.

Discovery-only transport completion proves the mechanically enforced phase boundary, not strategy-matrix completion
or population convergence. Raw session arguments can derive Top/Latest, historical-shard, positive-Reply, thread,
tool-family, and other query-attempt cells. They do not retain native-X tool result payloads or query-to-new-lead
attribution, so zero-yield sequences and population convergence remain permanently unproven for this version. The
phase-aware projector therefore keeps every otherwise successful discovery-only result at `X_SEARCH_PARTIAL`, replaces
both model OK and model PARTIAL status reasons with one operator-owned unproven reason, does not apply the later
hydration stage's per-handle Post/Reply surface limitation, and prohibits a
`discovery_converged` claim. That partial status is expected and is distinct from discovery failure; the outer operator
receipt may still be `completed` because it describes verified transport completion rather than population
convergence.

All operator-owned transformations reuse one post-transform serializer. It reapplies the request-frozen JSON byte,
depth, and node ceilings after version-selected mechanical normalization, transcript-terminal recovery, and ledger/status
projection. Crossing a ceiling sets the corresponding technical-limit terminal state and suppresses publication of an
oversized or structurally invalid `sanitized.json`; replay derives the same outcome. For a current-policy live bundle
whose terminal state is `technical_limit_exceeded`, replay reruns transcript-terminal recovery and compares the
rederived limit kind with the receipt, so a changed `json_bytes`/`json_structure` label fails with
`structured_technical_limit_mismatch`; the same comparison rejects a process-level limit relabelled as a JSON limit
when replay derives no JSON failure. Replay-only older digests retain their sealed interpretation. Crash recovery retains
`crash_recovered` as the lifecycle state but records the same `technical_limit_exceeded` flag and exact JSON limit kind
instead of discarding the reason.

Example fixture request shape:

```json
{
  "schema_version": "x.grok.adaptive_recall_wave.request.v2",
  "request_id": "xwave_req_11111111111111111111111111111111",
  "target": {
    "lab_id": "synthetic_lab",
    "research_focus_id": "base_model_training",
    "scope": "Public professional evidence for a synthetic target."
  },
  "prompt_source": {
    "path": "/private/tmp/operator-fixture.invalid/prompt.md",
    "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  },
  "prior_waves": [],
  "transport": {
    "provider_id": "grok_cli_oauth",
    "model_id": "grok-4.5",
    "reasoning_effort": "high",
    "grok_binary_sha256": null,
    "operator_account_ref": null,
    "oauth_auth_sha256": null
  },
  "emergency": {
    "max_turns": 64,
    "deadline_ms": 1800000,
    "term_grace_ms": 1000,
    "kill_grace_ms": 1000
  },
  "technical_limits": {
    "max_stdout_bytes": 16777216,
    "max_stderr_bytes": 1048576,
    "max_json_bytes": 16777216,
    "max_json_depth": 64,
    "max_json_nodes": 250000,
    "max_prompt_bytes": 1048576,
    "max_compiled_prompt_bytes": 16777216,
    "max_prior_wave_bytes": 16777216,
    "max_total_prior_wave_bytes": 268435456,
    "max_prior_json_depth": 64,
    "max_prior_json_nodes": 250000,
    "max_session_files": 64,
    "max_session_file_bytes": 33554432,
    "max_session_total_bytes": 67108864,
    "max_session_updates_bytes": 16777216,
    "max_session_update_line_bytes": 4194304
  },
  "budget": {
    "pricing_policy_id": "synthetic_pricing.v1",
    "max_total_tokens": 1000000,
    "max_cost_usd_micros": 100000000,
    "input_token_cost_usd_micros_per_million": 1000000,
    "output_token_cost_usd_micros_per_million": 2000000
  },
  "retention": {
    "policy_id": "adaptive_private_24h.v1",
    "ttl_seconds": 86400,
    "deletion_receipt_required": true
  },
  "approval": {"grant_id": null},
  "authority": {
    "canonical_identity_write_authorized": false,
    "outreach_authorized": false,
    "product_write_authorized": false,
    "protected_identity_inference_authorized": false,
    "provider_fallback_authorized": false
  }
}
```

No array in the request or result schemas has `maxItems`.

### Live effective-prompt owner

`configs/adaptive_grok_wave_effective_prompt_policy.v1.json` is the production owner for the complete live objective,
not merely a prompt allowlist. A row binds the exact target tuple and exact source-prompt SHA-256 and is explicitly
`fixture_only` or `live_authorized`. Grant issuance accepts only `live_authorized`; the policy digest and entry ID are
bound into the grant, intent, command binding, terminal receipt, and bundle replay. The digest is an entry-scoped
semantic binding over the binding version, schema version, policy ID, owner, globally allowed discovery dimensions,
and exact selected entry. It is deliberately not the complete mutable registry-file digest: appending an unrelated
valid row cannot invalidate an issued grant, retained bundle, or TTL purge, while changing the selected row or any
immutable owner semantic fails replay. Production tests use an explicit lab-to-prompt-family table, discover each
family's prompt files, and reconcile the complete live registry without hard-coded per-lab or global entry totals.
Each lab has its own exact target tuple; prompt digests cannot be swapped between them. The OpenAI tuple is:

```text
lab_id=openai
research_focus_id=pretraining
scope=Public professional evidence of current or historical OpenAI affiliation and current or historical pre-training or base-model training relevance.
```

The Google DeepMind tuple changes only the lab ID and lab name in the scope. Tests reconcile both exact prompt sets,
reject wrong-target and wrong-prompt combinations, and grant only a matching append-only row.

The synthetic row is `fixture_only`. Tests replace the module path with an isolated test policy; no public live
entrypoint accepts a caller-supplied policy path. Adding another lab, focus, scope, or prompt therefore requires a
reviewed append-only registry row rather than an algorithm special case. Prior-wave files remain parsed handle exclusions and
material-update baselines only; they cannot replace or extend the approved objective.

## Native-X command and isolation

The live process executes the staged binary in an empty owner-only workspace with an isolated owner-only `HOME`,
`GROK_HOME`, temporary directory, and XDG directories. It does not inherit API keys, plugin/MCP configuration, hook
configuration, shell state, or proxy environment variables. The base prompt is in a `0600` file and is passed through
`--prompt-file`; prompt text and handles never enter argv.

The relevant policy is:

```text
--output-format json
--json-schema <exact result-v3 schema>
--disable-web-search
--disallowed-tools run_terminal_cmd,grep,read_file,search_replace,list_dir,web_search,web_fetch,todo_write,task,Agent
--no-subagents
--no-plan
--no-memory
--no-auto-update
--permission-mode dontAsk
--sandbox read-only
--leader-socket <isolated-path>
```

There is no `--always-approve`, `bypassPermissions`, generic browser/search tool, connector, subagent, shell, or local
file tool. Grok CLI does **not** map the hosted `x_keyword_search`, `x_semantic_search`, `x_user_search`, or
`x_thread_fetch` names through its built-in `--tools` allowlist. A 2026-07-15 live A/B produced zero native-X calls
when those names were passed and immediately exposed `x_user_search` when the flag was removed. The command therefore
does not pass `--tools`. `--disable-web-search` plus the local-tool denylist constrain the launch surface, and the
retained session proof accepts only the four registered native-X tool names and their closed argument profiles before
the result can complete. Grok 0.2.101 may additionally emit a paired internal `UpdateGoal` progress event. The parser
accepts only its exact versioned `grok_build/update_goal` metadata and start/completion shapes, excludes it from every
native-X count, and still rejects every other non-X tool event. The registry digest explicitly binds both
`cli_native_x_allowlist_enforced=false` and
`native_x_session_proof_required=true` so replay cannot silently restore the broken flag or weaken evidence checks.

Grok 0.2.101 is invoked with `--output-format json --json-schema <exact schema>`. Stdout must be one strict headless
envelope matching `contracts/x.grok.adaptive_recall_wave.headless_envelope.v1.schema.json`. The outer `sessionId`,
`EndTurn`, turns, and usage are the terminal authority and must bind the command session. Session updates own native-X
starts, completions, names, arguments, and query-surface attempts. They may contain a closed pre-user
`_x.ai/session/update` `retry_state`, progress `agent_message_chunk` events interleaved with later tool calls, and no
`turn_completed`; an exact assistant-message suffix must equal outer `text`. If outer `text` is one strict result-v3
document, that is the model result. Grok may instead concatenate a structured progress message and the structured
terminal message. In that exact case the runner does not scan for a convenient JSON object: it clears terminal-message
state whenever a later native-X tool starts, selects only the contiguous assistant message after the final completed
tool, requires that message to be one strict result-v3 document, and binds the entire transcript by hash. If a legacy
`turn_completed` exists, its terminal/usage must agree with the outer envelope. Already-sealed rejected receipts replay
under their original parser behavior until their 24-hour retention purge; this compatibility path cannot authorize a
new run or convert a rejected receipt into `completed`.

The result schema keeps every evidence field in one closed object and intentionally does not use a nested `oneOf` for
the Bio-versus-Post `kind/thread_relation` dependency. A real Grok 0.2.101 structured-output run projected only the
branch-local fields and dropped the shared evidence properties when that redundant `oneOf` was present. The runtime
validator remains authoritative for the same dependency: Bio requires `thread_relation=null` and profile binding;
Post/mention/thread evidence requires one of the five non-null thread relations plus author, post, URL, timestamp,
subject, excerpt, and typed support binding. Removing the provider-incompatible schema branch does not weaken that
semantic validation.

Retained pre-headless bundles remain replayable under an explicit replay-only legacy plain command-policy digest and
their original self-reconciliation semantics. New grant issuance and execution accept only the structured command
policy, so an old grant cannot authorize a changed command. The executable digest, result-schema digest, exact argv,
and versioned redacted command-policy digest are bound into the receipt.

## Binary and OAuth preflight

The configured Grok locator may be an owner-controlled symlink. Before grant consumption, the runner:

1. resolves the canonical final target;
2. opens it with `O_NOFOLLOW`;
3. checks regular-file type, owner, link count, user-executable mode, nonempty size, and no group/world write bits;
4. descriptor-copies and hashes it into `run/executable/grok` with mode `0700`;
5. compares pre-copy fd, post-copy fd, canonical path, and locator identities;
6. executes only the staged copy.

The canonical OAuth file must be a current-owner, one-link, regular `0600` file within the size ceiling and must match
the request-pinned digest. A mismatch now fails before prompt access or run-root creation. Digest equality proves byte
identity, not a usable login. Grant issuance therefore parses duplicate-key-safe JSON as exactly one current Grok
0.2.101 xAI OIDC row. The row locator must be exactly `oidc_issuer::oidc_client_id`, `auth_mode` must be `oidc`, and
the issuer must be `https://auth.x.ai`. Required client, subject/user/principal, principal-type, and team fields must
agree between row metadata and the bounded base64url-decoded access-JWT payload. The payload must contain integer
`iat` and `exp`, may contain integer `nbf`, cannot be not-yet-valid, and must include the required xAI/Grok access
scopes; additional scopes are allowed. Payload decoding is byte/depth/node bounded and duplicate-key-safe. The runner
does **not** verify the JWT signature and makes no cryptographic authenticity claim; signature validation remains with
xAI/Grok.

The usable expiry is `min(metadata expires_at, JWT exp)`. It must be strictly later than the complete grant TTL plus
process deadline, TERM/KILL grace, and a fixed 600-second refresh-avoidance margin. Execution repeats the strict
credential and runtime-window check after loading the grant but before reading the prompt or creating a run root. The
descriptor-copied auth is checked once more with a fresh wall clock before the single-use grant is consumed.

The live lane does not rely on Grok refreshing an expired token inside the disposable home. A provider refresh can
rotate credential state; deleting that home would discard the new state while leaving the canonical file stale for
the next run. Refreshing or writing back canonical OAuth state is not part of this runner. A stale, malformed,
wrong-issuer/scope/identity, multi-row, not-yet-valid, or near-horizon auth file therefore fails locally without a
provider call and, at execution, before grant consumption.

The approval root owns two closed, replay-independent records per auth digest. An
`auth-active-use-<original-auth-sha256>.json` claim binds exactly one run ID, canonical request digest, run-lease
digest, grant digest, and closed `live_consumption|legacy_recovery` origin. It is published before grant consumption
and remains durable across process death; grant
issuance, live execution, and sibling consumption all fail while it exists. Recovery acquires the run lease first,
then read-binds the consumption record and every independently durable process-boundary artifact before it may mutate
the claim or retained run. Missing exact consumption plus a bound `live_consumption` claim, current executor-return
journal (including no-spawn), process ledger, process spool, or pending publication fails with the typed
`recovery_grant_consumption_missing`; deleting the claim or rewriting its origin to `legacy_recovery` cannot downgrade
that state. One explicit artifact-name registry owns both the runtime layout/real publication call sites and this
classifier. Its post-consumption set includes process journal/ledger/spools, retained session updates, promoted raw
stdout/stderr, sanitized output, and the terminal receipt, including their `_atomic_publish` pending files. Pending
compiled prompt, operator request, or operator intent files are explicitly pre-consumption and do not by themselves
upgrade the state. The same registry drives two independent pre-mutation scans: registered `.pending-*` target names
and already-published registered final names, including a dangling symlink under such a final name. Journal and ledger
contents remain deeply bound before that presence classifier; the classifier is not a substitute for their schema and
run/session/lease validation. A fully published `operator-receipt.json` takes the earlier terminal-run path and returns
`run_already_terminal` without consulting or mutating approval state, while a pending receipt is still incomplete
post-consumption evidence. Only a genuine pre-D2/pre-consumption incomplete run with no post-consumption evidence may
receive the synthesized `legacy_recovery` claim that closes its reuse window.

The separate `auth-taint-<original-auth-sha256>.json` marker contains only the original digest, source run/request
digests, detection time, closed reason, and blocking state—never a token, claim value, profile field, or refreshed
credential. A per-digest owner-only `flock` serializes short claim, taint, grant-publication, and consumption
transactions. Acquisition is nonblocking with a 250 ms budget, and the lock path is revalidated after acquisition;
the lock is never held across provider work. Issuance checks both registries, execution checks them again before
prompt access, and consumption checks them under the same lock, so a prior sibling run can invalidate an already
issued but unconsumed grant. An absent claim and marker is an ordinary clean state; an existing malformed, unreadable,
or wrongly bound record fails closed. The first valid taint marker is idempotent and remains authoritative for that
digest. Recovery from taint requires a newly logged-in canonical auth file with a new SHA-bound request; restoring the
old bytes does not clear the marker.

The auth file is descriptor-copied with pre/post identity checks. The durable intent is published before auth is
copied. Consumption atomically establishes the durable active-use claim before it links the one-shot consumption
record. Every owned exit follows one order: audit the copied auth, durably delete the complete ephemeral home, then
resolve only that run's exact claim. An audit/taint-publication failure retains both home and claim; a deletion failure
retains the claim. Either state blocks same-digest grants until explicit recovery completes the same order.

Provider mutation, deletion, or unreadability records a taint before deletion. For an unchanged copy, an abnormal
result is tainted only after the gated launcher callback passed every release check and authorized target execution.
A post-consumption expiry, executor failure before spawn, or durable process ledger whose final release check fails
still consumes that grant but does not taint exact unchanged OAuth bytes in the live process. Once target release was
authorized, a nonzero return, timeout, execution error, or process/session-tree technical limit is conservatively
tainted. Mutation/deletion/unreadability takes precedence over that generic reason. Recovery cannot reconstruct the
in-memory release-authorization fact; a durable process ledger therefore conservatively means the provider may have
run and forces the generic non-clean reason when the copy is otherwise unchanged. Taint does not rewrite the current
run's otherwise valid evidence or terminal status; it governs future grants only. Normal execution deletes the home before
raw stdout/stderr publication, structured-output parsing, sanitized-result publication, or terminal receipt
construction. A terminal receipt requires `ephemeral_tree_deleted=true`; a post-executor publication failure can be
recovered from the durable intent/spools without retaining OAuth bytes. Because the provider can write this tree,
deletion restores owner-only traversal mode through no-follow directory descriptors and unlinks without following
provider-created links. A provider that changes any directory away from `0700` or regular file away from `0600` fails
the session-tree boundary; even mode-`000` nested directories are deleted without an operator chmod step.

## Preissued single-use live grant

Grant creation and execution are separate commands. Issuance is write-only local authority preparation; it never
starts Grok:

```text
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_adaptive_grok_wave.py --request <private-request.json> --issue-live-grant
```

The owner-only grant binds request/target, model, executable digest, account-reference digest, OAuth digest, result schema, canonical
command policy, tool registry, environment policy, emergency envelope, technical envelope, budget, retention, and the
SHA-256 of the complete execution scope. Its default lifetime is 900 seconds and cannot exceed 3600 seconds. The
opaque grant ID is stored only as a digest.

Before reading the prompt, staging the binary, or creating a run root, live execution validates the module-owned
effective-prompt row and the complete preissued grant at a fresh wall-clock value. Missing, expired, wrong-scope, or
wrong-prompt authority therefore leaves no unowned run directory. Disk/staging failures before durable intent are
also recursively discarded; purge sees no intent-less orphan. After binary and auth preflight, execution revalidates
grant expiry plus the auth-digest taint/active-use state. Under one short per-digest transaction it first publishes the
run/request/lease/grant-bound active-use claim and then publishes exactly one `O_EXCL` consumption record. It then
reads the authoritative clock again after the exclusive link. If the grant expired in that window, the record remains
consumed, the executor is never invoked, unchanged OAuth is audited/deleted without taint, and the claim is resolved.
A conservative monotonic deadline is derived from the remaining lifetime. The gated launcher rechecks both wall and
monotonic time after its durable process ledger is published and immediately before target `execve`; expiry closes
the gate and the staged Grok binary never runs. Reuse therefore fails before target execution. Changing any
execution-scope field invalidates the grant. The durable grant, consumption and launcher timestamp are replayed by
`validate_operator_bundle` rather than trusted from the receipt; active-use state is operator recovery coordination,
not a new retained-bundle dependency.

## Result and prior-wave semantics

`contracts/x.grok.adaptive_recall_wave.result.v3.schema.json` keeps target-lab affiliation and pretraining experience
as two independent temporal dimensions: `current`, `historical`, `ambiguous`, or `unsupported`. Current lab plus
historical pretraining, historical lab plus historical pretraining, and other combinations remain representable;
downstream precision views may select a subset without deleting the broader recall pool.

Each v3 support is a typed `{dimension, asserted_value}` proposal. A current/historical candidate state requires an
evidence claim with the same dimension and temporal value; a dimension-only label is accepted only while reading a
retained v1 prior wave and is never assigned an invented value. Every evidence row carries a
`subject_handle` equal to the candidate. For posts, mentions, and threads, the URL author and status ID must equal
`author_handle` and `post_id`; `self` evidence also requires author equals candidate. Bio evidence must bind the exact
candidate profile and has `thread_relation=null`. Every non-Bio row requires one Stage-2-aligned relation:
`self_post`, `reply`, `quote`, `thread_root`, or `thread_reply`. The relation is a model-mediated classification and
is deliberately excluded from the immutable source-identity fingerprint. Nullable strings must be either null or nonempty. In live mode a profile URL must equal
`https://x.com/<exact handle>`. Handles are unique under casefold.

Prior handles are completion exclusions, not permanent suppression:

- a new handle uses `overlap_status=novel`;
- a prior handle uses `overlap_status=prior_material_update` only when local comparison finds a new source-identity
  fingerprint or a strict dated transition proof for a current/historical state;
- zero-evidence `ambiguous/ambiguous`, merely repeating an old source, or changing model-editable excerpt prose fails.

The raw inner result's reported calls, query list, observations, and `local_reconciliation` are diagnostic and may
disagree with each other or the transport ledger. They remain unmodified inside raw outer `text`. After the session
proof validates, the operator writes `sanitized.json` with `counts.candidates_retained` and all
`local_reconciliation` candidate/evidence/post-URL/tool fields recomputed from the inner arrays and completed raw
session calls. The reported provenance fields remain diagnostic so KPI analysis can expose, rather than erase, model
versus-ledger discrepancies.

Result schema v2 remains byte-for-byte available only for replay of previously sealed bundles. New command policies,
compiled prompts, grants, and executions bind v3. Bundle replay selects v2 only when the recorded schema digest and
replay-only structured-v2 command-policy digest agree; either digest alone is insufficient, and v2 cannot authorize a
new grant.

A live `completed` state additionally requires the headless envelope and raw Grok session transcript to prove the
effective model, one closed set of native-X tool starts/completions, exact parsed arguments, a unique prompt chain,
outer terminal `end_turn`, model turns, token usage, and estimated cost. Post bodies remain model-mediated
(`provider_post_bodies_replayable=false`); transcript proof authenticates execution facts, not every quoted X payload.

Grok 0.2.101 extended usage owns three independent counters: uncached `input_tokens`, `output_tokens`, and
`cache_read_input_tokens`; outer `total_tokens` must equal their exact sum. The receipt keeps
`cache_read_input_tokens` as an optional additive v3 field so retained pre-extension receipts remain replayable. Its
absence is valid only when `total_tokens=input_tokens+output_tokens`; a positive cache delta must be explicit. The
request currently has no distinct cache-price field, so the emergency estimate conservatively charges cache reads at
the full configured input-token rate. Both the token ceiling and bundle replay use the provider-inclusive total.

`contracts/x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json` additionally records unique mechanically
classified keyword-query attempts in `session_proof.candidate_surface_attempts`. Reconciliation projects those exact
query hashes onto each returned candidate as separate `authored_post` and `authored_reply` attempted states. A global
reply query, a query containing more than one `from:` handle, or any non-keyword native-X tool cannot satisfy this
coverage. Bundle replay reparses the retained transcript and recomputes both structures, so editing both receipt
copies consistently still fails. This proves that a scoped search was attempted; it does not prove exhaustive X
results or turn model excerpts into source-bound evidence.

The coverage classifier admits only a closed conjunctive query subset. Any standalone Boolean `OR` token or pipe
alternative makes the whole query unattributed, even when exactly one branch contains `from:<handle>` and a valid
reply filter. The v3 prompt requires separate conjunctive vocabulary queries for the status-gating pair; broader
Boolean searches remain allowed for discovery but cannot claim per-handle authored-surface coverage.

For result status ownership, the model may propose `X_SEARCH_OK`, `X_SEARCH_PARTIAL`, or `X_SEARCH_BLOCKED`; the
operator owns a single monotonic downgrade. If any retained candidate has `pretraining_experience_state` ambiguous or
unsupported and lacks either mechanically classified per-handle authored-Post or authored-Reply attempt, an OK result
is projected to PARTIAL and a counted coverage limitation is appended. The operator never upgrades status, removes a
candidate, or treats a broad Reply query as per-handle coverage. Raw stdout retains the model proposal, while
`sanitized.json`, the receipt coverage matrix, and bundle replay expose the operator-owned final state.

## Recall-campaign bridge is fail-closed

`src/x_first/adaptive_recall_campaign_bridge.py` and
`contracts/x.grok.adaptive_recall_wave.campaign_bridge.v1.schema.json` provide the only current adaptive-to-campaign
boundary. The bridge holds the run's exclusive lease for the complete read/build/recheck interval, pins the run-root
device/inode, first replays `validate_operator_bundle`, then descriptor-reads the canonical private request, operator
receipt, sanitized result, and retained transcript under their request-owned byte ceilings. Before returning it
re-reads the exact four artifacts, compares every bound hash, replays the bundle again and verifies the run-root
identity is unchanged. It accepts only a live `completed` run with `session_proof.status=verified`.

Even for that valid input, v1 always emits:

- `campaign_admission=blocked`;
- `source_payload_status=replay_unavailable`;
- `reason_code=native_x_source_payload_not_replayable`;
- zero captured payload hashes;
- `wave_input=null` and zero campaign, product, live-provider, identity, evidence-claim, outreach, or canonical-write
  authority.

The blocker binds the exact request-file, canonical-request, receipt-file, sanitized-result, transcript, and bridge
schema SHA-256 values. It contains no candidates and never manufactures campaign raw-session files. Transcript tool
completion metadata is not the X response payload, so it cannot support a source-bound evidence claim.

This v1 schema has no promotion state. The only future unlock is a new reviewed contract in which the runner retains
the exact native-X response payload bytes for every completed tool call and binds each payload to the provider call,
tool arguments, session/request, candidate subject, canonical X source identity, and retention/deletion lifecycle.
The campaign replay-context owner must then explicitly accept or adapt that proof. Until both owners close those
contracts and an independent review passes, no adaptive result may become a campaign `WaveInput`.

## Output, process ledger, and recovery

Stdout and stderr are read from pipes in bounded chunks and spooled only up to their configured ceilings. Crossing a
ceiling immediately enters process-group cleanup; oversized bytes are never accumulated in the artifact. JSON byte,
depth, and node ceilings produce `technical_limit_exceeded`, not a business-quality verdict.

Prompt bytes are checked before reading. Each prior-wave file is independently byte-bounded, and its JSON is bounded
by depth and node count before handle or evidence extraction. These are high technical ceilings, not array-item or
candidate-count limits.

The v2 wire field `max_session_files` is retained for compatibility but is enforced as a stricter all-entry ceiling:
regular files, directories, and the one exact `ephemeral-home/leader.sock` all count. The scanner rejects every other
socket and all symlinks/special entries, requires exact current-owner `0700` directories and `0600` regular files,
enforces a hard depth ceiling, and checks a monotonic deadline during traversal. Limit observations are recorded at
the admitted ceiling rather than one past it, so a depth overflow records at most 64 and every generated measurement
remains valid under both runtime and receipt schema. Receipts record both regular-file and all-entry counts plus
maximum depth. The executor deadline owns process timeout. The final post-process scan and recovery each receive a
fresh, independent, five-second cleanup deadline; an expired process deadline can therefore never be relabelled
`session_tree_scan_deadline`. Recovery applies the same entry, mode, socket, depth, and bounded-scan rules.
If a dead provider left a structurally invalid or mode-`000` tree, recovery discards its transcript, deletes the tree,
and seals only `crash_recovered` in that same invocation; it never promotes the invalid provider evidence.

Completed session transcripts are admitted only when every actual native-X tool call passes the shared versioned,
closed per-tool argument predicate. All semantic string operands are checked, not merely the first `query`; unknown,
nested, or secondary arguments fail closed, while only explicitly typed count/limit/mode controls are non-semantic.
The shared argument-policy version is part of the tool-registry digest bound into grants and receipts.

The direct child first runs a tiny isolated gated launcher in a new session with `umask 077`. It acknowledges a random inherited
identity token, then blocks. The parent reads a kernel/process-table birth identity and atomically publishes
`process-ledger.json` with PID, PGID, token, birth identity, session, request, run, and a grant-window timestamp. Only
after durable publication and a final grant-expiry check does the parent release the launcher to `execve` the staged
Grok binary with the same PID. If the parent
dies before publication or release, gate EOF exits without running Grok. Every normal or exceptional executor path
attempts bounded group cleanup.

Recovery is explicit and does not retry the provider:

```text
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_adaptive_grok_wave.py --recover-incomplete-run <private-run-directory>
```

If the recorded process group is alive, recovery fails. The operator must explicitly request bounded termination:

```text
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_adaptive_grok_wave.py --recover-incomplete-run <private-run-directory> --terminate-orphan
```

Recovery first acquires the nonblocking run lease. Before pending-publication cleanup, claim publication, process
liveness/termination hooks, spool promotion, copied-home audit/deletion, tainting, or receipt output, it read-binds the
current executor-return journal, process ledger, process spools/pending publications, exact grant consumption, and
active-use claim. A missing consumption record is a typed pre-mutation failure whenever any independently bound
current evidence exists. Pending post-consumption targets are phase-classified from the same immutable registry used
to construct `runtime_layout` and the publication paths, and existence under each registered final name is classified
from that same set. The pre-consumption prompt/request/intent targets remain outside it, even though their final files
normally exist before consumption. A `legacy_recovery` claim origin is not authority to override current evidence.
Only an actual pre-consumption state with no post-consumption evidence may synthesize a legacy claim and retain the
former recovery path. A fully published terminal receipt is not an incomplete run: it returns
`run_already_terminal`, byte-preserving both trees and invoking no process hook. A sibling claim blocks recovery
without mutation. After any recorded process group is confirmed dead, recovery audits
the copied auth before measuring retained session state, durably deletes the ephemeral home, and only then resolves
the claim. Audit/taint or deletion failure leaves the claim blocking retries. If recovery finds a
`live_consumption` claim but the ephemeral home is already absent, D2 ordering proves that audit and durable deletion
completed before the crash; recovery preserves any existing taint, creates no deleted-auth taint, and resolves the
claim. A synthesized or previously retained `legacy_recovery` claim has no such proof. If its home is absent and a
durable process ledger says the provider may have been released, recovery publishes
`post_consumption_execution_not_clean` before resolving the claim; with no ledger, the pre-release legacy state may
resolve cleanly.
Before it signals an apparently live PGID, it enumerates group members and verifies their inherited random token;
if the original leader remains, its current kernel birth identity must also match. Identity is checked again before
SIGKILL, so PID/PGID reuse cannot redirect cleanup. Only after the recorded group is confirmed dead can recovery seal
`crash_recovered`. It replays the request, exact emergency/budget/retention bindings, actual argv digest, process
ledger, private spools, compiled prompt, full ephemeral-home deletion, retained session transcript, result
reconciliation, and any pre-existing sanitized artifact. It then validates the complete candidate receipt as an
in-memory bundle before atomically publishing it. A mismatch blocks publication. Unknown exit and elapsed facts remain
null.

## Terminal states

| State | Meaning |
|---|---|
| `fixture_complete` | Strict deterministic result; zero external process/provider call |
| `completed` | Live child spawned, exit 0, strict headless envelope/inner JSON valid, operator projection written, and session tool transcript fully verified |
| `process_failed` | Spawn/execution error or nonzero exit |
| `timed_out` | The process monotonic deadline fired and cleanup ran; a final scan deadline cannot override this owner |
| `technical_limit_exceeded` | Stream, JSON-complexity, or session-tree ceiling fired and cleanup ran |
| `structured_output_noncompliant` | JSON had a non-whitespace prefix/suffix or no exact object |
| `result_contract_invalid` | Exact JSON syntax but schema/runtime semantics failed |
| `provider_evidence_invalid` | Result JSON was valid but model/tool/usage/terminal transcript proof was missing or invalid |
| `crash_recovered` | Incomplete run sealed after process-group death; never success |

A parsed object can be retained as `sanitized.json` for diagnosis even when a prefix makes the terminal state
noncompliant. No failure state is silently promoted to completion.

## Private artifacts

Each run root is `0700`; regular artifacts are current-owner, one-link, non-symlink `0600` files. Publication uses a
unique `O_CREAT|O_EXCL` pending file, fsync, no-replace hard link, and directory fsync. The bundle contains:

- canonical operator request and pre-spawn intent;
- compiled prompt and its digest binding;
- raw bounded stdout/stderr and optional canonical sanitized result;
- a retained, bounded raw session update log for live execution proof;
- optional process ledger for an actual spawn;
- terminal operator receipt with process, session, budget, retention, artifact, grant, and reconciliation bindings;
- no ephemeral home: it is recursively deleted before terminal publication.

Run-root creation is transactional: if validation or parent fsync fails after `mkdir` but before lease/intent
ownership, the still-empty root is removed and the runtime parent is fsynced again. Purge inventory therefore never
inherits an empty, valid-looking run directory from a failed creation attempt.

`validate_operator_bundle` uses descriptor-bound, owner/link/mode/size-checked reads. It re-derives model, effort,
emergency values, exact actual argv, command policy, tool registry, isolated-environment policy, result-schema hash,
account/auth binding, session proof, usage/cost, and retention deadline from the canonical request and code, then
rehashes the staged executable. A symlink/hardlink swap or forged but internally self-consistent binding fails.

Expired bundles are removed with:

```text
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_adaptive_grok_wave.py --purge-expired
```

Purge acquires each run lease, replays the complete bundle, writes an external durable deletion journal, removes the
run recursively, fsyncs the runtime owner, and writes a deletion receipt bound to the journal and terminal operator
receipt. A later purge reconciles a crash after deletion but before receipt publication.

## Runbook and validation

Offline fixture, the default:

```text
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_adaptive_grok_wave.py --request <private-request.json>
```

Live, only after a separate grant issuance and explicit operator decision:

```text
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_adaptive_grok_wave.py --request <private-request.json> --execute-live
```

The CLI prints only a small state object. It never prints paths, prompts, handles, OAuth data, model stdout, stderr, or
exception details. Its `external_execution` field is derived from the durable `process_spawn_attempted` fact, never
from the requested `fixture|live` mode; a live-mode spawn failure therefore reports `false`.

Focused offline validation:

```text
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  -m unittest tests.test_adaptive_grok_wave_runner -v
```

The suite covers unbounded synthetic candidate arrays; 120 bound prior waves; strict overlap completion; v2
schema/runtime key parity; staged symlinked binary; account/auth binding; strict one-row xAI OIDC locator,
issuer/client/identity and required-scope binding; bounded duplicate-safe JWT payloads; metadata/JWT expiry minimum;
future `iat`/`nbf`; malformed, multi-row, expired, boundary, and near-horizon OAuth state; pre-grant, pre-run-root, and
copied-auth freshness checks; clean-copy reuse; clean-copy/nonzero-exit rejection; 401-style mutation, deletion,
unreadability, post-consumption exception,
idempotent taint, bounded auth-lock contention, durable same-digest active-use exclusion, real concurrent sibling release, legacy-recovery claim
publication, audit/taint-publication failure retention, deletion-failure retention, rotated-auth recovery,
origin-aware current/legacy missing-home claim resolution, clean pre-release recovery/reuse, sibling-grant rejection,
and new-auth recovery;
missing, pre-link, post-link and
gated-release expiry, wrong-scope, and consumed grants; launcher-time replay; exact closed tools;
raw model/tool/usage/terminal proof; prompt-file argv privacy; stream/JSON/session
ceilings; entry-scoped prompt-policy append replay and purge; full actual-argument protected-boundary checks;
strict 0.2.101 headless parsing; retry-state and progress/tool interleaving; outer/session/inner tamper; model-versus-ledger
diagnostic disagreement; replay-only legacy plain command policy;
transactional pre-intent root rollback; independent timeout/cleanup-scan ownership; exact session-tree modes and
mode-`000` no-follow auth deletion/recovery; measurement/schema parity; descriptor symlink rejection; TERM-to-KILL;
active-owner recovery exclusion; full recovery replay; deletion
journal/receipt; lease-held blocked campaign bridge, same-path bundle swap, missing-source and schema/authority mutation
cases; bundle tampering; and
redacted CLI output. It performs no Grok or X live call.

## Residual boundary

- This repair made no additional Grok/X call. It is grounded in the retained 0.2.101 Google DeepMind wave that
  exposed the former plain-prefix, retry/progress, missing-terminal-update, and model-ledger disagreement failures.
- Native-X tool execution counts, model identity, terminal causality, and usage are transcript-verified. X post bodies
  and candidate evidence remain model-mediated discovery leads until a separate source-bound hydration/replay lane
  proves them.
- The outer provider request ID and provider-reported optional cost remain raw-stdout diagnostics bound by the raw
  artifact digest; they are not separate receipt fields. The receipt binds the operator session and computes the
  budget charge from verified input/output usage plus the request-pinned pricing policy.
- `completed` proves the local execution contract only. It is not an independent-review `GO`, product-quality verdict,
  milestone signoff, identity decision, or outreach authorization.
- OAuth/JWT and grant timing still depend on the host wall clock. The runner detects not-yet-valid credentials and
  wall-clock rollback across the grant's in-process checkpoints, but it does not implement a trusted-clock service or
  durable monotonic epoch across separate processes. This remains an explicit P2 operational residual; the bounded
  experiment requires a correctly synchronized host clock.
