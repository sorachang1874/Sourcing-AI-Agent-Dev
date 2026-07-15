# Adaptive Grok Native-X Recall-Wave Contract

Status: offline implementation and fake-live contract tests complete; real Grok/X execution remains explicitly
operator-triggered and review-gated.

## Product outcome

`src/x_first/adaptive_grok_wave_runner.py` runs one target-neutral Grok research wave using only the four native X
tools exposed by the local Grok CLI. Its goal is broad, evidence-bearing discovery. It does **not** cap the number of
candidates, observations, search queries, evidence items, excluded examples, or native-X calls as a business rule.

The model should continue varying X-native search strategies while they produce materially new evidence. Search
saturation and marginal yield are evaluated after the wave; they are not encoded as an arbitrary `25 candidates`,
`200 observations`, or `16 calls` cutoff.

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
| Prior waves | Private `0600` files plus SHA-256 | Casefold duplicates inside a prior wave fail |
| Prior completion | Local validator | An old handle may reappear only with new evidence digest or a new temporal state |
| Grok executable | Request digest plus canonical local locator | Stable owner-controlled target is descriptor-copied to a private staged executable |
| OAuth state | Canonical private auth file and request-pinned digest/account reference | Descriptor-copied into isolated `GROK_HOME`; the entire ephemeral home is deleted before terminal receipt |
| Live authority | Preissued request-scoped grant | Account/auth, model, command/tool/schema/environment policies, emergency, budget, retention, and request scope are bound; expiry is checked before consumption, after the exclusive link, and at the gated target release |
| Native-X tool surface | Operator command policy | Exact closed allowlist of four tools; generic web and local/agent tools denied |
| Run ownership | Random durable `run.lock` token plus nonblocking `flock` | Execution owns the lease for the whole run; recovery/purge mutate nothing while an active owner exists |
| Process identity | Gated launcher, kernel birth identity, inherited random token, process-group ledger, and run lease | Target exec is released only after durable ledger; recovery never kills a numeric PGID alone |
| Process cleanup | Monotonic operator | All normal, timeout, output-limit, callback-error, and exception paths are bounded |
| Output truth | Raw private bytes plus local validator | Prefix/suffix, duplicate keys, nonfinite numbers, shape drift, or bad reconciliation fails |
| Tool-call facts | Raw Grok session `updates.jsonl` | Effective model, native-X starts/completions, arguments, terminal causality, turns, token usage, and cost are replayed; model-reported counts must match |
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
immutable owner semantic fails replay. The production registry currently authorizes seven tracked OpenAI prompts
and three tracked Google DeepMind prompts. Each lab has its own exact target tuple; prompt digests cannot be swapped
between them. The OpenAI tuple is:

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
--output-format plain
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
file tool. Grok CLI 0.2.99 does **not** map the hosted `x_keyword_search`, `x_semantic_search`, `x_user_search`, or
`x_thread_fetch` names through its built-in `--tools` allowlist. A 2026-07-15 live A/B produced zero native-X calls
when those names were passed and immediately exposed `x_user_search` when the flag was removed. The command therefore
does not pass `--tools`. `--disable-web-search` plus the local-tool denylist constrain the launch surface, and the
retained session proof accepts only the four registered native-X tool names and their closed argument profiles before
the result can complete. The registry digest explicitly binds both `cli_native_x_allowlist_enforced=false` and
`native_x_session_proof_required=true` so replay cannot silently restore the broken flag or weaken evidence checks.

The operator intentionally uses `--output-format plain` and embeds the result schema in the compiled prompt. On the
locally inspected Grok CLI, `--json-schema` changes output to a CLI envelope; plain mode is needed to preserve exact
model stdout and detect non-JSON prefixes. The executable digest, result-schema digest, and redacted flag-policy digest
are all bound into the receipt.

## Binary and OAuth preflight

The configured Grok locator may be an owner-controlled symlink. Before grant consumption, the runner:

1. resolves the canonical final target;
2. opens it with `O_NOFOLLOW`;
3. checks regular-file type, owner, link count, user-executable mode, nonempty size, and no group/world write bits;
4. descriptor-copies and hashes it into `run/executable/grok` with mode `0700`;
5. compares pre-copy fd, post-copy fd, canonical path, and locator identities;
6. executes only the staged copy.

The canonical OAuth file must be a current-owner, one-link, regular `0600` file within the size ceiling and must match
the request-pinned digest. It is descriptor-copied with pre/post identity checks. The durable intent is published
before auth is copied. From the copy through grant consumption, executor completion, bounded session-tree measurement,
and transcript capture, every exit deletes the complete ephemeral home in a `finally` boundary. Normal execution
deletes it before raw stdout/stderr publication, structured-output parsing, sanitized-result publication, or terminal
receipt construction. A terminal receipt requires `ephemeral_tree_deleted=true`; a post-executor publication failure
can be recovered from the durable intent/spools without retaining OAuth bytes. Because the provider can write this
tree, deletion restores owner-only traversal mode through no-follow directory descriptors and unlinks without
following provider-created links. A provider that changes any directory away from `0700` or regular file away from
`0600` fails the session-tree boundary; even mode-`000` nested directories are deleted without an operator chmod step.

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
grant expiry and
publishes exactly one `O_EXCL` consumption record bound to the run lease. It then reads the authoritative clock again
after the exclusive link. If the grant expired in that window, the record remains consumed but the executor is never
invoked. A conservative monotonic deadline is derived from the remaining lifetime. The gated launcher rechecks both
wall and monotonic time after its durable process ledger is published and immediately before target `execve`; expiry
closes the gate and the staged Grok binary never runs. Reuse therefore fails before target execution. Changing any
execution-scope field invalidates the grant. The durable grant, consumption and launcher timestamp are replayed by
`validate_operator_bundle` rather than trusted from the receipt.

## Result and prior-wave semantics

`contracts/x.grok.adaptive_recall_wave.result.v2.schema.json` keeps target-lab affiliation and pretraining experience
as two independent temporal dimensions: `current`, `historical`, `ambiguous`, or `unsupported`. Current lab plus
historical pretraining, historical lab plus historical pretraining, and other combinations remain representable;
downstream precision views may select a subset without deleting the broader recall pool.

Each v2 support is a typed `{dimension, asserted_value}` proposal. A current/historical candidate state requires an
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

The result's candidate, evidence, post-URL, query, tool-count, and observation counts are locally reconciled. A live
`completed` state additionally requires the raw Grok session transcript to prove the effective model, one closed set
of native-X tool starts/completions, exact parsed arguments, a unique prompt chain, terminal `end_turn`, model turns,
token usage, and estimated cost. Transcript tool counts must exactly equal the model result. Post bodies remain
model-mediated (`provider_post_bodies_replayable=false`); transcript proof authenticates execution facts, not every
quoted X payload.

`contracts/x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json` additionally records unique mechanically
classified keyword-query attempts in `session_proof.candidate_surface_attempts`. Reconciliation projects those exact
query hashes onto each returned candidate as separate `authored_post` and `authored_reply` attempted states. A global
reply query, a query containing more than one `from:` handle, or any non-keyword native-X tool cannot satisfy this
coverage. Bundle replay reparses the retained transcript and recomputes both structures, so editing both receipt
copies consistently still fails. This proves that a scoped search was attempted; it does not prove exhaustive X
results or turn model excerpts into source-bound evidence.

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

Recovery first acquires the nonblocking run lease, before cleaning pending publications or reading mutable run state.
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
| `completed` | Live child spawned, exit 0, exact JSON, strict result valid, and session transcript fully verified |
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
schema/runtime key parity; staged symlinked binary; account/auth binding; missing, pre-link, post-link and gated-release
expiry, wrong-scope, and consumed grants; launcher-time replay; exact closed tools; raw model/tool/usage/terminal proof; prompt-file argv privacy; stream/JSON/session
ceilings; entry-scoped prompt-policy append replay and purge; full actual-argument protected-boundary checks;
transactional pre-intent root rollback; independent timeout/cleanup-scan ownership; exact session-tree modes and
mode-`000` no-follow auth deletion/recovery; measurement/schema parity; descriptor symlink rejection; TERM-to-KILL;
active-owner recovery exclusion; full recovery replay; deletion
journal/receipt; lease-held blocked campaign bridge, same-path bundle swap, missing-source and schema/authority mutation
cases; bundle tampering; and
redacted CLI output. It performs no Grok or X live call.

## Residual boundary

- No real Grok/X execution was performed by this implementation slice.
- Native-X tool execution counts, model identity, terminal causality, and usage are transcript-verified. X post bodies
  and candidate evidence remain model-mediated discovery leads until a separate source-bound hydration/replay lane
  proves them.
- `completed` proves the local execution contract only. It is not an independent-review `GO`, product-quality verdict,
  milestone signoff, identity decision, or outreach authorization.
