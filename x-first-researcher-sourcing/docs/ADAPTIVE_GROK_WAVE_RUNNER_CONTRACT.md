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
- bounded process-group `TERM`, then `KILL` cleanup.

Those limits protect the workstation from runaway execution or malformed output. They are not recall targets and do
not make model-reported provider counts mechanically true.

The resulting files are private research artifacts only. They cannot write product, canonical identity, ranking,
CRM, export, billing, permission, or outreach state. Protected-identity inference and provider fallback remain false.

## Authority and source-of-truth matrix

| Concern | Owner/source of truth | Fail-closed rule |
|---|---|---|
| Target lab and focus | SHA-bound request | No hard-coded company or research focus |
| Prompt | Private `0600` source plus SHA-256 | Symlink, wrong owner/mode, digest mismatch, or invalid UTF-8 fails |
| Prior waves | Private `0600` files plus SHA-256 | Casefold duplicates inside a prior wave fail |
| Prior completion | Local validator | An old handle may reappear only with new evidence digest or a new temporal state |
| Grok executable | Request digest plus canonical local locator | Stable owner-controlled target is descriptor-copied to a private staged executable |
| OAuth state | Canonical private auth file | Descriptor-copied into isolated `GROK_HOME`; always deleted before terminal receipt |
| Live authority | Preissued request-scoped grant | Missing, expired, changed-scope, tampered, or consumed grant fails before spawn |
| Native-X tool surface | Operator command policy | Exact closed allowlist of four tools; generic web and local/agent tools denied |
| Process identity | Gated launcher, kernel birth identity, inherited random token, and process-group ledger | Target exec is released only after durable ledger; recovery never kills a numeric PGID alone |
| Process cleanup | Monotonic operator | All normal, timeout, output-limit, callback-error, and exception paths are bounded |
| Output truth | Raw private bytes plus local validator | Prefix/suffix, duplicate keys, nonfinite numbers, shape drift, or bad reconciliation fails |
| Tool-call facts | Model-mediated result | Persisted as reported facts; never upgraded to provider-side mechanical proof |

## Request contract

The closed schema is `contracts/x.grok.adaptive_recall_wave.request.v1.schema.json`. Required sections are:

- `target`: configurable `lab_id`, `research_focus_id`, and free-text scope;
- `prompt_source`: private path and exact digest;
- `prior_waves`: any number of private wave locators and digests;
- `transport`: fixed provider owner, configurable model/effort, and exact live binary digest;
- `emergency`: turn ceiling, deadline, and TERM/KILL grace periods;
- `technical_limits`: high byte and JSON-complexity ceilings;
- `approval.grant_id`: opaque locator for a separately preissued, single-use grant;
- closed negative authority flags.

The runtime additionally enforces `max_json_bytes <= max_stdout_bytes`, owner-only files, cross-field profile URLs, and
the state relationships that plain JSON Schema cannot express.

Example fixture request shape:

```json
{
  "schema_version": "x.grok.adaptive_recall_wave.request.v1",
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
    "grok_binary_sha256": null
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
    "max_prior_json_nodes": 250000
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

## Native-X command and isolation

The live process executes the staged binary in an empty owner-only workspace with an isolated owner-only `HOME`,
`GROK_HOME`, temporary directory, and XDG directories. It does not inherit API keys, plugin/MCP configuration, hook
configuration, shell state, or proxy environment variables. The base prompt is in a `0600` file and is passed through
`--prompt-file`; prompt text and handles never enter argv.

The relevant policy is:

```text
--output-format plain
--tools x_keyword_search,x_semantic_search,x_user_search,x_thread_fetch
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
file tool. The closed `--tools` list is the primary capability boundary; the denylist is defense in depth.

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

The canonical OAuth file must be a current-owner, one-link, regular `0600` file within the size ceiling. It is also
descriptor-copied with pre/post identity checks. The temporary copy is removed in success and exception paths, and a
terminal receipt requires both `ephemeral_auth_deleted=true` and a fully private retained tree.

## Preissued single-use live grant

Grant creation and execution are separate commands. Issuance is write-only local authority preparation; it never
starts Grok:

```text
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_adaptive_grok_wave.py --request <private-request.json> --issue-live-grant
```

The owner-only grant binds the request ID, target, model, emergency envelope, technical envelope, and the SHA-256 of
the complete execution scope. Its default lifetime is 900 seconds and cannot exceed 3600 seconds. The opaque grant ID
is stored only as a digest.

After binary and auth preflight, live execution publishes exactly one `O_EXCL` consumption record. Reuse therefore
fails before process spawn. Changing target, prompt digest, prior inputs, model, executable digest, limits, authority,
or any other execution-scope field invalidates the grant. The durable grant and consumption hashes are replayed by
`validate_operator_bundle` rather than trusted from the receipt.

## Result and prior-wave semantics

`contracts/x.grok.adaptive_recall_wave.result.v1.schema.json` keeps target-lab affiliation and pretraining experience
as two independent temporal dimensions: `current`, `historical`, `ambiguous`, or `unsupported`. Current lab plus
historical pretraining, historical lab plus historical pretraining, and other combinations remain representable;
downstream precision views may select a subset without deleting the broader recall pool.

Each current/historical state requires evidence explicitly supporting that dimension. Every evidence row carries a
`subject_handle` equal to the candidate. For posts, mentions, and threads, the URL author and status ID must equal
`author_handle` and `post_id`; `self` evidence also requires author equals candidate. Bio evidence must bind the exact
candidate profile. Nullable strings must be either null or nonempty. In live mode a profile URL must equal
`https://x.com/<exact handle>`. Handles are unique under casefold.

Prior handles are completion exclusions, not permanent suppression:

- a new handle uses `overlap_status=novel`;
- a prior handle uses `overlap_status=prior_material_update` only when local comparison finds a new source-identity
  fingerprint or a strict dated transition proof for a current/historical state;
- zero-evidence `ambiguous/ambiguous`, merely repeating an old source, or changing model-editable excerpt prose fails.

The result's candidate, evidence, post-URL, query, tool-count, and observation counts are locally reconciled. Tool
names and counts must agree internally, but they remain model-reported because the CLI does not expose an independent
provider event log in this lane. `provider_post_bodies_replayable` is always false.

## Output, process ledger, and recovery

Stdout and stderr are read from pipes in bounded chunks and spooled only up to their configured ceilings. Crossing a
ceiling immediately enters process-group cleanup; oversized bytes are never accumulated in the artifact. JSON byte,
depth, and node ceilings produce `technical_limit_exceeded`, not a business-quality verdict.

Prompt bytes are checked before reading. Each prior-wave file is independently byte-bounded, and its JSON is bounded
by depth and node count before handle or evidence extraction. These are high technical ceilings, not array-item or
candidate-count limits.

The direct child first runs a tiny isolated gated launcher in a new session. It acknowledges a random inherited
identity token, then blocks. The parent reads a kernel/process-table birth identity and atomically publishes
`process-ledger.json` with PID, PGID, token, birth identity, session, request, run, and timestamp. Only after durable
publication does the parent release the launcher to `execve` the staged Grok binary with the same PID. If the parent
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

Before recovery signals an apparently live PGID, it enumerates group members and verifies their inherited random token;
if the original leader remains, its current kernel birth identity must also match. Identity is checked again before
SIGKILL, so PID/PGID reuse cannot redirect cleanup. Only after the recorded group is confirmed dead can recovery seal
`crash_recovered`. It replays the request, process ledger, private spools, compiled prompt, auth deletion, result
reconciliation, and any pre-existing sanitized artifact. A mismatched sanitized artifact blocks receipt publication.
Unknown exit and elapsed facts remain null.

## Terminal states

| State | Meaning |
|---|---|
| `fixture_complete` | Strict deterministic result; zero external process/provider call |
| `completed` | Live child spawned, exit 0, exact JSON, strict result valid |
| `process_failed` | Spawn/execution error or nonzero exit |
| `timed_out` | Monotonic deadline fired and cleanup ran |
| `technical_limit_exceeded` | Byte or JSON-complexity ceiling fired and cleanup ran |
| `structured_output_noncompliant` | JSON had a non-whitespace prefix/suffix or no exact object |
| `result_contract_invalid` | Exact JSON syntax but schema/runtime semantics failed |
| `crash_recovered` | Incomplete run sealed after process-group death; never success |

A parsed object can be retained as `sanitized.json` for diagnosis even when a prefix makes the terminal state
noncompliant. No failure state is silently promoted to completion.

## Private artifacts

Each run root is `0700`; regular artifacts are current-owner, one-link, non-symlink `0600` files. Publication uses a
unique `O_CREAT|O_EXCL` pending file, fsync, no-replace hard link, and directory fsync. The bundle contains:

- canonical operator request and pre-spawn intent;
- compiled prompt and its digest binding;
- raw bounded stdout/stderr and optional canonical sanitized result;
- optional process ledger for an actual spawn;
- terminal operator receipt with process, artifact, grant, and reconciliation bindings;
- isolated retained runtime tree with auth removed.

`validate_operator_bundle` reopens and recomputes these inputs. It re-derives model, effort, max turns, exact tool and
prompt-file flag policy, isolated-environment policy, and result-schema hash from the canonical request and code, then
rehashes the retained staged executable. A forged but internally self-consistent command binding therefore fails. The
validator does not accept a self-attested receipt hash as proof.

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

The suite covers unbounded synthetic candidate arrays; 120 bound prior waves; strict overlap completion; schema/runtime
key parity; staged symlinked binary; isolated OAuth; missing, expired, wrong-scope, and consumed grants; exact closed
tools; prompt-file argv privacy; bounded streaming; JSON structure limits; TERM-to-KILL; process-ledger recovery; bundle
tampering; and redacted CLI output. It performs no Grok or X live call.

## Residual boundary

- No real Grok/X execution was performed by this implementation slice.
- Native-X queries, tool counts, post bodies, and candidate evidence remain model-mediated discovery leads until a
  separate source-bound hydration/replay lane proves them.
- `completed` proves the local execution contract only. It is not an independent-review `GO`, product-quality verdict,
  milestone signoff, identity decision, or outreach authorization.
