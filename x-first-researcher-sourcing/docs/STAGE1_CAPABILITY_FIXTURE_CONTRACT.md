# Stage 1 capability-probe fixture contract

> Status: offline author-complete; independent review pending. Live X/Grok access and researcher mapping remain
> `NO-GO`.

## Purpose

This slice makes the proposed capability-probe envelope executable and testable without pretending to prove X
access. It uses one synthetic official-lab account, three synthetic public-technical-post observations, reserved
`.invalid` URLs, no real people, and no external execution.

The two sibling contracts are:

- `x.grok.capability_probe.request.v1`
- `x.grok.capability_probe.result.v1`

Both are fixed to `execution_mode=fixture_only`. A future live probe cannot reuse v1 by changing a field; it requires
a new reviewed contract version.

## Owner/source-of-truth matrix

| Concern | Owner/source of truth | Fixture rule | Live deletion condition |
| --- | --- | --- | --- |
| Request shape | request schema + executable validator | Exact synthetic official-account query | Replaced only by approved new version |
| Request binding | canonical sorted-JSON SHA-256 | Result and provenance hashes must match | Never use an unbound result |
| Runtime state | explicit run/task/verdict registries | Only terminal-consistent tuples pass | Live registry requires owner review |
| Capability claim | executable result validator | `fixture_contract_validated`; X access is false | Live proof needs separate handshake contract |
| Provider provenance | result fixture | offline fixture, no model/tool/request ID | Live access mode must be owner-pinned |
| Budget | request + reconciled result usage | maxima `1/1/1/5`; actual external use and cost are zero | Live values require owner approval |
| Target | request fixture | one synthetic official-lab account | Real stable account ID requires owner pin |
| Retention | request/result fixture | synthetic-only, bounded excerpts, no full body | Live TTL/deletion evidence requires privacy owner |
| Safety | sibling AGENTS + executable scanners | unsafe fields/values, credentials, live URLs rejected | No bypass or permissive fallback |
| Fixture pair writer | capability fixture generator | trusted fixture-directory descriptor lock owned by the local OS user; request/result commit as one serialized pair | Replaced only by a reviewed durable artifact writer |
| Canonical/product writers | existing product owners | all writer arrays empty | Separate adjudicated adapter gate |

## State and verdict contract

The result is terminal-total. Accepted tuples are:

| Run | Task | Verdict | Observation rule |
| --- | --- | --- | --- |
| `completed` | `succeeded` | `fixture_contract_validated` | 1–5 synthetic observations |
| `failed` | `failed` | `capability_unavailable` | zero observations, one structured error |
| `failed` | `failed` | `probe_error` | zero observations, one structured error |
| `cancelled` | `cancelled` | `probe_error` | zero observations, one structured error |
| `killed` | `cancelled\|expired` | `killed` | zero observations, one structured error |

Unknown or cross-paired states are invalid. `x_native_proven` is intentionally not a v1 verdict, and
`x_native_access_proven` is always false.

Run duration is derived from `completed_at - started_at` at exact millisecond precision. It must equal
`usage.elapsed_ms`. Every run and observation timestamp has one canonical representation:
`YYYY-MM-DDTHH:MM:SS.sssZ` (ASCII RFC 3339, UTC `Z`, and exactly three fractional-second digits). Offsets, basic or
week-date forms, omitted/sub-millisecond fractions, whitespace, and control characters are rejected rather than
normalized or rounded. The fixed
`raw_response_sha256` is the digest of the canonical synthetic raw bytes owned by the executable contract, not an
unbound caller-supplied hash.

Fixture regeneration refuses symlink destinations, and `--check` treats symlink fixtures as stale even when their
targets contain the expected bytes. The generator opens the existing fixture directory itself with
`O_DIRECTORY|O_NOFOLLOW`, verifies that its descriptor and pathname still name the same user-owned directory inode,
rejects group/other-writable POSIX mode bits, and takes a non-blocking advisory `flock` on that descriptor. This
macOS-supported lock has no persistent lock
pathname or residue, so replacing the former `.x-first-capability-fixture.pair.lock` inode cannot split holder and
contender onto independent locks. An in-process mutex covers threads. Both layers share one five-second monotonic
acquisition budget.

The exclusive directory lock is held across destination preflight, stale-temp reaping, both temporary writes and
atomic replacements, rollback, temporary cleanup, and final directory fsync. Directory descriptor/path identity is
rechecked at acquisition and each mutation boundary. Therefore another cooperating writer in the same trusted
fixture directory cannot reap an active temporary file, snapshot a half-written pair, or roll an earlier generation
back over a later successful generation.
Writes materialize every candidate file in an owned, same-directory temporary file, fsync it, and use atomic
replacement. A successful repair reaps only temp files in the generator-owned
`.x-first-capability-fixture.<destination>.<32 lowercase hex>.tmp` namespace; other files remain untouched. A normal
multi-file replacement failure rolls already replaced files back to their prior bytes. A hard process loss releases
the kernel lock; the next writer reaps owned orphan temps and replaces both files, while the request hash, `--check`,
and validator continue to reject any half-written pair before repair.

The fixture directory is an explicit operator-controlled trust boundary: it and its parent must not be writable by
an untrusted actor. The descriptor/path checks detect identity changes at the tested acquisition and mutation
boundaries, but the writer does not claim protection against an attacker that can replace the trusted directory
itself between system calls. In that environment the offline generator must not be run.

## Fail-closed rules

Validation rejects:

- any live execution mode, owner approval claim, model/tool/provider request ID, external call, external page, or cost;
- generic-web provenance, live X/Twitter URL, credential-bearing field, or unbound request hash;
- a second execution/call/page, a sixth observation, deadline overrun, or totals that do not reconcile; observation
  validation and content scanning are sliced to the fixed five-item maximum before iteration, producing one static
  overflow diagnostic and no submitted array index above four;
- a second structured error; error validation and content scanning are sliced to the schema's fixed one-item maximum
  before iteration, producing one static overflow diagnostic and no submitted array index above zero;
- unknown or inconsistent run/task/verdict values;
- missing or duplicate stable IDs, target-account mismatch, invalid timestamps, or unbounded excerpt/full-body
  retention;
- any raw URL that is not byte-for-byte equal to the object-bound
  `https://posts.invalid/{handle}/status/{object_id}` string before defensive parsing, including case variants, empty
  query/fragment delimiters, leading whitespace/C0 bytes, or inserted LF/CR/TAB characters;
- excerpts or terminal error messages outside their exact object/verdict-bound synthetic template registries;
- parser exceptions, raw-response hash drift, or a mismatch between run timestamp duration and `elapsed_ms`;
- any structured terminal error outside its verdict-bound envelope; v1 errors always have `retryable=false` in both
  the executable registry and declarative schema;
- candidate packets, identity links, assertions, canonical writes, outreach/ranking authorization, or protected-trait
  and proxy fields/values.

All executable validation diagnostics use one of the fixed codes `XCAP_REQUEST_INVALID`,
`XCAP_BOUND_REQUEST_INVALID`, or `XCAP_RESULT_INVALID`. Diagnostics may identify only static schema paths and bounded
array indices. They never interpolate submitted values, unknown field names or paths, status/verdict strings,
observation IDs, excerpts, terminal-error text, credentials, protected-trait text, or person-like text. This applies
equally to the Python validation API and CLI JSON output. CLI file reads, JSON decoding, top-level object checks, and
unexpected validation exceptions are caught at the command boundary and collapse to fixed code/message pairs; input
paths and submitted values are never emitted, and no traceback is returned.

The fixture makes no affiliation, employment, relevance, identity-link, exhaustiveness, or outreach claim.

## Fixture profile extraction boundary

Version 1 intentionally hard-codes the exact OpenAI synthetic account, IDs, query, schema constants, and fixture
paths. That bounded duplication is deleted before — not after — any second lab becomes fixture-enabled. The extension
condition is an owner-reviewed contract batch that introduces a typed `CapabilityFixtureProfile` registry containing
the lab ID, synthetic account ID/handle, probe/run/task IDs, fixed query, URL namespace, and template registry. That
batch must version schemas and generate independently validated fixtures/tests for every profile.

Do not extract the registry speculatively in v1, and do not interpret a future second profile as approval for live or
multi-lab collection. Until the extension condition is met, any non-OpenAI target fails closed.

## Local validation

From `x-first-researcher-sourcing/`:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  -m unittest tests.test_x_first_capability_probe -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  -m x_first.capability_probe
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/generate_capability_probe_fixtures.py --check
../sourcing-ai-agent/.venv/bin/ruff check .
```

The full sibling unittest discovery must also pass so Stage 1 cannot weaken Stage 0.

## Deferred live gate

Before a real capability call, owners must approve the exact user trigger, official account stable ID, legal/privacy
basis, access mode/model/tool, cost/rate/deadline/kill-switch values, retention/deletion policy, and result storage.
Then a new live-capable schema/validator/runner batch needs independent adversarial review. A valid synthetic v1 result
does not satisfy any of those gates and does not authorize Stage 2.
