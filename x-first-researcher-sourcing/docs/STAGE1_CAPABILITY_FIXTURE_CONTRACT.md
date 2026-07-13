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

## Fail-closed rules

Validation rejects:

- any live execution mode, owner approval claim, model/tool/provider request ID, external call, external page, or cost;
- generic-web provenance, live X/Twitter URL, credential-bearing field, or unbound request hash;
- a second execution/call/page, a sixth observation, deadline overrun, or totals that do not reconcile;
- unknown or inconsistent run/task/verdict values;
- missing or duplicate stable IDs, URL/object-ID mismatch, target-account mismatch, invalid timestamps, or unbounded
  excerpt/full-body retention;
- candidate packets, identity links, assertions, canonical writes, outreach/ranking authorization, or protected-trait
  and proxy fields/values.

The fixture makes no affiliation, employment, relevance, identity-link, exhaustiveness, or outreach claim.

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
