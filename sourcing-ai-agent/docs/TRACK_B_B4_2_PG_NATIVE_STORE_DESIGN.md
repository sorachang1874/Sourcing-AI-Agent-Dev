# Track B B4.2 — Forward-looking PG-native typed store (DESIGN — RATIFIED; execution in progress)

> Status: **RATIFIED by owner (2026-06-21/23); execution in progress.** Roadmap: ① dead-shadow/mirror
> teardown — executed as **B4.3, 100% complete 2026-07-02** (`storage.py` is PG-pure; mirror machinery
> deleted) → ② repository query methods + per-domain caller migration — **current round**, execution entry
> doc: `docs/TRACK_B_REPOSITORY_MIGRATION_HANDBOOK.md` → ③ jsonb/timestamptz data migration — **still
> owner-gated** (explicit GO + full-stop window). Ratified deltas vs. this doc's recommendations:
> repositories become the PUBLIC API (callers migrate directly; no permanent facade); pilot domain =
> `linkedin_profile_registry`. Builds on the completed B4.1 (shadow schema removed, commit d6f6f1a) and the
> deficiency assessment in [[track_b_pg_pure_store]] (workflow wf_c0a3b7c3).

## §0 Why B4.2 (recap of the deficiencies)

After B4.1, `ControlPlaneStore` is PG-only for schema, but the *method layer* is still the inherited
SQLite-shaped, non-specialist design. The six structural deficiencies (located, with costs):

1. **SQLite-shaped schema ported onto PG.** 223 JSON columns are `TEXT` (0 `jsonb`); timestamps are `TEXT`;
   a hand-written `_sqlite_decl_to_postgres_type` translator. Cost: ~479 Python `json.dumps/loads`
   round-trips, no in-DB JSON indexing/querying, ad-hoc `::jsonb` casts, lexicographic time sorting, no tz
   correctness. **The single biggest "non-specialist author" tell.**
2. **516 ad-hoc methods, no repository/query layer.** Column lists, placeholders, conflict policies, and pk
   knowledge are duplicated 2–3× per table across `storage.py` SQL, native PG SQL, `_PRIMARY_KEY_COLUMNS`,
   `_SERIAL_SEQUENCE_NAMES`, and the DDL. Every schema change touches 4+ places.
3. **Untyped string dispatch.** `getattr(adapter, method_name)` by string (109 sites) + `row_builder: Any`
   (197 sites) + raw `where_sql` string fragments throw away every static guarantee.
4. **Double-write-then-mirror** (at design time ~274 dead
   SQLite tails + `_mirror_control_plane_row` ×73 + `_replace_control_plane_table_from_sqlite` ×16).
   **RESOLVED by B4.3 (100% complete 2026-07-02): all deleted; `storage.py` is PG-pure.**
5. **Schema source-of-truth inversion** — **RESOLVED in B4.1a/B4.1**: migrations are now the sole source.
6. **Routing scaffolding all constant-true** post-B3.0 yet evaluated on the hot path; three parallel
   registries (`CONTROL_PLANE_LIVE_TABLES` / `_CONTROL_PLANE_POSTGRES_NATIVE_TABLES` /
   `_NATIVE_READ_METHODS`) hand-synced and **had drifted** (`target_candidate_public_web_runs` — a
   logical table that silently never read PG). Moot since B4.3: the SQLite path no longer exists, so all
   reads are PG-native.

## §1 The dead-tail cleanup is part of B4.2, not a separable sweep (empirical finding 2026-06-21)

> **Superseded:** the cleanup was in fact executed as its own batched sweep — the **B4.3 shadow teardown**
> (using exactly the per-method, sentinel-preserving collapse described below) — and is **100% complete
> 2026-07-02**: `storage.py` contains zero SQLite code. Kept as historical rationale.

The ~274 dead SQLite tails cannot be removed by a blind deletion. A blanket AST truncation (cut each method
at its dead `with self._lock, self._connection:`) **broke 231 methods**: many read methods return their
empty-case sentinel (`[]`/`{}`/`None`/`0`) *from the SQLite path itself*, not from an explicit guard before
it, so truncation made them fall through to `None`. Removing a dead tail therefore requires the
**per-method, sentinel-preserving collapse** (the B3.2 pattern: `if not postgres_rows: return <sentinel>;
return postgres_rows`) — which is exactly what rewriting the method to the typed-repository form does. So
the cleanup is **subsumed by the B4.2 per-method rewrite**, not done separately. (B4.1 was reverted from the
truncation experiment; the init_schema-removal milestone stands.)

## §2 Target architecture

A typed, decomposed, PG-native store. Four pillars:

### §2.1 One typed table descriptor per table (single source)
A declarative descriptor: columns (name + PG type), primary key, serial sequences, conflict policy
(`ON CONFLICT` target + per-column merge rule: `excluded` / `GREATEST` / keep-if-nonempty / monotonic),
jsonb columns, timestamptz columns. The descriptor **generates and caches** the upsert / select / delete
SQL. Replaces `_PRIMARY_KEY_COLUMNS`, `_SERIAL_SEQUENCE_NAMES`, the per-table hand-written SQL, and the DDL
column lists — schema knowledge lives in **one** place per table. The baseline migration is generated from
the descriptors (or the descriptors are validated against the migration by the drift guard).

### §2.2 Per-domain repositories (decompose the God-class)
Split the 28k-line `ControlPlaneStore` into per-domain repositories (jobs, candidates/evidence,
crm, public-web, projection/serving, agent-runtime, linkedin-registry, criteria/confidence,
asset-materialization, …) over a shared typed primitive layer. `ControlPlaneStore` becomes a thin facade
that composes the repositories (preserving its public import surface so callers don't change), or callers
migrate to the repositories directly over time (strangler). Each repository owns its tables' descriptors +
domain methods.

### §2.3 Typed query primitives (kill the string dispatch)
Replace `getattr(adapter, method_name)`-by-string, `row_builder: Any`, and raw `where_sql` fragments with:
typed row models (dataclasses/`TypedDict` or pydantic) per table, a small typed query builder (or a thin
typed wrapper over the existing `select_one/select_many/upsert_row` primitives with descriptor-driven SQL),
and `RETURNING`-based writes that return the persisted row (no re-SELECT, no mirror). mypy/IDE/call-graph
work again.

### §2.4 Unit-of-work / transaction discipline + bounded reads
A per-operation connection/transaction boundary (unit-of-work) instead of per-call `with self._connect()`.
Reads get a mandatory `limit` discipline (kill the 52 `limit=0` unbounded reads; the 6 of 15 `list_*`
without a `limit` param get one). This is the 20-concurrent-user serving hardening.

## §3 Schema modernization — full jsonb + timestamptz (owner-ratified scope)

- **223 `*_json TEXT` → `jsonb`.** Add **GIN indexes** only where we actually query into the JSON (most
  stay plain jsonb; index the ones with containment/path filters). Drops the ~479 `json.dumps/loads`
  round-trips at the boundary — the typed mapper (de)serializes once.
- **Timestamps `TEXT` → `timestamptz`.** Real chronological ordering + tz correctness; kills lexicographic
  time sorting. Default `now()` server-side.
- **Native arrays / enums where clearly apt** (e.g. status enums, id-list columns) — case-by-case, not
  forced.

## §4 Production data-migration plan (the Contract change — staged + reversible)

The schema lives in versioned migrations (`migrations/000N_*.sql`) applied by the runner. The jsonb/tz
change is one or a few new migrations:

1. **`ALTER TABLE ... ALTER COLUMN x TYPE jsonb USING x::jsonb`** per JSON column (all current values are
   valid JSON text — verified by the existing `json.dumps` writers; a pre-migration validation pass scans
   for any non-JSON rows and fails closed before the ALTER). Timestamps:
   `ALTER COLUMN ts TYPE timestamptz USING ts::timestamptz` (current values are ISO-8601 text).
2. **Reversibility:** each forward ALTER has a documented inverse (`jsonb → text USING x::text`); the
   migration runner is forward-only, so rollback is a new down-migration prepared in advance, not an
   automatic revert. Keep a tested rollback script.
3. **Deploy window:** the existing PG-only contract already requires a **full-stop deploy** (advisory-lock
   identity, [[recovery_driver_facts]] / the schema-namespacing note). The `ALTER ... TYPE` rewrites the
   table (takes a lock); run it in the controlled full-stop window. Estimate per-table rewrite time on a
   prod-sized copy first.
4. **Validation:** post-migration, the drift guard (B4.1a, now `ensure_bootstrapped`-based) + a typed
   round-trip test (write typed model → read back → equals) + the full contract lane.
5. **Code/schema ordering:** land the typed mapper that can read *both* TEXT and jsonb first (tolerant
   read), then migrate the schema, then drop the TEXT-compat path. This decouples the code deploy from the
   data migration (no big-bang).

## §5 Sequencing (incremental strangler, each step verifiable)

> **Progress (2026-07-02):** foundations B4.2.0–.12 landed (`control_plane_repository.py` typed
> descriptor layer + `src/sourcing_agent/repositories/`; pilot = `linkedin_profile_registry`; 34 tables
> read/write single-source). The dead-tail cleanup completed separately as B4.3, and the
> `target_candidate_public_web_runs` drift is moot (no SQLite path remains). Per-domain execution is now
> tracked in `docs/TRACK_B_REPOSITORY_MIGRATION_HANDBOOK.md`.

1. **B4.2.0 — typed primitive layer + first descriptor.** Build the descriptor + typed-mapper + typed
   query primitives; convert ONE small cohesive domain (e.g. `frontend_history_links` or
   `plan_review_sessions`) end-to-end as the pilot — dead SQLite tail dropped, typed methods, repository
   extracted, public surface preserved. Prove the pattern + the perf.
2. **B4.2.1..n — per-domain repository rewrites.** One domain per batch (jobs, candidates, crm,
   public-web, projection, agent-runtime, linkedin-registry, criteria, materialization). Each batch:
   rewrite methods to typed/RETURNING form (dropping their dead SQLite tails — the cleanup), extract the
   repository, contract-lane verify. Fix the drifted `target_candidate_public_web_runs` registry here.
3. **B4.2.S — schema migration (jsonb/timestamptz).** After (or interleaved with) the typed mapper landing
   the tolerant read, apply the §4 migration in the deploy window.
4. **B4.2.F — finalize.** *Largely done by B4.3:* the mirror machinery, `self._conn`/`_configure_connection`,
   and the adapter's `replace_table_from_sqlite` / `sync_runtime_control_plane_to_postgres` are already
   deleted; the shadow accessors survive only as inert retired labels. Remaining: delete the constant-true
   routing scaffolding and retire `ControlPlaneStore` in favor of repositories as the public API (ratified
   endstate — callers migrate directly; no permanent facade).

## §6 Risks & non-negotiables

- **Behavior preservation per method.** Each rewrite is characterize-first + contract-lane verified; the
  invariant-7 row-absent sentinels (`None`/`{}`/`[]`/`0`/raise) are preserved exactly (this is what the
  blind truncation got wrong).
- **No big-bang.** Strangler, one domain per batch, green at every commit.
- **The data migration is the only true Contract change** — gated on this ratification + a full-stop
  window + a prepared rollback.
- **Scope honesty.** This is multi-session, large. It is the "改革性的重构" the owner asked for; the payoff is
  a typed, jsonb-native, decomposed, query-efficient store fit for the 20-concurrent-user serving target
  and the agent-native direction.

## §7 Decisions needed from the owner

> **All decided (owner 2026-06-21/23):** 1) data migration ratified as roadmap step ③, execution still gated
> on an explicit GO + full-stop window; 2) repositories become the **PUBLIC API** — callers migrate directly
> and the God-class facade retires (not the facade-first recommendation); 3) pilot domain =
> `linkedin_profile_registry` (not the candidates recommended below); 4) typed/tolerant-read code first,
> schema migration deferred to gated step ③ after the ② repository round.

1. **Ratify the data migration** (jsonb + timestamptz) per §4 — the Contract change.
2. **Repository decomposition shape:** thin `ControlPlaneStore` facade composing repositories (preserve
   import surface), vs. migrate callers to repositories directly. (Recommend: facade first, migrate callers
   opportunistically.)
3. **Pilot domain** for B4.2.0 (recommend a small cohesive one: `plan_review_sessions` or
   `frontend_history_links`).
4. **Timing of the schema migration** relative to the typed-layer rollout (recommend: tolerant-read code
   first, migrate mid-way, drop compat at the end).
