# Working-tree adversarial remediation reviews — 2026-07-14

Status: **ADVISORY_ONLY**. This records non-author review rounds and author-side dispositions while `HEAD` remained
`c9a26ee92c4c497e336c8851d44a89cb595d75dc`. It is not a pinned review of the eventual remediation commits, not a
formal `GO`, and not live-provider, promotion, product-write, milestone, or outreach authorization.

## Recall campaign

The non-author review reported `P0=0 / P1=1 / P2=2`:

1. persisted evidence did not revalidate URL author and status ID after a coherent record rehash;
2. `thread` was absent from the campaign evidence-kind closure;
3. the aggregate byte ceiling was checked only after multiple files had already been loaded.

Disposition: closed in the working tree. The persisted validator now repeats URL-author/status-ID binding; native-X
threads remain first-class evidence; descriptor-size preflight occurs before content reads, and each preflighted size
is the later read maximum. The focused suite is `27/27` on Python 3.12 and 3.14. The owner-only v5 seven-wave replay
validated twice at `534,287` bytes, mode `0600`, SHA-256
`4f27d046c8618e424c1f24b7dcf4978b71799284d03e75d77054dc4ab6a0d19a`, with `7 waves / 702 native-X calls / 98
unique handles`.

## Query commitments, evaluation and Bio semantics

The non-author review reported `P0=0 / P1=4 / P2=3` after the original c9 findings had been addressed. It reproduced
an unbound/concurrent private migration, private-policy metadata reaching stdout before validation, reusable HMAC
lineage material, incomplete protected-category value coverage, nested deadline resets, missing direct migration-script
coverage, and a missing directory `fsync`.

Disposition: author-complete, still awaiting pinned non-author review. The v2 lane now uses an owner-only locked,
source/target-hash-bound migration with durable prepare/purge receipts; validates before public output; requires unique
key/nonce issuance lineage; binds a versioned protected-category boundary; forwards one absolute deadline; executes
the real migration script in concurrency/symlink/crash tests; and flushes directory state. Focused evaluation plus
migration is `79/79` on Python 3.12 and 3.14; migration-only is `11/11`. Public query-policy privacy scans returned
zero private query/session/key/nonce fields.

## Stage 2 field capability

Two non-author rounds each reported `P0=0 / P1=8 / P2=4`. A subsequent static audit found two additional P1
compositions: multiple full profile sources could be mislabeled unavailable, and multi-ID plus cross-handle conflict
precedence had no legal terminal state.

Disposition: author-complete, still awaiting pinned non-author review. Request-frozen scenario semantics now bind the
expectation; decisions distinguish conformance from mismatch; field states, quarantine reasons, temporal closure and
collection identity are source-derived; Post/source/receipt/task ownership is closed; external selection binds the
full tuple; Post IDs are globally unique; multiple profile payloads use typed `multiple_profile_sources`; and one
precedence owner resolves multi-ID plus cross-handle conflicts. Focused tests are `37/37` on Python 3.12 and 3.14;
the deterministic fixture SHA-256 is `f369128352b0f831f531b6d14de9a1de626e46217d09bf0e1e65fae6bedb02af`.

## Adaptive live operator and blocked campaign bridge

The latest non-author review reproduced `P0=0 / P1=1 / P2=1`:

1. a grant could pass the pre-link expiry check, expire during the exclusive consumption link, and still execute;
2. two individually replay-valid bundles could be atomically exchanged at one canonical run path while the bridge
   returned a stale binding artifact.

Disposition: a targeted re-review of the latest working tree returned `P0=0 / P1=0 / P2=0`, advisory `GO` for only
these two findings. Post-link expiry leaves exactly one consumed record but invokes no executor. The gated launcher
persists its process ledger, then rechecks wall and monotonic time immediately before target release; expiry produces
zero target releases, preserves the ledger, and supports a `crash_recovered` bundle. Completed bundle replay rejects
launcher timestamps outside the grant window. The bridge holds the run lease across its complete snapshot, pins the
run-root device/inode, exact-re-reads every bound artifact, and full-replays before returning. The identical-path A/B
swap mutation now fails with `adaptive_bundle_changed_during_campaign_bridge`. Adaptive plus recall focused tests are
`64/64` on Python 3.12 and 3.14.

## Stable aggregate validation

After the dispositions above, the complete sibling offline suite passed `302/302` on Python 3.12 and `302/302` on
Python 3.14.4. Ruff lint, changed-file format checks, compileall, JSON parsing, deterministic fixture checks,
`git diff --check`, public UUID/privacy scans, and the private v5 replay also passed.

## Remaining gate

The remediation and Stage 2 scopes must be committed separately and reviewed by non-authors against their exact pinned
Git objects. This working-tree record must not be relabeled or reused as the formal promotion artifact. Until those
reviews pass, no new Grok/Luna live canary, campaign admission, source-bound claim, product write, or milestone signoff
is authorized.
