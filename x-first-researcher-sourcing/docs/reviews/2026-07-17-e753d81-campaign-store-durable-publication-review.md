# Independent pinned review: campaign-store durable publication

Date: 2026-07-17
Review type: non-author, read-only, pinned Git-object review

## Reviewer independence

- The reviewer did not author the implementation and did not edit any implementation, test, or contract file.
- The final review ran in a clean detached worktree at
  `/private/tmp/xfirst-e753-pinned-review.gDyDnG`.
- Only the pinned Git objects and the three-file scope below were used for the implementation verdict. Mutable-tree,
  Live, and private artifacts were not read.
- This artifact was written only after the pinned verdict. It is not part of the reviewed commit and was not staged or
  committed by the reviewer.

## Exact object and scope binding

- Commit: `e753d817f5df8c3081223baec9387f76179fb2db`
- Parent: `1683f974a8c1a7ebddb12b35d01853d52b5b8aa8`
- Commit tree: `c22bf38a8ed0956a9ad3fc5814daf9f7e2d120f8`
- Commit subject: `fix(x-first): make store publication durable`
- Three-file binary diff SHA-256:
  `a105dea0f49317d557f372111b3bc7feab4e10f895362eb36fe3b0eaa47e8953`

Reviewed paths:

1. `x-first-researcher-sourcing/src/x_first/source_neutral_campaign_store.py`
2. `x-first-researcher-sourcing/tests/test_source_neutral_campaign_store.py`
3. `x-first-researcher-sourcing/docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md`

Pinned blob bindings:

| Path | Git blob | File SHA-256 |
|---|---|---|
| `src/x_first/source_neutral_campaign_store.py` | `68f82adb8c596d2490fd374bf734a0631d17cd2b` | `b915396084cb1b6d0296abcd115b227c8d2ebf289a7c143065217f5fd2589d84` |
| `tests/test_source_neutral_campaign_store.py` | `c6e4ee972498790061493bdda82723496f3374f7` | `f14b96db9f87686d3bd634cda1e9e6891d3e6e57ed18b12cc88a4b60279d9a08` |
| `docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md` | `bda6f02176331faf5eafa8c19709d100566731f4` | `6f4ff79d9c9f180e6e35687279bd5f84dd210d0183a45df6cf747747344897cd` |

The commit also adds
`docs/reviews/2026-07-17-f386928-campaign-store-publication-order-review.md`. That prior review artifact is outside this
three-file scope; it was not read and is not covered by this verdict.

## Findings

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 0 |
| P2 | 0 |
| P3 | 0 |

The pre-pin adversarial pass found one ambiguous link-completion defect: an exception could arrive after `link(2)`
created the target but before the next Python assignment, allowing cleanup to remove the recovery alias. The pinned
commit closes that mechanism by entering the conservative `target_link_attempted` state before the syscall and adds
manifest, object, and journal regressions that perform the real link and then raise `KeyboardInterrupt`. It is therefore
not a remaining finding in the pinned objects.

The reviewed implementation now uses the same publication order on both normal and recovery paths:

1. create the hardlink target;
2. `fsync` the target directory;
3. prove exact temp/target inode, owner, mode, namespace, bytes, and two-link binding;
4. unlink only that temp alias and prove the target is single-link;
5. `fsync` the temp directory.

Failures before target-directory durability retain the exact two-link marker when the target exists. Failures after
target-directory durability cannot erase the durable target, and fresh open either recovers the marker or replays the
already committed journal entry. Non-target aliases, wrong content-addressed bytes, unexpected namespaces, mismatched
inodes, and invalid link counts remain fail-closed.

## Validation evidence

Interpreter used for Python lanes:
`/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent/.venv/bin/python` with
`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src`.

| Lane / command category | Exact result |
|---|---:|
| `python -m unittest tests.test_source_neutral_campaign_store -v` | 37 tests passed |
| `python -m unittest tests.test_source_neutral_campaign_store tests.test_source_neutral_mapping -v` | 63 tests passed |
| Provider-free `python -m unittest discover -s tests -q` | 544 tests passed in 71.475 s |
| Critical post-link / fsync / signal regression selection | 13 tests passed |
| `python -m x_first.source_neutral_mapping` | `{"errors": [], "status": "valid"}` |
| `python -m x_first.contracts` | valid; precision `1.0`, recall `1.0`, false merges `0`, predicted/relevant `20/20` |
| Full-tree `ruff check .` | passed |
| Scoped `ruff format --check` on the changed Python and test files | 2 files already formatted |
| Pinned `git diff --check <parent> <commit> -- <three paths>` | passed |
| Final detached-worktree status | clean |

The 13-test critical selection re-ran:

- object and journal exit immediately after append-only link creation;
- object and journal exit after target-directory `fsync` and before temp unlink;
- object and journal exit after temp unlink and before temp-directory `fsync`;
- both manifest exit boundaries in one two-subtest regression;
- object, journal, and manifest target-directory `fsync` failure;
- object, journal, and manifest real-link-then-`KeyboardInterrupt` ambiguity.

The full-tree `ruff format --check .` command reports 19 historical, scope-external files that would be reformatted.
None is one of the two changed Python/test files, and none was modified during review:

1. `scripts/generate_capability_probe_fixtures.py`
2. `src/x_first/adaptive_grok_wave_kpis.py`
3. `src/x_first/adaptive_grok_wave_runner.py`
4. `src/x_first/capability_probe.py`
5. `src/x_first/compact_grok_discovery.py`
6. `src/x_first/contracts.py`
7. `src/x_first/grok_cli_exploration.py`
8. `src/x_first/grok_operator_session_replay.py`
9. `src/x_first/grok_profile_hydration.py`
10. `src/x_first/recall_pool_campaign.py`
11. `src/x_first/reported_profile_text_semantic.py`
12. `tests/grok_raw_session_fixture.py`
13. `tests/test_adaptive_grok_wave_kpis.py`
14. `tests/test_adaptive_grok_wave_runner.py`
15. `tests/test_compact_grok_discovery.py`
16. `tests/test_grok_profile_hydration.py`
17. `tests/test_recall_pool_campaign.py`
18. `tests/test_reported_profile_text_semantic.py`
19. `tests/test_x_first_capability_probe.py`

## Explicit create-retry boundary

The reviewed gate proves fresh-`CampaignStore.open` recovery after interrupted manifest publication. It does not claim
that a direct retry through `CampaignStore.create` recovers the two-link manifest intermediate. A pinned behavioral
probe confirmed that direct create currently returns `private_file_link_count_invalid`; a subsequent fresh open
recovers the store and replays the expected `store_id`. This is an explicit scope boundary, not a finding against the
required fresh-open contract and not evidence for a broader create-idempotency claim.

## Final verdict

GO
