# PROGRESS — workspace current-state snapshot

```
owner: operator        refreshed: 2026-07-22       next-cleanup: next milestone
budget: ≤200 lines, replace-not-append; detail lives in linked module docs / archive
```

## Where the project is

- **Pipeline deliverables (2026-07-20/21 era)**: 6 lab Layer-1-3 CSVs delivered —
  GDM 1491 rows, TML 67 (v2), OpenAI 950 (v2 → **v3 2026-07-22** restoring 14
  wrongly-blanked X handles), Anthropic, xAI, Meta-TBD. Files under
  `sourcing-ai-agent/runtime/test_env_live/` (deliverables tier + manifest = reorg R4).
- **Takeover (2026-07-21)**: Kimi-era uncommitted work landed as 11 atomic commits;
  daemon queue neutralized (13 cancelled / 23 parked to 2027-01-01 / 2 perma-zombies
  terminalized — NEVER requeue them: duplicate paid submit); 221MB spin log archived.
- **Phase C data layer (2026-07-22)**: OpenAI 40-person three-generation identity merge
  landed via committed alias map (`configs/identity/openai_identity_alias_map_v1.json`)
  + first canonical completeness gate (`scripts/check_canonical_completeness.py`);
  authoritative flag restored to gen-6 snapshot 20260720T104157; July snapshots copied
  checksum-verified into canonical `runtime/company_assets`; 5 labs' pointers repaired;
  registry source_path rewritten; conflicts list rebuilt (2 genuine collisions).
- **Daemon restart preconditions (2026-07-22)**: 3 code fixes landed — alt-ref
  admission/resume asymmetry closed (paid-submit hazard; ORPHAN_SECONDS=0 no longer
  required), authoritative-promotion regression guard (lineage replay + coverage
  subset), pointer writer self-consistency. **Worker daemon remains STOPPED** by
  operator decision until harness reorg completes, then runtime-root selection.
- **Harness reorg (2026-07-22)**: research (external harness survey + playbook
  doc 06/18 + 22-item gap analysis) + design committed
  (`sourcing-ai-agent/docs/HARNESS_REORG_DESIGN.md`); **R0–R2 DONE**
  (26bcff4/60fa305/bc8b5d4): root hygiene + legacy archive, single entry chain
  (README/AGENTS/CLAUDE → snapshots → docs router → module index), bounded
  snapshots, tool-native skills (/takeover, live-wave, review-gate, daemon-ops,
  session-handoff), scripts registry at `sourcing-ai-agent/scripts/README.md`.
  R3–R5 (docs lifecycle / deliverables manifest / CI lints) await approval.

## Standing walls / hazards

- chshapi relay quota EXHAUSTED (independent review re-fire queue parked);
  HarvestAPI monthly quota EXHAUSTED (no live acquisition). Local-only window.
- Perma-zombie commands `cmd_a32cc93e15b5f52150ac4da0` / `cmd_1330a600a2f91e5c0617a4b7`
  are terminal — retry/resume would re-submit paid work.
- google hot-cache snapshot 20260720T152139 lost 60 files to the (now fixed)
  self-symlink bug; regenerable via reconcile once the daemon restarts.
- Known dup pending a lane: anthropic "Jennifer Wang" (same URL, two candidate ids).

## Recent handoff evidence

Takeover intake + neutralization ledgers: operator memory
`artifacts-intake-20260721/` (incl. `neutralization_20260721.md`). Superseded takeover
docs archived at `sourcing-ai-agent/docs/archive/2026-07-21-takeover/`.
Live lane state: `sourcing-ai-agent/.coord/BOARD.md` (gitignored; git wins).
