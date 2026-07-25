#!/usr/bin/env python3
"""WS7 shadow-path engagement proof + ladder-vs-scripted divergence report.

Covers both AI shadow tracks before the S5 flip:
  * 议案① / W7.2 — the profile-prefetch AI **batch divider**
    (`profile_batch_division.record_profile_prefetch_division_shadow`)
  * 议案③ / W7.3 — the organization-asset AI **promote judge**
    (`organization_promote_judgment.record_organization_promote_shadow`)

=============================================================================
READ THIS BEFORE QUOTING ANY NUMBER THIS SCRIPT PRINTS
=============================================================================
Every AI-side number here comes from a **SCRIPTED deterministic client**, not
from a model. It therefore evidences **the code path and the retained validator
battery — never AI judgment quality**:

  * ``ScriptedProfileBatchDividerModelClient`` is
    ``clamp(ceil(eligible/300), 4, 8)`` contiguous near-equal chunks. It reads
    inventory size and the provider envelope and **nothing else** — it ignores
    shard mix, failure history, queue state, attempt counts and priority. Every
    "divergence" it produces is ``f(n)`` vs ``g(n, worker_budget)``: two
    arithmetic formulas.
  * ``ScriptedOrganizationPromoteJudgeModelClient`` is a 3-line rule (simulate
    taint → reject; narrower effective lane total → reject; else promote). Its
    two reject arms are exactly the predicates V_PROV/V_COMP re-check, so a
    scripted promote survives the S5 conjunction iff the guard-and-battery floor
    admits it. The CEILING on authority churn is therefore the floor's own
    admission count (452/806 extended); ``ai_more_permissive`` (246/806) is the
    SUBSET of those admissions where the ladder disagreed — churn relative to
    today's ladder, a lower bound on the ceiling, NOT equal to it. Both describe
    the FLOOR and this scripted rule, not AI behaviour.

Nothing here says anything about prompts, reasoning, hallucination, cost,
latency, or the F2/F3 real-provider failure modes. A real-model corpus is a
separate, operator-gated step (S6). This is also NOT a historical replay: the
rows are current post-fix state, and the extended promote corpus contains
counterfactual incumbents that never held authority.

THE INPUT SIDE IS RECONSTRUCTED TOO (divider mode). The live registry has ZERO
rows in ``refill_queue_state='ready'`` (the distribution is ''=7513,
retry_wait=2, planned_dispatch=1; the terminal statuses are completed=7295 /
fetched=218). A ready set is therefore NOT observed — the replay takes each
wave's REAL url population and REAL attempt/failure history and FABRICATES the
queue state as ``ready`` for every member, because replaying the terminal state
would collapse the plan to an empty wave. So a "4,297-member set" is the
cumulative all-time membership of a job token, not a set of items ever
simultaneously awaiting refill. The emitted JSON carries this under
``input_reconstruction``; every count below must be read as
"real population, reconstructed queue state". Two further consequences: V8
(retry isolation) can never fire because the fabrication overwrites the 2 genuine
retry_wait rows, and the R6 durable-wave gate can never fire because the replay
zeroes the recorded wave fields.

What it CAN evidence: (a) how SOME of the retained validator battery behaves
against legal-but-non-ladder shapes — see ``battery_coverage`` in the emitted
summary for exactly which validators fired and which are structurally incapable
of firing on this input; (b) the end-to-end offline path at real data volumes;
(c) an upper bound on authority churn; (d) data-quality facts about the real
corpus.

=============================================================================
SAFETY CONTRACT
=============================================================================
  * ZERO paid/provider calls. The script refuses to run if any live-gate env var
    is set (``SOURCING_EXTERNAL_PROVIDER_MODE=live`` /
    ``SOURCING_LIVE_PROVIDER_CONFIRM`` /
    ``SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS``) and instantiates the
    scripted clients directly (never ``build_model_client``).
  * The live schema is **READ-ONLY**. Replay modes open ONE psycopg connection
    with ``default_transaction_read_only = on`` — Postgres itself rejects any
    write — and never open a ``ControlPlaneStore`` against it (that would run
    ``ensure_bootstrapped`` → ``apply_pending_migrations``, i.e. DDL).
  * ``engagement`` mode needs writes, so it provisions its OWN ephemeral
    ``sourcing_test_ws7_*`` schema and drops it on exit. It never touches the
    live schema.
  * Nothing here can change a dispatch decision or which snapshot is
    authoritative: both shadow recorders are record-only by construction.

=============================================================================
USAGE
=============================================================================
    set -a; source .local-postgres.env; set +a
    PYTHONPATH=src:. ./.venv/bin/python scripts/ws7_shadow_divergence_report.py all \
        --out-dir /tmp/ws7_corpus

Subcommands:
    engagement   end-to-end proof that BOTH shadow seams engage (own schema)
    divider      ladder-vs-scripted divergence over reconstructed ready sets (read-only)
    promote      ladder-vs-scripted divergence over real registry pairs (read-only)
    all          engagement + divider + promote
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import statistics
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

LIVE_SCHEMA_DEFAULT = "sourcing_live_tml_path_20260719"

# The three live-gate env vars. Any of them set → hard refusal (fail closed).
_LIVE_GATE_ENV_KEYS = (
    "SOURCING_LIVE_PROVIDER_CONFIRM",
    "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS",
)

_SCRIPTED_DIVIDER_ENV_KEY = "SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER"
_SCRIPTED_JUDGE_ENV_KEY = "SOURCING_SCRIPTED_ORGANIZATION_PROMOTE_JUDGE"

_HONESTY_BANNER = (
    "SCRIPTED-CLIENT CORPUS — this measures the PATH and PART of the validator battery, never a model. "
    "The scripted divider is clamp(ceil(n/300),4,8) equal chunks; the scripted judge is a 3-line rule "
    "whose two reject arms are exactly the predicates V_PROV/V_COMP re-check, so its permissive count "
    "equals the guard-and-battery FLOOR's admission count (an upper bound no judge can exceed) — that "
    "is a ceiling argument about the floor, NOT a claim that a real model is less permissive. "
    "DIVIDER INPUT IS RECONSTRUCTED: the live registry has ZERO rows in refill_queue_state='ready'; "
    "the replay uses real url populations + real attempt history but FABRICATES the ready queue state, "
    "so a 'ready set' here is a wave's cumulative membership, not an observed refill queue. "
    "Treat every AI-side number as a SHAPE PROBE. A real-model corpus is a separate, "
    "operator-gated step (S6)."
)

# The live `linkedin_profile_registry.refill_queue_state` census this replay
# overwrites — probed read-only 2026-07-25 and re-probed on every divider run
# (see `_live_refill_queue_state_census`). Kept as a constant so the disk-only
# mode can still disclose the substitution honestly.
_KNOWN_LIVE_READY_ROW_COUNT = 0


# ---------------------------------------------------------------------------
# Fail-closed preflight
# ---------------------------------------------------------------------------


def assert_no_live_provider_gate() -> None:
    """Refuse to run under any live-provider gate (batch hard rule)."""

    offenders = [key for key in _LIVE_GATE_ENV_KEYS if str(os.environ.get(key) or "").strip()]
    provider_mode = str(os.environ.get("SOURCING_EXTERNAL_PROVIDER_MODE") or "").strip().lower()
    if provider_mode == "live":
        offenders.append("SOURCING_EXTERNAL_PROVIDER_MODE=live")
    if offenders:
        raise SystemExit(
            "REFUSED: ws7_shadow_divergence_report is a ZERO-PAID-CALL offline harness, but the "
            f"live provider gate is set: {', '.join(sorted(offenders))}. Unset it and re-run."
        )
    # Pin the mode the scripted clients require; never 'live'.
    os.environ["SOURCING_EXTERNAL_PROVIDER_MODE"] = provider_mode or "simulate"


# ---------------------------------------------------------------------------
# READ-ONLY live-schema access
# ---------------------------------------------------------------------------


@contextmanager
def read_only_live_connection(*, dsn: str) -> Iterator[Any]:
    """One connection, hard-set to read-only at the SESSION level.

    ``default_transaction_read_only = on`` makes Postgres itself reject any
    INSERT/UPDATE/DELETE/DDL on this connection, so "strictly read-only" is
    enforced by the server rather than by reviewer discipline.
    """

    import psycopg

    from sourcing_agent.local_postgres import normalize_control_plane_postgres_connect_dsn

    connection = psycopg.connect(
        normalize_control_plane_postgres_connect_dsn(dsn), connect_timeout=10, client_encoding="utf8"
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
            cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        connection.commit()
        yield connection
    finally:
        try:
            connection.rollback()
        except Exception:
            pass
        connection.close()


def _resolve_dsn() -> str:
    from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn

    dsn = str(os.environ.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip()
    if not dsn:
        dsn = str(resolve_control_plane_postgres_dsn(_REPO_ROOT) or "").strip()
    if not dsn:
        raise SystemExit(
            "REFUSED: no control-plane Postgres DSN. Run `set -a; source .local-postgres.env; set +a` first."
        )
    return dsn


def _fetch_rows(connection: Any, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        columns = [str(description[0]) for description in (cursor.description or [])]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _quoted(schema: str) -> str:
    from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier

    return quote_control_plane_postgres_identifier(schema)


def _json_load(raw: Any, default: Any) -> Any:
    if raw is None:
        return default
    if isinstance(raw, (list, dict)):
        return raw
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Ephemeral isolated schema (engagement mode only — it needs to write)
# ---------------------------------------------------------------------------


@contextmanager
def ephemeral_control_plane_store(*, label: str) -> Iterator[Any]:
    """A pristine per-run ``sourcing_test_ws7_*`` schema + store, dropped on exit.

    Mirrors tests/pg_durable_runtime.PGDurableRuntimeFixture without importing
    from tests/ (committed scripts must not depend on the test tree).
    """

    import psycopg

    from sourcing_agent.local_postgres import (
        normalize_control_plane_postgres_connect_dsn,
        normalize_control_plane_postgres_schema,
    )
    from sourcing_agent.storage import ControlPlaneStore

    dsn = normalize_control_plane_postgres_connect_dsn(_resolve_dsn())
    digest = hashlib.sha1(f"{label}:{time.time_ns()}".encode("utf-8")).hexdigest()[:10]
    schema = normalize_control_plane_postgres_schema(f"sourcing_test_ws7_{label}_{digest}")
    quoted = _quoted(schema)
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
            cursor.execute(f"CREATE SCHEMA {quoted}")
    previous = {
        key: os.environ.get(key)
        for key in (
            "SOURCING_CONTROL_PLANE_POSTGRES_DSN",
            "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA",
            "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE",
            "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES",
            "SOURCING_PG_ONLY_SQLITE_BACKEND",
            "SOURCING_RUNTIME_ENVIRONMENT",
        )
    }
    os.environ.update(
        {
            "SOURCING_CONTROL_PLANE_POSTGRES_DSN": dsn,
            "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": schema,
            "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
            "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
            "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
            "SOURCING_RUNTIME_ENVIRONMENT": "test",
        }
    )
    store = None
    try:
        with tempfile.TemporaryDirectory() as tempdir:
            store = ControlPlaneStore(Path(tempdir) / "control_plane.db")
            store._control_plane_postgres.ensure_bootstrapped()  # noqa: SLF001
            yield store, schema
    finally:
        if store is not None:
            try:
                store.close()
            except Exception:
                pass
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            with psycopg.connect(dsn, autocommit=True, connect_timeout=10, client_encoding="utf8") as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# OBJECTIVE 1 — engagement proof (both seams, end to end, own schema)
# ---------------------------------------------------------------------------


def _engagement_divider(*, url_count: int) -> dict[str, Any]:
    """Drive the REAL enrichment mint seam and capture the shadow record.

    Runs ``MultiSourceEnricher.queue_background_profile_prefetch`` — the single
    production callsite of ``record_profile_prefetch_division_shadow`` — with a
    divider-capable scripted client and
    ``execute_profile_refill_submit_commands=False`` so no submit ever reaches
    the connector (ZERO provider calls).
    """

    from sourcing_agent.asset_catalog import AssetCatalog
    from sourcing_agent.domain import Candidate
    from sourcing_agent.enrichment import MultiSourceEnricher
    from sourcing_agent.model_provider import ScriptedProfileBatchDividerModelClient

    class _StubConnector:
        settings = type("_Settings", (), {"enabled": True})()

    os.environ[_SCRIPTED_DIVIDER_ENV_KEY] = "1"
    try:
        with ephemeral_control_plane_store(label="divider") as (store, schema):
            with tempfile.TemporaryDirectory() as tempdir:
                root = Path(tempdir)
                catalog = AssetCatalog(
                    project_root=root,
                    dev_root=root,
                    anthropic_root=root,
                    anthropic_workbook=root / "anthropic.xlsx",
                    anthropic_readme=root / "README.md",
                    anthropic_progress=root / "PROGRESS.md",
                    legacy_api_accounts=root / "api_accounts.json",
                    legacy_company_ids=root / "company_ids.json",
                    anthropic_publications=root / "publications.json",
                    scholar_scan_results=root / "scholar.json",
                    investor_members_json=root / "investor.json",
                    employee_scan_skill=root / "employee_skill.md",
                    investor_scan_skill=root / "investor_skill.md",
                    onepager_skill=root / "onepager_skill.md",
                )
                enricher = MultiSourceEnricher(
                    catalog,
                    accounts=[],
                    harvest_profile_connector=_StubConnector(),
                    model_client=ScriptedProfileBatchDividerModelClient(mode="simulate"),
                    store=store,
                )
                enricher.worker_runtime = object()
                candidates = [
                    Candidate(
                        candidate_id=f"engage_{index:04d}",
                        name_en=f"Engage {index}",
                        display_name=f"Engage {index}",
                        linkedin_url=f"https://www.linkedin.com/in/ws7-engagement-{index:04d}/",
                    )
                    for index in range(url_count)
                ]
                result = enricher.queue_background_profile_prefetch(
                    candidates=candidates,
                    snapshot_dir=root,
                    job_id="ws7_engagement_divider",
                    request_payload={},
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                    submit_provider=True,
                    execute_profile_refill_submit_commands=False,
                )
                refill_plan_items = dict(result.get("refill_plan_items") or {})
                record = refill_plan_items.get("ai_batch_division_shadow")
                return {
                    "path": "divider",
                    "seam": (
                        "enrichment.MultiSourceEnricher.queue_background_profile_prefetch "
                        "-> record_profile_prefetch_division_shadow"
                    ),
                    "isolated_schema": schema,
                    "eligible_url_count": url_count,
                    "engaged": bool(record is not None and record.get("engaged")),
                    "shadow_record": record,
                    "provider_calls": 0,
                }
    finally:
        os.environ.pop(_SCRIPTED_DIVIDER_ENV_KEY, None)


def _engagement_promote() -> dict[str, Any]:
    """Drive the REAL artifact-build entrypoint and capture the shadow record.

    ``build_company_candidate_artifacts`` → ``sync_company_asset_registration``
    → ``upsert_organization_asset_registry_with_guard`` →
    ``record_organization_promote_shadow``. Before 2026-07-25 the
    ``model_client`` never reached that seam from any production caller; this is
    the proof that it now does.
    """

    from sourcing_agent.candidate_artifacts import build_company_candidate_artifacts
    from sourcing_agent.domain import Candidate, EvidenceRecord, make_evidence_id
    from sourcing_agent.model_provider import ScriptedOrganizationPromoteJudgeModelClient

    def _write_snapshot(runtime_dir: Path, snapshot_id: str, count: int) -> None:
        snapshot_dir = runtime_dir / "company_assets" / "acme" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (runtime_dir / "company_assets" / "acme" / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "aliases": [],
                    },
                }
            ),
            encoding="utf-8",
        )
        candidates = []
        evidence = []
        for index in range(count):
            url = f"https://www.linkedin.com/in/ws7-acme-{index:04d}/"
            candidates.append(
                Candidate(
                    candidate_id=f"c{index}",
                    name_en=f"Person {index}",
                    display_name=f"Person {index}",
                    category="employee",
                    target_company="Acme",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url=url,
                    source_dataset="linkedin_roster",
                ).to_record()
            )
            evidence.append(
                EvidenceRecord(
                    evidence_id=make_evidence_id(f"c{index}", "linkedin_profile", "LinkedIn", url),
                    candidate_id=f"c{index}",
                    source_type="linkedin_profile",
                    title="LinkedIn",
                    url=url,
                    summary="Roster evidence.",
                    source_dataset="linkedin_roster",
                    source_path="/tmp/roster.json",
                    metadata={"profile_url": url},
                ).to_record()
            )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": candidates, "evidence": evidence}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    os.environ[_SCRIPTED_JUDGE_ENV_KEY] = "1"
    try:
        with ephemeral_control_plane_store(label="promote") as (store, schema):
            with tempfile.TemporaryDirectory() as tempdir:
                runtime_dir = Path(tempdir) / "runtime"
                runtime_dir.mkdir(parents=True, exist_ok=True)
                client = ScriptedOrganizationPromoteJudgeModelClient(mode="simulate")
                _write_snapshot(runtime_dir, "20260101T000000", 40)
                build_company_candidate_artifacts(
                    runtime_dir=runtime_dir,
                    store=store,
                    target_company="Acme",
                    snapshot_id="20260101T000000",
                    model_client=client,
                )
                _write_snapshot(runtime_dir, "20260202T000000", 80)
                second = build_company_candidate_artifacts(
                    runtime_dir=runtime_dir,
                    store=store,
                    target_company="Acme",
                    snapshot_id="20260202T000000",
                    model_client=client,
                )
                refresh = dict(dict(second.get("sync_status") or {}).get("organization_asset_registry_refresh") or {})
                record = dict(refresh.get("result") or {}).get("ai_promote_decision_shadow")
                authoritative = store.get_authoritative_organization_asset_registry(
                    target_company="Acme", asset_view="canonical_merged"
                )
                return {
                    "path": "promote",
                    "seam": (
                        "candidate_artifacts.build_company_candidate_artifacts "
                        "-> sync_company_asset_registration "
                        "-> upsert_organization_asset_registry_with_guard "
                        "-> record_organization_promote_shadow"
                    ),
                    "isolated_schema": schema,
                    "engaged": bool(record is not None and record.get("engaged")),
                    "authoritative_snapshot_id": str(authoritative.get("snapshot_id") or ""),
                    "shadow_record": record,
                    "provider_calls": 0,
                }
    finally:
        os.environ.pop(_SCRIPTED_JUDGE_ENV_KEY, None)


def run_engagement(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "mode": "engagement",
        "honesty": _HONESTY_BANNER,
        "note": (
            "Both records below were produced by a REAL run through the production seam in an "
            "ephemeral PG schema (created and dropped by this script). The two scripted opt-ins are "
            "mutually exclusive inside one build_model_client process (model_provider.py precedence), "
            "so the two paths are exercised as two separate runs — which is exactly how the daemon "
            "would have to be configured."
        ),
        "input_provenance": (
            "SYNTHETIC INPUTS. What is real here is the SEAM (the production call chain fires and a "
            "record lands); the data is fabricated by this script — the divider runs on generated "
            "`ws7-engagement-*` urls and the promote path on two generated `ws7-acme-*` snapshots. "
            "Quote this block as PATH evidence only. The `division_id`/`decision_id` are fresh per run, "
            "so they identify nothing and must never be cited as evidence identifiers."
        ),
        "provider_calls_total": 0,
        "divider": _engagement_divider(url_count=int(args.divider_url_count)),
        "promote": _engagement_promote(),
    }


# ---------------------------------------------------------------------------
# OBJECTIVE 2a — divider divergence replay (READ-ONLY live schema)
# ---------------------------------------------------------------------------


def _live_refill_queue_state_census(connection: Any, *, schema: str) -> dict[str, Any]:
    """The REAL `refill_queue_state` / `refill_terminal_status` distribution the
    replay overwrites.

    Emitted verbatim into the corpus so no reader has to take the word
    "reconstructed" on trust: if `ready` is 0 here, then every "ready set" in
    this report is a reconstruction, full stop.
    """

    queue_rows = _fetch_rows(
        connection,
        f"""
        SELECT COALESCE(refill_queue_state, '') AS state, COUNT(*) AS n
          FROM {_quoted(schema)}.linkedin_profile_registry
         GROUP BY 1 ORDER BY 2 DESC
        """,
    )
    terminal_rows = _fetch_rows(
        connection,
        f"""
        SELECT COALESCE(refill_terminal_status, '') AS status, COUNT(*) AS n
          FROM {_quoted(schema)}.linkedin_profile_registry
         GROUP BY 1 ORDER BY 2 DESC
        """,
    )
    queue_counts = {str(row["state"]): int(row["n"]) for row in queue_rows}
    return {
        "refill_queue_state_counts": queue_counts,
        "refill_terminal_status_counts": {str(row["status"]): int(row["n"]) for row in terminal_rows},
        "actual_ready_row_count": int(queue_counts.get("ready", 0)),
    }


def _load_divider_ready_sets(
    connection: Any, *, schema: str, min_urls: int, grouping: str = "job_token"
) -> list[dict[str, Any]]:
    """RECONSTRUCT historical wave-scoped ready sets from the live registry.

    NOT observed ready sets — see `_live_refill_queue_state_census`: no row in
    the live registry is in `refill_queue_state='ready'`. What IS real here is
    the url population of each wave and its attempt/failure history; the queue
    state is fabricated by `_divider_case` (documented there and disclosed in the
    emitted `input_reconstruction` block).

    Two groupings, both derived from ``source_jobs_json`` (the wave identity the
    refill path records):

    * ``job_token`` (default) — one ready set per individual job id, i.e. exactly
      what the mint seam scopes (``source_jobs=[job_id]``). A url that belongs to
      several jobs appears in each of their sets, which is faithful: each of those
      waves really did carry it.
    * ``job_tuple`` — one ready set per distinct sorted job-id tuple. Partitions
      the registry (no url counted twice) but under-counts any wave whose members
      were later re-attributed to additional jobs.
    """

    rows = _fetch_rows(
        connection,
        f"""
        SELECT profile_url_key,
               profile_url,
               source_shards_json,
               source_jobs_json,
               refill_queue_state,
               status,
               last_refill_attempt_count,
               last_refill_deferred_reason,
               refill_terminal_status,
               refill_plan_batch_size,
               refill_plan_batch_count,
               refill_plan_window_url_count
          FROM {_quoted(schema)}.linkedin_profile_registry
        """,
    )
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        jobs = [
            str(item or "").strip() for item in _json_load(row.get("source_jobs_json"), []) if str(item or "").strip()
        ]
        if grouping == "job_tuple":
            groups.setdefault("|".join(sorted(jobs)) or "<no_job>", []).append(row)
            continue
        for job in sorted(set(jobs)) or ["<no_job>"]:
            groups.setdefault(job, []).append(row)
    ready_sets: list[dict[str, Any]] = []
    for key, members in sorted(groups.items(), key=lambda pair: (-len(pair[1]), pair[0])):
        if len(members) < min_urls:
            continue
        ready_sets.append({"job_group": key, "rows": members, "provenance": "live_pg_registry"})
    return ready_sets


def _divider_case(
    *,
    job_group: str,
    rows: list[dict[str, Any]],
    available_new_worker_count: int,
    inflight_values: Sequence[int],
    provenance: str = "live_pg_registry",
) -> dict[str, Any]:
    from sourcing_agent.enrichment import (
        _build_profile_prefetch_batch_plan,
        _build_profile_prefetch_queue_items,
    )
    from sourcing_agent.model_provider import ScriptedProfileBatchDividerModelClient
    from sourcing_agent.profile_batch_division import record_profile_prefetch_division_shadow

    urls: list[str] = []
    source_shards_by_url: dict[str, list[str]] = {}
    registry_entries: dict[str, dict[str, Any]] = {}
    actual_queue_states: dict[str, int] = {}
    for row in rows:
        url = str(row.get("profile_url") or row.get("profile_url_key") or "").strip()
        if not url:
            continue
        urls.append(url)
        source_shards_by_url[url] = [
            str(item or "").strip() for item in _json_load(row.get("source_shards_json"), []) if str(item or "").strip()
        ]
        actual_queue_states[str(row.get("refill_queue_state") or "")] = (
            actual_queue_states.get(str(row.get("refill_queue_state") or ""), 0) + 1
        )
        registry_entries[str(row.get("profile_url_key") or "")] = {
            # FABRICATED, NOT OBSERVED. The replay treats every member as a fresh
            # normal-wave ready item: no live row is in `ready` at all (the live
            # rows are terminal `fetched`/`completed`), and replaying their
            # terminal queue_state would collapse the plan to an empty wave.
            # The url population and the attempt/failure history ARE real (the
            # latter is the signal the shadow recorder folds into
            # `failure_history`); the queue state is not. Disclosed per case
            # under `input_reconstruction` and in the report banner.
            "refill_queue_state": "ready",
            "status": str(row.get("status") or ""),
            "last_refill_attempt_count": int(row.get("last_refill_attempt_count") or 0),
            "last_refill_deferred_reason": str(row.get("last_refill_deferred_reason") or ""),
            "refill_terminal_status": str(row.get("refill_terminal_status") or ""),
            "refill_plan_batch_size": 0,
            "refill_plan_batch_count": 0,
            "refill_plan_window_url_count": 0,
        }
    queue_items = _build_profile_prefetch_queue_items(
        urls,
        source_shards_by_url=source_shards_by_url,
        source_jobs=[job_group],
        priority=False,
        queue_state="ready",
        registry_entries=registry_entries,
    )
    plan = _build_profile_prefetch_batch_plan(
        dispatch_urls=urls,
        requested_url_count=len(urls),
        candidate_count=len(urls),
        priority=False,
        source_shards_by_url=source_shards_by_url,
        worker_budget={
            "submit_budget": available_new_worker_count,
            "actor_budget": available_new_worker_count,
            "active_worker_count": 0,
            "scheduler_reserved_worker_count": 0,
            "effective_active_worker_count": 0,
            "available_new_worker_count": available_new_worker_count,
        },
        dispatch_window=None,  # let the FULL ladder run (sizer + R1-R5)
        queue_items=queue_items,
        allow_under_target_final_tail_dispatch=False,
    )
    record = record_profile_prefetch_division_shadow(
        ScriptedProfileBatchDividerModelClient(mode="simulate"),
        plan=plan,
        registry_entries=registry_entries,
        runtime_tuning_context=None,
        wave_mint_provider_submit=True,
    )
    ladder_batches = [
        [item.registry_key or item.profile_url for item in chunk] for _, chunk in plan.dispatch_item_specs
    ]
    member_keys = sorted({str(row.get("profile_url_key") or "") for row in rows} - {""})
    case: dict[str, Any] = {
        "job_group": job_group,
        "provenance": provenance,
        # Exact-membership identity: the corpus counts sets, and the same roster
        # is reachable through several job tokens / snapshot ids, so every
        # aggregate below is also reported deduped by this hash.
        "member_set_sha256": hashlib.sha256("\n".join(member_keys).encode("utf-8")).hexdigest(),
        "input_reconstruction": {
            "refill_queue_state": "FABRICATED as 'ready' for every member",
            "actual_refill_queue_state_counts": dict(sorted(actual_queue_states.items())),
            "real_inputs": ["url population", "source shard tokens", "attempt/failure history"],
            "reconstructed_inputs": [
                "refill_queue_state",
                "refill_plan_batch_size/batch_count/window_url_count (zeroed)",
            ],
        },
        "member_count": len(urls),
        "distinct_shard_token_count": len({shard for shards in source_shards_by_url.values() for shard in shards}),
        "ladder": {
            "plan_reason": str(plan.plan_reason or ""),
            "batch_count": len(ladder_batches),
            "batch_sizes": [len(batch) for batch in ladder_batches],
            "deferred_item_count": len(plan.deferred_items),
            "batch_size_reason": str(dict(plan.dispatch_window or {}).get("batch_size_reason") or ""),
            "strategy": str(dict(plan.dispatch_window or {}).get("strategy") or ""),
        },
        "shadow_record_present": record is not None,
    }
    if record is None:
        case["skip"] = "structural_non_invocation"
        return case

    division = dict(record.get("division") or {})
    case["shadow_status"] = str(record.get("shadow_status") or "")
    case["engaged"] = bool(record.get("engaged"))
    case["skip_reason"] = str(record.get("skip_reason") or "")
    case["eligible_member_count"] = int(record.get("eligible_member_count") or 0)
    case["fallback_reason"] = str(dict(record.get("fallback_audit") or {}).get("fallback_reason") or "")
    comparison = dict(record.get("ladder_comparison") or {})
    case["ladder_comparison"] = comparison
    # A battery-REJECTED proposal has no `division` (the F5 fallback replaces
    # it), so the validator results live on the fallback audit instead.
    validator_results = list(division.get("validator_results") or []) or list(
        dict(record.get("fallback_audit") or {}).get("validator_results") or []
    )
    case["validator_results"] = [
        {"validator": str(entry.get("validator") or ""), "status": str(entry.get("status") or "")}
        for entry in validator_results
    ]
    case["apply_time_validator_results"] = [
        {"validator": str(entry.get("validator") or ""), "status": str(entry.get("status") or "")}
        for entry in list(record.get("apply_time_validator_results") or [])
    ]
    failing = [
        entry for entry in case["validator_results"] + case["apply_time_validator_results"] if entry["status"] == "fail"
    ]
    if failing:
        case["first_failing_validator"] = failing[0]["validator"]
    elif case["fallback_reason"].startswith("divider_validator_rejected:"):
        case["first_failing_validator"] = case["fallback_reason"].split(":", 1)[1]
    else:
        case["first_failing_validator"] = ""
    case["battery_valid"] = str(record.get("shadow_status") or "") == "proposed"

    if not division:
        return case

    ai_batches = _expand_division_membership(division, queue_items)
    case["ai"] = {
        "batch_count": int(division.get("batch_count") or 0),
        "batch_sizes": [len(batch) for batch in ai_batches],
        "coverage_ratio": (
            round(sum(len(batch) for batch in ai_batches) / max(1, len(queue_items)), 4) if queue_items else 0.0
        ),
        "envelope_utilisation": (
            round(sum(len(batch) for batch in ai_batches) / max(1, len(ai_batches) * 300), 4) if ai_batches else 0.0
        ),
    }
    case["mean_best_match_jaccard"] = _mean_best_match_jaccard(ladder_batches, ai_batches)
    case["rounds"] = {
        str(inflight): {
            "ladder": math.ceil(len(ladder_batches) / inflight) if ladder_batches else 0,
            "ai": math.ceil(len(ai_batches) / inflight) if ai_batches else 0,
        }
        for inflight in inflight_values
    }
    return case


def _expand_division_membership(division: Mapping[str, Any], queue_items: Sequence[Any]) -> list[list[str]]:
    """Expand `member_index_ranges` (canonical inventory ordering) to url keys."""

    keys = [str(getattr(item, "registry_key", "") or getattr(item, "profile_url", "")) for item in queue_items]
    batches: list[list[str]] = []
    for batch in list(division.get("batches") or []):
        members: list[str] = []
        for span in list(dict(batch).get("member_index_ranges") or []):
            try:
                start, end = int(span[0]), int(span[1])
            except (TypeError, ValueError, IndexError):
                continue
            members.extend(keys[start : end + 1])
        batches.append(members)
    return batches


def _mean_best_match_jaccard(ladder_batches: Sequence[Sequence[str]], ai_batches: Sequence[Sequence[str]]) -> float:
    """Mean over ladder batches of the best Jaccard overlap with any AI batch.

    The only metric that measures REGROUPING (the thing ruling ① buys); the
    shadow digest's sha comparison only answers "identical yes/no". Note the
    ladder's batches cover a strict subset of the ready set (it defers the
    surplus), so a low value is expected and is not by itself a defect.
    """

    if not ladder_batches or not ai_batches:
        return 0.0
    scores: list[float] = []
    ai_sets = [set(batch) for batch in ai_batches]
    for batch in ladder_batches:
        left = set(batch)
        if not left:
            continue
        best = 0.0
        for right in ai_sets:
            union = left | right
            if not union:
                continue
            best = max(best, len(left & right) / len(union))
        scores.append(best)
    return round(statistics.fmean(scores), 4) if scores else 0.0


# A production snapshot id STARTS with a UTC stamp (`YYYYmmddTHHMMSS`); a
# `<stamp>.<label>` variant is a real population under a quarantine/debt label
# and is kept (the exact-member-set dedupe below is what stops it inflating a
# count). A directory whose name does not start with a stamp at all is test
# residue — e.g. `snapshot-company-roster-prefetch-defers`, whose path literal is
# constructed by tests/test_worker_completion_pipeline.py — and must not be
# counted as a real population (adversarial finding 2026-07-25).
_PRODUCTION_SNAPSHOT_ID_PATTERN = re.compile(r"^\d{8}T\d{6}")


def _load_disk_ready_sets(
    *, runtime_dirs: Sequence[Path], min_urls: int, skipped_out: list[str] | None = None
) -> list[dict[str, Any]]:
    """On-disk ``candidate_documents.json`` rosters as replayable populations.

    READ THIS BEFORE TREATING A DISK CASE AS A REFILL WAVE. A disk case is a
    CANDIDATE ROSTER, not a refill queue: every candidate carrying a linkedin_url
    is taken as a pending item with no eligibility / already-fetched filter
    (unlike the production mint path), and its shard face is SYNTHESIZED from a
    single ``source_dataset`` token (1-2 values) versus 157-2,389 real shard
    tokens on the PG cases — so ``distinct_shard_token_count`` is NOT comparable
    between the two provenances. Disk sets exist only to widen the SIZE
    distribution: they are the only way to reach the 2,400+ band where V1
    (≤300/batch) and V2/V10 (≤8 batches) become jointly unsatisfiable.

    Test residue is excluded (non-timestamp snapshot ids) and sets loaded from a
    test runtime tree are tagged with their own provenance so they can be
    excluded from any denominator.
    """

    from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key

    ready_sets: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    skipped_non_production: list[str] = []
    for runtime_dir in runtime_dirs:
        company_assets = Path(runtime_dir) / "company_assets"
        if not company_assets.is_dir():
            continue
        test_tree = "test_env" in Path(runtime_dir).name
        for document in sorted(company_assets.glob("*/*/candidate_documents.json")):
            company = document.parent.parent.name
            snapshot_id = document.parent.name
            dedupe = f"{company}/{snapshot_id}"
            if dedupe in seen_keys:
                continue
            if not _PRODUCTION_SNAPSHOT_ID_PATTERN.match(snapshot_id):
                skipped_non_production.append(f"disk::{company}::{snapshot_id}")
                continue
            try:
                payload = json.loads(document.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            rows: list[dict[str, Any]] = []
            url_keys: set[str] = set()
            for candidate in list(dict(payload).get("candidates") or []):
                url = str(dict(candidate).get("linkedin_url") or "").strip()
                if not url:
                    continue
                key = normalize_linkedin_profile_url_key(url)
                if not key or key in url_keys:
                    continue
                url_keys.add(key)
                rows.append(
                    {
                        "profile_url": url,
                        "profile_url_key": key,
                        # SYNTHESIZED shard face — 1-2 source_dataset tokens, not
                        # the 157-2,389 real shard tokens the PG cases carry.
                        "source_shards_json": json.dumps(
                            [str(dict(candidate).get("source_dataset") or "unknown_shard")]
                        ),
                        "status": "",
                        "refill_queue_state": "<none: candidate roster, not a refill queue>",
                        "last_refill_attempt_count": 0,
                        "last_refill_deferred_reason": "",
                        "refill_terminal_status": "",
                    }
                )
            if len(rows) < min_urls:
                continue
            seen_keys.add(dedupe)
            ready_sets.append(
                {
                    "job_group": f"disk::{company}::{snapshot_id}",
                    "rows": rows,
                    "provenance": ("disk_snapshot_roster_test_env" if test_tree else "disk_snapshot_roster"),
                }
            )
    ready_sets.sort(key=lambda entry: (-len(entry["rows"]), entry["job_group"]))
    if skipped_out is not None:
        skipped_out.extend(sorted(skipped_non_production))
    return ready_sets


# The full validator battery the divider contract defines, so the report can
# state which members actually FIRED instead of implying whole-battery coverage.
_DIVIDER_VALIDATOR_IDS = (
    "V1_provider_envelope",
    "V2_batch_ceiling",
    "V3_exact_partition",
    "V4_tiny_batch_reason",
    "V5_worker_budget",
    "V6_wave_mint_only",
    "V7_reason_audit",
    "V8_retry_isolation",
    "V9_round_budget",
    "V10_batch_count_band",
)
# Validators that CANNOT return a non-pass on this replay, with the mechanism.
# Recorded so a reader never has to infer coverage from "all pass".
_DIVIDER_STRUCTURALLY_INERT_VALIDATORS = {
    "V5_worker_budget": (
        "profile_batch_division.py computes simulated_dispatched = min(batch_count, available) and then "
        "asks validate_v5_worker_budget whether that exceeds available — false by construction for every "
        "input, so V5 can only ever return pass/skipped"
    ),
    "V6_wave_mint_only": (
        "V6 fails only on a FOREIGN live division id; pre-S4 no writer exists, so the live set is empty "
        "by construction (the recorder's own docstring says so)"
    ),
    "V8_retry_isolation": (
        "the replay FABRICATES refill_queue_state='ready' for every member, so retry_wait_indices is empty "
        "in every case and V8 has nothing to isolate — the 2 genuine live retry_wait rows are overwritten"
    ),
}
_DIVIDER_UNEXERCISED_GATES = {
    "R6_durable_wave_inherited_window": (
        "the replay zeroes refill_plan_batch_size/batch_count/window_url_count, so prior_round_context "
        "recorded_wave_* is 0 for every case and the DURABLE_WAVE_BATCH_SIZE_REASON skip can never fire"
    ),
}


def _ai_r5_deferred_item_count(case: Mapping[str, Any], *, available_new_worker_count: int) -> int:
    """AI-side deferral under the SAME apply-time R5 bound the ladder is measured
    against — the like-for-like counterpart to `ladder.deferred_item_count`.

    Without this the corpus reported a six-figure ladder deferral next to an
    "AI covers 100% of eligible members" row, which compares a PLAN against a
    DISPATCH. A division is a plan: at apply time only
    ``min(batch_count, available_new_worker_count)`` batches dispatch and the
    surplus defers (profile_batch_division.py). A battery-REJECTED case produces
    no division at all — the ruling-④ F5 fallback runs the ladder plan verbatim,
    so its AI-side deferral IS the ladder's.

    CAVEAT (stated, not hidden): this applies the R5 batch bound only; it ignores
    the ladder's 50-url-per-actor-slot sizing rule, so it is an upper bound on how
    much the AI division would actually close.
    """

    if not case.get("battery_valid"):
        return int(dict(case.get("ladder") or {}).get("deferred_item_count") or 0)
    sizes = [int(value) for value in list(dict(case.get("ai") or {}).get("batch_sizes") or [])]
    dispatched = sum(sizes[: max(0, int(available_new_worker_count))])
    return max(0, int(case.get("eligible_member_count") or 0) - dispatched)


def _jaccard_clusters(member_sets: Sequence[frozenset[str]], *, threshold: float = 0.9) -> int:
    """Single-linkage cluster count at a Jaccard threshold — used to report how
    many INDEPENDENT populations a band really contains."""

    parent = list(range(len(member_sets)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    for left in range(len(member_sets)):
        for right in range(left + 1, len(member_sets)):
            union = member_sets[left] | member_sets[right]
            if not union:
                continue
            if len(member_sets[left] & member_sets[right]) / len(union) >= threshold:
                parent[find(left)] = find(right)
    return len({find(index) for index in range(len(member_sets))})


def run_divider(args: argparse.Namespace) -> dict[str, Any]:
    os.environ[_SCRIPTED_DIVIDER_ENV_KEY] = "1"
    live_census: dict[str, Any] = {}
    skipped_non_production: list[str] = []
    try:
        inflight_values = [int(value) for value in str(args.inflight).split(",") if str(value).strip()]
        ready_sets: list[dict[str, Any]] = []
        sources: list[str] = []
        if args.source in {"pg", "both"}:
            with read_only_live_connection(dsn=_resolve_dsn()) as connection:
                live_census = _live_refill_queue_state_census(connection, schema=args.schema)
                ready_sets.extend(
                    _load_divider_ready_sets(
                        connection, schema=args.schema, min_urls=int(args.min_urls), grouping=str(args.grouping)
                    )
                )
            sources.append(f"{args.schema}.linkedin_profile_registry (READ-ONLY, rich shard mix)")
        if args.source in {"disk", "both"}:
            runtime_dirs = [_REPO_ROOT / "runtime", _REPO_ROOT / "runtime" / "test_env_live"]
            ready_sets.extend(
                _load_disk_ready_sets(
                    runtime_dirs=runtime_dirs, min_urls=int(args.min_urls), skipped_out=skipped_non_production
                )
            )
            sources.append("runtime/**/company_assets/*/*/candidate_documents.json (READ-ONLY, coarse shard mix)")
        selected = ready_sets[: int(args.limit)]
        member_sets = {
            str(entry["job_group"]): frozenset(
                {str(row.get("profile_url_key") or "") for row in entry["rows"]} - {""}
            )
            for entry in selected
        }
        cases = [
            _divider_case(
                job_group=entry["job_group"],
                rows=entry["rows"],
                available_new_worker_count=int(args.available_workers),
                inflight_values=inflight_values,
                provenance=str(entry.get("provenance") or "live_pg_registry"),
            )
            for entry in selected
        ]
    finally:
        os.environ.pop(_SCRIPTED_DIVIDER_ENV_KEY, None)

    engaged = [case for case in cases if case.get("engaged")]
    valid = [case for case in engaged if case.get("battery_valid")]
    rejected = [case for case in engaged if not case.get("battery_valid")]
    jaccards = [float(case["mean_best_match_jaccard"]) for case in valid if "mean_best_match_jaccard" in case]
    failing_counts: dict[str, int] = {}
    for case in rejected:
        failing_counts[str(case.get("first_failing_validator") or "unknown")] = (
            failing_counts.get(str(case.get("first_failing_validator") or "unknown"), 0) + 1
        )
    fallback_counts: dict[str, int] = {}
    for case in cases:
        reason = str(case.get("fallback_reason") or "")
        if reason:
            fallback_counts[reason] = fallback_counts.get(reason, 0) + 1

    # ---- provenance split (a disk case is a candidate roster, not a refill queue)
    provenance_counts: dict[str, int] = {}
    rejected_provenance_counts: dict[str, int] = {}
    for case in cases:
        key = str(case.get("provenance") or "")
        provenance_counts[key] = provenance_counts.get(key, 0) + 1
    for case in rejected:
        key = str(case.get("provenance") or "")
        rejected_provenance_counts[key] = rejected_provenance_counts.get(key, 0) + 1

    # ---- exact-membership dedupe (the same roster is reachable via several
    # job tokens / snapshot ids, so raw case counts double-count populations)
    distinct_all = {str(case.get("member_set_sha256") or "") for case in cases}
    distinct_rejected = {str(case.get("member_set_sha256") or "") for case in rejected}
    above_ceiling = [case for case in cases if int(case.get("member_count") or 0) > 2400]
    distinct_above_ceiling = {str(case.get("member_set_sha256") or "") for case in above_ceiling}
    above_ceiling_sets = [member_sets.get(str(case.get("job_group") or ""), frozenset()) for case in above_ceiling]

    # ---- validator firing (only what actually fired; never "the battery")
    validator_status_counts: dict[str, dict[str, int]] = {}
    for case in cases:
        for entry in list(case.get("validator_results") or []) + list(case.get("apply_time_validator_results") or []):
            bucket = validator_status_counts.setdefault(str(entry.get("validator") or ""), {})
            status = str(entry.get("status") or "")
            bucket[status] = bucket.get(status, 0) + 1
    fired = sorted(
        validator_id
        for validator_id, statuses in validator_status_counts.items()
        if any(status not in {"pass", "skipped"} for status in statuses)
    )
    never_observed = sorted(set(_DIVIDER_VALIDATOR_IDS) - set(validator_status_counts))

    # ---- like-for-like deferral
    available = int(args.available_workers)
    ai_deferred_total = sum(_ai_r5_deferred_item_count(case, available_new_worker_count=available) for case in cases)
    ladder_deferred_total = sum(int(case["ladder"]["deferred_item_count"]) for case in cases)
    fallback_deferred_total = sum(int(case["ladder"]["deferred_item_count"]) for case in rejected)

    return {
        "mode": "divider",
        "honesty": _HONESTY_BANNER,
        "source": " + ".join(sources),
        "grouping": f"source_jobs_json / {args.grouping} (wave identity); disk sets are per (company, snapshot)",
        "input_reconstruction": {
            "headline": (
                "THESE ARE NOT OBSERVED READY SETS. Real url populations + real attempt/failure history; "
                "the refill queue state is FABRICATED as 'ready' for every member."
            ),
            "live_refill_queue_state_census": live_census
            or {"note": "disk-only run; the live census was not read this run"},
            "actual_live_ready_row_count": int(live_census.get("actual_ready_row_count", _KNOWN_LIVE_READY_ROW_COUNT)),
            "why": (
                "the live rows are terminal (fetched/completed); replaying their real queue_state collapses "
                "the plan to an empty wave, so nothing could be measured at all"
            ),
            "consequences": [
                "a 'set' is a wave's CUMULATIVE all-time membership, not items simultaneously awaiting refill",
                "V8 (retry isolation) can never fire — the 2 genuine live retry_wait rows are overwritten",
                "the R6 durable-wave gate can never fire — recorded wave fields are zeroed",
            ],
            "disk_case_caveat": (
                "a disk case is a CANDIDATE ROSTER, not a refill queue: no eligibility/already-fetched filter, "
                "and its shard face is synthesized from one source_dataset token (1-2 values) versus 157-2,389 "
                "real shard tokens on PG cases — distinct_shard_token_count is NOT comparable across provenances"
            ),
            "excluded_non_production_snapshots": skipped_non_production,
        },
        "summary": {
            "ready_set_count": len(cases),
            "provenance_counts": provenance_counts,
            "engaged_count": len(engaged),
            "below_engagement_threshold_count": sum(
                1 for case in cases if case.get("skip_reason") == "ready_set_at_or_below_engagement_threshold"
            ),
            "battery_valid_count": len(valid),
            "battery_rejected_count": len(rejected),
            "battery_rejected_provenance_counts": rejected_provenance_counts,
            "first_failing_validator_counts": failing_counts,
            "fallback_reason_counts": fallback_counts,
            "mean_best_match_jaccard_range": ([min(jaccards), max(jaccards)] if jaccards else []),
            "mean_best_match_jaccard_mean": round(statistics.fmean(jaccards), 4) if jaccards else 0.0,
            "ladder_deferred_item_total": ladder_deferred_total,
            # LIKE-FOR-LIKE: the AI side is a PLAN; at apply time the same R5
            # bound applies, and a battery-rejected case falls back to the
            # ladder plan verbatim. Reporting the ladder figure alone next to
            # "AI covers 100% of eligible members" compared a plan to a dispatch.
            "ai_deferred_item_total_under_same_r5_bound": ai_deferred_total,
            "ai_deferral_closed_vs_ladder": ladder_deferred_total - ai_deferred_total,
            "ai_deferral_closed_ratio": (
                round((ladder_deferred_total - ai_deferred_total) / ladder_deferred_total, 4)
                if ladder_deferred_total
                else 0.0
            ),
            "fallback_case_ladder_deferred_item_total": fallback_deferred_total,
            "deferral_comparison_caveat": (
                "the AI figure applies the R5 batch bound only and ignores the ladder's 50-url-per-actor-slot "
                "sizing rule, so it is an UPPER bound on what the division would close"
            ),
            "membership_identical_count": sum(
                1
                for case in valid
                if bool(dict(case.get("ladder_comparison") or {}).get("dispatched_membership_identical"))
            ),
            # FORCED BY CONSTRUCTION — not an empirical finding. See the note.
            "forced_by_construction": {
                "membership_identical_count": (
                    "compares the ladder's POST-R5 DISPATCHED partition against the AI's WHOLE-eligible-set "
                    "division. The ladder deferred >=1 item in every case here (min "
                    f"{min([int(case['ladder']['deferred_item_count']) for case in cases] or [0])}), so its "
                    "dispatched set is always a proper subset and this count cannot be anything but 0. It "
                    "carries NO information about whether the two divisions are different partitions."
                ),
                "mean_best_match_jaccard": (
                    "both slicers are contiguous over the same ordering, so the best-match Jaccard is "
                    "essentially the chunk-size ratio (e.g. ladder 4x50 vs AI 8x287 -> 50/287 = 0.174). It "
                    "measures the SIZE difference the two formulas produce, not regrouping quality."
                ),
            },
            "battery_coverage": {
                "validator_status_counts": validator_status_counts,
                "validators_that_actually_fired": fired,
                "validators_never_observed": never_observed,
                "structurally_unable_to_fail_on_this_input": _DIVIDER_STRUCTURALLY_INERT_VALIDATORS,
                "unexercised_gates": _DIVIDER_UNEXERCISED_GATES,
                "claim_scope": (
                    "This corpus evidences exactly the validators listed in validators_that_actually_fired "
                    "plus the pass-path of the rest. It does NOT evidence 'the validator battery'. It also "
                    "does not exercise non-ladder SHAPES: the scripted divider is a contiguous near-equal "
                    "split and the ladder is contiguous slicing, so only the chunk-size parameter varies."
                ),
            },
            # V1 (<=300/batch) AND V2/V10 (<=8 batches) are jointly unsatisfiable
            # above 8*300 = 2,400 eligible members: no legal division exists.
            # NOTE: this FACT is not a discovery of this corpus — it was already
            # committed in ScriptedProfileBatchDividerModelClient's docstring
            # (model_provider.py, commit e04b3a6, 2026-07-23). What the corpus
            # adds is the PREVALENCE, deduped below.
            "sets_above_v1_v2_joint_ceiling_2400": len(above_ceiling),
            "dedupe_by_exact_member_set": {
                "note": (
                    "the same roster is reachable through several job tokens and snapshot ids, so raw case "
                    "counts double-count populations; these are the counts by DISTINCT member set"
                ),
                "distinct_member_set_count": len(distinct_all),
                "distinct_battery_rejected_member_set_count": len(distinct_rejected),
                "distinct_sets_above_ceiling_2400": len(distinct_above_ceiling),
                "raw_rejected_ratio": (round(len(rejected) / len(cases), 4) if cases else 0.0),
                "deduped_rejected_ratio": (
                    round(len(distinct_rejected) / len(distinct_all), 4) if distinct_all else 0.0
                ),
                "above_ceiling_near_duplicate_cluster_count_jaccard_0_9": (
                    _jaccard_clusters(above_ceiling_sets) if above_ceiling_sets else 0
                ),
            },
        },
        "cases": cases,
    }


# ---------------------------------------------------------------------------
# OBJECTIVE 2b — promote divergence replay (READ-ONLY live schema)
# ---------------------------------------------------------------------------


_PROMOTE_ROW_COLUMNS = (
    "target_company",
    "company_key",
    "snapshot_id",
    "asset_view",
    "status",
    "authoritative",
    "candidate_count",
    "evidence_count",
    "profile_detail_count",
    "missing_linkedin_count",
    "profile_completion_backlog_count",
    "source_snapshot_count",
    "completeness_score",
    "completeness_band",
    "current_lane_effective_candidate_count",
    "former_lane_effective_candidate_count",
    "materialization_generation_key",
    "materialization_generation_sequence",
    "selected_snapshot_ids_json",
    "source_snapshot_selection_json",
)


def _load_promote_rows(connection: Any, *, schema: str) -> list[dict[str, Any]]:
    rows = _fetch_rows(
        connection,
        f"""
        SELECT {", ".join(_PROMOTE_ROW_COLUMNS)}
          FROM {_quoted(schema)}.organization_asset_registry
         ORDER BY company_key, snapshot_id
        """,
    )
    normalized: list[dict[str, Any]] = []
    for row in rows:
        record = dict(row)
        record["selected_snapshot_ids"] = [
            str(item or "").strip()
            for item in _json_load(row.get("selected_snapshot_ids_json"), [])
            if str(item or "").strip()
        ]
        record["source_snapshot_selection"] = _json_load(row.get("source_snapshot_selection_json"), {})
        record["authoritative"] = int(row.get("authoritative") or 0)
        normalized.append(record)
    return normalized


def _promote_pair(incumbent: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    from sourcing_agent.asset_reuse_planning import evaluate_organization_asset_registry_promotion
    from sourcing_agent.model_provider import ScriptedOrganizationPromoteJudgeModelClient
    from sourcing_agent.organization_promote_judgment import record_organization_promote_shadow

    decision = evaluate_organization_asset_registry_promotion(
        existing_authoritative=dict(incumbent),
        candidate_record=dict(candidate),
    )
    record = record_organization_promote_shadow(
        ScriptedOrganizationPromoteJudgeModelClient(mode="simulate"),
        existing_authoritative=dict(incumbent),
        candidate_record=dict(candidate),
        ladder_decision=decision,
    )
    ladder_reason = str(decision.get("reason") or "")
    payload: dict[str, Any] = {
        "company_key": str(candidate.get("company_key") or ""),
        "incumbent_snapshot_id": str(incumbent.get("snapshot_id") or ""),
        "candidate_snapshot_id": str(candidate.get("snapshot_id") or ""),
        "ladder_promote": bool(decision.get("promote")),
        "ladder_reason": ladder_reason,
        "shadow_record_present": record is not None,
    }
    if ladder_reason == _LADDER_FALLTHROUGH_REASON:
        # NAME COLLISION, disclosed per pair. `guard_rejected` is the final
        # else-branch of the LADDER's reason ladder (asset_reuse_planning.py) —
        # "no promote branch fired". It has nothing to do with the storage
        # lineage guard, whose verdict is the separate
        # `guard_predicted_verdict` field below and is frequently
        # {"refused": false} on exactly these pairs.
        payload["ladder_reason_semantics"] = _LADDER_FALLTHROUGH_NOTE
    if record is None:
        payload["skip"] = "structural_non_invocation"
        return payload
    payload["engaged"] = bool(record.get("engaged"))
    payload["contested"] = bool(record.get("contested"))
    payload["ai_status"] = str(record.get("ai_status") or "")
    payload["divergence"] = str(dict(record.get("ladder_comparison") or {}).get("divergence") or "")
    payload["guard_predicted_verdict"] = dict(record.get("guard_predicted_verdict") or {})
    decision_payload = dict(record.get("decision") or {})
    payload["validator_results"] = [
        {"validator": str(entry.get("validator") or ""), "status": str(entry.get("status") or "")}
        for entry in list(decision_payload.get("validator_results") or [])
    ]
    audit = dict(record.get("audit") or {})
    payload["fallback_reason"] = str(audit.get("fallback_reason") or "")
    payload["reason_code"] = str(dict(decision_payload.get("judgment") or decision_payload).get("reason_code") or "")
    return payload


# The ladder's catch-all reason literal (asset_reuse_planning.py): emitted when
# NONE of the four promote branches fires. It is NOT the storage lineage guard.
_LADDER_FALLTHROUGH_REASON = "guard_rejected"
_LADDER_FALLTHROUGH_NOTE = (
    "MISNOMER (pre-existing, in asset_reuse_planning.py): `guard_rejected` is the ladder's FALL-THROUGH "
    "label meaning 'no promote branch fired'. It does NOT mean the storage lineage guard refused — that "
    "verdict is `guard_predicted_verdict` and is often {'refused': false} on these very pairs."
)


def _promote_structural_inertness(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Which retained validators can even fire on the real corpus?"""

    empty_generation_key = sum(1 for row in rows if not str(row.get("materialization_generation_key") or "").strip())
    zero_lane_total = sum(
        1
        for row in rows
        if int(row.get("current_lane_effective_candidate_count") or 0)
        + int(row.get("former_lane_effective_candidate_count") or 0)
        == 0
    )
    empty_selection = sum(1 for row in rows if not list(row.get("selected_snapshot_ids") or []))
    return {
        "row_count": len(rows),
        "rows_with_empty_materialization_generation_key": empty_generation_key,
        "rows_with_zero_effective_lane_total": zero_lane_total,
        "rows_with_empty_selected_snapshot_ids": empty_selection,
        "note": (
            "V_GEN can never fire for a row with an empty generation key; V_COMP's coverage arm "
            "compares effective_lane_total, so a zero lane total makes that arm inert (or inverts "
            "it when the incumbent has a non-zero total and the candidate does not). "
            "coverage_evidence.shards is empty for every row by construction pre-S4."
        ),
    }


def run_promote(args: argparse.Namespace) -> dict[str, Any]:
    os.environ[_SCRIPTED_JUDGE_ENV_KEY] = "1"
    try:
        with read_only_live_connection(dsn=_resolve_dsn()) as connection:
            rows = _load_promote_rows(connection, schema=args.schema)
        by_company: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_company.setdefault(str(row.get("company_key") or ""), []).append(row)

        realistic: list[dict[str, Any]] = []
        for company, company_rows in sorted(by_company.items()):
            incumbents = [row for row in company_rows if int(row.get("authoritative") or 0) == 1]
            if not incumbents:
                continue
            incumbent = incumbents[0]
            for candidate in company_rows:
                if candidate is incumbent:
                    continue
                realistic.append(_promote_pair(incumbent, candidate))

        extended: list[dict[str, Any]] = []
        if args.corpus in {"extended", "both"}:
            for company, company_rows in sorted(by_company.items()):
                for incumbent in company_rows:
                    for candidate in company_rows:
                        if incumbent is candidate:
                            continue
                        extended.append(_promote_pair(incumbent, candidate))

        incidents = _promote_incident_scenarios(by_company)
    finally:
        os.environ.pop(_SCRIPTED_JUDGE_ENV_KEY, None)

    return {
        "mode": "promote",
        "honesty": _HONESTY_BANNER,
        "source": f"{args.schema}.organization_asset_registry (READ-ONLY)",
        "structural_inertness": _promote_structural_inertness(rows),
        "realistic": {
            "note": (
                "The ACTUAL authoritative incumbent vs every other recorded snapshot of the same "
                "company — what select_organization_asset_registry_promotion_candidate would see."
            ),
            "summary": _promote_summary(realistic),
            "pairs": realistic,
        },
        "extended": {
            "note": (
                "Every (incumbent, candidate) permutation within a company. Metric vectors are real; "
                "the incumbents are COUNTERFACTUAL — never quote this as 'historical decisions'. "
                "The rows are ALSO mutable live state, so this corpus is reproducible only against the "
                "same schema contents: quote it with the run date, never as a stable constant, and never "
                "from a surface (a docstring, a design table) that cannot carry the artifact with it."
            ),
            "summary": _promote_summary(extended),
            # EMITTED BY DEFAULT (2026-07-25). The extended corpus previously
            # retained summary counts only, yet its 246/806 figure was quoted in
            # a permanent production docstring — an unauditable claim. Every pair
            # row now ships so the number can be re-derived from the artifact.
            "pairs": [] if args.no_extended_pairs else extended,
        },
        "incidents": incidents,
    }


def _promote_summary(pairs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    divergence_counts: dict[str, int] = {}
    guard_reason_counts: dict[str, int] = {}
    validator_fail_counts: dict[str, int] = {}
    ladder_reason_counts: dict[str, int] = {}
    fallback_counts: dict[str, int] = {}
    ai_status_counts: dict[str, int] = {}
    for pair in pairs:
        divergence_counts[str(pair.get("divergence") or "")] = (
            divergence_counts.get(str(pair.get("divergence") or ""), 0) + 1
        )
        guard_reason = str(dict(pair.get("guard_predicted_verdict") or {}).get("reason") or "pass")
        guard_reason_counts[guard_reason] = guard_reason_counts.get(guard_reason, 0) + 1
        ladder_reason_counts[str(pair.get("ladder_reason") or "")] = (
            ladder_reason_counts.get(str(pair.get("ladder_reason") or ""), 0) + 1
        )
        ai_status_counts[str(pair.get("ai_status") or "")] = (
            ai_status_counts.get(str(pair.get("ai_status") or ""), 0) + 1
        )
        reason = str(pair.get("fallback_reason") or "")
        if reason:
            fallback_counts[reason] = fallback_counts.get(reason, 0) + 1
        for entry in list(pair.get("validator_results") or []):
            if str(entry.get("status") or "") == "fail":
                name = str(entry.get("validator") or "")
                validator_fail_counts[name] = validator_fail_counts.get(name, 0) + 1
    # DECOMPOSE the agreement headline. When the storage guard predicts a
    # refusal, V_LINEAGE's pre-filter fails and the judge's output CANNOT change
    # the outcome — agreement is structurally forced, not a quality signal. Only
    # the guard-pass pairs are decision-relevant.
    guard_refused_pairs = [
        pair for pair in pairs if bool(dict(pair.get("guard_predicted_verdict") or {}).get("refused"))
    ]
    guard_pass_pairs = [
        pair for pair in pairs if not bool(dict(pair.get("guard_predicted_verdict") or {}).get("refused"))
    ]
    guard_forced_agreements = sum(1 for pair in guard_refused_pairs if str(pair.get("divergence")) == "agree")
    agree_total = int(divergence_counts.get("agree", 0))
    return {
        "pair_count": len(pairs),
        "divergence_counts": divergence_counts,
        "ai_more_permissive_count": int(divergence_counts.get("ai_more_permissive", 0)),
        "ai_more_conservative_count": int(divergence_counts.get("ai_more_conservative", 0)),
        "agreement_decomposition": {
            "note": (
                "`agree` is NOT an AI-quality figure. Where the storage guard predicts a refusal the "
                "V_LINEAGE pre-filter fails and no judge output can change the outcome, so agreement is "
                "FORCED. Quote the judge-decisive line, not the raw agreement count."
            ),
            "agree_total": agree_total,
            "agree_forced_by_guard_refusal": guard_forced_agreements,
            "agree_where_the_judge_was_decision_relevant": agree_total - guard_forced_agreements,
            "judge_decision_relevant_pair_count": len(guard_pass_pairs),
            "guard_refused_pair_count": len(guard_refused_pairs),
            "guard_forced_share_of_agreements": (
                round(guard_forced_agreements / agree_total, 4) if agree_total else 0.0
            ),
            "agree_on_promote_pair_count": sum(
                1
                for pair in pairs
                if str(pair.get("divergence")) == "agree" and bool(pair.get("ladder_promote"))
            ),
        },
        "guard_predicted_reason_counts": guard_reason_counts,
        "ladder_reason_counts": ladder_reason_counts,
        "ladder_reason_semantics": {_LADDER_FALLTHROUGH_REASON: _LADDER_FALLTHROUGH_NOTE},
        "ai_status_counts": ai_status_counts,
        "validator_fail_counts": validator_fail_counts,
        "fallback_reason_counts": fallback_counts,
    }


def _promote_incident_scenarios(by_company: Mapping[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """The named mis-promote incidents from WS7_AI_PROMOTE_DESIGN §1.6.

    Reconstructed from the CURRENT recorded rows — the metric/lineage vectors are
    real, but this is the SHAPE of each incident as recorded today, not a
    byte-exact time-travel replay.
    """

    def _find(company: str, snapshot_id: str) -> dict[str, Any] | None:
        for row in by_company.get(company, []):
            if str(row.get("snapshot_id") or "") == snapshot_id:
                return row
        return None

    scenarios = [
        ("incident_1_openai_generation_rollback", "openai", "20260720T104157", "20260720T041551", {}),
        ("incident_1b_strict_subset_direction", "openai", "20260720T023543", "20260720T041551", {}),
        ("incident_2_google_simulate_pollution", "google", "20260720T221424", "20260720T152139", {}),
        ("incident_2c_polluted_incumbent_recovery", "google", "20260720T152139", "20260720T221424", {}),
        ("incident_3_tml_control", "thinkingmachineslab", "20260722T113432", "20260719T183049", {}),
    ]
    results: list[dict[str, Any]] = []
    for label, company, incumbent_id, candidate_id, overrides in scenarios:
        incumbent = _find(company, incumbent_id)
        candidate = _find(company, candidate_id)
        if incumbent is None or candidate is None:
            results.append(
                {
                    "scenario": label,
                    "status": "unreconstructable",
                    "reason": f"missing row(s): incumbent={incumbent_id} candidate={candidate_id}",
                }
            )
            continue
        payload = _promote_pair(incumbent, dict(candidate, **overrides))
        payload["scenario"] = label
        payload["status"] = "replayed"
        results.append(payload)
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_divider_report(report: Mapping[str, Any]) -> None:
    summary = dict(report.get("summary") or {})
    reconstruction = dict(report.get("input_reconstruction") or {})
    dedupe = dict(summary.get("dedupe_by_exact_member_set") or {})
    coverage = dict(summary.get("battery_coverage") or {})
    print("\n=== DIVIDER — ladder vs scripted division ===")
    print(f"source: {report.get('source')}  ({report.get('grouping')})")
    print(f"!! INPUT: {reconstruction.get('headline')}")
    print(
        f"   live rows actually in refill_queue_state='ready': "
        f"{reconstruction.get('actual_live_ready_row_count')}  "
        f"census={dict(reconstruction.get('live_refill_queue_state_census') or {}).get('refill_queue_state_counts')}"
    )
    if reconstruction.get("excluded_non_production_snapshots"):
        print(f"   excluded non-production snapshot dirs: {reconstruction.get('excluded_non_production_snapshots')}")
    print(
        f"reconstructed sets: {summary.get('ready_set_count')} {summary.get('provenance_counts')} "
        f"| distinct member sets: {dedupe.get('distinct_member_set_count')} "
        f"| engaged: {summary.get('engaged_count')} "
        f"| below OQ5 threshold: {summary.get('below_engagement_threshold_count')}"
    )
    print(
        f"battery valid: {summary.get('battery_valid_count')} | rejected: {summary.get('battery_rejected_count')} "
        f"{summary.get('first_failing_validator_counts')} provenance={summary.get('battery_rejected_provenance_counts')}"
    )
    print(
        f"rejected ratio: raw {dedupe.get('raw_rejected_ratio')} vs DEDUPED {dedupe.get('deduped_rejected_ratio')} "
        f"| >2400 band: {summary.get('sets_above_v1_v2_joint_ceiling_2400')} cases -> "
        f"{dedupe.get('distinct_sets_above_ceiling_2400')} distinct sets -> "
        f"{dedupe.get('above_ceiling_near_duplicate_cluster_count_jaccard_0_9')} clusters @ Jaccard>=0.9"
    )
    print(
        f"validators that actually FIRED: {coverage.get('validators_that_actually_fired')} | structurally unable "
        f"to fail here: {sorted(dict(coverage.get('structurally_unable_to_fail_on_this_input') or {}))} | "
        f"unexercised gates: {sorted(dict(coverage.get('unexercised_gates') or {}))}"
    )
    print(
        f"mean best-match Jaccard: mean={summary.get('mean_best_match_jaccard_mean')} "
        f"range={summary.get('mean_best_match_jaccard_range')}  [FORCED BY CONSTRUCTION — chunk-size ratio]"
    )
    print(
        f"membership_identical: {summary.get('membership_identical_count')}/{summary.get('battery_valid_count')} "
        f"[FORCED BY CONSTRUCTION — post-R5 dispatched subset vs whole-set division]"
    )
    print(
        f"deferred items, LIKE-FOR-LIKE under the same R5 bound: ladder "
        f"{summary.get('ladder_deferred_item_total')} vs AI "
        f"{summary.get('ai_deferred_item_total_under_same_r5_bound')} "
        f"(AI closes {summary.get('ai_deferral_closed_vs_ladder')} = "
        f"{summary.get('ai_deferral_closed_ratio')}; of which "
        f"{summary.get('fallback_case_ladder_deferred_item_total')} sit on battery-REJECTED sets where the "
        f"AI produces nothing and the ladder plan runs verbatim)"
    )
    header = f"{'job group':<52} {'n':>6} {'ladder':>14} {'ai':>12} {'jac':>6} {'battery':>10}"
    print(header)
    for case in list(report.get("cases") or []):
        ladder = dict(case.get("ladder") or {})
        ai = dict(case.get("ai") or {})
        sizes = list(ladder.get("batch_sizes") or [])
        ai_sizes = list(ai.get("batch_sizes") or [])
        print(
            f"{str(case.get('job_group'))[:52]:<52} {case.get('member_count'):>6} "
            f"{f'{len(sizes)}x{max(sizes) if sizes else 0}':>14} "
            f"{f'{len(ai_sizes)}x{max(ai_sizes) if ai_sizes else 0}':>12} "
            f"{case.get('mean_best_match_jaccard', 0.0):>6} "
            f"{('valid' if case.get('battery_valid') else (case.get('first_failing_validator') or case.get('skip_reason') or '-')):>10}"
        )


def _print_promote_report(report: Mapping[str, Any]) -> None:
    print("\n=== PROMOTE — ladder vs scripted judge ===")
    print(f"source: {report.get('source')}")
    inert = dict(report.get("structural_inertness") or {})
    print(
        f"structural inertness: {inert.get('rows_with_empty_materialization_generation_key')}/"
        f"{inert.get('row_count')} rows have NO generation key (V_GEN can never fire); "
        f"{inert.get('rows_with_zero_effective_lane_total')}/{inert.get('row_count')} have lane_total=0 "
        f"(V_COMP coverage arm inert)"
    )
    for corpus in ("realistic", "extended"):
        block = dict(report.get(corpus) or {})
        summary = dict(block.get("summary") or {})
        if not summary.get("pair_count"):
            continue
        decomposition = dict(summary.get("agreement_decomposition") or {})
        print(f"\n-- {corpus} corpus ({summary.get('pair_count')} pairs)")
        print(f"   divergence: {summary.get('divergence_counts')}")
        print(
            f"   agree {decomposition.get('agree_total')} DECOMPOSED: "
            f"{decomposition.get('agree_forced_by_guard_refusal')} forced by a guard refusal "
            f"(V_LINEAGE pre-filter fails; no judge output can change the outcome) + "
            f"{decomposition.get('agree_where_the_judge_was_decision_relevant')} where the judge mattered; "
            f"judge decision-relevant in {decomposition.get('judge_decision_relevant_pair_count')}/"
            f"{summary.get('pair_count')} pairs; agree-on-PROMOTE pairs: "
            f"{decomposition.get('agree_on_promote_pair_count')}"
        )
        print(
            f"   ai_more_permissive (= authority flips S5 would newly permit): {summary.get('ai_more_permissive_count')}"
        )
        print(f"   guard predicted: {summary.get('guard_predicted_reason_counts')}")
        print(f"   validator FAILs: {summary.get('validator_fail_counts')}")
        print(f"   ladder reasons (BRANCH FALL-THROUGH labels, not the storage guard): "
              f"{summary.get('ladder_reason_counts')}")
    print("\n-- incident scenarios (design §1.6)")
    for scenario in list(report.get("incidents") or []):
        if scenario.get("status") != "replayed":
            print(f"   {scenario.get('scenario'):<44} {scenario.get('status')}: {scenario.get('reason')}")
            continue
        print(
            f"   {scenario.get('scenario'):<44} ladder={'PROMOTE' if scenario.get('ladder_promote') else 'KEEP':<7} "
            f"({str(scenario.get('ladder_reason'))[:38]:<38}) ai={scenario.get('ai_status'):<16} "
            f"divergence={scenario.get('divergence')}"
        )


def _print_engagement_report(report: Mapping[str, Any]) -> None:
    print("\n=== ENGAGEMENT — do the shadow seams actually fire? ===")
    for key in ("divider", "promote"):
        block = dict(report.get(key) or {})
        record = dict(block.get("shadow_record") or {})
        print(f"\n-- {key}")
        print(f"   seam: {block.get('seam')}")
        print(f"   isolated schema: {block.get('isolated_schema')}")
        print(f"   ENGAGED: {block.get('engaged')}  (provider calls: {block.get('provider_calls')})")
        if not record:
            print("   NO SHADOW RECORD — the path is inert.")
            continue
        if key == "divider":
            division = dict(record.get("division") or {})
            print(
                f"   shadow_status={record.get('shadow_status')} division_id={record.get('division_id')} "
                f"eligible={record.get('eligible_member_count')} batches={division.get('batch_count')}"
            )
            print(f"   ladder_comparison={record.get('ladder_comparison')}")
        else:
            print(
                f"   contested={record.get('contested')} ai_status={record.get('ai_status')} "
                f"decision_id={record.get('decision_id')}"
            )
            print(f"   ladder_comparison={record.get('ladder_comparison')}")
            print(f"   guard_predicted_verdict={record.get('guard_predicted_verdict')}")


def _write(out_dir: Path | None, name: str, payload: Any) -> None:
    if out_dir is None:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"[written] {path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=("engagement", "divider", "promote", "all"))
    parser.add_argument("--schema", default=LIVE_SCHEMA_DEFAULT, help="live schema to READ (never written)")
    parser.add_argument("--out-dir", default="", help="directory for the JSON corpus")
    parser.add_argument("--min-urls", type=int, default=301, help="divider: minimum ready-set size")
    parser.add_argument(
        "--source",
        choices=("pg", "disk", "both"),
        default="pg",
        help="divider: replay live-PG wave groups, on-disk snapshots, or both",
    )
    parser.add_argument("--limit", type=int, default=40, help="divider: max ready sets")
    parser.add_argument(
        "--inflight", default="4,8", help="divider: actor_global_inflight values for the V9 rounds metric"
    )
    parser.add_argument("--available-workers", type=int, default=4, help="divider: available_new_worker_count")
    parser.add_argument(
        "--grouping",
        choices=("job_token", "job_tuple"),
        default="job_token",
        help="divider: how to reconstruct a wave-scoped ready set from source_jobs_json",
    )
    parser.add_argument("--corpus", choices=("realistic", "extended", "both"), default="both")
    parser.add_argument(
        "--no-extended-pairs",
        action="store_true",
        help="promote: emit ONLY the extended summary, dropping the per-pair rows (default: emit them, "
        "so every quoted extended figure is auditable from the artifact)",
    )
    parser.add_argument("--divider-url-count", type=int, default=600, help="engagement: synthetic ready-set size")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    assert_no_live_provider_gate()
    out_dir = Path(args.out_dir).expanduser().resolve() if str(args.out_dir or "").strip() else None

    print(_HONESTY_BANNER)

    if args.mode in {"engagement", "all"}:
        report = run_engagement(args)
        _print_engagement_report(report)
        _write(out_dir, "ws7_engagement_evidence.json", report)
    if args.mode in {"divider", "all"}:
        report = run_divider(args)
        _print_divider_report(report)
        _write(out_dir, "ws7_divider_divergence.json", report)
    if args.mode in {"promote", "all"}:
        report = run_promote(args)
        _print_promote_report(report)
        _write(out_dir, "ws7_promote_divergence.json", report)

    print("\n" + _HONESTY_BANNER)
    return 0


if __name__ == "__main__":
    sys.exit(main())
