from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit

SCHEMA_VERSION = "x.grok.collection.v1"
FIXTURE_LAB_ID = "openai"
FIXTURE_COUNTS = MappingProxyType(
    {
        "tasks": 8,
        "accounts": 24,
        "observations": 96,
        "candidate_packets": 24,
        "coverage_ledger": 8,
        "pages": 8,
    }
)

RUN_STATUS_REGISTRY: Mapping[str, bool] = MappingProxyType(
    {
        "queued": False,
        "running": False,
        "blocked": False,
        "completed": True,
        "failed": True,
        "cancelled": True,
    }
)
TASK_STATUS_REGISTRY: Mapping[str, bool] = MappingProxyType(
    {
        "pending": False,
        "queued": False,
        "running": False,
        "succeeded": True,
        "failed": True,
        "cancelled": True,
        "expired": True,
        "quarantined": True,
    }
)
PRETRAIN_RELEVANCE = frozenset({"PRETRAIN_CORE", "PRETRAIN_ADJACENT", "OUT_OF_SCOPE", "UNKNOWN"})
IN_SCOPE_RELEVANCE = frozenset({"PRETRAIN_CORE", "PRETRAIN_ADJACENT"})
STOP_REASONS = frozenset({"fixture_complete", "budget_reached", "query_saturated", "no_more_pages"})
PROHIBITED_FIELD_TOKENS = frozenset(
    {
        "ancestry",
        "bilingual",
        "citizenship",
        "country_of_origin",
        "ethnicity",
        "ethnic",
        "gender",
        "language",
        "nationality",
        "race",
        "racial",
        "religion",
    }
)
PROHIBITED_VALUE_TERMS = (
    "chinese researcher",
    "chinese researchers",
    "bilingual",
    "ethnicity",
    "ethnic background",
    "racial",
    "nationality",
    "citizenship",
    "ancestry",
    "religion",
    "gender",
    "race",
    "name origin",
    "language signal",
    "region signal",
    "school signal",
    "community signal",
    "华人",
    "中文",
)
FORBIDDEN_WRITER_KEYS = frozenset({"person_assets", "person_evidence", "crm_records", "projections", "exports"})
AFFILIATION_STATUS = frozenset({"current_proposed", "conflicted", "unknown"})
REVIEW_STATUS = frozenset({"fixture_ready", "quarantined"})
OBSERVATION_KINDS = frozenset(
    {
        "official_lab_output",
        "bio",
        "post",
        "reply",
        "official_interaction",
        "list_reference",
        "paper_link",
        "graph_edge",
    }
)
KIND_BY_QUERY_FAMILY: Mapping[str, str] = MappingProxyType(
    {
        "official_lab_output": "official_lab_output",
        "public_bio_affiliation": "bio",
        "first_party_technical_posts": "post",
        "replies_mentions": "reply",
        "official_lab_interactions": "official_interaction",
        "curated_lists": "list_reference",
        "paper_conference_linkage": "paper_link",
        "one_hop_graph_and_conflict_checks": "graph_edge",
    }
)
EXPECTED_QUERY_PURPOSE: Mapping[str, str] = MappingProxyType(
    {
        "official_lab_output": "Find first-party lab technical output and named professional contributors.",
        "public_bio_affiliation": "Collect public current-affiliation proposals from account bios.",
        "first_party_technical_posts": (
            "Find first-party posts about model training, systems, optimization, and data work."
        ),
        "replies_mentions": (
            "Collect bounded professional reply and mention evidence without treating it as identity truth."
        ),
        "official_lab_interactions": ("Collect interactions from authoritative lab accounts as evidence proposals."),
        "curated_lists": ("Use public professional lists as low-authority seeds that require independent evidence."),
        "paper_conference_linkage": (
            "Connect public paper or conference evidence to an external account without automatic person merge."
        ),
        "one_hop_graph_and_conflict_checks": (
            "Perform one-hop professional graph expansion and explicit conflict checks."
        ),
    }
)
EXPECTED_LAB_STATUS: Mapping[str, str] = MappingProxyType(
    {
        "openai": "fixture_enabled",
        "anthropic": "planned",
        "google_deepmind": "planned",
        "xai": "planned",
        "meta": "owner_decision_required",
        "thinking_machines_lab": "planned",
    }
)

OBJECT_FIELDS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "run": frozenset(
            {"run_id", "mode", "lab_scope", "status", "provider", "hard_budgets", "started_at", "completed_at"}
        ),
        "provider": frozenset(
            {"provider_id", "access_mode", "model_id", "prompt_version", "external_calls", "cost_usd"}
        ),
        "hard_budgets": frozenset({"max_tasks", "max_pages", "max_observations", "max_candidate_packets"}),
        "task": frozenset(
            {
                "task_id",
                "task_key",
                "lab_id",
                "query_family",
                "window",
                "query_template",
                "status",
                "stop_reason",
                "pages",
                "cursor",
                "provenance",
                "observation_ids",
            }
        ),
        "task.provenance": frozenset({"query_registry_version", "collector_contract_version"}),
        "account": frozenset(
            {"platform_user_id", "current_handle", "handle_history", "profile_url", "display_label", "observed_at"}
        ),
        "handle_history": frozenset({"handle", "observed_from", "observed_to"}),
        "observation": frozenset(
            {
                "observation_id",
                "platform_object_id",
                "platform_user_id",
                "kind",
                "canonical_url",
                "authored_at",
                "observed_at",
                "excerpt",
                "source_task_id",
                "query_family",
                "evidence_scope",
                "technical_scope",
            }
        ),
        "candidate_packet": frozenset(
            {
                "provisional_person_id",
                "platform_user_id",
                "current_affiliation_proposal",
                "pretrain_relevance",
                "selected_for_review",
                "evidence_refs",
                "review_status",
            }
        ),
        "affiliation": frozenset({"lab_id", "status", "evidence_refs"}),
        "coverage": frozenset({"lab_id", "query_family", "task_id", "observation_count", "stop_reason", "exhaustive"}),
        "claims": frozenset(
            {"exhaustive", "current_employment_guaranteed", "outreach_permission", "protected_traits_inferred"}
        ),
    }
)


@dataclass(frozen=True)
class SelectionMetrics:
    precision: float
    recall: float
    predicted_count: int
    relevant_count: int
    false_merge_count: int


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object at {path}")
    return payload


def stable_task_key(*, lab_id: str, query_family: str, contract_version: str, window: str) -> str:
    canonical = json.dumps(
        {
            "contract_version": contract_version,
            "lab_id": lab_id,
            "query_family": query_family,
            "window": window,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"xtask_{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:24]}"


def _expected_fixture_relevance(account_id: str) -> str | None:
    match = re.fullmatch(r"xuid_fixture_([0-9]{3})", account_id)
    if match is None:
        return None
    number = int(match.group(1))
    if 1 <= number <= 14:
        return "PRETRAIN_CORE"
    if number <= 20:
        return "PRETRAIN_ADJACENT"
    if number <= 22:
        return "OUT_OF_SCOPE"
    if number <= 24:
        return "UNKNOWN"
    return None


def _iter_values(value: Any, path: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], Any]]:
    yield path, value
    if isinstance(value, dict):
        for key, child in value.items():
            yield from _iter_values(child, (*path, str(key)))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _iter_values(child, (*path, str(index)))


def _is_fixture_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlsplit(value)
    return parsed.scheme == "https" and bool(parsed.hostname) and parsed.hostname.endswith(".invalid")


def _duplicate_values(values: Iterable[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return duplicates


def _validate_object_shape(
    value: Any,
    *,
    shape: str,
    location: str,
    errors: list[str],
) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{location} must be an object")
        return {}
    allowed = OBJECT_FIELDS[shape]
    missing = sorted(allowed - set(value))
    extra = sorted(set(value) - allowed)
    if missing:
        errors.append(f"{location} missing fields: {missing}")
    if extra:
        errors.append(f"{location} unexpected fields: {extra}")
    return value


def _field_tokens(field: str) -> set[str]:
    separated = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", field)
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", separated).strip("_").lower()
    return {normalized, *(part for part in normalized.split("_") if part)}


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _is_datetime(value: Any) -> bool:
    return _parse_datetime(value) is not None


def _contains_prohibited_term(value: str, term: str) -> bool:
    if term == "race":
        return re.search(r"(?<![a-z0-9])race(?![a-z0-9])", value) is not None
    return term.casefold() in value


def _prohibited_content_errors(value: Any, *, root: str) -> list[str]:
    errors: list[str] = []
    for path, child in _iter_values(value):
        location = ".".join((root, *path)) if path else root
        if path:
            normalized_key = path[-1].strip().lower().replace("-", "_")
            if _field_tokens(path[-1]) & PROHIBITED_FIELD_TOKENS:
                errors.append(f"prohibited protected/proxy field: {location}")
            if normalized_key in FORBIDDEN_WRITER_KEYS:
                errors.append(f"forbidden canonical writer field: {location}")
        if isinstance(child, str):
            folded = child.casefold()
            for term in PROHIBITED_VALUE_TERMS:
                if _contains_prohibited_term(folded, term):
                    errors.append(f"prohibited protected/proxy value at {location}")
                    break
            if (
                "x.com/" in folded
                or "twitter.com/" in folded
                or "https://x.com" in folded
                or "https://twitter.com" in folded
            ):
                errors.append(f"live X URL is forbidden in fixture at {location}")
    return sorted(set(errors))


def validate_collection(
    payload: Mapping[str, Any],
    *,
    labs_registry: Mapping[str, Any],
    query_registry: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    required_top_level = {
        "schema_version",
        "run",
        "tasks",
        "accounts",
        "observations",
        "candidate_packets",
        "identity_link_proposals",
        "coverage_ledger",
        "errors",
        "assertions",
        "claims",
    }
    missing = sorted(required_top_level - set(payload))
    extra = sorted(set(payload) - required_top_level)
    if missing:
        errors.append(f"missing top-level fields: {missing}")
    if extra:
        errors.append(f"unexpected top-level fields: {extra}")
    if payload.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version must be x.grok.collection.v1")

    run = _validate_object_shape(payload.get("run"), shape="run", location="run", errors=errors)
    run_status = str(run.get("status") or "")
    if run_status not in RUN_STATUS_REGISTRY:
        errors.append(f"unknown run status: {run_status or '<missing>'}")
    if run.get("mode") != "fixture":
        errors.append("run.mode must be fixture")
    if run_status != "completed":
        errors.append("canonical fixture run must be completed")
    if run.get("run_id") != "xrun_fixture_openai_v1":
        errors.append("fixture run ID mismatch")
    run_started_at = _parse_datetime(run.get("started_at"))
    run_completed_at = _parse_datetime(run.get("completed_at"))
    if run_started_at is None or run_completed_at is None:
        errors.append("fixture run timestamps must be timezone-aware ISO-8601 values")
    elif run_started_at > run_completed_at:
        errors.append("fixture run started_at must not follow completed_at")
    provider = _validate_object_shape(run.get("provider"), shape="provider", location="run.provider", errors=errors)
    if provider.get("access_mode") != "fixture":
        errors.append("provider.access_mode must be fixture")
    if provider.get("provider_id") != "offline_fixture" or provider.get("prompt_version") != "fixture-v1":
        errors.append("fixture provider identity/version mismatch")
    if type(provider.get("external_calls")) is not int or provider.get("external_calls") != 0:
        errors.append("fixture provider external_calls must be integer zero")
    if type(provider.get("cost_usd")) not in {int, float} or provider.get("cost_usd") != 0:
        errors.append("fixture provider cost_usd must be numeric zero")
    if provider.get("external_calls") != 0 or provider.get("cost_usd") != 0:
        errors.append("fixture provider must have zero external calls and cost")
    if provider.get("model_id") is not None:
        errors.append("fixture provider model_id must be null")
    budgets = _validate_object_shape(
        run.get("hard_budgets"), shape="hard_budgets", location="run.hard_budgets", errors=errors
    )
    if any(type(budgets.get(field)) is not int for field in OBJECT_FIELDS["hard_budgets"]):
        errors.append("fixture hard budgets must be integers")

    if labs_registry.get("schema_version") != "x.lab.registry.v1":
        errors.append("lab registry schema version mismatch")
    lab_items = labs_registry.get("labs") if isinstance(labs_registry.get("labs"), list) else []
    if len(lab_items) != len(EXPECTED_LAB_STATUS):
        errors.append("lab registry must contain the canonical six labs")
    labs = {str(item.get("lab_id")): item for item in lab_items if isinstance(item, dict) and item.get("lab_id")}
    if len(labs) != len(lab_items):
        errors.append("lab registry IDs must be unique and non-empty")
    for index, lab in enumerate(lab_items):
        if not isinstance(lab, dict) or set(lab) != {"lab_id", "display_name", "status"}:
            errors.append(f"labs[{index}] has an invalid shape")
            continue
        lab_id = str(lab.get("lab_id") or "")
        if EXPECTED_LAB_STATUS.get(lab_id) != lab.get("status"):
            errors.append(f"lab registry status mismatch: {lab_id}")
        if not isinstance(lab.get("display_name"), str) or not lab["display_name"].strip():
            errors.append(f"lab registry display name is invalid: {lab_id}")
    lab_scope = run.get("lab_scope") if isinstance(run.get("lab_scope"), list) else []
    if lab_scope != [FIXTURE_LAB_ID]:
        errors.append("first fixture lab_scope must be exactly ['openai']")
    if any(lab_id not in labs for lab_id in lab_scope):
        errors.append("run.lab_scope contains an unknown lab")
    if labs.get(FIXTURE_LAB_ID, {}).get("status") != "fixture_enabled":
        errors.append("OpenAI lab registry entry must be fixture_enabled")

    if query_registry.get("schema_version") != "x.query-family.registry.v1":
        errors.append("query registry schema version mismatch")
    query_values = query_registry.get("families") if isinstance(query_registry.get("families"), list) else []
    query_items: list[dict[str, Any]] = []
    for index, item in enumerate(query_values):
        if not isinstance(item, dict) or set(item) != {
            "query_family",
            "purpose",
            "fixture_enabled",
            "max_pages",
            "max_observations",
        }:
            errors.append(f"query families[{index}] has an invalid shape")
            continue
        if item.get("fixture_enabled") is not True:
            errors.append(f"query family must be fixture-enabled: {item.get('query_family')}")
        if type(item.get("max_pages")) is not int or item.get("max_pages") != 2:
            errors.append(f"query family max_pages must equal 2: {item.get('query_family')}")
        if type(item.get("max_observations")) is not int or item.get("max_observations") != 20:
            errors.append(f"query family max_observations must equal 20: {item.get('query_family')}")
        if not isinstance(item.get("purpose"), str) or not item["purpose"].strip():
            errors.append(f"query family purpose is invalid: {item.get('query_family')}")
        elif item["purpose"] != EXPECTED_QUERY_PURPOSE.get(str(item.get("query_family") or "")):
            errors.append(
                f"query family purpose does not match the canonical safety-reviewed text: {item.get('query_family')}"
            )
        query_items.append(item)
    query_names = [str(item["query_family"]) for item in query_items]
    query_families = set(query_names)
    query_specs = {str(item["query_family"]): item for item in query_items}
    if len(query_names) != len(query_families):
        errors.append("enabled query family names must be unique")
    if query_families != set(KIND_BY_QUERY_FAMILY):
        errors.append("fixture query registry must contain the canonical eight families")
    errors.extend(_prohibited_content_errors(labs_registry, root="labs_registry"))
    errors.extend(_prohibited_content_errors(query_registry, root="query_registry"))

    tasks_value = payload.get("tasks")
    if not isinstance(tasks_value, list):
        errors.append("tasks must be an array")
    tasks = tasks_value if isinstance(tasks_value, list) else []
    if len(tasks) != FIXTURE_COUNTS["tasks"]:
        errors.append(f"fixture must contain exactly {FIXTURE_COUNTS['tasks']} tasks")
    task_ids = [str(task.get("task_id")) for task in tasks if isinstance(task, dict)]
    if _duplicate_values(task_ids):
        errors.append("task_id values must be unique")
    task_keys = [str(task.get("task_key")) for task in tasks if isinstance(task, dict)]
    if _duplicate_values(task_keys):
        errors.append("task_key values must be unique")
    task_by_id = {str(task.get("task_id")): task for task in tasks if isinstance(task, dict)}
    task_family_names = [str(task.get("query_family")) for task in tasks if isinstance(task, dict)]
    task_family_counts = Counter(task_family_names)
    if set(task_family_names) != query_families:
        errors.append("fixture tasks must cover every enabled query family exactly once")
    if any(task_family_counts[family] != 1 for family in query_families):
        errors.append("each enabled query family must have exactly one task")
    for index, task_value in enumerate(tasks):
        task = _validate_object_shape(task_value, shape="task", location=f"tasks[{index}]", errors=errors)
        if not task:
            continue
        task_status = str(task.get("status") or "")
        if task_status not in TASK_STATUS_REGISTRY:
            errors.append(f"unknown task status: {task_status or '<missing>'}")
        elif task_status != "succeeded":
            errors.append(f"canonical completed fixture task must be succeeded: {task.get('task_id')}")
        if task.get("lab_id") != FIXTURE_LAB_ID:
            errors.append(f"fixture task lab mismatch: {task.get('task_id')}")
        query_family = str(task.get("query_family") or "")
        if query_family not in query_families:
            errors.append(f"fixture task has unknown query family: {task.get('task_id')}")
        expected_key = stable_task_key(
            lab_id=str(task.get("lab_id") or ""),
            query_family=query_family,
            contract_version=SCHEMA_VERSION,
            window=str(task.get("window") or ""),
        )
        if task.get("task_key") != expected_key:
            errors.append(f"task_key mismatch: {task.get('task_id')}")
        if task.get("task_id") != f"xtask_fixture_{query_family}":
            errors.append(f"fixture task ID mismatch: {task.get('task_id')}")
        if task.get("stop_reason") != "fixture_complete":
            errors.append(f"canonical fixture task stop_reason must be fixture_complete: {task.get('task_id')}")
        pages = task.get("pages")
        max_pages = query_specs.get(query_family, {}).get("max_pages")
        if type(pages) is not int or pages != 1 or type(max_pages) is not int or pages > max_pages:
            errors.append(f"fixture task page budget mismatch: {task.get('task_id')}")
        if task.get("window") != "fixture-v1" or task.get("query_template") != f"fixture://openai/{query_family}":
            errors.append(f"fixture task window/template mismatch: {task.get('task_id')}")
        if task.get("cursor") is not None:
            errors.append(f"fixture task cursor must be null: {task.get('task_id')}")
        provenance = _validate_object_shape(
            task.get("provenance"),
            shape="task.provenance",
            location=f"tasks[{index}].provenance",
            errors=errors,
        )
        if provenance.get("query_registry_version") != query_registry.get("schema_version"):
            errors.append(f"task query registry provenance mismatch: {task.get('task_id')}")
        if provenance.get("collector_contract_version") != SCHEMA_VERSION:
            errors.append(f"task collector contract provenance mismatch: {task.get('task_id')}")
        task_observation_ids = task.get("observation_ids")
        if not isinstance(task_observation_ids, list) or len(task_observation_ids) != 12:
            errors.append(f"fixture task must reference exactly 12 observations: {task.get('task_id')}")
        elif _duplicate_values(str(value) for value in task_observation_ids):
            errors.append(f"fixture task observation refs must be unique: {task.get('task_id')}")

    accounts_value = payload.get("accounts")
    if not isinstance(accounts_value, list):
        errors.append("accounts must be an array")
    accounts = accounts_value if isinstance(accounts_value, list) else []
    if len(accounts) != FIXTURE_COUNTS["accounts"]:
        errors.append(f"fixture must contain exactly {FIXTURE_COUNTS['accounts']} accounts")
    account_ids = [str(account.get("platform_user_id")) for account in accounts if isinstance(account, dict)]
    if _duplicate_values(account_ids):
        errors.append("platform_user_id values must be unique")
    account_by_id = {str(account.get("platform_user_id")): account for account in accounts if isinstance(account, dict)}
    current_handles = [str(account.get("current_handle")) for account in accounts if isinstance(account, dict)]
    if _duplicate_values(current_handles):
        errors.append("current_handle values must be unique in a fixture snapshot")
    profile_urls = [str(account.get("profile_url")) for account in accounts if isinstance(account, dict)]
    if _duplicate_values(profile_urls):
        errors.append("profile_url values must be unique across fixture accounts")
    expected_account_ids = {f"xuid_fixture_{number:03d}" for number in range(1, 25)}
    if set(account_ids) != expected_account_ids:
        errors.append("fixture account IDs must use the complete synthetic namespace 001-024")
    for index, account_value in enumerate(accounts):
        account = _validate_object_shape(account_value, shape="account", location=f"accounts[{index}]", errors=errors)
        if not account:
            continue
        account_id = str(account.get("platform_user_id") or "")
        if not re.fullmatch(r"xuid_fixture_[0-9]{3}", account_id):
            errors.append(f"account ID is not synthetic: {account_id}")
        if not re.fullmatch(r"fixture_openai_[0-9]{3}_v[12]", str(account.get("current_handle") or "")):
            errors.append(f"account handle is not synthetic: {account_id}")
        if not re.fullmatch(r"Synthetic Account [0-9]{3}", str(account.get("display_label") or "")):
            errors.append(f"account label is not synthetic: {account_id}")
        account_observed_at = _parse_datetime(account.get("observed_at"))
        if account_observed_at is None:
            errors.append(f"account observed_at is invalid: {account_id}")
        elif (
            run_started_at is not None
            and run_completed_at is not None
            and not run_started_at <= account_observed_at <= run_completed_at
        ):
            errors.append(f"account observed_at must fall within the run window: {account_id}")
        if not _is_fixture_url(account.get("profile_url")):
            errors.append(f"account profile_url is not a .invalid fixture URL: {account_id}")
        if not re.fullmatch(
            r"https://profiles\.invalid/openai/account-[0-9]{3}", str(account.get("profile_url") or "")
        ):
            errors.append(f"account profile_url is not synthetic: {account_id}")
        history = account.get("handle_history") if isinstance(account.get("handle_history"), list) else []
        if not history:
            errors.append(f"account handle history must not be empty: {account_id}")
        normalized_history = [
            _validate_object_shape(
                item,
                shape="handle_history",
                location=f"accounts[{index}].handle_history[{history_index}]",
                errors=errors,
            )
            for history_index, item in enumerate(history)
        ]
        active = [item for item in normalized_history if item and item.get("observed_to") is None]
        if len(active) != 1 or active[0].get("handle") != account.get("current_handle"):
            errors.append(f"account must have one active current handle: {account_id}")
        elif account_observed_at is not None:
            active_from = _parse_datetime(active[0].get("observed_from"))
            if active_from is not None and active_from > account_observed_at:
                errors.append(f"active handle must start on or before account observed_at: {account_id}")
        if not normalized_history or normalized_history[-1].get("handle") != account.get("current_handle"):
            errors.append(f"current handle must be the last history entry: {account_id}")
        handles = [str(item.get("handle")) for item in normalized_history if item]
        if _duplicate_values(handles) or any(
            not re.fullmatch(r"fixture_openai_[0-9]{3}_v[12]", handle) for handle in handles
        ):
            errors.append(f"account handle history is not unique and synthetic: {account_id}")
        for item in normalized_history:
            if not item:
                continue
            if not _is_datetime(item.get("observed_from")):
                errors.append(f"account handle observed_from is invalid: {account_id}")
            if item.get("observed_to") is not None and not _is_datetime(item.get("observed_to")):
                errors.append(f"account handle observed_to is invalid: {account_id}")
            observed_from = _parse_datetime(item.get("observed_from"))
            observed_to = _parse_datetime(item.get("observed_to")) if item.get("observed_to") is not None else None
            if observed_from is not None and observed_to is not None and observed_from > observed_to:
                errors.append(f"account handle interval is reversed: {account_id}")
        for prior, following in zip(normalized_history, normalized_history[1:], strict=False):
            if prior and following:
                prior_to = _parse_datetime(prior.get("observed_to"))
                following_from = _parse_datetime(following.get("observed_from"))
                if prior_to is None or following_from is None or prior_to > following_from:
                    errors.append(f"account handle history intervals overlap: {account_id}")

    observations_value = payload.get("observations")
    if not isinstance(observations_value, list):
        errors.append("observations must be an array")
    observations = observations_value if isinstance(observations_value, list) else []
    if len(observations) != FIXTURE_COUNTS["observations"]:
        errors.append(f"fixture must contain exactly {FIXTURE_COUNTS['observations']} observations")
    observation_ids = [str(item.get("observation_id")) for item in observations if isinstance(item, dict)]
    if _duplicate_values(observation_ids):
        errors.append("observation_id values must be unique")
    observation_by_id = {str(item.get("observation_id")): item for item in observations if isinstance(item, dict)}
    expected_observation_ids = {f"xobs_fixture_{number:03d}" for number in range(1, 97)}
    if set(observation_ids) != expected_observation_ids:
        errors.append("fixture observation IDs must use the complete synthetic namespace 001-096")
    platform_object_ids = [str(item.get("platform_object_id")) for item in observations if isinstance(item, dict)]
    if _duplicate_values(platform_object_ids):
        errors.append("platform_object_id values must be unique")
    canonical_urls = [str(item.get("canonical_url")) for item in observations if isinstance(item, dict)]
    if _duplicate_values(canonical_urls):
        errors.append("canonical_url values must be unique across fixture observations")
    expected_platform_object_ids = {f"xobject_fixture_{number:03d}" for number in range(1, 97)}
    if set(platform_object_ids) != expected_platform_object_ids:
        errors.append("fixture platform object IDs must use the complete synthetic namespace 001-096")
    for index, observation_value in enumerate(observations):
        item = _validate_object_shape(
            observation_value, shape="observation", location=f"observations[{index}]", errors=errors
        )
        if not item:
            continue
        observation_id = item.get("observation_id")
        if not re.fullmatch(r"xobs_fixture_[0-9]{3}", str(observation_id or "")):
            errors.append(f"observation ID is not synthetic: {observation_id}")
        if not re.fullmatch(r"xobject_fixture_[0-9]{3}", str(item.get("platform_object_id") or "")):
            errors.append(f"platform object ID is not synthetic: {observation_id}")
        authored_at = _parse_datetime(item.get("authored_at"))
        observed_at = _parse_datetime(item.get("observed_at"))
        if authored_at is None or observed_at is None:
            errors.append(f"observation timestamps are invalid: {observation_id}")
        elif authored_at > observed_at:
            errors.append(f"observation authored_at must not follow observed_at: {observation_id}")
        if (
            observed_at is not None
            and run_started_at is not None
            and run_completed_at is not None
            and not run_started_at <= observed_at <= run_completed_at
        ):
            errors.append(f"observation observed_at must fall within the run window: {observation_id}")
        task = task_by_id.get(str(item.get("source_task_id")))
        if item.get("platform_user_id") not in account_by_id:
            errors.append(f"observation references unknown account: {observation_id}")
        if task is None:
            errors.append(f"observation references unknown task: {observation_id}")
        elif item.get("query_family") != task.get("query_family"):
            errors.append(f"observation query family does not match task: {observation_id}")
        expected_kind = KIND_BY_QUERY_FAMILY.get(str(item.get("query_family") or ""))
        if item.get("kind") not in OBSERVATION_KINDS or item.get("kind") != expected_kind:
            errors.append(f"observation kind does not match query family: {observation_id}")
        if not _is_fixture_url(item.get("canonical_url")):
            errors.append(f"observation canonical_url is not a .invalid fixture URL: {observation_id}")
        if not re.fullmatch(
            r"https://evidence\.invalid/openai/observation-[0-9]{3}", str(item.get("canonical_url") or "")
        ):
            errors.append(f"observation canonical_url is not synthetic: {observation_id}")
        if len(str(item.get("excerpt") or "")) > 280:
            errors.append(f"observation excerpt exceeds 280 characters: {observation_id}")
        if not re.fullmatch(
            r"Synthetic professional evidence [0-9]{3} about model training systems, optimization, "
            r"data quality, and reliable research infrastructure\.",
            str(item.get("excerpt") or ""),
        ):
            errors.append(f"observation excerpt is not synthetic: {observation_id}")
        if item.get("evidence_scope") != "public_professional":
            errors.append(f"observation evidence scope mismatch: {observation_id}")
        if item.get("technical_scope") not in PRETRAIN_RELEVANCE:
            errors.append(f"observation technical scope is unknown: {observation_id}")

    for task_id, task in task_by_id.items():
        declared = task.get("observation_ids") if isinstance(task.get("observation_ids"), list) else []
        observed = [
            str(item.get("observation_id"))
            for item in observations
            if isinstance(item, dict) and item.get("source_task_id") == task_id
        ]
        if set(map(str, declared)) != set(observed) or len(declared) != len(observed):
            errors.append(f"task observation refs do not close over observations: {task_id}")
        max_observations = query_specs.get(str(task.get("query_family") or ""), {}).get("max_observations")
        if not isinstance(max_observations, int) or len(observed) > max_observations:
            errors.append(f"task exceeds query-family observation budget: {task_id}")

    packets_value = payload.get("candidate_packets")
    if not isinstance(packets_value, list):
        errors.append("candidate_packets must be an array")
    packets = packets_value if isinstance(packets_value, list) else []
    if len(packets) != FIXTURE_COUNTS["candidate_packets"]:
        errors.append(f"fixture must contain exactly {FIXTURE_COUNTS['candidate_packets']} candidate packets")
    provisional_ids = [str(packet.get("provisional_person_id")) for packet in packets if isinstance(packet, dict)]
    if _duplicate_values(provisional_ids):
        errors.append("provisional_person_id values must be unique")
    packet_account_ids = [str(packet.get("platform_user_id")) for packet in packets if isinstance(packet, dict)]
    if _duplicate_values(packet_account_ids):
        errors.append("a fixture account may have only one candidate packet")
    if set(packet_account_ids) != set(account_ids):
        errors.append("candidate packets must cover every fixture account exactly once")
    for index, packet_value in enumerate(packets):
        packet = _validate_object_shape(
            packet_value, shape="candidate_packet", location=f"candidate_packets[{index}]", errors=errors
        )
        if not packet:
            continue
        packet_id = packet.get("provisional_person_id")
        if not isinstance(packet_id, str) or not re.fullmatch(r"pp_x_[0-7][0-9A-HJKMNP-TV-Z]{25}", packet_id):
            errors.append(f"invalid provisional person id: {packet_id}")
        account_id = str(packet.get("platform_user_id") or "")
        if account_id not in account_by_id:
            errors.append(f"candidate packet references unknown account: {packet_id}")
        relevance = str(packet.get("pretrain_relevance") or "")
        if relevance not in PRETRAIN_RELEVANCE:
            errors.append(f"unknown pretrain relevance: {packet_id}")
        expected_relevance = _expected_fixture_relevance(account_id)
        if relevance != expected_relevance:
            errors.append(f"fixture relevance does not match the canonical gold population: {packet_id}")
        account_number_match = re.fullmatch(r"xuid_fixture_([0-9]{3})", account_id)
        expected_packet_id = (
            f"pp_x_01J{int(account_number_match.group(1)):023d}" if account_number_match is not None else None
        )
        if packet_id != expected_packet_id:
            errors.append(f"provisional person id does not match the synthetic fixture account: {packet_id}")
        affiliation = _validate_object_shape(
            packet.get("current_affiliation_proposal"),
            shape="affiliation",
            location=f"candidate_packets[{index}].current_affiliation_proposal",
            errors=errors,
        )
        affiliation_status = str(affiliation.get("status") or "")
        review_status = str(packet.get("review_status") or "")
        if affiliation.get("lab_id") != FIXTURE_LAB_ID or affiliation_status not in AFFILIATION_STATUS:
            errors.append(f"invalid current affiliation proposal: {packet_id}")
        if review_status not in REVIEW_STATUS:
            errors.append(f"invalid review status: {packet_id}")
        selected_expected = (
            relevance in IN_SCOPE_RELEVANCE
            and affiliation_status == "current_proposed"
            and review_status == "fixture_ready"
        )
        if type(packet.get("selected_for_review")) is not bool:
            errors.append(f"selected_for_review must be boolean: {packet_id}")
        elif packet.get("selected_for_review") != selected_expected:
            errors.append(f"selected_for_review contradicts relevance: {packet_id}")
        if affiliation_status in {"unknown", "conflicted"} and review_status != "quarantined":
            errors.append(f"unresolved affiliation must be quarantined: {packet_id}")
        if relevance == "UNKNOWN" and (affiliation_status != "unknown" or review_status != "quarantined"):
            errors.append(f"unknown relevance must be quarantined with unknown affiliation: {packet_id}")
        evidence_refs = packet.get("evidence_refs") if isinstance(packet.get("evidence_refs"), list) else []
        if len(evidence_refs) != 4 or _duplicate_values(str(ref) for ref in evidence_refs):
            errors.append(f"candidate packet must have four unique evidence refs: {packet_id}")
        account_observation_ids = {
            str(observation.get("observation_id"))
            for observation in observations
            if isinstance(observation, dict) and observation.get("platform_user_id") == account_id
        }
        if set(map(str, evidence_refs)) != account_observation_ids:
            errors.append(f"candidate packet evidence must close over its account observations: {packet_id}")
        for ref in evidence_refs:
            observation = observation_by_id.get(str(ref))
            if observation is None:
                errors.append(f"candidate packet references unknown evidence: {packet_id}:{ref}")
            elif observation.get("platform_user_id") != account_id:
                errors.append(f"candidate packet evidence belongs to another account: {packet_id}:{ref}")
            elif observation.get("technical_scope") != relevance:
                errors.append(f"candidate packet relevance conflicts with evidence: {packet_id}:{ref}")
        affiliation_refs = (
            affiliation.get("evidence_refs") if isinstance(affiliation.get("evidence_refs"), list) else []
        )
        if len(affiliation_refs) != 2 or _duplicate_values(str(ref) for ref in affiliation_refs):
            errors.append(f"affiliation proposal must have two unique evidence refs: {packet_id}")
        affiliation_kinds: set[str] = set()
        for ref in affiliation_refs:
            observation = observation_by_id.get(str(ref))
            if ref not in evidence_refs:
                errors.append(f"affiliation evidence is outside candidate packet: {packet_id}:{ref}")
            if observation is None:
                errors.append(f"affiliation references unknown evidence: {packet_id}:{ref}")
            elif observation.get("platform_user_id") != account_id:
                errors.append(f"affiliation evidence belongs to another account: {packet_id}:{ref}")
            else:
                affiliation_kinds.add(str(observation.get("kind") or ""))
        if not affiliation_kinds & {"official_lab_output", "bio", "official_interaction", "paper_link"}:
            errors.append(f"affiliation proposal lacks a bounded professional affiliation source: {packet_id}")

    relevance_counts = Counter(str(packet.get("pretrain_relevance")) for packet in packets if isinstance(packet, dict))
    if relevance_counts != Counter({"PRETRAIN_CORE": 14, "PRETRAIN_ADJACENT": 6, "OUT_OF_SCOPE": 2, "UNKNOWN": 2}):
        errors.append("fixture relevance distribution must remain 14/6/2/2")

    coverage_value = payload.get("coverage_ledger")
    if not isinstance(coverage_value, list):
        errors.append("coverage_ledger must be an array")
    coverage = coverage_value if isinstance(coverage_value, list) else []
    if len(coverage) != FIXTURE_COUNTS["coverage_ledger"]:
        errors.append(f"fixture must contain exactly {FIXTURE_COUNTS['coverage_ledger']} coverage entries")
    coverage_family_names = [str(item.get("query_family")) for item in coverage if isinstance(item, dict)]
    coverage_family_counts = Counter(coverage_family_names)
    coverage_by_family = {str(item.get("query_family")): item for item in coverage if isinstance(item, dict)}
    if set(coverage_by_family) != query_families:
        errors.append("coverage ledger must contain every enabled query family")
    if any(coverage_family_counts[family] != 1 for family in query_families):
        errors.append("each enabled query family must have exactly one coverage entry")
    for index, coverage_value in enumerate(coverage):
        entry = _validate_object_shape(
            coverage_value, shape="coverage", location=f"coverage_ledger[{index}]", errors=errors
        )
        if not entry:
            continue
        family = str(entry.get("query_family") or "")
        observed_count = sum(
            1 for item in observations if isinstance(item, dict) and item.get("query_family") == family
        )
        if entry.get("observation_count") != observed_count:
            errors.append(f"coverage observation count mismatch: {family}")
        if entry.get("exhaustive") is not False:
            errors.append(f"coverage must remain non-exhaustive: {family}")
        if entry.get("stop_reason") != "fixture_complete":
            errors.append(f"canonical coverage stop_reason must be fixture_complete: {family}")
        task = task_by_id.get(str(entry.get("task_id") or ""))
        if (
            entry.get("lab_id") != FIXTURE_LAB_ID
            or task is None
            or task.get("query_family") != family
            or task.get("lab_id") != entry.get("lab_id")
            or task.get("stop_reason") != entry.get("stop_reason")
        ):
            errors.append(f"coverage task/family/lab linkage mismatch: {family}")

    expected_budgets = {
        "max_tasks": FIXTURE_COUNTS["tasks"],
        "max_pages": FIXTURE_COUNTS["pages"],
        "max_observations": FIXTURE_COUNTS["observations"],
        "max_candidate_packets": FIXTURE_COUNTS["candidate_packets"],
    }
    for field, expected in expected_budgets.items():
        if budgets.get(field) != expected:
            errors.append(f"hard budget {field} must equal {expected}")
    consumed_pages = sum(
        task.get("pages") if isinstance(task, dict) and type(task.get("pages")) is int else 0 for task in tasks
    )
    if consumed_pages != FIXTURE_COUNTS["pages"]:
        errors.append("fixture task pages must consume the exact page budget")

    if payload.get("identity_link_proposals") != []:
        errors.append("fixture identity_link_proposals must be empty")
    if payload.get("assertions") != []:
        errors.append("fixture assertions must be empty")
    if payload.get("errors") != []:
        errors.append("canonical fixture errors must be empty")
    claims = _validate_object_shape(payload.get("claims"), shape="claims", location="claims", errors=errors)
    for field in ("exhaustive", "current_employment_guaranteed", "outreach_permission", "protected_traits_inferred"):
        if claims.get(field) is not False:
            errors.append(f"claims.{field} must be false")

    errors.extend(_prohibited_content_errors(payload, root="payload"))

    return sorted(set(errors))


def evaluate_selection(payload: Mapping[str, Any], gold: Mapping[str, Any]) -> SelectionMetrics:
    packet_values = payload.get("candidate_packets")
    packets = packet_values if isinstance(packet_values, list) else []
    predicted = {
        str(packet.get("platform_user_id"))
        for packet in packets
        if isinstance(packet, dict) and packet.get("selected_for_review") is True
    }
    relevant_values = gold.get("relevant_platform_user_ids")
    relevant = {str(value) for value in relevant_values} if isinstance(relevant_values, list) else set()
    true_positive = len(predicted & relevant)
    precision = true_positive / len(predicted) if predicted else 0.0
    recall = true_positive / len(relevant) if relevant else 0.0
    link_proposals = payload.get("identity_link_proposals")
    false_merges = len(link_proposals) if isinstance(link_proposals, list) else 1
    return SelectionMetrics(
        precision=precision,
        recall=recall,
        predicted_count=len(predicted),
        relevant_count=len(relevant),
        false_merge_count=false_merges,
    )


def validate_acceptance(
    payload: Mapping[str, Any],
    gold: Mapping[str, Any],
    *,
    metrics: SelectionMetrics | None = None,
) -> list[str]:
    errors: list[str] = []
    required_gold_fields = {
        "schema_version",
        "lab_id",
        "labels",
        "relevant_platform_user_ids",
        "minimum_precision",
        "minimum_recall",
        "expected_false_merge_count",
        "expected_protected_output_count",
    }
    missing = sorted(required_gold_fields - set(gold))
    extra = sorted(set(gold) - required_gold_fields)
    if missing:
        errors.append(f"gold missing fields: {missing}")
    if extra:
        errors.append(f"gold unexpected fields: {extra}")
    if gold.get("schema_version") != "x.fixture.gold.v1" or gold.get("lab_id") != FIXTURE_LAB_ID:
        errors.append("gold schema/lab mismatch")

    account_values = payload.get("accounts")
    accounts = account_values if isinstance(account_values, list) else []
    account_ids = {str(account.get("platform_user_id")) for account in accounts if isinstance(account, dict)}
    labels = gold.get("labels") if isinstance(gold.get("labels"), dict) else {}
    if set(map(str, labels)) != account_ids:
        errors.append("gold labels must cover every fixture account exactly once")
    if any(not isinstance(label, str) or label not in PRETRAIN_RELEVANCE for label in labels.values()):
        errors.append("gold contains an unknown relevance label")
    packet_values = payload.get("candidate_packets")
    packets = packet_values if isinstance(packet_values, list) else []
    packet_labels = {
        str(packet.get("platform_user_id")): packet.get("pretrain_relevance")
        for packet in packets
        if isinstance(packet, dict)
    }
    if labels != packet_labels:
        errors.append("gold labels must exactly match candidate packet relevance")
    expected_relevant = {
        str(account_id)
        for account_id, label in labels.items()
        if isinstance(label, str) and label in IN_SCOPE_RELEVANCE
    }
    relevant_values = gold.get("relevant_platform_user_ids")
    relevant_ids = [str(value) for value in relevant_values] if isinstance(relevant_values, list) else []
    if _duplicate_values(relevant_ids) or set(relevant_ids) != expected_relevant:
        errors.append("gold relevant IDs must exactly match in-scope labels")
    if not set(relevant_ids) <= account_ids:
        errors.append("gold relevant IDs reference unknown fixture accounts")

    minimum_precision = gold.get("minimum_precision")
    minimum_recall = gold.get("minimum_recall")
    if type(minimum_precision) not in {int, float} or minimum_precision != 0.95:
        errors.append("gold minimum_precision must equal 0.95")
    if type(minimum_recall) not in {int, float} or minimum_recall != 0.9:
        errors.append("gold minimum_recall must equal 0.9")
    if type(gold.get("expected_false_merge_count")) is not int or gold.get("expected_false_merge_count") != 0:
        errors.append("gold expected_false_merge_count must be zero")
    if type(gold.get("expected_protected_output_count")) is not int or gold.get("expected_protected_output_count") != 0:
        errors.append("gold expected_protected_output_count must be zero")

    observed = metrics or evaluate_selection(payload, gold)
    if type(minimum_precision) in {int, float} and observed.precision < minimum_precision:
        errors.append("selection precision is below the gold threshold")
    if type(minimum_recall) in {int, float} and observed.recall < minimum_recall:
        errors.append("selection recall is below the gold threshold")
    if observed.false_merge_count != gold.get("expected_false_merge_count"):
        errors.append("selection false-merge count does not match gold")
    if observed.predicted_count != 20 or observed.relevant_count != 20:
        errors.append("selection population must contain exactly 20 predicted and 20 relevant accounts")
    return sorted(set(errors))


def _default_paths() -> tuple[Path, Path, Path, Path]:
    root = project_root()
    return (
        root / "fixtures/openai_pretrain_fixture_v1.json",
        root / "fixtures/openai_pretrain_gold_v1.json",
        root / "configs/labs.v1.json",
        root / "configs/query_families.v1.json",
    )


def main() -> int:
    default_fixture, default_gold, default_labs, default_queries = _default_paths()
    parser = argparse.ArgumentParser(description="Validate the fixture-only x.grok.collection.v1 artifact")
    parser.add_argument("--fixture", type=Path, default=default_fixture)
    parser.add_argument("--gold", type=Path, default=default_gold)
    parser.add_argument("--labs", type=Path, default=default_labs)
    parser.add_argument("--query-families", type=Path, default=default_queries)
    args = parser.parse_args()
    fixture = load_json(args.fixture)
    gold = load_json(args.gold)
    errors = validate_collection(
        fixture,
        labs_registry=load_json(args.labs),
        query_registry=load_json(args.query_families),
    )
    metrics = evaluate_selection(fixture, gold)
    errors.extend(validate_acceptance(fixture, gold, metrics=metrics))
    errors = sorted(set(errors))
    result = {"errors": errors, "metrics": asdict(metrics), "status": "valid" if not errors else "invalid"}
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
