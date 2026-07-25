from __future__ import annotations

import ast
from collections import Counter
from functools import cache
from pathlib import Path
from typing import TypeAlias

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
SCRIPTS_ROOT = REPO_ROOT / "scripts"
CHARACTERIZATION_DOC_PATH = REPO_ROOT / "docs" / "TRACK_D_D3A_COMPANY_IDENTITY_PERSISTENCE_SURFACE_CHARACTERIZATION.md"
SCRIPTED_DELTA_PATH = SCRIPTS_ROOT / "dev_scripted_openai_agent_delta.sh"

CallPoint: TypeAlias = tuple[str, str]
FallbackPoint: TypeAlias = tuple[str, str, str]

TARGET_CALL_SYMBOLS = frozenset(
    {
        "_combined_company_aliases",
        "_combined_company_identities",
        "_discover_local_company_identities",
        "_load_cached_company_identity_registry_records",
        "_load_seed_company_identity_records",
        "_mirror_snapshot_to_hot_cache",
        "load_company_identity_registry",
        "load_company_identity_seed_catalog",
        "load_company_snapshot_identity",
        "mirror_tree_link_first",
        "refresh_company_identity_registry",
        "resolve_company_identity",
        "resolve_manual_company_identity",
        "restore_bundle",
        "upsert_company_identity_registry_entry",
    }
)


EXPECTED_REGISTRY_WRITERS = frozenset(
    {
        ("company_registry.py", "refresh_company_identity_registry"),
        ("company_registry.py", "upsert_company_identity_registry_entry"),
    }
)

EXPECTED_REFRESH_CALLERS = Counter(
    {
        ("asset_registration.py", "sync_company_asset_registration"): 1,
        ("cloud_asset_import.py", "_post_import_runtime_refresh"): 1,
        ("organization_assets.py", "warmup_existing_organization_assets"): 1,
    }
)

EXPECTED_UPSERT_CALLERS = Counter(
    {
        ("acquisition.py", "AcquisitionEngine._resolve_company"): 1,
    }
)

EXPECTED_REGISTRY_READ_CHAIN_CALLERS = {
    "load_company_identity_registry": Counter(
        {("company_registry.py", "_load_cached_company_identity_registry_records"): 1}
    ),
    "_load_cached_company_identity_registry_records": Counter(
        {
            ("company_registry.py", "upsert_company_identity_registry_entry"): 1,
            ("company_registry.py", "_discover_local_company_identities"): 1,
        }
    ),
    "_discover_local_company_identities": Counter(
        {
            ("company_registry.py", "builtin_company_identity"): 1,
            ("company_registry.py", "_combined_company_identities"): 1,
        }
    ),
    "_combined_company_identities": Counter(
        {
            ("company_registry.py", "infer_target_company_from_text"): 1,
            ("company_registry.py", "_company_text_aliases"): 1,
            ("company_registry.py", "_combined_company_aliases"): 1,
        }
    ),
    "_combined_company_aliases": Counter({("company_registry.py", "resolve_company_alias_key"): 1}),
}

EXPECTED_SEED_CATALOG_READ_CHAIN_CALLERS = {
    "load_company_identity_seed_catalog": Counter(
        {
            ("company_registry.py", "_load_seed_company_identity_records"): 1,
            ("company_registry.py", "refresh_company_identity_registry"): 1,
        }
    ),
    "_load_seed_company_identity_records": Counter(
        {
            ("company_registry.py", "refresh_company_identity_registry"): 1,
            ("company_registry.py", "upsert_company_identity_registry_entry"): 1,
            ("company_registry.py", "_discover_local_company_identities"): 1,
        }
    ),
}

EXPECTED_GENERIC_SNAPSHOT_MATERIALIZER_CALLERS = {
    "restore_bundle": Counter(
        {
            ("cli.py", "main"): 1,
            ("cloud_asset_import.py", "import_cloud_assets"): 1,
        }
    ),
    "mirror_tree_link_first": Counter({("acquisition.py", "AcquisitionEngine._mirror_snapshot_to_hot_cache"): 1}),
    "_mirror_snapshot_to_hot_cache": Counter({("acquisition.py", "AcquisitionEngine._sync_snapshot_hot_cache"): 1}),
}

EXPECTED_CENTRAL_READER_CALLS = Counter(
    {
        ("artifact_cache.py", "collect_hot_cache_inventory"): 1,
        ("artifact_cache.py", "_repair_hot_cache_company_dir"): 1,
        ("asset_paths.py", "build_company_snapshot_match_entry"): 1,
        ("asset_paths.py", "resolve_company_snapshot_match_selection"): 1,
        ("asset_reuse_planning.py", "_load_available_organization_asset_registry_records"): 1,
        ("asset_sync.py", "AssetBundleManager._build_candidate_generation_manifest"): 1,
        ("authoritative_serving_repair.py", "_write_repair_snapshot"): 1,
        ("candidate_artifacts.py", "materialize_company_candidate_view"): 1,
        ("candidate_artifacts.py", "_iter_filtered_company_snapshot_groups"): 1,
        ("candidate_artifacts.py", "audit_candidate_artifact_hot_cache"): 1,
        ("candidate_artifacts.py", "cleanup_candidate_artifact_hot_cache"): 1,
        ("candidate_artifacts.py", "repair_missing_company_candidate_artifacts"): 1,
        (
            "candidate_artifacts.py",
            "rewrite_structured_timeline_in_company_candidate_artifacts",
        ): 1,
        ("candidate_artifacts.py", "_load_snapshot_provider_function_id_map"): 1,
        ("candidate_artifacts.py", "_export_compatibility_artifacts_from_serving_view"): 1,
        ("candidate_artifacts.py", "_resolve_company_snapshot"): 2,
        ("organization_assets.py", "discover_normalized_company_snapshots"): 1,
        ("runtime_rebuild.py", "rebuild_runtime_company_asset_control_plane"): 1,
    }
)

EXPECTED_RESOLVER_CALLS_BY_FILE = Counter(
    {
        "acquisition.py": 5,
        "company_asset_supplement.py": 1,
        "connectors.py": 1,
        "excel_intake.py": 8,
        "orchestrator.py": 1,
        "request_normalization.py": 1,
    }
)

EXPECTED_MANUAL_RESOLVER_CALLS_BY_FILE = Counter(
    {
        "acquisition.py": 1,
        "request_normalization.py": 1,
    }
)

# This is the currently recognized Python function population whose nearest
# lexical owner contains an ``identity.json`` literal. Dynamic/helper-composed
# paths are outside this exact lexical oracle and must update the Scout rather
# than being inferred safe from a green test.
EXPECTED_IDENTITY_LITERAL_CLASSIFICATION = {
    ("acquisition.py", "AcquisitionEngine._resolve_company"): ("production_snapshot_writer__pg_owner_then_mirror"),
    ("acquisition.py", "AcquisitionEngine._resolve_snapshot_hydration_identity"): (
        "production_semantic_reader_bypass__route_pg_retire_authority"
    ),
    ("asset_paths.py", "load_company_snapshot_identity"): ("central_snapshot_reader__bridge_then_retire_authority"),
    ("asset_paths.py", "_snapshot_dir_serving_preference_sort_key"): "existence_only_probe",
    ("asset_sync.py", "AssetBundleManager.hydrate_published_generation"): ("hot_cache_snapshot_writer__mirror_only"),
    ("authoritative_serving_repair.py", "_write_repair_snapshot"): (
        "operations_repair_snapshot_writer__pg_owner_then_mirror"
    ),
    ("candidate_artifacts.py", "_sync_snapshot_artifact_view_to_hot_cache"): ("hot_cache_snapshot_writer__mirror_only"),
    ("company_asset_supplement.py", "_resolve_or_create_company_snapshot"): (
        "production_snapshot_writer__pg_owner_then_mirror"
    ),
    ("company_registry.py", "refresh_company_identity_registry"): (
        "cached_registry_writer_and_snapshot_scan__bridge_then_retire_authority"
    ),
    ("company_registry.py", "upsert_company_identity_registry_entry"): (
        "cached_registry_writer_with_snapshot_provenance__bridge_then_retire_authority"
    ),
    ("company_registry.py", "_latest_company_identity_payload"): (
        "registry_snapshot_scan_reader_bypass__bridge_then_retire_authority"
    ),
    (
        "orchestrator.py",
        "SourcingOrchestrator._open_public_serving_artifact_store_from_candidate_source",
    ): "production_semantic_reader_bypass__route_pg_retire_authority",
    ("orchestrator.py", "_restore_search_seed_snapshot_from_snapshot_dir"): (
        "production_restore_reader_bypass__route_pg_retire_authority"
    ),
    ("smoke_runtime_seed.py", "_write_candidate_documents"): "scripted_seed_fixture_writer",
    ("smoke_runtime_seed.py", "_seed_google_large_baseline_real_asset"): "scripted_seed_fixture_writer",
    ("snapshot_materializer.py", "resolve_snapshot_company_identity"): (
        "production_semantic_reader_bypass__route_pg_retire_authority"
    ),
    ("scripts/seed_test_env_assets.py", "_load_company_identity"): "fixture_reader_bypass",
    ("scripts/sync_latest_snapshot_from_registry.py", "_load_snapshot_identity"): (
        "operations_cli_reader_bypass__bridge_then_retire_authority"
    ),
}

EXPECTED_PYTHON_SNAPSHOT_WRITERS = {
    ("acquisition.py", "AcquisitionEngine._resolve_company"): "write_json",
    ("asset_sync.py", "AssetBundleManager.hydrate_published_generation"): "write_text",
    ("authoritative_serving_repair.py", "_write_repair_snapshot"): "_write_json",
    (
        "candidate_artifacts.py",
        "_sync_snapshot_artifact_view_to_hot_cache",
    ): "materialize_link_first_file",
    ("company_asset_supplement.py", "_resolve_or_create_company_snapshot"): "write_text",
    ("smoke_runtime_seed.py", "_write_candidate_documents"): "write_text",
    ("smoke_runtime_seed.py", "_seed_google_large_baseline_real_asset"): "write_text",
}

# These are the root fallback files that ``load_company_snapshot_identity``
# can treat as identity-bearing when ``identity.json`` is absent.  Each tuple
# freezes one currently known physical write call rather than treating the
# much larger population of unrelated nested artifact manifests as identity
# sources.  The final string is a normalized AST call fragment.
EXPECTED_ROOT_MANIFEST_IDENTITY_WRITE_CALLS = (
    (
        "acquisition.py",
        "AcquisitionEngine._normalize_snapshot",
        "logger.write_json(manifest_path,",
    ),
    (
        "company_asset_supplement.py",
        "CompanyAssetSupplementManager.merge_candidates_into_snapshot",
        "logger.write_json(snapshot_dir / 'manifest.json',",
    ),
    (
        "company_asset_supplement.py",
        "CompanyAssetSupplementManager.rebuild_linkedin_stage_1_snapshot",
        "logger.write_json(snapshot_dir / 'manifest.json',",
    ),
)

EXPECTED_ROOT_CANDIDATE_DOCUMENT_WRITE_CALLS = (
    (
        "acquisition.py",
        "AcquisitionEngine._ensure_anthropic_local_candidate_documents",
        "logger.write_json(candidate_doc_path,",
    ),
    (
        "acquisition.py",
        "AcquisitionEngine._materialize_local_reuse_candidate_documents",
        "logger.write_json(candidate_doc_path,",
    ),
    (
        "acquisition.py",
        "AcquisitionEngine._enrich_profiles._reuse_delta_baseline_if_available",
        "logger.write_json(candidate_doc_path,",
    ),
    (
        "acquisition.py",
        "AcquisitionEngine._enrich_profiles",
        "logger.write_json(candidate_doc_path,",
    ),
    (
        "acquisition.py",
        "AcquisitionEngine._enrich_profiles._write_candidate_documents",
        "logger.write_json(candidate_doc_path,",
    ),
    (
        "acquisition.py",
        "AcquisitionEngine._normalize_snapshot",
        "logger.write_json(candidate_doc_path,",
    ),
    (
        "authoritative_serving_repair.py",
        "_write_repair_snapshot",
        "_write_json(repair_snapshot_dir / 'candidate_documents.json',",
    ),
    (
        "company_asset_supplement.py",
        "CompanyAssetSupplementManager.merge_candidates_into_snapshot",
        "logger.write_json(candidate_doc_path,",
    ),
    (
        "company_asset_supplement.py",
        "CompanyAssetSupplementManager.rebuild_linkedin_stage_1_snapshot",
        "logger.write_json(snapshot_dir / 'candidate_documents.json',",
    ),
    (
        "orchestrator.py",
        "SourcingOrchestrator._apply_background_reconcile_snapshot_candidate_update",
        "candidate_doc_path.write_text(serialized_payload,",
    ),
    (
        "search_seed_registry.py",
        "project_search_seed_snapshot_to_candidate_documents",
        "AssetLogger(snapshot.snapshot_dir).write_json(candidate_doc_path,",
    ),
    (
        "snapshot_materializer.py",
        "SnapshotMaterializer.apply_company_roster_workers_to_snapshot",
        "logger.write_json(candidate_doc_path,",
    ),
    (
        "snapshot_materializer.py",
        "SnapshotMaterializer.apply_harvest_profile_workers_to_snapshot",
        "logger.write_json(candidate_doc_path,",
    ),
    (
        "smoke_runtime_seed.py",
        "_write_candidate_documents",
        "(snapshot_dir / 'candidate_documents.json').write_text(",
    ),
    (
        "smoke_runtime_seed.py",
        "_seed_google_large_baseline_real_asset",
        "(snapshot_dir / 'candidate_documents.json').write_text(",
    ),
    (
        "scripts/run_candidate_artifact_benchmark.py",
        "_write_runtime_snapshot",
        "(snapshot_dir / 'candidate_documents.json').write_text(",
    ),
)

# ``latest_snapshot.json.company_identity`` is the source of the caller
# fallback passed by eleven of the nineteen shared-loader calls.  These are
# the direct Python write sites that can currently originate that pointer;
# the scripted shell fixture and the generic bundle restore are frozen
# separately below.
EXPECTED_LATEST_POINTER_IDENTITY_WRITE_CALLS = (
    (
        "acquisition.py",
        "AcquisitionEngine._write_latest_snapshot_pointer_to_dir",
        "AssetLogger(company_dir).write_json(latest_pointer,",
        "production_mirror__pg_owner_required",
    ),
    (
        "artifact_cache.py",
        "_repair_hot_cache_company_dir",
        "latest_snapshot_path.write_text(",
        "production_mirror__pg_owner_required",
    ),
    (
        "asset_sync.py",
        "AssetBundleManager.hydrate_published_generation",
        "(hot_company_dir / 'latest_snapshot.json').write_text(",
        "production_mirror__pg_owner_required",
    ),
    (
        "authoritative_serving_repair.py",
        "_write_repair_snapshot",
        "_write_json(repair_snapshot_dir.parent / 'latest_snapshot.json',",
        "operations_mirror__pg_owner_required",
    ),
    (
        "candidate_artifacts.py",
        "_sync_snapshot_artifact_view_to_hot_cache",
        "AssetLogger(hot_company_dir).write_json(hot_company_dir / 'latest_snapshot.json',",
        "production_mirror__pg_owner_required",
    ),
    (
        "company_asset_supplement.py",
        "_resolve_or_create_company_snapshot",
        "(company_dir / 'latest_snapshot.json').write_text(",
        "production_mirror__pg_owner_required",
    ),
    (
        "scripts/sync_latest_snapshot_from_registry.py",
        "main",
        "latest_path.write_text(",
        "operations_migration_bridge__retire_authority",
    ),
    (
        "smoke_runtime_seed.py",
        "_write_candidate_documents",
        "(snapshot_dir.parent / 'latest_snapshot.json').write_text(",
        "scripted_fixture_only",
    ),
    (
        "smoke_runtime_seed.py",
        "_seed_google_large_baseline_real_asset",
        "(snapshot_dir.parent / 'latest_snapshot.json').write_text(",
        "scripted_fixture_only",
    ),
    (
        "scripts/run_candidate_artifact_benchmark.py",
        "_write_runtime_snapshot",
        "(company_root / 'latest_snapshot.json').write_text(",
        "benchmark_fixture_only",
    ),
    (
        "scripts/seed_test_env_assets.py",
        "main",
        "(target_company_dir / 'latest_snapshot.json').write_text(",
        "seed_fixture_only",
    ),
)

EXPECTED_SNAPSHOT_IDENTITY_FALLBACK_EXPRESSIONS = Counter(
    {
        ("artifact_cache.py", "_repair_hot_cache_company_dir", "{}"): 1,
        ("artifact_cache.py", "collect_hot_cache_inventory", "latest_payload"): 1,
        ("asset_paths.py", "resolve_company_snapshot_match_selection", "latest_payload"): 1,
        ("asset_paths.py", "build_company_snapshot_match_entry", "latest_payload"): 1,
        ("asset_reuse_planning.py", "_load_available_organization_asset_registry_records", "{}"): 1,
        ("asset_sync.py", "AssetBundleManager._build_candidate_generation_manifest", "latest_payload"): 1,
        ("authoritative_serving_repair.py", "_write_repair_snapshot", "baseline_payload"): 1,
        ("candidate_artifacts.py", "_export_compatibility_artifacts_from_serving_view", "{}"): 1,
        ("candidate_artifacts.py", "_iter_filtered_company_snapshot_groups", "latest_payload"): 1,
        ("candidate_artifacts.py", "_resolve_company_snapshot", "latest_payload"): 2,
        ("candidate_artifacts.py", "materialize_company_candidate_view", "<absent>"): 1,
        ("candidate_artifacts.py", "audit_candidate_artifact_hot_cache", "latest_payload"): 1,
        (
            "candidate_artifacts.py",
            "cleanup_candidate_artifact_hot_cache",
            "dict(snapshot_entry.get('latest_payload') or {})",
        ): 1,
        ("candidate_artifacts.py", "repair_missing_company_candidate_artifacts", "latest_payload"): 1,
        (
            "candidate_artifacts.py",
            "rewrite_structured_timeline_in_company_candidate_artifacts",
            "latest_payload",
        ): 1,
        ("candidate_artifacts.py", "_load_snapshot_provider_function_id_map", "{}"): 1,
        ("organization_assets.py", "discover_normalized_company_snapshots", "{}"): 1,
        ("runtime_rebuild.py", "rebuild_runtime_company_asset_control_plane", "{}"): 1,
    }
)


def _python_paths() -> tuple[Path, ...]:
    return tuple(sorted(SOURCE_ROOT.rglob("*.py"))) + tuple(sorted(SCRIPTS_ROOT.rglob("*.py")))


def _source_label(path: Path) -> str:
    if path.is_relative_to(SOURCE_ROOT):
        return path.relative_to(SOURCE_ROOT).as_posix()
    return path.relative_to(REPO_ROOT).as_posix()


def _parent_map(tree: ast.Module) -> dict[ast.AST, ast.AST]:
    return {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}


def _qualified_owner(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str:
    names: list[str] = []
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names.append(current.name)
    return ".".join(reversed(names))


def _function_qualname(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    parents: dict[ast.AST, ast.AST],
) -> str:
    prefix = _qualified_owner(node, parents)
    return f"{prefix}.{node.name}" if prefix else node.name


def _call_tail(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


@cache
def _target_call_point_inventory() -> dict[str, Counter[CallPoint]]:
    inventory = {symbol: Counter() for symbol in TARGET_CALL_SYMBOLS}
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parents = _parent_map(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            symbol = _call_tail(node.func)
            if symbol not in inventory:
                continue
            owner = _qualified_owner(node, parents)
            assert owner, f"{symbol} must stay inside a lexical function owner: {path}:{node.lineno}"
            inventory[symbol][(_source_label(path), owner)] += 1
    return inventory


def _call_points(symbol: str) -> Counter[CallPoint]:
    assert symbol in TARGET_CALL_SYMBOLS
    return Counter(_target_call_point_inventory()[symbol])


def _snapshot_identity_fallback_inventory() -> Counter[FallbackPoint]:
    inventory: Counter[FallbackPoint] = Counter()
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parents = _parent_map(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or _call_tail(node.func) != "load_company_snapshot_identity":
                continue
            owner = _qualified_owner(node, parents)
            assert owner, f"loader call must stay inside a lexical function owner: {path}:{node.lineno}"
            fallback = next((keyword.value for keyword in node.keywords if keyword.arg == "fallback_payload"), None)
            expression = ast.unparse(fallback) if fallback is not None else "<absent>"
            inventory[(_source_label(path), owner, expression)] += 1
    return inventory


def _registry_file_writer_inventory() -> frozenset[CallPoint]:
    writers: set[CallPoint] = set()
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parents = _parent_map(tree)
        for function in (node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))):
            registry_path_names: set[str] = set()
            for node in ast.walk(function):
                if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                    continue
                value = node.value
                if not isinstance(value, ast.Call) or _call_tail(value.func) != "company_identity_registry_path":
                    continue
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                registry_path_names.update(target.id for target in targets if isinstance(target, ast.Name))
            physically_writes_registry = any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "write_text"
                and (
                    (isinstance(node.func.value, ast.Name) and node.func.value.id in registry_path_names)
                    or (
                        isinstance(node.func.value, ast.Call)
                        and _call_tail(node.func.value.func) == "company_identity_registry_path"
                    )
                    or "company_identity_registry.json" in ast.unparse(node)
                )
                for node in ast.walk(function)
            )
            if physically_writes_registry:
                writers.add((_source_label(path), _function_qualname(function, parents)))
    return frozenset(writers)


def _identity_literal_function_inventory() -> frozenset[CallPoint]:
    inventory: set[CallPoint] = set()
    for path in _python_paths():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parents = _parent_map(tree)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Constant) and isinstance(node.value, str) and "identity.json" in node.value):
                continue
            owner = _qualified_owner(node, parents)
            if owner:
                inventory.add((_source_label(path), owner))
    return frozenset(inventory)


def _function_call_tails(path: Path, qualname: str) -> frozenset[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    parents = _parent_map(tree)
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _function_qualname(node, parents) == qualname
    ]
    assert len(matches) == 1, f"expected one {path}:{qualname}, found {len(matches)}"
    return frozenset(_call_tail(node.func) for node in ast.walk(matches[0]) if isinstance(node, ast.Call))


@cache
def _source_for_qualname(path: Path, qualname: str) -> str:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    parents = _parent_map(tree)
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _function_qualname(node, parents) == qualname
    ]
    assert len(matches) == 1, f"expected one {path}:{qualname}, found {len(matches)}"
    segment = ast.get_source_segment(source, matches[0])
    assert segment is not None
    return segment


def _path_for_label(label: str) -> Path:
    if label.startswith("scripts/"):
        return REPO_ROOT / label
    return SOURCE_ROOT / label


@cache
def _direct_call_sources(path: Path, qualname: str) -> tuple[str, ...]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    parents = _parent_map(tree)
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _function_qualname(node, parents) == qualname
    ]
    assert len(matches) == 1, f"expected one {path}:{qualname}, found {len(matches)}"
    return tuple(
        ast.unparse(node)
        for node in ast.walk(matches[0])
        if isinstance(node, ast.Call) and _qualified_owner(node, parents) == qualname
    )


def test_registry_file_writers_and_direct_callers_are_frozen_on_the_recognized_surface() -> None:
    assert _registry_file_writer_inventory() == EXPECTED_REGISTRY_WRITERS
    assert _call_points("refresh_company_identity_registry") == EXPECTED_REFRESH_CALLERS
    assert _call_points("upsert_company_identity_registry_entry") == EXPECTED_UPSERT_CALLERS


def test_registry_file_reader_chain_and_semantic_consumers_are_frozen() -> None:
    for symbol, expected_callers in EXPECTED_REGISTRY_READ_CHAIN_CALLERS.items():
        assert _call_points(symbol) == expected_callers


def test_seed_catalog_live_input_chain_is_frozen_and_classifiable() -> None:
    for symbol, expected_callers in EXPECTED_SEED_CATALOG_READ_CHAIN_CALLERS.items():
        assert _call_points(symbol) == expected_callers


def test_generic_snapshot_materializers_and_their_entrypoints_are_frozen() -> None:
    for symbol, expected_callers in EXPECTED_GENERIC_SNAPSHOT_MATERIALIZER_CALLERS.items():
        assert _call_points(symbol) == expected_callers

    restore_source = _source_for_qualname(SOURCE_ROOT / "asset_sync.py", "AssetBundleManager.restore_bundle")
    assert 'destination = runtime_dir / record["runtime_relative_path"]' in restore_source
    assert "shutil.copy2(source, destination)" in restore_source

    mirror_source = _source_for_qualname(SOURCE_ROOT / "artifact_cache.py", "mirror_tree_link_first")
    assert 'resolved_source.rglob("*")' in mirror_source
    assert "materialize_link_first_file(file_path, resolved_destination / relative_path)" in mirror_source

    fixture_materializer_source = _source_for_qualname(
        SCRIPTS_ROOT / "seed_test_env_assets.py", "_materialize_snapshot_link"
    )
    assert "shutil.copytree(source_snapshot_dir, target_snapshot_dir)" in fixture_materializer_source
    assert (
        "target_snapshot_dir.symlink_to(source_snapshot_dir.resolve(), target_is_directory=True)"
        in fixture_materializer_source
    )
    fixture_main_calls = _direct_call_sources(SCRIPTS_ROOT / "seed_test_env_assets.py", "main")
    assert sum("_materialize_snapshot_link(" in call for call in fixture_main_calls) == 1


def test_loader_recognized_manifest_and_candidate_document_writers_are_frozen() -> None:
    for label, qualname, call_fragment in EXPECTED_ROOT_MANIFEST_IDENTITY_WRITE_CALLS:
        calls = _direct_call_sources(_path_for_label(label), qualname)
        assert sum(call_fragment in call for call in calls) == 1, (label, qualname, call_fragment, calls)
        function_source = _source_for_qualname(_path_for_label(label), qualname)
        assert "company_identity" in function_source

    for label, qualname, call_fragment in EXPECTED_ROOT_CANDIDATE_DOCUMENT_WRITE_CALLS:
        calls = _direct_call_sources(_path_for_label(label), qualname)
        assert sum(call_fragment in call for call in calls) == 1, (label, qualname, call_fragment, calls)

    assert len(EXPECTED_ROOT_MANIFEST_IDENTITY_WRITE_CALLS) == 3
    assert len(EXPECTED_ROOT_CANDIDATE_DOCUMENT_WRITE_CALLS[:13]) == 13
    assert len(EXPECTED_ROOT_CANDIDATE_DOCUMENT_WRITE_CALLS[13:]) == 3


def test_snapshot_and_fixture_writer_sinks_are_explicit() -> None:
    for (label, owner), expected_sink in EXPECTED_PYTHON_SNAPSHOT_WRITERS.items():
        assert expected_sink in _function_call_tails(_path_for_label(label), owner)

    scripted_source = SCRIPTED_DELTA_PATH.read_text(encoding="utf-8")
    assert scripted_source.count('(snapshot_dir / "identity.json").write_text(') == 1
    assert scripted_source.count('"company_key": "lovable"') == 1


def test_central_snapshot_reader_is_19_calls_across_8_files() -> None:
    calls = _call_points("load_company_snapshot_identity")
    assert calls == EXPECTED_CENTRAL_READER_CALLS
    assert sum(calls.values()) == 19
    assert len({path for path, _owner in calls}) == 8


def test_shared_loader_fallback_provenance_and_latest_pointer_writers_are_frozen() -> None:
    inventory = _snapshot_identity_fallback_inventory()
    assert inventory == EXPECTED_SNAPSHOT_IDENTITY_FALLBACK_EXPRESSIONS
    assert sum(inventory.values()) == 19
    assert sum(count for (*_point, expression), count in inventory.items() if expression == "<absent>") == 1
    assert sum(count for (*_point, expression), count in inventory.items() if expression == "{}") == 6
    assert (
        sum(
            count
            for (*_point, expression), count in inventory.items()
            if expression == "latest_payload" or "latest_payload" in expression
        )
        == 11
    )
    assert sum(count for (*_point, expression), count in inventory.items() if expression == "baseline_payload") == 1

    classifications: Counter[str] = Counter()
    for label, qualname, call_fragment, classification in EXPECTED_LATEST_POINTER_IDENTITY_WRITE_CALLS:
        calls = _direct_call_sources(_path_for_label(label), qualname)
        assert sum(call_fragment in call for call in calls) == 1, (label, qualname, call_fragment, calls)
        classifications[classification] += 1
    assert len(EXPECTED_LATEST_POINTER_IDENTITY_WRITE_CALLS) == 11
    assert sum(classifications.values()) == 11
    assert sum("fixture_only" in classification for classification in classifications.elements()) == 4
    assert sum("pg_owner_required" in classification for classification in classifications.elements()) == 6
    assert classifications["operations_migration_bridge__retire_authority"] == 1

    scripted_source = SCRIPTED_DELTA_PATH.read_text(encoding="utf-8")
    assert scripted_source.count('(company_dir / "latest_snapshot.json").write_text(') == 1
    assert scripted_source.count('json.dumps({"snapshot_id": snapshot_dir.name, "company_identity": identity}') == 1


def test_every_identity_literal_owner_has_an_explicit_current_classification() -> None:
    discovered = _identity_literal_function_inventory()
    assert discovered == frozenset(EXPECTED_IDENTITY_LITERAL_CLASSIFICATION)

    classifications = Counter(EXPECTED_IDENTITY_LITERAL_CLASSIFICATION.values())
    assert classifications["hot_cache_snapshot_writer__mirror_only"] == 2
    assert classifications["scripted_seed_fixture_writer"] == 2
    assert sum(value.endswith("__pg_owner_then_mirror") for value in classifications.elements()) == 3
    assert sum(value.endswith("__route_pg_retire_authority") for value in classifications.elements()) == 4
    assert sum(value.endswith("__bridge_then_retire_authority") for value in classifications.elements()) == 5
    assert not any("unknown" in value for value in classifications)


def test_resolver_calls_do_not_hide_the_manual_override_prepass() -> None:
    resolver_calls = _call_points("resolve_company_identity")
    resolver_by_file = Counter(point[0] for point in resolver_calls.elements())
    assert resolver_by_file == EXPECTED_RESOLVER_CALLS_BY_FILE
    assert sum(resolver_by_file.values()) == 17
    assert len(resolver_by_file) == 6

    manual_calls = _call_points("resolve_manual_company_identity")
    manual_by_file = Counter(point[0] for point in manual_calls.elements())
    assert manual_by_file == EXPECTED_MANUAL_RESOLVER_CALLS_BY_FILE
    assert sum(manual_by_file.values()) == 2


def test_characterization_record_keeps_pg_authority_and_file_dispositions_explicit() -> None:
    lines = CHARACTERIZATION_DOC_PATH.read_text(encoding="utf-8").splitlines()
    assert any("Status:" in line for line in lines[:8])
    text = "\n".join(lines)
    for required in (
        "2 physical writers / 3 refresh callers / 1 upsert caller",
        "5 production/operations snapshot writers",
        "3 scripted/seed fixture writers",
        "cached-registry read chain",
        "seed-catalog live-input chain",
        "3** root `manifest.json.company_identity`",
        "13+3",
        "19 calls / 8 files",
        "6 empty payloads + 11 `latest_snapshot.json.company_identity` payloads + 1 baseline",
        "7 production/operations + 5 fixture/scripted direct write sites",
        "generic whole-snapshot materializers",
        "17 calls / 6 files",
        "future current-state PG read model is authoritative",
        "migration bridge then retire as authority",
        "mirror-only",
        "fixture-only",
        "eighteen caller-fallback paths",
    ):
        assert required in text
