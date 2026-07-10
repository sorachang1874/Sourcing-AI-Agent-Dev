import ast
import json
from pathlib import Path

import pytest

from sourcing_agent.repositories.manual_review import ManualReviewRepository
from sourcing_agent.repositories.serving_projection import ServingProjectionRepository

_RETIRED_MANUAL_REVIEW_STORE_METHODS = {
    "replace_manual_review_items",
    "list_manual_review_items",
    "count_manual_review_items",
    "cleanup_manual_review_items",
    "review_manual_review_item",
    "get_manual_review_item",
    "merge_manual_review_item_metadata",
}
_MANUAL_REVIEW_REPOSITORY_METHODS = {
    "replace_items",
    "list_items",
    "count_items",
    "cleanup_items",
    "review_item",
    "get_item",
    "merge_item_metadata",
}
_RETIRED_SERVING_PROJECTION_CATALOG_STORE_METHODS = {
    "upsert_serving_projection",
    "get_serving_projection",
    "list_serving_projections",
    "upsert_run_projection_link",
    "get_run_projection_link",
    "list_run_projection_links",
    "upsert_collection_authoritative_pointer",
    "get_collection_authoritative_pointer",
    "list_collection_authoritative_pointers",
    "_serving_projection_from_row",
    "_run_projection_link_from_row",
    "_collection_authoritative_pointer_from_row",
}
_SERVING_PROJECTION_CATALOG_REPOSITORY_METHODS = {
    "upsert",
    "get",
    "list",
    "upsert_run_link",
    "get_run_link",
    "list_run_links",
    "upsert_authoritative_pointer",
    "get_authoritative_pointer",
    "list_authoritative_pointers",
    "_projection_from_row",
    "_run_link_from_row",
    "_authoritative_pointer_from_row",
}


def _class_method_names(path: Path, class_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    class_node = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
    return {node.name for node in class_node.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def test_production_code_does_not_reintroduce_sqlitestore_facade_name() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            if path.name == "__pycache__":
                continue
            if "SQLiteStore" in path.read_text(encoding="utf-8"):
                offenders.append(str(path.relative_to(repo_root)))

    assert offenders == []


def test_production_code_uses_storage_neutral_linkedin_url_normalization() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if ".normalize_linkedin_profile_url(" in text:
                offenders.append(str(path.relative_to(repo_root)))

    assert offenders == []


def test_production_code_does_not_reintroduce_retired_sqlite_snapshot_entrypoints() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    retired_tokens = {
        "export-sqlite-snapshot",
        "restore-sqlite-snapshot",
        "--confirm-legacy-sqlite",
        "--allow-sqlite-snapshot-restore",
        "--with-sqlite-backup",
        "--without-sqlite",
        "SOURCING_ENABLE_SQLITE_PROFILE_REGISTRY_FALLBACK",
        "--source-db-path",
        "export_sqlite_snapshot",
        "restore_sqlite_snapshot",
    }
    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if any(token in text for token in retired_tokens):
                offenders.append(str(path.relative_to(repo_root)))

    assert offenders == []


def test_manual_review_storage_facade_is_retired_to_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    storage_methods = _class_method_names(repo_root / "src" / "sourcing_agent" / "storage.py", "ControlPlaneStore")
    repository_methods = _class_method_names(
        repo_root / "src" / "sourcing_agent" / "repositories" / "manual_review.py",
        "ManualReviewRepository",
    )
    namespace_source = (repo_root / "src" / "sourcing_agent" / "repositories" / "__init__.py").read_text(
        encoding="utf-8"
    )

    assert storage_methods.isdisjoint(_RETIRED_MANUAL_REVIEW_STORE_METHODS)
    assert _MANUAL_REVIEW_REPOSITORY_METHODS <= repository_methods
    assert "self.manual_review = ManualReviewRepository(adapter)" in namespace_source


def test_manual_review_repository_mapper_preserves_nullable_and_json_error_contract() -> None:
    repository = ManualReviewRepository(object())
    row = {
        "review_item_id": 7,
        "job_id": None,
        "candidate_id": "candidate-1",
        "target_company": None,
        "review_type": "manual_identity_resolution",
        "priority": "high",
        "status": "open",
        "summary": None,
        "candidate_json": '{"name":"Test"}',
        "evidence_json": "[]",
        "metadata_json": '{"source":"manual"}',
        "reviewed_by": None,
        "review_notes": None,
        "reviewed_at": None,
        "created_at": "2026-07-10 00:00:00",
        "updated_at": "2026-07-10 00:00:00",
    }

    mapped = repository._item_from_row(row)
    assert mapped["job_id"] is None
    assert mapped["target_company"] is None
    assert mapped["summary"] is None
    assert mapped["metadata"] == {"source": "manual"}

    with pytest.raises(json.JSONDecodeError):
        repository._item_from_row({**row, "metadata_json": "{"})


def test_serving_projection_catalog_storage_facade_is_retired_to_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    storage_methods = _class_method_names(repo_root / "src" / "sourcing_agent" / "storage.py", "ControlPlaneStore")
    repository_methods = _class_method_names(
        repo_root / "src" / "sourcing_agent" / "repositories" / "serving_projection.py",
        "ServingProjectionRepository",
    )
    namespace_source = (repo_root / "src" / "sourcing_agent" / "repositories" / "__init__.py").read_text(
        encoding="utf-8"
    )

    assert storage_methods.isdisjoint(_RETIRED_SERVING_PROJECTION_CATALOG_STORE_METHODS)
    assert _SERVING_PROJECTION_CATALOG_REPOSITORY_METHODS <= repository_methods
    assert "self.serving_projection = ServingProjectionRepository(adapter)" in namespace_source


def test_serving_projection_catalog_repository_mappers_preserve_descriptor_contract() -> None:
    repository = ServingProjectionRepository(object())
    projection_row = {
        "projection_id": "proj-guard",
        "projection_type": "run_scope_projection",
        "projection_version": None,
        "counts_json": '{"candidate_count":2}',
        "readiness_json": "not-json",
    }
    run_link_row = {
        "run_id": "run-guard",
        "projection_id": "proj-guard",
        "link_type": "result",
        "metadata_json": '{"source":"guard"}',
    }
    pointer_row = {
        "collection_id": "company:guard",
        "active_projection_id": "proj-guard",
        "metadata_json": "[]",
    }

    projection = repository._projection_from_row(projection_row)
    assert projection["projection_version"] == "serving_projection_v1"
    assert projection["counts"] == {"candidate_count": 2}
    assert projection["readiness"] == {}
    assert repository._run_link_from_row(run_link_row)["metadata"] == {"source": "guard"}
    assert repository._authoritative_pointer_from_row(pointer_row)["metadata"] == {}
