import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from sourcing_agent.repositories import serving_projection_repo
from sourcing_agent.repositories.manual_review import ManualReviewRepository
from sourcing_agent.repositories.serving_projection import ServingProjectionRepository
from sourcing_agent.storage import ControlPlaneStore

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
_RETIRED_SERVING_PROJECTION_STORE_METHODS = {
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
    "upsert_projection_manifest_shard",
    "get_projection_manifest_shard",
    "list_projection_manifest_shards",
    "_projection_manifest_shard_from_row",
    "upsert_serving_projection_members",
    "replace_serving_projection_members",
    "list_serving_projection_members",
    "list_serving_projection_members_by_identity_keys",
    "list_serving_projection_members_by_person_identity",
    "count_serving_projection_members_by_readiness",
    "get_serving_projection_member",
    "count_serving_projection_members",
    "_serving_projection_member_from_row",
    "_serving_projection_member_row_payload",
}
_RETIRED_SERVING_PROJECTION_CALL_ATTRIBUTES = {
    name for name in _RETIRED_SERVING_PROJECTION_STORE_METHODS if not name.startswith("_")
}
_RETIRED_SERVING_PROJECTION_NATIVE_DISPATCH_KEYS = {
    "upsert_serving_projection",
    "upsert_run_projection_link",
    "upsert_collection_authoritative_pointer",
    "upsert_projection_manifest_shard",
    "upsert_serving_projection_members",
    "replace_serving_projection_members",
}
_SERVING_PROJECTION_REPOSITORY_METHODS = {
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
    "upsert_manifest_shard",
    "get_manifest_shard",
    "list_manifest_shards",
    "_manifest_shard_from_row",
    "upsert_members",
    "replace_members",
    "list_members",
    "list_members_by_identity_keys",
    "list_members_by_person_identity",
    "count_members_by_readiness",
    "get_member",
    "count_members",
    "_member_from_row",
    "_member_row_payload",
}


def _class_method_names(path: Path, class_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    class_node = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
    return {node.name for node in class_node.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _assigned_literal_dict_keys(path: Path, assignment_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(isinstance(target, ast.Name) and target.id == assignment_name for target in targets):
            continue
        value = node.value
        if not isinstance(value, ast.Dict):
            return set()
        return {key.value for key in value.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)}
    raise AssertionError(f"assignment not found: {assignment_name}")


def _is_store_receiver(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "store" or node.id.endswith("_store")
    return isinstance(node, ast.Attribute) and node.attr in {"store", "_store"}


def _retired_serving_projection_references(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in _RETIRED_SERVING_PROJECTION_CALL_ATTRIBUTES
            and _is_store_receiver(node.value)
        ):
            offenders.append((node.lineno, node.attr))
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and _is_store_receiver(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _RETIRED_SERVING_PROJECTION_CALL_ATTRIBUTES
        ):
            offenders.append((node.lineno, f"getattr:{node.args[1].value}"))
    return offenders


def _invalid_bulk_upsert_wrapper_calls(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    wrapper_names = {"_call_control_plane_postgres_native", "_call_native_write"}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in wrapper_names or not node.args:
            continue
        method_arg = node.args[0]
        if not isinstance(method_arg, ast.Constant) or method_arg.value != "bulk_upsert_rows":
            continue
        keyword_names = {keyword.arg for keyword in node.keywords if keyword.arg is not None}
        if len(node.args) != 1 or not {"table_name", "rows"} <= keyword_names:
            offenders.append((node.lineno, node.func.attr))
    return offenders


class _CatalogFaultAdapter:
    def __init__(self, *, authoritative: bool, failing_method: str, prefer_read: bool = True) -> None:
        self.authoritative = authoritative
        self.failing_method = failing_method
        self.prefer_read = prefer_read

    def should_prefer_read(self, table_name: str) -> bool:
        return self.prefer_read

    def is_authoritative(self, table_name: str) -> bool:
        return self.authoritative

    def _raise_if_failing(self, method_name: str) -> None:
        if self.failing_method == method_name:
            raise RuntimeError(f"{method_name}-boom")

    def select_one(self, table_name: str, **kwargs: object) -> None:
        self._raise_if_failing("select_one")
        return None

    def select_many(self, table_name: str, **kwargs: object) -> list[dict[str, object]]:
        self._raise_if_failing("select_many")
        return []

    def upsert_row(self, table_name: str, row: dict[str, object]) -> None:
        self._raise_if_failing("upsert_row")

    def bulk_upsert_rows(
        self,
        *,
        table_name: str,
        rows: list[dict[str, object]],
        **kwargs: object,
    ) -> int:
        self._raise_if_failing("bulk_upsert_rows")
        return len(rows)

    def delete_rows(self, *, table_name: str, **kwargs: object) -> int:
        self._raise_if_failing("delete_rows")
        return 1

    def replace_rows(self, *, table_name: str, rows: list[dict[str, object]], **kwargs: object) -> int:
        self._raise_if_failing("replace_rows")
        return len(rows)

    def upsert_row_and_replace_rows(
        self,
        *,
        table_name: str,
        row: dict[str, object],
        replace_rows: list[dict[str, object]],
        **kwargs: object,
    ) -> dict[str, int]:
        self._raise_if_failing("upsert_row_and_replace_rows")
        return {"upserted_count": 1, "replaced_count": len(replace_rows)}

    def upsert_row_and_upsert_rows(
        self,
        *,
        table_name: str,
        row: dict[str, object],
        upsert_rows: list[dict[str, object]],
        **kwargs: object,
    ) -> dict[str, int]:
        self._raise_if_failing("upsert_row_and_upsert_rows")
        return {"upserted_count": 1, "child_upserted_count": len(upsert_rows)}

    def count_rows(self, table_name: str, **kwargs: object) -> int:
        self._raise_if_failing("count_rows")
        return 0


class _BulkUpsertRecordingAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[dict[str, object]]]] = []

    def should_prefer_read(self, _table_name: str) -> bool:
        return True

    def is_authoritative(self, _table_name: str) -> bool:
        return True

    def bulk_upsert_rows(
        self,
        *,
        table_name: str,
        rows: list[dict[str, object]],
        **kwargs: object,
    ) -> int:
        self.calls.append((table_name, rows))
        return len(rows)


def _runtime_error(callable_) -> RuntimeError:
    with pytest.raises(RuntimeError) as raised:
        callable_()
    return raised.value


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

    assert storage_methods.isdisjoint(_RETIRED_SERVING_PROJECTION_STORE_METHODS)
    assert _SERVING_PROJECTION_REPOSITORY_METHODS <= repository_methods
    assert "self.serving_projection = ServingProjectionRepository(adapter)" in namespace_source


def test_serving_projection_duck_accessor_never_falls_back_to_retired_store_facade() -> None:
    repository = object()
    canonical_store = SimpleNamespace(repos=SimpleNamespace(serving_projection=repository))
    legacy_only_store = SimpleNamespace(get_serving_projection_member=lambda *_args: {})

    assert serving_projection_repo(canonical_store) is repository
    assert serving_projection_repo(legacy_only_store) is None


def test_serving_projection_retired_calls_and_native_dispatch_keys_cannot_return() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    synthetic = ast.parse(
        "\n".join(
            [
                'store.get_serving_projection("proj")',
                "callback = store.list_collection_authoritative_pointers",
                'dynamic = getattr(self.store, "upsert_run_projection_link")',
                'orchestrator.get_run_projection_link("run")',
            ]
        )
    )
    assert {label for _line, label in _retired_serving_projection_references(synthetic)} == {
        "get_serving_projection",
        "list_collection_authoritative_pointers",
        "getattr:upsert_run_projection_link",
    }

    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders.extend(
                f"{path.relative_to(repo_root)}:{line}:{label}"
                for line, label in _retired_serving_projection_references(tree)
            )

    native_dispatch_keys = _assigned_literal_dict_keys(
        repo_root / "src" / "sourcing_agent" / "storage.py",
        "_CONTROL_PLANE_POSTGRES_NATIVE_TABLES",
    )
    assert offenders == []
    assert native_dispatch_keys.isdisjoint(_RETIRED_SERVING_PROJECTION_NATIVE_DISPATCH_KEYS)


def test_bulk_upsert_wrapper_calls_require_explicit_table_and_rows_keywords() -> None:
    valid_source = (
        'self._call_control_plane_postgres_native("bulk_upsert_rows", table_name="members", rows=payload_rows)'
    )
    assert _invalid_bulk_upsert_wrapper_calls(ast.parse(valid_source)) == []

    mutated_source = valid_source.replace('table_name="members"', '"members"')
    assert _invalid_bulk_upsert_wrapper_calls(ast.parse(mutated_source)) == [(1, "_call_control_plane_postgres_native")]

    synthetic = ast.parse(
        "\n".join(
            [
                'self._call_control_plane_postgres_native("bulk_upsert_rows", "members", payload_rows)',
                'self._call_native_write("bulk_upsert_rows", table_name="members")',
                'self._call_native_write("bulk_upsert_rows", rows=payload_rows)',
                valid_source,
            ]
        )
    )
    assert _invalid_bulk_upsert_wrapper_calls(synthetic) == [
        (1, "_call_control_plane_postgres_native"),
        (2, "_call_native_write"),
        (3, "_call_native_write"),
    ]

    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders.extend(
                f"{path.relative_to(repo_root)}:{line}:{wrapper}"
                for line, wrapper in _invalid_bulk_upsert_wrapper_calls(tree)
            )

    assert offenders == []


@pytest.mark.parametrize(
    "invoke",
    [
        lambda store: store._call_control_plane_postgres_native("bulk_upsert_rows", "members", []),
        lambda store: (lambda call: call("bulk_upsert_rows", "members", []))(store._call_control_plane_postgres_native),
        lambda store: getattr(store, "_call_control_plane_postgres_native")("bulk_upsert_rows", "members", []),
        lambda store: ControlPlaneStore._call_control_plane_postgres_native(store, "bulk_upsert_rows", "members", []),
        lambda store: store._call_control_plane_postgres_native("".join(("bulk_upsert", "_rows")), "members", []),
    ],
    ids=["direct-positional", "alias", "getattr", "unbound", "computed-method-name"],
)
def test_bulk_upsert_runtime_contract_rejects_positional_ast_bypasses(invoke) -> None:
    adapter = _BulkUpsertRecordingAdapter()
    store = object.__new__(ControlPlaneStore)
    store._control_plane_postgres = adapter

    with pytest.raises(TypeError, match="positional payload arguments are not allowed"):
        invoke(store)

    assert adapter.calls == []


@pytest.mark.parametrize(
    ("kwargs", "error_type", "message"),
    [
        ({"rows": []}, TypeError, "explicit table_name keyword argument"),
        ({"table_name": "", "rows": []}, ValueError, "non-empty table_name"),
        ({"table_name": "   ", "rows": []}, ValueError, "non-empty table_name"),
        ({"table_name": "members"}, TypeError, "explicit rows keyword argument"),
    ],
    ids=["missing-table", "empty-table", "blank-table", "missing-rows"],
)
def test_bulk_upsert_runtime_contract_requires_table_and_explicit_rows(
    kwargs: dict[str, object],
    error_type: type[Exception],
    message: str,
) -> None:
    adapter = _BulkUpsertRecordingAdapter()
    store = object.__new__(ControlPlaneStore)
    store._control_plane_postgres = adapter

    with pytest.raises(error_type, match=message):
        store._call_control_plane_postgres_native("bulk_upsert_rows", **kwargs)

    assert adapter.calls == []


def test_bulk_upsert_runtime_contract_allows_explicit_empty_rows_across_write_boundaries() -> None:
    adapter = _BulkUpsertRecordingAdapter()
    store = object.__new__(ControlPlaneStore)
    store._control_plane_postgres = adapter
    repository = ServingProjectionRepository(adapter)

    assert (
        store._call_control_plane_postgres_native(
            "bulk_upsert_rows",
            table_name="store_members",
            rows=[],
        )
        == 0
    )
    assert (
        repository._call_native_write(
            "bulk_upsert_rows",
            table_name="repository_members",
            rows=[],
        )
        == 0
    )
    assert adapter.calls == [("store_members", []), ("repository_members", [])]


@pytest.mark.parametrize(
    ("table_name", "include_rows", "error_type", "message"),
    [
        ("", True, ValueError, "non-empty table_name"),
        ("repository_members", False, TypeError, "explicit rows keyword argument"),
    ],
    ids=["repository-empty-table", "repository-missing-rows"],
)
def test_repository_bulk_upsert_runtime_contract_fails_closed(
    table_name: str,
    include_rows: bool,
    error_type: type[Exception],
    message: str,
) -> None:
    adapter = _BulkUpsertRecordingAdapter()
    repository = ServingProjectionRepository(adapter)
    kwargs: dict[str, object] = {"rows": []} if include_rows else {}

    with pytest.raises(error_type, match=message):
        repository._call_native_write("bulk_upsert_rows", table_name=table_name, **kwargs)

    assert adapter.calls == []


def test_serving_projection_catalog_repository_preserves_all_tier_a_b_fault_contracts() -> None:
    read_cases = [
        ("serving_projections", "select_one", lambda repo: repo.get("proj"), {}),
        ("serving_projections", "select_many", lambda repo: repo.list(), []),
        ("run_projection_links", "select_one", lambda repo: repo.get_run_link("run"), {}),
        ("run_projection_links", "select_many", lambda repo: repo.list_run_links("run"), []),
        (
            "collection_authoritative_pointers",
            "select_one",
            lambda repo: repo.get_authoritative_pointer("company:test"),
            {},
        ),
        (
            "collection_authoritative_pointers",
            "select_many",
            lambda repo: repo.list_authoritative_pointers(),
            [],
        ),
    ]
    write_cases = [
        (
            "serving_projections",
            "upsert_serving_projection",
            lambda repo: repo.upsert({"projection_id": "proj"}),
        ),
        (
            "run_projection_links",
            "upsert_run_projection_link",
            lambda repo: repo.upsert_run_link({"run_id": "run", "projection_id": "proj"}),
        ),
        (
            "collection_authoritative_pointers",
            "upsert_collection_authoritative_pointer",
            lambda repo: repo.upsert_authoritative_pointer(
                {"collection_id": "company:test", "active_projection_id": "proj"}
            ),
        ),
    ]
    checks = 0
    for table_name, primitive, call, sentinel in read_cases:
        strict_repo = ServingProjectionRepository(_CatalogFaultAdapter(authoritative=True, failing_method=primitive))
        strict_error = _runtime_error(lambda: call(strict_repo))
        assert str(strict_error) == (
            f"Postgres authoritative read failed for {table_name} via {primitive}: RuntimeError: {primitive}-boom"
        )
        assert isinstance(strict_error.__cause__, RuntimeError)
        checks += 1

        non_authoritative_repo = ServingProjectionRepository(
            _CatalogFaultAdapter(authoritative=False, failing_method=primitive)
        )
        assert call(non_authoritative_repo) == sentinel
        checks += 1

    for table_name, old_method_name, call in write_cases:
        strict_repo = ServingProjectionRepository(_CatalogFaultAdapter(authoritative=True, failing_method="upsert_row"))
        strict_error = _runtime_error(lambda: call(strict_repo))
        assert str(strict_error) == (
            f"Postgres authoritative write failed for {table_name} via upsert_row: RuntimeError: upsert_row-boom"
        )
        assert isinstance(strict_error.__cause__, RuntimeError)
        checks += 1

        non_authoritative_repo = ServingProjectionRepository(
            _CatalogFaultAdapter(authoritative=False, failing_method="upsert_row")
        )
        no_confirmation = _runtime_error(lambda: call(non_authoritative_repo))
        assert str(no_confirmation) == (
            f"Postgres authoritative write failed for {table_name} via {old_method_name}: "
            "postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)"
        )
        assert no_confirmation.__cause__ is None
        checks += 1

    assert checks == 18


def test_serving_projection_manifest_repository_preserves_all_tier_a_b_fault_contracts() -> None:
    read_cases = [
        ("select_one", lambda repo: repo.get_manifest_shard("manifest:1"), {}),
        ("select_many", lambda repo: repo.list_manifest_shards("proj"), []),
    ]
    checks = 0
    for primitive, call, sentinel in read_cases:
        strict_repo = ServingProjectionRepository(_CatalogFaultAdapter(authoritative=True, failing_method=primitive))
        strict_error = _runtime_error(lambda: call(strict_repo))
        assert str(strict_error) == (
            "Postgres authoritative read failed for projection_manifest_shards "
            f"via {primitive}: RuntimeError: {primitive}-boom"
        )
        assert isinstance(strict_error.__cause__, RuntimeError)
        checks += 1

        non_authoritative_repo = ServingProjectionRepository(
            _CatalogFaultAdapter(authoritative=False, failing_method=primitive)
        )
        assert call(non_authoritative_repo) == sentinel
        checks += 1

    payload = {"projection_id": "proj", "shard_id": "manifest:1"}
    strict_write_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="upsert_row")
    )
    strict_write_error = _runtime_error(lambda: strict_write_repo.upsert_manifest_shard(payload))
    assert str(strict_write_error) == (
        "Postgres authoritative write failed for projection_manifest_shards via upsert_row: "
        "RuntimeError: upsert_row-boom"
    )
    assert isinstance(strict_write_error.__cause__, RuntimeError)
    checks += 1

    non_authoritative_write_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=False, failing_method="upsert_row")
    )
    no_confirmation = _runtime_error(lambda: non_authoritative_write_repo.upsert_manifest_shard(payload))
    assert str(no_confirmation) == (
        "Postgres authoritative write failed for projection_manifest_shards via upsert_projection_manifest_shard: "
        "postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)"
    )
    assert no_confirmation.__cause__ is None
    checks += 1

    assert checks == 6


def test_serving_projection_member_repository_preserves_tier_a_b_and_count_sentinels() -> None:
    read_cases = [
        ("select_many", lambda repo: repo.list_members("proj"), []),
        ("select_many", lambda repo: repo.list_members_by_identity_keys("proj", ["candidate:1"]), []),
        ("select_many", lambda repo: repo.list_members_by_person_identity("person:1"), []),
        ("select_one", lambda repo: repo.get_member("proj", "candidate:1"), {}),
    ]
    checks = 0
    for primitive, call, sentinel in read_cases:
        strict_repo = ServingProjectionRepository(_CatalogFaultAdapter(authoritative=True, failing_method=primitive))
        strict_error = _runtime_error(lambda: call(strict_repo))
        assert str(strict_error) == (
            "Postgres authoritative read failed for serving_projection_members "
            f"via {primitive}: RuntimeError: {primitive}-boom"
        )
        assert isinstance(strict_error.__cause__, RuntimeError)
        checks += 1

        non_authoritative_repo = ServingProjectionRepository(
            _CatalogFaultAdapter(authoritative=False, failing_method=primitive)
        )
        assert call(non_authoritative_repo) == sentinel
        checks += 1

    strict_bulk_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="bulk_upsert_rows")
    )
    bulk_error = _runtime_error(
        lambda: strict_bulk_repo.upsert_members("proj", [{"candidate_identity_key": "candidate:1"}])
    )
    assert str(bulk_error) == (
        "Postgres authoritative write failed for serving_projection_members via bulk_upsert_rows: "
        "RuntimeError: bulk_upsert_rows-boom"
    )
    assert isinstance(bulk_error.__cause__, RuntimeError)
    checks += 1

    non_authoritative_bulk_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=False, failing_method="bulk_upsert_rows")
    )
    assert non_authoritative_bulk_repo.upsert_members("proj", [{"candidate_identity_key": "candidate:1"}]) == 0
    checks += 1

    strict_readiness_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="select_many")
    )
    readiness_error = _runtime_error(lambda: strict_readiness_repo.count_members_by_readiness("proj"))
    assert str(readiness_error) == (
        "Postgres authoritative read failed for serving_projection_members via select_many: "
        "RuntimeError: select_many-boom"
    )
    assert isinstance(readiness_error.__cause__, RuntimeError)
    checks += 1
    non_authoritative_readiness_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=False, failing_method="select_many")
    )
    assert non_authoritative_readiness_repo.count_members_by_readiness("proj") == {}
    checks += 1

    strict_count_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="count_rows")
    )
    count_error = _runtime_error(lambda: strict_count_repo.count_members("proj"))
    assert str(count_error) == (
        "Postgres authoritative read failed for serving_projection_members via count_rows: "
        "RuntimeError: count_rows-boom"
    )
    assert isinstance(count_error.__cause__, RuntimeError)
    checks += 1
    non_authoritative_count_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=False, failing_method="count_rows")
    )
    assert non_authoritative_count_repo.count_members("proj") == 0
    checks += 1

    strict_replace_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="replace_rows")
    )
    replace_error = _runtime_error(
        lambda: strict_replace_repo.replace_members("proj", [{"candidate_identity_key": "candidate:1"}])
    )
    assert str(replace_error) == (
        "Postgres authoritative write failed for serving_projection_members via replace_rows: "
        "RuntimeError: replace_rows-boom"
    )
    assert isinstance(replace_error.__cause__, RuntimeError)
    checks += 1
    non_authoritative_replace_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=False, failing_method="replace_rows")
    )
    assert non_authoritative_replace_repo.replace_members("proj", [{"candidate_identity_key": "candidate:1"}]) == 0
    checks += 1

    strict_publication_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="upsert_row_and_replace_rows")
    )
    publication_error = _runtime_error(
        lambda: strict_publication_repo.upsert_with_replaced_members(
            {"projection_id": "proj", "state": "serving"},
            [{"candidate_identity_key": "candidate:1"}],
        )
    )
    assert str(publication_error) == (
        "Postgres authoritative write failed for serving_projections via upsert_row_and_replace_rows: "
        "RuntimeError: upsert_row_and_replace_rows-boom"
    )
    assert isinstance(publication_error.__cause__, RuntimeError)
    checks += 1

    invariant_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="", prefer_read=False)
    )
    invariant_error = _runtime_error(
        lambda: invariant_repo.upsert_members("proj", [{"candidate_identity_key": "candidate:1"}])
    )
    assert str(invariant_error) == (
        "postgres-only invariant violated for serving_projection_members in upsert_serving_projection_members: "
        "should_prefer_read returned False; legacy SQLite tail retired (B4)"
    )
    checks += 1

    assert checks == 18


def test_serving_projection_member_mapper_preserves_irregular_read_contract() -> None:
    repository = ServingProjectionRepository(object())
    row = {
        "projection_id": "proj-guard",
        "candidate_identity_key": "candidate:1",
        "rank_index": -7,
        "public_summary_json": '{"name":"Ada"}',
        "projection_metrics_json": "{",
        "crm_overlay_summary_json": "[]",
        "provenance_json": None,
        "metadata_json": '{"source":"guard"}',
    }

    class BrokenRow:
        def __getitem__(self, key: str) -> object:
            raise RuntimeError(key)

    mapped = repository._member_from_row(row)
    assert mapped["rank_index"] == 0
    assert mapped["public_summary"] == {"name": "Ada"}
    assert mapped["projection_metrics"] == {}
    assert mapped["crm_overlay_summary"] == {}
    assert mapped["provenance"] == {}
    assert mapped["metadata"] == {"source": "guard"}
    assert repository._member_from_row(None) == {}
    broken = repository._member_from_row(BrokenRow())
    assert broken["projection_id"] == ""
    assert broken["rank_index"] == 0
    assert broken["public_summary"] == {}


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


def test_serving_projection_manifest_shard_mapper_preserves_irregular_read_contract() -> None:
    repository = ServingProjectionRepository(object())
    negative_row = {
        "shard_id": "negative",
        "projection_id": "proj-guard",
        "shard_kind": "candidate_identity_manifest",
        "shard_index": -7,
        "manifest_ref": "s3://bucket/negative.json",
        "row_count": -3,
        "metadata_json": "{",
    }
    malformed_row = {
        "shard_id": "malformed",
        "projection_id": "proj-guard",
        "shard_index": "bad",
        "row_count": "bad",
        "metadata_json": "[]",
    }

    class BrokenRow:
        def __getitem__(self, key: str) -> object:
            raise RuntimeError(key)

    assert repository._manifest_shard_from_row(None) == {}
    assert repository._manifest_shard_from_row(negative_row) == {
        "shard_id": "negative",
        "projection_id": "proj-guard",
        "shard_kind": "candidate_identity_manifest",
        "shard_index": 0,
        "manifest_ref": "s3://bucket/negative.json",
        "row_count": 0,
        "content_signature": "",
        "metadata": {},
        "created_at": "",
        "updated_at": "",
    }
    malformed = repository._manifest_shard_from_row(malformed_row)
    assert malformed["shard_index"] == 0
    assert malformed["row_count"] == 0
    assert malformed["metadata"] == {}
    assert repository._manifest_shard_from_row(BrokenRow()) == {
        "shard_id": "",
        "projection_id": "",
        "shard_kind": "",
        "shard_index": 0,
        "manifest_ref": "",
        "row_count": 0,
        "content_signature": "",
        "metadata": {},
        "created_at": "",
        "updated_at": "",
    }
