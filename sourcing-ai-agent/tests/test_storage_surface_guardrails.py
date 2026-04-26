from pathlib import Path


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
