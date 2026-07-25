"""AssetCatalog discovery contracts.

Provenance: skill-sibling independence adopted as characterization 2026-07;
SOURCING_ASSET_PACKAGE_ROOT env-override seam added 2026-07-24 for the CI
asset-dependency blind spot (gitignored personal-data package cannot exist on
GitHub runners; lanes point discover() at tests/synthetic_asset_package.py
output instead — production resolution order unchanged when the env is unset).
"""

from pathlib import Path

import pytest

from sourcing_agent import asset_catalog
from sourcing_agent.ingestion import load_bootstrap_bundle
from tests.synthetic_asset_package import build_synthetic_asset_package


def test_asset_catalog_discover_does_not_require_legacy_skill_siblings(tmp_path, monkeypatch) -> None:
    # This test pins the PRODUCTION resolution order, so the CI env seam must
    # not be active even when the surrounding lane exports it.
    monkeypatch.delenv(asset_catalog.ASSET_PACKAGE_ROOT_ENV, raising=False)
    project_root = tmp_path / "repo" / "sourcing-ai-agent"
    anthropic_root = project_root / "local_asset_packages" / "anthropic"
    data_root = anthropic_root / "data"
    data_root.mkdir(parents=True)
    (anthropic_root / "anthropic_v1.xlsx").write_text("", encoding="utf-8")
    (anthropic_root / "README.md").write_text("", encoding="utf-8")
    (anthropic_root / "PROGRESS.md").write_text("", encoding="utf-8")
    (anthropic_root / "api_accounts.json").write_text("{}", encoding="utf-8")
    (anthropic_root / "company_ids.json").write_text("{}", encoding="utf-8")
    (anthropic_root / "investor_chinese_members_final.json").write_text("[]", encoding="utf-8")
    (data_root / "publications_unified.json").write_text("[]", encoding="utf-8")
    (data_root / "scholar_scan_results.json").write_text("[]", encoding="utf-8")

    monkeypatch.setattr(asset_catalog, "_project_root", lambda: project_root)

    catalog = asset_catalog.AssetCatalog.discover()

    assert catalog.project_root == project_root
    assert catalog.anthropic_asset_source == "project_local"
    optional_skill_root = project_root / "local_asset_packages" / "optional_skills"
    assert catalog.employee_scan_skill == optional_skill_root / "anthropic-employee-scan" / "SKILL.md"
    assert catalog.investor_scan_skill == optional_skill_root / "investor-chinese-scan" / "SKILL.md"
    assert catalog.onepager_skill == optional_skill_root / "biz-visit-onepager" / "SKILL.md"


def test_env_override_resolves_synthetic_package_and_supports_bootstrap_ingestion(
    tmp_path, monkeypatch
) -> None:
    """The CI seam end-to-end: env override -> discover -> workbook/json ingestion.

    This is the lane's guarantee that GitHub runners (no real package) resolve a
    catalog from the fully synthetic fixture package, including the homegrown
    xlsx reader path.
    """
    package_root = build_synthetic_asset_package(tmp_path / "synthetic_pkg")
    monkeypatch.setenv(asset_catalog.ASSET_PACKAGE_ROOT_ENV, str(package_root))

    catalog = asset_catalog.AssetCatalog.discover()

    assert catalog.anthropic_asset_source == "env_override"
    assert catalog.anthropic_root == package_root
    assert catalog.anthropic_workbook.name == "synthetic_roster_v1.xlsx"
    assert catalog.anthropic_project_root is None
    assert catalog.anthropic_external_root is None

    bundle = load_bootstrap_bundle(catalog)
    names = sorted(candidate.display_name for candidate in bundle.candidates)
    assert names == [
        "Synthetic Currentperson",
        "Synthetic Formerperson",
        "Synthetic Investorperson",
    ]


def test_env_override_fails_closed_when_path_is_not_a_package(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(asset_catalog.ASSET_PACKAGE_ROOT_ENV, str(tmp_path / "does_not_exist"))
    with pytest.raises(FileNotFoundError, match="SOURCING_ASSET_PACKAGE_ROOT"):
        asset_catalog.AssetCatalog.discover()


def test_synthetic_package_fixture_stays_reviewably_fake() -> None:
    """No real names/slugs/URLs may ever creep into the synthetic fixture.

    Every linkedin URL must carry the synthetic-fixture marker and every roster
    name must be prefixed 'Synthetic' — a reviewer can reject anything else on
    sight.
    """
    from tests import synthetic_asset_package as fixture

    for rows in fixture._SHEETS:
        header, *data_rows = rows
        for row in data_rows:
            record = dict(zip(header, row))
            assert record["姓名"].startswith("Synthetic ")
            assert record["LinkedIn链接"].startswith(
                "https://www.linkedin.com/in/synthetic-fixture-"
            )
            for value in record.values():
                assert "华人专项" not in value
