from pathlib import Path

from sourcing_agent import asset_catalog


def test_asset_catalog_discover_does_not_require_legacy_skill_siblings(tmp_path, monkeypatch) -> None:
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
