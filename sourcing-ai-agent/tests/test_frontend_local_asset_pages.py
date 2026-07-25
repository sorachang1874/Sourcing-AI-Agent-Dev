from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_local_asset_pages_do_not_expose_internal_contract_jargon() -> None:
    page_sources = [
        REPO_ROOT / "frontend-demo/src/pages/CollectionsPage.tsx",
        REPO_ROOT / "frontend-demo/src/pages/CollectionPage.tsx",
    ]
    combined = "\n".join(path.read_text(encoding="utf-8") for path in page_sources)

    forbidden_user_facing_fragments = [
        "collection authoritative projection",
        "Canonical Projection",
        "Acquisition",
        "不在本页启动 workflow",
        "Raw index",
        "Evidence index",
    ]
    for fragment in forbidden_user_facing_fragments:
        assert fragment not in combined


def test_local_asset_flow_has_navigation_to_board_and_targets() -> None:
    tabs = (REPO_ROOT / "frontend-demo/src/components/LocalAssetTabs.tsx").read_text(encoding="utf-8")
    collection_page = (REPO_ROOT / "frontend-demo/src/pages/CollectionPage.tsx").read_text(encoding="utf-8")
    results_page = (REPO_ROOT / "frontend-demo/src/pages/ResultsPage.tsx").read_text(encoding="utf-8")
    collections_page = (REPO_ROOT / "frontend-demo/src/pages/CollectionsPage.tsx").read_text(encoding="utf-8")
    api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
    presentation_source = (REPO_ROOT / "frontend-demo/src/lib/localAssetPresentation.ts").read_text(encoding="utf-8")

    assert "公司资产 Overview" in tabs
    assert "候选人看板" in tabs
    assert "人工审核" in tabs
    assert "目标候选人" in tabs
    assert "getCollectionAssetEntry(normalizedCollectionId)" in tabs
    assert "resolvedAsset?.projectionId" in tabs
    assert "查看目标候选人" in collection_page
    assert "collectionId={collectionId}" in results_page
    target_page = REPO_ROOT.joinpath("frontend-demo/src/pages/TargetCandidatesPage.tsx").read_text(
        encoding="utf-8"
    )
    assert "sourceCollectionId={collectionId}" in target_page

    assert "companyMedia: mapCollectionCompanyMedia" in api_source
    assert "resolveApiMediaUrl" in api_source
    assert "127.0.0.1:8765" in api_source
    assert "company_media" in api_source
    assert "company_media_missing" in api_source
    assert "getCompanyAssetFacts" in api_source
    assert "/api/company-assets" in api_source
    assert "/api/company-assets/evidence" in api_source
    assert "/api/company-assets/assertions" in api_source
    assert "公司资料资产" in collection_page
    assert "companyFacts.assets" in collection_page
    assert "collectionLogoText(item)" in collections_page
    assert "collectionLogoText(assetEntry)" in collection_page
    assert "collectionLogoImageUrl(item)" in collections_page
    assert "collectionLogoImageUrl(assetEntry)" in collection_page
    assert "collectionLogoLabel(item)" in collections_page
    assert "collectionLogoLabel(assetEntry)" in collection_page
    assert "collectionLogoText" in presentation_source
    assert "collectionLogoImageUrl" in presentation_source
    assert "company_media_missing" in presentation_source


def test_local_asset_overview_uses_tabs_without_duplicate_page_header() -> None:
    collections_page = (REPO_ROOT / "frontend-demo/src/pages/CollectionsPage.tsx").read_text(encoding="utf-8")

    assert '<LocalAssetTabs active="overview" />' in collections_page
    assert '<header className="page-header' not in collections_page
    assert "<h2>公司资产 Overview</h2>" not in collections_page
    assert "浏览已经整理好的公司候选人资产" not in collections_page
    assert "选择公司后进入资产主页" not in collections_page


def test_collection_scoped_target_candidates_page_uses_asset_tabs_without_duplicate_header() -> None:
    target_page = (REPO_ROOT / "frontend-demo/src/pages/TargetCandidatesPage.tsx").read_text(encoding="utf-8")

    assert 'className="page local-asset-target-page"' in target_page
    assert '<LocalAssetTabs active="targets" collectionId={collectionId} />' in target_page
    assert 'sourceCollectionId={collectionId}' in target_page
    assert '!collectionId ? (' not in target_page
    assert "持续跟进" not in target_page
    assert "沉淀候选人跟进状态、质量评价、备注和公开信息审核。" not in target_page
    assert '<p className="eyebrow">目标候选人</p>' not in target_page
    assert "<h2>目标候选人</h2>\n            <p className=\"muted\">沉淀持续跟进" not in target_page
