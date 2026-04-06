from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _pick_single_inner_dir(path: Path) -> Path:
    candidates = [child for child in path.iterdir() if child.is_dir() and child.name != "__MACOSX"]
    if not candidates:
        raise FileNotFoundError(f"No extracted directory found under {path}")
    if len(candidates) == 1:
        return candidates[0]
    return sorted(candidates)[0]


def _latest_file(path: Path, pattern: str) -> Path:
    matches = list(path.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files matching {pattern} under {path}")
    return max(matches, key=lambda item: (_natural_key(item.stem), item.stat().st_mtime))


def _natural_key(value: str) -> tuple:
    parts = re.split(r"(\d+)", value)
    normalized = []
    for part in parts:
        if not part:
            continue
        if part.isdigit():
            normalized.append(int(part))
        else:
            normalized.append(part.lower())
    return tuple(normalized)


@dataclass(frozen=True, slots=True)
class AssetCatalog:
    project_root: Path
    dev_root: Path
    anthropic_root: Path
    anthropic_workbook: Path
    anthropic_readme: Path
    anthropic_progress: Path
    legacy_api_accounts: Path
    legacy_company_ids: Path
    anthropic_publications: Path
    scholar_scan_results: Path
    investor_members_json: Path
    employee_scan_skill: Path
    investor_scan_skill: Path
    onepager_skill: Path

    @classmethod
    def discover(cls) -> "AssetCatalog":
        project_root = _project_root()
        dev_root = project_root.parent
        anthropic_root = _pick_single_inner_dir(dev_root / "Anthropic华人专项")
        employee_skill_root = _pick_single_inner_dir(dev_root / "anthropic-employee-scan")
        investor_skill_root = _pick_single_inner_dir(dev_root / "investor-chinese-scan")
        onepager_skill_root = _pick_single_inner_dir(dev_root / "biz-visit-onepager")
        return cls(
            project_root=project_root,
            dev_root=dev_root,
            anthropic_root=anthropic_root,
            anthropic_workbook=_latest_file(anthropic_root, "*v*.xlsx"),
            anthropic_readme=anthropic_root / "README.md",
            anthropic_progress=anthropic_root / "PROGRESS.md",
            legacy_api_accounts=anthropic_root / "api_accounts.json",
            legacy_company_ids=anthropic_root / "company_ids.json",
            anthropic_publications=anthropic_root / "data" / "publications_unified.json",
            scholar_scan_results=anthropic_root / "data" / "scholar_scan_results.json",
            investor_members_json=anthropic_root / "investor_chinese_members_final.json",
            employee_scan_skill=employee_skill_root / "SKILL.md",
            investor_scan_skill=investor_skill_root / "SKILL.md",
            onepager_skill=onepager_skill_root / "SKILL.md",
        )
