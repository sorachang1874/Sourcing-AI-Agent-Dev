"""Fully-synthetic local asset package builder (CI asset-dependency fix, 2026-07-24).

Why: the real Anthropic local asset package is gitignored PERSONAL DATA that
exists only on the developer machine. The curated CI lanes (backend-ci
contract lane + ci-containerized-pre-release gates) contain tests that reach
``AssetCatalog.discover()``, which fails with FileNotFoundError on GitHub
runners where the package cannot exist. This module builds a minimal,
REVIEWABLY FAKE package that satisfies the full ``AssetCatalog`` contract
(workbook glob, README/PROGRESS, legacy jsons, data/ jsons) so lanes point
``SOURCING_ASSET_PACKAGE_ROOT`` (see ``sourcing_agent.asset_catalog``) at it
and never depend on real candidate data.

HARD RULE: everything in here must stay obviously synthetic — placeholder
names, ``example.invalid`` style URLs, empty account/id maps. Never copy real
names, slugs, URLs, or workbook rows into this file.

Usage:
    python -m tests.synthetic_asset_package <dest-dir>       # CLI (Makefile/CI)
    build_synthetic_asset_package(Path(...))                 # from tests
"""

from __future__ import annotations

import json
import sys
import zipfile
from pathlib import Path

WORKBOOK_NAME = "synthetic_roster_v1.xlsx"

_XLSX_CONTENT_TYPES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/worksheets/sheet3.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>
"""

_XLSX_ROOT_RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
"""

_XLSX_WORKBOOK = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="在职华人员工" sheetId="1" r:id="rId1"/>
    <sheet name="已离职华人员工" sheetId="2" r:id="rId2"/>
    <sheet name="主要投资方华人成员" sheetId="3" r:id="rId3"/>
  </sheets>
</workbook>
"""

_XLSX_WORKBOOK_RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/>
</Relationships>
"""

# Sheet headers mirror what sourcing_agent.ingestion reads; every ROW value is
# an obviously synthetic placeholder.
_SHEETS: list[list[list[str]]] = [
    # 在职华人员工 (current employees)
    [
        ["姓名", "现任职位/团队", "加入时间", "主要研究方向", "教育经历", "主要工作经历", "备注", "LinkedIn链接", "媒体链接"],
        [
            "Synthetic Currentperson",
            "Synthetic Role",
            "2020-01",
            "synthetic fixture area",
            "Synthetic University",
            "Synthetic Employer",
            "synthetic fixture row — not a real person",
            "https://www.linkedin.com/in/synthetic-fixture-current-000/",
            "https://media.example.invalid/synthetic-current",
        ],
    ],
    # 已离职华人员工 (former employees)
    [
        ["姓名", "原职位/团队", "加入时间", "离职时间", "当前去向", "主要研究方向", "教育经历", "主要工作经历", "LinkedIn链接", "媒体链接"],
        [
            "Synthetic Formerperson",
            "Synthetic Former Role",
            "2019-01",
            "2021-06",
            "Synthetic Next Org",
            "synthetic fixture area",
            "Synthetic University",
            "Synthetic Employer",
            "https://www.linkedin.com/in/synthetic-fixture-former-000/",
            "https://media.example.invalid/synthetic-former",
        ],
    ],
    # 主要投资方华人成员 (investor members)
    [
        ["姓名", "职位", "机构", "族裔背景", "是否涉及Anthropic投资", "投资方向", "备注", "LinkedIn链接", "媒体链接"],
        [
            "Synthetic Investorperson",
            "Synthetic Partner",
            "Synthetic Capital",
            "synthetic",
            "否",
            "synthetic fixture direction",
            "synthetic fixture row — not a real person",
            "https://www.linkedin.com/in/synthetic-fixture-investor-000/",
            "https://media.example.invalid/synthetic-investor",
        ],
    ],
]

_README = """# Synthetic local asset package (test fixture)

> Status: Generated test fixture (tests/synthetic_asset_package.py). Fully synthetic; never real candidate data.

This package is generated by `tests/synthetic_asset_package.py` so CI lanes can
resolve `AssetCatalog.discover()` without the real, gitignored personal-data
package. Every value in here is deliberately fake. Do not put real candidate
data in this package or in its generator.
"""

_PROGRESS = """# Synthetic progress notes (test fixture)

> Status: Generated test fixture (tests/synthetic_asset_package.py). Fully synthetic; never real candidate data.

Generated placeholder. No real project or candidate information.
"""


def _column_ref(index: int) -> str:
    letters = ""
    value = index
    while True:
        letters = chr(ord("A") + value % 26) + letters
        value = value // 26 - 1
        if value < 0:
            return letters


def _escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _sheet_xml(rows: list[list[str]]) -> str:
    row_chunks: list[str] = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for col_index, value in enumerate(row):
            ref = f"{_column_ref(col_index)}{row_index}"
            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{_escape(value)}</t></is></c>')
        row_chunks.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{''.join(row_chunks)}</sheetData></worksheet>"
    )


def _write_workbook(path: Path) -> None:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", _XLSX_CONTENT_TYPES)
        archive.writestr("_rels/.rels", _XLSX_ROOT_RELS)
        archive.writestr("xl/workbook.xml", _XLSX_WORKBOOK)
        archive.writestr("xl/_rels/workbook.xml.rels", _XLSX_WORKBOOK_RELS)
        for sheet_number, rows in enumerate(_SHEETS, start=1):
            archive.writestr(f"xl/worksheets/sheet{sheet_number}.xml", _sheet_xml(rows))


def build_synthetic_asset_package(dest: Path) -> Path:
    """Build the synthetic package under ``dest`` and return ``dest``.

    Idempotent: rewrites the fixture files in place on every call.
    """
    dest = Path(dest)
    (dest / "data").mkdir(parents=True, exist_ok=True)
    _write_workbook(dest / WORKBOOK_NAME)
    (dest / "README.md").write_text(_README, encoding="utf-8")
    (dest / "PROGRESS.md").write_text(_PROGRESS, encoding="utf-8")
    (dest / "api_accounts.json").write_text(json.dumps({"accounts": []}), encoding="utf-8")
    (dest / "company_ids.json").write_text(json.dumps({}), encoding="utf-8")
    (dest / "investor_chinese_members_final.json").write_text(
        json.dumps({"institutions": {}}), encoding="utf-8"
    )
    (dest / "data" / "publications_unified.json").write_text(
        json.dumps({"publications": {}}), encoding="utf-8"
    )
    (dest / "data" / "scholar_scan_results.json").write_text(
        json.dumps({"new_chinese_candidates": [], "pending_review": []}), encoding="utf-8"
    )
    return dest


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: python -m tests.synthetic_asset_package <dest-dir>", file=sys.stderr)
        return 2
    dest = build_synthetic_asset_package(Path(argv[0]))
    print(f"synthetic asset package ready at {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
