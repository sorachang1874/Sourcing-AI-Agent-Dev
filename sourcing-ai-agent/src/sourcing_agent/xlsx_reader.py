from __future__ import annotations

from collections import defaultdict
from pathlib import Path, PurePosixPath
import xml.etree.ElementTree as ET
import zipfile


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"a": MAIN_NS, "r": REL_NS}


def _column_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    value = 0
    for letter in letters:
        value = value * 26 + (ord(letter.upper()) - 64)
    return value - 1


def _resolve_target(target: str) -> str:
    if target.startswith("/"):
        return target.lstrip("/")
    pure = PurePosixPath(target)
    if pure.parts and pure.parts[0] == "xl":
        return str(pure)
    return str(PurePosixPath("xl") / pure)


class XlsxWorkbook:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def sheet_names(self) -> list[str]:
        with zipfile.ZipFile(self.path) as archive:
            workbook = ET.fromstring(archive.read("xl/workbook.xml"))
            sheets = workbook.find("a:sheets", NS)
            if sheets is None:
                return []
            return [sheet.attrib["name"] for sheet in sheets]

    def read_sheet(self, sheet_name: str) -> list[dict[str, str]]:
        with zipfile.ZipFile(self.path) as archive:
            shared = self._shared_strings(archive)
            workbook = ET.fromstring(archive.read("xl/workbook.xml"))
            rel_map = self._relationship_map(archive)
            target = None
            sheets = workbook.find("a:sheets", NS)
            for sheet in sheets if sheets is not None else []:
                if sheet.attrib["name"] == sheet_name:
                    rel_id = sheet.attrib[f"{{{REL_NS}}}id"]
                    target = _resolve_target(rel_map[rel_id])
                    break
            if target is None:
                raise KeyError(f"Sheet {sheet_name!r} not found in {self.path}")
            root = ET.fromstring(archive.read(target))
            rows = self._sheet_rows(root, shared)
        if not rows:
            return []
        header = rows[0]
        records: list[dict[str, str]] = []
        for raw in rows[1:]:
            if not any(raw):
                continue
            record = {}
            for index, column in enumerate(header):
                if not column:
                    continue
                record[column] = raw[index] if index < len(raw) else ""
            records.append(record)
        return records

    def _shared_strings(self, archive: zipfile.ZipFile) -> list[str]:
        if "xl/sharedStrings.xml" not in archive.namelist():
            return []
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        values: list[str] = []
        for item in root.findall("a:si", NS):
            values.append("".join(item.itertext()).strip())
        return values

    def _relationship_map(self, archive: zipfile.ZipFile) -> dict[str, str]:
        root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        mapping: dict[str, str] = {}
        for rel in root.findall(f"{{{PACKAGE_REL_NS}}}Relationship"):
            mapping[rel.attrib["Id"]] = rel.attrib["Target"]
        return mapping

    def _sheet_rows(self, root: ET.Element, shared: list[str]) -> list[list[str]]:
        rows: list[list[str]] = []
        for row in root.findall(".//a:sheetData/a:row", NS):
            cells: defaultdict[int, str] = defaultdict(str)
            for cell in row.findall("a:c", NS):
                cell_ref = cell.attrib.get("r", "")
                index = _column_index(cell_ref)
                cells[index] = self._cell_value(cell, shared)
            if not cells:
                rows.append([])
                continue
            max_index = max(cells)
            rows.append([cells[idx] for idx in range(max_index + 1)])
        return rows

    def _cell_value(self, cell: ET.Element, shared: list[str]) -> str:
        cell_type = cell.attrib.get("t", "")
        if cell_type == "s":
            value = cell.find("a:v", NS)
            if value is None or value.text is None:
                return ""
            return shared[int(value.text)]
        if cell_type == "inlineStr":
            inline = cell.find("a:is", NS)
            return "".join(inline.itertext()).strip() if inline is not None else ""
        value = cell.find("a:v", NS)
        if value is None or value.text is None:
            return ""
        return value.text.strip()
