from pathlib import Path
import unittest

from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.xlsx_reader import XlsxWorkbook


class XlsxReaderTest(unittest.TestCase):
    def test_can_read_anthropic_workbook(self) -> None:
        catalog = AssetCatalog.discover()
        workbook = XlsxWorkbook(catalog.anthropic_workbook)
        self.assertIn("在职华人员工", workbook.sheet_names())

        current_rows = workbook.read_sheet("在职华人员工")
        investor_rows = workbook.read_sheet("主要投资方华人成员")

        self.assertGreaterEqual(len(current_rows), 100)
        self.assertGreaterEqual(len(investor_rows), 100)
        self.assertIn("姓名", current_rows[0])
        self.assertTrue(Path(catalog.anthropic_workbook).exists())
