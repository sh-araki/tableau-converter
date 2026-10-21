import unittest
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from main.prep.main import PrepMainProcess


class PrepHtmlTests(unittest.TestCase):

  def test_nested_values_are_serialized_in_table_cells(self):
    template = (
      PROJECT_ROOT / 'main' / 'prep' / 'templates' / 'report.html'
    ).read_text(encoding='utf-8')
    self.assertIn('function formatCell(value)', template)
    self.assertIn('JSON.stringify(value, null, 2)', template)
    self.assertIn('escapeHtml(formatCell(row[col]))', template)
  def test_embeds_valid_json_values(self):
    template = 'const elements = []; style: []; const dfStore = [];'
    html = PrepMainProcess.return_html(
      [{'data': {'id': 'n1', 'value': None}}],
      [{'selector': 'node', 'enabled': True}],
      {'details': pd.DataFrame([{'node_baseid': 'n1', 'value': None}])},
      template,
    )

    self.assertIn('"value": null', html)
    self.assertIn('"enabled": true', html)
    self.assertNotIn("'enabled': True", html)

  def test_escapes_closing_script_in_embedded_data(self):
    template = 'const elements = []; style: []; const dfStore = [];'
    html = PrepMainProcess.return_html(
      [{'data': {'id': '</script>'}}],
      [],
      {},
      template,
    )

    self.assertNotIn('"id": "</script>"', html)
    self.assertIn('"id": "<\\/script>"', html)


if __name__ == '__main__':
  unittest.main()
