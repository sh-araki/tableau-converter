import unittest
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from main.common.file_classifier import FileClassifier
from main.desktop.convert import DesktopNodesView
from main.desktop.extract import DesktopInfoExtract
from main.desktop.html_export import DesktopHtmlExporter
from main.prep.main import PrepMainProcess


class SampleIntegrationTests(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.sample_dir = PROJECT_ROOT / 'tests' / 'samples'
    cls.samples = sorted(
      path for path in cls.sample_dir.iterdir()
      if path.is_file() and path.suffix.lower() in FileClassifier.SUPPORTED_SUFFIXES
    )

  def test_samples_are_available(self):
    self.assertTrue(self.samples, 'No Tableau sample files were found')

  def test_all_samples_can_be_converted(self):
    for path in self.samples:
      with self.subTest(sample=path.name):
        loaded = FileClassifier(path, f'test-{path.stem}').process()
        if path.suffix.lower() in {'.tfl', '.tflx'}:
          elements, stylesheet, dataframes, uml, html = (
            PrepMainProcess(loaded.data_dict, 'test').return_dataframes()
          )
          self.assertIsInstance(elements, list)
          self.assertIsInstance(stylesheet, list)
          self.assertIsInstance(dataframes, dict)
          self.assertTrue(uml)
          self.assertIn('const elements =', html)
        else:
          extractor = DesktopInfoExtract(loaded.data_xml, 'test')
          extracted = [
            extractor.extract_info(),
            extractor.extract_datasource_connection(),
            extractor.extract_zone_info(),
          ]
          view = DesktopNodesView(extracted, 'test')
          elements, stylesheet = view.cytoscape_elements()
          self.assertIsInstance(elements, list)
          self.assertIsInstance(stylesheet, list)
          datasource_elements, _ = view.datasource_cytoscape_element()
          for datasource_nodes in (datasource_elements or {}).values():
            nodes = [item for item in datasource_nodes if 'source' not in item['data']]
            self.assertTrue(all('position' in node for node in nodes))
            positions = {(node['position']['x'], node['position']['y']) for node in nodes}
            self.assertEqual(len(nodes), len(positions))
          options = view.dashboard_and_layout_in_zone()
          if options:
            dashboard = sorted(options['dashboard_name'])[0]
            layout = sorted(options['layout_type'])[0]
            zone_elements, _ = view.zone_elements(dashboard, layout)
            self.assertTrue(all(item['data']['label'] for item in zone_elements))
          html = DesktopHtmlExporter(view, extracted, 'test').build(path.name)
          self.assertIn('const graphStore =', html)
          self.assertNotIn('const graphStore = [];', html)
          self.assertIn('collapseStructure()', html)
          self.assertIn("renderRows('Node details', nodeRows)", html)
          self.assertIn("cy.elements(':visible').layout", html)
          self.assertIn('id="dataframe-select"', html)
          self.assertIn('const dataframeNames = Object.keys(dataframeStore)', html)
          for name, dataframe in view.datasource_overview().items():
            if not dataframe.empty:
              self.assertIn(f'"{name}"', html)
          self.assertIn(path.name, html)


if __name__ == '__main__':
  unittest.main()
