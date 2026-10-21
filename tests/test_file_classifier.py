import json
import tempfile
import unittest
import zipfile
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from main.common.file_classifier import FileClassifier


class FileClassifierTests(unittest.TestCase):
  def test_reads_plain_workbook_xml(self):
    with tempfile.TemporaryDirectory() as directory:
      path = Path(directory) / 'sample.twb'
      path.write_text('<workbook/>', encoding='utf-8')

      result = FileClassifier(path, 'test').process()

      self.assertEqual(result.data_xml.tag, 'workbook')

  def test_reads_prep_archive(self):
    with tempfile.TemporaryDirectory() as directory:
      path = Path(directory) / 'sample.tfl'
      with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('flow', json.dumps({'nodes': {}}))
        archive.writestr('displaySettings', json.dumps({'displaySettings': {}}))

      result = FileClassifier(path, 'test').process()

      self.assertEqual(set(result.data_dict), {'flow', 'displaySettings'})

  def test_rejects_unsupported_extension(self):
    with tempfile.TemporaryDirectory() as directory:
      path = Path(directory) / 'sample.txt'
      path.write_text('not tableau', encoding='utf-8')

      with self.assertRaises(ValueError):
        FileClassifier(path, 'test').process()


if __name__ == '__main__':
  unittest.main()
