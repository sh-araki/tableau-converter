"""Generate standalone HTML reports from files in the local samples folder."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from main.common.file_classifier import FileClassifier
from main.desktop.convert import DesktopNodesView
from main.desktop.extract import DesktopInfoExtract
from main.desktop.html_export import DesktopHtmlExporter
from main.prep.main import PrepMainProcess
SAMPLE_DIR = PROJECT_ROOT / 'tests' / 'samples'
OUTPUT_DIR = PROJECT_ROOT / 'tests' / 'output'


def convert_sample(path):
  loaded = FileClassifier(path, f'export-{path.stem}').process()
  if path.suffix.lower() in {'.tfl', '.tflx'}:
    _, _, _, _, html = PrepMainProcess(loaded.data_dict, 'export').return_dataframes()
  else:
    extractor = DesktopInfoExtract(loaded.data_xml, 'export')
    extracted = [
      extractor.extract_info(),
      extractor.extract_datasource_connection(),
      extractor.extract_zone_info(),
    ]
    view = DesktopNodesView(extracted, 'export')
    html = DesktopHtmlExporter(view, extracted, 'export').build(path.name)

  destination = OUTPUT_DIR / f'{path.name}_report.html'
  destination.write_text(html, encoding='utf-8')
  return destination


def main():
  OUTPUT_DIR.mkdir(exist_ok=True)
  samples = sorted(
    path for path in SAMPLE_DIR.iterdir()
    if path.is_file() and path.suffix.lower() in FileClassifier.SUPPORTED_SUFFIXES
  )
  for sample in samples:
    output = convert_sample(sample)
    print(f'{sample.name} -> {output.name}')


if __name__ == '__main__':
  main()
