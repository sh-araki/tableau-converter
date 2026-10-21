import zipfile
import json
from pathlib import Path
import logging
from lxml import etree as ET

class FileClassifier:
  SUPPORTED_SUFFIXES = {'.tds', '.tdsx', '.twb', '.twbx', '.tfl', '.tflx'}
  ARCHIVE_SUFFIXES = {'.tdsx', '.twbx', '.tfl', '.tflx'}
  PREP_ARCHIVE_MEMBERS = {'displaySettings', 'flow'}

  #selfでファイル拡張子や、zip対象の拡張子、空のdictを定義
  def __init__(self, file_path, run_id):
    p = Path(file_path)
    self.path = p
    self.parent = p.parent
    self.suffix = p.suffix.lower()
    self.name = p.name
    self.stem = p.stem
    self.data_dict = {}
    self.data_xml = None
    self.logger = logging.getLogger(__name__)
    self.run_id = run_id

  @staticmethod
  def _parse_xml(data):
    """Parse uploaded XML without resolving entities or using the network."""
    parser = ET.XMLParser(resolve_entities=False, no_network=True, huge_tree=False)
    return ET.fromstring(data, parser=parser)

  #zipの中身のデータをdictに追加
  def add_data_in_zip(self):
    self.logger.info(f"[{self.run_id}] extract zip data")
    with zipfile.ZipFile(self.path, 'r') as zip_ref:
      for name in zip_ref.namelist():
        with zip_ref.open(name) as f:
          if name.endswith('.twb') or name.endswith('.tds'):
            self.logger.info(f"[{self.run_id}] extract twb or tds data")
            try:
              self.data_xml = self._parse_xml(f.read())
            except Exception as e:
              self.logger.exception(f"[{self.run_id}] Error in extract or convert")
              raise ValueError(f"Invalid XML in archive member: {name}") from e
          elif name in self.PREP_ARCHIVE_MEMBERS:
            self.logger.info(f"[{self.run_id}] extract flow or displaySettings data")
            try:
              data = json.load(f)
              self.data_dict[name] = data
            except Exception as e:
              self.logger.exception(f"[{self.run_id}] Error in extract or convert")
              raise ValueError(f"Invalid JSON in archive member: {name}") from e
    if self.suffix in ['.tfl', '.tflx']:
      missing = self.PREP_ARCHIVE_MEMBERS - self.data_dict.keys()
      if missing:
        raise ValueError(f"Missing required Prep data: {', '.join(sorted(missing))}")
    elif self.data_xml is None:
      raise ValueError("The Tableau archive does not contain a .twb or .tds file")
    return self

  #xmlをdictに変換
  def convert_xml(self):
    self.logger.info(f"[{self.run_id}] convert twb or tds to json data")
    try:
      with open(self.path, 'rb') as f:
        self.data_xml = self._parse_xml(f.read())
    except Exception as e:
      self.logger.exception(f"[{self.run_id}] Error in extract or convert")
      raise ValueError(f"Invalid Tableau XML: {self.name}") from e
    return self

  #メイン処理
  def process(self):
    if self.suffix not in self.SUPPORTED_SUFFIXES:
      raise ValueError(f"Unsupported Tableau file type: {self.suffix or '(none)'}")
    try:
      self.logger.info(f"[{self.run_id}] extract data in file")
      if self.suffix in ['.tfl', '.tflx']:
        return self.add_data_in_zip()  # tfl, tflx: zip内json
      elif self.suffix in ['.tdsx', '.twbx']:
        return self.add_data_in_zip()  # tdsx, twbx: zip内xml
      elif self.suffix in ['.twb', '.tds']:
        return self.convert_xml()    # twb, tds: xml
      else:
        raise ValueError(f"Unsupported Tableau file type: {self.suffix or '(none)'}")
    except Exception as e:
      self.logger.exception(f"[{self.run_id}] Error in extract or convert")
      raise
