import zipfile
import json
from pathlib import Path
import logging
from lxml import etree as ET

class FileClassifier:
  #selfでファイル拡張子や、zip対象の拡張子、空のdictを定義
  def __init__(self, file_path, run_id):
    p = Path(file_path)
    self.path = p
    self.parent = p.parent
    self.suffix = p.suffix.lower()
    self.name = p.name
    self.stem = p.stem
    self.target_suffix = ['.tdsx', '.twbx', '.tfl', '.tflx']
    self.data_dict = {}
    self.logger = logging.getLogger(__name__)
    self.run_id = run_id

  #zipの中身のデータをdictに追加
  def add_data_in_zip(self):
    self.logger.info(f"[{self.run_id}] extract zip data")
    target_file = ['displaySettings', 'flow']
    with zipfile.ZipFile(self.path, 'r') as zip_ref:
      for name in zip_ref.namelist():
        with zip_ref.open(name) as f:
          if name.endswith('.twb') or name.endswith('.tds'):
            self.logger.info(f"[{self.run_id}] extract twb or tds data")
            try:
              self.data_xml = ET.fromstring(f.read())
            except Exception as e:
              self.logger.exception(f"[{self.run_id}] Error in extract or convert")
          elif name in target_file:
            self.logger.info(f"[{self.run_id}] extract flow or displaySettings data")
            try:
              data = json.load(f)
              self.data_dict[name] = data
            except Exception as e:
              self.logger.exception(f"[{self.run_id}] Error in extract or convert")
    return self

  #xmlをdictに変換
  def convert_xml(self):
    self.logger.info(f"[{self.run_id}] convert twb or tds to json data")
    try:
      with open(self.path, 'rb') as f:
        self.data_xml = ET.fromstring(f.read())
    except Exception as e:
      self.logger.exception(f"[{self.run_id}] Error in extract or convert")
    return self

  #メイン処理
  def process(self):
    try:
      self.logger.info(f"[{self.run_id}] extract data in file")
      if self.suffix in ['.tfl', '.tflx']:
        return self.add_data_in_zip()  # tfl, tflx: zip内json
      elif self.suffix in ['.tdsx', '.twbx']:
        return self.add_data_in_zip()  # tdsx, twbx: zip内xml
      elif self.suffix in ['.twb', '.tds']:
        return self.convert_xml()    # twb, tds: xml
      else:
        self.logger.info(f"[{self.run_id}] not supported file: {self.name}")
        return None
    except Exception as e:
      self.logger.exception(f"[{self.run_id}] Error in extract or convert")