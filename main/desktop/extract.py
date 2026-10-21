#import io
#import base64
from pathlib import Path
import pandas as pd
#import numpy as np
import json
#from PIL import Image, ImageColor, ImageDraw
#from lxml import etree as ET
from collections import defaultdict
import re
import logging


class DesktopInfoExtract:
  def __init__(self, data_xml, run_id):
    self.data_xml = data_xml
    self.run_id = run_id
    base_dir = Path(__file__).resolve().parent
    json_path = base_dir / "desktop_maps.json"
    with open(json_path, encoding="utf-8") as f:
      self.extract_maps = json.load(f)
    self.logger = logging.getLogger(self.__class__.__name__)
  
  #desktop_mapsのキーをフラット化してパスにする
  def get_all_key_values(self, d, parent_key=""):
    kv = {}
    for k, v in d.items():
      full_key = f"{parent_key}/{k}" if parent_key else k
      if isinstance(v, dict):
        kv.update(self.get_all_key_values(v, full_key))
      else:
        kv[full_key] = v
    self.logger.debug(f"[{self.run_id}] all key and values: {kv}")
    return kv

  def extract_info(self):
    root = self.data_xml
    run_id = self.run_id
    self.logger.info(f"[{run_id}] extract infos from xml using key maps")
    extract_maps = self.extract_maps
    organized_maps = self.get_all_key_values(extract_maps)
    self.logger.info(f"[{run_id}] extract infos")
    if root.tag == 'datasource':
      self.logger.info(f"[{run_id}] root tag is datasource")
      organized_maps = {
        k.replace("datasources/datasource/", ""): v
        for k, v in organized_maps.items()
        if "datasource/" in k and "datasources/" in k
      }
      self.logger.debug(f"[{run_id}] organized maps: {organized_maps}")
    dfs = defaultdict(list)
    for key, value in organized_maps.items():
      self.logger.info(f"[{run_id}] extract data from path: {key}")
      if 'self' in key:
        key = key.replace('/self', '')
        self.logger.info(f"[{run_id}] key replace: {key}")
      attrib_list = []
      for index, element in enumerate(root.findall(f'./{key}')):
        attrib = dict(element.attrib)
        attrib = {
            k: v.replace(r'\,', '<<COMMA>>')
            for k, v in attrib.items()
        }
        attrib['index'] = index
        attrib['text'] = element.text
        attrib['tag'] = element.tag
        self.logger.debug(f"[{run_id}] index: {index}, text: {element.text}, tag: {element.tag}")
        for num in value:
          parent = self.climb(element, num)
          parent_tag = parent.tag
          parent_attrib = {
              f'{parent_tag}_{k}': v
              for k, v in parent.attrib.items()
          }
          parent_attrib[f'{parent_tag}_text'] = parent.text
          #parent_tag = parent.tag
          #parent_attrib = parent.attrib
          #parent_text = parent.text
          #parent_attrib = {parent_tag + '_' + k: v for k, v in dict(parent_attrib).items()}
          #parent_attrib[f'{parent_tag}_text'] = parent_text
          self.logger.debug(f"[{run_id}] parent tag: {parent.tag}, text: {parent.text}, attrib: {parent_attrib}")
          attrib |= parent_attrib
        if 'formatted-text' in key:
          for idx, child in enumerate(element.findall("run")):
            child_attrib = attrib.copy()
            child_attrib["sub_index"] = idx
            child_attrib["sub_text"] = child.text or ""
            self.logger.debug(f"[{run_id}] formatted-text attrib: {child_attrib}")
            attrib_list.append(child_attrib)
        elif 'metadata-record' in key:
          for idx, child in enumerate(list(element)):
            child_attrib = attrib.copy()
            child_attrib["sub_index"] = idx
            child_attrib["sub_tag"] = child.tag
            child_attrib["sub_text"] = child.text or ""
            self.logger.debug(f"[{run_id}] metadata-record attrib: {child_attrib}")
            attrib_list.append(child_attrib)
        else:
          attrib_list.append(attrib)
      self.logger.info(f"[{run_id}] replace '' to None")
      attrib_list = [
        {
          k: None if isinstance(v, str) and v.strip() == "" else v
          for k, v in d.items()
        }
        for d in attrib_list
      ]
      df = pd.DataFrame(attrib_list)
      if not df.empty:
        self.logger.info(f"[{run_id}] attrib is not empty: add to dfs list")
        dfs[key].append(df)
    dfs_copy = {}
    for k, v in dfs.items():
      self.logger.info(f"[{run_id}] concat df list in dfs")
      df = pd.concat(v)
      dfs_copy[f"/{k}"] = df
    return dfs_copy

  @staticmethod
  #該当の要素のdepth階層上の要素を取り出す
  def climb(element, depth):
    for _ in range(depth):
      element = element.getparent()
    return element

  #データソースのリレーション・結合の情報を取り出す→課題：可読性(relationとjoinで処理を同じにできるか)
  def extract_datasource_connection(self):
    run_id = self.run_id
    root = self.data_xml
    self.logger.info(f"[{run_id}] extract datasource connection(relationship / join)")
    #twbなら複数データソースあり/tdsは単一
    datasources = (
      root.findall('./datasources/datasource')
      if root.tag == 'workbook'
      else [root]
    )
    grouped_rows = defaultdict(list)
    for datasource in datasources:
      if datasource.attrib.get('name') == 'Parameters':
        continue
      datasource_name = datasource.attrib.get('name')
      datasource_caption = datasource.attrib.get('caption')
      self.logger.info(f"[{run_id}] datasource name: {datasource_name}, caption: {datasource_caption}")
      #join(=child)の要素を取り出す
      self.logger.info(f"[{run_id}] try extract join elements in datasource")
      for obj in datasource.findall('.//object'):
        parent_caption = obj.attrib.get('caption')
        parent_id = obj.attrib.get('id')
        self.logger.debug(f"[{run_id}] parent id: {parent_id}, caption: {parent_caption}")
        self.logger.info(f"[{run_id}] extract expression")
        expressions = obj.xpath(
          ".//expression[contains(@op, '].[')]"
        )
        for expr in expressions:
          #一番下のexpressionタグの要素の、一つ上の要素にoperationが入っている
          #+一つ上の要素までを共通パスとしてgrouped_rowsに投入
          path = expr.getroottree().getpath(expr)
          pair_path = path.rsplit('/', 1)[0]
          target_dict = {
            'op': expr.attrib.get('op'),
            'operator': self.climb(expr, 1).attrib.get('op'),
            'parent_id': parent_id,
            'parent_caption': parent_caption,
            'type': 'child',
            'datasource_name': datasource_name,
            'datasource_caption': datasource_caption
          }
          self.logger.debug(f"[{run_id}] target dict: {target_dict}")
          grouped_rows[pair_path].append(target_dict)
      #relashionshipの要素を取り出す
      self.logger.info(f"[{run_id}] try extract relationship elements in datasource")
      for rel in datasource.findall('.//relationship'):
        expressions = rel.xpath(
          ".//expression[contains(@op, '[')]"
        )
        first_end_point = rel.find("first-end-point").attrib.get('object-id')
        second_end_point = rel.find("second-end-point").attrib.get('object-id')
        self.logger.debug(f"[{run_id}] first=left: {first_end_point}, second=right: {second_end_point}")
        self.logger.info(f"[{run_id}] extract expression")
        for expr in expressions:
          path = expr.getroottree().getpath(expr)
          path_index = path.rsplit('/', 1)[-1].split('[')[1].split(']')[0]
          self.logger.debug(f"[{run_id}] path: {path}")
          if int(path_index) % 2 == 0:
            table = first_end_point
          else:
            table = second_end_point
          pair_path = path.rsplit('/', 1)[0]
          target_dict = {
            'op': f"[{table}].{expr.attrib.get('op')}",
            'operator': self.climb(expr, 1).attrib.get('op'),
            'parent_id': None,
            'parent_caption': None,
            'type': 'parent',
            'datasource_name': datasource_name,
            'datasource_caption': datasource_caption
          }
          self.logger.debug(f"[{run_id}] target dict: {target_dict}")
          grouped_rows[pair_path].append(target_dict)
    rows = []
    self.logger.info(f"[{run_id}] make key and operation maps from grouped elements(using pair key)")
    #[テーブル名].[キー]となっている部分のテーブル名([]無し)を取り出す
    def extract_table(expr):
      m = re.search(r'\[([^\]]+)\]\.\[', expr)
      return m.group(1) if m else None
    for pair in grouped_rows.values():
      if len(pair) != 2:
        continue
      left, right = pair
      target_dict = {
        'left': left['op'],
        'left_table': extract_table(left['op']),
        'right': right['op'],
        'right_table': extract_table(right['op']),
        'operator': left['operator'],
        'parent_id': left['parent_id'],
        'parent_caption': left['parent_caption'],
        'type': left['type'],
        'datasource_name': left['datasource_name'],
        'datasource_caption': left['datasource_caption']
      }
      self.logger.debug(f"[{run_id}] target dict: {target_dict}")
      rows.append(target_dict)
    df = pd.DataFrame(rows)
    self.logger.debug(f"[{run_id}] datasource connection: {df}")
    return df

  ##zone dataを取り出す
  def extract_zone_info(self):
    root = self.data_xml
    run_id = self.run_id
    # zone, layout-cache, zone_style_format それぞれの辞書リストをためる
    tmp = defaultdict(list)
    self.logger.info(f"[{run_id}] extract zone data from dashboard")
    for db in root.findall('./dashboards/dashboard'):
      db_name = db.attrib['name']
      # デフォルトレイアウト
      self.logger.info(f"[{run_id}] extract default zone data from {db_name}")
      default_zone = db.find('./zones')
      dfs = self.extract_zonedata(default_zone, db_name)
      self.logger.debug(f"[{run_id}] default zone : {dfs}")
      for key, dict_list in dfs.items():
        tmp[key].extend(dict_list)
      # devicelayoutsレイアウト
      devicelayouts_zone = db.find('./devicelayouts/zones')
      if devicelayouts_zone is not None:
        self.logger.info(f"[{run_id}] extract devicelayouts zone data from {db_name}")
        dfs_dev = self.extract_zonedata(devicelayouts_zone, db_name, layout_type='devicelayouts')
        for key, dict_list in dfs_dev.items():
          tmp[key].extend(dict_list)
          self.logger.debug(f"[{run_id}] devicelayouts zone : {dict_list}")
    # 各カテゴリごとに一括でDataFrame化
    dfs = {
      k: pd.DataFrame(v) if v else pd.DataFrame()
      for k, v in tmp.items()
    }
    self.logger.debug(f"[{run_id}] all zone data : {dfs}")
    return dfs

  @staticmethod
  def extract_zonedata(zones, db_name, layout_type='default'):
    all_zone_elements = zones.findall('.//zone') if zones is not None else []
    result = defaultdict(list)
    for zone_element in all_zone_elements:
      parent = zone_element.getparent()
      parent_id = parent.get('id') if parent is not None else None
      zone_dict = dict(
        **zone_element.attrib,
        parent_id=parent_id,
        layout_type=layout_type,
        dashboard_name=db_name
      )
      result['zone'].append(zone_dict)
      #子zoneがない場合に、そのzoneの直接の子要素から layout-cache / zone-style を探す
      has_child_zone = any(child.tag == "zone" for child in zone_element)
      if not has_child_zone:
        member_id = zone_element.get('id')
        for child in zone_element:
          # layout-cache
          if child.tag == 'layout-cache':
            layout_dict = dict(
                **child.attrib,
                id=member_id,
                layout_type=layout_type,
                dashboard_name=db_name
            )
            result['layout-cache'].append(layout_dict)
          # zone-style
          elif child.tag == 'zone-style':
            for format_element in child:
              format_dict = dict(
                  **format_element.attrib,
                  id=member_id,
                  layout_type=layout_type,
                  dashboard_name=db_name
              )
              result['zone_style_format'].append(format_dict)
    return result