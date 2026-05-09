import io
import base64
from pathlib import Path
import pandas as pd
import numpy as np
import json
from PIL import Image, ImageColor, ImageDraw
from lxml import etree as ET
from collections import defaultdict
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
  
  def get_all_key_values(self, d, parent_key=""):
    kv = {}
    for k, v in d.items():
      full_key = f"{parent_key}/{k}" if parent_key else k
      if isinstance(v, dict):
        kv.update(self.get_all_key_values(v, full_key))
      else:
        kv[full_key] = v
    return kv

  def extract_info(self):
    root = self.data_xml
    run_id = self.run_id
    extract_maps = self.extract_maps
    organized_maps = self.get_all_key_values(extract_maps)
    dfs = defaultdict(list)
    if root.tag == 'workbook':
      for key, value in organized_maps.items():
        element_list = []
        if 'self' in key:
          key = key.replace('/self', '')
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
          for num in value:
            parent = self.climb(element, num)
            parent_tag = parent.tag
            parent_attrib = parent.attrib
            parent_text = parent.text
            parent_attrib = {parent_tag + '_' + k: v for k, v in dict(parent_attrib).items()}
            parent_attrib[f'{parent_tag}_text'] = parent_text
            attrib |= attrib | parent_attrib
          if 'formatted-text' in key:
            for idx, child in enumerate(element.findall("run")):
              child_attrib = attrib.copy()
              child_attrib["sub_index"] = idx
              child_attrib["sub_text"] = child.text or ""
              attrib_list.append(child_attrib)
          elif 'metadata-record' in key:
            for idx, child in enumerate(list(element)):
                child_attrib = attrib.copy()
                child_attrib["sub_index"] = idx
                child_attrib["sub_tag"] = child.tag
                child_attrib["sub_text"] = child.text or ""
                attrib_list.append(child_attrib)
          else:
            attrib_list.append(attrib)
        attrib_list = self.normalize_list_of_dict(attrib_list)
        df = pd.DataFrame(attrib_list)
        dfs[key].append(df)
      dfs_copy = {}
      for k, v in dfs.items():
        if v is not []:
          df = pd.concat(v)
          dfs_copy[f"/{k}"] = df
    return dfs_copy

  @staticmethod
  def normalize_list_of_dict(data):
    return [
      {
        k: None if isinstance(v, str) and v.strip() == "" else v
        for k, v in d.items()
      }
      for d in data
    ]

  @staticmethod
  def climb(element, depth):
    for _ in range(depth):
      element = element.getparent()
    return element

  ##datasource
  def extract_datasource_connection(self):
    root = self.data_xml
    if root.tag == 'workbook':
      datasources = root.findall('./datasources/datasource')
    else:
      datasources = [root]
    join_list = []
    relationship_list = []
    for datasource in datasources:
      for child in datasource:
        if 'object-graph' in child.tag:
          for data_object in child.findall('./objects/object'):
            object_info = data_object.attrib
            element_list = data_object.findall('./properties/relation')
            while(True):
              if len(element_list)==0:
                break
              else:
                for element in element_list:
                  connection_dict = element.attrib
                  connection_dict.update({f'object-{key}': value for key, value in object_info.items()})
                  if element.attrib.get('connection', None) is not None:
                    element_list.remove(element)
                    element_list.extend(element.findall('relation'))
                  elif element.attrib.get('join', None) is not None:
                    for join_condition in self.return_join_list(element.find('clause')):
                      table_list = join_condition[0].split(join_condition[1])
                      connection_dict['left'] = table_list[0]
                      connection_dict['right'] = table_list[1]
                      connection_dict['key'] = join_condition[0]
                      join_list.append(connection_dict)
                    element_list.remove(element)
                    element_list.extend(element.findall('relation'))
                  else:
                    element_list.remove(element)
          for data_relationship in child.findall('./relationships/relationship'):
            left_table = data_relationship.find('first-end-point').attrib['object-id']
            right_table = data_relationship.find('second-end-point').attrib['object-id']
            for join_condition in self.return_join_list(data_relationship):
              relationship_list.append({'left': '[' + left_table + ']', 'right': '[' + right_table + ']', 'key': join_condition[0]})
    df_join = pd.DataFrame(join_list)
    df_relationship = pd.DataFrame(relationship_list)
    return {'join': df_join, 'relationship': df_relationship} 

  @staticmethod
  def return_join_list(element):
    join_list = []
    join_element = element.find('expression')
    if join_element is not None:
      if join_element.attrib.get('op', None) == 'AND':
        for key_element in join_element.findall('expression'):
          join_key = key_element.attrib.get('op', None)
          left_value = key_element.findall('expression')[0].attrib.get('op', None)
          right_value = key_element.findall('expression')[1].attrib.get('op', None)
          join_list.append([left_value + join_key + right_value, join_key])
      else:
        join_key = join_element.attrib.get('op', None)
        left_value = join_element.findall('expression')[0].attrib.get('op', None)
        right_value = join_element.findall('expression')[1].attrib.get('op', None)
        join_list.append([left_value + join_key + right_value, join_key])
    return join_list

  ##zone data
  def extract_zone_info(self):
    root = self.data_xml
    tmp = defaultdict(list)
    for db in root.findall('./dashboards/dashboard'):
      db_name = db.attrib['name']
      default_zone = db.find('./zones')
      dfs = self.extract_zonedata(
        default_zone,
        db_name
      )
      for key, df in dfs.items():
        tmp[key].append(df)
      devicelayouts_zone = db.find('./devicelayouts/zones')
      if devicelayouts_zone is not None:
        dfs_dev = self.extract_zonedata(
          devicelayouts_zone,
          db_name
        )
        for key, df in dfs_dev.items():
          tmp[key].append(df)
    dfs = {
      k: pd.concat(v, ignore_index=True)
      for k, v in tmp.items()
    }
    return dfs

  @staticmethod
  def extract_zonedata(zones, db_name, layout_type='default'):
    all_zone_elements = zones.findall('.//zone')
    dfs = defaultdict(list)
    for zone_element in all_zone_elements:
      parent_id = zone_element.getparent().get('id')
      zone_dict = dict(
        **zone_element.attrib,
        **{'parent_id': parent_id}
      )
      df = pd.DataFrame([zone_dict])
      df['layout_type'] = layout_type
      df['dashboard_name'] = db_name
      dfs['zone'].append(df)
      has_child_zone = any(
          child.tag == "zone"
          for child in zone_element
      )
      if has_child_zone is False:
        for child in zone_element:
          member_id = zone_element.get('id')
          if child.tag == 'layout-cache':
            layout_dict = dict(
              **child.attrib,
              **{'id': member_id}
            )
            df = pd.DataFrame([layout_dict])
            df['layout_type'] = layout_type
            df['dashboard_name'] = db_name
            dfs['layout-cache'].append(df)
          elif child.tag == 'zone-style':
            for format_element in child:
              format_dict = dict(
                **format_element.attrib,
                **{'id': member_id}
              )
              df = pd.DataFrame([format_dict])
              df['layout_type'] = layout_type
              df['dashboard_name'] = db_name
              dfs['zone_style_format'].append(df)
    dfs_copy = {}
    for k, v in dfs.items():
      if v is not []:
        df = pd.concat(v)
        dfs_copy[k] = df
    return dfs_copy