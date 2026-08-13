#import io
#import base64
from pathlib import Path
import pandas as pd
import numpy as np
import json
#from PIL import Image, ImageColor, ImageDraw
#from io import BytesIO
#from lxml import etree as ET
#from collections import defaultdict
import logging
import math
import hashlib


class DesktopNodesView:
  def __init__(self, datas, run_id):
    base_dir = Path(__file__).resolve().parent
    json_path = base_dir / "desktop_maps.json"
    with open(json_path, encoding="utf-8") as f:
      self.extract_maps = json.load(f)
    self.logger = logging.getLogger(self.__class__.__name__)
    self.run_id = run_id
    self.dfs, self.datasource_connection, self.zone_info = datas
    stylesheet_path = base_dir / "stylesheet.json"
    with open(stylesheet_path, encoding="utf-8") as f:
      self.stylesheet_maps = json.load(f)
    
  def cytoscape_elements(self):
    dfs = self.dfs
    paths = dfs.keys()
    nodes = {}
    edges = {}
    # root node
    nodes["root"] = {
        "data": {
            "id": "root",
            "label": "/",
            "fullPath": "/",
            "isLeaf": False
        }
    }
    for full_path in paths:
      parts = [p for p in full_path.split("/") if p]
      current = ""
      for i, part in enumerate(parts):
        parent = current if current else "root"
        current += "/" + part
        is_leaf = (i == len(parts) - 1)
        # -------------------------
        # node
        # -------------------------
        if current not in nodes:
          nodes[current] = {
            "data": {
              "id": current,
              "label": part,
              "fullPath": current,
              "isLeaf": is_leaf
            }
          }
        # -------------------------
        # edge
        # -------------------------
        if current not in edges:
          edges[current] = {
            "data": {
              "id": f"edge:{current}",
              "source": parent,
              "target": current
            }
          }
    elements = (
      list(nodes.values()) +
      list(edges.values())
    )
    stylesheet = self.stylesheet_maps['node_info']
    return elements, stylesheet

  #空列を削除
  @staticmethod
  def clean_dataframe(df):
      return df.dropna(axis=1, how="all")

  #フィールドの概要ビューを作成する
  def datasource_overview(self):
    dfs = self.dfs
    data_map = {
      'metadata': '/datasources/datasource/connection/metadata-records/metadata-record',
      'calculation': '/datasources/datasource/column/calculation',
      'connection': '/datasources/datasource/connection/named-connections/named-connection/connection',
      'datasource_column': '/datasources/datasource/column'
    }
    df_map = {}
    for k, v in data_map.items():
      df = dfs.get(v)
      if df is not None:
        if k == 'connection':
          df = df.rename(columns={
            "named-connection_caption": "connect_caption", 
            "class": "source_connection_class"
          })
        df_map[k] = self.clean_dataframe(df)
    #metadata
    metadata = df_map['metadata']
    columns_list = metadata.columns
    remove_list = ["sub_index", "sub_tag", "sub_text"]
    index_list = [x for x in columns_list if x not in remove_list]
    pivot = (
      metadata.pivot(
        index=index_list,
        columns="sub_tag",
        values="sub_text"
      )
    ).reset_index().rename(columns={'local-name': 'name'}).drop(columns=['index'])
    df_map['metadata'] = pivot

    #calculation
    if 'calculation' in df_map:
      calculation = df_map['calculation']
      df_replacemap = df_map['datasource_column'][['caption', 'name', 'datasource_name']]
      #行カウントとパラメーターの文字列を変換
      df_replacemap["column"] = df_replacemap.apply(
          lambda r:
              f"[{r['datasource_name']}].{r['name']}"
              if r["datasource_name"] == "Parameters"
              else (
                  f"[{r['datasource_name']}].[{r['caption']}]"
                  if '[__tableau_internal_object_id__]' in r["name"]
                  else r['name']
              ),
          axis=1
      )
      df_replacemap = df_replacemap[df_replacemap['caption'].notna()]
      df_replacemap['caption'] = '[' + df_replacemap['caption'] +']'
      #mappingを作ってcalculationの計算式列を変換
      mapping = df_replacemap.set_index("column")["caption"].to_dict()
      for old, new in mapping.items():
        calculation["formula"] = calculation["formula"].str.replace(
          old,
          new,
          regex=False
        )
      df_map['calculation'] = calculation
    return df_map

  #データソースのマップをデータソースごとに作る(親子関係のみ)
  #elements = [
  #    # ===== group nodes =====
  #    {"data": {"id": "group1", "comment": "parent"}},
  #    {"data": {"id": "group2", "comment": "parent"}},
  #
  #    # ===== child nodes (group1) =====
  #    {"data": {"id": "n1", "parent": "group1", "comment": "child"}},
  #    {"data": {"id": "n2", "parent": "group1", "comment": "child"}},
  #
  #    # ===== child nodes (group2) =====
  #    {"data": {"id": "n3", "parent": "group2", "comment": "child"}},
  #    {"data": {"id": "n4", "parent": "group2", "comment": "child"}},
  #    # ===== edges group =====
  #    {"data": {"id": "parent_e1", "source": "group1", "target": "group2", "comment": "parent"}},
  #
  #    # ===== edges within group =====
  #    {"data": {"id": "e1", "source": "n1", "target": "n2", "comment": "child"}},
  #    {"data": {"id": "e2", "source": "n3", "target": "n4", "comment": "child"}},
  #]

  @staticmethod
  def datasource_node(row, node_type, table_type):#, i):
    #if table_type == 'left_table':
    #  x = (i % 5) * 250
    #  y = (i // 5) * 150
    #elif table_type == 'right_table':
    #  x = ((i % 5) + 1) * 250
    #  y = (i // 5) * 150
    data_dict = {
      "id": row[table_type],
      "label": row[table_type],
      "comment": node_type
    }
    #position_dict ={"x": x, "y": y}
    if node_type == 'child':
      data_dict["parent"] = row['parent_id']
    return {"data": data_dict}#, "position": position_dict}

  @staticmethod
  def datasource_edge(row, node_type, edges):
    edge_id = f"{row['left_table']}_{row['right_table']}"
    edge_label = f"{row['left']}{row['operator']}{row['right']}"
    edge_dict = {
      "data": {
        "id": edge_id, 
        "source": row['left_table'], 
        "target": row['right_table'],
        "label": edge_label,
        "comment": node_type
      }
    }
    if not edge_id in edges:
      edges[edge_id] = edge_dict
    else:
      edges[edge_id]['data']['label'] += f"\n{edge_label}"

  #datasourceの結合・リレーション関係のelementを返す
  def datasource_cytoscape_element(self):
    all_data = self.datasource_connection
    if not all_data.empty:
      datasources = all_data['datasource_name'].unique().tolist()
      elements_dict = {}
      for datasource in datasources:
        nodes = {}
        edges = {}
        df = all_data[all_data['datasource_name'] == datasource]
        #parent
        df_parent = df[df['type']=='parent']
        if not df_parent.empty:
          for i, (_, row) in enumerate(df_parent.iterrows()):
            #nodes[row['left_table']] = self.datasource_node(row, 'parent', 'left_table', i)
            #nodes[row['right_table']] = self.datasource_node(row, 'parent', 'right_table', i)
            nodes[row['left_table']] = self.datasource_node(row, 'parent', 'left_table')
            nodes[row['right_table']] = self.datasource_node(row, 'parent', 'right_table')
            self.datasource_edge(row, 'parent', edges)
        #child
        df_child = df[df['type']=='child']
        if not df_child.empty:
          for i, (_, row) in enumerate(df_child.iterrows()):
            #nodes[row['left_table']] = self.datasource_node(row, 'child', 'left_table', i)
            #nodes[row['right_table']] = self.datasource_node(row, 'child', 'right_table', i)
            nodes[row['left_table']] = self.datasource_node(row, 'child', 'left_table')
            nodes[row['right_table']] = self.datasource_node(row, 'child', 'right_table')
            self.datasource_edge(row, 'child', edges)
        elements_dict[datasource] = list(nodes.values()) + list(edges.values())
      stylesheet = self.stylesheet_maps['datasource_info']
      return elements_dict, stylesheet
    else:
      return None, None

  #ダッシュボードに入っているワークシートのマスタを作成する
  def return_worksheet_dashboard_masta(self):
    dfs = self.dfs
    zone_info = self.zone_info
    #worksheet/dashboard(→zoneからどのワークシートが入っているかも作成)/worksheetがどのdatasourceを使用しているか、の一覧を作成
    data_map = {
      'worksheet': ['/worksheets/worksheet/simple-id', ['worksheet_name']],
      'dashboard': ['/dashboards/dashboard/simple-id', ['dashboard_name']],
      'worksheet_datasource': ['/worksheets/worksheet/table/view/datasources/datasource', ['name', 'caption', 'worksheet_name']]
    }
    df_map = {}
    for k, v in data_map.items():
      df = dfs.get(v[0])
      if df is not None:
        df = df[v[1]]
        if k == 'worksheet_datasource':
          df = df[df['name']!='Parameters']
        if k == 'dashboard':
          zone_ws = zone_info.get('zone', None)
          zone_ws = zone_ws[['dashboard_name', 'name']].dropna().drop_duplicates()
          df = pd.merge(df, zone_ws, on='dashboard_name', how='left')
        df_map[k] = self.clean_dataframe(df)
    return df_map

  #actionのsourceを取り出す(source dashboard='~' / exclude-sheet name='~')
  def actions_source(self):
    dfs = self.dfs
    df = dfs.get("/actions/action/source")
    if df is not None:
      df = self.clean_dataframe(df)
      source_exclude = dfs.get("/actions/action/source/exclude-sheet")
      if source_exclude is not None:
        source_exclude= (
          source_exclude
          .groupby("action_name", as_index=False)["name"]
          .apply(lambda x: list(dict.fromkeys(x)))
          .rename(columns={"name": "source_exclude"})
        )
        df = pd.merge(df, source_exclude, on='action_name', how='left')
      return df
    else:
      return None

  def actions_target(self):
    df = self.dfs.get("/actions/action/command/param")
    if df is not None:
      base = (
        df
        .pivot_table(
          index=[
            "action_name",
            "action_caption",
            "command_command"
          ],
          columns="name",
          values="value",
          aggfunc="first"
        ).reset_index()
      )
      base = base.rename(columns={"command_command": "command_type", "exclude": "target_exclude"})
      # exclude列をリスト化
      base["target_exclude"] = base["target_exclude"].fillna("").str.split(",")
      # <<COMMA>> 復元
      base["target_exclude"] = base["target_exclude"].apply(
          lambda lst: [x.replace("<<COMMA>>", ",") for x in lst]
      )
      return base

  ##アクションのマップを作成する
  ##const elements = [
  ##
  ##  // parents
  ##  { data: { id: '概要', label: '概要' }},
  ##  { data: { id: '製品', label: '製品' }},
  ##  { data: { id: '顧客', label: '顧客' }},
  ##
  ##  // Sales
  ##  { data: { id: '概要/売上', parent: '概要', label: '売上' }, position: { x: 0, y: 0 } },
  ##  { data: { id: '製品/売上', parent: '製品', label: '売上' }, position: { x: 0, y: 0 } },
  ##  { data: { id: '顧客/売上', parent: '顧客', label: '売上' }, position: { x: 0, y: 0 } },
  ##
  ##  // Profit
  ##  { data: { id: '概要/利益', parent: '概要', label: '利益' }, position: { x: 100, y: 0 } },
  ##  { data: { id: '製品/利益', parent: '製品', label: '利益' }, position: { x: 100, y: 0 } },
  ##  { data: { id: '顧客/利益', parent: '顧客', label: '利益' }, position: { x: 100, y: 0 } },
  ##
  ##  // worksheet entities→ダッシュボードに入っていないワークシート
  ##  { data: { id: '明細', label: '明細' }, position: { x: 500, y: 500 } },
  ##  { data: { id: '利益明細', label: '利益明細' }, position: { x: 700, y: 500 } },
  ##
  ##  // represents
  ##  { data: { id: 'r1', source: '概要/売上', target: '明細' } },
  ##  { data: { id: 'r2', source: '製品/売上', target: '明細' } },
  ##  { data: { id: 'r3', source: '顧客/売上', target: '明細' } },
  ##  { data: { id: 'r4', source: '概要/利益', target: '利益明細' } },
  ##  { data: { id: 'r5', source: '製品/利益', target: '利益明細' } },
  ##  { data: { id: 'r6', source: '顧客/利益', target: '利益明細' } }
  ##
  ##];

  def actions_overview(self):
    # worksheet master
    ws_masta = self.return_worksheet_dashboard_masta()
    actions_source = self.actions_source()
    actions_target = self.actions_target()
    nodes = []
    edges = []

    #ワークシートのみを取り出す→座標、id作成(worksheet一覧を取り出しparentのあるワークシートを除外していく)
    ws = self.ws_positions(ws_masta['worksheet'])
    ws['worksheet_id'] = ws['worksheet_name'].map(self.make_id)
    ws_not_has_parent = ws['worksheet_name'].dropna().unique()
    #ダッシュボードとワークシート→上記wsビューと結合
    db = ws_masta['dashboard'].rename(columns={'name': 'worksheet_name'})
    db['dashboard_id'] = db['dashboard_name'].map(self.make_id)
    db = pd.merge(db, ws, on='worksheet_name', how='left')
    #データソースとワークシート→上記wsビューと結合
    ds = ws_masta['worksheet_datasource']
    ds['datasource_id'] = ds['name'].map(self.make_id)
    ds = pd.merge(ds, ws, on='worksheet_name', how='left')
    #nodeを追加
    nodes.extend(self.create_nodes(db, {'id': 'dashboard_id', 'name': 'dashboard_name'}))
    nodes.extend(self.create_nodes(ds, {'id': 'datasource_id', 'name': 'name'}))
    #ws_not_has_parentに残ったwsをnodeとして追加
    ws_has_parent = db['worksheet_name'].dropna().unique()
    ws_not_has_parent = ws[~ws['worksheet_name'].isin(ws_has_parent)]
    if len(ws_not_has_parent)>0:
      nodes.extend(self.create_nodes(ws_not_has_parent))
    db_ws = ws_masta['dashboard'].rename(columns={'dashboard_name': 'dashboard', 'name': 'worksheet'}).melt(
      value_vars=["dashboard", "worksheet"],
      var_name="source",
      value_name="name"
    ).drop_duplicates()
    #worksheetのnodeを追加
    source_rename = ['dashboard', 'datasource', 'worksheet']
    actions_source = actions_source.rename(
      columns={
        col: f'source_{col}'
        for col in source_rename
        if col in actions_source.columns
      }
    )
    actions = pd.merge(actions_source, actions_target, on=['action_name', 'action_caption'])
    for col in ['source_exclude', 'target_exclude']:
      if col in actions.columns:
        actions[col] = actions[col].apply(
          lambda x: np.nan if isinstance(x, list) and len(x) == 0 else x
        )
    edges = self.create_edges(ws, db, ds, db_ws, actions)
    stylesheet = self.stylesheet_maps['actions_info']
    return nodes+edges, stylesheet

  #parent idを特定する
  @staticmethod
  def get_id(df, row):
    for col in ('dashboard_id', 'datasource_id'):
      if col in df.columns and pd.notna(row.get(col)):
        return row[col]
    return None

  #wsの一覧から座標を計算する
  @staticmethod
  def ws_positions(df):
    WS_X_START = 80
    WS_Y_START = 80
    WS_GAP_X = 200
    WS_GAP_Y = 120
    df = df.drop_duplicates().reset_index(drop=True)
    WS_COLS = math.ceil(math.sqrt(len(df)))
    df['x'] = WS_X_START + (df.index % WS_COLS) * WS_GAP_X
    df['y'] = WS_Y_START + (df.index // WS_COLS) * WS_GAP_Y
    return df

  #文字列をハッシュ値にしてid化する
  @staticmethod
  def make_id(value):
    return hashlib.sha256(str(value).encode('utf-8')).hexdigest()[:10]

  @staticmethod
  def create_nodes(df, parent_cols=None):
    elements = []
    if parent_cols is not None:
      parent_id = parent_cols['id']
      parent_name = parent_cols['name']
      # parentは重複排除
      parents = df[[parent_id, parent_name]].drop_duplicates()
      for _, row in parents.iterrows():
        elements.append({
          "data": {
            "id": row[parent_id],
            "label": row[parent_name]
          }
        })
      # worksheet view
      for _, row in df.iterrows():
        worksheet_view_id = f'{row[parent_id]}/{row["worksheet_id"]}'
        elements.append({
          "data": {
              "id": worksheet_view_id,
              "parent": row[parent_id],
              "label": row["worksheet_name"],
              "entity": row["worksheet_id"]
          },
          "position": {
              "x": row["x"],
              "y": row["y"]
          }
        })
    else:
      # worksheet view
      for _, row in df.iterrows():
        worksheet_view_id = row["worksheet_id"]
        elements.append({
          "data": {
              "id": worksheet_view_id,
              "label": row["worksheet_name"],
              "entity": row["worksheet_id"]
          },
          "position": {
              "x": row["x"],
              "y": row["y"]
          }
        })
    return elements

  #取り出した値がNaNだった際にNoneに置き換える
  @staticmethod
  def convert_nan(value, menu='string'):
    if isinstance(value, list):
      return value
    if pd.isna(value):
      return None if menu == 'string' else []
    return value

  #source targetの２つのdfからedgeのelementを作成する
  def create_edges(self, ws, db, ds, db_ws, actions):
    elements = []
    for _, row in actions.iterrows():
      source_dashboard = self.convert_nan(row.get('source_dashboard', None))
      source_datasource = self.convert_nan(row.get('source_datasource', None))
      source_worksheet = self.convert_nan(row.get('source_worksheet', None))
      source_exclude = self.convert_nan(row.get('source_exclude', None))
      action_name = row['action_name']
      action_caption = row['action_caption']
      target = self.convert_nan((row.get('target', None)))
      target_exclude = self.convert_nan(row.get('target_exclude', None))
      #source
      if source_worksheet is not None:
        source_list = [f'{self.make_id(source_dashboard)}/{self.make_id(source_worksheet)}']
      else:
        if source_dashboard is not None:
          source_parent = source_dashboard
          source_df = db[db['dashboard_name']==source_parent]
        elif source_datasource is not None:
          source_parent = source_datasource
          source_df = ds[ds['name']==source_parent]
        if source_exclude is None:
          source_list = [self.make_id(source_parent)]
        else:
          source_df = source_df[~source_df['worksheet_name'].isin(source_exclude)]
          id_cols = ['dashboard_id', 'datasource_id']
          existing_cols = [col for col in id_cols if col in source_df.columns]
          if existing_cols:
            source_df['parent_id'] = source_df[existing_cols].bfill(axis=1).iloc[:, 0]
          else:
            source_df['parent_id'] = None
          source_list = (source_df['parent_id'] + '/' + source_df['worksheet_id']).tolist()
      #target
      target_list = []
      target_type = db_ws[db_ws['name']==target]['source'].iloc[0]
      if target_type == 'dashboard':
        target_db = db[db['dashboard_name']==target]
        target_db = target_db[~target_db['worksheet_name'].isin(target_exclude)]
        target_list.extend((target_db['dashboard_id'] + '/' + target_db['worksheet_id']).tolist())
      elif target_type == 'worksheet':
        target_db = db[db['worksheet_name']==target]
        target_ds = ds[ds['worksheet_name']==target]
        if len(target_db)>0:
          target_list.extend((target_db['dashboard_id'] + '/' + target_db['worksheet_id']).tolist())
        if len(target_ds)>0:
          target_list.extend((target_ds['datasource_id'] + '/' + target_ds['worksheet_id']).tolist())
        if not len(target_list)>0:
          target_list.extend((ws['worksheet_id']).tolist())
      edges = [
        {
          "data": {
            "id": f"{action_name}/{source_id}/{target_id}",
            "source": source_id,
            "target": target_id,
            "label": action_caption
          }
        }
        for source_id in source_list
        for target_id in target_list
      ]
      elements.extend(edges)
    return elements

  def dashboard_and_layout_in_zone(self):
    zone_info = self.zone_info['zone']
    if not zone_info.empty:
      db_set = set(zone_info['dashboard_name'].tolist())
      ly_set = set(zone_info['layout_type'].tolist())
      return {
        "dashboard_name": db_set,
        "layout_type": ly_set
      }

  def zone_elements(self, dashboard_name, layout_type):
    zone_info = self.zone_info
    zone = zone_info['zone']
    elements = []
    zone = zone[(zone['dashboard_name']==dashboard_name) & (zone['layout_type']==layout_type)]
    for _, row in zone.iterrows():
      elements.append({
        "data": {
          "id": row["id"],
          "label": row["id"],
          "w": int(row["w"]),
          "h": int(row["h"])
        },
        "position": {
          "x": int(row["x"]),
          "y": int(row["y"])
        }
      })
    stylesheet = self.stylesheet_maps['zones_info']
    return elements, stylesheet