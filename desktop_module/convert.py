import io
import base64
from pathlib import Path
import pandas as pd
import numpy as np
import json
from PIL import Image, ImageColor, ImageDraw
import base64
from io import BytesIO
from lxml import etree as ET
from collections import defaultdict
import logging


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
  
  #datasourceの結合・リレーション関係のelementを返す
  def datasource_cytoscape_element(self):
    datasource_connection = self.datasource_connection
    relation_df = datasource_connection['relationship']
    join_df = datasource_connection['join']
    #same as action_element
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
    elements = []
    if relation_df is not None:
      for _, row in relation_df.iterrows():
        left_relation = row['left'].replace('[','').replace(']','')
        right_relation = row['right'].replace('[','').replace(']','')
        group_left = {"data": {"id": left_relation, "label": left_relation.split('_')[0], "comment": "parent"}}
        elements.append(group_left)
        group_right = {"data": {"id": right_relation, "label": right_relation.split('_')[0], "comment": "parent"}}
        elements.append(group_right)
        group_edge = {"data": {"id": row['key'], "source": left_relation, "target": right_relation, "label": row['key'], "comment": "parent"}}
        elements.append(group_edge)
    if join_df is not None:
      for _, row in join_df.iterrows():
        left_table = row['left'].split('[')[1].split(']')[0]
        right_table = row['right'].split('[')[1].split(']')[0]
        join_left = {"data": {"id": left_table, "parent": row['object-id'], "label": left_table, "comment": "child"}}
        elements.append(join_left)
        join_right = {"data": {"id": right_table, "parent": row['object-id'], "label": right_table, "comment": "child"}}
        elements.append(join_right)
        join_key = f'{row['join']}・{row['key']}'
        join_edge = {"data": {"id": join_key, "source": left_table, "target": right_table, "label": join_key, "comment": "child"}}
        elements.append(join_edge)
    stylesheet = self.stylesheet_maps['datasource_info']
    return elements, stylesheet
  
  def datasource_overview(self):
    dfs = self.dfs
    metadata = dfs.get('/datasources/datasource/connection/metadata-records/metadata-record')
    calclation = dfs.get('/datasources/datasource/column/calculation')
    connection = dfs.get('/datasources/datasource/connection/named-connections/named-connection/connection')
    datasource_column = dfs.get('/datasources/datasource/column')
    #connection
    connection = connection[["datasource_name", "named-connection_caption", "filename", "class"]]
    conenction = connection.rename(columns={"named-connection_caption": "connect_caption", "class": "source_connection_class"})
    #metadata
    metadata = pd.merge(metadata, connection, on="datasource_name", how='left')
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
    metadata_rename = datasource_column[['datasource_name', 'name', 'caption']]
    metadata_rename = metadata_rename.rename(columns={'caption': 'rename'})
    metadata = pd.merge(pivot, metadata_rename, on=['datasource_name', 'name'], how='left').set_index(keys='ordinal')
    #calclation
    if not calclation.empty:
      calclation = calclation[["datasource_name", "datasource_caption", "column_name", "column_caption", "column_datatype", "formula"]]
      calclation = calclation.rename(columns={'column_name': 'name', "column_caption": 'caption', "column_datatype": 'type'})
      calclation = pd.merge(calclation, connection, on="datasource_name", how='left')
      #datasource_column
      datasource_column = datasource_column[["datasource_name", "name", "role"]]
      calclation = pd.merge(calclation, connection, on="datasource_name", how='left')
      calclation = pd.merge(calclation, datasource_column, on=["datasource_name", 'name'], how='left')
    else:
      calclation = pd.DataFrame()
    df = pd.concat([metadata, calclation])
    df = df.reset_index(drop=True)
    if 'object-id' in df.columns:
      df_datasource = df[['datasource_name', 'datasource_caption', 'object-id', 'parent-name']].drop_duplicates()
      df_datasource = df_datasource[~df_datasource['object-id'].isnull()].reset_index(drop=True)
    else:
      df_datasource = df[['datasource_name', 'datasource_caption', 'parent-name']].drop_duplicates()
    return df_datasource, df

  def return_worksheet_dashboard_masta(self):
    dfs = self.dfs
    zone_info = self.zone_info
    #worksheet and worksheet in dashboard
    ws = dfs.get('/worksheets/worksheet/simple-id', None)
    if not ws.empty:
      ws = ws[['worksheet_name']].rename(columns={'worksheet_name': 'name'})
    else:
      ws = pd.DataFrame()
   
    zone_ws = zone_info.get('zone', None)
    if not zone_ws.empty:
      zone_ws = zone_ws[['dashboard_name', 'name']].drop_duplicates()
      zone_ws = zone_ws[~zone_ws['name'].isna()]
    else:
      zone_ws = pd.DataFrame()
    df_ws = pd.concat([ws, zone_ws])
    #dashbaord masta
    if not df_ws.empty:
      df_dashboard = dfs.get('/dashboards/dashboard/simple-id')[['dashboard_name']]
      df_ws = pd.merge(df_ws, df_dashboard, on='dashboard_name', how='outer')
      #worksheet's datasource
      ws_datasource = dfs.get('/worksheets/worksheet/table/view/datasources/datasource')[['name', 'worksheet_name']].rename(columns={'name': 'datasource_name', 'worksheet_name': 'name'})
      df_ws = pd.merge(df_ws, ws_datasource, how='outer')
      return df_ws

  def desktop_actions(self):
    dfs = self.dfs
    df = dfs["/actions/action/command/param"]
    # -------------------------
    # pivot用
    # -------------------------
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
        )
        .reset_index()
    )
    # -------------------------
    # rename
    # -------------------------
    base = base.rename(columns={
        "command_command": "command_type"
    })
    # -------------------------
    # exclude explode
    # -------------------------
    base["exclude"] = (
        base["exclude"]
        .fillna("")
        .str.split(",")
    )
    result = (
        base
        .explode("exclude")
    )
    # -------------------------
    # <<COMMA>> 復元
    # -------------------------
    result["exclude"] = (
      result["exclude"]
      .str.replace("<<COMMA>>", ",", regex=False)
    )
    return result

  def actions_overview(self):
    dfs = self.dfs
    # worksheet master
    ws_masta = self.return_worksheet_dashboard_masta()
    ws_masta = ws_masta[ws_masta["datasource_name"] != "Parameters"]
    ws_list = set(ws_masta["name"].dropna().tolist())
    target_masta = ws_masta[["dashboard_name", "name"]]
    df_dict = {}
    # ======================
    # TARGET PROCESS
    # ======================
    target_df_list_sql = []
    target_df_list_sql.append(self.desktop_actions())

    if not target_df_list_sql:
      df_dict["target"] = pd.DataFrame()
    else:
      target_df = pd.concat(target_df_list_sql, ignore_index=True)
      target_rows = []
      for action_name, temp_df in target_df.groupby("action_name"):
        temp_df = temp_df.dropna(axis=1, how="all")
        if "exclude" in temp_df.columns:
          exclude_df = (
            temp_df[temp_df["exclude"].notna()]
            .rename(columns={"target": "dashboard_name", "exclude": "name"})
            [["dashboard_name", "name"]]
          )
          if not exclude_df.empty:
            include_df = target_masta[
              (target_masta["dashboard_name"].isin(exclude_df["dashboard_name"])) &
              (~target_masta["name"].isin(exclude_df["name"]))
            ].copy()
            include_df["action_name"] = action_name
            target_rows.append(include_df)
          include_targets = temp_df[temp_df["exclude"].isna()]["target"].dropna().tolist()
        else:
          include_targets = temp_df["target"].dropna().tolist()
        include_df_list = []
        for include_target in include_targets:
          if include_target in ws_list:
            include_df_list.append(
              target_masta[
                (target_masta["name"] == include_target) &
                (target_masta["dashboard_name"].isna())
              ]
            )
          else:
            include_df_list.append(
              target_masta[target_masta["dashboard_name"] == include_target]
            )
        if include_df_list:
          include_df = pd.concat(include_df_list).copy()
          include_df["action_name"] = action_name
          target_rows.append(include_df)
      df_dict["target"] = (
        pd.concat(target_rows, ignore_index=True)
        .drop_duplicates()
      )
    # ======================
    # SOURCE PROCESS
    # ======================
    source_df = dfs.get("/actions/action/source")
    source_exclude_df = dfs.get("/actions/action/source/exclude-sheet")
    source_rows = []
    for _, row in source_df.iterrows():
      if row["type"] == "datasource":
        temp_df = ws_masta[
          (ws_masta["datasource_name"] == row["datasource"]) &
          (ws_masta["dashboard_name"].isna())
        ].copy()
      else:
        if row["worksheet"] is None:
          temp_df = ws_masta[
            ws_masta["dashboard_name"] == row["dashboard"]
          ].copy()
          temp_df["datasource_name"] = None
        else:
          if row['dashboard'] is not None:
            temp_df = ws_masta[
              (ws_masta["dashboard_name"] == row["dashboard"]) &
              (ws_masta["name"]==row['worksheet'])
            ].copy()
            temp_df["datasource_name"] = None
          else:
            temp_df = ws_masta[
              (ws_masta["dashboard_name"].isna()) &
              (ws_masta["name"] == row["worksheet"])
            ].copy()
            temp_df["dashboard_name"] = None
            temp_df["datasource_name"] = None
      temp_df["action_name"] = row["action_name"]
      temp_df["action_caption"] = row["action_caption"]
      exclude_names = source_exclude_df.loc[
        source_exclude_df["action_name"] == row["action_name"],
        "name"
      ]
      if not exclude_names.empty:
        temp_df = temp_df[~temp_df["name"].isin(exclude_names)]
      source_rows.append(temp_df)
    df_dict["source"] = (
      pd.concat(source_rows, ignore_index=True)
      if source_rows else pd.DataFrame()
    )
    elements = self.action_elements(df_dict["source"], df_dict["target"])
    stylesheet = self.stylesheet_maps['actions_info']
    return elements, stylesheet

  @staticmethod
  def action_elements(df_source, df_target):
      #same as datasource_element
      df_source["parent"] = df_source["datasource_name"].combine_first(df_source["dashboard_name"])
      elements = []
      node_ids = set()

      NONE_PARENT = "None・Parent・Node"

      def add_node(node_id, label, parent=None, comment=None):
          if node_id not in node_ids:
              data = {"id": node_id, "label": label}
              if parent:
                  data["parent"] = parent
              if comment:
                  data["comment"] = comment
              elements.append({"data": data})
              node_ids.add(node_id)

      def add_edge(src, tgt, label, comment=None):
          data = {"source": src, "target": tgt, "label": label}
          if comment:
              data["comment"] = comment
          elements.append({"data": data})

      # =====================
      # Parent nodes
      # =====================
      parents = pd.concat([
          df_source["parent"],
          df_target["dashboard_name"],
      ]).dropna().unique()

      for p in parents:
          add_node(p, p, comment="parent")

      add_node(NONE_PARENT, NONE_PARENT, comment="parent")

      # =====================
      # Nodes（name 単位）
      # =====================
      name_parent = {}
      # source 側（優先）
      for _, row in df_source.iterrows():
          name = row["name"]
          if pd.notna(row.get("parent")):
              parent = row["parent"]
          else:
              parent = NONE_PARENT
          name_parent.setdefault(name, parent)

      # target 側（source に無い name のみ）
      for _, row in df_target.iterrows():
          name = row["name"]

          if name not in name_parent:
              if pd.notna(row.get("dashboard_name")):
                  parent = row["dashboard_name"]
              else:
                  parent = NONE_PARENT
              name_parent[name] = parent

      # ノード生成
      for name, parent in name_parent.items():
          add_node(name, name, parent=parent, comment="child")

      # =====================
      # Edges（action_name 単位）
      # =====================
      for _, src in df_source.iterrows():
          action = src["action_name"]
          caption = src["action_caption"]
          src_name = src["name"]

          targets = df_target[df_target["action_name"] == action]
          for _, tgt in targets.iterrows():
              tgt_name = tgt["name"]
              add_edge(src_name, tgt_name, caption, comment="child")
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
