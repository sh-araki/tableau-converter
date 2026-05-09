import pandas as pd
from pathlib import Path
import json
import logging

class PrepNodesView:
  def __init__(self, data_dict, run_id):
    self.settings = data_dict['settings']
    self.parameters = data_dict['parameters']
    self.initial_nodes = data_dict['initialNodes']
    self.initial_node_detail = data_dict['initial_node_detail']
    self.node_masta = data_dict['node_masta']
    self.nextnode_info = data_dict['nextnode_info']
    self.connections = data_dict['connections']
    self.action_detail = data_dict['action_detail']
    self.fields_info = data_dict['fields_info']
    base_dir = Path(__file__).resolve().parent
    json_path = base_dir / "prep_maps.json"
    with open(json_path, encoding="utf-8") as f:
      self.convert_maps = json.load(f)
    stylesheet_path = base_dir / "stylesheet.json"
    with open(stylesheet_path, encoding="utf-8") as f:
      self.stylesheet_maps = json.load(f)
    self.logger = logging.getLogger(self.__class__.__name__)
    self.run_id = run_id

  #cytoscapeで使用可能なdictに変換する
  def cytoscape_elements(self):
    self.logger.info(f"[{self.run_id}] convert cytoscape elements")
    node_masta = self.node_masta
    settings = self.settings
    edges = self.nextnode_info
    node_masta = node_masta[node_masta['node_basetype']=='node']
    settings = settings.merge(self.clean_dataframe(node_masta), on='node_baseid', how='inner')
    settings_json = settings.apply(lambda row: {
        "data": {
            "id": row["node_baseid"],
            "label": row["name"]
        },
        "position": {
            "x": row["x"] * 200,
            "y": row["y"] * 100
        },
        "style": {
            "background-color": row["color"]
        }
    }, axis=1).tolist()
    #edgeの一覧を作成する
    edges_json = edges.apply(lambda row: {
      "data": {
          "source": row["node_baseid"],
          "target": row["nextNodeId"],
          "label": row["nextNamespace"][0:10]
      }
    }, axis=1).tolist()
    elements = settings_json + edges_json
    stylesheet = self.stylesheet_maps['stylesheet']
    return elements, stylesheet

  #nodeの接続先、座標、色等を結合、一つのビューにする
  def node_baseinfo(self):
    self.logger.info(f"[{self.run_id}] merge node baseinfo")
    masta = self.node_masta
    df = (
      self.settings
      .merge(masta[masta['node_basetype']=='node'], on="node_baseid", how="left")
      .merge(self.initial_node_detail, on="node_baseid", how="left")
      .merge(self.connections, on="connectionId", how="left")
    )
    df["initialNodes"] = df["node_baseid"].isin(self.initial_nodes["node_baseid"])
    df = self.clean_dataframe(df)
    self.logger.debug(f"[{self.run_id}] merged node baseinfo df: {self.clean_dataframe(df)}")
    return df

  #空列を削除、id、index列も削除
  @staticmethod
  def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """共通のDataFrame整形"""
    df = df.dropna(axis=1, how="all")
    return df

  #node_baseidとnextNodeIdを入れ替えて次のnode用のnode_fieldsに投入する
  @staticmethod
  def replace_nodeid_nextnodeid(node_fields, nextnode_info):
    df_dict = {}
    column_dict = {'nextNodeId': 'node_baseid', 'nextNamespace': 'namespace'}
    node_fields = node_fields.drop(columns=['namespace'], errors="ignore")
    df = pd.merge(node_fields, nextnode_info, on='node_baseid', how='left')
    # 元のnodeの情報を削除
    df = df.drop(columns = list(column_dict.values()))
    # 次のnodeの情報に書き換える
    df = df.rename(columns = column_dict)
    # 必要カラムのみを取得する
    df = df[list(column_dict.values()) + ['name']]
    # 次の各nodeごとのdictを作成
    for node_baseid in set(df['node_baseid'].to_list()):
      df_dict[node_baseid] = df[df['node_baseid']==node_baseid]
    return df_dict

  #actionの取り出し
  def return_node_action(self, node_baseid, basetype):
    node_actions = (
      self.node_masta
      .query(f"node_baseid == '{node_baseid}'")
      .query(f"node_basetype == '{basetype}'")
    )
    return node_actions

  #各nodeの最終fieldsをdictに入れて返す
  def node_fields_and_uml_results(self):
    node_masta = self.node_masta
    node_fields_dict= {}
    node_uml_dict = {}
    for _, row in self.node_baseinfo().iterrows():
      node_baseid = row['node_baseid']
      node_type = row['nodeType']
      node_actions = node_masta[node_masta['node_baseid']==node_baseid]
      self.logger.info(f"[{self.run_id}] field convert start(node_baseid: {node_baseid}, node_type: {node_type})")
      convert_map = self.convert_maps[node_type]
      annotation_option = convert_map.get('annotations', None)
      temp_fields_dict = {}
      info_dict = {'name': row['name'], 'nodeType': node_type}
      if annotation_option is None:
        self.logger.info(f"[{self.run_id}] get node fields")
        node_fields = node_fields_dict[node_baseid]
        self.logger.info(f"[{self.run_id}] get node loom_action")
        node_action = node_actions[node_actions['node_basetype']=='loom_action']
        self.logger.info(f"[{self.run_id}] convert node fields")
        node_fields, node_uml = self.convert_fields_and_uml(node_baseid, node_fields, node_action)
        info_dict['actions'] = node_uml
      elif annotation_option is False:
        self.logger.info(f"[{self.run_id}] get node fields")
        node_fields = (
          self.fields_info
          .query(f"node_baseid == '{node_baseid}'")
        )
        self.logger.info(f"[{self.run_id}] get node action")
        node_action = node_actions[node_actions['node_basetype']=='action']
        self.logger.info(f"[{self.run_id}] convert node fields")
        node_fields, node_uml = self.convert_fields_and_uml(node_baseid, node_fields, node_action)
        info_dict['actions'] = node_uml
      elif annotation_option is True:
        self.logger.info(f"[{self.run_id}] get node fields")
        node_fields = node_fields_dict[node_baseid]
        self.logger.info(f"[{self.run_id}] get node before action")
        before_annotations = node_actions[node_actions['node_basetype']=='before']
        self.logger.info(f"[{self.run_id}] convert node fields(before)")
        node_fields, node_before_uml = self.convert_fields_and_uml(node_baseid, node_fields, before_annotations)
        info_dict['before'] = node_before_uml
        self.logger.info(f"[{self.run_id}] get node action")
        node_action = node_actions[node_actions['node_basetype']=='action']
        self.logger.info(f"[{self.run_id}] convert node fields(action)")
        node_fields, node_actions_uml = self.convert_fields_and_uml(node_baseid, node_fields, node_action)
        info_dict['actions'] = node_actions_uml
        self.logger.info(f"[{self.run_id}] get node after action")
        after_annotations = node_actions[node_actions['node_basetype']=='after']
        self.logger.info(f"[{self.run_id}] convert node fields(after)")
        node_fields, node_after_uml = self.convert_fields_and_uml(node_baseid, node_fields, after_annotations)
        info_dict['after'] = node_after_uml
      node_uml_dict[node_baseid] = info_dict
      temp_fields_dict[node_baseid] = node_fields
      self.logger.info(f"[{self.run_id}] replace next node id")
      nextnode_fields_dict = self.replace_nodeid_nextnodeid(node_fields, self.nextnode_info)
      temp_fields_dict.update(nextnode_fields_dict)
      for k, df in temp_fields_dict.items():
        if k in node_fields_dict:
          node_fields_dict[k] = pd.concat([node_fields_dict[k], df], ignore_index=True)
        else:
          node_fields_dict[k] = df
    node_fields_list = []
    for _, df in node_fields_dict.items():
      node_fields_list.append(df)
    node_fields_df = pd.concat(node_fields_list)
    return node_fields_df, json.dumps(node_uml_dict, ensure_ascii=False, indent=2)

  #各annotationやloom container、initialNodeのアクション上の各ステップ内で行われる列の削除等を処理
  def convert_fields_and_uml(self, node_baseid, node_fields, node_actions):
    self.node_masta_indexed = self.node_masta.set_index(['node_baseid', 'id'])
    self.action_detail_indexed = self.action_detail.set_index(['node_baseid', 'id'])
    action_uml = {}
    if not node_actions.empty:
      convert_maps = self.convert_maps
      for node_action in node_actions.itertuples(index=False):
        action_id = node_action.id
        action_type = node_action.nodeType
        action_namespace = getattr(node_action, "namespace", None)
        convert_map = convert_maps[action_type]
        info = self.node_masta_indexed.loc[(node_baseid, action_id)]
        info_dict = info.to_dict()
        info_dict = {k: info_dict[k] for k in ["nodeType", "name", "description"] if k in info_dict}
        detail = self.action_detail_indexed.loc[(node_baseid, action_id)]
        temp_detail = detail[convert_map['column']]

        match convert_map['type']:
          case 'remove_list':
            field_target = temp_detail[convert_map['field_target']]
            if pd.isna(action_namespace):
              node_fields = node_fields[~node_fields['name'].isin(field_target)]
            else:
              node_fields[
                (node_fields['namespace'] != action_namespace) |
                (~node_fields['name'].isin(field_target))
              ]
              info_dict['namespace'] = action_namespace
            info_dict['field_target'] = field_target
          case 'add_column':
            field_target = temp_detail[convert_map['field_target']]
            if pd.isna(action_namespace):
              action_namespace = 'Default'
            if field_target not in set(node_fields["name"]):
              new_row = {
                'node_baseid': node_baseid,
                'namespace': action_namespace,
                'name': field_target
              }
              node_fields = pd.concat([node_fields, pd.DataFrame([new_row])], ignore_index=True)
            info_dict['namespace'] = action_namespace
            info_dict[field_target] = detail.expression
          case 'keep_list':
            field_target = temp_detail[convert_map['field_target']]
            if pd.isna(action_namespace):
              node_fields = node_fields[node_fields['name'].isin(field_target)]
            else:
              node_fields[
                (node_fields['namespace'] == action_namespace) |
                (node_fields['name'].isin(field_target))
              ]
              info_dict['namespace'] = action_namespace
            info_dict['field_target'] = field_target
          case 'merge_column':
            field_target = temp_detail[convert_map['field_target']['from']]
            leave = temp_detail[convert_map['field_target']['to']]
            info_dict['from'] = field_target
            info_dict['to'] = leave
            field_target.remove(leave)
            if pd.isna(action_namespace):
              node_fields = node_fields[~node_fields['name'].isin(field_target)]
            else:
              node_fields[
                (node_fields['namespace'] != action_namespace) |
                (~node_fields['name'].isin(field_target))
              ]
          case 'rename_column':
            mapping = {
              temp_detail[convert_map["field_target"]["from"]]:
              temp_detail[convert_map["field_target"]["to"]]
            }
            if pd.isna(action_namespace):
              node_fields['name'] = node_fields['name'].replace(mapping)
            else:
              mask = (
                (node_fields['namespace'] == action_namespace) &
                (node_fields['name'].isin(mapping.keys()))
              )
              node_fields.loc[mask, 'name'] = node_fields.loc[mask, 'name'].replace(mapping)
              info_dict['namespace'] = action_namespace
            info_dict = dict(**info_dict, **mapping)
          case 'bulk_rename':
            info_dict['actions_detail'] = temp_detail.to_dict()
            temp_detail = self.apply_rule(temp_detail, convert_map['field_target'])[0]
            field_target = temp_detail['included_columns']
            leave = temp_detail['exempted_columns'].iloc[0]
            field_target = [x for x in field_target if x not in leave]
            operation_type = temp_detail['operation_type'].iloc[0]
            match operation_type:
              case 'replaceColumnAllSubStringOperation':
                existing_subString = temp_detail['existing_subString']
                new_substring = temp_detail['new_substring']
                node_fields["name"] = node_fields["name"].apply(
                    lambda x: x.replace(existing_subString, new_substring) if x in field_target else x
                )
              case 'addColumnPrefixOperation':
                column_name_prefix = temp_detail['column_name_prefix']
                node_fields["name"] = node_fields["name"].apply(
                  lambda x: column_name_prefix + x if x in field_target else x
                )
              case 'addColumnSuffixOperation':
                column_name_suffix = temp_detail['column_name_suffix']
                node_fields["name"] = node_fields["name"].apply(
                  lambda x: x + column_name_suffix if x in field_target else x
                )
          case 'union':
            node_fields = node_fields.drop_duplicates()
            new_df = pd.DataFrame([{
              'name': 'Table Names',
              'node_baseid': node_baseid,
              'namespace': 'Default'
            }])
            node_fields = pd.concat([node_fields, new_df])
            info_dict['new_column'] = 'Table Names'
          case 'join':
            info_dict['conditions'] = detail.conditions
            try:
              base = node_fields['name'].str.split('-', n=1).str[0]
              count = node_fields.groupby(base).cumcount()
              node_fields['name'] = base.where(count == 0, base + '-' + count.astype(str))
            except:
              continue
          case 'pivot':
            info_dict['actions_detail'] = temp_detail.to_dict()
            field_target = [temp_detail[convert_map['field_target']['from']]]
            node_fields = node_fields[~node_fields['name'].isin(field_target)]
            leave = temp_detail[convert_map['field_target']['to']]
            leave = (
              pd.DataFrame(leave)
              .assign(node_baseid=node_baseid, namespace='Default')
              .rename(columns={'newColumnName': 'name'})
            )
            node_fields = pd.concat([node_fields, leave])
          case 'newrows':
            info_dict['actions_detail'] = temp_detail.to_dict()
            field_target = self.apply_rule(temp_detail, convert_map['field_target'])[0]
            new_df = pd.DataFrame({
              'name': field_target['name'],
              'node_baseid': node_baseid,
              'namespace': 'Default'
            }).dropna().drop_duplicates()
            missing_df = new_df[~new_df['name'].isin(node_fields['name'])]
            if not missing_df.empty:
              node_fields = pd.concat([node_fields, missing_df], ignore_index=True)
          case 'unpivot':
            info_dict['actions_detail'] = temp_detail.to_dict()
            temp_detail = self.apply_rule(temp_detail, convert_map['field_target'])
            literal = temp_detail[0]
            unpivot_menu = temp_detail[1]
            #unpivot対象の新規カラムを追加
            new_name =  pd.concat([literal, unpivot_menu])["name"].dropna().to_list()
            for val in new_name:
              if val not in set(node_fields["name"]):
                new_row = {
                  'node_baseid': node_baseid,
                  'namespace': 'Default',
                  'name': val
                }
                node_fields = pd.concat([node_fields, pd.DataFrame([new_row])], ignore_index=True)
            #wildカードunpivot対象かカスタムピボット対象のカラムを削除
            for _, row in unpivot_menu.iterrows():
              if row['bindings_type'] == 'manual':
                node_fields = node_fields[~node_fields['name'].isin(row['manual_bindings'])]
              elif row['bindings_type'] == 'wildcard':
                additional_columns = row['additional_columns']
                if isinstance(additional_columns, list) and additional_columns: 
                  node_fields = node_fields[~node_fields['name'].isin(additional_columns)]
                else:
                  wildcard_expression = row['wildcard_expression']
                  wildcard_type = row['wildcard_type']
                  #含む=含むものは削除する
                  if wildcard_type == 'Contains':
                    node_fields = node_fields[~node_fields['name'].str.contains(wildcard_expression)]
          case 'aggregate':
            df = pd.DataFrame()
            info_dict['actions_detail'] = temp_detail.to_dict()
            for col in convert_map['column']:
              df_temp = pd.DataFrame(temp_detail[col])
              df = pd.concat([df, df_temp])
            node_fields = pd.DataFrame({
              'name': df['columnName'],
              'node_baseid': node_baseid,
              'namespace': 'Default'
            })
          case _:
            info_dict['actions_detail'] = temp_detail.to_dict()
        action_uml[action_id] = info_dict
    return node_fields, action_uml

  def apply_rule(self, df, rules):
    # --- 型正規化 ---
    if isinstance(df, dict):
      df = pd.DataFrame([df])
    elif isinstance(df, pd.Series):
      df = df.to_frame().T
    result_cols = {}
    explode_results = []
    for new_col, rule in rules.items():
      col = rule["column"]
      path = rule.get("path", [])
      mode = rule.get("mode", "scalar")
      # --- 共通処理（apply回数削減） ---
      extracted = df[col].map(lambda x: self.extract_by_path(x, path))
      if mode == "scalar":
        result_cols[new_col] = extracted
      elif mode == "explode":
        df_explode = pd.DataFrame(extracted.iloc[0])
        for child_new_col, child_rule in rule.get("child_columns", {}).items():
            child_col = child_rule["column"]
            child_path = child_rule.get("path", [])
            df_explode[child_new_col] = df_explode[child_col].map(
                lambda x: self.extract_by_path(x, child_path)
            )
        explode_results.append(
            df_explode[list(rule.get("child_columns", {}).keys())]
        )
        result_cols[new_col] = extracted
    df_main = pd.DataFrame(result_cols)
    df_explode = (
        pd.concat(explode_results, ignore_index=True)
        if explode_results else pd.DataFrame()
    )
    return df_main, df_explode


  @staticmethod
  def extract_by_path(d, path):
    for key in path:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
    return d