import json
import logging
from pathlib import Path

import pandas as pd


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

  def cytoscape_elements(self):
    self.logger.info(f"[{self.run_id}] convert cytoscape elements")
    node_masta = self.node_masta
    settings = self.settings
    edges = self.nextnode_info
    node_masta = node_masta[node_masta['node_basetype'] == 'node']
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

  def node_baseinfo(self):
    self.logger.info(f"[{self.run_id}] merge node baseinfo")
    masta = self.node_masta
    df = (
      self.settings
      .merge(masta[masta['node_basetype'] == 'node'], on="node_baseid", how="left")
      .merge(self.initial_node_detail, on="node_baseid", how="left")
      .merge(self.connections, on="connectionId", how="left")
    )
    df["initialNodes"] = df["node_baseid"].isin(self.initial_nodes["node_baseid"])
    df = self.clean_dataframe(df)
    self.logger.debug("[%s] merged node baseinfo df: %s", self.run_id, df)
    return df

  @staticmethod
  def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(axis=1, how="all")

  @staticmethod
  def replace_nodeid_nextnodeid(node_fields, nextnode_info):
    df_dict = {}
    column_dict = {'nextNodeId': 'node_baseid', 'nextNamespace': 'namespace'}
    node_fields = node_fields.drop(columns=['namespace'], errors="ignore")
    df = pd.merge(node_fields, nextnode_info, on='node_baseid', how='left')
    df = df.drop(columns=list(column_dict.values()), errors='ignore')
    df = df.rename(columns=column_dict)
    df = df[list(column_dict.values()) + ['name']]
    for node_baseid in set(df['node_baseid'].to_list()):
      df_dict[node_baseid] = df[df['node_baseid'] == node_baseid]
    return df_dict

  def return_node_action(self, node_baseid, basetype):
    return self.node_masta[
      self.node_masta['node_baseid'].eq(node_baseid)
      & self.node_masta['node_basetype'].eq(basetype)
    ]

  def node_fields_and_uml_results(self):
    node_masta = self.node_masta
    initial_nodes = self.initial_nodes
    node_fields_dict = {}
    node_uml_dict = {}
    for _, row in self.node_baseinfo().iterrows():
      node_baseid = row['node_baseid']
      node_type = row['nodeType']
      node_actions = node_masta[node_masta['node_baseid'] == node_baseid]
      self.logger.info(f"[{self.run_id}] field convert start(node_baseid: {node_baseid}, node_type: {node_type})")
      convert_map = self.convert_maps[node_type]
      annotation_option = convert_map.get('annotations', None)
      temp_fields_dict = {}
      info_dict = {'name': row['name'], 'nodeType': node_type}
      if annotation_option is None:
        self.logger.info(f"[{self.run_id}] get node fields")
        node_fields = node_fields_dict[node_baseid]
        self.logger.info(f"[{self.run_id}] get node loom_action")
        node_action = node_actions[node_actions['node_basetype'] == 'loom_action']
        self.logger.info(f"[{self.run_id}] convert node fields")
        node_fields, node_uml = self.convert_fields_and_uml(node_baseid, node_fields, node_action)
        info_dict['actions'] = node_uml
      elif annotation_option is False:
        self.logger.info(f"[{self.run_id}] get node fields")
        node_fields = self.fields_info[
          self.fields_info['node_baseid'].eq(node_baseid)
        ]
        exists = initial_nodes['node_baseid'].eq(node_baseid).any()
        if exists:
          self.logger.info(f"[{self.run_id}] write fields to uml")
          info_dict['fields'] = node_fields.drop(columns='node_baseid').to_dict(orient='records')
        self.logger.info(f"[{self.run_id}] get node action")
        node_action = node_actions[node_actions['node_basetype'] == 'action']
        self.logger.info(f"[{self.run_id}] convert node fields")
        node_fields, node_uml = self.convert_fields_and_uml(node_baseid, node_fields, node_action)
        info_dict['actions'] = node_uml
      elif annotation_option is True:
        self.logger.info(f"[{self.run_id}] get node fields")
        node_fields = node_fields_dict[node_baseid]
        self.logger.info(f"[{self.run_id}] get node before action")
        before_annotations = node_actions[node_actions['node_basetype'] == 'before']
        self.logger.info(f"[{self.run_id}] convert node fields(before)")
        node_fields, node_before_uml = self.convert_fields_and_uml(node_baseid, node_fields, before_annotations)
        info_dict['before'] = node_before_uml
        self.logger.info(f"[{self.run_id}] get node action")
        node_action = node_actions[node_actions['node_basetype'] == 'action']
        self.logger.info(f"[{self.run_id}] convert node fields(action)")
        node_fields, node_actions_uml = self.convert_fields_and_uml(node_baseid, node_fields, node_action)
        info_dict['actions'] = node_actions_uml
        self.logger.info(f"[{self.run_id}] get node after action")
        after_annotations = node_actions[node_actions['node_basetype'] == 'after']
        self.logger.info(f"[{self.run_id}] convert node fields(after)")
        node_fields, node_after_uml = self.convert_fields_and_uml(node_baseid, node_fields, after_annotations)
        info_dict['after'] = node_after_uml
      node_uml_dict[node_baseid] = info_dict
      node_fields_dict[node_baseid] = node_fields.reset_index(drop=True)
      self.logger.info(f"[{self.run_id}] replace next node id")
      nextnode_fields_dict = self.replace_nodeid_nextnodeid(node_fields, self.nextnode_info)
      for key, df in nextnode_fields_dict.items():
        if key in node_fields_dict:
          node_fields_dict[key] = pd.concat([node_fields_dict[key], df], ignore_index=True)
        else:
          node_fields_dict[key] = df
    node_fields_list = []
    for _, df in node_fields_dict.items():
      node_fields_list.append(df)
    node_fields_df = (
      pd.concat(node_fields_list, ignore_index=True)
      if node_fields_list else pd.DataFrame()
    )
    return node_fields_df, json.dumps(node_uml_dict, ensure_ascii=False, indent=2)

  def convert_fields_and_uml(self, node_baseid, node_fields, node_actions):
    self._index_action_sources()
    action_uml = {}
    if not node_actions.empty:
      for node_action in node_actions.itertuples(index=False):
        action_id = node_action.id
        node_fields, info_dict = self._apply_action(node_baseid, node_fields, node_action)
        action_uml[action_id] = info_dict
    return node_fields, action_uml

  def _index_action_sources(self):
    if hasattr(self, 'node_masta_indexed') and hasattr(self, 'action_detail_indexed'):
      return
    self.node_masta_indexed = self.node_masta.set_index(['node_baseid', 'id'])
    self.action_detail_indexed = self.action_detail.set_index(['node_baseid', 'id'])

  def _apply_action(self, node_baseid, node_fields, node_action):
    action_id = node_action.id
    action_type = node_action.nodeType
    action_namespace = getattr(node_action, "namespace", None)
    convert_map = self.convert_maps[action_type]
    info = self.node_masta_indexed.loc[(node_baseid, action_id)]
    detail = self.action_detail_indexed.loc[(node_baseid, action_id)]
    temp_detail = detail[convert_map['column']]
    info_dict = {
      key: value
      for key, value in info.to_dict().items()
      if key in ["nodeType", "name", "description"]
    }
    handler = self._action_handler(convert_map['type'])
    return handler(node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict)

  def _action_handler(self, action_type):
    handlers = {
      'remove_list': self._handle_remove_list,
      'add_column': self._handle_add_column,
      'keep_list': self._handle_keep_list,
      'merge_column': self._handle_merge_column,
      'rename_column': self._handle_rename_column,
      'bulk_rename': self._handle_bulk_rename,
      'union': self._handle_union,
      'join': self._handle_join,
      'pivot': self._handle_pivot,
      'newrows': self._handle_newrows,
      'unpivot': self._handle_unpivot,
      'aggregate': self._handle_aggregate,
    }
    return handlers.get(action_type, self._handle_default)

  @staticmethod
  def _remove_matching_fields(node_fields, field_target, action_namespace):
    if pd.isna(action_namespace):
      return node_fields[~node_fields['name'].isin(field_target)]
    return node_fields[
      (node_fields['namespace'] != action_namespace) |
      (~node_fields['name'].isin(field_target))
    ]

  @staticmethod
  def _append_field(node_fields, node_baseid, namespace, field_name):
    new_row = {
      'node_baseid': node_baseid,
      'namespace': namespace,
      'name': field_name
    }
    return pd.concat([node_fields, pd.DataFrame([new_row])], ignore_index=True)

  def _handle_remove_list(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    field_target = temp_detail[convert_map['field_target']]
    node_fields = self._remove_matching_fields(node_fields, field_target, action_namespace)
    if not pd.isna(action_namespace):
      info_dict['namespace'] = action_namespace
    info_dict['field_target'] = field_target
    return node_fields, info_dict

  def _handle_add_column(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    field_target = temp_detail[convert_map['field_target']]
    namespace = action_namespace if not pd.isna(action_namespace) else 'Default'
    if field_target not in set(node_fields["name"]):
      node_fields = self._append_field(node_fields, node_baseid, namespace, field_target)
    info_dict['namespace'] = namespace
    info_dict[field_target] = detail.expression
    return node_fields, info_dict

  def _handle_keep_list(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    field_target = temp_detail[convert_map['field_target']]
    if pd.isna(action_namespace):
      node_fields = node_fields[node_fields['name'].isin(field_target)]
    else:
      node_fields = node_fields[
        (node_fields['namespace'] == action_namespace) |
        (node_fields['name'].isin(field_target))
      ]
      info_dict['namespace'] = action_namespace
    info_dict['field_target'] = field_target
    return node_fields, info_dict

  def _handle_merge_column(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    field_target = temp_detail[convert_map['field_target']['from']]
    leave = temp_detail[convert_map['field_target']['to']]
    info_dict['from'] = field_target
    info_dict['to'] = leave
    node_fields = self._remove_matching_fields(
      node_fields,
      [field for field in field_target if field != leave],
      action_namespace,
    )
    return node_fields, info_dict

  def _handle_rename_column(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
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
    return node_fields, info_dict

  def _handle_bulk_rename(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    info_dict['actions_detail'] = temp_detail.to_dict()
    renamed = self.apply_rule(temp_detail, convert_map['field_target'])[0]
    field_target = renamed['included_columns'].iloc[0] or []
    leave = renamed['exempted_columns'].iloc[0] or []
    field_target = [name for name in field_target if name not in leave]
    operation_type = renamed['operation_type'].iloc[0]
    if operation_type == 'replaceColumnAllSubStringOperation':
      existing = renamed['existing_subString'].iloc[0]
      replacement = renamed['new_substring'].iloc[0]
      node_fields["name"] = node_fields["name"].apply(
        lambda value: value.replace(existing, replacement) if value in field_target else value
      )
    elif operation_type == 'addColumnPrefixOperation':
      prefix = renamed['column_name_prefix'].iloc[0]
      node_fields["name"] = node_fields["name"].apply(
        lambda value: prefix + value if value in field_target else value
      )
    elif operation_type == 'addColumnSuffixOperation':
      suffix = renamed['column_name_suffix'].iloc[0]
      node_fields["name"] = node_fields["name"].apply(
        lambda value: value + suffix if value in field_target else value
      )
    return node_fields, info_dict

  def _handle_union(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    node_fields = node_fields.drop_duplicates()
    new_df = pd.DataFrame([{
      'name': 'Table Names',
      'node_baseid': node_baseid,
      'namespace': 'Default'
    }])
    node_fields = pd.concat([node_fields, new_df])
    info_dict['new_column'] = 'Table Names'
    return node_fields, info_dict

  def _handle_join(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    info_dict['conditions'] = detail.conditions
    info_dict['jointype'] = detail.joinType
    try:
      base = node_fields['name'].str.split('-', n=1).str[0]
      count = node_fields.groupby(base).cumcount()
      node_fields['name'] = base.where(count == 0, base + '-' + count.astype(str))
    except (AttributeError, KeyError, TypeError):
      self.logger.warning(
        "[%s] Could not normalize joined field names for node %s",
        self.run_id,
        node_baseid,
        exc_info=True,
      )
    return node_fields, info_dict

  def _handle_pivot(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    info_dict['actions_detail'] = temp_detail.to_dict()
    field_target = [temp_detail[convert_map['field_target']['from']]]
    node_fields = node_fields[~node_fields['name'].isin(field_target)]
    leave = (
      pd.DataFrame(temp_detail[convert_map['field_target']['to']])
      .assign(node_baseid=node_baseid, namespace='Default')
      .rename(columns={'newColumnName': 'name'})
    )
    node_fields = pd.concat([node_fields, leave])
    return node_fields, info_dict

  def _handle_newrows(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
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
    return node_fields, info_dict

  def _handle_unpivot(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    info_dict['actions_detail'] = temp_detail.to_dict()
    literal, unpivot_menu = self.apply_rule(temp_detail, convert_map['field_target'])
    new_names = pd.concat([literal, unpivot_menu])["name"].dropna().to_list()
    for value in new_names:
      if value not in set(node_fields["name"]):
        node_fields = self._append_field(node_fields, node_baseid, 'Default', value)
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
          if wildcard_type == 'Contains':
            node_fields = node_fields[~node_fields['name'].str.contains(wildcard_expression)]
    return node_fields, info_dict

  def _handle_aggregate(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
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
    return node_fields, info_dict

  def _handle_default(self, node_baseid, node_fields, action_namespace, detail, temp_detail, convert_map, info_dict):
    info_dict['actions_detail'] = temp_detail.to_dict()
    return node_fields, info_dict

  def apply_rule(self, df, rules):
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
      extracted = df[col].map(lambda value: self.extract_by_path(value, path))
      if mode == "scalar":
        result_cols[new_col] = extracted
      elif mode == "explode":
        df_explode = pd.DataFrame(extracted.iloc[0])
        for child_new_col, child_rule in rule.get("child_columns", {}).items():
          child_col = child_rule["column"]
          child_path = child_rule.get("path", [])
          df_explode[child_new_col] = df_explode[child_col].map(
            lambda value: self.extract_by_path(value, child_path)
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
  def extract_by_path(data, path):
    for key in path:
      if not isinstance(data, dict):
        return None
      data = data.get(key)
    return data
