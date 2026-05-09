import pandas as pd
from pathlib import Path
import json
import logging

#Prepフロー関連ファイルからデータ取り出し
class PrepInfoExtract:
  def __init__(self, data_dict, run_id):
    self.settings = data_dict['displaySettings']['flowDisplaySettings']['flowNodeDisplaySettings']
    flow_data = data_dict['flow']
    self.parameters = flow_data['parameters']['parameters']
    self.initial_nodes = flow_data['initialNodes']
    self.nodes = flow_data['nodes']
    self.connections = flow_data['connections']
    base_dir = Path(__file__).resolve().parent
    json_path = base_dir / "prep_maps.json"
    with open(json_path, encoding="utf-8") as f:
      self.extract_maps = json.load(f)
    self.logger = logging.getLogger(self.__class__.__name__)
    self.run_id = run_id

  def initial_nodes_df(self):
    self.logger.info(f"[{self.run_id}] extract initial nodes")
    df = pd.DataFrame(self.initial_nodes, columns=['node_baseid'])
    df_dict = {'initialNodes': df}
    self.logger.debug(f"[{self.run_id}] df dict: {df_dict}")
    return df_dict

  def settings_df(self):
    self.logger.info(f"[{self.run_id}] extract settings")
    dict_list = []
    for key, value in self.settings.items():
      node_id = key
      node_color = value['color']['hexCss']
      node_x = value['position']['x']
      node_y = value['position']['y']
      node_w = value['size']['width']
      node_h = value['size']['height']
      dict_list.append(
        {
          'node_baseid': node_id,
          'color': node_color,
          'x': node_x,
          'y': node_y,
          'w': node_w,
          'h': node_h
        }
      )
    df = pd.DataFrame(dict_list).sort_values(by=["x", "y"])
    df_dict = {'settings': df.reset_index(drop=True)}
    self.logger.debug(f"[{self.run_id}] df dict: {df_dict}")
    return df_dict

  def connections_df(self):
    self.logger.info(f"[{self.run_id}] extract connections")
    dict_list = []
    for _, value in self.connections.items():
      item_dict = {}
      #item_dict['id'] = key
      for k, v in value.items():
        if k == 'connectionAttributes':
          for attr_k, attr_v in v.items():
            item_dict[f'attribute_{attr_k}'] = attr_v
        else:
          item_dict[k] = v
      dict_list.append(item_dict)
    df = pd.DataFrame(dict_list)
    df = df.rename(columns={'id': 'connectionId', 'name': 'connectionName'})
    df_dict = {'connections': df.reset_index(drop=True)}
    self.logger.debug(f"[{self.run_id}] df dict: {df_dict}")
    return df_dict

  def parameters_df(self):
    self.logger.info(f"[{self.run_id}] extract parameters")
    dict_list = []
    for _, value in self.parameters.items():
      item_dict = {}
      #item_dict['id'] = key
      for k, v in value.items():
        if k == 'domain':
          for attr_k, attr_v in v.items():
            item_dict[f'domain_{attr_k}'] = attr_v
        else:
          item_dict[k] = v
      dict_list.append(item_dict)
    df = pd.DataFrame(dict_list)
    df_dict = {'parameters': df.reset_index(drop=True)}
    self.logger.debug(f"[{self.run_id}] df dict: {df_dict}")
    return df_dict

  def node_info(self):
    self.logger.info(f"[{self.run_id}] extract node info")
    extract_maps = self.extract_maps
    collectors = {
      'node_masta': [],
      'action_detail': [],
      'nextnode_info': [],
      'initial_node_detail': [],
      'fields_info': [],
    }
    for node_baseid, node_dict in self.nodes.items():
      node_type = node_dict.get('nodeType')
      # --- node base ---
      self.logger.info(f"[{self.run_id}] extract node base info(node_type: {node_type}, node_baseid: {node_baseid})")
      df_node = (
        pd.DataFrame([node_dict])[extract_maps['common']]
        .rename(columns={'id': 'node_baseid'})
        .assign(node_basetype='node')
      )
      collectors['node_masta'].append(df_node)
      # --- next node ---
      self.logger.info(f"[{self.run_id}] extract next node info")
      df_next = pd.DataFrame(node_dict.get('nextNodes', []))
      df_next['node_baseid'] = node_baseid
      collectors['nextnode_info'].append(df_next)
      # --- extract map ---
      extract_map = extract_maps.get(node_type)
      if not extract_map:
        self.logger.warning(f"[{self.run_id}] {node_type}:unknown node type")
        continue
      annotation_option = extract_map.get('annotations')
  
      # ==================================================
      # annotationあり（True）
      # ==================================================
      if annotation_option is True:
        self.logger.info(f"[{self.run_id}] extract action base")
        action_dict = node_dict.get('actionNode', {})
        # --- action base ---
        self.action_base(action_dict, node_baseid, collectors, 'action')
        # --- action detail ---
        self.logger.info(f"[{self.run_id}] extract action detail")
        df_action_detail = (
          pd.DataFrame([{k: action_dict.get(k) for k in extract_map['column']}])
          .assign(node_baseid=node_baseid, id=action_dict['id'])
        )
        collectors['action_detail'].append(df_action_detail)
        self.logger.info(f"[{self.run_id}] extract annotation base and detail")
        # --- annotation ---
        for anno_type, key in [("before", "beforeActionAnnotations"), ("after", "afterActionAnnotations")]:
          df_base, df_detail = self.anno_node_convert(node_dict.get(key, []))
          collectors['node_masta'].append(
            df_base.assign(node_baseid=node_baseid, node_basetype=anno_type)
          )
          collectors['action_detail'].append(
            df_detail.assign(node_baseid=node_baseid)
          )
      # ==================================================
      # annotationなし（False）
      # ==================================================
      elif annotation_option is False:
        self.logger.info(f"[{self.run_id}] extract initial node detail")
        # --- initial detail ---
        df_initial = (
          pd.DataFrame([{k: node_dict.get(k) for k in extract_map['column']}])
          .assign(node_baseid=node_baseid)
        )
        collectors['initial_node_detail'].append(df_initial)
        # --- fields ---
        self.logger.info(f"[{self.run_id}] extract initial node fields")
        if 'fields' in extract_map:
          df_fields = (
            pd.DataFrame(node_dict.get('fields', []))
            .assign(node_baseid=node_baseid)
          )
          collectors['fields_info'].append(df_fields)
        # --- actions ---
        self.logger.info(f"[{self.run_id}] extract initial node actions")
        for action_dict in node_dict.get('actions', []):
          action_type = action_dict.get('nodeType')
          # --- action base ---
          self.logger.info(f"[{self.run_id}] extract initial node action base(action type: {action_type} | action id: {action_dict.get('id')})")
          self.action_base(action_dict, node_baseid, collectors, 'action')
          self.logger.info(f"[{self.run_id}] extract initial node action detail")
          df_action_detail = (
            pd.DataFrame([
              {k: action_dict.get(k) for k in extract_maps.get(action_type, []).get('column', [])}
            ])
            .assign(node_baseid=node_baseid, id=action_dict.get('id'))
          )
          collectors['action_detail'].append(df_action_detail)
        # --- generatedInputs ---
        if 'generatedInputs' in extract_map:
          self.logger.info(f"[{self.run_id}] extract initial node's generatedInputs")
          for generated_input in node_dict['generatedInputs']:
            action_dict = generated_input['inputNode']
            action_type = action_dict['nodeType']
            # --- action base ---
            self.logger.info(f"[{self.run_id}] extract initial node's generatedInputs base(action type: {action_type}| action id: {action_dict.get('id')})")
            self.action_base(action_dict, node_baseid, collectors, 'generated_inputs')
            self.logger.info(f"[{self.run_id}] extract initial node's generatedInputs detail")
            df_action_detail = (
              pd.DataFrame([
                {k: action_dict.get(k) for k in extract_maps.get(action_type, []).get('column', [])}
              ])
              .assign(node_baseid=node_baseid, id=action_dict.get('id'))
            )
            collectors['action_detail'].append(df_action_detail)
        # --- outputのみ特殊処理 ---
        if extract_map.get('type') == 'output':
          self.action_base(node_dict, node_baseid, collectors, 'action')
          df_action_detail = (
            pd.DataFrame([node_dict])[extract_map['column']]
            .assign(node_baseid=node_baseid, id=node_baseid)
          )
          collectors['action_detail'].append(df_action_detail)
  
      # ==================================================
      # cleaning step
      # ==================================================
      elif annotation_option is None:
        self.logger.info(f"[{self.run_id}] extract cleaning step")
        action_type = node_dict['nodeType']
        loom_action = node_dict['loomContainer']
        if loom_action:
          self.logger.info(f"[{self.run_id}] extract loom action info")
          loom_nodeid = loom_action.get('initialNodes', None)
          if loom_nodeid != []:
            loom_nodeid = loom_nodeid[0]
            nodes_in_loom = loom_action.get('nodes', None)
            df_list = []
            while(True):
              action_dict = nodes_in_loom[loom_nodeid]
              next_nodeid = pd.DataFrame(action_dict.pop('nextNodes', None))
              self.logger.info(f"[{self.run_id}] extract action base(action type: {action_type}, action id: {action_dict['id']})")
              extract_map = extract_maps[action_dict['nodeType']]
              # --- action base ---
              self.action_base(action_dict, node_baseid, collectors, 'loom_action')

              # --- action detail ---
              self.logger.info(f"[{self.run_id}] extract action detail")
              df_action_detail = (
                pd.DataFrame([action_dict])[extract_map['column']]
                .assign(node_baseid=node_baseid, id=action_dict['id'])
              )
              collectors['action_detail'].append(df_action_detail)
              if not next_nodeid.empty:
                loom_nodeid = next_nodeid['nextNodeId'].iloc[0]
              else:
                break
    # --- concat ---
    self.logger.info(f"[{self.run_id}] return collectors item")
    return {
      k: pd.concat(v).reset_index(drop=True) if v else pd.DataFrame()
      for k, v in collectors.items()
    }

  #action_baseの情報を取り出し
  def action_base(self, action_dict, node_baseid, collectors, basetype):
    df_action_base = (
      pd.DataFrame([action_dict])[self.extract_maps['common']]
      .assign(node_baseid=node_baseid, node_basetype=basetype)
    )
    self.logger.debug(f"[{self.run_id}] df action base: {df_action_base}")
    collectors['node_masta'].append(df_action_base)

  #annotationNodeのすべての列を取り出し
  def anno_node_convert(self, data_list):
    self.logger.info(f"[{self.run_id}] return annotation node data")
    extract_maps = self.extract_maps
    base_info_list = []
    detail_info_list = []
    if not len(data_list)>0:
      self.logger.info(f"[{self.run_id}] annotation is None")
    for data_dict in data_list:
      anno_info = data_dict.get('annotationNode')
      anno_info = anno_info.copy()
      anno_info['namespace'] = data_dict.get('namespace')
      anno_type = anno_info.get('nodeType')
      anno_map = extract_maps.get(anno_type)
      if not anno_map:
        self.logger.warning(f"[{self.run_id}] unknown annotation data(action type: {anno_type}, action id: {anno_info['id']})")
        continue
      anno_map_cols = anno_map.get('column', [])
      # --- base ---
      self.logger.info(f"[{self.run_id}] extract annotation base data(action type: {anno_type}, action id: {anno_info['id']})")
      base_info_cols = extract_maps['common'] + ['namespace']
      base_info = {k: anno_info.get(k) for k in base_info_cols}
      self.logger.debug(f"[{self.run_id}] annotation base info: {base_info}")
      base_info_list.append(base_info)
      # --- detail ---
      self.logger.info(f"[{self.run_id}] extract annotation base detail")
      detail_info_cols = anno_map_cols + ['id']
      detail_info = {k: anno_info.get(k) for k in detail_info_cols}
      detail_info_list.append(detail_info)
      self.logger.debug(f"[{self.run_id}] annotation detail info: {detail_info}")
    return pd.DataFrame(base_info_list), pd.DataFrame(detail_info_list)

  #すべてのjsonを結合して取り出し
  def prep_info(self):
    info_dicts = dict(
      **self.initial_nodes_df(),
      **self.settings_df(),
      **self.connections_df(),
      **self.parameters_df(),
      **self.node_info()
    )
    for info_dict in info_dicts.items():
      self.logger.debug(f"[{self.run_id}] info dict list(all_extract_data): {info_dict}")
    return info_dicts