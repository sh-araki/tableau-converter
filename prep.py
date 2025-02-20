import tempfile
import glob
import json
import pandas as pd
import graphviz
import zipfile
from pathlib import Path

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

#--------------advance_function------------
def settings_info(path):
  f = open(path, 'r', encoding="utf-8")
  json_dict = json.load(f)
  field_list = []
  table_list = []
  for item in json_dict['fieldOrder']['fieldOrdinals'].items():
    field_list.append([item[0] ,str(item[1])])
  for item in json_dict['flowDisplaySettings']['flowNodeDisplaySettings'].items():
    test = lambda x: '0'+x if len(x)==1 else x
    table_list.append([item[0], item[1]['color']['hexCss'], int(test(str(item[1]['position']['x']))), int(test(str(item[1]['position']['y'])))])
  df_table = pd.DataFrame(table_list, columns = ['id', 'color', 'longitude', 'latitude'])
  return df_table.sort_values(by=["longitude","latitude"])

def return_properties(properties):
  property_info = []
  for node_id in properties:
    node_property = properties[node_id]
    for property_name in node_property:
      property_detail = node_property[property_name]
      property_detail['node_id'] = node_id
      property_info.append(property_detail)
  return pd.DataFrame(property_info)

def return_connection_info(node_connection):
  connection_info = []
  for connection in node_connection:
    element = node_connection[connection]
    connection_info.append([element.get('id'), element.get('name'), element.get('connectionType'), element.get('connectionAttributes').get('class'), element.get('connectionAttributes').get('port'), element.get('connectionAttributes').get('username')])
  connection_info = pd.DataFrame(connection_info, columns=['connection_id', 'connection_name', 'connection_type', 'connection_class', 'connection_port', 'connection_username'])
  return connection_info

#--------------function------------
def insert_next_fields(next_node_info, node_field):
  next_node_info = next_node_info.reset_index(drop=True)[['nextNodeId']]
  next_node_info['namespace'] = ''
  next_field = node_field.reset_index(drop=True)[['name', 'remove', 'merge', 'rename', 'keep']]
  next_field.loc[next_field['rename'] != '', 'name'] = next_field['rename']
  next_field['namespace'] =''
  next_field = next_field[(next_field['remove']=='') & (next_field['merge']=='')]
  if 'keep' in next_field['keep'].to_list():
    next_field=next_field[next_field['keep']=='keep']
  next_field = pd.merge(next_field[['name', 'namespace']], next_node_info, on='namespace', how='left').drop(columns='namespace').rename(columns={'nextNodeId':'node_id'})
  return next_field

def merge_remove_rename_add(node_actions, node_field):
  if len(node_actions)>0:
    if len(node_actions[node_actions['nodeType'].str.contains('Remove')])>0:
      remove_list = []
      remove_actions = node_actions[node_actions['nodeType'].str.contains('Remove')][['columnNames']]
      for column_name, item in remove_actions.iterrows():
        remove_list = remove_list + item['columnNames']
      remove_actions = pd.DataFrame(remove_list).rename(columns={0:'name'})
      remove_actions['remove']='removed'
      node_field = pd.merge(node_field, remove_actions, on='name', how='left')
      print(node_field)
    else:
      node_field['remove'] = None
    if len(node_actions[node_actions['nodeType'].str.contains('Merge')])>0:
      merge_list = []
      merge_actions = node_actions[node_actions['nodeType'].str.contains('Merge')][['mergeColumnsList', 'mergedColumnName']]
      for row, item in merge_actions.iterrows():
        item['mergeColumnsList'].remove(item['mergedColumnName'])
        merge_list = merge_list + item['mergeColumnsList']
      merge_actions = pd.DataFrame(merge_list).rename(columns={0:'name'})
      merge_actions['merge']='merged'
      node_field = pd.merge(node_field, merge_actions, on='name', how='left')
      print(node_field)
    else:
      node_field['merge'] = None
    if len(node_actions[node_actions['nodeType'].str.contains('Rename')])>0:
      if len(node_actions[node_actions['nodeType'].str.contains('Bulk')])>0:
        rename_actions = []
        df_bulk = node_actions[node_actions['nodeType'].str.contains('BulkRename')]
        df_normal = node_actions[node_actions['nodeType'].str.contains('Rename')]
        df_normal = df_normal[~df_normal['nodeType'].str.contains('Bulk')]
        if len(df_normal)>0:
          rename_actions.append(df_normal[df_normal['nodeType'].str.contains('Rename')][['columnName', 'rename']].rename(columns={'columnName': 'name'}))
        column_selection = df_bulk['columnsSelection'].to_list()
        column_operation = df_bulk['columnsOperation'].to_list()
        for i in range(len(column_selection)):
          column_list = column_selection[i]['includedColumns']
          column_exclude = column_selection[i]['exemptedColumns']
          column_list = [i for i in column_list if i not in column_exclude]
          rename_action = pd.DataFrame(column_list, columns=['name'])
          rename_from = column_operation[i]['existingSubString']
          rename_to = column_operation[i]['newSubString']
          rename_action['rename'] = rename_action['name'].str.replace(rename_from, rename_to)
          rename_actions.append(rename_action)
        rename_actions = pd.concat(rename_actions)
      else:
        rename_actions = node_actions[node_actions['nodeType'].str.contains('Rename')][['columnName', 'rename']].rename(columns={'columnName': 'name'})
      node_field = pd.merge(node_field, rename_actions, on='name', how='left')
    else:
      node_field['rename'] = None
    if len(node_actions[node_actions['nodeType'].str.contains('AddColumn')])>0:
      add_actions = node_actions[node_actions['nodeType'].str.contains('AddColumn')][['columnName']].rename(columns={'columnName': 'name'})
      add_actions['add'] = 'added'
      node_field = pd.concat([node_field, add_actions]).drop_duplicates(subset=['name'], keep='first')
    else:
      node_field['add'] = None
    if len(node_actions[node_actions['nodeType'].str.contains('KeepOnlyColumns')])>0:
      keep_actions = node_actions[node_actions['nodeType'].str.contains('KeepOnlyColumns')]['columnNames'].to_list()
      keep_actions = pd.DataFrame(keep_actions[0], columns=['name'])
      keep_actions['keep'] = 'keep'
      node_field = pd.merge(node_field, keep_actions, on='name', how='left')
    else:
      node_field['keep'] = None
  else:
    node_field['remove'] = None
    node_field['merge'] = None
    node_field['rename'] = None
    node_field['add'] = None
    node_field['keep'] = None
  return node_field.fillna('')

def return_annotation_info(json_dict):
  before_annotation = pd.DataFrame([x["annotationNode"] for x in json_dict.get('beforeActionAnnotations')])
  after_annotation = pd.DataFrame([x["annotationNode"] for x in json_dict.get('afterActionAnnotations')])
  node_annotation = pd.concat([before_annotation, after_annotation])
  return node_annotation

def return_node_field(node_id, df_list):
  node_field = pd.concat(df_list)
  node_field = node_field[node_field['node_id']==node_id].reset_index(drop=True).reindex(columns=['node_id', 'name'])
  return node_field

def return_node_action_annotation_string(node_actions, replace_str):
  if len(node_actions)>0:
    node_actions = node_actions[~(node_actions['nodeType'].str.contains('Remove') | node_actions['nodeType'].str.contains('Rename'))].drop(columns=['serialize', 'baseType'])
    if 'description' in node_actions.columns.to_list():
      node_actions = node_actions.drop(columns='description')
    if 'nextNodeId' in node_actions.columns.to_list():
      node_actions = node_actions.drop(columns='nextNodeId')
    node_actions_info = node_actions[['nodeType', 'name', 'id']]
    node_actions_expressions = make_expression_column(node_actions.drop(columns=['nodeType', 'name']))
    node_actions = pd.merge(node_actions_info, node_actions_expressions, on='id', how='left').drop(columns='id')
    if len(node_actions)>0:
      node_actions.loc[node_actions['nodeType'].str.contains('KeepOnlyColumns'), 'expression'] = 'refer to the following'
      node_actions = replace_str(node_actions.to_html(index=False)).replace('<td', '<td colspan="4"')
    else:
      node_actions =''
  else:
    node_actions = ''
  return node_actions

def return_process_index(initial_node_id, df): #cleaning_step
  if not initial_node_id is None:
    df = df[['id', 'nextNodeId']]
    final_id = df[df['nextNodeId']=='']['id'].to_string(index=False)
    df_n = df[df['id']==initial_node_id]
    i=0
    while True:
      df_n = df_n.rename(columns={'id': str(i), 'nextNodeId':'id'})
      df_n = pd.merge(df_n, df, on='id', how='inner')
      if len(df_n)>0:
        if df_n['id'][0] == final_id:
          df_n = df_n.rename(columns = {'id': str(i+1)}).drop(columns='nextNodeId')
          break
      else:
        break
      i+=1
    if len(df_n)>0:
      return df_n.stack().to_list()
    else:
      return []
  else:
    return []

def make_expression_column(df):
  index_id = df['id'].to_list()
  new_columns = []
  for index, row in df.iterrows():
    expressions = []
    for col_name, value in row.items():
      if col_name == 'id':
        continue
      else:
        if value=='True' or value == 'False' or value is None or value == '':
          continue
        else:
          expressions.append(f"{str(value).replace('[', '').replace(']', '')}")
    new_columns.append(expressions)
  new_df = pd.DataFrame(new_columns,  index=index_id)
  new_df['expression'] = ''
  if len(new_df)>0:
    for i in range(len(new_columns[0])):
      new_df['expression'] = new_df['expression'] + '|' + new_df[i]
  new_df['expression'] = new_df['expression'].str.replace('|nan', '').str.replace('||', '|')
  return new_df.reset_index().rename(columns=({'index': 'id'}))[['id', 'expression']]

def convert_json_to_dataframe(json):
  data_list = []
  for item in json:
    for key, value in item.items():
        data_list.append({'sheet_name': key, 'file_name': value})
  df = pd.DataFrame(data_list)
  return df

def convert_process(flow, display_settings):
  graph = graphviz.Digraph(format='svg')
  graph.attr('graph', rankdir="LR")
  replace_str = lambda x: x.replace(' class="dataframe"', '').replace('<table border="1">\n', '').replace('\n</table>', '').replace('<tbody>', '').replace('</tbody>', '').replace('<thead>','').replace('</thead>', '').replace('th>', 'td>').replace('\\n', '<br/>')
  #extract displaysettings
  settings = settings_info(display_settings)
  node_list = settings['id'].to_list()
  #extract flow
  with open(flow, 'r', encoding="utf-8") as f:
    json_dict = json.load(f)
  #extract json's info
  initial_node_list = json_dict['initialNodes']
  connection = return_connection_info(json_dict['connections'])
  properties = return_properties(json_dict['nodeProperties'])

  df_list = []
  input_list = []
  output_list = []
  for i in node_list:
    #node_info
    node_json = json_dict['nodes'][i]
    node_name = node_json['name'].replace('->', '⇒')
    node_type = node_json['nodeType']
    node_settings = settings[settings['id']==i]
    node_color = node_settings['color'].to_string(index=False)
    node_longitude = node_settings['longitude'].to_string(index=False)
    node_latitude = node_settings['latitude'].to_string(index=False)
    node_pos = str(node_longitude) + ',' + str(node_latitude) + '!'
    #next_node_namespace
    next_node_info = pd.DataFrame(node_json['nextNodes'])
    if len(next_node_info)>0:
      for column_name, item in next_node_info.iterrows():
        graph.edge(i, item['nextNodeId'], label=item['nextNamespace'])
    #initial_node
    if i in initial_node_list:
      node_field = pd.DataFrame(node_json['fields'])
      if len(node_field)==0:
        print("some initial node's fields have error. please check this flow")
        continue
      node_actions = pd.DataFrame(node_json['actions'])
      node_pk = pd.DataFrame(columns=['name', 'PK'])
      if len(properties)>0:
        node_property = properties[properties['node_id']==i]
        pk_list = node_property['fieldNames'].to_list()
        if len(pk_list)>0:
          node_pk = pd.DataFrame(pk_list[0], columns=['name'])
          node_pk['PK']='〇'
      node_field = pd.merge(node_field, node_pk, on='name', how='left')
      node_field = merge_remove_rename_add(node_actions, node_field)
      node_field = node_field.sort_values('ordinal').drop(columns='ordinal')
      if 'isGenerated' in node_field.columns.to_list():
        node_field = node_field.drop(columns='isGenerated')
      if len(next_node_info)>0:
        df_list.append(insert_next_fields(next_node_info, node_field))
      connection_id = node_json.get('connectionId')
      node_connection = connection[connection['connection_id']==connection_id].copy()
      input_view = node_connection.copy()
      input_view['node_name'] = node_name
      input_list.append(input_view)
      table_relation = node_json.get('relation')
      if table_relation is None:
        node_connection['relation'] = None
      else:
        table_relation_key = table_relation.get('type')
        table_relation = table_relation.get(table_relation_key)
        node_connection['relation'] = table_relation
      #file input union
      union_directory = node_json.get('connectionAttributes')
      union_filepattren = None
      union_files = None
      if union_directory is None:
        union_directory = None
      else:
        union_directory = union_directory.get('directory')
        union_filepattren = node_json.get('filePattern')
        union_files = node_json.get('matchedSheetsAndFiles')
        if union_files is not None:
          union_files = convert_json_to_dataframe(union_files)
          union_files['directory'] = union_directory
          union_files['files_pattern'] = union_filepattren
      node_info = '<tr><td colspan="6" bgcolor="'+ node_color +'">' + node_name+ '</td>'
      node_info = node_info + '\n' + '<td colspan="6" bgcolor="'+ node_color +'">' + node_type+ '</td></tr>'
      node_connection = replace_str(node_connection.drop(columns='connection_id').to_html(index=False))
      node_connection = node_connection.replace('<td', '<td colspan="2"')
      if union_files is not None:
        node_union_info = replace_str(union_files.to_html(index=False))
        node_union_info = node_union_info.replace('<td', '<td colspan="3"')
      else:
        node_union_info = ''
      node_actions = return_node_action_annotation_string(node_actions, replace_str)
      node_field = replace_str(node_field.drop(columns=['caption', 'add', 'keep']).to_html(index=False))
      node_field = node_field.replace('<td', '<td colspan="2"')
      label = '<<table border="1">\n' + node_info + '\n' + node_connection + '\n' + node_union_info+ '\n' + node_actions + '\n' + node_field + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
    #cleaning_step
    if 'Container' in node_type:
      node_field = return_node_field(i, df_list)
      loomContainer = node_json['loomContainer']
      node_actions = loomContainer['nodes']
      initial_node_id = loomContainer.get('initialNodes')
      if len(initial_node_id)>0:
        initial_node_id = initial_node_id[0]
      else:
        initial_node_id = None
      container_list = []
      for container_id in node_actions:
        container = node_actions[container_id]
        next_node = container.pop('nextNodes')
        if len(next_node)>0:
          next_node_id = next_node[0]['nextNodeId']
        else:
          next_node_id = ''
        container['nextNodeId'] = next_node_id
        container_list.append(container)
      order_list = return_process_index(initial_node_id, pd.DataFrame(container_list))
      if len(container_list)>0:
        if len(order_list)>0:
          node_actions = pd.DataFrame(container_list).set_index('id').loc[order_list].reset_index()
        else:
          node_actions = pd.DataFrame(container_list).set_index('id').reset_index()
      node_field = merge_remove_rename_add(node_actions, node_field)
      node_field = node_field.reindex(columns=['name', 'remove', 'merge', 'rename', 'add', 'keep'])
      if len(next_node_info)>0:
        df_list.append(insert_next_fields(next_node_info, node_field))
      node_info = '<tr><td colspan="6" bgcolor="'+ node_color +'">' + node_name+ '</td>'
      node_info = node_info + '\n' + '<td colspan="6" bgcolor="'+ node_color +'">' + node_type+ '</td></tr>'
      node_actions = return_node_action_annotation_string(node_actions, replace_str)
      if len(node_field)>0:
        node_field = replace_str(node_field.drop(columns=['add']).to_html(index=False))
        node_field = node_field.replace('<td', '<td colspan="3"')
      else:
        node_field = ''
      label = '<<table border="1">\n' + node_info + '\n' + node_actions + '\n' + node_field + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
    if 'SuperTransform' in node_type:
      node_field = return_node_field(i, df_list)
      node_annotation = return_annotation_info(node_json)
      node_annotation = node_annotation.drop(columns='nextNodes')
      node_field = merge_remove_rename_add(node_annotation, node_field)
      if len(next_node_info)>0:
        df_list.append(insert_next_fields(next_node_info, node_field))
      node_info = '<tr><td colspan="6" bgcolor="'+ node_color +'">' + node_name + '</td>'
      node_info = node_info + '\n' + '<td colspan="6" bgcolor="'+ node_color +'">' + node_type + '</td></tr>'
      node_annotation = return_node_action_annotation_string(node_annotation, replace_str)
      node_field = replace_str(node_field.drop(columns=['node_id', 'add']).to_html(index=False))
      node_field = node_field.replace('<td', '<td colspan="3"')
      label = '<<table border="1">\n' + node_info + '\n' + node_annotation + '\n' + node_field + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
    #Combine
    if 'SuperJoin' in node_type:
      node_field = return_node_field(i, df_list)
      node_field['name']  = node_field['name'].str.split(pat='-', expand=True)[0]
      node_field['count'] = node_field.groupby('name').cumcount()
      node_field.loc[node_field['count']!=0, 'name'] = node_field['name'].str.cat(node_field['count'].astype(str), sep='-')
      node_field = node_field.drop(columns='count')
      node_actions = node_json['actionNode']
      join_type = node_actions['joinType']
      node_actions = pd.DataFrame(node_actions['conditions'])
      node_annotation = return_annotation_info(node_json)
      node_field = merge_remove_rename_add(node_annotation, node_field)
      if len(next_node_info)>0:
        df_list.append(insert_next_fields(next_node_info, node_field))
      node_info = '<tr><td colspan="4" bgcolor="'+ node_color +'">' + node_name + '</td>'
      node_info = node_info + '\n' + '<td colspan="4" bgcolor="'+ node_color +'">' + node_type + '</td>'
      node_info = node_info + '\n' + '<td colspan="4" bgcolor="'+ node_color +'">' + join_type + '</td></tr>'
      node_annotation = return_node_action_annotation_string(node_annotation, replace_str)
      node_actions = replace_str(node_actions.to_html(index=False))
      node_actions = node_actions.replace('<td', '<td colspan="4"')
      node_field = replace_str(node_field.drop(columns=['node_id', 'add']).to_html(index=False))
      node_field = node_field.replace('<td', '<td colspan="3"')
      label = '<<table border="1">\n' + node_info + '\n' + node_actions + '\n' + node_annotation + '\n' + node_field + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
    #Union
    if 'SuperUnion' in node_type:
      node_field = return_node_field(i, df_list).drop_duplicates()
      node_actions = node_json['actionNode']
      node_actions = pd.DataFrame(node_actions['namespaceFieldMappings'])
      node_annotation = return_annotation_info(node_json)
      node_field = merge_remove_rename_add(node_annotation, node_field)
      if len(next_node_info)>0:
        df_list.append(insert_next_fields(next_node_info, node_field))
      node_info = '<tr><td colspan="6" bgcolor="'+ node_color +'">' + node_name + '</td>'
      node_info = node_info + '\n' + '<td colspan="6" bgcolor="'+ node_color +'">' + node_type + '</td></tr>'
      node_annotation = return_node_action_annotation_string(node_annotation, replace_str)
      node_actions = replace_str(node_actions.to_html(index=False))
      node_actions = node_actions.replace('<td', '<td colspan="6"')
      node_field = replace_str(node_field.drop(columns=['node_id', 'add']).to_html(index=False))
      node_field = node_field.replace('<td', '<td colspan="3"')
      label = '<<table border="1">\n' + node_info + '\n' + node_actions + '\n' + node_annotation + '\n' + node_field + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
    #Pivot
    if 'SuperPivot' in node_type:
      node_field = return_node_field(i, df_list)
      node_actions = node_json['actionNode']
      new_column = pd.json_normalize(node_actions['newPivotColumns']).rename(columns={'newColumnName':'name'})
      new_column['node_id'] = i
      node_field = pd.concat([node_field, new_column])
      node_field = node_field[~node_field['name'].isin([node_actions['aggregateColumnName'], node_actions['pivotColumnName']])]
      node_actions = pd.json_normalize(node_actions).drop(columns=['nodeType', 'name', 'id', 'baseType', 'nextNodes','serialize', 'description', 'newPivotColumns'])
      node_annotation = return_annotation_info(node_json)
      node_field = merge_remove_rename_add(node_annotation, node_field)
      if len(next_node_info)>0:
        df_list.append(insert_next_fields(next_node_info, node_field))
      node_info = '<tr><td colspan="6" bgcolor="'+ node_color +'">' + node_name + '</td>'
      node_info = node_info + '\n' + '<td colspan="6" bgcolor="'+ node_color +'">' + node_type + '</td></tr>'
      node_annotation = return_node_action_annotation_string(node_annotation, replace_str)
      node_actions = replace_str(node_actions.to_html(index=False))
      node_actions = node_actions.replace('<td', '<td colspan="3"')
      node_field = replace_str(node_field.drop(columns=['node_id', 'add']).to_html(index=False))
      node_field = node_field.replace('<td', '<td colspan="3"')
      label = '<<table border="1">\n' + node_info + '\n' + node_actions + '\n' + node_annotation + '\n' + node_field + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
    #Unpivot
    if 'SuperUnpivot' in node_type:
      node_field = return_node_field(i, df_list)
      node_actions = node_json['actionNode']
      if 'Extended' in node_type:
        new_name = node_actions['unpivotGroup']['literalColumn']['literalColumnName']
        new_value = node_actions['unpivotGroup']['unpivotColumns'][0]['unpivotColumnName']
        if node_actions['unpivotGroup']['unpivotColumns'][0].get('columnInformation', None).get('bindingsType', None) is None:
          unpivot_columns = pd.DataFrame(node_actions['unpivotGroup']['literalColumn'])[['names']]['names']
        else:
          column_info = node_actions['unpivotGroup']['unpivotColumns'][0]['columnInformation']
          if column_info.get('wildcardExpression', None) is not None:
            column_key = column_info['wildcardExpression']
            unpivot_columns = node_field[node_field['name'].str.contains(column_key)]['name']
          else:
            column_key = column_info['manualBindings']
            unpivot_columns = node_field[node_field['name'].isin(column_key)]['name']
      else:
        binding_list = []
        unpivot_expressions = node_actions['unpivotGroups'][0]['expressions']
        for binding in unpivot_expressions:
          binding_list += binding['bindings']
        unpivot_expressions = pd.DataFrame(binding_list)
        new_name = unpivot_expressions[unpivot_expressions['bindingType']=='literal']['newColumnName'].to_list()[0]
        new_value = unpivot_expressions[unpivot_expressions['bindingType']=='column']['newColumnName'].to_list()[0]
        unpivot_columns = unpivot_expressions['columnName']
      node_field.loc[len(node_field)] = [i, new_name]
      node_field.loc[len(node_field)] = [i, new_value]
      node_field = node_field[~node_field['name'].isin(unpivot_columns.to_list())]
      unpivot_columns = unpivot_columns.str.cat(sep=', ')
      node_actions = pd.DataFrame(columns=['unpivot_columns'])
      node_actions.loc[0] = unpivot_columns
      node_annotation = return_annotation_info(node_json)
      node_field = merge_remove_rename_add(node_annotation, node_field)
      if len(next_node_info)>0:
        df_list.append(insert_next_fields(next_node_info, node_field))
      node_info = '<tr><td colspan="6" bgcolor="'+ node_color +'">' + node_name + '</td>'
      node_info = node_info + '\n' + '<td colspan="6" bgcolor="'+ node_color +'">' + node_type + '</td></tr>'
      node_annotation = return_node_action_annotation_string(node_annotation, replace_str)
      node_actions = replace_str(node_actions.to_html(index=False))
      node_actions = node_actions.replace('<td', '<td colspan="12"')
      node_field = replace_str(node_field.drop(columns=['node_id', 'add']).to_html(index=False))
      node_field = node_field.replace('<td', '<td colspan="3"')
      label = '<<table border="1">\n' + node_info + '\n' + node_actions + '\n' + node_annotation + '\n' + node_field + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
    #Aggregate
    if 'SuperAggregate' in node_type:
      node_field = return_node_field(i, df_list)
      node_actions = node_json['actionNode']
      groupby_fields = pd.DataFrame(node_actions['groupByFields'])
      aggregate_fields = pd.DataFrame(node_actions['aggregateFields'])
      node_actions = pd.concat([groupby_fields, aggregate_fields])
      node_field = node_actions[['columnName']].rename(columns={'columnName': 'name'})
      node_field['node_id'] = i
      node_annotation = return_annotation_info(node_json)
      node_field = merge_remove_rename_add(node_annotation, node_field)
      if len(next_node_info)>0:
        df_list.append(insert_next_fields(next_node_info, node_field))
      node_info = '<tr><td colspan="6" bgcolor="'+ node_color +'">' + node_name + '</td>'
      node_info = node_info + '\n' + '<td colspan="6" bgcolor="'+ node_color +'">' + node_type + '</td></tr>'
      node_annotation = return_node_action_annotation_string(node_annotation, replace_str)
      node_actions = replace_str(node_actions.to_html(index=False))
      node_actions = node_actions.replace('<td', '<td colspan="3"')
      node_field = replace_str(node_field.drop(columns=['node_id', 'add']).to_html(index=False))
      node_field = node_field.replace('<td', '<td colspan="3"')
      label = '<<table border="1">\n' + node_info + '\n' + node_actions + '\n' + node_annotation + '\n' + node_field + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
    #NewRows
    if 'NewRows' in node_type:
      node_field = return_node_field(i, df_list)
      node_actions = node_json['actionNode']
      rows_generator = pd.DataFrame([node_actions.pop('newRowsGenerator')])
      rows_step = pd.DataFrame([node_actions.pop('step')])
      generate_type = node_actions['globalDefaultValue']
      if 'newColumnName' in rows_generator.columns:
        node_field.loc[len(node_field)] = [i, rows_generator['newColumnName'].to_string(index=False)]
      node_annotation = return_annotation_info(node_json)
      node_field = merge_remove_rename_add(node_annotation, node_field)
      if len(next_node_info)>0:
        df_list.append(insert_next_fields(next_node_info, node_field))
      node_info = '<tr><td colspan="6" bgcolor="'+ node_color +'">' + node_name + '</td>'
      node_info = node_info + '\n' + '<td colspan="6" bgcolor="'+ node_color +'">' + node_type + '</td></tr>'
      node_annotation = return_node_action_annotation_string(node_annotation, replace_str)
      rows_generator = replace_str(rows_generator.to_html(index=False))
      rows_generator = rows_generator.replace('<td', '<td colspan="3"')
      rows_step = replace_str(rows_step.to_html(index=False))
      rows_step = rows_step.replace('<td', '<td colspan="4"')
      node_field = replace_str(node_field.drop(columns=['node_id', 'add']).to_html(index=False))
      node_field = node_field.replace('<td', '<td colspan="3"')
      label = '<<table border="1">\n' + node_info + '\n' + rows_generator + '\n' + rows_step + '\n' + node_annotation + '\n' + node_field + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
    #PublishExtract
    if 'PublishExtract' in node_type or 'WriteToHyper' in node_type:
      node_info = pd.DataFrame([node_json])
      if 'projectLuid' in node_info.columns:
        node_info = node_info.drop(columns='projectLuid')
      node_info = node_info.drop(columns=['id', 'baseType', 'nextNodes', 'serialize'])
      if 'description' in node_info.columns:
        node_info.drop(columns=['description'])
      output_list.append(node_info)
      node_info = replace_str(node_info.to_html(index=False))
      node_info = node_info.replace('<td', '<td colspan="2"')
      label = '<<table border="1">\n' + node_info + '\n</table>>'
      graph.node(i,  shape='folder', label=label, pos=node_pos)
  #node infos graph
  if len(input_list)>0:
    input_list = replace_str(pd.concat(input_list).drop(columns='connection_id').fillna('').to_html(index=False))
    input_list = input_list.replace('<td', '<td colspan="2"')
  else:
    input_list = ''
  if len(output_list)>0:
    output_list = replace_str(pd.concat(output_list).fillna('').to_html(index=False))
    output_list = output_list.replace('<td', '<td colspan="2"')
  else:
    output_list = ''
  label = '<<table border="1">\n' + input_list + '\n' + output_list + '\n</table>>'
  graph.node('node_info', shape='folder', label=label)
  return graph

def convert_prep(temp_file):
  with tempfile.TemporaryDirectory() as temp_dir:
    with zipfile.ZipFile(temp_file, 'r') as zip_ref:
      zip_ref.extractall(temp_dir)  # temp_dirに解凍
    temp_folder = Path(temp_dir)
    flow = f'{temp_folder}/flow'
    display_settings = f'{temp_folder}/displaySettings'
    return convert_process(flow, display_settings)
