import io
import os
import glob
import base64
import shutil
import zipfile
import xlsxwriter
import pandas as pd
import numpy as np
import graphviz
from PIL import Image, ImageColor, ImageDraw
from lxml import etree as ET


#------------common------------
def write_df_to_excel(worksheet, df, start_row, string):
  df = df.fillna('')
  if len(df)>0:
    #write header
    for col_num, value in enumerate(df.columns):
      worksheet.write(start_row, col_num+1, value)
    #write data
    for row_num, (_, series) in enumerate(df.iterrows(), start=start_row + 1):
      for col_num, cell_value in enumerate(series):
        worksheet.write(row_num, col_num+1, cell_value)
    worksheet.merge_range(start_row, 0, worksheet.dim_rowmax, 0, string)
  else:
    worksheet.write(start_row, 1, string)

#------------workbook info------------
def write_workbook_info(root, excel_book):
  excel_worksheet = excel_book.add_worksheet('workbook')
  wb_info = pd.DataFrame([root.attrib])
  repository_location = root.find('repository-location')
  if repository_location is not None:
    wb_info.update(repository_location.attrib)
  wb_preference = pd.DataFrame(list(map(lambda x:x.attrib, root.findall('./preferences/preference'))))
  #wb_style
  wb_style_list = []
  for style_rule in root.findall('./style/style-rule/format'):
    wb_style = style_rule.attrib
    wb_style.update(style_rule.getparent().attrib)
    wb_style_list.append(wb_style)
  wb_style_list = pd.DataFrame(wb_style_list)
  write_df_to_excel(excel_worksheet, wb_info, 0, 'info')
  write_df_to_excel(excel_worksheet, wb_preference, excel_worksheet.dim_rowmax + 1, 'preference')
  write_df_to_excel(excel_worksheet, wb_style_list, excel_worksheet.dim_rowmax + 1, 'style')

#------------datasource------------
#dimension/measure
def return_dimension_measure(root, str_type):
  info_list = []
  if root.tag == 'workbook':
    dm_list = root.findall(f'./datasources/datasource/column[@role="{str_type}"]')
  else:
    dm_list = root.findall("./column[@role='" + str_type + "']")
  for info in dm_list:
    info_dict = info.attrib
    info_calc = info.find('calculation')
    if info_calc is not None:
      info_dict.update({f'calculation_{key}': value for key, value in info_calc.attrib.items()})
    info_dict.update({f'datasource_{key.split("}", 1)[-1]}': value for key, value in info.getparent().attrib.items()})
    info_list.append(info_dict)
    
  return pd.DataFrame(info_list)

#datasource
def return_datasource_connection(root):
  if root.tag == 'workbook':
    datasources = root.findall('./datasources/datasource')
  else:
    datasources = [root]
  datasource_info_list = []
  connection_detail_list = []
  join_list = []
  join_connection_list = []
  relationship_list = []
  for datasource in datasources:
    if datasource.attrib.get('name', None)!='Parameters':
      datasource_info = datasource.attrib
      datasource_info_list.append(datasource_info)
      datasource_name = datasource_info.get('name', None)
      if datasource_name is None:
        datasource_name = datasource_info.get('formatted-name', None)
      if datasource.find('./connection').attrib['class']=='federated':
        ds_connections = datasource.findall('./connection/named-connections/named-connection/connection')
        for ds_connection in ds_connections:
          connection_detail = ds_connection.attrib
          connection_detail.update({key: value for key, value in ds_connection.getparent().attrib.items()})
          connection_detail.update({'datasource_name': datasource_name})
          connection_detail_list.append(connection_detail)
      else:
        ds_connection = datasource.find('./connection').attrib
        ds_connection.update({key: value for key, value in datasource.find('./repository-location').attrib.items()})
        connection_detail_list.append(ds_connection)
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
                connection_dict.update({f'object_{key}': value for key, value in object_info.items()})
                if element.attrib.get('connection', None) is not None:
                  join_connection_list.append(connection_dict)
                  element_list.remove(element)
                  element_list.extend(element.findall('relation'))
                elif element.attrib.get('join', None) is not None:
                  for join_condition in return_join_list(element.find('clause')):
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
          for join_condition in return_join_list(data_relationship):
            relationship_list.append({'left': left_table, 'right': right_table, 'key': join_condition[0]})
  return pd.DataFrame(datasource_info_list), pd.DataFrame(connection_detail_list), pd.DataFrame(join_list), pd.DataFrame(join_connection_list), pd.DataFrame(relationship_list)

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

def write_datasource_info(root, datasource_connection, excel_book):
  excel_worksheet = excel_book.add_worksheet('datasource')
  #datasource_info
  write_df_to_excel(excel_worksheet, datasource_connection[0], 0, 'datasource_info')
  write_df_to_excel(excel_worksheet, datasource_connection[1], excel_worksheet.dim_rowmax + 1, 'connection_detail')

  #metadata_record
  metadata_record_list = []
  if root.tag == 'workbook':
    mr_list = root.findall('./datasources/datasource/connection/metadata-records/metadata-record')
  else:
    mr_list = root.findall('./connection/metadata-records/metadata-record')
  for metadata_record in mr_list:
    record_info = metadata_record.attrib
    for child in metadata_record:
      record_info[child.tag]=child.text if child.text is not None else ''
    record_info.update({f'connection_{key}': value for key, value in metadata_record.getparent().getparent().attrib.items()})
    record_info.update({f'datasource_{key.split("}", 1)[-1]}': value for key, value in metadata_record.getparent().getparent().getparent().attrib.items()})
    metadata_record_list.append(record_info)
  write_df_to_excel(excel_worksheet, pd.DataFrame(metadata_record_list), excel_worksheet.dim_rowmax + 1, 'metadata_record')
  #parameters
  parameter_list = []
  parameter_alias_list = []
  parameter_member_list = []

  if root.tag == 'workbook':
    for parameter in root.findall("./datasources/datasource[@name='Parameters']/column"):
      param_info = parameter.attrib
      param_name = param_info['name']
      param_info.update({f'calculation_{key}': value for key, value in parameter.find('calculation').attrib.items()})
      param_info.update({f'datasource_{key.split("}", 1)[-1]}': value for key, value in parameter.getparent().attrib.items()})
      parameter_list.append(param_info)
      for alias in parameter.findall('./aliases/alias'):
        alias_info = alias.attrib
        alias_info['name'] = param_name
        parameter_alias_list.append(alias_info)
      for member in parameter.findall('./members/member'):
        member_info = member.attrib
        member_info['name'] = param_name
        parameter_member_list.append(member_info)
  write_df_to_excel(excel_worksheet, pd.DataFrame(parameter_list), excel_worksheet.dim_rowmax + 1, 'parameter')
  write_df_to_excel(excel_worksheet, pd.DataFrame(parameter_alias_list), excel_worksheet.dim_rowmax + 1, 'parameter_alias')
  write_df_to_excel(excel_worksheet, pd.DataFrame(parameter_member_list), excel_worksheet.dim_rowmax + 1, 'parameter_member')
  #dimension/measure
  write_df_to_excel(excel_worksheet, return_dimension_measure(root, 'dimension'), excel_worksheet.dim_rowmax + 1, 'dimension')
  write_df_to_excel(excel_worksheet, return_dimension_measure(root, 'meassure'), excel_worksheet.dim_rowmax + 1, 'measure')

#------------action------------
def return_action_info(root):
  #actions
  action_types = ['action', 'edit-group-action', 'edit-parameter-action']
  action_list = []
  exclude_sheet_list = []
  action_link_list = []
  action_param_list = []
  action_command_list = []
  for action_type in action_types:
    for action in root.findall(f'./actions/{action_type}'):
      action_info = {}
      action_info_temp = action.attrib
      action_name = action_info_temp['name']
      action_info.update({'action_name':action_name})
      del action_info_temp['name']
      action_info.update(action_info_temp)
      if action.find('activation') is not None:
        action_info.update({f'activation_{key}': value for key, value in action.find('activation').attrib.items()})
      if action.find('source') is not None:
        action_info.update({f'source_{key}': value for key, value in action.find('source').attrib.items()})
      action_list.append(action_info)
      #exclude sheet
      info_exclude_sheet = action.findall('./source/exclude-sheet')
      if info_exclude_sheet is not None:
        for exclude_sheet in info_exclude_sheet:
          es_dict = exclude_sheet.attrib
          es_dict["worksheet_name"] = es_dict.pop("name")
          es_dict.update({key: value for key, value in exclude_sheet.getparent().getparent().attrib.items()})
          es_dict['action_name']=action_name            
          exclude_sheet_list.append(es_dict)
      #link
      info_link = action.find('link')
      if info_link is not None:
        link_dict = {}
        link_dict.update({'action_name': action_name})
        link_dict.update(info_link.attrib)
        for child in info_link:
          link_dict[child.tag] = child.text
        action_link_list.append(link_dict)
      #param
      info_params = action.findall('./params/param')
      if len(info_params)>0:
        param_dict = {}
        for param in info_params:
          param_dict.update({'action_name': action_name})
          param_dict[param.attrib['name']] = param.attrib['value']
        action_param_list.append(param_dict)
      #command
      info_commands = action.findall('./command/param')
      if len(info_commands)>0:
        command_dict = {}
        for command in info_commands:
          command_dict.update({'action_name': action_name})
          command_dict[command.attrib['name']] = command.attrib['value']
        action_command_list.append(command_dict)
  return pd.DataFrame(action_list).rename(columns={'name': 'action_name'}), pd.DataFrame(exclude_sheet_list), pd.DataFrame(action_link_list), pd.DataFrame(action_param_list).fillna(''), pd.DataFrame(action_command_list)

def write_action_info(action_info, excel_book):
  excel_worksheet = excel_book.add_worksheet('action')
  write_df_to_excel(excel_worksheet, action_info[0], 0, 'action')
  write_df_to_excel(excel_worksheet, action_info[1], excel_worksheet.dim_rowmax + 1, 'source_exclude')
  write_df_to_excel(excel_worksheet, action_info[2], excel_worksheet.dim_rowmax + 1, 'link_list')
  write_df_to_excel(excel_worksheet, action_info[3], excel_worksheet.dim_rowmax + 1, 'set_parameter_action')
  write_df_to_excel(excel_worksheet, action_info[4], excel_worksheet.dim_rowmax + 1, 'target')

#------------worksheet------------
def return_worksheet_info(root, excel_book):
  excel_worksheet = excel_book.add_worksheet('worksheet')
  ws_info_list = []
  ws_dd_list = []
  filter_list = []
  style_list = []
  pane_encodings = []
  pane_style_list = []
  for ws in root.findall('./worksheets/worksheet'):
    ws_info = {}
    worksheet_name = ws.attrib['name']
    ws_info['worksheet_name'] = worksheet_name
    ws_rows = ws.find('./table/rows').text
    ws_info['rows'] = ws_rows
    ws_cols = ws.find('./table/cols').text
    ws_info['cols'] = ws_cols
    #datasource dependencies
    for ws_dd in ws.findall('./table/view/datasources/datasource'):
      dd_info = {}
      dd_info.update({'worksheet_name': worksheet_name})
      dd_info.update(ws_dd.attrib) 
      ws_dd_list.append(dd_info)
    #filter
    for ws_filter in ws.findall('./table/view/filter'):
      filter_info = {}
      filter_info.update({'worksheet_name': worksheet_name})
      filter_info.update(ws_filter.attrib)
      filter_group = ws_filter.find('groupfilter')
      if filter_group is not None:
        filter_info.update({f'groupfilter_{key}': value for key, value in ET.fromstring(ET.tostring(filter_group).decode('utf-8').replace(':', '_')).items()})
      filter_list.append(filter_info)
    #style
    for ws_style_format in ws.findall('./table/style/style-rule/format'):
      style_format = {}
      style_format.update({'worksheet_name': worksheet_name})
      style_format.update(ws_style_format.attrib)
      style_format.update({f'style_rule_{key}': value for key, value in ws_style_format.getparent().attrib.items()})
      style_list.append(style_format)
    #pane
    ws_pane = ws.find('./table/panes/pane')
    pane_view = ws_pane.find('./view/breakdown').attrib
    ws_info.update({f'pane_view_{key}': value for key, value in pane_view.items()})
    pane_mark = ws_pane.find('./mark').attrib
    ws_info.update({f'pane_view_{key}': value for key, value in pane_mark.items()})
    for child in ws_pane.findall('./encodings'):
      for grand_child in child:
        grand_child_attrib = {}
        grand_child_attrib.update({'worksheet_name': worksheet_name})
        grand_child_attrib.update(grand_child.attrib)
        grand_child_attrib['tag'] = grand_child.tag
        pane_encodings.append(grand_child_attrib)
    for pane_style_format in ws_pane.findall('./style/style-rule/format'):
      style_format = {}
      style_format.update({'worksheet_name': worksheet_name})
      style_format.update(pane_style_format.attrib)
      style_format.update({f'style_rule_{key}': value for key, value in pane_style_format.getparent().attrib.items()})
      pane_style_list.append(style_format)
    ws_info_list.append(ws_info)
  
  ws_info_list = pd.DataFrame(ws_info_list)
  ws_dd_list = pd.DataFrame(ws_dd_list)
  filter_list = pd.DataFrame(filter_list)
  style_list = pd.DataFrame(style_list)
  pane_encodings = pd.DataFrame(pane_encodings)
  pane_style_list = pd.DataFrame(pane_style_list)

  write_df_to_excel(excel_worksheet, ws_info_list, 0, 'worksheet_info')
  write_df_to_excel(excel_worksheet, ws_dd_list, excel_worksheet.dim_rowmax + 1, 'workdheet_datasource_dependencies')
  write_df_to_excel(excel_worksheet, filter_list, excel_worksheet.dim_rowmax + 1, 'filter_list')
  write_df_to_excel(excel_worksheet, style_list, excel_worksheet.dim_rowmax + 1, 'style_list')
  write_df_to_excel(excel_worksheet, pane_encodings, excel_worksheet.dim_rowmax + 1, 'pane_encodings')
  write_df_to_excel(excel_worksheet, pane_style_list, excel_worksheet.dim_rowmax + 1, 'pane_style')

  return ws_info_list, ws_dd_list, filter_list, style_list, pane_encodings, pane_style_list

#------------dashboard------------
def return_dashboard_info(root, excel_book):
  excel_worksheet = excel_book.add_worksheet('dashboard')
  db_info_list = []
  style_list = []
  zone_list = []
  zone_style_format_list = []
  zone_format_text_list = []
  zone_button_list = []
  for db in root.findall('./dashboards/dashboard'):
    #info
    db_name = db.attrib['name']
    db_info = {}
    db_info['dashboard_name'] = db_name
    if db.find('./layout-options/title/formatted-text/run') is not None:
      db_info['dashboard_title'] = db.find('./layout-options/title/formatted-text/run').text
    else:
      None
    db_info.update(db.find('./size').attrib)
    if db.find('./repository-location') is not None:
      db_info.update(db.find('./repository-location').attrib)
    db_info_list.append(db_info)
    #style
    for db_style_format in db.findall('./style/style-rule/format'):
      style_format = {}
      style_format.update({'dashboard_name': db_name})
      style_format.update(db_style_format.attrib)
      style_format.update(db_style_format.getparent().attrib)
      style_list.append(style_format)
    #zone
    db_zone = db.findall('./zones/zone')
    i = 0
    while(True):
      if len(db_zone)==0:
        break
      else:
        for zone in db_zone:
          zone_dict = zone.attrib
          zone_dict['dashboard_name'] = db_name
          zone_id = zone_dict['id']
          layout_cache = zone.find('./layout-cache')
          if layout_cache is not None:
            zone_dict.update({f'layout_cache_{key}': value for key, value in layout_cache.attrib.items()})
          for zone_style_format in zone.findall('./zone-style/format'):
            if zone_style_format is not None:
              style_format = zone_style_format.attrib
              style_format['id'] = zone_id
              zone_style_format_list.append(style_format)
          for zone_format_text in zone.findall('./formatted-text/run'):
            if zone_format_text is not None:
              format_text = zone_format_text.attrib
              format_text['id'] = zone_id
              format_text['run_text'] = zone_format_text.text
              zone_format_text_list.append(format_text)
          button_dict = {}
          for zone_button in zone.findall('./button/button-visual-state'):
            if zone_button is not None:
              button_dict.update({zone_button.tag: zone_button.text})
              button_dict['id'] = zone_id
              button_dict.update(zone_button.getparent().attrib)
              zone_button_list.append(button_dict)
          zone_dict['layer'] = str(i)
          zone_parent = zone.getparent().attrib
          if 'id' in zone_parent:
            zone_dict['parent_id'] = zone_parent['id']
          zone_list.append(zone_dict)
          db_zone = db_zone + zone.findall('zone')
          db_zone.remove(zone)
      i+=1

  db_info_list = pd.DataFrame(db_info_list)
  style_list = pd.DataFrame(style_list)
  zone_list = pd.DataFrame(zone_list)
  zone_style_format_list = pd.DataFrame(zone_style_format_list)
  zone_format_text_list = pd.DataFrame(zone_format_text_list)
  zone_button_list = pd.DataFrame(zone_button_list)

  write_df_to_excel(excel_worksheet, db_info_list, 0, 'dashboard')
  write_df_to_excel(excel_worksheet, style_list, excel_worksheet.dim_rowmax + 1, 'style_list')

  return db_info_list, style_list, zone_list,zone_style_format_list, zone_format_text_list, zone_button_list

def write_dashboard_info(db_info, ws_pane_style_list, excel_book, temp_path):
  dashboard_info = db_info[0]
  dashboard_list = dashboard_info['dashboard_name'].to_list()
  zone_info = db_info[2]
  zone_style_info = db_info[3][db_info[3]['attr']=='background-color']
  for db_name in dashboard_list:
    excel_worksheet = excel_book.add_worksheet(f'{db_name}_image')
    #max_height/max_width
    df = db_info[0]
    max_height, max_width = return_max_hw(df, db_name)
    [excel_worksheet.set_row(i, 7.5) for i in range(int(max_height/10))]
    excel_worksheet.set_column(0, int(max_width/10), 0.85)
    excel_worksheet.insert_image("A1", f'{temp_path}db_image/{db_name}.png')

    #zone data
    df = db_info[2][db_info[2]['dashboard_name']==db_name].fillna('')
    for index, row in df.iterrows():
      row_dict = row.to_dict()
      zone_id = row_dict['id']
      x, y = return_x1_y1(row_dict, max_height, max_width, 10)
      w = int(max_width * int(row_dict['w']) / 100000)
      h = int(max_height * int(row_dict['h']) / 100000)
      #write text
      if len(db_info[4])>0:
        text_info = db_info[4][db_info[4]['id']==zone_id].reset_index(drop=True)
        text_info = text_info[text_info['run_text']!='Æ']
        text_info['run_text'] = text_info['run_text'].str.replace('Æ','')
        if len(text_info)>0:
          df_text = text_info[['run_text']].T
          text = ''
          for index, row in df_text.iterrows():
            for col_name, value in row.items():
              text += value
          excel_worksheet.insert_textbox(y, x, text, {'width':w, 'height':h, 'fill':{'color':'white'}, 'line': {'color': 'red'}})
          excel_worksheet.write_comment(y, x, text_info.drop(columns='id').fillna('').T.to_csv(header=False), {'visible': True})
      #button
      if 'id' in db_info[5].columns:
        df_button = db_info[5][db_info[5]['id']==zone_id]
        if len(df_button)>0:
          excel_worksheet.write_comment(y, x, df_button.drop(columns='id').fillna('').T.to_csv(header=False), {'visible': True})
      #params
      if row_dict.get('type-v2', None)=='paramctrl':
        excel_worksheet.insert_textbox(y, x, row_dict['param'], {'width':w, 'height':h, 'fill':{'color':'white'}, 'line': {'color': 'green'}})
        excel_worksheet.write_comment(y, x, row_dict['mode'], {'visible': True})
      if row_dict.get('name', None) is not None:
        #shape
        if len(ws_pane_style_list)>0:
          df_shape = ws_pane_style_list[ws_pane_style_list['worksheet_name']==row_dict['name']]
          df_shape = df_shape[df_shape['attr']=='shape']
          if len(df_shape)==0:
            excel_worksheet.insert_textbox(y, x, row_dict['name'], {'width':w, 'height':h, 'fill':{'color':'white'}, 'line': {'color': 'blue'}})
            if row_dict.get('type-v2', None) == 'filter':
              excel_worksheet.write_comment(y, x, row_dict.get('param', '')+'|'+row_dict.get('mode', ''), {'visible': True})
            elif row_dict.get('type-v2', None) == 'color':
              excel_worksheet.write_comment(y, x, row_dict.get('param', ''), {'visible': True})

def make_dashboard_image(db_info, ws_pane_style_list, temp_path):
  dashboard_list = db_info[0]['dashboard_name'].to_list()
  for db_name in dashboard_list:
    shape_dict = []
    #max_height/max_width
    df = db_info[0]
    max_height, max_width = return_max_hw(df, db_name)
    im = Image.new('RGB', (max_width, max_height), (255, 255, 255))
    #zone data
    df = db_info[2][db_info[2]['dashboard_name']==db_name]
    draw = ImageDraw.Draw(im)
    max_layer = df['layer'].astype(int).max()
    zone_bc = db_info[3][db_info[3]['attr']=='background-color']
    i = 0
    while(i<=max_layer):
      zone = df[df['layer']==str(i)]
      if len(zone)>0:
        for index, row in zone.iterrows():
          row_dict = row.to_dict()
          zone_id = row_dict['id']
          x1, y1 = return_x1_y1(row_dict, max_height, max_width, 1)
          x2 = int(max_width * (int(row_dict['x'])+int(row_dict['w'])) / 100000)
          y2 = int(max_height * (int(row_dict['y'])+int(row_dict['h'])) / 100000)
          #background color
          background_color = zone_bc[zone_bc['id']==zone_id]
          if len(background_color)>0:
            draw.rectangle([(x1, y1), (x2, y2)], ImageColor.getcolor(background_color['value'].iloc[0], "RGB"))
          else:
            draw.rectangle([(x1, y1), (x2, y2)], outline='black', width=1)
          #bitmap/shape
          if row_dict.get('type-v2', None) == 'bitmap' or not pd.isnull(row_dict.get('name', None)):
            shape_dict.append(row_dict)
      i+=1
      #shape/bitmap
      for row_dict in shape_dict:
        x1, y1 = return_x1_y1(row_dict, max_height, max_width, 1)
        if len(ws_pane_style_list)>0:
          df_shape = ws_pane_style_list[ws_pane_style_list['worksheet_name']==row_dict['name']]
          df_shape = df_shape[df_shape['attr']=='shape']
          if len(df_shape)>0:
            path_string = df_shape['value'].to_string(index=False)
            if os.path.exists(f'{temp_path}{path_string}'):
              temp_im = Image.open(f'{temp_path}{path_string}')
              temp_im = temp_im.resize((int(max_width * int(row_dict['w']) / 100000), int(max_height * int(row_dict['h']) / 100000)))
              im.paste(temp_im, (x1, y1))
          if not pd.isnull(row_dict['param']):
            path_string = row_dict['param']
            if os.path.exists(f'{temp_path}/{path_string}'):
              temp_im = Image.open(f'{temp_path}/{path_string}')
              temp_im = temp_im.resize((int(max_width * int(row_dict['w']) / 100000), int(max_height * int(row_dict['h']) / 100000)))
              im.paste(temp_im, (x1, y1))
    db_image_path = f'{temp_path}db_image/{db_name}.png'
    if not os.path.exists(os.path.dirname(db_image_path)):
      os.mkdir(os.path.dirname(db_image_path))
    im.save(db_image_path)

def return_x1_y1(row_dict, max_height, max_width, div_num):
  if int(row_dict['x'])==0:
    x1 = 0
  else:
    x1 = int(max_width * int(row_dict['x']) / 100000 / div_num)
  if int(row_dict['y'])==0:
    y1 = 0
  else:
    y1 = int(max_height * int(row_dict['y']) / 100000 / div_num)
  return x1, y1

def return_max_hw(df, db_name):
  df = df[df['dashboard_name']==db_name]
  if 'maxheight' in df.columns:
    if pd.isnull(df['maxheight'].iloc[0]):
      max_height = 1080
    else:
      max_height = int(df['maxheight'].iloc[0])
  else:
    max_height = 1080
  if 'maxwidth' in df.columns:
    if pd.isnull(df['maxwidth'].iloc[0]):
      max_width = 1920
    else:
      max_width = int(df['maxwidth'].iloc[0])
  else:
    max_width = 1920
  return max_height, max_width


#------------dashboard edge------------
def make_datasource_graph_image(datasource_connection, temp_path):
  g = graphviz.Graph(format='svg')
  g.attr(rankdir='LR')
  if len(datasource_connection[4])>0:
    relation_list = datasource_connection[4]['left'].to_list()
    relation_list.extend(datasource_connection[4]['right'].to_list())
    relation_list = list(set(relation_list))
  else:
    relation_list = ['datasource']
  for relation_box in relation_list:
    with g.subgraph(name=f"cluster_{translate_string(relation_box)}") as c:
      c.attr(color='black', label=f"{translate_string(relation_box)}")
      c.node_attr.update(color='black', shape='folder')
      if len(datasource_connection[3])>0:
        df_node = datasource_connection[3][datasource_connection[3]['object_id']==relation_box]
        if len(df_node)>0:
          for relation_node in df_node['name'].to_list():
            c.node(name=f'{relation_box}_{relation_node}')
        else:
          c.node(name=datasource_connection[1].get('caption', pd.Series()).to_string(index=False))
      else:
        c.node(name=datasource_connection[0].get('caption', pd.Series()).to_string(index=False))
    #join_edge
    if len(datasource_connection[2])>0:
      if relation_box == 'datasource':
        join_edge = datasource_connection[2]
      else:
        join_edge = datasource_connection[2][datasource_connection[2]['object_id']==relation_box]
      join_edge = join_edge[['left', 'right', 'join', 'object_id', 'key']].drop_duplicates()
      for index, row in join_edge.iterrows():
        row['left'] = row['left'].split('[')[1].split(']')[0]
        row['right'] = row['right'].split('[')[1].split(']')[0]
      join_edge = join_edge.groupby(['left', 'right', 'join', 'object_id'])['key'].apply(lambda x: '\n'.join(x) if not pd.isnull('NaN') else '').reset_index(drop=False)
      if len(join_edge)>0:
        for idx, row in join_edge.iterrows():
          left_table = row['left']
          right_table = row['right']
          object_id = row['object_id']
          key = row['key']
          join_type = row['join']
          g.edge(f"{translate_string(object_id)}_{translate_string(left_table)}", f"{translate_string(object_id)}_{translate_string(right_table)}", label=f"{join_type}\n{translate_string(key)}")
  #relation_edge
  if len(datasource_connection[4])>0:
    relation_list = datasource_connection[3].drop_duplicates(subset='object_id').reset_index(drop=True).copy()
    relation_list['node'] = relation_list['object_id'] + '_' + relation_list['name']
    relation_list = relation_list[['node', 'object_id']]
    df = datasource_connection[4]
    df = pd.merge(df, relation_list, left_on='left', right_on='object_id').rename(columns={'node': 'left_node'})
    df = pd.merge(df, relation_list, left_on='right', right_on='object_id').rename(columns={'node': 'right_node'})
    df = df.groupby(['left_node', 'right_node', 'left', 'right'])['key'].apply(lambda x: '\n'.join(x) if not pd.isnull('NaN') else '').reset_index(drop=False)
    for idx, row in df.iterrows():
      g.edge(row['left_node'], row['right_node'], ltail = 'cluster_'+row['left'], lhead = 'cluster_'+row['right'], label=row['key'])
  #g.render(f'{temp_path}datasource', format='svg')
  return g

#------------action edge------------
def make_action_graph_image(action_info, zone_data, temp_path, worksheet_info):
  db_worksheet = return_dashboard_worksheet_list(zone_data, worksheet_info)
  g = graphviz.Digraph(format='svg')
  g.attr(rankdir='LR')
  edge_list = []
  edge_list = return_dashboard_action_edge(action_info, db_worksheet)
  if edge_list is not None:
    non_write_list = edge_list[1]
    for dashboard_name in db_worksheet['dashboard_name'].unique():
      sub_db_worksheet = db_worksheet[db_worksheet['dashboard_name'] == dashboard_name]
      with g.subgraph(name=f"cluster_{translate_string(dashboard_name)}") as c:
        c.attr(color='black', label=f"{translate_string(dashboard_name)}")
        c.node_attr.update(color='black', shape='folder')
        #Add child nodes to the subgraph
        for idx, row in sub_db_worksheet.iterrows():
          c.node(name = f"{translate_string(row['dashboard_name'])}_{translate_string(row['worksheet_name'])}")
          if not row['dashboard_name'] in non_write_list:
            c.node(name = f"{translate_string(row['dashboard_name'])}_allsheet")
    #edge
    action_edge = edge_list[0]
    for idx, row in action_edge.iterrows():
      if not 'tsl：' in translate_string(row['end']):
        g.edge(f"{translate_string(row['start'])}", f"{translate_string(row['end'])}", label = f"{translate_string(row['action_name'])}")
    #g.render(f'{temp_path}action', format='svg')
    return g

def return_dashboard_action_edge(action_info, db_worksheet):
  df_edge_list = []
  node_exclude_list = []
  action_list = edit_action_info(action_info[0])
  if len(action_list)>0:
    source_exclude = groupby_exclude_list(action_info[1])
    if len(source_exclude)>0:
      action_list = pd.merge(action_list, source_exclude, on='action_name', how='left')
    else:
      action_list = action_list.copy()
      action_list['source_exclude'] = ''
    action_link_list = edit_link_list(action_info[2])
    set_action_list = edit_set_list(action_info[3])
    target_list = edit_target_list(action_info[4])
    target_list = pd.concat([action_link_list, set_action_list, target_list]).reset_index(drop=True)
    action_list = pd.merge(action_list, target_list, on='action_name').fillna('')
    dashboard_list = db_worksheet['dashboard_name'].to_list()
    worksheet_list = db_worksheet['worksheet_name'].to_list()
    if len(action_list)>0:
      for idx, row in action_list.iterrows():
        action_name = row['action_name']
        source_datasource = row['source_datasource']
        source_dashboard = row['source_dashboard']
        source_worksheet = row['source_worksheet']
        source_exclude = [i for i in row.get('source_exclude', '').split(',') if i != '']
        target = row['target']
        target_exclude = [i for i in row.get('target_exclude', '').split(',') if i != '']
        #start
        if source_datasource != '':
          edge_start = pd.DataFrame([source_datasource], columns=['start'])
        else:
          if source_dashboard != '':
            if source_worksheet != '':
              edge_start = pd.DataFrame([f'{source_dashboard}_{source_worksheet}'], columns=['start'])
            else:
              if len(source_exclude)>0:
                edge_start = db_worksheet[(db_worksheet['dashboard_name']==source_dashboard)&(~db_worksheet['worksheet_name'].isin(source_exclude))].copy()
                edge_start = edge_start[['dashboard_name', 'worksheet_name']].drop_duplicates()
                edge_start['start'] = edge_start['dashboard_name'].str.cat(edge_start['worksheet_name'], sep='_')
                edge_start = edge_start[['start']]
              else:
                edge_start = pd.DataFrame([f'{source_dashboard}_allsheet'], columns=['start'])
                node_exclude_list.append(source_dashboard)
          else:
            edge_start = db_worksheet[db_worksheet['worksheet_name']==source_worksheet].copy()
            edge_start = edge_start[['dashboard_name', 'worksheet_name']].drop_duplicates()
            edge_start['start'] = edge_start['dashboard_name'].str.cat(edge_start['worksheet_name'], sep='_')
            edge_start = edge_start[['start']]
        edge_start['action_name'] = action_name
        #target
        if target in dashboard_list:
          if len(target_exclude)>0:
            edge_end = db_worksheet[(db_worksheet['dashboard_name']==target)&(~db_worksheet['worksheet_name'].isin(target_exclude))].copy()
            edge_end = edge_end[['dashboard_name', 'worksheet_name']].drop_duplicates()
            edge_end['end'] = edge_end['dashboard_name'].str.cat(edge_end['worksheet_name'], sep='_')
            edge_end = edge_end[['end']]
          else:
            edge_end = pd.DataFrame([f'{source_dashboard}_allsheet'], columns=['end'])
            node_exclude_list.append(source_dashboard)
        else:
          if target in worksheet_list:
            edge_end = db_worksheet[(db_worksheet['worksheet_name']==target)].copy()
            edge_end = edge_end[['dashboard_name', 'worksheet_name']].drop_duplicates()
            edge_end['end'] = edge_end['dashboard_name'].str.cat(edge_end['worksheet_name'], sep='_')
            edge_end = edge_end[['end']]
          else:
            edge_end = pd.DataFrame([target], columns=['end'])
        edge_end['action_name'] = action_name
        df_edge_list.append(pd.merge(edge_start, edge_end, on='action_name'))
      df = pd.concat(df_edge_list)
      node_exclude_list = set([i for i in dashboard_list if (not i in node_exclude_list) or i=='None'])
      return df, node_exclude_list

def return_dashboard_worksheet_list(zone_data, worksheet_info):
  #dashboard to worksheet
  if len(zone_data)>0:
    db_worksheet_columns = zone_data.filter(regex='name').columns
    db_worksheet = zone_data[db_worksheet_columns].copy()
    db_worksheet['dashboard_name'] = zone_data['dashboard_name']
    db_worksheet = db_worksheet.melt(id_vars=['dashboard_name'], value_vars=db_worksheet_columns, var_name='name', value_name='worksheet_name').drop(columns='name')
    db_worksheet = db_worksheet.dropna().drop_duplicates()
    if len(worksheet_info[0])>0:
      non_db_worksheet = worksheet_info[0][~worksheet_info[0]['worksheet_name'].isin(db_worksheet['worksheet_name'].to_list())].copy()[['worksheet_name']]
      db_worksheet = pd.concat([db_worksheet, non_db_worksheet]).fillna('None')
  else:
    db_worksheet = worksheet_info[0]
    db_worksheet['dashboard_name'] = 'None'
  #worksheet datasource
  if len(worksheet_info[1])>0:
    worksheet_datasource = worksheet_info[1].rename(columns={'name': 'datasource'})[['worksheet_name', 'datasource']]
    worksheet_datasource = worksheet_datasource[worksheet_datasource['datasource']!='Parameters']
    db_worksheet = pd.merge(db_worksheet, worksheet_datasource, on='worksheet_name')
  return db_worksheet

def translate_string(t_string):
  digit_dict = {':': '：', ',': '，'}
  for key, value in digit_dict.items():
    t_string = t_string.replace(key, value)
  return t_string

def edit_action_info(df):
  if len(df)>0:
    df_columns = df.columns
    if not 'source_dashboard' in df.columns:
      df['source_dashboard'] = None
    if not 'source_datasource' in df.columns:
      df['source_datasource'] = None
    if not 'source_worksheet' in df.columns:
      df['source_worksheet'] = None
    df = df[['action_name', 'source_dashboard', 'source_datasource', 'source_worksheet']]
  else:
    df = pd.DataFrame()
  return df

def groupby_exclude_list(df):
  if len(df)>0:
    df['worksheet_name'] = df['worksheet_name'].str.split(',')
    df = df.explode('worksheet_name')
    df = df.groupby('action_name')['worksheet_name'].apply(lambda x: ','.join(x) if not pd.isnull('NaN') else '').reset_index(drop=False)
    df = df.rename(columns={'worksheet_name':'source_exclude'})
  else:
    df = pd.DataFrame()
  return df

def edit_link_list(df):
  if len(df)>0:
    df = df.rename(columns={'expression': 'target'})[['action_name', 'target']]
  else:
    df = pd.DataFrame()
  return df

def edit_set_list(df):
  if len(df)>0:
    if not 'target-parameter' in df.columns:
      df['target-parameter'] = None
    if not 'target-group' in df.columns:
      df['target-group'] = None
    df['target'] = df.apply(lambda row: row['target-group'] if row['target-group'] else row['target-parameter'], axis=1)
    df = df[['target', 'action_name']]
  else:
    df = pd.DataFrame()
  return df

def edit_target_list(df):
  if len(df)>0:
    if 'exclude' in df.columns:
      df = df[['action_name', 'target', 'exclude']]
      df = df.rename(columns={'exclude': 'target_exclude'})
    else:
      df = df[['action_name', 'target']].copy()
      df['target_exclude'] = ''
  else:
    df = pd.DataFrame()
  return df

def make_external_image(root, temp_path):
  #external
  for shape in root.findall('./external/shapes/shape'):
    if shape is not None:
      shape_name = shape.attrib['name']
      file_path = f'{temp_path}{shape_name}'
      im = Image.open(io.BytesIO(base64.b64decode(shape.text)))
      if not os.path.exists(os.path.dirname(file_path)):
        os.mkdir(os.path.dirname(file_path))
      im.save(file_path)   

#------------main------------
def convert_desktop(temp_path, file_name, file_type, excel_dir_path):
  tree_file = next(temp_path.glob(f'*{file_type}'))
  tree = ET.parse(tree_file)
  excel_path = f'{temp_path}/{file_name}.xlsx'
  excel_book = xlsxwriter.Workbook(excel_path)
  root = tree.getroot()

  if root.tag == 'workbook':
    make_external_image(root, temp_path)
    write_workbook_info(root, excel_book)
    datasource_connection = return_datasource_connection(root)
    write_datasource_info(root, datasource_connection, excel_book)
    action_info = return_action_info(root)
    worksheet_info = return_worksheet_info(root, excel_book)
    dashboard_info = return_dashboard_info(root, excel_book)
    write_action_info(action_info, excel_book)
    make_dashboard_image(dashboard_info, worksheet_info[5], temp_path)
    write_dashboard_info(dashboard_info, worksheet_info[5], excel_book, temp_path)
    datasource_graph = make_datasource_graph_image(datasource_connection, temp_path)
    action_graph = make_action_graph_image(action_info, dashboard_info[2], temp_path, worksheet_info)
  else:
    datasource_connection = return_datasource_connection(root)
    write_datasource_info(root, datasource_connection, excel_book)
    datasource_graph = make_datasource_graph_image(datasource_connection, temp_path)
    action_graph = ''
  excel_book.close()
  shutil.copy(excel_path, f'{excel_dir_path}/{file_name}.xlsx')
  return [datasource_graph, action_graph, f'{excel_dir_path}/{file_name}.xlsx']