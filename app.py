from common.file_classifier import FileClassifier
from prep_module.main import PrepMainProcess
from desktop_module.extract import DesktopInfoExtract
from desktop_module.convert import DesktopNodesView
import pandas as pd
import glob
import logging
import uuid
import numpy as np
import streamlit as st
from st_cytoscape import cytoscape
import json
import tempfile
from pathlib import Path
import io
import base64
from PIL import Image, ImageColor, ImageDraw
import streamlit.components.v1 as components


@st.cache_data
def prep_process(data_dict, run_id):
  prep_main_data = PrepMainProcess(data_dict, run_id)
  elements, stylesheet, dfs, node_uml, html = prep_main_data.return_dataframes()
  return elements, stylesheet, dfs, node_uml, html

def main(logger):
  #タイトル・デフォルト設定
  st.set_page_config(layout="wide")
  st.title("Tableau File's Converter")
  #ファイルアップローダー
  uploaded_files = st.file_uploader(
    "Upload your Tableau file",
    type=["tfl", "tflx", "twb", "twbx"],
    accept_multiple_files=True,
  )
  if not uploaded_files:
    return
  for uploaded_file in uploaded_files:
    run_id = uuid.uuid4().hex[:8]
    file_bytes = uploaded_file.getvalue()
    file_name = uploaded_file.name
    st.title(file_name)
    logger.info(f"[{run_id}][{file_name}:run id is {run_id}] ===== File Data Convert START =====")
    with tempfile.TemporaryDirectory() as tmp:
      tmp_file = Path(tmp) / file_name
      tmp_file.write_bytes(file_bytes)
      try:
        fc = FileClassifier(tmp_file, run_id)
        if '.tfl' in fc.suffix.lower():
          data_dict = fc.process().data_dict
          elements, stylesheet, dfs, node_uml, html = prep_process(data_dict, run_id)
          logger.info(f"[{run_id}][{file_name}] ===== File Data Convert END =====")
          logger.info(f"[{run_id}][{file_name}] ===== Rendering START =====")
          col1, col2 = st.columns(2)
          with col1:
            st.download_button(
              "Download as html",
              html,
              f"{uploaded_file.name}_cytoscape.html",
              "text/html"
            )
          with col2:
            st.download_button(
              "Download flow uml",
              node_uml,
              f"{uploaded_file.name}_uml.json",
              "application/json"
            )
          selected = cytoscape(
            elements=elements,
            stylesheet=stylesheet,
            layout={"name": "preset"},
            key=uploaded_file.name,
            width="100%",
            height="540px",
          )
          if selected and selected.get("nodes"):
            node_id = selected["nodes"][-1]
            logger.debug(f"selected node: {node_id}")
            for key, df in dfs.items():
              df = df[df['node_baseid']==node_id]
              if not df.empty:
                df = clean_dataframe(df)
              st.write(key)
              st.dataframe(df)
          logger.info(f"[{run_id}][{file_name}] ===== Rendering END =====")
#        elif '.tds' in fc.suffix.lower():
        elif '.twb' in fc.suffix.lower():
          data_xml = fc.process().data_xml
          desktop_data = DesktopInfoExtract(data_xml, run_id)
          datas = [
            desktop_data.extract_info(),
            desktop_data.extract_datasource_connection(),
            desktop_data.extract_zone_info()
          ]
          logger.info(f"[{run_id}][{file_name}] ===== File Data Convert END =====")
          logger.info(f"[{run_id}][{file_name}] ===== Rendering START =====")
          desktop_node_view = DesktopNodesView(datas, run_id)
          base_dir = Path(__file__).resolve().parent
          component_path = base_dir / "desktop_module" / "frontend"
          _component_func = components.declare_component(
              "cyto_tree",
              path = component_path
          )
          st.write('info maps')
          elements, stylesheet = desktop_node_view.cytoscape_elements()
          selected = _component_func(
            elements=elements,
            stylesheet=stylesheet,
            key=f"cyto_{uploaded_file.name}"
          )
          st.write("selected:", selected)
          st.dataframe(desktop_data.extract_info().get(selected, None))
          datasource_element, datasource_stylesheet = desktop_node_view.datasource_cytoscape_element()
          if datasource_element:
            st.write("datasource relationships")
            cytoscape(
              elements=datasource_element,
              stylesheet=datasource_stylesheet,
              layout={"name": "cose"},
              key=f"datasource_{uploaded_file.name}",
              width="100%",
              height="540px",
            )
          st.write("datasource overview")
          for df in desktop_node_view.datasource_overview():
            st.dataframe(df)
          actions_element, actions_stylesheet = desktop_node_view.actions_overview()
          if actions_element:
            st.write('actions graph')
            cytoscape(
              elements=actions_element,
              stylesheet=actions_stylesheet,
              layout={"name": "cose"},
              key=f"actions_{uploaded_file.name}",
              width="100%",
              height="540px",
            )
          option_dict = desktop_node_view.dashboard_and_layout_in_zone()
          if option_dict:
            st.write('zone graph')
            if option_dict:
              col3, col4 = st.columns(2)
              with col3:
                dashboard_name = st.radio(
                  "dashboard name",
                  option_dict['dashboard_name']
                )
              with col4:
                layout_type = st.radio(
                  "layout type",
                  option_dict['layout_type']
                )
              zone_element, zone_stylesheet = desktop_node_view.zone_elements(dashboard_name, layout_type)
              selected_zone = cytoscape(
                elements=zone_element,
                stylesheet=zone_stylesheet,
                layout={"name": "preset"},
                key=f"zone_{uploaded_file.name}",
                width="100%",
                height="540px",
              )
              node_id = selected_zone["nodes"][-1]
              st.write(node_id)
              for k, dfs in desktop_data.extract_zone_info().items():
                st.write(k)
                temp_df = dfs[
                  (dfs['dashboard_name']==dashboard_name) & 
                  (dfs['layout_type']==layout_type) &
                  (dfs['id']==node_id)
                ]
                st.write(temp_df)
        logger.info(f"[{run_id}][{file_name}] ===== Rendering END =====")
      except Exception as e:
        st.write('some error has occurred')
        logger.exception(f"[{run_id}] Error in extract or convert")
  st.download_button(
    label=f"Download app log",
    data=log_stream.getvalue(),
    file_name=f"app.log",
    mime="text/plain"
  )

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
  """共通のDataFrame整形"""
  df = df.dropna(axis=1, how="all")
  return df

if __name__ == '__main__':
  #ログ設定
  log_stream = io.StringIO()
  root_logger = logging.getLogger()
  handler = logging.StreamHandler(log_stream)
  formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s")
  handler.setFormatter(formatter)
  root_logger.addHandler(handler)
  if st.toggle("debug mode"):
    #デバッグ用pandas設定
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    root_logger.setLevel(logging.DEBUG)
  else:
    root_logger.setLevel(logging.INFO)
  logger = logging.getLogger(__name__)
  main(logger)