import streamlit as st
import tempfile
import os
import shutil
import glob
import zipfile
from pathlib import Path
from component.prep import convert_prep
from component.desktop import convert_desktop
import datetime as dt
import time
import xlsxwriter


def main():
  tab1, tab2 = st.tabs(["Prep", "Desktop"])
  with tab1:
    st.title("Tableau Prep's Flow File Converter")
    prep_dict = {}
    uploaded_files = st.file_uploader("Choose your Prep file", type=['tfl', 'tflx'], accept_multiple_files=True)
    if uploaded_files is not None:
      for uploaded_file in uploaded_files:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
          fp = Path(tmp_file.name)
          fp.write_bytes(uploaded_file.getvalue())
          graph_str = convert_prep(fp)
          prep_dict[uploaded_file.name]=graph_str
    if len(prep_dict)>0:
      dict_keys = list(prep_dict.keys())
      st_tabs = st.tabs(dict_keys)
      for i in range(len(dict_keys)):
        with st_tabs[i]:
          st.graphviz_chart(prep_dict[dict_keys[i]])
  with tab2:
    st.title("Tableau Desktop's Workbook File Converter")
    desktop_dict = {}
    uploaded_files = st.file_uploader("Choose your Desktop file ※If '/' in Dashboard or Worksheet name, this converter will not work", type=['twb', 'twbx', 'tds', 'tdsx'], accept_multiple_files=True)
    with tempfile.TemporaryDirectory() as excel_dir:
      excel_dir_path = Path(excel_dir)
      if uploaded_files is not None:
        for uploaded_file in uploaded_files:
          file_name = uploaded_file.name
          file_type = file_name.split('.')[-1]
          with tempfile.TemporaryDirectory() as tmp_dir:
            to_path = Path(tmp_dir)
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
              fp = Path(tmp_file.name)
              if hasattr(uploaded_file, 'getvalue'):
                fp.write_bytes(uploaded_file.getvalue())
              else:
                fp.write_bytes(uploaded_file)
              if file_type == 'tdsx' or file_type == 'twbx':
                with zipfile.ZipFile(fp, 'r') as zip_ref:
                  zip_ref.extractall(to_path)
                graph_list = convert_desktop(to_path, file_name, file_type.replace('x', ''), excel_dir_path)
                desktop_dict[file_name]=graph_list
              else:
                new_name = fp.with_suffix(f'.{file_type}')
                shutil.copy(fp, to_path/new_name.name)
                graph_list = convert_desktop(to_path, file_name, file_type, excel_dir_path)
                desktop_dict[file_name]=graph_list
      if len(desktop_dict)>0:
        dict_keys = list(desktop_dict.keys())
        st_tabs = st.tabs(dict_keys)
        for i in range(len(dict_keys)):
          with st_tabs[i]:
            with open(desktop_dict[dict_keys[i]][2], "rb") as file:
                file_data = file.read()
            st.download_button(
                label=f"Download {dict_keys[i]} Info File",
                data=file_data,
                file_name=f"{dict_keys[i]} info.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.write('datasource graph')
            #st.graphviz_chart(desktop_dict[dict_keys[i]][0])
            st.markdown(f'<div>{desktop_dict[dict_keys[i]][0].pipe(format='svg').decode('utf-8')}</div>', unsafe_allow_html=True)

            st.write('action graph')
            st.graphviz_chart(desktop_dict[dict_keys[i]][1])


if __name__ == '__main__':
  main()
