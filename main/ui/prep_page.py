import pandas as pd
import streamlit as st
from st_cytoscape import cytoscape

from main.prep.main import PrepMainProcess


@st.cache_data
def process_prep(data, _run_id):
  # A leading underscore excludes diagnostic-only run_id from Streamlit's key.
  return PrepMainProcess(data, _run_id).return_dataframes()


def _without_empty_columns(dataframe):
  return dataframe.dropna(axis=1, how='all')


def render_prep(data, file_name, run_id, logger):
  elements, stylesheet, dataframes, node_uml, html = process_prep(data, run_id)
  logger.info('[%s][%s] Prep conversion completed', run_id, file_name)

  html_column, uml_column = st.columns(2)
  with html_column:
    st.download_button(
      'Download as HTML', html, f'{file_name}_cytoscape.html', 'text/html'
    )
  with uml_column:
    st.download_button(
      'Download flow UML', node_uml, f'{file_name}_uml.json', 'application/json'
    )

  selected = cytoscape(
    elements=elements,
    stylesheet=stylesheet,
    layout={'name': 'preset'},
    key=file_name,
    width='100%',
    height='540px',
  )
  selected_nodes = selected.get('nodes', []) if selected else []
  if not selected_nodes:
    return

  node_id = selected_nodes[-1]
  logger.debug('Selected Prep node: %s', node_id)
  for name, dataframe in dataframes.items():
    if 'node_baseid' not in dataframe.columns:
      continue
    selected_rows = dataframe[dataframe['node_baseid'].eq(node_id)]
    st.write(name)
    st.dataframe(_without_empty_columns(selected_rows))
