from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
from st_cytoscape import cytoscape
from lxml import etree as ET

from main.desktop.convert import DesktopNodesView
from main.desktop.extract import DesktopInfoExtract
from main.desktop.html_export import DesktopHtmlExporter


COMPONENT_PATH = Path(__file__).resolve().parent / 'components' / 'cyto_tree'


@st.cache_data(hash_funcs={ET._Element: ET.tostring})
def _extract_desktop_data(data_xml, _run_id):
  """Cache expensive XML extraction by document content across UI reruns."""
  extractor = DesktopInfoExtract(data_xml, _run_id)
  return [
    extractor.extract_info(),
    extractor.extract_datasource_connection(),
    extractor.extract_zone_info(),
  ]


def _render_structure(view, info, file_name):
  component = components.declare_component('cyto_tree', path=COMPONENT_PATH)
  elements, stylesheet = view.cytoscape_elements()
  st.write('Information map')
  selected = component(
    elements=elements,
    stylesheet=stylesheet,
    key=f'cyto_{file_name}',
  )
  if selected:
    dataframe = info.get(selected)
    if dataframe is not None:
      st.dataframe(dataframe.reset_index(drop=True))


def _render_datasources(view, file_name):
  elements_by_source, stylesheet = view.datasource_cytoscape_element()
  if not elements_by_source:
    return

  st.write('Datasource relationships')
  datasource_name = st.selectbox(
    'Select datasource', elements_by_source.keys(), key=f'datasource_select_{file_name}'
  )
  if datasource_name:
    cytoscape(
      elements=elements_by_source[datasource_name],
      stylesheet=stylesheet,
      layout={'name': 'preset'},
      key=f'datasource_{file_name}',
      width='100%',
      height='540px',
    )

  st.write('Datasource overview')
  for name, dataframe in view.datasource_overview().items():
    st.write(name)
    st.dataframe(dataframe)


def _render_actions(view, file_name, run_id, logger):
  try:
    elements, stylesheet = view.actions_overview()
    if elements:
      st.write('Actions graph')
      cytoscape(
        elements=elements,
        stylesheet=stylesheet,
        layout={'name': 'preset'},
        key=f'actions_{file_name}',
        width='100%',
        height='540px',
      )
  except (KeyError, TypeError, ValueError, AttributeError) as exc:
    logger.exception('[%s] Error converting action data: %s', run_id, exc)


def _render_zones(view, zone_data, file_name, run_id, logger):
  try:
    options = view.dashboard_and_layout_in_zone()
    if not options:
      return

    st.write('Zone graph')
    dashboard_column, layout_column = st.columns(2)
    with dashboard_column:
      dashboard_name = st.radio(
        'Dashboard name', sorted(options['dashboard_name']), key=f'dashboard_{file_name}'
      )
    with layout_column:
      layout_type = st.radio(
        'Layout type', sorted(options['layout_type']), key=f'layout_{file_name}'
      )

    elements, stylesheet = view.zone_elements(dashboard_name, layout_type)
    selected = cytoscape(
      elements=elements,
      stylesheet=stylesheet,
      layout={'name': 'preset'},
      key=f'zone_{file_name}',
      width='100%',
      height='540px',
    )
    selected_nodes = selected.get('nodes', []) if selected else []
    if not selected_nodes:
      return

    node_id = selected_nodes[-1]
    for name, dataframe in zone_data.items():
      required = {'dashboard_name', 'layout_type', 'id'}
      if not required.issubset(dataframe.columns):
        continue
      rows = dataframe[
        dataframe['dashboard_name'].eq(dashboard_name)
        & dataframe['layout_type'].eq(layout_type)
        & dataframe['id'].eq(node_id)
      ]
      st.write(name)
      st.dataframe(rows)
  except (KeyError, TypeError, ValueError, AttributeError) as exc:
    logger.exception('[%s] Error converting zone data: %s', run_id, exc)


def render_desktop(data_xml, file_name, run_id, logger):
  extracted = _extract_desktop_data(data_xml, run_id)
  view = DesktopNodesView(extracted, run_id)
  html = DesktopHtmlExporter(view, extracted, run_id).build(file_name)

  st.download_button(
    'Download as HTML', html, f'{file_name}_report.html', 'text/html'
  )
  logger.info('[%s][%s] Desktop conversion completed', run_id, file_name)

  _render_structure(view, extracted[0], file_name)
  _render_datasources(view, file_name)
  _render_actions(view, file_name, run_id, logger)
  _render_zones(view, extracted[2], file_name, run_id, logger)
