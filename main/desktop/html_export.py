import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd


class DesktopHtmlExporter:
  """Build a standalone HTML report from extracted Tableau Desktop data."""

  def __init__(self, view, extracted_data, run_id):
    self.view = view
    self.info, self.datasource_connection, self.zone_info = extracted_data
    self.run_id = run_id
    self.logger = logging.getLogger(self.__class__.__name__)
    template_path = Path(__file__).resolve().parent / 'templates' / 'report.html'
    self.template = template_path.read_text(encoding='utf-8')

  @staticmethod
  def _records(dataframes):
    result = {}
    for name, dataframe in dataframes.items():
      if dataframe is None or dataframe.empty:
        continue
      clean = dataframe.astype(object).where(pd.notna(dataframe), None)
      result[name] = clean.to_dict('records')
    return result

  @staticmethod
  def _json_default(value):
    if isinstance(value, np.integer):
      return int(value)
    if isinstance(value, np.floating):
      return None if np.isnan(value) else float(value)
    if isinstance(value, np.bool_):
      return bool(value)
    if isinstance(value, (set, tuple)):
      return list(value)
    return str(value)

  @classmethod
  def _json_for_script(cls, value):
    return json.dumps(
      value,
      ensure_ascii=False,
      default=cls._json_default,
    ).replace('</', '<\\/')

  def _graph(self, graph_id, title, elements, stylesheet, layout='preset'):
    return {
      'id': graph_id,
      'title': title,
      'elements': elements,
      'stylesheet': stylesheet,
      'layout': layout,
    }

  def build(self, source_name):
    graphs = []
    elements, stylesheet = self.view.cytoscape_elements()
    graphs.append(self._graph('structure', 'Workbook structure', elements, stylesheet, 'breadthfirst'))

    datasource_elements, datasource_stylesheet = self.view.datasource_cytoscape_element()
    for index, (name, items) in enumerate((datasource_elements or {}).items()):
      graphs.append(self._graph(
        f'datasource-{index}',
        f'Datasource: {name}',
        items,
        datasource_stylesheet,
      ))

    try:
      action_elements, action_stylesheet = self.view.actions_overview()
      if action_elements:
        graphs.append(self._graph(
          'actions', 'Dashboard actions', action_elements, action_stylesheet
        ))
    except (KeyError, TypeError, ValueError, AttributeError):
      self.logger.warning('[%s] Actions could not be exported', self.run_id, exc_info=True)

    options = self.view.dashboard_and_layout_in_zone()
    if options:
      for dashboard in sorted(options['dashboard_name']):
        for layout_type in sorted(options['layout_type']):
          zone_elements, zone_stylesheet = self.view.zone_elements(dashboard, layout_type)
          if zone_elements:
            graph_id = f'zone-{len(graphs)}'
            graphs.append(self._graph(
              graph_id,
              f'Layout: {dashboard} / {layout_type}',
              zone_elements,
              zone_stylesheet,
            ))

    details = self._records(self.info)
    details.update({f'zone/{name}': rows for name, rows in self._records(self.zone_info).items()})
    datasource_tables = self._records(self.view.datasource_overview())
    return (
      self.template
      .replace('__REPORT_TITLE__', self._json_for_script(source_name))
      .replace('const graphStore = [];', f'const graphStore = {self._json_for_script(graphs)};')
      .replace('const detailStore = {};', f'const detailStore = {self._json_for_script(details)};')
      .replace(
        'const dataframeStore = {};',
        f'const dataframeStore = {self._json_for_script(datasource_tables)};',
      )
    )
