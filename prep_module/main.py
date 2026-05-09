from .extract import PrepInfoExtract
from .convert import PrepNodesView
import pandas as pd
import logging
import numpy as np
import json
from pathlib import Path


class PrepMainProcess:
  def __init__(self, data_dict, run_id):
    self.data_dict = data_dict
    self.run_id = run_id
    base_dir = Path(__file__).resolve().parent
    html = base_dir / "index.html"
    self.html = html.read_text(encoding="utf-8")
    self.logger = logging.getLogger(self.__class__.__name__)
    self.run_id = run_id

  def return_dataframes(self):
    run_id = self.run_id
    html = self.html
    prep_flow = PrepInfoExtract(self.data_dict, run_id)
    prep_info = prep_flow.prep_info()
    prep_nodes_view = PrepNodesView(prep_info, run_id)
    elements, stylesheet = prep_nodes_view.cytoscape_elements()
    node_fields, node_uml = prep_nodes_view.node_fields_and_uml_results()
    dfs = {
      "base_info": prep_nodes_view.node_baseinfo(),
      "node_info": prep_info['node_masta'],
      "details": prep_info['action_detail'],
      "nextnode_info": prep_info['nextnode_info'],
      "node_fields": node_fields
    }
    html = self.return_html(elements, stylesheet, dfs, html)
    return elements, stylesheet, dfs, node_uml, html
  
  @staticmethod
  def return_html(elements, stylesheet, dfs, html):
    dfs_copy = {}
    for key, df in dfs.items():
      df = df.replace({np.nan: None})
      data = df.to_dict("records")
      dfs_copy[key] = data
    html = (
      html.replace("const elements = [];", f"const elements = {elements};")
        .replace("style: []", f"style: {stylesheet}")
        .replace("const dfStore = [];", f"const dfStore = {json.dumps(dfs_copy, ensure_ascii=False)};")
    )
    return html