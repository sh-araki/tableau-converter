import logging
import sys
import unittest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from main.prep.convert import PrepNodesView


class PrepConvertTests(unittest.TestCase):
  def _build_view(self, convert_map, action_rows, detail_rows):
    view = PrepNodesView.__new__(PrepNodesView)
    view.logger = logging.getLogger('PrepConvertTests')
    view.run_id = 'test'
    view.convert_maps = convert_map
    view.node_masta = pd.DataFrame(action_rows)
    view.action_detail = pd.DataFrame(detail_rows)
    return view

  def test_add_column_defaults_namespace(self):
    view = self._build_view(
      {
        'add_column_action': {
          'type': 'add_column',
          'column': ['field_name', 'expression'],
          'field_target': 'field_name',
        }
      },
      [{
        'node_baseid': 'node-1',
        'id': 'action-1',
        'nodeType': 'add_column_action',
        'name': 'Add Column',
        'description': 'test',
      }],
      [{
        'node_baseid': 'node-1',
        'id': 'action-1',
        'field_name': 'Calculated',
        'expression': '1 + 1',
      }],
    )
    node_fields = pd.DataFrame([
      {'node_baseid': 'node-1', 'namespace': 'Default', 'name': 'Existing'}
    ])
    node_actions = pd.DataFrame([
      {'id': 'action-1', 'nodeType': 'add_column_action', 'namespace': None}
    ])

    updated_fields, action_uml = view.convert_fields_and_uml('node-1', node_fields, node_actions)

    self.assertIn('Calculated', updated_fields['name'].tolist())
    added_row = updated_fields[updated_fields['name'].eq('Calculated')].iloc[0]
    self.assertEqual(added_row['namespace'], 'Default')
    self.assertEqual(action_uml['action-1']['namespace'], 'Default')
    self.assertEqual(action_uml['action-1']['Calculated'], '1 + 1')

  def test_rename_column_respects_namespace(self):
    view = self._build_view(
      {
        'rename_action': {
          'type': 'rename_column',
          'column': ['from_name', 'to_name'],
          'field_target': {'from': 'from_name', 'to': 'to_name'},
        }
      },
      [{
        'node_baseid': 'node-1',
        'id': 'action-1',
        'nodeType': 'rename_action',
        'name': 'Rename',
        'description': 'test',
      }],
      [{
        'node_baseid': 'node-1',
        'id': 'action-1',
        'from_name': 'Sales',
        'to_name': 'Revenue',
      }],
    )
    node_fields = pd.DataFrame([
      {'node_baseid': 'node-1', 'namespace': 'Primary', 'name': 'Sales'},
      {'node_baseid': 'node-1', 'namespace': 'Secondary', 'name': 'Sales'},
    ])
    node_actions = pd.DataFrame([
      {'id': 'action-1', 'nodeType': 'rename_action', 'namespace': 'Primary'}
    ])

    updated_fields, action_uml = view.convert_fields_and_uml('node-1', node_fields, node_actions)

    renamed = updated_fields[updated_fields['namespace'].eq('Primary')]['name'].tolist()
    untouched = updated_fields[updated_fields['namespace'].eq('Secondary')]['name'].tolist()
    self.assertEqual(renamed, ['Revenue'])
    self.assertEqual(untouched, ['Sales'])
    self.assertEqual(action_uml['action-1']['namespace'], 'Primary')
    self.assertEqual(action_uml['action-1']['Sales'], 'Revenue')

  def test_merge_column_keeps_target_and_limits_scope(self):
    view = self._build_view(
      {
        'merge_action': {
          'type': 'merge_column',
          'column': ['from_names', 'to_name'],
          'field_target': {'from': 'from_names', 'to': 'to_name'},
        }
      },
      [{
        'node_baseid': 'node-1',
        'id': 'action-1',
        'nodeType': 'merge_action',
        'name': 'Merge',
        'description': 'test',
      }],
      [{
        'node_baseid': 'node-1',
        'id': 'action-1',
        'from_names': ['City', 'Region'],
        'to_name': 'Region',
      }],
    )
    node_fields = pd.DataFrame([
      {'node_baseid': 'node-1', 'namespace': 'Primary', 'name': 'City'},
      {'node_baseid': 'node-1', 'namespace': 'Primary', 'name': 'Region'},
      {'node_baseid': 'node-1', 'namespace': 'Secondary', 'name': 'City'},
    ])
    node_actions = pd.DataFrame([
      {'id': 'action-1', 'nodeType': 'merge_action', 'namespace': 'Primary'}
    ])

    updated_fields, action_uml = view.convert_fields_and_uml('node-1', node_fields, node_actions)

    remaining_primary = updated_fields[updated_fields['namespace'].eq('Primary')]['name'].tolist()
    remaining_secondary = updated_fields[updated_fields['namespace'].eq('Secondary')]['name'].tolist()
    self.assertEqual(remaining_primary, ['Region'])
    self.assertEqual(remaining_secondary, ['City'])
    self.assertEqual(action_uml['action-1']['from'], ['City', 'Region'])
    self.assertEqual(action_uml['action-1']['to'], 'Region')

  def test_removed_field_is_not_restored_when_node_result_is_saved(self):
    view = self._build_view(
      {
        'input': {'annotations': False},
        'clean': {'annotations': None},
        'remove_action': {
          'type': 'remove_list',
          'column': ['fields'],
          'field_target': 'fields',
        },
      },
      [
        {'node_baseid': 'input-1', 'id': 'input-1', 'nodeType': 'input', 'node_basetype': 'node', 'name': 'Input'},
        {'node_baseid': 'clean-1', 'id': 'clean-1', 'nodeType': 'clean', 'node_basetype': 'node', 'name': 'Clean'},
        {'node_baseid': 'clean-1', 'id': 'remove-1', 'nodeType': 'remove_action', 'node_basetype': 'loom_action', 'name': 'Remove', 'namespace': None},
      ],
      [{'node_baseid': 'clean-1', 'id': 'remove-1', 'fields': ['Removed']}],
    )
    view.initial_nodes = pd.DataFrame([{'node_baseid': 'input-1'}])
    view.initial_node_detail = pd.DataFrame()
    view.fields_info = pd.DataFrame([
      {'node_baseid': 'input-1', 'namespace': 'Default', 'name': 'Kept'},
      {'node_baseid': 'input-1', 'namespace': 'Default', 'name': 'Removed'},
    ])
    view.nextnode_info = pd.DataFrame([
      {'node_baseid': 'input-1', 'nextNodeId': 'clean-1', 'nextNamespace': 'Default'}
    ])
    view.node_baseinfo = lambda: pd.DataFrame([
      {'node_baseid': 'input-1', 'nodeType': 'input', 'name': 'Input'},
      {'node_baseid': 'clean-1', 'nodeType': 'clean', 'name': 'Clean'},
    ])

    node_fields, _ = view.node_fields_and_uml_results()
    clean_fields = node_fields[node_fields['node_baseid'].eq('clean-1')]['name'].tolist()
    self.assertEqual(clean_fields, ['Kept'])


if __name__ == '__main__':
  unittest.main()
