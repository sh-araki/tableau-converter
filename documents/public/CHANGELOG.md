# Changelog

## Unreleased

- アプリ実装を短い `main/` パッケージ名へ変更し、公開文書を `documents/public/`、Codex向け実装資料を `documents/codex/` へ分離しました。
- サンプル、生成レポート、テスト用スクリプトを `tests/` 配下へ集約しました。
- Python 実装を `main/` パッケージへ集約し、Desktop / Prep / UI / 共通処理の境界を明確にしました。
- HTML を各機能の `templates/`、カスタム画面部品を `ui/components/`、運用スクリプトを `scripts/` へ移動しました。
- 公開文書と内部開発文書の文字化けを修正し、実装済み機能、対応形式、セキュリティ、試験、構成を再整理しました。
- 公開文書から内部資料への参照を除き、公開可能なアーキテクチャ文書を追加しました。
- `.gitignore` をローカル設定、ビルド成果物、一時ファイル、生成レポートに対応させました。

## v3.4

- Streamlit の再実行時にファイル内容単位で Desktop / Prep の変換結果を再利用するようにしました。
- Desktop の HTML レポートへ workbook structure、datasource、dashboard actions、layout、データソース概要表を追加しました。
- 複数ファイルのアップロードとファイル単位のエラー処理に対応しました。
- アプリログを画面からダウンロードできるようにしました。
- XML の外部エンティティとネットワーク参照を無効化し、HTML 埋め込み JSON を安全にしました。
- Prep のフィールド追加、削除、保持、名称変更、結合、ピボットなどの追跡処理を修正しました。

## v3.3

- Dashboard Layout と Workbook structure の表示を改善しました。
- Desktop 表示名、Prep の複合値表示、フィールド削除を修正しました。
- サンプル全件の HTML 変換テストを追加しました。

## v3.2

- Tableau Desktop の `.twb`、`.twbx`、`.tds`、`.tdsx` と HTML レポート出力に対応しました。
- Workbook structure の階層表示、Datasource relationship、Dashboard Layout を追加しました。
- PowerShell の起動・テストスクリプトと統合テストを追加しました。

## Tableau Prep の主な履歴

- フィールド追加・削除・保持・名称変更・結合・集計・ピボット・アンピボットの追跡に対応しました。
- Excel / CSV / SQL 入力、Union、Join、出力ノードの表示に対応しました。
- Cytoscape による画面表示、HTML 出力、フロー構造 JSON 出力に対応しました。
