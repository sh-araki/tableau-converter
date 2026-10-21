# Tableau File Converter

Tableau Desktop と Tableau Prep のファイルをローカルで解析し、構造をグラフや表で確認する Streamlit アプリです。Tableau 本体がない環境でも、ワークブック、データソース、ダッシュボード、Prep フローの定義を調査できます。

> 本プロジェクトは Tableau Software, LLC または Salesforce, Inc. の公式製品ではありません。

## 対応ファイル

| 製品 | 形式 | 内容 |
| --- | --- | --- |
| Tableau Desktop | `.twb`, `.tds` | XML を直接解析 |
| Tableau Desktop | `.twbx`, `.tdsx` | ZIP 内の `.twb` または `.tds` を解析 |
| Tableau Prep | `.tfl`, `.tflx` | ZIP 内の `flow` と `displaySettings` を解析 |

複数ファイルを同時にアップロードできます。ファイルごとに処理されるため、1件の失敗が他の結果を妨げません。

## 表示・出力内容

### Tableau Desktop

- workbook / datasource の構造と詳細
- データソースの relationship / join と概要表
- ダッシュボードアクション
- ダッシュボードの既定・端末別レイアウトと zone 詳細
- 上記を切り替えて閲覧できる HTML レポート

### Tableau Prep

- フローノードと接続関係
- ノード、処理、接続、フィールドの詳細表
- フィールド追加・削除・保持・名称変更・結合・ピボットなどの追跡結果
- HTML フロー図とフロー構造 JSON

Debug mode を有効にすると詳細ログが記録されます。アップロード後はアプリログもダウンロードできます。

## インストールと起動

Python 3.10 以上を使用します。依存ライブラリの対応が安定している Python 3.11 または 3.12 を推奨します。

```bash
python -m venv .venv
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -B -m streamlit run app.py
```

Windows 用の配布物に埋め込み Python が含まれている場合は、アプリ起動に `./scripts/run_app.ps1` を利用できます。

アップロードされたファイルは一時ディレクトリで処理され、処理後に削除されます。変換元ファイルは変更しません。

## 制限事項

- Tableau の内部ファイル形式は公開 API ではなく、バージョン差や未対応要素により一部情報を表示できない場合があります。
- ファイルに保存された接続情報や抽出データの正確性・安全性は検証しません。
- HTML のグラフ表示には Cytoscape.js の CDN 接続が必要です。
- アップロードサイズの上限は Streamlit の設定に依存します。
- 公開前にリポジトリのルートへ利用条件を定めた `LICENSE` を追加してください。

詳細は [アーキテクチャ](ARCHITECTURE.md)、[セキュリティ](SECURITY.md)、[コントリビューション](CONTRIBUTING.md)、[変更履歴](CHANGELOG.md)を参照してください。
