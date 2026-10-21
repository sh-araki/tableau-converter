# アーキテクチャ

## 処理の流れ

1. `app.py` が複数ファイルのアップロード、ログ、エラー表示を管理します。
2. `main/common/file_classifier.py` が拡張子を検証し、XML または ZIP 内の定義を安全な設定で読み込みます。
3. `main/ui/desktop_page.py` または `main/ui/prep_page.py` が対象形式に応じた解析処理を呼び出します。
4. 抽出モジュールが XML / JSON を表形式へ変換し、変換モジュールがグラフ要素と詳細情報を組み立てます。
5. Streamlit 上の対話表示と、ダウンロード可能な HTML / JSON を生成します。

## ディレクトリ

| パス | 役割 |
| --- | --- |
| `app.py` | Streamlit のエントリポイント |
| `main/common/` | ファイル判定、安全な読み込み、ログ設定 |
| `main/ui/` | Desktop / Prep の画面構成とカスタムコンポーネント |
| `main/desktop/` | Desktop ファイルの抽出、グラフ変換、HTML 出力 |
| `main/prep/` | Prep ファイルの抽出、フィールド追跡、HTML 出力 |
| `main/*/templates/` | ダウンロード用 HTML テンプレート |
| `scripts/` | 本番アプリの起動スクリプト |
| `documents/public/` | 公開可能な利用者・開発者向け文書 |
| `python/` | Windows 配布用の埋め込み Python（Git 管理対象外） |

テスト一式とCodex向け実装資料は内部開発用として公開対象から除外しています。

## キャッシュと HTML 出力

Desktop の XML 抽出と Prep の変換結果は Streamlit のデータキャッシュを使用します。アップロード内容の SHA-256 先頭8文字を実行 ID として使うため、画面操作による再実行でも同一ファイルのキャッシュを再利用できます。

Desktop と Prep は各機能配下の `templates/report.html` を使用します。データを JSON として埋め込み、ブラウザ側でグラフと表を描画します。Desktop の画面内構造ツリーは `ui/components/cyto_tree/` に分離されています。生成物は元ファイル由来の情報を含むため、公開前に確認してください。
