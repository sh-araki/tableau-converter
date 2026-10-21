# 実装・文書監査

更新日: 2026-09-25

## 今回の対応

- アプリパッケージを `tableau_converter/` から `main/` へ変更した。
- 公開文書を `documents/public/`、Codex向け資料を `documents/codex/` へ分離した。
- サンプル、生成レポート、テスト用スクリプトを `tests/` 配下へ集約し、本番起動スクリプトだけをルートの `scripts/` に残した。
- Python 実装を `main/` へ集約し、`common/`、`desktop/`、`prep/`、`ui/` をサブパッケージ化した。
- HTML レポートを機能別 `templates/report.html`、画面用 HTML を `ui/components/cyto_tree/` へ整理した。
- 起動、テスト、サンプル出力を `scripts/` へ移動し、移動後もプロジェクトルートを正しく解決するよう修正した。
- 文字化けしていた公開文書と内部文書を UTF-8 の日本語で再構成した。
- 実装済みの複数アップロード、キャッシュ、ログ出力、Desktop HTML、Prep JSON、セキュリティ対策を文書化した。
- 公開 README から内部資料への参照を外し、公開用 `documents/public/ARCHITECTURE.md` を追加した。
- `.gitignore` に生成レポート、ビルド成果物、追加のツールキャッシュ、一時ファイル、ローカル設定を追加した。

## ディレクトリ統合の評価

トップレベルに分散していた実装は `main/` に統合済み。Desktop と Prep は入力形式と処理モデルが異なるため、サブパッケージとして分離を維持する。

次の整理は有効:

1. `app.py` は Streamlit の起動規約に沿う薄いエントリポイントとしてルートに維持する。
2. `desktop/` と `prep/` の重複する JSON / HTML 変換処理は、仕様差を明確にできる場合だけ共通化する。
3. `python/` は約328 MB、`tests/output/` と `tests/samples/` は各約7 MBあり、ソース構成とは分離できている。配布工程ではリポジトリ外から生成・配置する方式が望ましい。

## 優先課題

1. 公開前に `LICENSE` と実際の脆弱性報告先を決める。
2. ZIP bomb 対策としてメンバー数、個別・合計非圧縮サイズ、圧縮率の上限を実装する。
3. `requirements.txt` のバージョン方針と再現可能な配布手順を定める。
4. 匿名化 fixture を用意し、サンプルなしでも Desktop / Prep の統合テストを実行できるようにする。
5. コメントアウトされた import と古いコメントを整理し、Python の標準的な4スペースインデントへ段階的に統一する。
