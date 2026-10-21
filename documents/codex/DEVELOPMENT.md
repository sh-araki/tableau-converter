# 開発ガイド

## セットアップ

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -B -m streamlit run app.py
```

同梱の Windows 埋め込み Python を使う場合は、起動に `scripts/run_app.ps1`、テストに `tests/scripts/run_tests.ps1` を実行する。

## 責務

- `app.py`: アップロード、一時ファイル、処理分岐、ログ UI
- `main/common/`: 形式検証、XML / ZIP / JSON 読み込み、ログ設定
- `main/ui/`: 画面描画、ダウンロード、カスタムコンポーネント
- `main/desktop/`: Desktop XML の抽出、グラフ変換、HTML 組み立て
- `main/prep/`: Prep JSON の抽出、フィールド追跡、HTML 組み立て
- `main/desktop/templates/`: Desktop レポートテンプレート
- `main/prep/templates/`: Prep レポートテンプレート
- `scripts/`: 本番アプリ起動
- `tests/scripts/`: テスト実行とサンプル一括変換

## 変更時の確認

1. 対象形式と Tableau バージョン差を確認する。
2. 抽出結果の空 DataFrame、欠落列、未定義ノードを考慮する。
3. 新しい変換ルールは `prep_maps.json` と処理ハンドラの整合を確認する。
4. HTML へ追加するデータは JSON と HTML の両方で安全にする。
5. 単体テスト後、可能ならローカルサンプル全件を変換する。
6. 利用者に見える変更は `documents/public/`、実装規則と設計判断は `documents/codex/` を更新する。

## ローカル生成物

`python/`, `tests/samples/`, `tests/output/`, `tests/local/`, `tests/private/` は Git 管理対象外。公開テスト用 fixture は匿名化し、専用の公開 fixture ディレクトリを作る場合だけ `.gitignore` の例外を明示する。
