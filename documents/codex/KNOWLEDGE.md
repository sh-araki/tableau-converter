# 調査・実装メモ

## ファイル形式

- `.twb`, `.tds` は XML。
- `.twbx`, `.tdsx` は ZIP で、内部の `.twb` または `.tds` を使う。
- `.tfl`, `.tflx` も ZIP として扱い、`flow` と `displaySettings` の JSON が必須。
- Tableau の内部形式はバージョンごとの差があるため、未知の要素を前提にする。

## 実装上の重要点

- Streamlit は UI 操作ごとに再実行される。ファイル内容のハッシュを安定した実行 ID に使い、抽出・変換キャッシュの再利用を可能にしている。
- Desktop の `DesktopNodesView` には `lru_cache` があるが、インスタンス単位の利用が中心。Streamlit 側のキャッシュが再実行間の主要な最適化である。
- Prep のフィールド追跡は namespace を区別する。名称変更、削除、結合で別 namespace の同名フィールドを変更しないこと。
- HTML テンプレートへは JSON を直接埋め込む。Python の辞書表現を埋め込まず、`</script>` を `<\/script>` に置換する。
- 生成 HTML は Cytoscape.js を CDN から取得するため、完全なオフライン表示ではない。

## 公開範囲

`documents/public/` は公開可能な説明のみを置く。`documents/codex/` は実装規則、監査結果、未達要件、リファクタリング候補など内部開発用の情報を置く。実ファイル、生成レポート、接続情報はどちらにも含めない。
