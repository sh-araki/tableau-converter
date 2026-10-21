# テスト方針

## 実行

```powershell
./tests/scripts/run_tests.ps1
```

または:

```bash
python -B -m unittest discover -s tests -v
```

## 現在の自動テスト

| ファイル | 対象 |
| --- | --- |
| `test_file_classifier.py` | 通常 XML、Prep ZIP、未対応拡張子 |
| `test_prep_convert.py` | フィールド追加、namespace 別名称変更、結合、削除伝播 |
| `test_prep_html.py` | JSON 値、複合値表示、`</script>` 無害化 |
| `test_samples.py` | ローカルサンプル全件の解析、グラフ、HTML 出力 |

## 追加が必要なテスト

- `.twbx` / `.tdsx` に対象 XML がない場合と、不正 XML のエラー
- Prep ZIP の必須メンバー不足、不正 JSON
- ZIP 内に複数の対象 XML がある場合の選択仕様
- Desktop HTML における埋め込み文字列の script 終端無害化
- 空 workbook、空 datasource、空 dashboard に対する回帰テスト
- 大容量または異常な ZIP に対する制限実装後の境界値テスト

ローカル `tests/samples/` の統合テストは実ファイル依存のため、公開 CI の必須テストにはできない。公開 CI を追加する際は匿名化した最小 fixture を別途用意する。`tests/scripts/export_samples.py` は同じサンプルを読み、目視確認用HTMLを `tests/output/` に出力する補助ツールであり、自動テスト本体ではない。
