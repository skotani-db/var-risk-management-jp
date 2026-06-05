# Git 差分メモ（var-risk-management-jp）

作成日: 2026-06-04  
ブランチ: main（未コミット変更）

---

## サマリー

| ファイル | 種別 | 変更概要 |
| --- | --- | --- |
| config/configure_notebook.py | 修正 | catalog を `shotkotani_demo_ws` → `takuyaa_azure_w2` に変更 |
| lakeflow/dp_pipeline.py | 修正 | VOLUME_PATH をハードコードから `spark.conf` 参照に変更（パイプライン設定で上書き可能に） |
| 04_unity_catalog_governance.py | 修正 | SQL マジックコマンドを Python `display(sql(...))` に変更、タグ設定セルにタイトル追加 |
| 03_lakeflow_data_quality.py | 修正 | `%run ./config/configure_notebook` セルを追加（共通設定読み込み） |
| 01_data_upload_and_volume.py | 修正 | environment_version ヘッダー追加、テーブル参照を完全修飾名からショート名に変更 |
| 00_introduction.py | 修正 | 末尾改行の修正のみ |
| utils/var_viz.py | 修正 | `fig.show()` → `display(fig)` に変更（Databricks 互換） |
| job_definition.json | 新規 | ジョブ定義ファイル（空） |
| 修正内容リスト.md | 新規 | 修正内容の記録 |
| 修正検討リスト.md | 新規 | 修正検討事項の記録 |

---

## 詳細

### 1. config/configure_notebook.py

- **変更箇所**: `config['database']['catalog']`
- **Before**: `'shotkotani_demo_ws'`
- **After**: `'takuyaa_azure_w2'`
- **理由**: 環境移行（カタログ変更）

### 2. lakeflow/dp_pipeline.py

- **変更箇所**: VOLUME_PATH の定義方法
- **Before**: ハードコード `/Volumes/shotkotani_demo_ws/var_risk_demo/raw_data`
- **After**: `spark.conf.get()` で `pipeline.catalog` / `pipeline.schema` / `pipeline.volume` から動的取得（デフォルト値あり）
- **理由**: パイプライン設定からの上書きを可能にし、環境依存を排除

### 3. 04_unity_catalog_governance.py

- 権限確認セル: `%sql SHOW GRANTS ON SCHEMA var_risk_demo` → `display(sql(f"SHOW GRANTS ON SCHEMA {config['database']['schema']}"))`（動的スキーマ参照）
- タグ設定セルに `# DBTITLE` 追加（`タグ設定 market_data`, `タグ設定 market_indicators`）

### 4. 03_lakeflow_data_quality.py

- 共通設定読み込みセル `%run ./config/configure_notebook` を追加（既存コードが `config` 変数を前提としているため）

### 5. 01_data_upload_and_volume.py

- ファイル先頭に `environment_version = "5"` ヘッダー追加
- SQL セル内のテーブル参照: `shotkotani_demo_ws.var_risk_demo.market_data` → `market_data`（USE CATALOG/SCHEMA 前提のショート名）

### 6. 00_introduction.py

- 末尾の改行文字修正（`No newline at end of file` 解消）

### 7. utils/var_viz.py

- `fig.show()` → `display(fig)` に変更（Plotly の Databricks ノートブック表示対応）

---

## 新規ファイル（ステージ済み）

- `job_e2e_test.json` — ジョブ定義（Notebook 00から11まで順番に実行/test）
- `修正検討リスト.md` — 修正検討中の項目リスト
