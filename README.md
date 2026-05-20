# VaR Risk Management on Databricks (JP)

Databricks の主要機能を **バリュー・アット・リスク（VaR）** のリスク管理ワークフローを通じて体系的に学ぶデモコンテンツです。

## 前提条件

- **Databricks ワークスペース**: Unity Catalog が有効
- **コンピュート**: Serverless（v5）
- **権限**: カタログ・スキーマ作成権限
- **追加ライブラリ**: 不要（全て Serverless Runtime にプリインストール済み）

## ノートブック構成

| # | ノートブック | 学べる機能 | リスク管理での活用 |
|---|---|---|---|
| 00 | Introduction | ワークスペース概要 | VaR入門 |
| 01 | Data Upload & Volume | Unity Catalog Volume | データの持ち込み |
| 02 | Auto Loader Ingestion | Auto Loader, 増分処理 | 市場データの継続取込 |
| 03 | Data Quality (Lakeflow) | Lakeflow SDP, Expectations | 異常値検出・品質管理 |
| 04 | Unity Catalog Governance | リネージ, 権限, タグ | 規制対応ガバナンス |
| 05 | Feature Engineering | Window関数, ASOF JOIN | ボラティリティ計算 |
| 06 | Model Training & MLflow | MLflow, Model Registry | モデルの追跡・登録 |
| 07 | Monte Carlo Simulation | Spark分散処理, Liquid Clustering | 大規模シミュレーション |
| 08 | VaR Aggregation & Compliance | Spark ML, バックテスト | バーゼル規制対応 |
| 09 | Dashboard & Genie | AI/BI Dashboard, Genie | リスクレポーティング |
| 10 | Operations & System Tables | System Tables, ジョブ監視 | 運用・コスト管理 |

## ディレクトリ構成

```
var-risk-management-jp/
├── 00_introduction.py              # VaR入門 + 環境設定
├── 01_data_upload_and_volume.py    # データアップロード
├── 02_autoloader_ingestion.py      # Auto Loader
├── 03_lakeflow_data_quality.py     # データ品質管理
├── 04_unity_catalog_governance.py  # ガバナンス
├── 05_feature_engineering.py       # 特徴量エンジニアリング
├── 06_model_training_mlflow.py     # モデル訓練
├── 07_monte_carlo_simulation.py    # モンテカルロ
├── 08_var_aggregation_compliance.py # VaR集計・コンプライアンス
├── 09_dashboard_and_genie.py       # ダッシュボード
├── 10_operations_system_tables.py  # 運用監視
├── config/
│   ├── application.yaml            # 環境設定
│   ├── configure_notebook.py       # 共通セットアップ
│   ├── portfolio.json              # ポートフォリオ定義
│   └── indicators.json             # 市場指標定義
├── data/
│   ├── sample_stocks.csv           # サンプル株式データ
│   └── sample_indicators.csv       # サンプル市場指標
├── utils/
│   ├── var_utils.py                # ユーティリティ関数
│   ├── var_udf.py                  # Spark UDF
│   └── var_viz.py                  # 可視化関数
└── lakeflow/
    └── dlt_pipeline.py             # DLTパイプライン定義
```

## セットアップ

1. このリポジトリを Databricks ワークスペースの Repos にクローン
2. `config/application.yaml` の `catalog` を自分の環境に合わせて変更
3. ノートブックを 00 番から順に実行

## 技術仕様

- **Serverless Runtime v5**: 全ノートブックが pip install なしで動作
- **外部依存なし**: scipy, statsmodels, seaborn, tempo, yfinance は一切使用しない
- **閉域環境対応**: インターネット接続なしで完全動作（ダミーデータを自動生成）
