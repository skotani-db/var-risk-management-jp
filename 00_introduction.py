# Databricks notebook source
# MAGIC %md
# MAGIC # VaR（バリュー・アット・リスク）リスク管理 on Databricks
# MAGIC
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要（全て Serverless Runtime にプリインストール済み）
# MAGIC
# MAGIC ## このデモについて
# MAGIC 市場環境の急速な変化やリスク規制の高度化に伴い、リスク計量基盤にも
# MAGIC より高い計算性能・柔軟性・トレーサビリティが求められるようになっています。
# MAGIC 本デモでは、クラウドネイティブなデータ基盤を活用することで、
# MAGIC リスク管理の高度化をどのように実現できるかを具体的に示します。
# MAGIC
# MAGIC 本デモでは、**バリュー・アット・リスク（VaR）** のユースケースを通じて、
# MAGIC Databricks の主要機能を体系的に学びます。
# MAGIC
# MAGIC ## ノートブック構成
# MAGIC | # | ノートブック | 学べる Databricks 機能 | リスク管理での活用 |
# MAGIC |---|---|---|---|
# MAGIC | 00 | Introduction（本ノートブック） | ワークスペース概要 | VaR入門 |
# MAGIC | 01 | Data Upload & Volume | Unity Catalog Volume | データの持ち込み |
# MAGIC | 02 | Auto Loader Ingestion | Auto Loader, 増分処理 | 市場データの継続取込 |
# MAGIC | 03 | Data Quality (Lakeflow) | Lakeflow SDP, Expectations | 異常値検出・品質管理 |
# MAGIC | 04 | Unity Catalog Governance | リネージ, 権限, タグ | 規制対応ガバナンス |
# MAGIC | 05 | Feature Engineering | Window関数, ASOF JOIN | ボラティリティ計算 |
# MAGIC | 06 | Model Training & MLflow | MLflow, Model Registry | モデルの追跡・登録 |
# MAGIC | 07 | Monte Carlo Simulation | Spark分散処理, Liquid Clustering | 大規模シミュレーション |
# MAGIC | 08 | VaR Aggregation & Compliance | Spark ML, バックテスト | バーゼル規制対応 |
# MAGIC | 09 | Dashboard & Genie | AI/BI Dashboard, Genie | リスクレポーティング |
# MAGIC | 10 | Operations & System Tables | System Tables, ジョブ監視 | 運用・コスト管理 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## VaR 入門
# MAGIC
# MAGIC VaR（バリュー・アット・リスク）は、特定の信頼区間における潜在的損失の指標です。
# MAGIC VaR統計量は3つの要素で構成されます：**期間**、**信頼水準**、**損失額（または損失率）**。
# MAGIC
# MAGIC > 「来月中に95%または99%の信頼水準で、最大いくらの損失が見込まれるか？」
# MAGIC
# MAGIC VaRの計算方法には3つのアプローチがあります：
# MAGIC
# MAGIC + **ヒストリカル法**: 実際の過去のリターンを最悪から最良の順に並べ替えるシンプルな手法
# MAGIC + **分散共分散法**: 株式リターンが正規分布に従うと仮定し、確率密度関数を使用
# MAGIC + **モンテカルロシミュレーション**: 将来の株価リターンのモデルを構築し、複数の仮想シナリオを実行
# MAGIC
# MAGIC ### なぜ Databricks でリスク管理か？
# MAGIC - **スケーラビリティ**: モンテカルロシミュレーションを数万〜数百万試行に並列化
# MAGIC - **ガバナンス**: Unity Catalog でデータ・モデル・リネージを一元管理し、規制要件に対応
# MAGIC - **再現性**: MLflow でモデルのバージョン管理・パラメータ追跡を実現
# MAGIC - **リアルタイム性**: Auto Loader + Lakeflow で市場データを継続的に取り込み
# MAGIC - **自然言語分析**: Genie でリスクアナリストが SQL を書かずにアドホック分析

# COMMAND ----------

# MAGIC %md
# MAGIC ## VaR のシンプルな例
# MAGIC 以下では、合成商品に対するシンプルなVaR計算を示します。
# MAGIC ボラティリティ（商品リターンの標準偏差）と時間軸（300日）が与えられています。
# MAGIC
# MAGIC **95%の信頼水準で、300日間に最大いくら損失する可能性があるか？**

# COMMAND ----------

# 時間軸
days = 300

# ボラティリティ
sigma = 0.04

# ドリフト（平均成長率）
mu = 0.05

# 初期価格
start_price = 10

# COMMAND ----------

import matplotlib.pyplot as plt
from utils.var_utils import generate_prices

plt.figure(figsize=(16,6))
for i in range(1, 500):
    plt.plot(generate_prices(start_price, mu, sigma, days))

plt.title('シミュレーション価格')
plt.xlabel("時間")
plt.ylabel("価格")
plt.show()

# COMMAND ----------

from utils.var_viz import plot_var
simulations = [generate_prices(start_price, mu, sigma, days)[-1] for i in range(10000)]
plot_var(simulations, 99)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 期待ショートフォール
# MAGIC 期待ショートフォールは、VaRよりもトレーダーに対してより良いインセンティブを生み出す指標です。
# MAGIC 条件付きVaRまたはテールロスとも呼ばれます。
# MAGIC
# MAGIC - **VaR**: 「最悪の場合どの程度悪くなりうるか？」
# MAGIC - **期待ショートフォール**: 「悪い事態が発生した場合、予想される損失はいくらか？」

# COMMAND ----------

from utils.var_utils import get_var, get_shortfall

print('VaR99: {}'.format(round(get_var(simulations, 99), 2)))
print('期待ショートフォール: {}'.format(round(get_shortfall(simulations, 99), 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## データフローアーキテクチャ
# MAGIC
# MAGIC このデモ全体のデータの流れを示します：
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐    ┌──────────────┐    ┌───────────────────┐
# MAGIC │  CSV ファイル  │───→│ UC Volume    │───→│  Auto Loader      │
# MAGIC │ (手動/システム) │    │ (raw_data)   │    │  (増分取り込み)     │
# MAGIC └─────────────┘    └──────────────┘    └────────┬──────────┘
# MAGIC                                                 │
# MAGIC                    ┌────────────────────────────┼────────────────────┐
# MAGIC                    ▼                            ▼                    ▼
# MAGIC          ┌──────────────┐           ┌────────────────┐    ┌──────────────┐
# MAGIC          │ market_data  │           │market_indicators│    │ Lakeflow SDP │
# MAGIC          │ (株式データ)  │           │ (市場指標)      │    │ (品質チェック) │
# MAGIC          └──────┬───────┘           └───────┬────────┘    └──────────────┘
# MAGIC                 │                           │
# MAGIC                 ▼                           ▼
# MAGIC          ┌──────────────────────────────────────┐
# MAGIC          │      特徴量エンジニアリング (05)       │
# MAGIC          │  Window関数 + 時点結合 → volatility   │
# MAGIC          └──────────────┬───────────────────────┘
# MAGIC                         │
# MAGIC                         ▼
# MAGIC          ┌──────────────────────────────────────┐
# MAGIC          │     MLflow モデル訓練・登録 (06)       │
# MAGIC          │  sklearn + pyfunc → UC Model Registry │
# MAGIC          └──────────────┬───────────────────────┘
# MAGIC                         │
# MAGIC                         ▼
# MAGIC          ┌──────────────────────────────────────┐
# MAGIC          │   モンテカルロシミュレーション (07)      │
# MAGIC          │  Spark 分散処理 → mc_trials テーブル   │
# MAGIC          └──────────────┬───────────────────────┘
# MAGIC                         │
# MAGIC                 ┌───────┴───────┐
# MAGIC                 ▼               ▼
# MAGIC     ┌────────────────┐  ┌──────────────────┐
# MAGIC     │ VaR集計 (08)   │  │ Dashboard (09)   │
# MAGIC     │ バーゼル準拠    │  │ Genie 分析       │
# MAGIC     └────────────────┘  └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## セットアップ: カタログ名の変更
# MAGIC
# MAGIC **重要**: 実行前に `config/configure_notebook.py` を開き、以下の値を自分の環境に変更してください：
# MAGIC
# MAGIC ```python
# MAGIC config = {
# MAGIC   ...
# MAGIC   'database': {
# MAGIC     'catalog': 'shotkotani_demo_ws',  # ← ここを自分のカタログ名に変更
# MAGIC     'schema': 'var_risk_demo',         # ← 必要に応じて変更
# MAGIC     ...
# MAGIC   },
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC > **UI操作**: 左メニュー「カタログ」で利用可能なカタログ名を確認できます。
# MAGIC > カタログ作成権限がない場合は、管理者に依頼してください。
# MAGIC
# MAGIC ## 用語集
# MAGIC
# MAGIC | Databricks 用語 | 説明 |
# MAGIC |---|---|
# MAGIC | **Unity Catalog** | データ・モデル・権限を一元管理する統合ガバナンス基盤 |
# MAGIC | **カタログ** | Unity Catalog の最上位の名前空間（部門・環境単位） |
# MAGIC | **スキーマ** | カタログ内のデータベース（プロジェクト単位） |
# MAGIC | **Volume** | ファイル（CSV, Parquet等）を格納するマネージドストレージ |
# MAGIC | **Delta テーブル** | ACID トランザクション対応の高性能テーブル形式 |
# MAGIC | **Auto Loader** | 新規ファイルを自動検出して増分取り込みする機能 |
# MAGIC | **Lakeflow SDP** | 宣言的データパイプライン（旧 Delta Live Tables） |
# MAGIC | **Expectations** | DLT/SDP のデータ品質ルール（制約チェック） |
# MAGIC | **MLflow** | ML ライフサイクル管理（実験追跡、モデル登録） |
# MAGIC | **Experiment / Run** | MLflow の実験（プロジェクト）と各試行 |
# MAGIC | **Model Registry** | モデルのバージョン管理・エイリアス（champion等） |
# MAGIC | **Liquid Clustering** | Delta テーブルの自動最適化機能 |
# MAGIC | **Genie** | 自然言語でデータに質問できる AI アシスタント |
# MAGIC | **System Tables** | Databricks の利用状況を記録するメタデータテーブル |
# MAGIC | **Serverless** | インフラ管理不要のコンピュート（クラスター作成不要） |
# MAGIC
# MAGIC ## トラブルシューティング
# MAGIC
# MAGIC | エラー | 原因 | 対処法 |
# MAGIC |---|---|---|
# MAGIC | `SCHEMA_NOT_FOUND` | カタログ名が環境と不一致 | `config/configure_notebook.py` の `catalog` を確認 |
# MAGIC | `TABLE_OR_VIEW_NOT_FOUND` | 前のノートブックを未実行 | 依存するノートブックを先に実行 |
# MAGIC | `INSUFFICIENT_PERMISSIONS` | 権限不足 | 管理者にカタログ/スキーマの権限を依頼 |
# MAGIC | `ModuleNotFoundError: yaml` | PyYAML 未インストール | Serverless v5 では不要（config は Python dict） |
# MAGIC | Genie が回答できない | メタデータ不足 | 09 の手順でテーブル/カラムコメントを充実させる |
# MAGIC | System Tables が見つからない | 未有効化 or 権限不足 | 管理者に system tables の有効化を依頼 |
# MAGIC
# MAGIC ## 次のステップ
# MAGIC 次のノートブック `01_data_upload_and_volume` では、実際のポートフォリオデータを
# MAGIC Databricks に持ち込む方法を学びます。Unity Catalog の Volume 機能を使って、
# MAGIC CSV ファイルをアップロードする体験から始めましょう。
