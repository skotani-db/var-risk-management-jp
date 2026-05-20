# Databricks notebook source
# MAGIC %md
# MAGIC # 07. モンテカルロシミュレーション
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **Spark の分散処理**: 数万〜数百万の試行を並列実行
# MAGIC - **Delta Lake の最適化**: Liquid Clustering によるクエリ高速化
# MAGIC - **再現性のあるシミュレーション**: シード戦略による結果の再現
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - **計算時間の短縮**: 従来のグリッドコンピューティングで数時間かかる計算をSparkで高速化
# MAGIC - **柔軟なシナリオ分析**: シミュレーション結果を最細粒度で保存し、あらゆる切り口で集計
# MAGIC - **規制対応**: シード固定により結果を完全に再現可能（監査対応）

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

import datetime
from datetime import timedelta
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

# モデル構築以降の毎週に対してモンテカルロシミュレーションを生成
today = datetime.datetime.strptime(config['yfinance']['maxdate'], '%Y-%m-%d')
first = datetime.datetime.strptime(config['model']['date'], '%Y-%m-%d')
run_dates = pd.date_range(first, today, freq='w')

print(f"シミュレーション対象期間: {first.date()} ~ {today.date()}")
print(f"シミュレーション日数: {len(run_dates)} 週")
print(f"試行回数/銘柄: {config['monte-carlo']['runs']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. マーケットボラティリティの取得
# MAGIC
# MAGIC 各シミュレーション日の **最新のボラティリティ統計量** を取得します。
# MAGIC Spark SQL の ASOF JOIN を使って、各日付に対して最も近い過去のボラティリティを結合します。

# COMMAND ----------

# ボラティリティテーブルと実行日テーブルを準備
vol_df = spark.read.table(config['database']['tables']['volatility'])
rdates_df = spark.createDataFrame(pd.DataFrame(run_dates, columns=['date']))

vol_df.createOrReplaceTempView("volatility_data")
rdates_df.createOrReplaceTempView("run_dates")

# ASOF JOIN で各実行日の最新ボラティリティを取得
volatility_df = spark.sql("""
    SELECT
        r.date,
        v.vol_cov,
        v.vol_avg
    FROM run_dates r
    ASOF JOIN volatility_data v
    ON r.date >= v.date
""")

display(volatility_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. シミュレーション試行の分散処理
# MAGIC
# MAGIC ### シード戦略
# MAGIC モンテカルロシミュレーションでは **再現性** が重要です。
# MAGIC 各試行に固有のシード（`trial_id`）を割り当てることで：
# MAGIC - 同じシードなら同じ乱数列が生成される → **完全な再現性**
# MAGIC - 各試行が独立 → **並列処理が安全**
# MAGIC
# MAGIC ### Spark の分散処理
# MAGIC ```
# MAGIC ボラティリティ × 試行ID → クロス結合 → 各ワーカーで独立にシミュレーション
# MAGIC (5週間)      (32,000)    (160,000行)    (並列実行)
# MAGIC ```

# COMMAND ----------

from utils.var_utils import create_seed_df
from utils.var_udf import simulate_market

seed_df = create_seed_df(config['monte-carlo']['runs'])

market_conditions = (
    volatility_df
    .join(spark.createDataFrame(seed_df))
    .withColumn('features', simulate_market('vol_avg', 'vol_cov', 'trial_id'))
    .select('date', 'features', 'trial_id')
)

display(market_conditions.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### シミュレーション結果の保存
# MAGIC
# MAGIC このクロス結合テーブルは **汎用的** です：
# MAGIC - 既知のボラティリティからサンプリングしただけで、特定のモデルに依存しない
# MAGIC - 新しいモデルや新しいトレーディング戦略を、再シミュレーションなしで適用可能

# COMMAND ----------

_ = (
    market_conditions
    .repartition(config['monte-carlo']['executors'], 'date')
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(config['database']['tables']['mc_market'])
)

print(f"保存完了: {config['database']['tables']['mc_market']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. リターンの予測
# MAGIC
# MAGIC MLflow に登録した champion モデルを Spark UDF としてロードし、
# MAGIC シミュレーション市場条件に対する各銘柄のリターンを予測します。

# COMMAND ----------

import mlflow

uc_model_name = "{}.{}.{}".format(
    config['database']['catalog'],
    config['database']['schema'],
    config['model']['name']
)
model_udf = mlflow.pyfunc.spark_udf(
    model_uri='models:/{}@champion'.format(uc_model_name),
    result_type='float',
    spark=spark
)

# COMMAND ----------

simulations = (
    spark.read.table(config['database']['tables']['mc_market'])
    .join(spark.createDataFrame(portfolio_df[['ticker']]))
    .withColumn('return', model_udf(F.struct('ticker', 'features')))
    .drop('features')
)

display(simulations.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ベクトル化と保存
# MAGIC
# MAGIC 各試行の結果を **ベクトル** にまとめることで、
# MAGIC 後続の集計（VaR計算）を効率化します。
# MAGIC
# MAGIC `pyspark.ml.linalg.Vectors` を使って、スパースな試行IDとリターンの
# MAGIC ペアを密ベクトルに変換します。

# COMMAND ----------

from pyspark.ml.linalg import Vectors, VectorUDT

@udf(VectorUDT())
def to_vector(xs, ys):
    v = Vectors.sparse(config['monte-carlo']['runs'], zip(xs, ys)).toArray()
    return Vectors.dense(v)

simulations_vectors = (
    simulations
    .groupBy('date', 'ticker')
    .agg(
        F.collect_list('trial_id').alias('xs'),
        F.collect_list('return').alias('ys')
    )
    .select(
        F.col('date'),
        F.col('ticker'),
        to_vector(F.col('xs'), F.col('ys')).alias('returns')
    )
)

# COMMAND ----------

_ = (
    simulations_vectors
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(config['database']['tables']['mc_trials'])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Liquid Clustering による最適化
# MAGIC
# MAGIC **Liquid Clustering** は Delta Lake の最新の最適化機能で、
# MAGIC クエリパターンに基づいてデータを自動的に再配置します。
# MAGIC
# MAGIC ### 従来のパーティショニングとの違い
# MAGIC | 観点 | パーティショニング | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | 設定変更 | テーブル再作成が必要 | ALTER TABLE で変更可能 |
# MAGIC | 小さいファイル問題 | 発生しやすい | 自動的に解消 |
# MAGIC | 複数カラムでの最適化 | 限定的 | 効果的 |
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > テーブルの最適化状態は、カタログ → テーブル → 「詳細」タブで確認できます。
# MAGIC > `OPTIMIZE` の実行履歴も `DESCRIBE HISTORY` で確認可能です。

# COMMAND ----------

_ = sql('ALTER TABLE {} CLUSTER BY (`date`, `ticker`)'.format(
    config['database']['tables']['mc_trials']
))
_ = sql('OPTIMIZE {}'.format(config['database']['tables']['mc_trials']))

print("Liquid Clustering 適用完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **Spark の分散処理** で数万試行のモンテカルロシミュレーションを並列実行
# MAGIC - **シード戦略** による再現性の確保
# MAGIC - **ASOF JOIN** でシミュレーション日ごとの最新ボラティリティを取得
# MAGIC - **Liquid Clustering** によるDeltaテーブルの最適化
# MAGIC - シミュレーション結果の **ベクトル化** による効率的な保存
# MAGIC
# MAGIC 次のノートブック `08_var_aggregation_compliance` では、
# MAGIC シミュレーション結果を集計して **VaR** を計算し、**バーゼル規制** のバックテストを行います。
