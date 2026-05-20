# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow SDP パイプライン定義
# MAGIC
# MAGIC このノートブックは **DLT パイプラインとして実行** するためのものです。
# MAGIC 通常のノートブックとしては実行できません。
# MAGIC
# MAGIC ## パイプラインの作成手順
# MAGIC 1. 左メニュー「ジョブ」→「Delta Live Tables」→「パイプラインを作成」
# MAGIC 2. ソースコードに `lakeflow/dlt_pipeline.py` を指定
# MAGIC 3. ターゲットスキーマ: `var_risk_demo`
# MAGIC 4. 「開始」をクリック
# MAGIC
# MAGIC ## 補足
# MAGIC 将来的に `databricks.declarative_pipelines` (dp) モジュールが GA になれば、
# MAGIC `import dlt` を `import databricks.declarative_pipelines as dp` に置き換え可能です。

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Volume パス（環境に合わせて変更）
VOLUME_PATH = "/Volumes/shotkotani_demo_ws/var_risk_demo/raw_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: 生データ取り込み（Auto Loader）

# COMMAND ----------

@dlt.table(
    name="bronze_stocks",
    comment="株式市場の生データ（Auto Loaderで取り込み）"
)
def bronze_stocks():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME_PATH}/stocks")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: クレンジング + 品質チェック（Expectations）

# COMMAND ----------

@dlt.table(
    name="silver_stocks",
    comment="品質チェック済みの株式データ"
)
@dlt.expect("valid_ticker", "ticker IS NOT NULL")
@dlt.expect("valid_date", "date IS NOT NULL")
@dlt.expect_or_drop("positive_close", "close > 0")
@dlt.expect_or_drop("positive_volume", "volume > 0")
def silver_stocks():
    return (
        dlt.read_stream("bronze_stocks")
        .withColumn("close", F.col("close").cast("double"))
        .withColumn("open", F.col("open").cast("double"))
        .withColumn("high", F.col("high").cast("double"))
        .withColumn("low", F.col("low").cast("double"))
        .withColumn("volume", F.col("volume").cast("double"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: 異常値検出 + 日次リターン計算

# COMMAND ----------

@dlt.table(
    name="gold_stocks_with_returns",
    comment="日次リターンと異常値フラグを含む株式データ"
)
@dlt.expect_or_drop("valid_return", "ABS(daily_return) < 0.5")
def gold_stocks_with_returns():
    """日次リターンを計算し、極端な値（50%超の日次変動）を除外"""
    from pyspark.sql import Window

    window_prev = Window.partitionBy("ticker").orderBy("date")

    return (
        dlt.read("silver_stocks")
        .withColumn("prev_close", F.lag("close", 1).over(window_prev))
        .filter(F.col("prev_close").isNotNull())
        .withColumn("daily_return", F.log(F.col("close") / F.col("prev_close")))
    )
