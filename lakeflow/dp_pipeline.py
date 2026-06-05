# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC    
# MAGIC # Lakeflow SDP パイプライン定義
# MAGIC
# MAGIC このノートブックは **Lakeflow Spark Declarative Pipeline として実行** するためのものです。
# MAGIC 通常のノートブックとしては実行できません。
# MAGIC
# MAGIC ## パイプラインの作成手順
# MAGIC 1. 左メニュー「ETLパイプライン」→「パイプラインを作成」
# MAGIC 2. ソースコードに `lakeflow/dlt_pipeline.py` を指定
# MAGIC 3. ターゲットスキーマ: `var_risk_demo`
# MAGIC 4. 「開始」をクリック
# MAGIC
# MAGIC ## 補足
# MAGIC `from pyspark import pipelines as dp` が現在の推奨インポートです。
# MAGIC 旧来の `import dlt` も引き続き動作しますが、新規コードでは `dp` を使用してください。

# COMMAND ----------

# DBTITLE 1,設定
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ハンズオン時は個人名等を設定（例: 'taro_'）名前衝突を防止します
PREFIX = "e2e_"

catalog = spark.conf.get("pipeline.catalog", "shotkotani_demo_ws")
schema = spark.conf.get("pipeline.schema", "var_risk_demo")
volume = PREFIX + "raw_data"
VOLUME_PATH = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: 生データ取り込み（Auto Loader）

# COMMAND ----------

# DBTITLE 1,Cell 4
@dp.table(
    name=f"{PREFIX}bronze_stocks",
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
        .withColumns({
            "_ingested_at": F.current_timestamp(),
            "_source_file": F.col("_metadata.file_path")
        })
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: クレンジング + 品質チェック（Expectations）

# COMMAND ----------

# DBTITLE 1,Cell 6
@dp.table(
    name=f"{PREFIX}silver_stocks",
    comment="品質チェック済みの株式データ"
)
@dp.expect("valid_ticker", "ticker IS NOT NULL")
@dp.expect("valid_date", "date IS NOT NULL")
@dp.expect_or_drop("positive_close", "close > 0")
@dp.expect_or_drop("positive_volume", "volume > 0")
def silver_stocks():
    return (
        spark.readStream.table(f"{PREFIX}bronze_stocks")
        .withColumns({
            "close": F.col("close").cast("double"),
            "open": F.col("open").cast("double"),
            "high": F.col("high").cast("double"),
            "low": F.col("low").cast("double"),
            "volume": F.col("volume").cast("double")
        })
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: 異常値検出 + 日次リターン計算

# COMMAND ----------

# DBTITLE 1,Cell 8
@dp.materialized_view(
    name=f"{PREFIX}gold_stocks_with_returns",
    comment="日次リターンと異常値フラグを含む株式データ"
)
@dp.expect_or_drop("valid_return", "ABS(daily_return) < 0.5")
def gold_stocks_with_returns():
    """日次リターンを計算し、極端な値（50%超の日次変動）を除外"""
    from pyspark.sql import Window

    window_prev = Window.partitionBy("ticker").orderBy("date")

    return (
        spark.read.table(f"{PREFIX}silver_stocks")
        .withColumn("prev_close", F.lag("close", 1).over(window_prev))
        .filter(F.col("prev_close").isNotNull())
        .withColumn("daily_return", F.log(F.col("close") / F.col("prev_close")))
    )
