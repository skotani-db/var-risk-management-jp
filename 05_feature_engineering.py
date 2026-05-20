# Databricks notebook source
# MAGIC %md
# MAGIC # 05. 特徴量エンジニアリングとボラティリティ計算
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **Window 関数**: スライディングウィンドウによるボラティリティ計算
# MAGIC - **ASOF JOIN**: 時系列データの時点結合（Spark SQL ネイティブ機能）
# MAGIC - **対数リターン**: リスクモデルの基本となるリターン計算
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - **パラメトリック VaR の核心**: 過去のボラティリティから将来のリスクを推定
# MAGIC - **マーケットファクター**: 個別銘柄のリスクを市場全体の動きで説明
# MAGIC - **時系列の正確な結合**: 異なるタイムスタンプのデータを正しく結合

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. マーケットリターンの計算
# MAGIC
# MAGIC 市場指標（S&P500、原油、国債等）の **日次対数リターン** を計算します。
# MAGIC 対数リターンは加法性があり、リスク計算に適しています。
# MAGIC
# MAGIC ```
# MAGIC 対数リターン = ln(今日の価格 / 昨日の価格)
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
import numpy as np

def get_market_returns():
    """市場指標の日次対数リターンを計算"""
    f_ret_pdf = spark.table(config['database']['tables']['indicators']).orderBy('date').toPandas()

    f_ret_pdf.index = f_ret_pdf['date']
    f_ret_pdf = f_ret_pdf.drop(columns=['date'])

    # _ingested_at, _source_file 等の監査カラムがあれば除外
    indicator_cols = [c for c in f_ret_pdf.columns if not c.startswith('_')]
    f_ret_pdf = f_ret_pdf[indicator_cols]

    # 日次対数リターンを計算
    f_ret_pdf = np.log(f_ret_pdf.shift(1) / f_ret_pdf)
    f_ret_pdf['date'] = f_ret_pdf.index
    f_ret_pdf = f_ret_pdf.dropna()

    return (
        spark
        .createDataFrame(f_ret_pdf)
        .select(
            F.array([F.col(c) for c in list(market_indicators.values())]).alias('features'),
            F.col('date')
        )
    )

# COMMAND ----------

market_returns_df = get_market_returns()
display(market_returns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. マーケットボラティリティの計算
# MAGIC
# MAGIC パラメトリックVaRの核心は **過去のボラティリティから学習** することです。
# MAGIC Window 関数を使って、各時点で過去X日間分のマーケットボラティリティを計算します。
# MAGIC
# MAGIC ### Window 関数とは
# MAGIC テーブルの各行に対して、前後の行を参照して計算を行う関数です。
# MAGIC `rangeBetween(-days(90), 0)` は「現在の行から過去90日分の行」を参照します。
# MAGIC
# MAGIC ```
# MAGIC 今日のボラティリティ = 過去90日間のリターンの共分散行列
# MAGIC ```

# COMMAND ----------

from pyspark.sql import Window
from utils.var_udf import compute_avg, compute_cov

days = lambda i: i * 86400
volatility_window = Window.orderBy(F.col('date').cast('long')).rangeBetween(
    -days(config['monte-carlo']['volatility']), 0
)

volatility_df = (
    get_market_returns()
    .select(
        F.col('date'),
        F.col('features'),
        F.collect_list('features').over(volatility_window).alias('volatility')
    )
    .filter(F.size('volatility') > 1)
    .select(
        F.col('date'),
        F.col('features'),
        compute_avg(F.col('volatility')).alias('vol_avg'),
        compute_cov(F.col('volatility')).alias('vol_cov')
    )
)

# COMMAND ----------

# Delta テーブルとして保存
volatility_df.write.format('delta').mode('overwrite').saveAsTable(
    config['database']['tables']['volatility']
)

display(spark.read.table(config['database']['tables']['volatility']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 株式リターンの計算
# MAGIC
# MAGIC 各銘柄の日次対数リターンを計算します。
# MAGIC Window 関数で前日の終値を取得し、対数リターンを算出します。

# COMMAND ----------

from pyspark.sql import Window
from utils.var_udf import compute_return

def get_stock_returns():
    """各銘柄の日次対数リターンを計算"""
    window = Window.partitionBy('ticker').orderBy('date').rowsBetween(-1, 0)

    stocks_df = (
        spark.table(config['database']['tables']['stocks'])
        .filter(F.col('close').isNotNull())
        .withColumn("first", F.first('close').over(window))
        .withColumn("return", compute_return('first', 'close'))
        .select('date', 'ticker', 'return')
    )
    return stocks_df

# COMMAND ----------

import datetime
model_date = datetime.datetime.strptime(config['model']['date'], '%Y-%m-%d')
stocks_df = get_stock_returns().filter(F.col('date') < model_date)
display(stocks_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ASOF JOIN による特徴量作成
# MAGIC
# MAGIC マーケット指標データと株式リターンを **時点結合** します。
# MAGIC
# MAGIC ### ASOF JOIN とは
# MAGIC 通常のJOINは完全に一致するキーでしか結合できませんが、
# MAGIC **ASOF JOIN** は「最も近い時刻」で結合します。
# MAGIC
# MAGIC ```
# MAGIC 株式リターン (10:05)  ←ASOF JOIN→  市場指標 (10:03)  ← 10:05以前で最も近い時刻
# MAGIC 株式リターン (10:10)  ←ASOF JOIN→  市場指標 (10:08)
# MAGIC ```
# MAGIC
# MAGIC Spark SQL の `ASOF JOIN` 構文（DBR 13.3+ / Serverless v5 対応）を使用します。

# COMMAND ----------

# 市場指標データを一時ビューとして登録
market_df = (
    spark.read.table(config['database']['tables']['volatility'])
    .filter(F.col('date') < model_date)
    .select('date', 'features')
)

# 各銘柄に対してクロス結合で市場データを紐付け
tickers_df = stocks_df.select('ticker').distinct()
market_with_tickers = market_df.crossJoin(tickers_df)

# ASOF JOIN のため一時ビューを作成
stocks_df.createOrReplaceTempView("stock_returns")
market_with_tickers.createOrReplaceTempView("market_features")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL の ASOF JOIN を使用
# MAGIC
# MAGIC `ON ... AND s.date >= m.date` + `MATCH_CONDITION` で、
# MAGIC 各株式リターンの日付に対して「最も近い過去の市場指標」を結合します。

# COMMAND ----------

features_df = spark.sql("""
    SELECT
        s.date,
        s.ticker,
        m.features,
        s.`return`
    FROM stock_returns s
    ASOF JOIN market_features m
    ON s.ticker = m.ticker AND s.date >= m.date
""")

display(features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 特徴量の相関分析
# MAGIC
# MAGIC リスクモデルを構築する前に、マーケットファクター間の **相関関係** を確認します。
# MAGIC 高い相関を持つファクター（例: S&P500 と ダウ平均）は多重共線性の原因となります。

# COMMAND ----------

import pandas as pd
market_pd = pd.DataFrame(
    market_df.toPandas()['features'].to_list(),
    columns=list(market_indicators.values())
)

# 相関行列を計算（Spearman順位相関）
f_cor_pdf = market_pd.corr(method='spearman', min_periods=12)

# COMMAND ----------

from utils.var_viz import plot_correlation_heatmap
fig_corr, ax_corr = plot_correlation_heatmap(f_cor_pdf, list(market_indicators.values()))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **Window 関数** でスライディングウィンドウによるボラティリティ計算
# MAGIC - **ASOF JOIN** で時系列データの時点結合（tempo ライブラリ不要）
# MAGIC - **対数リターン** の計算と特徴量の作成
# MAGIC - **相関分析** によるマーケットファクター間の関係性把握
# MAGIC
# MAGIC 次のノートブック `06_model_training_mlflow` では、
# MAGIC 作成した特徴量を使って **リスクモデル** を訓練し、**MLflow** で管理します。
