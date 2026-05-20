# Databricks notebook source
# MAGIC %md
# MAGIC # 08. VaR 集計とバーゼル規制コンプライアンス
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **Spark ML Summarizer**: ベクトルの集約関数でVaRを効率的に計算
# MAGIC - **スライス＆ダイス**: 国別・業種別のリスク分解
# MAGIC - **バーゼル規制バックテスト**: VaR閾値超過の検出とカラーゾーン分類
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - **ポートフォリオVaR**: 銘柄間の相関を考慮した統合リスク指標
# MAGIC - **リスク分解**: どの国・業種がリスクに最も寄与しているか
# MAGIC - **バーゼルII/III準拠**: 規制が定めるバックテスト手法の実装

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.stat import Summarizer
from utils.var_udf import weighted_returns, get_var_udf

trials_df = spark.read.table(config['database']['tables']['mc_trials'])
simulation_df = (
    trials_df
    .join(spark.createDataFrame(portfolio_df), ['ticker'])
    .withColumn('weighted_returns', weighted_returns('returns', 'weight'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ポイント・イン・タイムVaR
# MAGIC
# MAGIC 特定の日のシミュレーション結果から、ポートフォリオ全体のVaRを計算します。
# MAGIC
# MAGIC ### 計算の流れ
# MAGIC ```
# MAGIC 各銘柄の試行ベクトル × ウェイト → 加重リターン → 全銘柄合計 → 99パーセンタイル = VaR99
# MAGIC ```

# COMMAND ----------

min_date = trials_df.select(F.min('date').alias('date')).toPandas().iloc[0].date

point_in_time_vector = (
    simulation_df
    .filter(F.col('date') == min_date)
    .groupBy('date')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .toPandas().iloc[0].returns.toArray()
)

# COMMAND ----------

from utils.var_viz import plot_var
plot_var(point_in_time_vector, 99)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. リスクエクスポージャーの推移
# MAGIC
# MAGIC 全期間にわたって VaR99 の推移を確認します。
# MAGIC 市場環境の変化に応じてリスクがどう変動するかを把握できます。

# COMMAND ----------

import matplotlib.pyplot as plt

risk_exposure = (
    simulation_df
    .groupBy('date')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
    .drop('returns')
    .orderBy('date')
    .toPandas()
)

plt.figure(figsize=(20, 8))
plt.plot(risk_exposure['date'], risk_exposure['var_99'])
plt.title('ポートフォリオ全体のVaR99推移')
plt.ylabel('バリュー・アット・リスク')
plt.xlabel('日付')
plt.axhline(y=0, linestyle='--', alpha=0.3, color='gray')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 国別リスク分解
# MAGIC
# MAGIC モンテカルロデータを最細粒度で保持する利点は、
# MAGIC **任意のセグメントでスライス＆ダイス** できることです。
# MAGIC
# MAGIC ポートフォリオマネージャーは「どの国のエクスポージャーが最も大きいか」を
# MAGIC アドホックに分析し、リバランスの判断に活用できます。

# COMMAND ----------

risk_exposure_country = (
    simulation_df
    .groupBy('date', 'country')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
    .drop('returns')
    .orderBy('date')
    .toPandas()
)

fig, ax = plt.subplots(figsize=(20, 8))
for label, df in risk_exposure_country.groupby('country'):
    df.plot.line(x='date', y='var_99', ax=ax, label=label)

plt.title('国別VaR99推移')
plt.ylabel('バリュー・アット・リスク')
plt.xlabel('日付')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 業種別リスク寄与度
# MAGIC
# MAGIC 特定の国（例: ペルー）における業種別リスク寄与度を分析します。
# MAGIC 「全体的なリスクのうち、鉱業への投資にどの程度関連しているか？」

# COMMAND ----------

import pandas as pd
import numpy as np

risk_exposure_industry = (
    simulation_df
    .filter(F.col('country') == 'PERU')
    .groupBy('date', 'industry')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
    .drop('returns')
    .orderBy('date')
    .toPandas()
)

risk_contribution = pd.crosstab(
    risk_exposure_industry['date'],
    risk_exposure_industry['industry'],
    values=risk_exposure_industry['var_99'],
    aggfunc=np.sum
)
risk_contribution = risk_contribution.div(risk_contribution.sum(axis=1), axis=0)
risk_contribution.plot.bar(figsize=(20, 8), colormap="Pastel1", stacked=True, width=0.9)
plt.title('ペルー: 業種別リスク寄与度')
plt.ylabel('寄与割合')
plt.xlabel('日付')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. バーゼル規制バックテスト
# MAGIC
# MAGIC バーゼル委員会は VaR のバックテスト手法を規定しています。
# MAGIC 1日VaR99の結果は日次P&L（損益）と比較され、250日間の超過回数に基づき
# MAGIC 3つのカラーゾーンに分類されます。
# MAGIC
# MAGIC | ゾーン | 超過回数 | 意味 |
# MAGIC |---|---|---|
# MAGIC | **グリーン** | 4回以下 | 特段の懸念なし |
# MAGIC | **イエロー** | 5〜9回 | モニタリングが必要 |
# MAGIC | **レッド** | 10回以上 | VaR指標の改善が必要 |
# MAGIC
# MAGIC ### リスク管理での重要性
# MAGIC - レッドゾーンに入ると **規制上のペナルティ**（追加資本要件）が発生
# MAGIC - バックテスト結果は **四半期ごとに規制当局に報告** する必要がある

# COMMAND ----------

# MAGIC %md
# MAGIC ### 投資リターンの取得

# COMMAND ----------

from pyspark.sql import Window
from utils.var_udf import compute_return

window = Window.partitionBy('ticker').orderBy('date').rowsBetween(-1, 0)

inv_returns_df = (
    spark.table(config['database']['tables']['stocks'])
    .filter(F.col('close').isNotNull())
    .join(spark.createDataFrame(portfolio_df), ['ticker'])
    .withColumn("first", F.first('close').over(window))
    .withColumn("return", compute_return('first', 'close'))
    .withColumn("weighted_return", F.col('return') * F.col('weight'))
    .groupBy('date')
    .agg(F.sum('weighted_return').alias('return'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VaR と投資リターンの時点結合

# COMMAND ----------

risk_exposure_df = (
    simulation_df
    .groupBy('date')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
    .drop('returns')
    .orderBy('date')
)

# pandas merge_asof で時点結合
inv_pd = inv_returns_df.toPandas().sort_values('date')
risk_pd = risk_exposure_df.toPandas().sort_values('date').rename(columns={'date': 'risk_date'})

asof_pd = pd.merge_asof(inv_pd, risk_pd, left_on='date', right_on='risk_date', direction='backward')
asof_pd = asof_pd.dropna(subset=['var_99'])
asof_df = spark.createDataFrame(asof_pd[['date', 'return', 'var_99']]).orderBy('date')

display(asof_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 閾値超過の検出とカラーゾーン分類

# COMMAND ----------

from utils.var_udf import count_breaches

days_fn = lambda i: i * 86400
compliance_window = Window.orderBy(F.col("date").cast("long")).rangeBetween(-days_fn(250), 0)

compliance_df = (
    asof_df
    .withColumn('previous_return', F.collect_list('return').over(compliance_window))
    .withColumn('basel', count_breaches('previous_return', 'var_99'))
    .drop('previous_return')
    .toPandas()
    .set_index('date')
)

idx = pd.date_range(np.min(compliance_df.index), np.max(compliance_df.index), freq='d')
compliance_df = compliance_df.reindex(idx, method='pad')

# COMMAND ----------

# MAGIC %md
# MAGIC ### コンプライアンス結果の可視化

# COMMAND ----------

f, (a0, a1) = plt.subplots(2, 1, figsize=(20, 8), gridspec_kw={'height_ratios': [10, 1]})

a0.plot(compliance_df.index, compliance_df['return'], color='#86bf91', label='リターン')
a0.plot(compliance_df.index, compliance_df['var_99'], label="VaR99", c='red', linestyle='--')
a0.axhline(y=0, linestyle='--', alpha=0.4, color='#86bf91', zorder=1)
a0.title.set_text('VaR99 コンプライアンス（バーゼル規制バックテスト）')
a0.set_ylabel('日次対数リターン')
a0.legend(loc="upper left")

colors = ['green', 'orange', 'red']
a1.bar(compliance_df.index, 1,
       color=[colors[i] for i in compliance_df['basel']],
       label='超過', alpha=0.5, align='edge', width=1.0)
a1.get_yaxis().set_ticks([])
a1.set_xlabel('日付')

plt.subplots_adjust(wspace=0, hspace=0)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **Spark ML Summarizer** でベクトルを効率的に集約し、VaRを計算
# MAGIC - **国別・業種別のリスク分解** によるポートフォリオのリスク構造分析
# MAGIC - **バーゼル規制バックテスト** によるVaRモデルの検証（カラーゾーン分類）
# MAGIC
# MAGIC 次のノートブック `09_dashboard_and_genie` では、
# MAGIC これらの分析結果を **AI/BI Dashboard** と **Genie** で可視化・共有します。
