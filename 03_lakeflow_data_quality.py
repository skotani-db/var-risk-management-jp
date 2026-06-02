# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Lakeflow SDP によるデータ品質管理と異常値検出
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **Lakeflow SDP（旧 Delta Live Tables / DLT）**: 宣言的なデータパイプライン
# MAGIC - **Expectations（データ品質ルール）**: 制約違反を検出・隔離
# MAGIC - **異常値検出**: 統計的手法で急騰・急落・欠損を検知
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - 市場データの **異常値を自動検出** し、VaR計算の信頼性を確保
# MAGIC - 品質違反データを **隔離テーブル** に記録し、原因調査を効率化
# MAGIC - パイプラインの品質メトリクスを **ダッシュボード化** し、監査証跡として活用
# MAGIC
# MAGIC ## UI操作ポイント
# MAGIC > **Lakeflow SDP パイプラインの確認方法**:
# MAGIC > 1. 左メニュー「ジョブとパイプライン」→「パイプライン」タブ
# MAGIC > 2. パイプラインをクリック → DAG（有向非巡回グラフ）表示で依存関係を確認
# MAGIC > 3. 各テーブルのメトリクス（行数、品質違反数）を確認
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **注意**: Lakeflow SDP パイプラインは通常、専用ノートブック (`lakeflow/dlt_pipeline.py`)
# MAGIC として定義し、UIまたはAPIからパイプラインを作成して実行します。
# MAGIC このノートブックでは概念説明と、同等のロジックをバッチで実行するデモを行います。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lakeflow SDP（Delta Live Tables）とは
# MAGIC
# MAGIC Lakeflow SDP は、ETL パイプラインを **宣言的** に定義するフレームワークです。
# MAGIC
# MAGIC ### 従来のETL vs Lakeflow SDP
# MAGIC | 観点 | 従来のETL | Lakeflow SDP |
# MAGIC |---|---|---|
# MAGIC | パイプライン定義 | 命令的（順番にコードを書く） | 宣言的（何が欲しいかを定義） |
# MAGIC | データ品質 | 後付けでチェック | **Expectations** で組み込み |
# MAGIC | 依存関係管理 | 手動 | 自動解決 |
# MAGIC | エラーハンドリング | 自前実装 | フレームワークが管理 |
# MAGIC
# MAGIC ### Expectations（データ品質ルール）
# MAGIC ```python
# MAGIC @dp.expect("valid_price", "close > 0")           # 警告のみ（行は通過）
# MAGIC @dp.expect_or_drop("valid_date", "date IS NOT NULL")  # 違反行を除外
# MAGIC @dp.expect_or_fail("valid_ticker", "ticker IS NOT NULL")  # 違反でパイプライン停止
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SDP パイプライン定義の紹介
# MAGIC
# MAGIC `lakeflow/dp_pipeline.py` に完全な DLT パイプライン定義があります。
# MAGIC このパイプラインは以下のテーブルを生成します：
# MAGIC
# MAGIC ```
# MAGIC Volume (CSV)
# MAGIC   ↓ Auto Loader
# MAGIC raw_stocks (Bronze)
# MAGIC   ↓ Expectations: NOT NULL, close > 0, volume > 0
# MAGIC cleaned_stocks (Silver)
# MAGIC   ↓ 統計的異常値検出（Z-score）
# MAGIC validated_stocks (Gold)
# MAGIC ```
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > DLT パイプラインの作成手順:
# MAGIC > 1. 左メニュー「ジョブとパイプライン」→「ETLパイプライン」→「すべてのファイル」タブを選択
# MAGIC > 2. `lakeflow/dp_pipeline.py` を指定し、メニューから「パイプラインのソースコードに追加」を選択
# MAGIC > 3. 右上からターゲットスキーマを設定 → 「開始」

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. バッチでの品質チェック実装
# MAGIC
# MAGIC 以下では、SDP の Expectations と同等のロジックをバッチ処理として実装します。
# MAGIC これにより、SDP を使わない環境でも品質チェックの概念を理解できます。

# COMMAND ----------

from pyspark.sql import functions as F

stocks_df = spark.read.table(config['database']['tables']['stocks'])
print(f"取り込み行数: {stocks_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 基本的な品質チェック（Expectations 相当）
# MAGIC
# MAGIC SDP の `@dp.expect` は、以下のような条件チェックを宣言的に書けます。
# MAGIC ここではバッチ処理で同等のチェックを実装します。

# COMMAND ----------

# 品質ルールの定義
quality_rules = {
    "valid_ticker":  "ticker IS NOT NULL",
    "valid_date":    "date IS NOT NULL",
    "positive_close": "close > 0",
    "positive_volume": "volume > 0",
    "close_not_null": "close IS NOT NULL",
}

# 各ルールの違反件数を集計
print("=== データ品質チェック結果 ===")
total_rows = stocks_df.count()
for rule_name, rule_expr in quality_rules.items():
    violations = stocks_df.filter(f"NOT ({rule_expr})").count()
    status = "PASS" if violations == 0 else "WARN"
    print(f"  [{status}] {rule_name}: 違反 {violations}/{total_rows} 行 ({violations/total_rows*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 品質違反行の隔離
# MAGIC
# MAGIC SDP では `@dp.expect_or_drop` で違反行を自動除外できます。
# MAGIC ここでは違反行を隔離（quarantine）テーブルに分離します。
# MAGIC
# MAGIC **リスク管理の観点**: 隔離データは削除せず保持し、原因調査と監査に活用します。

# COMMAND ----------

# 全ルールを満たす行 = クリーンデータ
all_rules_expr = " AND ".join([f"({rule})" for rule in quality_rules.values()])
clean_df = stocks_df.filter(all_rules_expr)
quarantine_df = stocks_df.filter(f"NOT ({all_rules_expr})")

print(f"クリーンデータ:  {clean_df.count()} 行")
print(f"隔離データ:      {quarantine_df.count()} 行")

# 隔離テーブルに保存
if quarantine_df.count() > 0:
    (
        quarantine_df
        .withColumn("_quarantine_reason", F.lit("basic_quality_check"))
        .withColumn("_quarantine_at", F.current_timestamp())
        .write.format("delta").mode("overwrite")
        .saveAsTable(config['database']['tables']['stocks_quarantine'])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 統計的異常値検出
# MAGIC
# MAGIC 基本的な品質チェックに加え、**統計的な異常値** を検出します。
# MAGIC リスクデータでは以下のような異常が VaR 計算を歪める原因になります：
# MAGIC
# MAGIC - **急騰・急落**: 日次リターンが通常の分布から大きく逸脱（フラッシュクラッシュ等）
# MAGIC - **ゼロボリューム**: 流動性の問題を示唆（取引停止、データ欠損）
# MAGIC - **価格の固着**: 連続する日で価格が全く同じ（データフィードの停止）
# MAGIC
# MAGIC ### Z-score による異常値検出
# MAGIC 日次リターンの Z-score（標準偏差からの乖離度）を計算し、
# MAGIC 閾値（例: |Z| > 3）を超えるデータを異常値としてフラグ付けします。

# COMMAND ----------

from pyspark.sql import Window

# 日次対数リターンを計算
window_prev = Window.partitionBy('ticker').orderBy('date')
returns_df = (
    clean_df
    .withColumn('prev_close', F.lag('close', 1).over(window_prev))
    .filter(F.col('prev_close').isNotNull())
    .withColumn('daily_return', F.log(F.col('close') / F.col('prev_close')))
)

# 銘柄ごとの平均・標準偏差を計算
stats_df = (
    returns_df
    .groupBy('ticker')
    .agg(
        F.avg('daily_return').alias('mean_return'),
        F.stddev('daily_return').alias('std_return')
    )
)

# Z-score を計算
anomaly_df = (
    returns_df
    .join(stats_df, 'ticker')
    .withColumn('z_score',
        (F.col('daily_return') - F.col('mean_return')) / F.col('std_return'))
    .withColumn('is_anomaly', F.abs(F.col('z_score')) > 3)
    .withColumn('anomaly_type',
        F.when(F.col('z_score') > 3, 'SPIKE_UP')
         .when(F.col('z_score') < -3, 'SPIKE_DOWN')
         .otherwise(None))
)

# COMMAND ----------

# 異常値の集計
anomaly_summary = (
    anomaly_df
    .filter(F.col('is_anomaly'))
    .groupBy('ticker', 'anomaly_type')
    .count()
    .orderBy('ticker', 'anomaly_type')
)

print("=== 異常値検出結果（|Z-score| > 3）===")
display(anomaly_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 異常値の可視化

# COMMAND ----------

import matplotlib.pyplot as plt

# 特定銘柄の Z-score 推移を可視化
sample_ticker = portfolio_df.iloc[0].ticker
sample_df = anomaly_df.filter(F.col('ticker') == sample_ticker).orderBy('date').toPandas()

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 10), gridspec_kw={'height_ratios': [3, 2]})

# 価格推移
ax1.plot(sample_df['date'], sample_df['close'], color='#2c7fb8', linewidth=0.8)
anomalies = sample_df[sample_df['is_anomaly']]
ax1.scatter(anomalies['date'], anomalies['close'], color='red', s=50, zorder=5, label='異常値')
ax1.set_title(f'{sample_ticker} - 価格推移と異常値')
ax1.set_ylabel('終値')
ax1.legend()

# Z-score
ax2.bar(sample_df['date'], sample_df['z_score'], color='#86bf91', width=1.0, alpha=0.7)
ax2.axhline(y=3, color='red', linestyle='--', alpha=0.7, label='閾値 (Z=3)')
ax2.axhline(y=-3, color='red', linestyle='--', alpha=0.7)
ax2.set_title(f'{sample_ticker} - Z-score')
ax2.set_ylabel('Z-score')
ax2.set_xlabel('日付')
ax2.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 追加の異常検出: 価格の固着・ゼロボリューム

# COMMAND ----------

# 価格固着の検出（3日連続で同じ終値）
window_3d = Window.partitionBy('ticker').orderBy('date').rowsBetween(-2, 0)
stale_df = (
    clean_df
    .withColumn('close_list', F.collect_list('close').over(window_3d))
    .withColumn('unique_prices', F.size(F.array_distinct(F.col('close_list'))))
    .filter((F.size('close_list') == 3) & (F.col('unique_prices') == 1))
)

stale_count = stale_df.count()
print(f"価格固着（3日連続同値）: {stale_count} 件")

# ゼロまたは極小ボリュームの検出
zero_vol_df = clean_df.filter(F.col('volume') <= 100)
print(f"極小ボリューム（<= 100）: {zero_vol_df.count()} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 品質メトリクスの記録
# MAGIC
# MAGIC 品質チェック結果をメトリクステーブルに記録します。
# MAGIC これは監査証跡として、また品質推移のモニタリングに活用できます。

# COMMAND ----------

import datetime

quality_metrics = [
    {"check_date": datetime.datetime.now(), "metric": "total_rows", "value": float(total_rows)},
    {"check_date": datetime.datetime.now(), "metric": "clean_rows", "value": float(clean_df.count())},
    {"check_date": datetime.datetime.now(), "metric": "anomaly_rows", "value": float(anomaly_df.filter(F.col('is_anomaly')).count())},
    {"check_date": datetime.datetime.now(), "metric": "stale_price_rows", "value": float(stale_count)},
]

metrics_df = spark.createDataFrame(quality_metrics)
display(metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **Lakeflow SDP (DLT)** の概念と Expectations によるデータ品質管理
# MAGIC - **品質ルール** による基本チェック（NULL, 正値, 範囲）
# MAGIC - **Z-score** による統計的異常値検出（急騰・急落）
# MAGIC - **価格固着・ゼロボリューム** の検出
# MAGIC - 品質違反データの **隔離テーブル** への分離
# MAGIC
# MAGIC 次のノートブック `04_unity_catalog_governance` では、
# MAGIC データの **リネージ確認**、**権限管理**、**タグ付け** を学びます。
