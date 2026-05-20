# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Auto Loader による増分データ取り込み
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **Auto Loader** (`cloudFiles`): 新しいファイルを自動検出して増分取り込み
# MAGIC - **スキーマ推論・進化**: ファイルのスキーマを自動検出し、変更にも対応
# MAGIC - **チェックポイント**: どのファイルまで処理したかを記録し、重複処理を防止
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - 上流のブッキングシステムやマーケットデータ基盤から配信されるファイルを **自動的に取り込み**
# MAGIC - 取り込み済みファイルの追跡（チェックポイント）により **データの欠落・重複を防止**
# MAGIC - スキーマ進化機能で、上流システムの改修でカラムが追加されても **パイプラインが壊れない**
# MAGIC
# MAGIC ## UI操作ポイント
# MAGIC > Auto Loader のストリーミング状態は、セル実行後に表示されるストリーミングダッシュボードで
# MAGIC > リアルタイムに確認できます。処理レート、バッチサイズ、遅延が表示されます。

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Auto Loader とは
# MAGIC
# MAGIC Auto Loader は、クラウドストレージ（Volume を含む）に到着する新しいファイルを
# MAGIC **自動的に検出** して取り込む Databricks の機能です。
# MAGIC
# MAGIC ```
# MAGIC Volume (CSVファイル)  →  Auto Loader  →  Delta テーブル
# MAGIC   新ファイル到着         自動検出           増分追記
# MAGIC ```
# MAGIC
# MAGIC ### 主な特徴
# MAGIC - **増分処理**: 新しいファイルのみを処理（処理済みファイルはスキップ）
# MAGIC - **スキーマ推論**: CSV/JSON のスキーマを自動検出（手動定義不要）
# MAGIC - **スキーマ進化**: ソースのスキーマ変更を自動検出・対応
# MAGIC - **チェックポイント**: 処理状態を永続化し、障害時のリカバリも安全

# COMMAND ----------

volume_path = "/Volumes/{}/{}/{}".format(
    config['database']['catalog'],
    config['database']['schema'],
    config['database']['volume']
)

stocks_source = f"{volume_path}/stocks"
indicators_source = f"{volume_path}/indicators"

print(f"株式データソース:   {stocks_source}")
print(f"市場指標ソース:     {indicators_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 株式データの Auto Loader 取り込み
# MAGIC
# MAGIC `cloudFiles` フォーマットを指定し、Volume 内の CSV ファイルを増分取り込みします。
# MAGIC
# MAGIC ### 重要なオプション
# MAGIC - `cloudFiles.format`: ソースファイル形式（csv, json, parquet等）
# MAGIC - `cloudFiles.schemaLocation`: スキーマ推論結果の保存先
# MAGIC - `cloudFiles.inferColumnTypes`: 型推論を有効化（デフォルトは全て string）
# MAGIC - `header`: CSVのヘッダー行を使用

# COMMAND ----------

from pyspark.sql import functions as F

# チェックポイントパス（Auto Loader の処理状態を保存）
stocks_checkpoint = f"{volume_path}/{config['database']['tables']['stocks_checkpoint']}"
stocks_schema_location = f"{volume_path}/_schema/stocks"

# Auto Loader でストリーミング読み込み
stocks_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", stocks_schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(stocks_source)
    # 取り込み時刻を付与（監査証跡として有用）
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta テーブルへの書き込み
# MAGIC
# MAGIC Auto Loader で読み取ったストリームを Delta テーブルに書き込みます。
# MAGIC `trigger(availableNow=True)` は、現在利用可能な全ファイルを処理して停止します。
# MAGIC
# MAGIC ### 本番運用では
# MAGIC - `trigger(availableNow=True)` → ジョブとしてスケジュール実行（日次バッチ）
# MAGIC - `trigger(processingTime="1 minute")` → 継続的なストリーミング処理
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > セル実行後、ストリーミングの進捗状況がリアルタイムで表示されます。
# MAGIC > 「Input」「Processing」「Batch」の各メトリクスを確認してください。

# COMMAND ----------

stocks_table = config['database']['tables']['stocks']

(
    stocks_stream.writeStream
    .format("delta")
    .option("checkpointLocation", stocks_checkpoint)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(stocks_table)
    .awaitTermination()
)

print(f"取り込み完了: {stocks_table}")

# COMMAND ----------

# 取り込み結果を確認
display(
    spark.read.table(stocks_table)
    .orderBy(F.desc("_ingested_at"))
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 市場指標データの Auto Loader 取り込み

# COMMAND ----------

indicators_checkpoint = f"{volume_path}/{config['database']['tables']['indicators_checkpoint']}"
indicators_schema_location = f"{volume_path}/_schema/indicators"
indicators_table = config['database']['tables']['indicators']

indicators_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", indicators_schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(indicators_source)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(
    indicators_stream.writeStream
    .format("delta")
    .option("checkpointLocation", indicators_checkpoint)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(indicators_table)
    .awaitTermination()
)

print(f"取り込み完了: {indicators_table}")

# COMMAND ----------

display(spark.read.table(indicators_table).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 増分取り込みの確認
# MAGIC
# MAGIC Auto Loader の最大の利点は **増分処理** です。
# MAGIC 同じセルを再度実行しても、既に処理済みのファイルはスキップされます。
# MAGIC
# MAGIC 新しいファイルを Volume に追加すると、次回実行時にそのファイルのみが取り込まれます。
# MAGIC
# MAGIC ### リスク部門での運用例
# MAGIC ```
# MAGIC [毎朝 7:00] 上流のブッキングシステムが新しい市場データCSVを Volume に配置
# MAGIC      ↓
# MAGIC [毎朝 7:30] スケジュールジョブが Auto Loader ノートブックを実行
# MAGIC      ↓
# MAGIC [自動] 新ファイルのみを検出 → Delta テーブルに追記
# MAGIC      ↓
# MAGIC [毎朝 8:00] 後続の VaR 計算パイプラインが起動
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **Auto Loader** (`cloudFiles`) でファイルを自動検出・増分取り込み
# MAGIC - **スキーマ推論** でCSVの型を自動検出
# MAGIC - **チェックポイント** で処理済みファイルを追跡し、重複を防止
# MAGIC - 取り込み時に **監査列**（`_ingested_at`, `_source_file`）を付与
# MAGIC
# MAGIC 次のノートブック `03_lakeflow_data_quality` では、取り込んだデータの
# MAGIC **品質チェック** と **異常値検出** を Lakeflow SDP で実現する方法を学びます。
