# Databricks notebook source
# MAGIC %md
# MAGIC ### 共通セットアップ
# MAGIC このノートブックは各ノートブックの冒頭で `%run` され、共通の設定を読み込みます。
# MAGIC - Unity Catalog のカタログ・スキーマ・Volumeを作成
# MAGIC - ポートフォリオ定義、市場指標定義を読み込み
# MAGIC - MLflow の実験を設定

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

import yaml
with open('config/application.yaml', 'r') as f:
  config = yaml.safe_load(f)

# COMMAND ----------

# Unity Catalog: カタログとスキーマを作成
_ = sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(
  config['database']['catalog'],
  config['database']['schema']
))

# COMMAND ----------

# Unity Catalog: Volume を作成（CSVアップロード先）
_ = sql("CREATE VOLUME IF NOT EXISTS {}.{}.{}".format(
  config['database']['catalog'],
  config['database']['schema'],
  config['database']['volume']
))

# COMMAND ----------

# デフォルトのカタログとスキーマを設定
_ = sql("USE CATALOG {}".format(config['database']['catalog']))
_ = sql("USE SCHEMA {}".format(config['database']['schema']))

# COMMAND ----------

import pandas as pd
portfolio_df = pd.read_json('config/portfolio.json', orient='records')

# COMMAND ----------

import json
with open('config/indicators.json', 'r') as f:
  market_indicators = json.load(f)

# COMMAND ----------

import mlflow
# Unity Catalog対応のMLflowレジストリを使用
mlflow.set_registry_uri("databricks-uc")
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/value_at_risk'.format(username))

# COMMAND ----------

def teardown():
  """デモ環境のクリーンアップ（全テーブル・スキーマを削除）"""
  _ = sql("DROP SCHEMA IF EXISTS {}.{} CASCADE".format(
    config['database']['catalog'],
    config['database']['schema']
  ))
