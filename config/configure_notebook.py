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

# matplotlib 日本語フォント設定（文字化け防止）
import matplotlib.pyplot as plt
import subprocess, os

# IPAexGothic フォントをインストール（Serverless 環境対応）
try:
  import matplotlib.font_manager as fm
  # pip で japanize-matplotlib が使えない場合のフォールバック
  subprocess.run(["pip", "install", "-q", "japanize-matplotlib"], check=True, capture_output=True)
  import japanize_matplotlib
except Exception:
  # japanize-matplotlib が入らない場合は IPAexGothic を直接取得
  try:
    font_url = "https://moji.or.jp/wp-content/ipafont/IPAexfont/IPAexfont00401.zip"
    font_dir = "/tmp/fonts"
    os.makedirs(font_dir, exist_ok=True)
    subprocess.run(["wget", "-q", "-O", f"{font_dir}/ipafont.zip", font_url], check=True, capture_output=True)
    subprocess.run(["unzip", "-o", "-q", f"{font_dir}/ipafont.zip", "-d", font_dir], check=True, capture_output=True)
    font_path = os.path.join(font_dir, "IPAexfont00401", "ipaexg.ttf")
    fm.fontManager.addfont(font_path)
    plt.rcParams['font.family'] = 'IPAexGothic'
  except Exception:
    pass  # フォントが取得できない場合はデフォルトのまま

plt.rcParams['axes.unicode_minus'] = False  # マイナス記号の文字化け防止

# COMMAND ----------

# Serverless v5 では PyYAML が未インストールのため、Python dict で設定を定義
# 変更したい場合はここを編集してください
config = {
  'yfinance': {
    'mindate': '2024-05-01',
    'maxdate': '2026-05-01',
  },
  'model': {
    'name': 'value_at_risk',
    'date': '2026-04-01',
  },
  'database': {
    'catalog': 'shotkotani_demo_ws',
    'schema': 'var_risk_demo',
    'volume': 'raw_data',
    'tables': {
      'stocks': 'market_data',
      'indicators': 'market_indicators',
      'volatility': 'market_volatility',
      'mc_market': 'monte_carlo_market',
      'mc_trials': 'monte_carlo_trials',
      'stocks_checkpoint': '_checkpoints/stocks',
      'indicators_checkpoint': '_checkpoints/indicators',
      'stocks_quarantine': 'market_data_quarantine',
      'indicators_quarantine': 'market_indicators_quarantine',
    },
  },
  'monte-carlo': {
    'executors': 20,
    'volatility': 90,
    'runs': 10000,
  },
}

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
