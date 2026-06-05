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
  'prefix': '',  # ← ハンズオン時は個人名等を設定（例: 'taro_'）名前衝突を防止します
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
    'volume_adjustments': 'risk_adjustments',
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
    'views': {
      'daily_risk_summary': 'v_daily_risk_summary',
      'portfolio_daily_return': 'v_portfolio_daily_return',
      'country_risk_profile': 'v_country_risk_profile',
    },
  },
  'monte-carlo': {
    'executors': 20,
    'volatility': 90,
    'runs': 10000,
  },
}

# COMMAND ----------

# prefix を適用（テーブル名・Volume名・モデル名・ビュー名の衝突防止）
_prefix = config.get('prefix', '')
if _prefix:
    for _key in ['stocks', 'indicators', 'volatility', 'mc_market', 'mc_trials',
                 'stocks_quarantine', 'indicators_quarantine']:
        config['database']['tables'][_key] = _prefix + config['database']['tables'][_key]
    config['database']['volume'] = _prefix + config['database']['volume']
    config['database']['volume_adjustments'] = _prefix + config['database']['volume_adjustments']
    config['model']['name'] = _prefix + config['model']['name']
    for _key in config['database']['views']:
        config['database']['views'][_key] = _prefix + config['database']['views'][_key]
    print(f"prefix '{_prefix}' を適用しました")

# ショートハンド（各ノートブックで利用）
tbl = config['database']['tables']
vw = config['database']['views']

# COMMAND ----------

# Unity Catalog: スキーマを作成（権限がない場合はスキップ）
try:
    _ = sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(
      config['database']['catalog'],
      config['database']['schema']
    ))
except Exception as e:
    print(f"スキーマ作成をスキップ（既存スキーマを使用します）: {str(e)[:200]}")

# COMMAND ----------

# Unity Catalog: Volume を作成（CSVアップロード先）
_ = sql("CREATE VOLUME IF NOT EXISTS {}.{}.{}".format(
    config['database']['catalog'],
    config['database']['schema'],
    config['database']['volume']
))

# Unity Catalog: 調整用 Volume を作成（Excelアップロード先、リネージで区別するため分離）
_ = sql("CREATE VOLUME IF NOT EXISTS {}.{}.{}".format(
    config['database']['catalog'],
    config['database']['schema'],
    config['database']['volume_adjustments']
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
  """デモ環境のクリーンアップ"""
  _p = config.get('prefix', '')
  if _p:
    # prefix 付きオブジェクトを個別に削除（共有スキーマを壊さない）
    for _v in config['database']['views'].values():
        sql(f"DROP VIEW IF EXISTS {_v}")
    for _k, _v in config['database']['tables'].items():
        if not _k.endswith('_checkpoint'):
            sql(f"DROP TABLE IF EXISTS {_v}")
    sql(f"DROP VOLUME IF EXISTS {config['database']['volume']}")
    sql(f"DROP VOLUME IF EXISTS {config['database']['volume_adjustments']}")
    print(f"prefix '{_p}' のオブジェクトを削除しました")
  else:
    _ = sql("DROP SCHEMA IF EXISTS {}.{} CASCADE".format(
      config['database']['catalog'],
      config['database']['schema']
    ))
