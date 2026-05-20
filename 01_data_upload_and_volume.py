# Databricks notebook source
# MAGIC %md
# MAGIC # 01. データアップロードと Unity Catalog Volume
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **Unity Catalog Volume**: ファイル（CSV, Parquet等）を格納するマネージドストレージ
# MAGIC - **カタログ・スキーマ・テーブル**: Unity Catalog の3レベル名前空間
# MAGIC - **手動CSVアップロード**: UIからのファイルアップロード体験
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - ポートフォリオ定義や市場データを **一元管理** し、チーム全体で共有
# MAGIC - Volume に格納したファイルは **リネージ追跡** の対象となり、規制対応に活用
# MAGIC - アクセス権限を **Unity Catalog** で統一管理（誰がどのデータを見られるか）
# MAGIC
# MAGIC ## UI操作ポイント
# MAGIC > **Volume へのファイルアップロード手順**:
# MAGIC > 1. 左メニュー「カタログ」→ 対象カタログ → 対象スキーマ → 「Volumes」タブ
# MAGIC > 2. Volume名をクリック → 右上「アップロード」ボタン
# MAGIC > 3. `data/sample_stocks.csv` と `data/sample_indicators.csv` をドラッグ＆ドロップ
# MAGIC >
# MAGIC > このデモではプログラム的にデータを生成しますが、実運用では上記UIから手動アップロードも可能です。

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Unity Catalog の構造を確認
# MAGIC
# MAGIC Unity Catalog は **3レベルの名前空間** でデータを管理します：
# MAGIC - **カタログ**: 最上位の組織単位（例：部門、環境）
# MAGIC - **スキーマ**: カタログ内のデータベース（例：プロジェクト、用途）
# MAGIC - **テーブル/Volume**: 実際のデータ格納先
# MAGIC
# MAGIC ```
# MAGIC catalog_name
# MAGIC   └── schema_name
# MAGIC       ├── table_1        ← 構造化データ（Delta テーブル）
# MAGIC       ├── table_2
# MAGIC       └── volume_name    ← 非構造化ファイル（CSV, 画像, モデル等）
# MAGIC ```
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > 左メニュー「カタログ」から、この階層構造をブラウズできます。
# MAGIC > テーブルをクリックすると、スキーマ情報・サンプルデータ・リネージが確認できます。

# COMMAND ----------

# 現在の設定を確認
print(f"カタログ: {config['database']['catalog']}")
print(f"スキーマ: {config['database']['schema']}")
print(f"Volume:  {config['database']['volume']}")

# COMMAND ----------

# Volume のパスを確認（ファイルアップロード先）
volume_path = "/Volumes/{}/{}/{}".format(
    config['database']['catalog'],
    config['database']['schema'],
    config['database']['volume']
)
print(f"Volume パス: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ポートフォリオの確認
# MAGIC
# MAGIC この演習では、ラテンアメリカの均等加重ポートフォリオ（27銘柄）を使用します。
# MAGIC 実運用では、ポートフォリオ定義はリスク管理システムから取得し、Volume経由でDatabricksに取り込みます。

# COMMAND ----------

display(portfolio_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 株式市場データの生成と保存
# MAGIC
# MAGIC 閉域環境で外部API（Yahoo Finance等）にアクセスできないケースを想定し、
# MAGIC **幾何ブラウン運動**（GBM）ベースのリアルなダミーデータを生成します。
# MAGIC
# MAGIC 実運用では、以下のいずれかの方法でデータを取り込みます：
# MAGIC - 上流のブッキングシステムやマーケットデータ基盤からのファイル → Volume にアップロード
# MAGIC - API連携（Lakeflow Connect）
# MAGIC - 社内データウェアハウスからの連携

# COMMAND ----------

import numpy as np
import pandas as pd

np.random.seed(42)

y_min_date = config['yfinance']['mindate']
y_max_date = config['yfinance']['maxdate']
dates = pd.bdate_range(start=y_min_date, end=y_max_date)
tickers = portfolio_df['ticker'].tolist()

# 各銘柄のパラメータ（実勢値ベース）
ticker_params = {
    'BCH':  {'price': 22.5,  'mu': 0.00015, 'sigma': 0.018},
    'BSAC': {'price': 19.8,  'mu': 0.00010, 'sigma': 0.020},
    'CCU':  {'price': 12.3,  'mu': 0.00005, 'sigma': 0.016},
    'ITCB': {'price': 4.8,   'mu': -0.0001, 'sigma': 0.025},
    'ENIC': {'price': 3.2,   'mu': 0.00008, 'sigma': 0.015},
    'SQM':  {'price': 52.0,  'mu': -0.0002, 'sigma': 0.028},
    'CIB':  {'price': 32.5,  'mu': 0.00012, 'sigma': 0.022},
    'EC':   {'price': 11.2,  'mu': 0.00005, 'sigma': 0.026},
    'AVAL': {'price': 2.4,   'mu': -0.0001, 'sigma': 0.020},
    'AMX':  {'price': 17.8,  'mu': 0.00008, 'sigma': 0.017},
    'AMOV': {'price': 18.2,  'mu': 0.00008, 'sigma': 0.017},
    'CX':   {'price': 7.3,   'mu': 0.00010, 'sigma': 0.024},
    'KOF':  {'price': 92.0,  'mu': 0.00012, 'sigma': 0.014},
    'VLRS': {'price': 8.5,   'mu': 0.00005, 'sigma': 0.032},
    'FMX':  {'price': 132.0, 'mu': 0.00010, 'sigma': 0.015},
    'PAC':  {'price': 172.0, 'mu': 0.00015, 'sigma': 0.018},
    'ASR':  {'price': 285.0, 'mu': 0.00012, 'sigma': 0.019},
    'BSMX': {'price': 8.1,   'mu': 0.00010, 'sigma': 0.021},
    'SIM':  {'price': 28.5,  'mu': 0.00008, 'sigma': 0.023},
    'TV':   {'price': 3.1,   'mu': -0.0003, 'sigma': 0.030},
    'IBA':  {'price': 48.0,  'mu': 0.00005, 'sigma': 0.016},
    'BLX':  {'price': 32.0,  'mu': 0.00015, 'sigma': 0.019},
    'CPA':  {'price': 98.0,  'mu': 0.00012, 'sigma': 0.022},
    'CPAC': {'price': 5.8,   'mu': 0.00005, 'sigma': 0.018},
    'SCCO': {'price': 108.0, 'mu': 0.00018, 'sigma': 0.025},
    'FSM':  {'price': 4.5,   'mu': 0.00010, 'sigma': 0.030},
    'BAP':  {'price': 168.0, 'mu': 0.00015, 'sigma': 0.020},
}

all_records = []
for ticker in tickers:
    params = ticker_params.get(ticker, {'price': 50.0, 'mu': 0.0001, 'sigma': 0.020})
    n_days = len(dates)
    initial_price = params['price']

    # 幾何ブラウン運動 + レジームシフトイベント
    daily_returns = np.random.normal(params['mu'], params['sigma'], n_days)
    n_events = np.random.randint(3, 8)
    event_days = np.random.choice(n_days, n_events, replace=False)
    daily_returns[event_days] += np.random.normal(0, params['sigma'] * 3, n_events)
    price_series = initial_price * np.exp(np.cumsum(daily_returns))

    for i, date in enumerate(dates):
        close = price_series[i]
        intraday_vol = params['sigma'] * 0.6
        open_price = close * np.exp(np.random.normal(0, intraday_vol))
        high = max(open_price, close) * (1 + abs(np.random.normal(0, intraday_vol * 0.5)))
        low = min(open_price, close) * (1 - abs(np.random.normal(0, intraday_vol * 0.5)))
        base_volume = 2_000_000 if initial_price < 20 else 800_000 if initial_price < 100 else 400_000
        volume = float(max(10000, int(np.random.lognormal(np.log(base_volume), 0.5))))
        all_records.append((ticker, pd.Timestamp(date), open_price, high, low, close, volume))

stocks_pdf = pd.DataFrame(all_records, columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'])
print(f"生成データ: {len(tickers)}銘柄 x {len(dates)}営業日 = {len(all_records)}行")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Volume に CSV として保存
# MAGIC 生成したデータを Volume に CSV として保存します。
# MAGIC これにより、**Auto Loader** で増分取り込みするソースファイルとして利用できます。
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > Volume に保存されたファイルは、左メニュー「カタログ」→ Volume名 から確認できます。
# MAGIC > ファイルをクリックするとプレビューが表示されます。

# COMMAND ----------

# Volume に CSV を保存（Auto Loader のソースとして使用）
stocks_csv_path = f"{volume_path}/stocks"
dbutils.fs.mkdirs(stocks_csv_path)

# 日付ごとにファイルを分割（Auto Loaderの増分取り込みをシミュレーション）
for date_str in stocks_pdf['date'].dt.strftime('%Y-%m-%d').unique()[:5]:
    day_df = stocks_pdf[stocks_pdf['date'].dt.strftime('%Y-%m-%d') == date_str]
    spark.createDataFrame(day_df).coalesce(1).write.mode("overwrite").option("header", True).csv(f"{stocks_csv_path}/{date_str}")

print(f"CSV保存先: {stocks_csv_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta テーブルとしても保存
# MAGIC 全データをDeltaテーブルにも保存します。後続のノートブックで利用します。

# COMMAND ----------

(
    spark.createDataFrame(stocks_pdf)
    .write.format('delta').mode('overwrite')
    .saveAsTable(config['database']['tables']['stocks'])
)
display(spark.read.table(config['database']['tables']['stocks']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ローソク足チャートで確認
# MAGIC
# MAGIC Databricks Runtime には **Plotly** がプリインストールされているため、
# MAGIC インタラクティブなチャートをすぐに作成できます。

# COMMAND ----------

from pyspark.sql import functions as F

stock_df = (
  spark
    .read
    .table(config['database']['tables']['stocks'])
    .filter(F.col('ticker') == portfolio_df.iloc[0].ticker)
    .orderBy(F.asc('date'))
    .toPandas()
)

# COMMAND ----------

from utils.var_viz import plot_candlesticks
plot_candlesticks(stock_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. マーケット指標データの生成と保存
# MAGIC
# MAGIC 各資産は S&P500、原油、国債、ダウ平均などの市場指標で説明できると仮定します。
# MAGIC これらの指標は、後ほどリスクモデルの入力特徴量として使用されます。

# COMMAND ----------

# 相関構造を持つ市場指標データを生成
np.random.seed(123)
n_days = len(dates)

# 相関行列: SP500, NYSE, OIL, TREASURY, DOWJONES
corr_matrix = np.array([
    [1.00, 0.92, 0.35, -0.40, 0.96],
    [0.92, 1.00, 0.30, -0.35, 0.90],
    [0.35, 0.30, 1.00, 0.10,  0.32],
    [-0.40,-0.35, 0.10, 1.00, -0.38],
    [0.96, 0.90, 0.32, -0.38, 1.00],
])
L = np.linalg.cholesky(corr_matrix)
uncorrelated = np.random.normal(0, 1, (n_days, 5))
correlated = uncorrelated @ L.T

indicator_params = {
    'SP500':    {'start': 5205.0, 'mu': 0.00035, 'sigma': 0.011},
    'NYSE':     {'start': 18120.0,'mu': 0.00025, 'sigma': 0.009},
    'OIL':      {'start': 78.5,   'mu': 0.00005, 'sigma': 0.022},
    'TREASURY': {'start': 4.32,   'mu': 0.0,     'sigma': 0.008},
    'DOWJONES': {'start': 39170.0,'mu': 0.00030, 'sigma': 0.010},
}
indicator_cols = list(market_indicators.values())

dummy_data = {'date': dates}
for idx, col_name in enumerate(indicator_cols):
    params = indicator_params.get(col_name, {'start': 100, 'mu': 0.0002, 'sigma': 0.015})
    if col_name == 'TREASURY':
        mean_level = 4.0
        kappa = 0.02
        prices = np.zeros(n_days)
        prices[0] = params['start']
        for i in range(1, n_days):
            prices[i] = prices[i-1] + kappa * (mean_level - prices[i-1]) + params['sigma'] * correlated[i, idx]
            prices[i] = max(2.0, min(6.0, prices[i]))
    else:
        returns = params['mu'] + params['sigma'] * correlated[:, idx]
        n_shocks = np.random.randint(2, 5)
        shock_days = np.random.choice(n_days, n_shocks, replace=False)
        returns[shock_days] += np.random.normal(0, params['sigma'] * 2.5, n_shocks)
        prices = params['start'] * np.exp(np.cumsum(returns))
    dummy_data[col_name] = prices

indicators_pdf = pd.DataFrame(dummy_data)

# COMMAND ----------

# Volume に CSV 保存（Auto Loader 用）
indicators_csv_path = f"{volume_path}/indicators"
spark.createDataFrame(indicators_pdf).coalesce(1).write.mode("overwrite").option("header", True).csv(indicators_csv_path)

# Delta テーブルとして保存
(
    spark.createDataFrame(indicators_pdf)
    .write.format('delta').mode('overwrite')
    .saveAsTable(config['database']['tables']['indicators'])
)

display(spark.read.table(config['database']['tables']['indicators']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **Unity Catalog Volume** にファイルを格納する方法（UI / プログラム）
# MAGIC - **3レベル名前空間**（カタログ.スキーマ.テーブル）の概念
# MAGIC - ポートフォリオデータと市場指標データを **Delta テーブル** として保存
# MAGIC
# MAGIC 次のノートブック `02_autoloader_ingestion` では、Volume に置いた CSV ファイルを
# MAGIC **Auto Loader** で増分取り込みする方法を学びます。
