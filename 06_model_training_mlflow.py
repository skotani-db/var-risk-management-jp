# Databricks notebook source
# MAGIC %md
# MAGIC # 06. モデル訓練と MLflow によるモデル管理
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **MLflow Experiment**: モデル訓練の実験管理（パラメータ、メトリクス、成果物の記録）
# MAGIC - **MLflow pyfunc**: 任意のロジックをMLflowモデルとしてパッケージ化
# MAGIC - **Unity Catalog Model Registry**: モデルのバージョン管理・エイリアス・権限管理
# MAGIC - **モデルシグネチャ**: 入出力スキーマの強制によるデータドリフト防止
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - **モデルリスク管理 (SR 11-7)**: モデルの全バージョンと訓練パラメータを追跡
# MAGIC - **再現性**: 任意の時点のモデルを正確に再現し、バックテストに活用
# MAGIC - **承認フロー**: `champion`/`challenger` エイリアスでモデルの昇格管理
# MAGIC - **データセット追跡**: モデルに使用したデータのバージョンを記録
# MAGIC
# MAGIC ## UI操作ポイント
# MAGIC > **MLflow Experiment の確認方法**:
# MAGIC > 1. 左メニュー「Experiments」→ 実験名をクリック
# MAGIC > 2. 各 Run のパラメータ・メトリクス・成果物を比較
# MAGIC >
# MAGIC > **Model Registry の確認方法**:
# MAGIC > 1. 左メニュー「カタログ」→ カタログ → スキーマ → 「モデル」タブ
# MAGIC > 2. モデルバージョン、エイリアス（champion等）、リネージを確認

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

import datetime
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql import Window

model_date = datetime.datetime.strptime(config['model']['date'], '%Y-%m-%d')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. MLflow とは
# MAGIC
# MAGIC MLflow は ML ライフサイクル全体を管理するプラットフォームです。
# MAGIC
# MAGIC ### 4つの主要コンポーネント
# MAGIC | コンポーネント | 説明 | リスク管理での活用 |
# MAGIC |---|---|---|
# MAGIC | **Tracking** | パラメータ・メトリクス・成果物の記録 | モデル訓練の全履歴を監査可能に |
# MAGIC | **Models** | モデルのパッケージング（pyfunc等） | 任意のリスクモデルを統一的に管理 |
# MAGIC | **Registry** | モデルのバージョン管理・エイリアス | champion/challenger の昇格管理 |
# MAGIC | **Serving** | モデルのデプロイ・推論 | リアルタイムリスク計算 |
# MAGIC
# MAGIC ### Experiment と Run
# MAGIC ```
# MAGIC Experiment: "value_at_risk"    ← プロジェクト単位
# MAGIC   ├── Run 1: 線形回帰モデル     ← 各試行
# MAGIC   │   ├── パラメータ: {volatility_window: 90, features: 5}
# MAGIC   │   ├── メトリクス: {wsse: 0.023}
# MAGIC   │   └── 成果物: model, correlation_plot.png
# MAGIC   ├── Run 2: 非線形モデル
# MAGIC   └── Run 3: ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 訓練データの準備
# MAGIC
# MAGIC 前のノートブックで計算した特徴量（マーケットファクター）と
# MAGIC 株式リターンを結合して、訓練データセットを作成します。

# COMMAND ----------

import pandas as pd

# マーケットファクターのリターン
market_df = (
    spark.read.table(config['database']['tables']['volatility'])
    .filter(F.col('date') < model_date)
    .select('date', 'features')
)
market_pd = pd.DataFrame(
    market_df.toPandas()['features'].to_list(),
    columns=list(market_indicators.values())
)

# COMMAND ----------

# 株式リターンの計算
from utils.var_udf import compute_return

window = Window.partitionBy('ticker').orderBy('date').rowsBetween(-1, 0)
stocks_df = (
    spark.table(config['database']['tables']['stocks'])
    .filter(F.col('close').isNotNull())
    .withColumn("first", F.first('close').over(window))
    .withColumn("return", compute_return('first', 'close'))
    .select('date', 'ticker', 'return')
    .filter(F.col('date') < model_date)
)

# COMMAND ----------

# 時点結合で特徴量を結合（pandas merge_asof）
import pandas as pd

stocks_pd = stocks_df.toPandas().sort_values('date')
market_pd_asof = market_df.select(F.col('date').alias('market_date'), 'features').toPandas().sort_values('market_date')

result_dfs = []
for ticker in stocks_pd['ticker'].unique():
    ticker_df = stocks_pd[stocks_pd['ticker'] == ticker].copy()
    merged = pd.merge_asof(ticker_df, market_pd_asof, left_on='date', right_on='market_date', direction='backward')
    result_dfs.append(merged)

features_pd = pd.concat(result_dfs, ignore_index=True).dropna(subset=['features'])
features_df = spark.createDataFrame(features_pd[['date', 'ticker', 'features', 'return']])

display(features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. モデルの訓練
# MAGIC
# MAGIC 各銘柄ごとに **線形回帰** モデルを訓練します。
# MAGIC pandas UDF を使って **並列訓練** を実行します。
# MAGIC
# MAGIC ### なぜ pyfunc か？
# MAGIC 実際のリスクモデルは、scikit-learn や TensorFlow の既成モデルではなく、
# MAGIC カスタムロジック（非線形特徴量 + OLS等）を含むことが多いです。
# MAGIC `mlflow.pyfunc` なら **任意のPythonオブジェクト** をMLflowモデルとして管理できます。

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from utils.var_utils import non_linear_features

# sklearn で OLS 回帰（statsmodels の代替）
train_model_schema = StructType([
    StructField('ticker', StringType(), True),
    StructField('weights', ArrayType(FloatType()), True)
])

@pandas_udf(train_model_schema, PandasUDFType.GROUPED_MAP)
def train_model(group, pdf):
    import pandas as pd
    import numpy as np
    from sklearn.linear_model import LinearRegression

    X = np.array([non_linear_features(row) for row in np.array(pdf['features'])])
    y = np.array(pdf['return'])

    model = LinearRegression(fit_intercept=True)
    model.fit(X, y)

    # 重み: [intercept, coef1, coef2, ...]
    weights = [float(model.intercept_)] + [float(c) for c in model.coef_]
    w_df = pd.DataFrame(data=[[weights]], columns=['weights'])
    w_df['ticker'] = group[0]
    return w_df

# COMMAND ----------

# 全銘柄を並列で訓練
model_df = features_df.groupBy('ticker').apply(train_model).toPandas()
print(f"訓練済みモデル数: {len(model_df)}")
display(model_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MLflow pyfunc モデルの作成
# MAGIC
# MAGIC 訓練した全銘柄の重みを1つの `pyfunc` モデルにパッケージ化します。
# MAGIC
# MAGIC ### pyfunc モデルの構造
# MAGIC ```python
# MAGIC class RiskMLFlowModel(PythonModel):
# MAGIC     def __init__(self, model_df):     # 全銘柄の重みを保持
# MAGIC     def predict(self, context, input): # 銘柄 + 特徴量 → リターン予測
# MAGIC ```
# MAGIC
# MAGIC ### メリット
# MAGIC - 統計モデル、ML モデル、ルールベースなど **何でも pyfunc としてパッケージ化** 可能
# MAGIC - モデルの **入出力シグネチャ** を強制し、誤った入力データを防止
# MAGIC - Unity Catalog で **バージョン管理・権限管理** を一元化

# COMMAND ----------

import mlflow
from mlflow.pyfunc import PythonModel

class RiskMLFlowModel(PythonModel):

    def __init__(self, model_df):
        self.weights = dict(zip(model_df.ticker, model_df.weights))

    @staticmethod
    def _non_linear_features(xs):
        import numpy as np
        fs = []
        for x in xs:
            fs.append(x)
            fs.append(np.sign(x) * x ** 2)
            fs.append(x ** 3)
            fs.append(np.sign(x) * np.sqrt(abs(x)))
        return fs

    @staticmethod
    def _predict_non_linears(ps, fs):
        s = ps[0]
        for i, f in enumerate(fs):
            s = s + ps[i + 1] * f
        return float(s)

    def _predict_record(self, ticker, xs):
        ps = self.weights[ticker]
        fs = self._non_linear_features(xs)
        return self._predict_non_linears(ps, fs)

    def predict(self, context, model_input):
        predicted = model_input[['ticker','features']].apply(
            lambda x: self._predict_record(*x), axis=1
        )
        return predicted

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MLflow にモデルを記録・登録
# MAGIC
# MAGIC ### 記録されるもの
# MAGIC - **パラメータ**: ボラティリティウィンドウ、銘柄数、特徴量数
# MAGIC - **メトリクス**: WSSE（加重二乗誤差の合計）
# MAGIC - **成果物**: モデルオブジェクト、相関行列プロット
# MAGIC - **シグネチャ**: 入出力の型情報
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > モデル登録後、以下で確認できます：
# MAGIC > 1. 左メニュー「Experiments」→ 実験名 → Run をクリック
# MAGIC > 2. 「パラメータ」「メトリクス」「成果物」タブで詳細確認
# MAGIC > 3. 左メニュー「カタログ」→ スキーマ → 「モデル」タブでレジストリを確認

# COMMAND ----------

from mlflow.models.signature import infer_signature

uc_model_name = "{}.{}.{}".format(
    config['database']['catalog'],
    config['database']['schema'],
    config['model']['name']
)

with mlflow.start_run(run_name='value-at-risk') as run:
    run_id = run.info.run_id

    # パラメータを記録
    mlflow.log_param("volatility_window_days", config['monte-carlo']['volatility'])
    mlflow.log_param("num_tickers", len(model_df))
    mlflow.log_param("num_features", len(list(market_indicators.values())))
    mlflow.log_param("model_type", "linear_regression_with_nonlinear_features")
    mlflow.log_param("model_date", config['model']['date'])

    # pyfunc モデルを作成
    python_model = RiskMLFlowModel(model_df)

    # シグネチャを推論（入出力の型を自動検出）
    model_input_df = features_df.select('ticker', 'features').limit(10).toPandas()
    model_output_df = python_model.predict(None, model_input_df)
    model_signature = infer_signature(model_input_df, model_output_df)

    # モデルをログ + Unity Catalog に登録
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=python_model,
        signature=model_signature,
        registered_model_name=uc_model_name
    )

    # champion エイリアスを設定
    # 登録済みモデルの最新バージョンを取得
    client = mlflow.tracking.MlflowClient()
    model_versions = client.search_model_versions(f"name='{uc_model_name}'")
    latest_version = max(int(mv.version) for mv in model_versions)
    client.set_registered_model_alias(
        name=uc_model_name,
        alias="champion",
        version=latest_version
    )

    # 相関行列プロットを記録
    f_cor_pdf = market_pd.corr(method='spearman', min_periods=12)
    from utils.var_viz import plot_correlation_heatmap
    fig_corr, _ = plot_correlation_heatmap(f_cor_pdf, list(market_indicators.values()))
    mlflow.log_figure(fig_corr, "factor_correlation.png")

    print(f"モデル登録完了: {uc_model_name} v{latest_version} (champion)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. モデルの評価
# MAGIC
# MAGIC 登録したモデルを **Spark UDF** としてロードし、分散推論を実行します。
# MAGIC Unity Catalog のエイリアス（`@champion`）を使うことで、
# MAGIC 常に最新の承認済みモデルを参照できます。

# COMMAND ----------

# champion モデルを Spark UDF としてロード
model_udf = mlflow.pyfunc.spark_udf(
    model_uri='models:/{}@champion'.format(uc_model_name),
    result_type='float',
    spark=spark
)

prediction_df = features_df.withColumn('predicted', model_udf(F.struct('ticker', 'features')))

# COMMAND ----------

# 銘柄ごとの予測精度（WSSE）を計算
from utils.var_udf import wsse_udf

wsse_df = (
    prediction_df
    .withColumn('wsse', wsse_udf(F.col('predicted'), F.col('return')))
    .groupBy('ticker')
    .agg(F.sum('wsse').alias('wsse'))
)

wsse = wsse_df.select(F.avg('wsse').alias('wsse')).toPandas().iloc[0].wsse

# COMMAND ----------

# モデル精度を可視化
fig_wsse, ax_wsse = plt.subplots(figsize=(24, 5))
wsse_df.toPandas().plot.bar(x='ticker', y='wsse', rot=0, label=None, ax=ax_wsse)
ax_wsse.get_legend().remove()
ax_wsse.set_title("各銘柄のモデルWSSE（低いほど精度が高い）")
plt.xticks(rotation=45)
plt.ylabel("WSSE")
plt.show()

# COMMAND ----------

# メトリクスとプロットを実験に追記
with mlflow.start_run(run_id=run_id) as run:
    mlflow.log_metric("wsse", wsse)
    mlflow.log_figure(fig_wsse, "model_wsse.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 予測結果の可視化

# COMMAND ----------

import numpy as np

sample_ticker = "EC"
df_past = prediction_df.filter(F.col('ticker') == sample_ticker).orderBy('date').toPandas()

plt.figure(figsize=(25, 8))
plt.plot(df_past.date, df_past['return'], label='実績リターン', alpha=0.7)
plt.plot(df_past.date, df_past['predicted'], color='green', linestyle='--', label='予測リターン', alpha=0.7)
plt.title(f'{sample_ticker} の対数リターン: 実績 vs 予測')
plt.ylabel('対数リターン')
plt.xlabel('日付')
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **MLflow Experiment**: パラメータ・メトリクス・成果物の体系的な記録
# MAGIC - **pyfunc モデル**: カスタムリスクモデルを MLflow モデルとしてパッケージ化
# MAGIC - **Unity Catalog Model Registry**: モデルのバージョン管理とエイリアス（champion）
# MAGIC - **シグネチャ**: モデルの入出力スキーマの強制
# MAGIC - **Spark UDF**: 登録済みモデルを分散推論に利用
# MAGIC
# MAGIC 次のノートブック `07_monte_carlo_simulation` では、
# MAGIC このモデルを使って **モンテカルロシミュレーション** を大規模に実行します。
