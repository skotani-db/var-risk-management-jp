# Databricks notebook source
# MAGIC %md
# MAGIC # 11. Lakeflow Designer によるリスク調整パイプライン & レポート生成
# MAGIC
# MAGIC
# MAGIC ### 前提条件
# MAGIC > **08_var_aggregation_compliance** を先に実行してください（VaR 結果テーブルが必要です）。
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: `openpyxl`（Excel読み書き用、セットアップセルで自動インストール）
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **Lakeflow Designer（ビジュアル ETL）**: GUI でドラッグ＆ドロップのパイプライン構築
# MAGIC - **Excel ファイルのアップロードと取り込み**: Volume 経由で Excel を Delta テーブル化
# MAGIC - **リスク調整ワークフロー**: ポートフォリオ変更・リスクリミット設定をパイプラインで反映
# MAGIC - **コンプライアンスレポート自動生成**: 調整後 VaR と限度額の比較レポート
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - リスクマネージャーが **Excel で定義した調整** をそのままパイプラインに取り込める
# MAGIC - Lakeflow Designer の **ビジュアル UI** で非エンジニアでもパイプラインを理解・修正可能
# MAGIC - 調整→計算→レポートの **エンドツーエンド自動化** で運用ミスを削減
# MAGIC
# MAGIC ## ユースケース（PoC シナリオ）
# MAGIC > あなたはラテンアメリカ株式ファンドのリスクマネージャーです。
# MAGIC > 四半期末のリバランスに伴い、**ポートフォリオのウェイト変更** と
# MAGIC > **国別リスクリミットの見直し** を Excel で作成しました。
# MAGIC > この Excel をアップロードし、最新の VaR 計算結果と突合して
# MAGIC > **コンプライアンスレポート** を自動生成します。
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Excel ライブラリの準備

# COMMAND ----------

# openpyxl のインストール（Serverless v5 にプリインストールされていない場合）
try:
    import openpyxl
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "-q", "openpyxl"], check=True, capture_output=True)
    import openpyxl

print(f"openpyxl version: {openpyxl.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. リスク調整 Excel ファイルの作成
# MAGIC
# MAGIC 実務では、リスクマネージャーが Excel で以下を作成し、メールや共有フォルダで連携します：
# MAGIC - **Sheet 1: ウェイト調整** — 銘柄ごとの新しいポートフォリオ比率
# MAGIC - **Sheet 2: リスクリミット** — 国別・業種別の VaR 上限値
# MAGIC - **Sheet 3: ストレスシナリオ** — 特定イベントの想定損失率
# MAGIC
# MAGIC ここではサンプル Excel をプログラムで生成しますが、
# MAGIC 実際の PoC では **お客様が用意した Excel をそのままアップロード** できます。

# COMMAND ----------

import pandas as pd
import numpy as np
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows

np.random.seed(42)

# --- Sheet 1: ウェイト調整 ---
weight_data = []
for _, row in portfolio_df.iterrows():
    old_weight = row['weight']
    # リバランス: Banks を増やし、Media/Travel を減らす
    if row['industry'] == 'Banks':
        new_weight = old_weight * 1.3
    elif row['industry'] in ['Media', 'Travel & Leisure']:
        new_weight = old_weight * 0.6
    else:
        new_weight = old_weight * np.random.uniform(0.9, 1.1)
    weight_data.append({
        'ティッカー': row['ticker'],
        '企業名': row['company'],
        '国': row['country'],
        '業種': row['industry'],
        '現行ウェイト(%)': round(old_weight * 100, 2),
        '新ウェイト(%)': round(new_weight * 100, 2),
        '変更理由': 'リバランス' if abs(new_weight - old_weight) > 0.005 else '据え置き'
    })
weight_pdf = pd.DataFrame(weight_data)

# 新ウェイトを正規化（合計100%）
total = weight_pdf['新ウェイト(%)'].sum()
weight_pdf['新ウェイト(%)'] = round(weight_pdf['新ウェイト(%)'] / total * 100, 2)

# --- Sheet 2: リスクリミット ---
limit_data = [
    {'対象': 'ポートフォリオ全体', 'リミット種別': 'VaR99 (日次)', '上限値': -0.025, '承認者': '山田太郎 (CRO)', '有効開始日': '2026-04-01'},
    {'対象': 'CHILE', 'リミット種別': 'VaR99 (国別)', '上限値': -0.035, '承認者': '山田太郎 (CRO)', '有効開始日': '2026-04-01'},
    {'対象': 'COLOMBIA', 'リミット種別': 'VaR99 (国別)', '上限値': -0.040, '承認者': '山田太郎 (CRO)', '有効開始日': '2026-04-01'},
    {'対象': 'MEXICO', 'リミット種別': 'VaR99 (国別)', '上限値': -0.030, '承認者': '山田太郎 (CRO)', '有効開始日': '2026-04-01'},
    {'対象': 'PANAMA', 'リミット種別': 'VaR99 (国別)', '上限値': -0.045, '承認者': '山田太郎 (CRO)', '有効開始日': '2026-04-01'},
    {'対象': 'PERU', 'リミット種別': 'VaR99 (国別)', '上限値': -0.038, '承認者': '山田太郎 (CRO)', '有効開始日': '2026-04-01'},
    {'対象': 'Banks', 'リミット種別': 'VaR99 (業種別)', '上限値': -0.030, '承認者': '鈴木花子 (リスク部長)', '有効開始日': '2026-04-01'},
    {'対象': 'Oil & Gas Producers', 'リミット種別': 'VaR99 (業種別)', '上限値': -0.050, '承認者': '鈴木花子 (リスク部長)', '有効開始日': '2026-04-01'},
    {'対象': 'Mining', 'リミット種別': 'VaR99 (業種別)', '上限値': -0.055, '承認者': '鈴木花子 (リスク部長)', '有効開始日': '2026-04-01'},
]
limit_pdf = pd.DataFrame(limit_data)

# --- Sheet 3: ストレスシナリオ ---
stress_data = [
    {'シナリオ名': '新興国通貨危機', '対象国': 'ALL', '株価ショック(%)': -15.0, 'ボラティリティ倍率': 2.5, '発生確率': '低'},
    {'シナリオ名': '米金利急騰', '対象国': 'ALL', '株価ショック(%)': -8.0, 'ボラティリティ倍率': 1.8, '発生確率': '中'},
    {'シナリオ名': 'チリ政情不安', '対象国': 'CHILE', '株価ショック(%)': -20.0, 'ボラティリティ倍率': 3.0, '発生確率': '低'},
    {'シナリオ名': 'メキシコ関税強化', '対象国': 'MEXICO', '株価ショック(%)': -12.0, 'ボラティリティ倍率': 2.0, '発生確率': '中'},
    {'シナリオ名': '原油価格暴落', '対象国': 'COLOMBIA', '株価ショック(%)': -18.0, 'ボラティリティ倍率': 2.8, '発生確率': '低'},
]
stress_pdf = pd.DataFrame(stress_data)

print(f"ウェイト調整: {len(weight_pdf)}銘柄")
print(f"リスクリミット: {len(limit_pdf)}件")
print(f"ストレスシナリオ: {len(stress_pdf)}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Excel ファイルを書式付きで生成
# MAGIC
# MAGIC 実務で使われるような見やすいフォーマットで Excel を作成します。

# COMMAND ----------

def style_excel_sheet(ws, header_fill, df):
    """Excel シートにヘッダー書式を適用"""
    thin_border = Border(
        left=Side(style='thin'), right=Side(style='thin'),
        top=Side(style='thin'), bottom=Side(style='thin')
    )
    header_font = Font(bold=True, color="FFFFFF", size=11)

    for col_idx in range(1, len(df.columns) + 1):
        cell = ws.cell(row=1, column=col_idx)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center')

    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=len(df.columns)):
        for cell in row:
            cell.border = thin_border

    for col in ws.columns:
        max_len = max(len(str(cell.value or '')) for cell in col) + 4
        ws.column_dimensions[col[0].column_letter].width = max_len


# Excel ファイル作成
excel_path = "/tmp/risk_adjustment_q2_2026.xlsx"
wb = Workbook()

# Sheet 1: ウェイト調整
ws1 = wb.active
ws1.title = "ウェイト調整"
for r in dataframe_to_rows(weight_pdf, index=False, header=True):
    ws1.append(r)
style_excel_sheet(ws1, PatternFill(start_color="1F4E79", fill_type="solid"), weight_pdf)

# Sheet 2: リスクリミット
ws2 = wb.create_sheet("リスクリミット")
for r in dataframe_to_rows(limit_pdf, index=False, header=True):
    ws2.append(r)
style_excel_sheet(ws2, PatternFill(start_color="C00000", fill_type="solid"), limit_pdf)

# Sheet 3: ストレスシナリオ
ws3 = wb.create_sheet("ストレスシナリオ")
for r in dataframe_to_rows(stress_pdf, index=False, header=True):
    ws3.append(r)
style_excel_sheet(ws3, PatternFill(start_color="BF8F00", fill_type="solid"), stress_pdf)

wb.save(excel_path)
print(f"Excel 生成完了: {excel_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Excel を Volume にアップロード
# MAGIC
# MAGIC 生成した Excel ファイルを Unity Catalog Volume にアップロードします。
# MAGIC
# MAGIC ### 実際の PoC では
# MAGIC > 1. 左メニュー「**カタログ**」→ Volume を開く
# MAGIC > 2. 「**アップロード**」ボタンをクリック
# MAGIC > 3. リスクマネージャーが作成した **Excel ファイルをドラッグ＆ドロップ**
# MAGIC >
# MAGIC > このデモではプログラム的にアップロードしますが、UI からの手動アップロードも全く同じ結果になります。

# COMMAND ----------

volume_path = "/Volumes/{}/{}/{}".format(
    config['database']['catalog'],
    config['database']['schema'],
    config['database']['volume']
)

# Volume に Excel をコピー
upload_path = f"{volume_path}/risk_adjustments"
dbutils.fs.mkdirs(upload_path)
dbutils.fs.cp(f"file:{excel_path}", f"{upload_path}/risk_adjustment_q2_2026.xlsx")
print(f"アップロード完了: {upload_path}/risk_adjustment_q2_2026.xlsx")

# Volume 内のファイルを確認
display(dbutils.fs.ls(upload_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Excel データの読み込みと Delta テーブル化
# MAGIC
# MAGIC Volume にアップロードされた Excel を pandas で読み込み、Delta テーブルとして保存します。
# MAGIC これが Lakeflow Designer パイプラインのソースデータになります。

# COMMAND ----------

# Volume 上の Excel を読み込み（ローカルパスに変換）
excel_volume_path = f"{volume_path}/risk_adjustments/risk_adjustment_q2_2026.xlsx".replace("/Volumes/", "/Volumes/")

# 各シートを読み込み
df_weights = pd.read_excel(excel_volume_path, sheet_name="ウェイト調整", engine="openpyxl")
df_limits = pd.read_excel(excel_volume_path, sheet_name="リスクリミット", engine="openpyxl")
df_stress = pd.read_excel(excel_volume_path, sheet_name="ストレスシナリオ", engine="openpyxl")

print("=== ウェイト調整 ===")
display(spark.createDataFrame(df_weights))

# COMMAND ----------

print("=== リスクリミット ===")
display(spark.createDataFrame(df_limits))

# COMMAND ----------

print("=== ストレスシナリオ ===")
display(spark.createDataFrame(df_stress))

# COMMAND ----------

# Delta テーブルとして保存
(
    spark.createDataFrame(df_weights)
    .write.format("delta").mode("overwrite")
    .saveAsTable("weight_adjustments")
)

(
    spark.createDataFrame(df_limits)
    .write.format("delta").mode("overwrite")
    .saveAsTable("risk_limits")
)

(
    spark.createDataFrame(df_stress)
    .write.format("delta").mode("overwrite")
    .saveAsTable("stress_scenarios")
)

print("Delta テーブル作成完了: weight_adjustments, risk_limits, stress_scenarios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lakeflow Designer でパイプラインを構築
# MAGIC
# MAGIC ここからが **Lakeflow Designer** の出番です。
# MAGIC ビジュアル UI でドラッグ＆ドロップしながら、Excel データと VaR 結果を結合するパイプラインを作成します。
# MAGIC
# MAGIC ### Lakeflow Designer の操作手順
# MAGIC
# MAGIC #### Step 1: パイプラインを作成
# MAGIC 1. 左メニュー「**Data Engineering**」→「**Pipelines**」→「**Create pipeline**」
# MAGIC 2. パイプライン名: `risk_adjustment_pipeline`
# MAGIC 3. 「**Lakeflow Designer**」タブを選択（コードではなく GUI モード）
# MAGIC
# MAGIC #### Step 2: ソーステーブルを追加
# MAGIC 1. 左パネルの「**Sources**」から以下をドラッグ＆ドロップ:
# MAGIC    - `weight_adjustments`（ウェイト調整）
# MAGIC    - `risk_limits`（リスクリミット）
# MAGIC    - `monte_carlo_trials`（VaR シミュレーション結果）
# MAGIC
# MAGIC #### Step 3: 変換ノードを追加
# MAGIC 1. 「**Transformations**」から「**Join**」をドラッグ
# MAGIC    - `monte_carlo_trials` と `weight_adjustments` を `ticker = ティッカー` で結合
# MAGIC 2. 「**Transformations**」から「**Aggregate**」をドラッグ
# MAGIC    - 結合結果を国別に集約し、新ウェイトで加重平均 VaR を計算
# MAGIC 3. 「**Transformations**」から「**Join**」をもう1つドラッグ
# MAGIC    - 集約結果と `risk_limits` を `国 = 対象` で結合
# MAGIC
# MAGIC #### Step 4: シンクを設定
# MAGIC 1. 「**Destinations**」から「**Delta Table**」をドラッグ
# MAGIC    - テーブル名: `risk_compliance_report`
# MAGIC    - カタログ・スキーマ: 現在のデモ環境を選択
# MAGIC
# MAGIC #### Step 5: 実行
# MAGIC 1. 右上の「**Start**」をクリック → パイプラインが実行されます
# MAGIC 2. 各ノードの処理件数・品質メトリクスをリアルタイムで確認
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 以下のセルでは、**Lakeflow Designer が内部で行う処理と同等のロジック** をコードで実行します。
# MAGIC GUI で構築したパイプラインの裏側を理解するのに役立ちます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. パイプラインロジック（コード版）
# MAGIC
# MAGIC Lakeflow Designer の各ノードが行う処理を、Spark SQL / PySpark で再現します。

# COMMAND ----------

from pyspark.sql import functions as F

# --- Step A: ウェイト調整テーブルを英語カラム名に正規化 ---
adjustments = (
    spark.read.table("weight_adjustments")
    .withColumnRenamed("ティッカー", "ticker")
    .withColumnRenamed("国", "country")
    .withColumnRenamed("業種", "industry")
    .withColumnRenamed("現行ウェイト(%)", "old_weight_pct")
    .withColumnRenamed("新ウェイト(%)", "new_weight_pct")
    .withColumn("new_weight", F.col("new_weight_pct") / 100)
)

display(adjustments.select("ticker", "country", "industry", "old_weight_pct", "new_weight_pct"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-1. 調整後ウェイトでの加重リターン再計算
# MAGIC
# MAGIC Monte Carlo シミュレーション結果に新しいウェイトを適用し、
# MAGIC ポートフォリオ全体のリターン分布を再計算します。

# COMMAND ----------

from utils.var_udf import weighted_returns, get_var_udf

# Monte Carlo 結果と新ウェイトを結合
trials_df = spark.read.table(config['database']['tables']['mc_trials'])
adjusted_simulation = (
    trials_df
    .join(adjustments.select("ticker", "new_weight", "country", "industry"), ["ticker"])
    .withColumn("adjusted_weighted_return", weighted_returns("returns", "new_weight"))
)

print(f"結合結果: {adjusted_simulation.count()} 行")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-2. 調整後 VaR の計算（国別）

# COMMAND ----------

from pyspark.ml.stat import Summarizer
from pyspark.sql.column import Column

# 国別に調整後 VaR99 を計算
adjusted_var_by_country = (
    adjusted_simulation
    .groupBy("date", "country")
    .agg(
        Summarizer.mean(Summarizer.metrics("mean")
            .summary(F.collect_list("adjusted_weighted_return")))
        if False else
        F.sum("adjusted_weighted_return").alias("portfolio_return")
    )
    .groupBy("country")
    .agg(
        F.expr("percentile(portfolio_return, 0.01)").alias("var_99"),
        F.mean("portfolio_return").alias("avg_return"),
        F.stddev("portfolio_return").alias("std_return"),
        F.count("*").alias("n_observations")
    )
    .orderBy("var_99")
)

display(adjusted_var_by_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-3. リスクリミットとの突合（コンプライアンスチェック）
# MAGIC
# MAGIC 調整後 VaR が設定されたリミットを超過していないかチェックします。

# COMMAND ----------

# リスクリミットを読み込み
limits = (
    spark.read.table("risk_limits")
    .withColumnRenamed("対象", "target")
    .withColumnRenamed("リミット種別", "limit_type")
    .withColumnRenamed("上限値", "limit_value")
    .withColumnRenamed("承認者", "approver")
    .filter(F.col("limit_type") == "VaR99 (国別)")
)

# 国別 VaR とリミットを結合
compliance_check = (
    adjusted_var_by_country
    .join(limits, adjusted_var_by_country["country"] == limits["target"], "left")
    .withColumn(
        "status",
        F.when(F.col("var_99") >= F.col("limit_value"), "OK（リミット内）")
         .otherwise("BREACH（リミット超過）")
    )
    .withColumn(
        "buffer",
        F.round((F.col("limit_value") - F.col("var_99")) / F.abs(F.col("limit_value")) * 100, 1)
    )
    .select("country", "var_99", "limit_value", "status", "buffer", "approver")
)

display(compliance_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. コンプライアンスレポートの可視化

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

report_df = compliance_check.toPandas()

fig, ax = plt.subplots(figsize=(14, 7))

countries = report_df['country'].tolist()
var_values = report_df['var_99'].tolist()
limit_values = report_df['limit_value'].tolist()
statuses = report_df['status'].tolist()

x = np.arange(len(countries))
width = 0.35

# VaR バー（超過は赤、OK は青）
colors = ['#C00000' if 'BREACH' in s else '#1F4E79' for s in statuses]
bars_var = ax.bar(x - width/2, var_values, width, label='調整後 VaR99', color=colors, alpha=0.85)
bars_lim = ax.bar(x + width/2, limit_values, width, label='リスクリミット', color='#BF8F00', alpha=0.6)

ax.set_xlabel('国', fontsize=12, fontweight='bold')
ax.set_ylabel('VaR99', fontsize=12, fontweight='bold')
ax.set_title('国別 VaR99 vs リスクリミット（調整後ポートフォリオ）', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(countries)
ax.legend()
ax.axhline(y=0, linestyle='--', alpha=0.3, color='gray')

# ステータスラベル
for i, (v, s) in enumerate(zip(var_values, statuses)):
    label = "BREACH" if "BREACH" in s else "OK"
    color = '#C00000' if "BREACH" in s else '#006400'
    ax.text(i - width/2, v - 0.001, label, ha='center', va='top',
            fontweight='bold', fontsize=10, color=color)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. ストレステスト（Excel シナリオ適用）
# MAGIC
# MAGIC アップロードした Excel のストレスシナリオを適用し、
# MAGIC 極端な市場環境での損失を推定します。

# COMMAND ----------

stress_scenarios = spark.read.table("stress_scenarios").toPandas()

# 各シナリオのストレス VaR を推定
stress_results = []
for _, scenario in stress_scenarios.iterrows():
    target_country = scenario['対象国']
    shock = scenario['株価ショック(%)'] / 100
    vol_mult = scenario['ボラティリティ倍率']

    if target_country == 'ALL':
        # 全体にショック適用
        stressed_var = report_df['var_99'].mean() * vol_mult + shock
    else:
        country_var = report_df[report_df['country'] == target_country]['var_99']
        if len(country_var) > 0:
            stressed_var = country_var.values[0] * vol_mult + shock
        else:
            stressed_var = shock

    stress_results.append({
        'シナリオ': scenario['シナリオ名'],
        '対象': target_country,
        'ストレスVaR': round(stressed_var, 4),
        '通常VaR': round(report_df['var_99'].mean(), 4),
        '追加損失': round(stressed_var - report_df['var_99'].mean(), 4),
        '発生確率': scenario['発生確率']
    })

stress_result_df = pd.DataFrame(stress_results)
display(spark.createDataFrame(stress_result_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. ストレステスト結果の可視化

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 7))

scenarios = stress_result_df['シナリオ'].tolist()
normal_var = stress_result_df['通常VaR'].tolist()
stress_var = stress_result_df['ストレスVaR'].tolist()

x = np.arange(len(scenarios))
width = 0.35

ax.barh(x - width/2, normal_var, width, label='通常 VaR99', color='#1F4E79', alpha=0.8)
ax.barh(x + width/2, stress_var, width, label='ストレス VaR', color='#C00000', alpha=0.8)

ax.set_xlabel('VaR / 損失率', fontsize=12, fontweight='bold')
ax.set_title('ストレスシナリオ別 VaR 比較', fontsize=14, fontweight='bold')
ax.set_yticks(x)
ax.set_yticklabels(scenarios)
ax.legend(loc='lower left')
ax.axvline(x=0, linestyle='--', alpha=0.3, color='gray')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. レポートを Delta テーブルとして保存
# MAGIC
# MAGIC 最終レポートを Delta テーブルに保存し、AI/BI Dashboard や Genie で活用できるようにします。

# COMMAND ----------

from datetime import datetime

# コンプライアンスレポート
report_final = (
    compliance_check
    .withColumn("report_date", F.lit(datetime.now().strftime("%Y-%m-%d")))
    .withColumn("report_type", F.lit("Q2 2026 リバランス後"))
)

(
    report_final
    .write.format("delta").mode("overwrite")
    .saveAsTable("risk_compliance_report")
)

# ストレステスト結果
(
    spark.createDataFrame(stress_result_df)
    .withColumn("report_date", F.lit(datetime.now().strftime("%Y-%m-%d")))
    .write.format("delta").mode("overwrite")
    .saveAsTable("stress_test_report")
)

print("レポートテーブル作成完了:")
print("  - risk_compliance_report（コンプライアンスチェック結果）")
print("  - stress_test_report（ストレステスト結果）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Lakeflow Designer パイプライン定義（参考）
# MAGIC
# MAGIC 上記のコードロジックを Lakeflow Designer で構築すると、以下のようなパイプライン DAG になります：
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
# MAGIC │  weight_adjustments │    │  monte_carlo_trials  │    │    risk_limits       │
# MAGIC │  (Excel → Volume)   │    │  (VaR計算結果)        │    │  (Excel → Volume)   │
# MAGIC └────────┬────────────┘    └──────────┬───────────┘    └──────────┬──────────┘
# MAGIC          │                            │                           │
# MAGIC          └───────────┐   ┌────────────┘                           │
# MAGIC                      ▼   ▼                                        │
# MAGIC               ┌──────────────────┐                                │
# MAGIC               │   Join (ticker)  │                                │
# MAGIC               │  加重リターン再計算 │                                │
# MAGIC               └────────┬─────────┘                                │
# MAGIC                        │                                          │
# MAGIC                        ▼                                          │
# MAGIC               ┌──────────────────┐                                │
# MAGIC               │  Aggregate       │                                │
# MAGIC               │  国別VaR99計算    │                                │
# MAGIC               └────────┬─────────┘                                │
# MAGIC                        │                                          │
# MAGIC                        └──────────────────┐   ┌───────────────────┘
# MAGIC                                           ▼   ▼
# MAGIC                                    ┌──────────────────┐
# MAGIC                                    │   Join (country) │
# MAGIC                                    │  リミット突合     │
# MAGIC                                    └────────┬─────────┘
# MAGIC                                             │
# MAGIC                                             ▼
# MAGIC                                    ┌──────────────────────────┐
# MAGIC                                    │  risk_compliance_report  │
# MAGIC                                    │  (Delta テーブル)         │
# MAGIC                                    └──────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Lakeflow Designer のメリット
# MAGIC - **非エンジニアでも理解可能**: DAG を見れば処理の流れが一目瞭然
# MAGIC - **変更が容易**: ノードをドラッグして条件を変更するだけ
# MAGIC - **品質管理**: 各ノードに Expectations（品質ルール）を GUI で設定可能
# MAGIC - **スケジュール実行**: パイプラインをジョブとして定期実行（日次・週次等）

# COMMAND ----------

# MAGIC %md
# MAGIC ## やってみよう
# MAGIC
# MAGIC 以下の演習に挑戦してみましょう。
# MAGIC **わからないことがあれば、ノートブック右側の Genie Code（AI アシスタント）に質問しながら進めてください。**
# MAGIC
# MAGIC 1. **Excel を修正してリスクリミットを変更**:
# MAGIC    - `risk_adjustment_q2_2026.xlsx` をダウンロードし、リスクリミットの上限値を変更
# MAGIC    - 変更後の Excel を Volume に再アップロードし、このノートブックを再実行
# MAGIC    - BREACH / OK の判定がどう変わるか確認
# MAGIC
# MAGIC 2. **Lakeflow Designer でパイプラインを構築**:
# MAGIC    - 左メニュー「Data Engineering」→「Pipelines」→「Create pipeline」
# MAGIC    - セクション 4 の手順に従い、ビジュアルパイプラインを構築
# MAGIC    - コード版（セクション 5）と同じ結果が得られるか比較
# MAGIC
# MAGIC 3. **ストレスシナリオを追加**:
# MAGIC    - Excel の「ストレスシナリオ」シートに独自のシナリオを追加
# MAGIC    - 例:「中国景気減速」「コモディティ価格高騰」など

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC
# MAGIC - **Excel → Volume → Delta テーブル** のデータ取り込みフロー
# MAGIC - **Lakeflow Designer** のビジュアルパイプライン構築手順
# MAGIC - **リスク調整後 VaR** の再計算とリミット突合
# MAGIC - **ストレステスト**: Excel で定義したシナリオの適用
# MAGIC - **コンプライアンスレポート** の自動生成と Delta テーブルへの保存
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `09_dashboard_and_genie` で作成したダッシュボードに `risk_compliance_report` テーブルを追加
# MAGIC - Lakeflow Designer パイプラインをジョブとしてスケジュール実行（日次レポート自動化）
# MAGIC - レポートを PDF エクスポートし、規制当局への提出資料として活用
