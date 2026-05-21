# Databricks notebook source
# MAGIC %md
# MAGIC # 11. Lakeflow Designer によるリスク調整パイプライン & レポート生成
# MAGIC
# MAGIC
# MAGIC ### 前提条件
# MAGIC > **08_var_aggregation_compliance** を先に実行してください（`monte_carlo_trials` テーブルが必要です）。
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **Lakeflow Designer（ビジュアル ETL）**: GUI でドラッグ＆ドロップのパイプライン構築
# MAGIC - **手元の Excel をデータソースとしてアップロード**: Designer から直接ファイルを取り込み
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
# MAGIC > この Excel を Lakeflow Designer にアップロードし、最新の VaR 計算結果と突合して
# MAGIC > **コンプライアンスレポート** を自動生成します。
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. リスク調整 Excel の準備
# MAGIC
# MAGIC このリポジトリの `data/risk_adjustment_q2_2026.xlsx` をお手元の PC にダウンロードしてください。
# MAGIC
# MAGIC この Excel は3つのシートで構成されています：
# MAGIC
# MAGIC | シート名 | 内容 | 主なカラム |
# MAGIC |---|---|---|
# MAGIC | **ウェイト調整** | 27銘柄のポートフォリオ比率変更 | `ticker`, `old_weight_pct`, `new_weight_pct` |
# MAGIC | **リスクリミット** | 国別・業種別の VaR99 上限値 | `target`, `limit_type`, `limit_value`, `approver` |
# MAGIC | **ストレスシナリオ** | 5つの極端イベントの想定損失率 | `scenario_name`, `price_shock_pct`, `volatility_multiplier` |
# MAGIC
# MAGIC > **PoC のポイント**: 実際のお客様環境では、リスクマネージャーが普段使っている
# MAGIC > Excel をそのまま使えます。カラム名さえ合っていれば、Lakeflow Designer が自動で取り込みます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lakeflow Designer でパイプラインを構築
# MAGIC
# MAGIC 手元にダウンロードした Excel を使って、Lakeflow Designer でパイプラインを構築します。
# MAGIC
# MAGIC ### Step 1: パイプラインを作成
# MAGIC 1. 左メニュー「**Data Engineering**」→「**Pipelines**」→「**Create pipeline**」
# MAGIC 2. パイプライン名: `risk_adjustment_pipeline`
# MAGIC 3. 「**Lakeflow Designer**」タブを選択（コードではなく GUI モード）
# MAGIC 4. ターゲットカタログ・スキーマを選択
# MAGIC
# MAGIC ### Step 2: Excel をデータソースとして追加
# MAGIC 1. キャンバス上で「**+**」→「**Source**」→「**Upload file**」を選択
# MAGIC 2. 手元の `risk_adjustment_q2_2026.xlsx` を **ドラッグ＆ドロップ**
# MAGIC 3. 取り込み対象のシートを選択:
# MAGIC    - まず「**ウェイト調整**」シートを選択 → テーブル名を `weight_adjustments` に設定
# MAGIC 4. 同様に「**Upload file**」をもう2回追加:
# MAGIC    - 「**リスクリミット**」シート → テーブル名: `risk_limits`
# MAGIC    - 「**ストレスシナリオ**」シート → テーブル名: `stress_scenarios`
# MAGIC
# MAGIC ### Step 3: 既存テーブルをソースに追加
# MAGIC 1. 「**+**」→「**Source**」→「**Catalog table**」を選択
# MAGIC 2. `monte_carlo_trials`（VaR シミュレーション結果）を選択
# MAGIC
# MAGIC ### Step 4: 変換ノードを追加
# MAGIC 1. 「**+**」→「**Transformation**」→「**Join**」
# MAGIC    - `monte_carlo_trials` と `weight_adjustments` を `ticker` で結合
# MAGIC 2. 「**+**」→「**Transformation**」→「**Aggregate**」
# MAGIC    - 結合結果を `country` で集約し、加重リターンの合計を計算
# MAGIC 3. 「**+**」→「**Transformation**」→「**Join**」
# MAGIC    - 集約結果と `risk_limits` を `country = target` で結合
# MAGIC
# MAGIC ### Step 5: シンク（出力先）を設定
# MAGIC 1. 「**+**」→「**Destination**」→「**Delta Table**」
# MAGIC    - テーブル名: `risk_compliance_report`
# MAGIC
# MAGIC ### Step 6: 実行
# MAGIC 1. 右上の「**Start**」をクリック
# MAGIC 2. 各ノードの処理件数・品質メトリクスをリアルタイムで確認
# MAGIC
# MAGIC ### パイプライン DAG（完成イメージ）
# MAGIC ```
# MAGIC ┌──────────────────────┐  ┌──────────────────────┐  ┌─────────────────────┐
# MAGIC │  weight_adjustments  │  │  monte_carlo_trials  │  │    risk_limits       │
# MAGIC │  (Excel アップロード) │  │  (既存テーブル)       │  │  (Excel アップロード) │
# MAGIC └────────┬─────────────┘  └──────────┬───────────┘  └──────────┬──────────┘
# MAGIC          │                           │                          │
# MAGIC          └──────────┐   ┌────────────┘                          │
# MAGIC                     ▼   ▼                                       │
# MAGIC              ┌──────────────────┐                               │
# MAGIC              │   Join (ticker)  │                               │
# MAGIC              │  加重リターン再計算│                               │
# MAGIC              └────────┬─────────┘                               │
# MAGIC                       │                                         │
# MAGIC                       ▼                                         │
# MAGIC              ┌──────────────────┐                               │
# MAGIC              │  Aggregate       │                               │
# MAGIC              │  国別VaR99計算   │                               │
# MAGIC              └────────┬─────────┘                               │
# MAGIC                       │                                         │
# MAGIC                       └─────────────────┐   ┌──────────────────┘
# MAGIC                                         ▼   ▼
# MAGIC                                  ┌──────────────────┐
# MAGIC                                  │  Join (country)  │
# MAGIC                                  │  リミット突合    │
# MAGIC                                  └────────┬─────────┘
# MAGIC                                           │
# MAGIC                                           ▼
# MAGIC                                  ┌────────────────────────┐
# MAGIC                                  │ risk_compliance_report │
# MAGIC                                  │ (Delta テーブル)       │
# MAGIC                                  └────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ここから先は、Lakeflow Designer で作成されたテーブルを使ったレポート生成です
# MAGIC
# MAGIC Designer パイプラインを実行すると、以下の Delta テーブルが作成されます：
# MAGIC - `weight_adjustments` — ウェイト調整
# MAGIC - `risk_limits` — リスクリミット
# MAGIC - `stress_scenarios` — ストレスシナリオ
# MAGIC - `risk_compliance_report` — コンプライアンスチェック結果
# MAGIC
# MAGIC 以下のセルでは、これらのテーブルを使って **可視化とストレステスト** を行います。
# MAGIC
# MAGIC > **注意**: Lakeflow Designer のパイプラインを先に実行してからこのセクションを実行してください。
# MAGIC > または、次のセルでコード版のパイプラインロジックを実行して同等のテーブルを作成することもできます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. パイプラインロジック（コード版 — Designer の代替）
# MAGIC
# MAGIC Lakeflow Designer を使わずにコードで同等の処理を実行する場合は、
# MAGIC このセクションを実行してください。Designer パイプラインを既に実行済みの場合はスキップできます。
# MAGIC
# MAGIC ### 3-0. Excel を Volume 経由で取り込み

# COMMAND ----------

import pandas as pd

volume_path = "/Volumes/{}/{}/{}".format(
    config['database']['catalog'],
    config['database']['schema'],
    config['database']['volume']
)

# data/ フォルダの Excel を Volume にコピー
upload_path = f"{volume_path}/risk_adjustments"
dbutils.fs.mkdirs(upload_path)

notebook_dir = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0]
dbutils.fs.cp(
    f"file:/Workspace{notebook_dir}/data/risk_adjustment_q2_2026.xlsx",
    f"{upload_path}/risk_adjustment_q2_2026.xlsx"
)

excel_path = f"{upload_path}/risk_adjustment_q2_2026.xlsx"
print(f"Excel アップロード完了: {excel_path}")

# COMMAND ----------

# openpyxl のインストール（Serverless v5 にプリインストールされていない場合）
try:
    import openpyxl
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "-q", "openpyxl"], check=True, capture_output=True)
    import openpyxl

# 各シートを読み込んで Delta テーブル化
sheet_table_map = {
    "ウェイト調整": "weight_adjustments",
    "リスクリミット": "risk_limits",
    "ストレスシナリオ": "stress_scenarios",
}
for sheet_name, table_name in sheet_table_map.items():
    df = pd.read_excel(excel_path, sheet_name=sheet_name, engine="openpyxl")
    spark.createDataFrame(df).write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"  {table_name}: {len(df)} 行")

print("\nDelta テーブル作成完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1. 調整後ウェイトでの加重リターン再計算

# COMMAND ----------

from pyspark.sql import functions as F
from utils.var_udf import weighted_returns, get_var_udf

# ウェイト調整テーブル
adjustments = (
    spark.read.table("weight_adjustments")
    .withColumn("new_weight", F.col("new_weight_pct") / 100)
)

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
# MAGIC ### 3-2. 調整後 VaR の計算（国別）

# COMMAND ----------

adjusted_var_by_country = (
    adjusted_simulation
    .groupBy("date", "country")
    .agg(F.sum("adjusted_weighted_return").alias("portfolio_return"))
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
# MAGIC ### 3-3. リスクリミットとの突合（コンプライアンスチェック）

# COMMAND ----------

limits = (
    spark.read.table("risk_limits")
    .filter(F.col("limit_type") == "VaR99_country")
)

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
# MAGIC ## 4. コンプライアンスレポートの可視化

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

for i, (v, s) in enumerate(zip(var_values, statuses)):
    label = "BREACH" if "BREACH" in s else "OK"
    color = '#C00000' if "BREACH" in s else '#006400'
    ax.text(i - width/2, v - 0.001, label, ha='center', va='top',
            fontweight='bold', fontsize=10, color=color)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ストレステスト（Excel シナリオ適用）
# MAGIC
# MAGIC アップロードした Excel のストレスシナリオを適用し、
# MAGIC 極端な市場環境での損失を推定します。

# COMMAND ----------

stress_scenarios = spark.read.table("stress_scenarios").toPandas()

stress_results = []
for _, scenario in stress_scenarios.iterrows():
    target_country = scenario['target_country']
    shock = scenario['price_shock_pct'] / 100
    vol_mult = scenario['volatility_multiplier']

    if target_country == 'ALL':
        stressed_var = report_df['var_99'].mean() * vol_mult + shock
    else:
        country_var = report_df[report_df['country'] == target_country]['var_99']
        if len(country_var) > 0:
            stressed_var = country_var.values[0] * vol_mult + shock
        else:
            stressed_var = shock

    stress_results.append({
        'scenario': scenario['scenario_name'],
        'target': target_country,
        'stressed_var': round(stressed_var, 4),
        'normal_var': round(report_df['var_99'].mean(), 4),
        'additional_loss': round(stressed_var - report_df['var_99'].mean(), 4),
        'probability': scenario['probability']
    })

stress_result_df = pd.DataFrame(stress_results)
display(spark.createDataFrame(stress_result_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ストレステスト結果の可視化

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 7))

scenarios = stress_result_df['scenario'].tolist()
normal_var = stress_result_df['normal_var'].tolist()
stress_var = stress_result_df['stressed_var'].tolist()

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
# MAGIC ## 7. レポートを Delta テーブルとして保存

# COMMAND ----------

from datetime import datetime

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
# MAGIC ## 8. リネージの確認（Excel でもデータの来歴が追跡可能）
# MAGIC
# MAGIC Excel からアップロードしたデータでも、Lakeflow Designer 経由で取り込めば
# MAGIC **Unity Catalog のリネージ（データの来歴）が自動的に記録** されます。
# MAGIC
# MAGIC ### 確認方法
# MAGIC 1. 左メニュー「**カタログ**」→ `risk_compliance_report` テーブルを開く
# MAGIC 2. 「**リネージ**」タブをクリック
# MAGIC 3. 以下のようなデータフローが可視化されます：
# MAGIC
# MAGIC ```
# MAGIC [Excel: risk_adjustment_q2_2026.xlsx]
# MAGIC     │
# MAGIC     ├── weight_adjustments ──┐
# MAGIC     └── risk_limits ─────────┤
# MAGIC                              │
# MAGIC [monte_carlo_trials] ────────┤
# MAGIC                              │
# MAGIC                              ▼
# MAGIC                   risk_compliance_report
# MAGIC                              │
# MAGIC                              ▼
# MAGIC                     stress_test_report
# MAGIC ```
# MAGIC
# MAGIC ### 規制対応での価値
# MAGIC - **監査証跡**: 「このレポートのリスクリミットはどの Excel から来たのか？」に即座に回答可能
# MAGIC - **影響分析**: リミット定義を変更した場合、どのレポートに影響するかを事前に把握
# MAGIC - **データ品質の透明性**: パイプラインの各ステップで何件のデータが処理されたか追跡可能
# MAGIC - **Excel でも安心**: 手元のファイルからの取り込みでも、テーブル間の依存関係が Unity Catalog に自動記録される
# MAGIC
# MAGIC > **PoC のデモポイント**: カタログ UI でリネージグラフを見せることで、
# MAGIC > 「Excel 運用でもガバナンスが効く」ことを視覚的に示せます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow Designer のメリット
# MAGIC
# MAGIC | 観点 | コード版（セクション3） | Lakeflow Designer（セクション2） |
# MAGIC |---|---|---|
# MAGIC | 構築者 | エンジニア | 非エンジニアでも可 |
# MAGIC | データソース追加 | コード変更が必要 | Excel をドラッグ＆ドロップ |
# MAGIC | パイプラインの可視化 | コードを読む必要がある | DAG が自動表示 |
# MAGIC | 品質ルール | `@dlt.expect` をコーディング | GUI でチェックボックス設定 |
# MAGIC | スケジュール実行 | ジョブ設定が必要 | UI からワンクリック |
# MAGIC | Excel 更新時の再実行 | ファイルの再アップロード＋再実行 | Excel を差し替えて「Start」 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## やってみよう
# MAGIC
# MAGIC **わからないことがあれば、ノートブック右側の Genie Code（AI アシスタント）に質問しながら進めてください。**
# MAGIC
# MAGIC 1. **Excel を修正してリスクリミットを変更**:
# MAGIC    - `data/risk_adjustment_q2_2026.xlsx` をダウンロードして Excel で開く
# MAGIC    - 「リスクリミット」シートの `limit_value` を変更（例: MEXICO の上限を -0.020 に引き下げ）
# MAGIC    - 変更後の Excel を Lakeflow Designer に再アップロードしてパイプラインを再実行
# MAGIC    - BREACH / OK の判定がどう変わるか確認
# MAGIC
# MAGIC 2. **ストレスシナリオを追加**:
# MAGIC    - 「ストレスシナリオ」シートに新しい行を追加（例: `中国景気減速, ALL, -10.0, 2.0, 中`）
# MAGIC    - パイプラインを再実行し、セクション 5〜6 で新シナリオの影響を確認
# MAGIC
# MAGIC 3. **Lakeflow Designer で Expectations を設定**:
# MAGIC    - `weight_adjustments` ノードに品質ルールを追加: `new_weight_pct > 0`
# MAGIC    - 不正なウェイト（マイナス値）が入った Excel をアップロードして、品質ルールの動作を確認

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC
# MAGIC - **Lakeflow Designer** で手元の Excel をデータソースとしてアップロードする方法
# MAGIC - ビジュアル UI で **Join → Aggregate → リミット突合** のパイプラインを構築
# MAGIC - **リスク調整後 VaR** の再計算とコンプライアンスチェック
# MAGIC - **ストレステスト**: Excel で定義したシナリオの適用と可視化
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `09_dashboard_and_genie` で作成したダッシュボードに `risk_compliance_report` テーブルを追加
# MAGIC - Lakeflow Designer パイプラインをジョブとしてスケジュール実行（日次レポート自動化）
# MAGIC - Excel を更新して再アップロードするだけでレポートが自動更新されるフローを体験
