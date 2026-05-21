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
# MAGIC - **Lakeflow Designer（ビジュアルデータ準備）**: ドラッグ＆ドロップでデータ変換パイプラインを構築
# MAGIC - **手元の Excel をデータソースとしてアップロード**: キャンバスにドラッグするだけで取り込み
# MAGIC - **組み込みオペレータ**: 結合・集計・フィルター・変換をコード不要で設定
# MAGIC - **コンプライアンスレポート自動生成**: 調整後 VaR と限度額の比較レポート
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - リスクマネージャーが **Excel で定義した調整** をそのままパイプラインに取り込める
# MAGIC - Lakeflow Designer の **ビジュアルキャンバス** で非エンジニアでもパイプラインを理解・修正可能
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
# MAGIC ---
# MAGIC
# MAGIC ### Step 1: ビジュアルデータ準備を新規作成
# MAGIC 1. 左サイドバーの「**＋ 新規**」→「**ビジュアルデータ準備**」を選択
# MAGIC 2. キャンバス（メインのワークスペース）が開き、ウェルカム画面が表示されます
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2: Excel をデータソースとして追加（ドラッグ＆ドロップ）
# MAGIC
# MAGIC 手元の `risk_adjustment_q2_2026.xlsx` を **キャンバスに直接ドラッグ＆ドロップ** します。
# MAGIC Designer が自動的にワークスペースファイルシステムにアップロードし、ソース演算子を作成します。
# MAGIC
# MAGIC > **Excel ファイルの注意点**: 事前にワークスペースで Excel ファイル形式のサポートが有効化されている必要があります。
# MAGIC
# MAGIC シートごとにソース演算子が必要なため、以下の3つを作成します:
# MAGIC
# MAGIC | ソース演算子名 | 取り込むシート | 説明 |
# MAGIC |---|---|---|
# MAGIC | `weight_adjustments` | ウェイト調整 | 銘柄ごとの新ポートフォリオ比率 |
# MAGIC | `risk_limits` | リスクリミット | 国別・業種別の VaR99 上限値 |
# MAGIC | `stress_scenarios` | ストレスシナリオ | 極端イベントの想定損失率 |
# MAGIC
# MAGIC **演算子の名前変更**: ソース演算子をダブルクリック → 設定ペイン上部のテキストフィールドで名前を編集
# MAGIC
# MAGIC > **別の取り込み方法**: ソース構成ペインで「**ファイルからテーブルを作成**」を選択すると、
# MAGIC > マネージドテーブルとして Unity Catalog に保存されるため、大量データではパフォーマンスが優れます。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: 既存テーブルをソースに追加
# MAGIC 1. キャンバス上の「**＋**」ボタン → 「**ソース**」を選択
# MAGIC 2. 「**既存の参照**」をクリック → アセットセレクターが開く
# MAGIC 3. Unity Catalog から `monte_carlo_trials`（VaR シミュレーション結果）を選択
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 4: 変換演算子を追加・接続・設定
# MAGIC
# MAGIC **演算子の追加方法**（3通り）:
# MAGIC - キャンバス左側の **オペレーターメニュー** からドラッグ＆ドロップ
# MAGIC - 既存演算子の右側に表示される「**＋**」ボタンをクリック（自動接続）
# MAGIC - キャンバス上の「**＋**」ボタンから選択
# MAGIC
# MAGIC **演算子の接続**: 出力ハンドル（演算子右端の小さな円）から次の演算子の入力ハンドル（左端）へドラッグ。
# MAGIC データは **左から右** へ流れます。結合（Join）など一部の演算子は複数入力を受け付けます。
# MAGIC
# MAGIC **演算子の設定**: 演算子をダブルクリック、またはホバー時の鉛筆アイコンをクリック → 設定ペインが開く → 設定後「**適用**」。
# MAGIC
# MAGIC #### 4-1. 結合（Join）: Monte Carlo 結果 × ウェイト調整
# MAGIC 1. `monte_carlo_trials` の右側「**＋**」→「**結合**」を選択
# MAGIC 2. `weight_adjustments` の出力ハンドルを、この結合演算子の入力にドラッグして接続
# MAGIC 3. 結合演算子をダブルクリック → 設定ペインで条件を設定:
# MAGIC    - 結合タイプ: **Inner**
# MAGIC    - 一致する列: `monte_carlo_trials.ticker` = `weight_adjustments.ticker`
# MAGIC 4. 「**適用**」をクリック
# MAGIC
# MAGIC > **プレビュー**: 演算子を選択すると画面下部の **出力ペイン** で結果を確認できます。
# MAGIC > 右上のサイドバーアイコンで **データプロファイリング**（行数、値の分布等）も表示されます。
# MAGIC
# MAGIC #### 4-2. 変換（Transform）: 加重リターンの計算
# MAGIC 1. 結合演算子の右側「**＋**」→「**変換**」を選択
# MAGIC 2. ダブルクリック → 設定ペインで「**列を追加**」:
# MAGIC    - 列名: `adjusted_weighted_return`
# MAGIC    - 式: `returns * (new_weight_pct / 100)`
# MAGIC 3. 「**適用**」をクリック
# MAGIC
# MAGIC > **Genie Code 活用**: 式の記述がわからない場合、設定ペイン内の AI アシスタントに
# MAGIC > 「returns と new_weight_pct から加重リターンを計算する列を追加して」と自然言語で指示できます。
# MAGIC
# MAGIC #### 4-3. 集計（Aggregate）: 国別 VaR99 の計算
# MAGIC 1. 変換演算子の右側「**＋**」→「**集計**」を選択
# MAGIC 2. ダブルクリック → 設定ペインで:
# MAGIC    - グループ化列: `country`
# MAGIC    - 集計関数: `SUM(adjusted_weighted_return)` → 別名: `portfolio_return`
# MAGIC 3. 「**適用**」をクリック
# MAGIC
# MAGIC #### 4-4. 結合（Join）: 集計結果 × リスクリミット
# MAGIC 1. 集計演算子の右側「**＋**」→「**結合**」を選択
# MAGIC 2. `risk_limits` の出力ハンドルをこの結合演算子に接続
# MAGIC 3. 結合条件:
# MAGIC    - 結合タイプ: **Left**
# MAGIC    - 一致する列: `country` = `target`
# MAGIC 4. 「**適用**」をクリック
# MAGIC
# MAGIC #### 4-5. 変換（Transform）: BREACH / OK 判定
# MAGIC 1. 結合演算子の右側「**＋**」→「**変換**」を選択
# MAGIC 2. 「**列を追加**」:
# MAGIC    - 列名: `status`
# MAGIC    - 式: `CASE WHEN var_99 >= limit_value THEN 'OK' ELSE 'BREACH' END`
# MAGIC 3. 「**適用**」をクリック
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 5: 出力演算子を設定（Unity Catalog に書き込み）
# MAGIC 1. 最後の変換演算子の右側「**＋**」→「**出力**」を選択
# MAGIC 2. 出力演算子をダブルクリック → 設定ペインで以下を指定:
# MAGIC    - **テーブル名**: `risk_compliance_report`
# MAGIC    - **カタログ・スキーマ**: 現在のデモ環境を選択
# MAGIC 3. 「**実行**」をクリック → 結果が Delta テーブルに書き込まれます
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 6: スケジュール設定（本番自動化）
# MAGIC パイプラインを定期実行するには:
# MAGIC - 上部メニューの「**スケジュール**」ボタンからスケジュールを設定
# MAGIC - または「**ジョブに追加**」で既存ワークフローのタスクとして組み込み
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 便利な機能
# MAGIC | 操作 | 方法 |
# MAGIC |---|---|
# MAGIC | 演算子の名前変更 | 設定ペイン上部のテキストフィールドを編集 |
# MAGIC | 結果プレビュー | 演算子を選択 → 画面下部の出力ペインに表示 |
# MAGIC | 行数制御 | 出力ペインで「制限モード」（先頭N行）/ 「最大モード」（全行）を切替 |
# MAGIC | データプロファイリング | 出力ペイン右上のサイドバーアイコンをクリック |
# MAGIC | Genie Code（AI支援） | 設定ペイン内でプロンプトを入力して変換を自然言語で生成 |
# MAGIC | 自動レイアウト | ヘッダーの水平 DAG アイコンをクリック |
# MAGIC | フィット表示 | ヘッダーの拡大アイコンで全演算子をキャンバスに収める |
# MAGIC | 元に戻す/やり直し | `Cmd+Z` / `Cmd+Shift+Z` |
# MAGIC | 演算子コピー | ホバー時のコピーアイコン or `Cmd+C` |
# MAGIC
# MAGIC ### 利用可能な組み込み演算子（参考）
# MAGIC | カテゴリ | 演算子 | 概要 |
# MAGIC |---|---|---|
# MAGIC | ソース | ソース | テーブル参照、ファイルアップロード、Google Drive / SharePoint 連携 |
# MAGIC | 変換 | 結合（Join） | 一致する列で2テーブルをリンク |
# MAGIC | 変換 | 集計（Aggregate） | グループ化 + 集計関数（AVG, COUNT, MAX, SUM 等） |
# MAGIC | 変換 | フィルター | 条件ビルダーで行を絞り込み |
# MAGIC | 変換 | 変換（Transform） | 列の選択・追加・名前変更 |
# MAGIC | 変換 | ソート | 1つ以上の列で昇順/降順に並べ替え |
# MAGIC | 変換 | ピボット | 行↔列方向にデータを整形 |
# MAGIC | 変換 | 組み合わせ（Combine） | Union / Intersect / Except |
# MAGIC | 変換 | 制限（Limit） | 最大行数を制限 |
# MAGIC | 変換 | SQL | カスタム SQL SELECT 文を実行 |
# MAGIC | 変換 | Python | カスタム PySpark 処理を実行 |
# MAGIC | AI | ai_summarize, ai_classify 等 | 感情分析・テキスト分類・要約・翻訳など10種の AI 関数 |
# MAGIC | 出力 | 出力 | Unity Catalog のテーブルに結果を書き込み |
# MAGIC | 整理 | 注記 / グループ | Markdown メモの追加、演算子の視覚的グループ化 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 完成イメージ（DAG）
# MAGIC ```
# MAGIC ┌──────────────────────┐  ┌──────────────────────┐  ┌─────────────────────┐
# MAGIC │  weight_adjustments  │  │  monte_carlo_trials  │  │    risk_limits       │
# MAGIC │  (Excel D&D)         │  │  (既存テーブル参照)   │  │  (Excel D&D)         │
# MAGIC └────────┬─────────────┘  └──────────┬───────────┘  └──────────┬──────────┘
# MAGIC          │                           │                          │
# MAGIC          └──────────┐   ┌────────────┘                          │
# MAGIC                     ▼   ▼                                       │
# MAGIC              ┌──────────────────┐                               │
# MAGIC              │  結合 (ticker)   │                               │
# MAGIC              └────────┬─────────┘                               │
# MAGIC                       ▼                                         │
# MAGIC              ┌──────────────────┐                               │
# MAGIC              │  変換            │                               │
# MAGIC              │  加重リターン計算 │                               │
# MAGIC              └────────┬─────────┘                               │
# MAGIC                       ▼                                         │
# MAGIC              ┌──────────────────┐                               │
# MAGIC              │  集計 (country)  │                               │
# MAGIC              │  国別VaR99       │                               │
# MAGIC              └────────┬─────────┘                               │
# MAGIC                       │                                         │
# MAGIC                       └─────────────────┐   ┌──────────────────┘
# MAGIC                                         ▼   ▼
# MAGIC                                  ┌──────────────────┐
# MAGIC                                  │  結合 (country=  │
# MAGIC                                  │       target)    │
# MAGIC                                  └────────┬─────────┘
# MAGIC                                           ▼
# MAGIC                                  ┌──────────────────┐
# MAGIC                                  │  変換            │
# MAGIC                                  │  BREACH/OK 判定  │
# MAGIC                                  └────────┬─────────┘
# MAGIC                                           ▼
# MAGIC                                  ┌────────────────────────┐
# MAGIC                                  │  出力                  │
# MAGIC                                  │  risk_compliance_report│
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
# MAGIC | 構築者 | エンジニア | アナリスト・非エンジニアでも可 |
# MAGIC | データソース追加 | コード変更が必要 | Excel をキャンバスにドラッグ＆ドロップ |
# MAGIC | パイプラインの可視化 | コードを読む必要がある | DAG が自動表示、自動レイアウト |
# MAGIC | 変換の作成 | PySpark / SQL を記述 | 組み込み演算子を選択、または Genie Code に自然言語で指示 |
# MAGIC | 中間結果の確認 | `display()` でセルごとに実行 | 出力ペインでリアルタイムプレビュー＋データプロファイリング |
# MAGIC | スケジュール実行 | ジョブ設定が必要 | 「スケジュール」ボタンまたは「ジョブに追加」 |
# MAGIC | Excel 更新時の再実行 | ファイルの再アップロード＋再実行 | Excel を差し替えて「実行」 |
# MAGIC | Git 管理 | `.py` ファイル | `.designer.ipynb` ファイルとして Git 連携可能 |

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
