# Databricks notebook source
# MAGIC %md
# MAGIC # 演習の回答集
# MAGIC
# MAGIC 各ノートブックの「やってみよう」セクションの回答・解説です。
# MAGIC 自分で考えてから参照してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01: Data Upload & Volume
# MAGIC
# MAGIC ### 演習3: 銘柄ごとの行数を確認

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ticker, COUNT(*) as cnt FROM market_data GROUP BY ticker ORDER BY cnt DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **解説**: 全銘柄がほぼ同じ行数（営業日数）になるはずです。
# MAGIC 特定の銘柄だけ行数が少ない場合、データ取り込みの欠落が疑われます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05: Feature Engineering
# MAGIC
# MAGIC ### 演習1: ボラティリティウィンドウを 90日 → 30日 に変更
# MAGIC
# MAGIC `config/configure_notebook.py` の以下を変更して 05 を再実行:
# MAGIC ```python
# MAGIC 'volatility': 30,  # 元は 90
# MAGIC ```
# MAGIC
# MAGIC **期待される結果**:
# MAGIC - ウィンドウが短いため、直近の市場変動に敏感なボラティリティが計算される
# MAGIC - ボラティリティの値がより大きく変動する（スパイクが鋭くなる）
# MAGIC - 長期ウィンドウ（90日）は平滑化効果があり、短期イベントの影響を緩和する
# MAGIC
# MAGIC ### 演習2: 相関行列の読み方
# MAGIC
# MAGIC **回答例**:
# MAGIC - S&P500 と最も相関が高いのは **DOWJONES**（約0.96）: 同じ米国株式市場を反映するため
# MAGIC - S&P500 と最も相関が低い（負の相関）のは **TREASURY**（約-0.40）: 株式が下落すると安全資産の国債に資金が流れるため（フライト・トゥ・クオリティ）
# MAGIC - OIL は他の指標との相関が比較的低い: 地政学リスクなど固有の要因で動くため

# COMMAND ----------

# MAGIC %md
# MAGIC ## 08: VaR Aggregation & Compliance
# MAGIC
# MAGIC ### 演習1: VaR95 と VaR99 の比較

# COMMAND ----------

# VaR95: 信頼水準 95% → 上位5%の損失
# VaR99: 信頼水準 99% → 上位1%の損失
# VaR99 のほうが絶対値が大きくなる（より極端な損失シナリオを捕捉）

# 以下のように変更して再実行:
# get_var_udf(F.col('returns'), F.lit(95))

# COMMAND ----------

# MAGIC %md
# MAGIC **解説**:
# MAGIC - VaR95 は VaR99 より **絶対値が小さい**（損失が少ない）
# MAGIC - 95%信頼水準 = 「20営業日に1回は超える損失」
# MAGIC - 99%信頼水準 = 「100営業日に1回は超える損失」
# MAGIC - バーゼル規制では **VaR99** が基準（より保守的）
# MAGIC
# MAGIC ### 演習2: メキシコの業種別リスク寄与度

# COMMAND ----------

# MAGIC %sql
# MAGIC -- メキシコはポートフォリオ内で最も銘柄数が多い（12銘柄）
# MAGIC -- 業種が多様（テレコム、建設、飲料、航空、銀行等）なため、
# MAGIC -- リスクが特定業種に集中しにくい = 分散効果が大きい
# MAGIC --
# MAGIC -- 一方ペルーは4銘柄で、鉱業のウェイトが高いため
# MAGIC -- コモディティ価格の影響を受けやすい
# MAGIC SELECT country, COUNT(DISTINCT ticker) as num_tickers, COUNT(DISTINCT industry) as num_industries
# MAGIC FROM market_data m
# MAGIC JOIN (SELECT * FROM VALUES
# MAGIC   ('AMX', 'MEXICO'), ('AMOV', 'MEXICO'), ('CX', 'MEXICO'), ('KOF', 'MEXICO'),
# MAGIC   ('VLRS', 'MEXICO'), ('FMX', 'MEXICO'), ('PAC', 'MEXICO'), ('ASR', 'MEXICO'),
# MAGIC   ('BSMX', 'MEXICO'), ('SIM', 'MEXICO'), ('TV', 'MEXICO'), ('IBA', 'MEXICO'),
# MAGIC   ('SCCO', 'PERU'), ('FSM', 'PERU'), ('CPAC', 'PERU'), ('BAP', 'PERU')
# MAGIC   AS p(ticker, country)
# MAGIC ) p ON m.ticker = p.ticker
# MAGIC GROUP BY country
