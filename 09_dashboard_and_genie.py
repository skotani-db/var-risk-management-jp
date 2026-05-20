# Databricks notebook source
# MAGIC %md
# MAGIC # 09. AI/BI Dashboard と Genie によるリスクレポーティング
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **AI/BI Dashboard (Lakeview)**: コードなしでインタラクティブなダッシュボードを作成
# MAGIC - **Genie**: 自然言語でデータに質問し、SQLを自動生成
# MAGIC - **SQL によるリスク分析**: ダッシュボード用の分析ビュー作成
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - **経営層向けレポート**: コードを見せずにVaR推移・リスク構造を可視化
# MAGIC - **規制当局向け**: バーゼルバックテスト結果をダッシュボードで即座に提示
# MAGIC - **セルフサービス分析**: リスクアナリストが自然言語でアドホック分析
# MAGIC
# MAGIC ## UI操作ポイント
# MAGIC > **AI/BI Dashboard の作成手順**:
# MAGIC > 1. 左メニュー「ダッシュボード」→「ダッシュボードを作成」→「Lakeview ダッシュボード」
# MAGIC > 2. 「データ」タブでSQLクエリを追加（このノートブックのSQLを使用）
# MAGIC > 3. 「キャンバス」タブでウィジェット（チャート、テーブル等）を配置
# MAGIC > 4. フィルターを追加（日付範囲、国、業種）
# MAGIC >
# MAGIC > **Genie の利用手順**:
# MAGIC > 1. 左メニュー「Genie」→「新しい Genie スペースを作成」
# MAGIC > 2. 対象テーブルを選択（market_data, monte_carlo_trials等）
# MAGIC > 3. 自然言語で質問（例:「先月のVaRが最も高かった国は？」）

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ダッシュボード用ビューの作成
# MAGIC
# MAGIC AI/BI Dashboard は **SQLクエリ** をデータソースとして使用します。
# MAGIC 複雑な計算はビューとして事前に定義し、ダッシュボードからはシンプルなSELECTで参照します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VaR結果を格納するビュー（ノートブック08の結果をSQLで再現）
# MAGIC CREATE OR REPLACE VIEW v_daily_risk_summary AS
# MAGIC SELECT
# MAGIC   s.date,
# MAGIC   s.ticker,
# MAGIC   s.close,
# MAGIC   p.country,
# MAGIC   p.industry,
# MAGIC   p.company,
# MAGIC   p.weight,
# MAGIC   -- 前日比リターン
# MAGIC   LN(s.close / LAG(s.close) OVER (PARTITION BY s.ticker ORDER BY s.date)) AS daily_return
# MAGIC FROM market_data s
# MAGIC JOIN (SELECT * FROM VALUES
# MAGIC   ('BCH', 'CHILE', 'Banks', 'Banco de Chile', 0.0344827586),
# MAGIC   ('BSAC', 'CHILE', 'Banks', 'Banco Santander-Chile', 0.0344827586),
# MAGIC   ('CIB', 'COLOMBIA', 'Banks', 'BanColombia S.A.', 0.0344827586),
# MAGIC   ('EC', 'COLOMBIA', 'Oil & Gas Producers', 'Ecopetrol S.A.', 0.0344827586),
# MAGIC   ('AMX', 'MEXICO', 'Mobile Telecommunications', 'America Movil', 0.0344827586),
# MAGIC   ('SCCO', 'PERU', 'Industrial Metals & Mining', 'Southern Copper', 0.0344827586),
# MAGIC   ('BAP', 'PERU', 'Banks', 'Credicorp Ltd.', 0.0344827586)
# MAGIC   AS p(ticker, country, industry, company, weight)
# MAGIC ) p ON s.ticker = p.ticker
# MAGIC WHERE s.close IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ポートフォリオ全体の日次加重リターン
# MAGIC CREATE OR REPLACE VIEW v_portfolio_daily_return AS
# MAGIC SELECT
# MAGIC   date,
# MAGIC   SUM(daily_return * weight) AS portfolio_return,
# MAGIC   COUNT(DISTINCT ticker) AS num_tickers
# MAGIC FROM v_daily_risk_summary
# MAGIC WHERE daily_return IS NOT NULL
# MAGIC GROUP BY date
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 国別の平均リターンとボラティリティ
# MAGIC CREATE OR REPLACE VIEW v_country_risk_profile AS
# MAGIC SELECT
# MAGIC   country,
# MAGIC   AVG(daily_return) AS avg_daily_return,
# MAGIC   STDDEV(daily_return) AS volatility,
# MAGIC   MIN(daily_return) AS worst_day,
# MAGIC   MAX(daily_return) AS best_day,
# MAGIC   COUNT(*) AS observations
# MAGIC FROM v_daily_risk_summary
# MAGIC WHERE daily_return IS NOT NULL
# MAGIC GROUP BY country

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ダッシュボードで使用するサンプルクエリ: 国別リスクプロファイル
# MAGIC SELECT * FROM v_country_risk_profile ORDER BY volatility DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ダッシュボードで使用するサンプルクエリ: ポートフォリオリターン推移
# MAGIC SELECT * FROM v_portfolio_daily_return ORDER BY date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. AI/BI Dashboard の作成ガイド
# MAGIC
# MAGIC ### 推奨ダッシュボード構成
# MAGIC
# MAGIC | ウィジェット | データソース | チャートタイプ |
# MAGIC |---|---|---|
# MAGIC | ポートフォリオVaR推移 | `v_portfolio_daily_return` | 折れ線グラフ |
# MAGIC | 国別リスク比較 | `v_country_risk_profile` | 棒グラフ |
# MAGIC | 銘柄別ボラティリティ | `v_daily_risk_summary` | ヒートマップ |
# MAGIC | バーゼルゾーン状況 | カスタムSQL | KPI / カウンター |
# MAGIC | 最新VaR値 | カスタムSQL | KPI |
# MAGIC
# MAGIC ### フィルター設定
# MAGIC - **日付範囲**: ユーザーが分析期間を選択
# MAGIC - **国**: 特定の国に絞り込み
# MAGIC - **業種**: 業種別のドリルダウン
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > 1. ダッシュボード作成画面で「データ」タブ → 「SQLクエリを追加」
# MAGIC > 2. 上記のビュー名を `SELECT * FROM v_portfolio_daily_return` のように指定
# MAGIC > 3. 「キャンバス」タブでドラッグ＆ドロップでウィジェットを配置
# MAGIC > 4. 各ウィジェットでチャートタイプ、X軸、Y軸を設定
# MAGIC > 5. 「フィルター」を追加して、インタラクティブな操作を実現

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Genie によるセルフサービス分析
# MAGIC
# MAGIC **Genie** は Databricks の AI アシスタントで、自然言語でデータに質問できます。
# MAGIC
# MAGIC ### Genie でできること
# MAGIC - 「先月のVaRが最も高かった日は？」→ SQLを自動生成して結果を返答
# MAGIC - 「メキシコの銘柄のボラティリティを比較して」→ チャート付きで回答
# MAGIC - 「過去1年で最大の損失を出した銘柄は？」→ 即座に分析
# MAGIC
# MAGIC ### Genie スペースの設定
# MAGIC
# MAGIC | 設定項目 | 推奨値 |
# MAGIC |---|---|
# MAGIC | テーブル | `market_data`, `market_indicators`, `v_daily_risk_summary` |
# MAGIC | 説明 | 「ラテンアメリカ株式ポートフォリオのリスク分析データ」 |
# MAGIC | サンプル質問 | 下記参照 |
# MAGIC
# MAGIC ### サンプル質問例
# MAGIC ```
# MAGIC - 「各国のポートフォリオ銘柄数と平均ボラティリティを教えて」
# MAGIC - 「最もボラティリティが高い銘柄トップ5は？」
# MAGIC - 「2025年1月の銘柄別リターンを棒グラフで表示して」
# MAGIC - 「S&P500と最も相関が高い銘柄は？」
# MAGIC ```
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > 1. 左メニュー「Genie」→「新しい Genie スペースを作成」
# MAGIC > 2. スペース名: 「VaR リスク分析」
# MAGIC > 3. テーブルを追加: 上記のテーブル/ビューを選択
# MAGIC > 4. 「指示」に分析のコンテキストを記述（ポートフォリオの説明等）
# MAGIC > 5. チャット欄に自然言語で質問を入力

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. リスクレポート用SQLサンプル集
# MAGIC
# MAGIC 以下のSQLは、ダッシュボードやGenieで活用できる分析クエリです。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI: ポートフォリオの主要リスク指標
# MAGIC SELECT
# MAGIC   ROUND(AVG(portfolio_return) * 252, 4) AS annualized_return,
# MAGIC   ROUND(STDDEV(portfolio_return) * SQRT(252), 4) AS annualized_volatility,
# MAGIC   ROUND(MIN(portfolio_return), 4) AS worst_daily_loss,
# MAGIC   ROUND(PERCENTILE(portfolio_return, 0.01), 4) AS var_99_historical
# MAGIC FROM v_portfolio_daily_return

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 月次リスクサマリー（経営報告用）
# MAGIC SELECT
# MAGIC   DATE_TRUNC('month', date) AS month,
# MAGIC   ROUND(AVG(portfolio_return), 6) AS avg_daily_return,
# MAGIC   ROUND(STDDEV(portfolio_return), 6) AS daily_volatility,
# MAGIC   ROUND(MIN(portfolio_return), 6) AS worst_day,
# MAGIC   COUNT(*) AS trading_days
# MAGIC FROM v_portfolio_daily_return
# MAGIC GROUP BY DATE_TRUNC('month', date)
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **SQL ビュー** でダッシュボード用のデータソースを事前定義
# MAGIC - **AI/BI Dashboard (Lakeview)** の作成手順と推奨構成
# MAGIC - **Genie** による自然言語でのセルフサービス分析
# MAGIC - リスクレポート用の **SQL クエリテンプレート**
# MAGIC
# MAGIC 次のノートブック `10_operations_system_tables` では、
# MAGIC VaR 計算パイプラインの **運用監視** と **コスト管理** を System Tables で実現します。
