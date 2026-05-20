# Databricks notebook source
# MAGIC %md
# MAGIC # 09. AI/BI Dashboard と Genie によるリスクレポーティング
# MAGIC
# MAGIC **進捗: ✅[00-08] → [09] ●○**
# MAGIC
# MAGIC ### 前提条件
# MAGIC > **01_data_upload_and_volume** を先に実行してください（`market_data` テーブルが必要です）。
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
# MAGIC **Genie** は Databricks の AI アシスタントで、自然言語でデータに質問し SQL を自動生成します。
# MAGIC ただし、**ドメイン固有の専門用語** に対応するには適切な設定が必要です。
# MAGIC
# MAGIC ここでは「設定なしで質問 → 失敗 → メタデータを充実 → 成功」の流れを体験します。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: まず Genie スペースを最小構成で作成
# MAGIC
# MAGIC > 1. 左メニュー「Genie」→「新しい Genie スペースを作成」
# MAGIC > 2. スペース名: 「VaR リスク分析」
# MAGIC > 3. テーブルに `v_daily_risk_summary` のみ追加
# MAGIC > 4. **インストラクション（指示）は空のまま**
# MAGIC > 5. 「保存」

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: リスクの専門用語で質問してみる（失敗体験）
# MAGIC
# MAGIC 以下の質問を Genie に投げてみてください：
# MAGIC
# MAGIC ```
# MAGIC ポートフォリオ全体のテールリスクが最も高い国はどこですか？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC ボラティリティが急騰した期間の銘柄別エクスポージャーを見せてください
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 2025年Q1のヒストリカルVaR99を月次で比較してください
# MAGIC ```
# MAGIC
# MAGIC **期待される結果**: Genie は「テールリスク」「ボラティリティ」「エクスポージャー」
# MAGIC 「ヒストリカルVaR99」といった専門用語がテーブルのどのカラム・どの計算に
# MAGIC 対応するか分からず、**的外れな SQL を生成するか、回答できない** はずです。
# MAGIC
# MAGIC これは Genie の限界ではなく、**テーブルのメタデータが不足している** ことが原因です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: テーブル・カラムのコメントを充実させる
# MAGIC
# MAGIC Genie はテーブルやカラムの **コメント（説明文）** を参照して SQL を生成します。
# MAGIC コメントを充実させることで、専門用語とデータの対応関係を教えます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルコメント: Genie にテーブルの目的と分析文脈を伝える
# MAGIC COMMENT ON TABLE v_daily_risk_summary IS
# MAGIC 'ラテンアメリカ27銘柄の均等加重ポートフォリオの日次リスクサマリー。各行は1銘柄の1営業日のデータ。VaR計算、ボラティリティ分析、国別・業種別リスク分解に使用。';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カラムコメント付きでビューを再作成
# MAGIC -- ビューは ALTER COLUMN COMMENT が使えないため、カラムコメント付き SELECT で再定義
# MAGIC CREATE OR REPLACE VIEW v_daily_risk_summary
# MAGIC (
# MAGIC   date COMMENT '営業日',
# MAGIC   ticker COMMENT '銘柄コード（Yahoo Finance ティッカー）',
# MAGIC   close COMMENT '当日の終値（USD）。株価の推移分析に使用。',
# MAGIC   country COMMENT '銘柄の所属国（CHILE, COLOMBIA, MEXICO, PANAMA, PERU）。国別リスク分解のグループキー。',
# MAGIC   industry COMMENT '銘柄の業種分類。業種別リスク寄与度の分析に使用。',
# MAGIC   company COMMENT '企業名',
# MAGIC   weight COMMENT 'ポートフォリオにおける銘柄のウェイト（均等加重=約3.4%）。エクスポージャー = weight * STDDEV(daily_return) で計算。',
# MAGIC   daily_return COMMENT '日次対数リターン（LN(当日終値/前日終値)）。ボラティリティ = この値の標準偏差（STDDEV）。テールリスク = この値の1パーセンタイル（PERCENTILE 0.01）。VaR99 = この値の1パーセンタイル。'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   s.date,
# MAGIC   s.ticker,
# MAGIC   s.close,
# MAGIC   p.country,
# MAGIC   p.industry,
# MAGIC   p.company,
# MAGIC   p.weight,
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
# MAGIC -- ポートフォリオビューにもコメントを追加
# MAGIC COMMENT ON TABLE v_portfolio_daily_return IS
# MAGIC 'ポートフォリオ全体の日次加重リターン。portfolio_return = 各銘柄の(daily_return * weight)の合計。ポートフォリオレベルのVaR、ボラティリティ計算に使用。';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE v_country_risk_profile IS
# MAGIC '国別のリスクプロファイル集計。volatility = 日次リターンの標準偏差、worst_day = 最大損失日のリターン。';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Genie インストラクション（指示）を設定
# MAGIC
# MAGIC Genie スペースの設定画面で「インストラクション」に以下を貼り付けてください。
# MAGIC これにより、リスク分野の専門用語を SQL に正しく変換できるようになります。
# MAGIC
# MAGIC > **UI操作**: Genie スペース → 右上の歯車アイコン → 「General instructions」に以下を貼り付け
# MAGIC
# MAGIC ```
# MAGIC このデータはラテンアメリカ株式ポートフォリオ（27銘柄、均等加重）のリスク分析用です。
# MAGIC
# MAGIC ## 用語とSQL計算の対応
# MAGIC - 「ボラティリティ」= STDDEV(daily_return) で計算。年率換算は × SQRT(252)
# MAGIC - 「テールリスク」= PERCENTILE(daily_return, 0.01) で計算（下位1%の損失）
# MAGIC - 「VaR99」「ヒストリカルVaR」= PERCENTILE(daily_return, 0.01) と同義
# MAGIC - 「エクスポージャー」= weight × STDDEV(daily_return) で計算
# MAGIC - 「シャープレシオ」= AVG(daily_return) / STDDEV(daily_return) × SQRT(252)
# MAGIC - 「最大ドローダウン」= 期間中の累積リターンの最大下落幅
# MAGIC - 「期待ショートフォール」「CVaR」= VaR99を超えた損失の平均値
# MAGIC
# MAGIC ## 分析の注意点
# MAGIC - daily_return が NULL の行はフィルタする (WHERE daily_return IS NOT NULL)
# MAGIC - 日付フィルタは date カラムを使用（例: date >= '2025-01-01'）
# MAGIC - 国別分析は country カラム、業種別は industry カラムでグループ化
# MAGIC - 四半期は Q1=1-3月, Q2=4-6月, Q3=7-9月, Q4=10-12月
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: 同じ質問を再度投げる（成功体験）
# MAGIC
# MAGIC Step 2 と同じ質問を Genie に再度投げてみてください：
# MAGIC
# MAGIC ```
# MAGIC ポートフォリオ全体のテールリスクが最も高い国はどこですか？
# MAGIC ```
# MAGIC
# MAGIC **期待される結果**: Genie が以下のような SQL を生成し、正しい回答を返すはずです：
# MAGIC ```sql
# MAGIC SELECT country, PERCENTILE(daily_return, 0.01) AS tail_risk
# MAGIC FROM v_daily_risk_summary
# MAGIC WHERE daily_return IS NOT NULL
# MAGIC GROUP BY country
# MAGIC ORDER BY tail_risk ASC
# MAGIC ```
# MAGIC
# MAGIC 他にも試してみましょう：
# MAGIC ```
# MAGIC ボラティリティが急騰した期間の銘柄別エクスポージャーを見せてください
# MAGIC ```
# MAGIC ```
# MAGIC 2025年Q1のヒストリカルVaR99を月次で比較してください
# MAGIC ```
# MAGIC ```
# MAGIC チリの銘柄のシャープレシオを比較して
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie の精度を上げる Tips まとめ
# MAGIC
# MAGIC | 施策 | 効果 | 優先度 |
# MAGIC |---|---|---|
# MAGIC | **テーブルコメント** | テーブルの用途・文脈を伝える | 高 |
# MAGIC | **カラムコメント** | 専門用語とカラムの対応を明示 | 高 |
# MAGIC | **インストラクション** | ドメイン用語の計算式を定義 | 高 |
# MAGIC | **サンプルクエリ** | 正しいSQLの手本を提供 | 中 |
# MAGIC | **ビュー名の工夫** | `v_country_risk_profile` のように意味のある名前 | 中 |
# MAGIC | **不要カラムの除外** | Genie の選択肢を絞り、精度を上げる | 低 |
# MAGIC
# MAGIC **ポイント**: Genie は「テーブルの中身を知っている AI」ではなく、
# MAGIC **「メタデータとインストラクションを手がかりにSQLを生成する AI」** です。
# MAGIC メタデータの質 = Genie の回答の質 と考えてください。

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
