# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Unity Catalog によるデータガバナンス
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **データリネージ**: テーブル間のデータの流れを自動追跡・可視化
# MAGIC - **アクセス制御**: GRANT/REVOKE でテーブル・スキーマ単位の権限管理
# MAGIC - **タグ付け**: テーブル・カラムにメタデータを付与して分類・検索
# MAGIC - **データ分類**: 個人情報（PII）や機密データのラベリング
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - **規制対応**: データの出所と変換履歴を自動追跡（バーゼルIII、BCBS239）
# MAGIC - **最小権限の原則**: リスクアナリスト/トレーダー/監査人ごとに適切なアクセス権限
# MAGIC - **データカタログ**: リスクデータ資産の発見・理解を促進
# MAGIC
# MAGIC ## UI操作ポイント
# MAGIC > **リネージの確認方法**:
# MAGIC > 1. 左メニュー「カタログ」→ テーブルを選択
# MAGIC > 2. 「リネージ」タブをクリック
# MAGIC > 3. 上流（このテーブルの元データ）と下流（このテーブルを使うテーブル）が可視化
# MAGIC >
# MAGIC > **権限の確認方法**:
# MAGIC > 1. テーブルを選択 → 「権限」タブ
# MAGIC > 2. 現在のGRANT一覧と、権限の継承元を確認

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. データリネージ
# MAGIC
# MAGIC Unity Catalog は、ノートブックやジョブでの読み書き操作を **自動的に追跡** し、
# MAGIC テーブル間のデータフロー（リネージ）を記録します。
# MAGIC
# MAGIC ### VaRパイプラインのリネージ例
# MAGIC ```
# MAGIC Volume (CSV)
# MAGIC   → market_data (株式データ)
# MAGIC       → market_volatility (ボラティリティ)
# MAGIC           → monte_carlo_market (シミュレーション市場条件)
# MAGIC               → monte_carlo_trials (シミュレーション結果)
# MAGIC
# MAGIC   → market_indicators (市場指標)
# MAGIC       → market_volatility
# MAGIC ```
# MAGIC
# MAGIC ### リスク管理での活用
# MAGIC - **BCBS 239**: バーゼル委員会のリスクデータ集約原則では、データの正確性と**トレーサビリティ**が求められます
# MAGIC - **モデルリスク管理 (SR 11-7)**: モデルに使用したデータの**出所**を説明する必要があります
# MAGIC - **監査対応**: 規制当局の検査時に「このVaR値はどのデータから計算されたか」を即座に回答

# COMMAND ----------

# MAGIC %md
# MAGIC ### テーブル一覧とリネージの確認
# MAGIC
# MAGIC 以下のSQLで現在のスキーマ内のテーブルを確認します。
# MAGIC カタログUIでは各テーブルのリネージをグラフィカルに確認できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの詳細情報（作成日時、場所、統計等）
# MAGIC DESCRIBE EXTENDED market_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. アクセス制御（権限管理）
# MAGIC
# MAGIC Unity Catalog では **ANSI SQL 標準** の GRANT/REVOKE でアクセス制御を行います。
# MAGIC
# MAGIC ### 権限の階層
# MAGIC ```
# MAGIC カタログレベル    → スキーマ、テーブル全てに適用
# MAGIC  └ スキーマレベル  → そのスキーマ内のテーブルに適用
# MAGIC    └ テーブルレベル → 特定のテーブルのみに適用
# MAGIC ```
# MAGIC
# MAGIC ### リスク部門の典型的な権限設計
# MAGIC | ロール | カタログ | スキーマ | VaR結果テーブル | 生データ |
# MAGIC |---|---|---|---|---|
# MAGIC | リスクアナリスト | USE | USE | SELECT | SELECT |
# MAGIC | トレーダー | USE | USE | SELECT | - |
# MAGIC | 監査人 | USE | USE | SELECT | SELECT |
# MAGIC | データエンジニア | USE | ALL PRIVILEGES | ALL PRIVILEGES | ALL PRIVILEGES |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 現在の権限を確認（スキーマ名は config/application.yaml の値に合わせてください）
# MAGIC SHOW GRANTS ON SCHEMA var_risk_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### 権限付与の例
# MAGIC
# MAGIC 以下は、リスクアナリストグループにテーブルの読み取り権限を付与する例です。
# MAGIC
# MAGIC ```sql
# MAGIC -- リスクアナリストに VaR 結果テーブルの SELECT 権限を付与
# MAGIC GRANT SELECT ON TABLE monte_carlo_trials TO `risk_analysts`;
# MAGIC
# MAGIC -- トレーダーには集約結果のみ閲覧可能
# MAGIC GRANT SELECT ON TABLE monte_carlo_trials TO `traders`;
# MAGIC REVOKE SELECT ON TABLE market_data FROM `traders`;
# MAGIC
# MAGIC -- 監査人にはスキーマ全体の読み取り権限
# MAGIC GRANT USE SCHEMA ON SCHEMA var_risk_demo TO `auditors`;
# MAGIC GRANT SELECT ON SCHEMA var_risk_demo TO `auditors`;
# MAGIC ```
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > 権限はSQLだけでなく、UIからも設定できます：
# MAGIC > 1. カタログ → テーブル選択 → 「権限」タブ → 「権限を付与」
# MAGIC > 2. ユーザー/グループを選択し、権限レベルを設定

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. タグ付けとデータ分類
# MAGIC
# MAGIC テーブルやカラムに **タグ** を付与することで、データ資産の分類・検索が容易になります。
# MAGIC
# MAGIC ### リスク管理でのタグ活用例
# MAGIC - `risk_data`: リスク計算に使用するデータ
# MAGIC - `pii`: 個人情報を含むデータ（顧客名、口座番号等）
# MAGIC - `regulatory`: 規制報告に使用するデータ
# MAGIC - `confidential`: 社外秘データ

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルにタグを設定
# MAGIC ALTER TABLE market_data SET TAGS ('domain' = 'finance', 'data_classification' = 'internal');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE market_indicators SET TAGS ('domain' = 'finance', 'data_classification' = 'internal');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カラムレベルのタグ（将来的に顧客データを扱う場合）
# MAGIC -- ALTER TABLE customer_portfolio ALTER COLUMN customer_id SET TAGS ('pii' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルにコメントを追加（データカタログでの説明）
# MAGIC COMMENT ON TABLE market_data IS 'ラテンアメリカ27銘柄のOHLCVデータ。VaR計算のソースデータ。日次更新。';
# MAGIC COMMENT ON TABLE market_indicators IS '主要市場指標（S&P500, NYSE, 原油, 国債, ダウ）。VaRモデルの特徴量として使用。';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. テーブルのバージョン管理（タイムトラベル）
# MAGIC
# MAGIC Delta Lake の **タイムトラベル** 機能により、過去の任意の時点のデータにアクセスできます。
# MAGIC
# MAGIC ### リスク管理での活用
# MAGIC - **バックテスト**: 過去時点のデータでVaRを再計算し、現在の結果と比較
# MAGIC - **監査**: 「○月○日時点でどのデータが使われていたか」を正確に再現
# MAGIC - **誤操作のリカバリ**: データを誤って更新・削除した場合に過去バージョンに復元

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの変更履歴を確認
# MAGIC DESCRIBE HISTORY market_data LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 過去バージョンのデータにアクセス（例: バージョン0）
# MAGIC -- SELECT * FROM market_data VERSION AS OF 0 LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **データリネージ**: テーブル間のデータフローの自動追跡と可視化
# MAGIC - **アクセス制御**: GRANT/REVOKE による権限管理と最小権限の原則
# MAGIC - **タグ付け**: テーブル・カラムのメタデータ管理とデータ分類
# MAGIC - **タイムトラベル**: 過去時点のデータへのアクセスと監査対応
# MAGIC
# MAGIC 次のノートブック `05_feature_engineering` では、
# MAGIC 市場データから **ボラティリティ** と **特徴量** を計算する方法を学びます。
