# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Unity Catalog によるデータガバナンス
# MAGIC
# MAGIC **進捗: ✅[00-03] → [04] ●○○○○○○**
# MAGIC
# MAGIC ### 前提条件
# MAGIC > **01_data_upload_and_volume** を先に実行してください（テーブルとVolumeが必要です）。
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
# MAGIC ALTER TABLE market_data SET TAGS ('domain' = 'finance', 'data_classification' = 'private');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE market_indicators SET TAGS ('domain' = 'finance', 'data_classification' = 'private');

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
# MAGIC -- 変更前のバージョンを確認
# MAGIC DESCRIBE HISTORY market_data LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### データ更新を行い、バージョンが進むことを確認
# MAGIC
# MAGIC 実際にデータを更新して、タイムトラベルでの過去バージョンアクセスを体験します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: 更新前の行数を確認
# MAGIC SELECT '更新前' AS status, COUNT(*) AS row_count FROM market_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: テスト用のダミー行を INSERT（意図的な変更）
# MAGIC INSERT INTO market_data (ticker, date, open, high, low, close, volume)
# MAGIC VALUES ('TEST', '2099-01-01', 100.0, 105.0, 95.0, 102.0, 999999.0)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: さらに UPDATE を実行（価格修正シナリオ）
# MAGIC UPDATE market_data SET close = 999.99 WHERE ticker = 'TEST'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: 変更後のバージョン履歴を確認（3つ以上のバージョンが存在するはず）
# MAGIC DESCRIBE HISTORY market_data LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 5: 最新バージョンではテスト行が更新済み
# MAGIC SELECT ticker, date, close FROM market_data WHERE ticker = 'TEST'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 6: タイムトラベルで INSERT 直後のバージョン（UPDATE 前）にアクセス
# MAGIC -- close = 102.0（UPDATE 前の値）であることを確認
# MAGIC SELECT ticker, date, close
# MAGIC FROM market_data VERSION AS OF 1
# MAGIC WHERE ticker = 'TEST'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 7: 最初のバージョン（INSERT 前）にはテスト行が存在しないことを確認
# MAGIC SELECT COUNT(*) AS test_rows
# MAGIC FROM market_data VERSION AS OF 0
# MAGIC WHERE ticker = 'TEST'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- クリーンアップ: テスト行を削除
# MAGIC DELETE FROM market_data WHERE ticker = 'TEST'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 結果の確認
# MAGIC
# MAGIC | バージョン | テスト行の状態 | close の値 |
# MAGIC |---|---|---|
# MAGIC | 0（初期ロード） | 存在しない | - |
# MAGIC | 1（INSERT後） | 存在する | 102.0 |
# MAGIC | 2（UPDATE後） | 存在する | 999.99 |
# MAGIC | 3（DELETE後=現在） | 存在しない | - |
# MAGIC
# MAGIC このように、Delta Lake はすべての変更履歴を保持しており、
# MAGIC 任意の時点のデータに `VERSION AS OF` で正確にアクセスできます。
# MAGIC
# MAGIC **リスク管理での活用**: 「先週金曜日時点のポートフォリオデータで VaR を再計算したい」
# MAGIC といった監査・バックテスト要件に即座に対応できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Lakehouse Monitoring（データ品質の継続監視）
# MAGIC
# MAGIC **Lakehouse Monitoring** を使うと、テーブルのデータ品質・統計量の推移を
# MAGIC 自動的にモニタリングできます。
# MAGIC
# MAGIC ### リスク管理での活用
# MAGIC - 市場データの **分布ドリフト**（急にリターン分布が変わった）を自動検知
# MAGIC - カラムの **NULL率** や **統計量** の推移を時系列で監視
# MAGIC - 異常が検知されたら **アラート** を発火 → VaR計算前にデータ品質を確認
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > 1. カタログ → テーブル選択 → 「品質」タブ → 「モニターを有効化」
# MAGIC > 2. モニタリング対象のカラムとスケジュールを設定
# MAGIC > 3. 自動生成されるダッシュボードで統計量の推移を確認

# COMMAND ----------

# market_data テーブルにモニターを作成
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

monitor_table = f"{config['database']['catalog']}.{config['database']['schema']}.{config['database']['tables']['stocks']}"

try:
    monitor = w.quality_monitors.create(
        table_name=monitor_table,
        assets_dir=f"/Shared/lakehouse_monitoring/{config['database']['schema']}",
        output_schema_name=f"{config['database']['catalog']}.{config['database']['schema']}",
        snapshot={}  # Snapshot profile type
    )
    print(f"モニター作成完了: {monitor_table}")
    print(f"  ダッシュボード: {monitor.dashboard_id}")
except Exception as e:
    if "MONITOR_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
        print(f"モニターは既に存在します: {monitor_table}")
        print("  → カタログ UI でテーブルの「品質」タブからダッシュボードを確認できます")
    else:
        print(f"モニター作成をスキップ: {str(e)[:200]}")
        print("  → UI から手動で有効化することもできます")

# COMMAND ----------

# MAGIC %md
# MAGIC ### モニタリングダッシュボードで確認できること
# MAGIC
# MAGIC | メトリクス | 説明 | リスク管理での意味 |
# MAGIC |---|---|---|
# MAGIC | **行数の推移** | テーブルの行数が時間とともにどう変化するか | データ取り込みの欠落検知 |
# MAGIC | **NULL率** | 各カラムのNULL率の推移 | データフィードの異常検知 |
# MAGIC | **統計量** | 平均、標準偏差、最小/最大値の推移 | 市場データの分布ドリフト検知 |
# MAGIC | **カラム分布** | ヒストグラムの時間変化 | リターン分布の構造変化（テールリスク増加等） |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Insights（テーブル利用状況の把握）
# MAGIC
# MAGIC Unity Catalog の **Insights** 機能で、テーブルが **誰に・どのくらい使われているか** を確認できます。
# MAGIC
# MAGIC ### 確認できる情報
# MAGIC - **アクセス頻度**: 過去30日間のクエリ回数
# MAGIC - **利用ユーザー**: どのユーザー/サービスプリンシパルがアクセスしているか
# MAGIC - **読み取り/書き込み**: READ / WRITE の内訳
# MAGIC - **最終アクセス日時**: 最後にアクセスされた日時
# MAGIC
# MAGIC ### リスク管理での活用
# MAGIC - 「このリスクデータは誰が利用しているか」を即座に把握 → **影響範囲分析**
# MAGIC - 使われていないテーブルの特定 → **データ資産の棚卸し**
# MAGIC - 特定ユーザーの異常なアクセスパターン → **内部不正の検知**
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > 1. 左メニュー「カタログ」→ テーブルを選択
# MAGIC > 2. 「Insights」タブ（または「概要」画面の利用状況セクション）
# MAGIC > 3. 「過去30日間のクエリ数」「アクセスしたユーザー」「読み取り/書き込み比率」を確認
# MAGIC >
# MAGIC > テーブルの「Insights」が表示されない場合は、Unity Catalog のメタストアで
# MAGIC > System Tables（`system.access.audit`）が有効化されている必要があります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **データリネージ**: テーブル間のデータフローの自動追跡と可視化
# MAGIC - **アクセス制御**: GRANT/REVOKE による権限管理と最小権限の原則
# MAGIC - **タグ付け**: テーブル・カラムのメタデータ管理とデータ分類
# MAGIC - **タイムトラベル**: 過去時点のデータへのアクセスと監査対応
# MAGIC - **Lakehouse Monitoring**: データ品質の継続監視と分布ドリフト検知
# MAGIC - **Insights**: テーブル利用状況の把握と影響範囲分析
# MAGIC
# MAGIC 次のノートブック `05_feature_engineering` では、
# MAGIC 市場データから **ボラティリティ** と **特徴量** を計算する方法を学びます。
