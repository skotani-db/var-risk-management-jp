# Databricks notebook source
# MAGIC %md
# MAGIC # 10. 運用監視と System Tables によるコスト管理
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC - **System Tables**: Databricksの利用状況・ジョブ実行・コストを記録するメタデータテーブル
# MAGIC - **ジョブ実行履歴**: VaR計算パイプラインの成功/失敗・実行時間の追跡
# MAGIC - **コスト配賦**: チーム/ワークロード別の消費DBU・課金額の把握
# MAGIC - **SLA管理**: ジョブの実行時間が許容範囲内か監視
# MAGIC
# MAGIC ## リスク管理でのメリット
# MAGIC - **運用リスクの可視化**: VaR計算が予定通り完了しているか、遅延は発生していないか
# MAGIC - **コスト管理**: リスク計量のコンピュート費用をチーム・プロジェクト別に配賦
# MAGIC - **障害対応**: ジョブ失敗の早期検知と原因特定
# MAGIC - **キャパシティプランニング**: 将来の計算量増加に備えたリソース計画
# MAGIC
# MAGIC ## UI操作ポイント
# MAGIC > **System Tables の場所**:
# MAGIC > 左メニュー「カタログ」→「system」カタログ → 各スキーマ
# MAGIC > - `system.billing` → 課金・DBU消費
# MAGIC > - `system.compute` → クラスター利用状況
# MAGIC > - `system.lakeflow` → DLTパイプライン実行履歴
# MAGIC > - `system.workflow` → ジョブ実行履歴

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. System Tables の概要
# MAGIC
# MAGIC System Tables は Databricks が自動的に記録する **メタデータテーブル** です。
# MAGIC Unity Catalog の `system` カタログに格納されています。
# MAGIC
# MAGIC ### 主要な System Tables
# MAGIC | テーブル | 内容 | リスク部門での活用 |
# MAGIC |---|---|---|
# MAGIC | `system.billing.usage` | DBU消費量・課金 | VaR計算のコスト追跡 |
# MAGIC | `system.compute.clusters` | クラスター情報 | 計算リソースの効率性 |
# MAGIC | `system.workflow.job_run_timeline` | ジョブ実行履歴 | パイプラインの成功/失敗監視 |
# MAGIC | `system.workflow.jobs` | ジョブ定義 | パイプラインの構成管理 |
# MAGIC | `system.lakeflow.pipeline_event_log` | DLTイベントログ | データ品質パイプラインの監視 |
# MAGIC | `system.access.audit` | 監査ログ | データアクセスの追跡 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ジョブ実行履歴の確認
# MAGIC
# MAGIC VaR計算パイプラインをジョブとしてスケジュール実行している場合、
# MAGIC `system.workflow.job_run_timeline` で実行履歴を確認できます。
# MAGIC
# MAGIC ### SLA管理の観点
# MAGIC リスク計量部門では、以下のようなSLAが求められます：
# MAGIC - **日次VaR計算**: 毎朝8:00までに完了
# MAGIC - **モンテカルロシミュレーション**: 2時間以内に完了
# MAGIC - **データ取り込み**: 市場データ配信から30分以内

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ジョブ実行履歴（直近30日）
# MAGIC -- 注意: system テーブルはワークスペースの権限設定により閲覧できない場合があります
# MAGIC SELECT
# MAGIC   j.name AS job_name,
# MAGIC   r.run_id,
# MAGIC   r.result_state,
# MAGIC   r.start_time,
# MAGIC   r.end_time,
# MAGIC   TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time) AS duration_minutes
# MAGIC FROM system.workflow.job_run_timeline r
# MAGIC JOIN system.workflow.jobs j ON r.job_id = j.job_id
# MAGIC WHERE r.start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
# MAGIC   AND LOWER(j.name) LIKE '%var%' OR LOWER(j.name) LIKE '%risk%'
# MAGIC ORDER BY r.start_time DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ジョブの成功/失敗率（リスク計算パイプラインの信頼性）
# MAGIC SELECT
# MAGIC   j.name AS job_name,
# MAGIC   COUNT(*) AS total_runs,
# MAGIC   SUM(CASE WHEN r.result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
# MAGIC   SUM(CASE WHEN r.result_state != 'SUCCESS' THEN 1 ELSE 0 END) AS failure_count,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN r.result_state = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
# MAGIC   ) AS success_rate_pct,
# MAGIC   ROUND(AVG(TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time)), 1) AS avg_duration_minutes
# MAGIC FROM system.workflow.job_run_timeline r
# MAGIC JOIN system.workflow.jobs j ON r.job_id = j.job_id
# MAGIC WHERE r.start_time >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())
# MAGIC GROUP BY j.name
# MAGIC ORDER BY total_runs DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. コスト追跡（DBU消費量）
# MAGIC
# MAGIC `system.billing.usage` テーブルで、DBU（Databricks Unit）消費量と
# MAGIC 推定コストを確認できます。
# MAGIC
# MAGIC ### リスク部門のコスト配賦
# MAGIC - **ワークロード別**: VaR計算、データ取り込み、ダッシュボードのコストを分離
# MAGIC - **チーム別**: タグやクラスター名でコストをチーム配賦
# MAGIC - **トレンド分析**: 月次のコスト推移から予算超過を早期検知

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 日次DBU消費量（直近30日）
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
# MAGIC GROUP BY usage_date, sku_name
# MAGIC ORDER BY usage_date DESC, total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SKU別の月次コスト推移
# MAGIC SELECT
# MAGIC   DATE_TRUNC('month', usage_date) AS month,
# MAGIC   sku_name,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= DATEADD(MONTH, -6, CURRENT_DATE())
# MAGIC GROUP BY DATE_TRUNC('month', usage_date), sku_name
# MAGIC ORDER BY month, total_dbus DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. クラスター利用効率
# MAGIC
# MAGIC Serverless 環境ではクラスター管理は不要ですが、
# MAGIC 既存のクラスターを使用している場合は利用効率を確認できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- クラスター利用状況（Serverless以外の環境向け）
# MAGIC SELECT
# MAGIC   cluster_name,
# MAGIC   cluster_id,
# MAGIC   driver_node_type,
# MAGIC   worker_node_type,
# MAGIC   autoscale_min_workers,
# MAGIC   autoscale_max_workers,
# MAGIC   create_time
# MAGIC FROM system.compute.clusters
# MAGIC WHERE delete_time IS NULL  -- アクティブなクラスターのみ
# MAGIC ORDER BY create_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Lakeflow パイプライン監視
# MAGIC
# MAGIC DLTパイプラインのイベントログを確認し、データ品質パイプラインの状態を把握します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DLTパイプラインのイベントログ（直近7日）
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   event_type,
# MAGIC   message,
# MAGIC   level
# MAGIC FROM system.lakeflow.pipeline_event_log
# MAGIC WHERE timestamp >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
# MAGIC   AND level IN ('INFO', 'WARN', 'ERROR')
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 運用ダッシュボード用クエリ
# MAGIC
# MAGIC 以下のクエリを AI/BI Dashboard に追加することで、
# MAGIC リスク計量パイプラインの **運用ダッシュボード** を構築できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI: 直近のパイプライン健全性
# MAGIC -- （この結果を Dashboard の KPI ウィジェットに表示）
# MAGIC SELECT
# MAGIC   'ジョブ成功率' AS metric,
# MAGIC   CONCAT(
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN r.result_state = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
# MAGIC     ), '%'
# MAGIC   ) AS value
# MAGIC FROM system.workflow.job_run_timeline r
# MAGIC WHERE r.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   '直近7日間の総DBU' AS metric,
# MAGIC   CAST(ROUND(SUM(usage_quantity), 0) AS STRING) AS value
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= DATEADD(DAY, -7, CURRENT_DATE())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. アラート設定の推奨
# MAGIC
# MAGIC Databricks SQL のアラート機能を使って、以下の条件でアラートを設定できます。
# MAGIC
# MAGIC ### 推奨アラート
# MAGIC | アラート名 | 条件 | 通知先 |
# MAGIC |---|---|---|
# MAGIC | VaR計算遅延 | ジョブ実行時間 > SLA閾値 | Slack / メール |
# MAGIC | ジョブ失敗 | result_state != 'SUCCESS' | Slack / PagerDuty |
# MAGIC | コスト異常 | 日次DBU > 前週平均の200% | メール |
# MAGIC | データ品質低下 | DLT expectations 違反率 > 5% | Slack |
# MAGIC
# MAGIC ### UI操作ポイント
# MAGIC > **アラートの設定手順**:
# MAGIC > 1. 左メニュー「SQL」→「アラート」→「アラートを作成」
# MAGIC > 2. SQLクエリを指定（上記のクエリを改変）
# MAGIC > 3. 閾値条件を設定（例: result > 120 分）
# MAGIC > 4. 通知先を設定（Slack Webhook, メール等）
# MAGIC > 5. スケジュールを設定（例: 15分ごとにチェック）

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **System Tables** で Databricks の利用状況を自動的に記録・分析
# MAGIC - **ジョブ実行履歴** でVaR計算パイプラインの成功率・実行時間を監視
# MAGIC - **DBU消費量** でコスト追跡とチーム別配賦
# MAGIC - **DLTパイプラインログ** でデータ品質パイプラインの健全性確認
# MAGIC - **アラート設定** で障害の早期検知
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 全体のまとめ
# MAGIC
# MAGIC このデモシリーズを通じて、以下の Databricks 機能を VaR リスク管理の文脈で学びました：
# MAGIC
# MAGIC | ノートブック | Databricks 機能 |
# MAGIC |---|---|
# MAGIC | 00: Introduction | ワークスペース概要、Serverless |
# MAGIC | 01: Data Upload | Unity Catalog Volume |
# MAGIC | 02: Auto Loader | cloudFiles、増分処理 |
# MAGIC | 03: Data Quality | Lakeflow SDP、Expectations |
# MAGIC | 04: Governance | リネージ、権限、タグ |
# MAGIC | 05: Features | Window関数、ASOF JOIN |
# MAGIC | 06: MLflow | Experiment、Model Registry |
# MAGIC | 07: Monte Carlo | Spark分散処理、Liquid Clustering |
# MAGIC | 08: Compliance | Spark ML、バーゼルバックテスト |
# MAGIC | 09: Dashboard | AI/BI Dashboard、Genie |
# MAGIC | 10: Operations | System Tables、コスト管理 |
