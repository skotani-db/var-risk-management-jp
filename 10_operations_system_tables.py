# Databricks notebook source
# MAGIC %md
# MAGIC # 10. 運用監視と System Tables によるコスト管理
# MAGIC
# MAGIC
# MAGIC ### 前提条件
# MAGIC > 特になし（System Tables はワークスペース全体のメタデータです）。
# MAGIC > ただし、権限によっては一部のクエリがスキップされます。
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック右側「設定」→「基本環境」で選択）
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
# MAGIC
# MAGIC ## 注意事項
# MAGIC > System Tables は **ワークスペースの権限設定やリージョン** によって
# MAGIC > 利用できるテーブルが異なります。このノートブックでは、
# MAGIC > テーブルが存在しない場合はスキップしてエラーにならないようにしています。

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# System Tables のクエリを安全に実行するヘルパー関数
def safe_sql(query, description=""):
    """
    System Tables が存在しない場合はスキップ。
    Spark Connect の gRPC エラーログを避けるため、事前にテーブル存在を確認する。
    """
    import re
    # SQL から FROM 句のテーブル名を抽出
    match = re.search(r'FROM\s+([\w.]+)', query, re.IGNORECASE)
    if match:
        table_name = match.group(1)
        # SHOW コマンドや特殊クエリはチェック不要
        if not table_name.startswith('system'):
            pass  # system 以外はそのまま実行
        else:
            try:
                if not spark.catalog.tableExists(table_name):
                    print(f"[SKIP] {description}")
                    print(f"  → このワークスペースでは {table_name} が利用できません")
                    return
            except Exception as check_err:
                # tableExists 自体が権限不足でエラーになる場合
                print(f"[SKIP] {description}")
                print(f"  → {table_name} へのアクセス権限がありません: {str(check_err)[:200]}")
                return

    try:
        result = spark.sql(query)
        if description:
            print(f"[OK] {description}")
        display(result)
    except Exception as e:
        error_msg = str(e)
        if "INSUFFICIENT_PERMISSIONS" in error_msg:
            print(f"[SKIP] {description}")
            print(f"  → 権限不足です。管理者にアクセス権限を依頼してください")
            print(f"  → 詳細: {error_msg[:200]}")
        else:
            raise

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
# MAGIC | `system.lakeflow.pipeline_event_log` | DLTイベントログ | データ品質パイプラインの監視 |
# MAGIC | `system.access.audit` | 監査ログ | データアクセスの追跡 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 利用可能な System Tables の確認
# MAGIC
# MAGIC まず、このワークスペースでどの System Tables が利用可能か確認します。

# COMMAND ----------

safe_sql(
    "SHOW SCHEMAS IN system",
    "system カタログ内のスキーマ一覧"
)

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

safe_sql("""
    SELECT
      usage_date,
      sku_name,
      ROUND(SUM(usage_quantity), 2) AS total_dbus
    FROM system.billing.usage
    WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
    GROUP BY usage_date, sku_name
    ORDER BY usage_date DESC, total_dbus DESC
    LIMIT 30
""", "日次DBU消費量（直近30日）")

# COMMAND ----------

safe_sql("""
    SELECT
      DATE_TRUNC('month', usage_date) AS month,
      sku_name,
      ROUND(SUM(usage_quantity), 2) AS total_dbus
    FROM system.billing.usage
    WHERE usage_date >= DATEADD(MONTH, -6, CURRENT_DATE())
    GROUP BY DATE_TRUNC('month', usage_date), sku_name
    ORDER BY month, total_dbus DESC
""", "SKU別の月次コスト推移")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. クラスター利用状況
# MAGIC
# MAGIC Serverless 環境ではクラスター管理は不要ですが、
# MAGIC 既存のクラスターを使用している場合は利用効率を確認できます。

# COMMAND ----------

safe_sql("""
    SELECT
      cluster_name,
      cluster_id,
      driver_node_type,
      worker_node_type,
      min_autoscale_workers,
      max_autoscale_workers,
      create_time
    FROM system.compute.clusters
    WHERE delete_time IS NULL
    ORDER BY create_time DESC
    LIMIT 20
""", "アクティブなクラスター一覧")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Lakeflow パイプライン監視
# MAGIC
# MAGIC DLTパイプラインのイベントログを確認し、データ品質パイプラインの状態を把握します。

# COMMAND ----------

safe_sql("""
    SELECT
      timestamp,
      event_type,
      message,
      level
    FROM system.lakeflow.pipeline_event_log
    WHERE timestamp >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
      AND level IN ('INFO', 'WARN', 'ERROR')
    ORDER BY timestamp DESC
    LIMIT 50
""", "DLTパイプラインイベント（直近7日）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 運用ダッシュボード用クエリ
# MAGIC
# MAGIC 以下のクエリを AI/BI Dashboard に追加することで、
# MAGIC リスク計量パイプラインの **運用ダッシュボード** を構築できます。

# COMMAND ----------

safe_sql("""
    SELECT
      DATE_TRUNC('week', usage_date) AS week,
      ROUND(SUM(usage_quantity), 2) AS total_dbus
    FROM system.billing.usage
    WHERE usage_date >= DATEADD(MONTH, -3, CURRENT_DATE())
    GROUP BY DATE_TRUNC('week', usage_date)
    ORDER BY week
""", "週次DBU消費トレンド（運用ダッシュボード用）")

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
# MAGIC > 3. 閾値条件を設定（例: total_dbus > 1000）
# MAGIC > 4. 通知先を設定（Slack Webhook, メール等）
# MAGIC > 5. スケジュールを設定（例: 15分ごとにチェック）

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました：
# MAGIC - **System Tables** で Databricks の利用状況を自動的に記録・分析
# MAGIC - **DBU消費量** でコスト追跡とチーム別配賦
# MAGIC - **クラスター監視** でリソース効率の確認
# MAGIC - **DLTパイプラインログ** でデータ品質パイプラインの健全性確認
# MAGIC - **アラート設定** で障害の早期検知
# MAGIC
# MAGIC 次のノートブック `11_lakeflow_designer_risk_report` では、
# MAGIC **Lakeflow Designer** を使って Excel のリスク調整データを取り込み、
# MAGIC コンプライアンスレポートを自動生成する方法を学びます。
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
# MAGIC | 04: Governance | リネージ、権限、タグ、タイムトラベル |
# MAGIC | 05: Features | Window関数、時点結合 |
# MAGIC | 06: MLflow | Experiment、Model Registry、リネージ |
# MAGIC | 07: Monte Carlo | Spark分散処理、Liquid Clustering |
# MAGIC | 08: Compliance | Spark ML、バーゼルバックテスト |
# MAGIC | 09: Dashboard | AI/BI Dashboard、Genie精度改善 |
# MAGIC | 10: Operations | System Tables、コスト管理 |
# MAGIC | 11: Lakeflow Designer | Lakeflow Designer、AI/BI Dashboard |
