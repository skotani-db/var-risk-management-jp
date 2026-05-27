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
# MAGIC - **Serverless バージョン**: v5（ノートブック右側「設定」→「基本環境」で選択）
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
# MAGIC ### Step 2: 調整用 Volume に Excel をアップロード
# MAGIC
# MAGIC まず、手元の Excel を **専用の Volume (`risk_adjustments`)** にアップロードします。
# MAGIC 市場データ用の `raw_data` Volume とは別にしているのがポイントです。
# MAGIC
# MAGIC 1. 左メニュー「**カタログ**」→ カタログ → スキーマ `var_risk_demo` → 「**Volumes**」タブ
# MAGIC 2. `risk_adjustments` Volume をクリック
# MAGIC 3. 「**このボリュームにアップロード**」ボタンをクリック
# MAGIC 4. 手元の `risk_adjustment_q2_2026.xlsx` を **ドラッグ＆ドロップ**
# MAGIC
# MAGIC > **Volume を分ける理由（リネージのポイント）**:
# MAGIC > - `raw_data` Volume → 市場データ（株価 CSV 等）のソース
# MAGIC > - `risk_adjustments` Volume → リスク調整 Excel のソース
# MAGIC >
# MAGIC > Unity Catalog のリネージグラフでは **Volume 単位で依存関係が表示** されるため、
# MAGIC > 「レポートのリスクリミットはどの Volume（=どの業務プロセス）から来たのか？」が一目瞭然になります。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2b: ソース演算子で Volume 上の Excel を参照
# MAGIC
# MAGIC 1. キャンバス上の「**＋**」ボタン → 「**ソース**」を選択
# MAGIC 2. 「**ファイルをアップロード**」を選択
# MAGIC 3. アップロード先として `risk_adjustments` Volume を指定し、先ほどアップロードした Excel を選択
# MAGIC
# MAGIC シートごとにソース演算子が必要なため、以下の3つを作成します。
# MAGIC **シート選択**: ソース演算子の設定ペインで「**シートを選択**」欄に Excel のシート名を入力してください。
# MAGIC
# MAGIC | ソース演算子名 | 取り込むシート（`risk_adjustment_q2_2026.xlsx` のシート名） | 説明 |
# MAGIC |---|---|---|
# MAGIC | `weight_adjustments` | `ウェイト調整` | 銘柄ごとの新ポートフォリオ比率 |
# MAGIC | `risk_limits` | `リスクリミット` | 国別・業種別の VaR99 上限値 |
# MAGIC | `stress_scenarios` | `ストレスシナリオ` | 極端イベントの想定損失率 |
# MAGIC
# MAGIC **演算子の名前変更**: ソース演算子をダブルクリック → 設定ペイン上部のテキストフィールドで名前を編集
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
# MAGIC これから構築するパイプラインの全体像です。この DAG を左から右へ順に作っていきます：
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────┐  ┌──────────────────────┐  ┌─────────────────────┐
# MAGIC │  weight_adjustments  │  │  monte_carlo_trials  │  │    risk_limits       │
# MAGIC │  (Excel D&D)         │  │  (既存テーブル参照)   │  │  (Excel D&D)         │
# MAGIC └────────┬─────────────┘  └──────────┬───────────┘  └──────────┬──────────┘
# MAGIC          │                           │                          │
# MAGIC          └──────────┐   ┌────────────┘                          │
# MAGIC                     ▼   ▼                                       │
# MAGIC              ┌──────────────────┐                               │
# MAGIC              │ 4-1 結合(ticker) │                               │
# MAGIC              └────────┬─────────┘                               │
# MAGIC                       ▼                                         │
# MAGIC              ┌──────────────────┐                               │
# MAGIC              │ 4-2 変換         │                               │
# MAGIC              │ 加重リターン計算  │                               │
# MAGIC              └────────┬─────────┘                               │
# MAGIC                       ▼                                         │
# MAGIC              ┌──────────────────┐                               │
# MAGIC              │ 4-3 集計         │                               │
# MAGIC              │ (country)        │                               │
# MAGIC              └────────┬─────────┘                               │
# MAGIC                       │                                         │
# MAGIC                       └─────────────────┐   ┌──────────────────┘
# MAGIC                                         ▼   ▼
# MAGIC                                  ┌──────────────────┐
# MAGIC                                  │ 4-4 結合         │
# MAGIC                                  │ (country=target) │
# MAGIC                                  └────────┬─────────┘
# MAGIC                                           ▼
# MAGIC                                  ┌──────────────────┐
# MAGIC                                  │ 4-5 変換         │
# MAGIC                                  │ BREACH/OK 判定   │
# MAGIC                                  └────────┬─────────┘
# MAGIC                                           ▼
# MAGIC                                  ┌────────────────────────┐
# MAGIC                                  │  出力                  │
# MAGIC                                  │  risk_compliance_report│
# MAGIC                                  └────────────────────────┘
# MAGIC ```
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
# MAGIC > **確認**: スケジュールを設定すると、左メニュー「**ジョブとパイプライン**」にジョブとして表示されます。
# MAGIC > ここから実行履歴の確認や手動実行もできます。
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Genie Code にプロンプトを投げてパイプラインを生成する
# MAGIC
# MAGIC Step 2 でソース演算子を追加したら、あとは **Genie Code に1つのプロンプトを投げるだけ** で
# MAGIC パイプライン全体を自動生成できます。
# MAGIC
# MAGIC キャンバス上部の Genie Code アイコンをクリックし、以下のプロンプトをコピー＆ペーストしてください：
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **コンプライアンスレポート用プロンプト（そのままコピーして使えます）**:
# MAGIC
# MAGIC ```
# MAGIC 以下のパイプラインを構築してください:
# MAGIC
# MAGIC 1. monte_carlo_trials と weight_adjustments を ticker で Inner Join
# MAGIC 2. 結合結果に adjusted_weighted_return = returns * (new_weight_pct / 100) 列を追加
# MAGIC 3. country でグループ化して adjusted_weighted_return の合計を portfolio_return として集計
# MAGIC 4. 集計結果と risk_limits を country = target で Left Join
# MAGIC 5. status 列を追加: var_99 >= limit_value なら 'OK'、それ以外は 'BREACH'
# MAGIC 6. 出力テーブル名: risk_compliance_report
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **ストレステスト用プロンプト**（別のビジュアルデータ準備で使用）:
# MAGIC
# MAGIC ```
# MAGIC risk_compliance_report と stress_scenarios を使って以下を計算してください:
# MAGIC
# MAGIC 1. stress_scenarios の target_country が 'ALL' の場合は全国の var_99 の平均を使用、
# MAGIC    それ以外は対象国の var_99 を使用
# MAGIC 2. stressed_var = 対象 var_99 * volatility_multiplier + (price_shock_pct / 100)
# MAGIC 3. additional_loss = stressed_var - 通常の var_99
# MAGIC 4. 出力テーブル名: stress_test_report
# MAGIC ```
# MAGIC
# MAGIC > **ポイント**: Genie Code は画像アップロードにも対応しています。
# MAGIC > 既存の Excel レポートのスクリーンショットを貼り付けて
# MAGIC > 「このレポートと同じ形式になるよう変換して」と指示することもできます。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. AI/BI Dashboard でレポートを可視化する
# MAGIC
# MAGIC Lakeflow Designer が出力した `risk_compliance_report` と `stress_test_report` テーブルを
# MAGIC **AI/BI Dashboard（Lakeview）** で可視化します。
# MAGIC
# MAGIC Dashboard Agent に以下のプロンプトを投げるだけでダッシュボードが自動生成されます。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: AI/BI Dashboard を新規作成
# MAGIC 1. 左サイドバー「**＋ 新規**」→「**ダッシュボード**」を選択
# MAGIC 2. 空のダッシュボードが作成されます
# MAGIC
# MAGIC ### Step 2: Dashboard Agent にプロンプトを投げる
# MAGIC
# MAGIC ダッシュボード上部の **AI アイコン（✨）** をクリックし、以下のプロンプトをコピー＆ペーストしてください：
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **ダッシュボード生成プロンプト（そのままコピーして使えます）**:
# MAGIC
# MAGIC ```
# MAGIC 以下のテーブルを使ってリスク調整コンプライアンスダッシュボードを作成してください:
# MAGIC
# MAGIC データソース:
# MAGIC - risk_compliance_report: country, var_99, limit_value, status, buffer, approver 列を含む
# MAGIC - stress_test_report: scenario, target, stressed_var, normal_var, additional_loss, probability 列を含む
# MAGIC
# MAGIC 作成してほしいウィジェット:
# MAGIC 1. ヘッダー: 「Q2 2026 リスク調整コンプライアンスレポート」
# MAGIC 2. KPI カード: BREACH の件数、OK の件数
# MAGIC 3. 棒グラフ: 国別の var_99 と limit_value を並べて表示、BREACH は赤、OK は青
# MAGIC 4. テーブル: risk_compliance_report の全列を表示、status が BREACH の行を強調
# MAGIC 5. 横棒グラフ: ストレスシナリオ別の normal_var と stressed_var を比較
# MAGIC 6. テーブル: stress_test_report の全列を表示
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: Dashboard Agent が自動でダッシュボードを構築
# MAGIC
# MAGIC Agent がプロンプトに基づいて以下を自動生成します：
# MAGIC - **データセット（SQL クエリ）** が「データ」タブに追加
# MAGIC - **ウィジェット（チャート、テーブル、KPI）** が「キャンバス」タブに配置
# MAGIC
# MAGIC 生成されたダッシュボードは、そのまま **公開** して経営層やリスク委員会と共有できます。
# MAGIC
# MAGIC ### ダッシュボードの活用
# MAGIC | 機能 | 説明 |
# MAGIC |---|---|
# MAGIC | **フィルター追加** | 国、業種、承認者でインタラクティブに絞り込み |
# MAGIC | **スケジュール配信** | メールで定期レポートを自動送信 |
# MAGIC | **PDF エクスポート** | 規制当局への提出資料として活用 |
# MAGIC | **Genie スペース連携** | ダッシュボードのテーブルを Genie に接続し、自然言語でアドホック分析 |
# MAGIC
# MAGIC > **PoC のデモポイント**: Dashboard Agent がプロンプト1つでレポートを自動生成する様子を見せることで、
# MAGIC > 「Excel → Designer → Dashboard の全フローがノーコード」であることを実演できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. リネージの確認（Excel でもデータの来歴が追跡可能）
# MAGIC
# MAGIC Excel からアップロードしたデータでも、Volume 経由で Lakeflow Designer に取り込めば
# MAGIC **Unity Catalog のリネージ（データの来歴）が自動的に記録** されます。
# MAGIC
# MAGIC ### 確認方法
# MAGIC 1. 左メニュー「**カタログ**」→ `risk_compliance_report` テーブルを開く
# MAGIC 2. 「**リネージ**」タブをクリック
# MAGIC 3. 以下のようなデータフローが可視化されます：
# MAGIC
# MAGIC ```
# MAGIC [Volume: risk_adjustments]          [Volume: raw_data]
# MAGIC  (Excel アップロード先)               (市場データ CSV)
# MAGIC     │                                    │
# MAGIC     ├── weight_adjustments               │
# MAGIC     ├── risk_limits                      ├── market_data
# MAGIC     └── stress_scenarios                 └── ...
# MAGIC          │                                    │
# MAGIC          │              ┌─────────────────────┘
# MAGIC          │              │  (08 で計算済み)
# MAGIC          │              ▼
# MAGIC          │        monte_carlo_trials
# MAGIC          │              │
# MAGIC          └──────┬───────┘
# MAGIC                 ▼
# MAGIC      risk_compliance_report
# MAGIC                 │
# MAGIC                 ▼
# MAGIC        stress_test_report
# MAGIC ```
# MAGIC
# MAGIC ### Volume を分けたことで見えるもの
# MAGIC - **`risk_adjustments` Volume**: リスクマネージャーが Excel で定義した調整データの起点
# MAGIC - **`raw_data` Volume**: 市場データパイプラインの起点
# MAGIC - リネージグラフを見れば、**レポートが2つの独立した業務プロセス**（市場データ取込 + リスク調整）から
# MAGIC   生成されていることが一目瞭然
# MAGIC
# MAGIC ### 規制対応での価値
# MAGIC - **監査証跡**: 「このレポートのリスクリミットはどの Volume のどの Excel から来たのか？」に即座に回答可能
# MAGIC - **影響分析**: リミット定義の Excel を差し替えた場合、どのレポートに影響するかを事前に把握
# MAGIC - **業務プロセスの分離**: Volume を分けることで、市場データの問題と調整データの問題を切り分けて調査可能
# MAGIC - **Excel でも安心**: 手元のファイルからの取り込みでも、テーブル間の依存関係が Unity Catalog に自動記録される
# MAGIC
# MAGIC > **PoC のデモポイント**: カタログ UI でリネージグラフを見せることで、
# MAGIC > 「Excel 運用でもガバナンスが効く」「Volume を分ければ業務プロセスごとにデータの出自を追跡できる」
# MAGIC > ことを視覚的に示せます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow Designer のメリット
# MAGIC
# MAGIC | 観点 | 従来のコードベース ETL | Lakeflow Designer |
# MAGIC |---|---|---|
# MAGIC | 構築者 | エンジニアが PySpark / SQL を記述 | アナリスト・非エンジニアでも GUI で構築可能 |
# MAGIC | データソース追加 | コード変更＋デプロイが必要 | Volume に Excel をアップロードするだけ |
# MAGIC | 変換ロジックの作成 | コーディング | 組み込み演算子を選択、または Genie Code に自然言語で指示 |
# MAGIC | パイプラインの可視化 | コードを読む必要がある | DAG が自動表示、自動レイアウト |
# MAGIC | 中間結果の確認 | `display()` でセルごとに実行 | 出力ペインでリアルタイムプレビュー＋データプロファイリング |
# MAGIC | スケジュール実行 | ジョブ設定が必要 | 「スケジュール」ボタンまたは「ジョブに追加」 |
# MAGIC | Excel 更新時の再実行 | ファイルの再アップロード＋コード再実行 | Excel を Volume に差し替えて「実行」 |
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
# MAGIC    - パイプラインを再実行し、ダッシュボードで新シナリオの影響を確認
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
# MAGIC - **Volume に Excel をアップロード** し、Lakeflow Designer のデータソースとして利用
# MAGIC - **Lakeflow Designer** のビジュアルキャンバスで結合・集計・判定パイプラインを構築
# MAGIC - **Genie Code** にプロンプトを投げてパイプラインロジックを自動生成
# MAGIC - **AI/BI Dashboard Agent** にプロンプトを投げてレポートを自動生成
# MAGIC - **リネージ** で Excel からレポートまでのデータの来歴を追跡
# MAGIC
# MAGIC ### エンドツーエンドのノーコードフロー
# MAGIC ```
# MAGIC Excel 作成 → Volume アップロード → Lakeflow Designer → AI/BI Dashboard
# MAGIC  (手元PC)      (カタログUI)         (Genie Code)        (Dashboard Agent)
# MAGIC ```
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - Lakeflow Designer パイプラインをジョブとしてスケジュール実行（日次レポート自動化）
# MAGIC - ダッシュボードのスケジュール配信を設定し、リスク委員会にメールで定期レポート
# MAGIC - Excel を更新して Volume に再アップロードするだけで、パイプライン→ダッシュボードまで自動更新
