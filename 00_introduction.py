# Databricks notebook source
# MAGIC %md
# MAGIC # VaR（バリュー・アット・リスク）リスク管理 on Databricks
# MAGIC
# MAGIC ## 実行環境の設定
# MAGIC - **コンピュート**: Serverless を選択（ノートブック右上「接続」→「Serverless」）
# MAGIC - **Serverless バージョン**: v5（ノートブック上部「Configuration」→「Serverless version」で設定）
# MAGIC - **追加ライブラリ**: 不要（全て Serverless Runtime にプリインストール済み）
# MAGIC
# MAGIC ## このデモについて
# MAGIC 従来のオンプレミスインフラに依存する銀行は、もはやリスクを効果的に管理することができません。
# MAGIC 銀行はレガシー技術の計算上の非効率性を捨て、市場や経済のボラティリティに迅速に対応できる
# MAGIC アジャイルなモダンリスク管理体制を構築する必要があります。
# MAGIC
# MAGIC 本デモでは、**バリュー・アット・リスク（VaR）** のユースケースを通じて、
# MAGIC Databricks の主要機能を体系的に学びます。
# MAGIC
# MAGIC ## ノートブック構成
# MAGIC | # | ノートブック | 学べる Databricks 機能 | リスク管理での活用 |
# MAGIC |---|---|---|---|
# MAGIC | 00 | Introduction（本ノートブック） | ワークスペース概要 | VaR入門 |
# MAGIC | 01 | Data Upload & Volume | Unity Catalog Volume | データの持ち込み |
# MAGIC | 02 | Auto Loader Ingestion | Auto Loader, 増分処理 | 市場データの継続取込 |
# MAGIC | 03 | Data Quality (Lakeflow) | Lakeflow SDP, Expectations | 異常値検出・品質管理 |
# MAGIC | 04 | Unity Catalog Governance | リネージ, 権限, タグ | 規制対応ガバナンス |
# MAGIC | 05 | Feature Engineering | Window関数, ASOF JOIN | ボラティリティ計算 |
# MAGIC | 06 | Model Training & MLflow | MLflow, Model Registry | モデルの追跡・登録 |
# MAGIC | 07 | Monte Carlo Simulation | Spark分散処理, Liquid Clustering | 大規模シミュレーション |
# MAGIC | 08 | VaR Aggregation & Compliance | Spark ML, バックテスト | バーゼル規制対応 |
# MAGIC | 09 | Dashboard & Genie | AI/BI Dashboard, Genie | リスクレポーティング |
# MAGIC | 10 | Operations & System Tables | System Tables, ジョブ監視 | 運用・コスト管理 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## VaR 入門
# MAGIC
# MAGIC VaR（バリュー・アット・リスク）は、特定の信頼区間における潜在的損失の指標です。
# MAGIC VaR統計量は3つの要素で構成されます：**期間**、**信頼水準**、**損失額（または損失率）**。
# MAGIC
# MAGIC > 「来月中に95%または99%の信頼水準で、最大いくらの損失が見込まれるか？」
# MAGIC
# MAGIC VaRの計算方法には3つのアプローチがあります：
# MAGIC
# MAGIC + **ヒストリカル法**: 実際の過去のリターンを最悪から最良の順に並べ替えるシンプルな手法
# MAGIC + **分散共分散法**: 株式リターンが正規分布に従うと仮定し、確率密度関数を使用
# MAGIC + **モンテカルロシミュレーション**: 将来の株価リターンのモデルを構築し、複数の仮想シナリオを実行
# MAGIC
# MAGIC ### なぜ Databricks でリスク管理か？
# MAGIC - **スケーラビリティ**: モンテカルロシミュレーションを数万〜数百万試行に並列化
# MAGIC - **ガバナンス**: Unity Catalog でデータ・モデル・リネージを一元管理し、規制要件に対応
# MAGIC - **再現性**: MLflow でモデルのバージョン管理・パラメータ追跡を実現
# MAGIC - **リアルタイム性**: Auto Loader + Lakeflow で市場データを継続的に取り込み
# MAGIC - **自然言語分析**: Genie でリスクアナリストが SQL を書かずにアドホック分析

# COMMAND ----------

# MAGIC %md
# MAGIC ## VaR のシンプルな例
# MAGIC 以下では、合成商品に対するシンプルなVaR計算を示します。
# MAGIC ボラティリティ（商品リターンの標準偏差）と時間軸（300日）が与えられています。
# MAGIC
# MAGIC **95%の信頼水準で、300日間に最大いくら損失する可能性があるか？**

# COMMAND ----------

# 時間軸
days = 300

# ボラティリティ
sigma = 0.04

# ドリフト（平均成長率）
mu = 0.05

# 初期価格
start_price = 10

# COMMAND ----------

import matplotlib.pyplot as plt
from utils.var_utils import generate_prices

plt.figure(figsize=(16,6))
for i in range(1, 500):
    plt.plot(generate_prices(start_price, mu, sigma, days))

plt.title('シミュレーション価格')
plt.xlabel("時間")
plt.ylabel("価格")
plt.show()

# COMMAND ----------

from utils.var_viz import plot_var
simulations = [generate_prices(start_price, mu, sigma, days)[-1] for i in range(10000)]
plot_var(simulations, 99)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 期待ショートフォール
# MAGIC 期待ショートフォールは、VaRよりもトレーダーに対してより良いインセンティブを生み出す指標です。
# MAGIC 条件付きVaRまたはテールロスとも呼ばれます。
# MAGIC
# MAGIC - **VaR**: 「最悪の場合どの程度悪くなりうるか？」
# MAGIC - **期待ショートフォール**: 「悪い事態が発生した場合、予想される損失はいくらか？」

# COMMAND ----------

from utils.var_utils import get_var, get_shortfall

print('VaR99: {}'.format(round(get_var(simulations, 99), 2)))
print('期待ショートフォール: {}'.format(round(get_shortfall(simulations, 99), 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 次のステップ
# MAGIC 次のノートブック `01_data_upload_and_volume` では、実際のポートフォリオデータを
# MAGIC Databricks に持ち込む方法を学びます。Unity Catalog の Volume 機能を使って、
# MAGIC CSV ファイルをアップロードする体験から始めましょう。
