def plot_candlesticks(stock_df):
  """ローソク足チャートを描画"""
  import plotly.graph_objects as go
  layout = go.Layout(
    autosize=False,
    width=1200,
    height=800,
  )
  fig = go.Figure(
    data=[go.Candlestick(
      x=stock_df['date'],
      open=stock_df['open'],
      high=stock_df['high'],
      low=stock_df['low'],
      close=stock_df['close']
    )],
    layout=layout
  )
  display(fig)


def plot_var(simulations, var):
  """VaR分布のヒストグラムと正規分布のオーバーレイを描画（scipy不要）"""

  import pandas as pd
  import numpy as np
  import matplotlib.pyplot as plt
  from utils.var_utils import get_var, norm_pdf

  mean = np.mean(simulations)
  m1 = np.min(simulations)
  m2 = np.max(simulations)
  std = np.std(simulations)
  q1 = get_var(simulations, var)

  x1 = np.arange(m1, m2, 0.001)
  y1 = norm_pdf(x1, mean, std)
  x2 = np.arange(m1, q1, 0.001)
  y2 = norm_pdf(x2, mean, std)

  mc_df = pd.DataFrame(data=simulations, columns=['return'])
  ax = mc_df.hist(column='return', bins=50, density=True, grid=False, figsize=(12,8), color='#86bf91', zorder=2, rwidth=0.9)
  ax = ax[0]

  for x in ax:
      x.spines['right'].set_visible(False)
      x.spines['top'].set_visible(False)
      x.spines['left'].set_visible(False)
      x.axvline(x=q1, color='r', linestyle='dashed', linewidth=1)
      x.fill_between(x2, y2, zorder=3, alpha=0.4)
      x.plot(x1, y1, zorder=3)
      x.tick_params(axis="both", which="both", bottom="off", top="off", labelbottom="on", left="off", right="off", labelleft="on")
      vals = x.get_yticks()
      for tick in vals:
          x.axhline(y=tick, linestyle='dashed', alpha=0.4, color='#eeeeee', zorder=1)

      x.set_title("VAR{} = {:.3f}".format(var, q1), weight='bold', size=15)
      x.set_xlabel("リターン", labelpad=20, weight='bold', size=12)
      x.set_ylabel("密度", labelpad=20, weight='bold', size=12)


def plot_correlation_heatmap(corr_matrix, labels):
  """相関行列のヒートマップを描画（seaborn不要）"""
  import numpy as np
  import matplotlib.pyplot as plt

  fig, ax = plt.subplots(figsize=(11, 8))
  im = ax.imshow(corr_matrix, cmap='coolwarm', vmin=-1, vmax=1, aspect='auto')

  ax.set_xticks(np.arange(len(labels)))
  ax.set_yticks(np.arange(len(labels)))
  ax.set_xticklabels(labels, rotation=45, ha='right')
  ax.set_yticklabels(labels)

  # 各セルに値を表示
  for i in range(len(labels)):
      for j in range(len(labels)):
          ax.text(j, i, f'{corr_matrix.iloc[i, j]:.2f}',
                  ha='center', va='center', color='black', fontsize=10)

  fig.colorbar(im, ax=ax)
  plt.tight_layout()
  return fig, ax
