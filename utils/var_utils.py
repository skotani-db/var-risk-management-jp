def generate_prices(start_price, mu, sigma, days):
    """幾何ブラウン運動による価格シミュレーション"""
    import numpy as np
    shock = np.zeros(days)
    price = np.zeros(days)
    sample_rate = 1 / float(days)
    price[0] = start_price
    for i in range(1, days):
        shock[i] = np.random.normal(loc=mu * sample_rate, scale=sigma * np.sqrt(sample_rate))
        price[i] = max(0, price[i - 1] + shock[i] * price[i - 1])
    return price


def create_seed_df(runs):
    """モンテカルロ試行用のシードデータフレームを作成"""
    import pandas as pd
    import numpy as np
    return pd.DataFrame(list(np.arange(0, runs)), columns=['trial_id'])


def get_shortfall(simulations, var):
    """期待ショートフォール（条件付きVaR）を計算"""
    import numpy as np
    var = get_var(simulations, var)
    return float(np.mean([s for s in simulations if s <= var]))


def get_var(simulations, var):
    """バリュー・アット・リスク（VaR）をパーセンタイルで計算"""
    import numpy as np
    return float(np.percentile(simulations, 100 - var))


def non_linear_features(xs):
    """非線形特徴量を生成（x, x^2, x^3, sqrt(|x|)）"""
    import numpy as np
    fs = []
    for x in xs:
        fs.append(x)
        fs.append(np.sign(x) * x ** 2)
        fs.append(x ** 3)
        fs.append(np.sign(x) * np.sqrt(abs(x)))
    return fs


def predict_non_linears(ps, fs):
    """非線形特徴量と重みから予測値を計算"""
    s = ps[0]
    for i, f in enumerate(fs):
        s = s + ps[i + 1] * f
    return float(s)


def norm_pdf(x, mean, std):
    """正規分布の確率密度関数（scipy不要）"""
    import numpy as np
    return (1.0 / (std * np.sqrt(2 * np.pi))) * np.exp(-0.5 * ((x - mean) / std) ** 2)


def norm_ppf(p):
    """正規分布の逆累積分布関数の近似（scipy不要）
    Abramowitz and Stegun の近似式を使用"""
    import numpy as np
    # Peter Acklam の近似式
    a = [-3.969683028665376e+01, 2.209460984245205e+02,
         -2.759285104469687e+02, 1.383577518672690e+02,
         -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02,
         -1.556989798598866e+02, 6.680131188771972e+01,
         -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01,
         -2.400758277161838e+00, -2.549732539343734e+00,
          4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01,
         2.445134137142996e+00, 3.754408661907416e+00]

    p_low = 0.02425
    p_high = 1 - p_low

    if p < p_low:
        q = np.sqrt(-2 * np.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    elif p <= p_high:
        q = p - 0.5
        r = q * q
        return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
               (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
    else:
        q = np.sqrt(-2 * np.log(1 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
                ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
