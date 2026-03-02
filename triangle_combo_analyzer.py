import pandas as pd
import numpy as np
from scipy.stats import linregress


# ── [1] 삼각수렴 ─────────────────────────────────
def analyze_triangle_convergence_pivot_v2(
    df: pd.DataFrame,
    window: int = 40,
    pivot_n: int = 2,
    r2_threshold: float = 0.7,
    convergence_threshold: float = 20
) -> dict | None:

    if len(df) < window + 20:
        return None

    vol_ma20 = df['Volume'].rolling(20).mean().iloc[-1]
    df = df.iloc[-window:].copy().reset_index(drop=True)

    # 피벗 탐지 (look-ahead bias 없는 방식) ✅
    piv_high, piv_low = [], []
    for i in range(pivot_n, len(df) - pivot_n):
        h_slice = df['High'].iloc[i - pivot_n: i + pivot_n + 1]
        l_slice = df['Low'].iloc[i  - pivot_n: i + pivot_n + 1]
        high, low = df['High'].iloc[i], df['Low'].iloc[i]
        if high == h_slice.max() and h_slice.tolist().count(high) == 1:
            piv_high.append((i, high))
        if low == l_slice.min() and l_slice.tolist().count(low) == 1:
            piv_low.append((i, low))

    if len(piv_high) < 2 or len(piv_low) < 2:
        return None

    high_confidence = len(piv_high) >= 3 and len(piv_low) >= 3

    xh = np.array([p[0] for p in piv_high])
    yh = np.array([p[1] for p in piv_high])
    xl = np.array([p[0] for p in piv_low])
    yl = np.array([p[1] for p in piv_low])

    slope_h, int_h, r_h, _, _ = linregress(xh, yh)
    slope_l, int_l, r_l, _, _ = linregress(xl, yl)

    # R² 검증 ✅
    if r_h**2 < r2_threshold or r_l**2 < r2_threshold:
        return None

    price_mean     = df['Close'].mean()
    slope_h_pct    = slope_h / price_mean * 100
    slope_l_pct    = slope_l / price_mean * 100

    # 수렴률
    x_start      = min(xh.min(), xl.min())
    start_width  = (int_h + slope_h * x_start) - (int_l + slope_l * x_start)
    end_upper    = int_h + slope_h * (window - 1)
    end_lower    = int_l + slope_l * (window - 1)
    end_width    = end_upper - end_lower

    if start_width <= 0:
        return None

    lines_crossed    = end_width < 0
    convergence_rate = max(min((1 - end_width / start_width) * 100, 100), 0)

    # 패턴 분류
    if slope_h_pct < -0.05 and slope_l_pct > 0.05:
        pattern = "Symmetrical"
    elif abs(slope_h_pct) <= 0.05 and slope_l_pct > 0.05:
        pattern = "Ascending"
    elif slope_h_pct < -0.05 and abs(slope_l_pct) <= 0.05:
        pattern = "Descending"
    else:
        pattern = "Unknown"

    # Apex
    denom = slope_h - slope_l
    bars_to_apex = (
        int((int_l - int_h) / denom - (window - 1))
        if abs(denom) > 1e-9 else None
    )

    upper_now  = end_upper
    upper_prev = int_h + slope_h * (window - 2)
    lower_now  = end_lower

    is_breakout_up = (
        df['Close'].iloc[-1] > upper_now  * 1.005 and
        df['Close'].iloc[-2] > upper_prev * 1.005 and
        df['Volume'].iloc[-1] > vol_ma20 * 1.5
    )
    is_breakout_down = (
        df['Close'].iloc[-1] < lower_now * 0.995 and
        df['Volume'].iloc[-1] > vol_ma20 * 1.5
    )

    return {
        'pattern':         pattern,
        'confidence':      'HIGH' if high_confidence else 'LOW',
        'convergence_pct': round(convergence_rate, 2),
        'lines_crossed':   lines_crossed,
        'bars_to_apex':    bars_to_apex,
        'is_triangle':     convergence_rate > convergence_threshold and pattern != "Unknown",
        'breakout_up':     is_breakout_up,
        'breakout_down':   is_breakout_down,
        'upper_line_now':  round(upper_now,  2),
        'lower_line_now':  round(lower_now,  2),
        'r2_upper':        round(r_h**2, 3),
        'r2_lower':        round(r_l**2, 3),
    }


# ── [2] Support DNA ──────────────────────────────
def analyze_support_dna(
    df: pd.DataFrame,
    target_ma: str = 'MA20',
    window: int = 120
) -> float:

    if target_ma not in df.columns:                # ✅ 컬럼 체크
        return 0.0

    subset = df.iloc[-window:].copy()
    touch_points = subset[
        abs(subset['Low'] - subset[target_ma]) / (subset[target_ma] + 1e-9) < 0.015
    ]
    if len(touch_points) == 0:
        return 0.0

    success = 0
    is_datetime = isinstance(df.index, pd.DatetimeIndex)  # ✅ 인덱스 타입 분기

    for idx in touch_points.index:
        if is_datetime:
            future = df.loc[idx: idx + pd.Timedelta(days=7)]
        else:
            loc    = df.index.get_loc(idx)
            future = df.iloc[loc: loc + 6]

        if len(future) > 1 and future['High'].max() > subset.loc[idx, target_ma] * 1.05:
            success += 1

    return success / len(touch_points)


# ── [3] 통합 엔진 ────────────────────────────────
def jongbe_triangle_combo_v3(df: pd.DataFrame) -> dict | None:

    if df is None or len(df) < 60:
        return {
            'date': 'N/A',
            'pass': False,
            'grade': 'C (❄️PASS)',
            'score': 0,
            'jongbe': False,
            'has_triangle': False,
            'ma20_dna': "0%",
            'triangle_pattern': 'None',
            'convergence_pct': 0,
            'apex_remain': None,
            'is_breakout': False,
            'lines_crossed': False,
            'triangle': {},
            'jongbe_detail': {}
        }

    df = df.copy()
    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA40'] = df['Close'].rolling(40).mean()
    df['MA20_slope'] = (df['MA20'] - df['MA20'].shift(5)) / (df['MA20'].shift(5) + 1e-9) * 100
    df['MA40_slope'] = (df['MA40'] - df['MA40'].shift(5)) / (df['MA40'].shift(5) + 1e-9) * 100

    # ✅ DMI 계산 추가
    df['plus_di']  = ta.trend.plus_di(df['High'], df['Low'], df['Close'], 14)
    df['minus_di'] = ta.trend.minus_di(df['High'], df['Low'], df['Close'], 14)
    df['adx']      = ta.trend.adx(df['High'], df['Low'], df['Close'], 14)

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    # ── 골든크로스
    cross_series = (
        (df['MA20'] > df['MA40']) &
        (df['MA20'].shift(1) <= df['MA40'].shift(1))
    )
    cross_recent = cross_series.iloc[-5:].any()
    gap_ratio    = abs(curr['MA20'] - curr['MA40']) / (curr['MA40'] + 1e-9)
    cross_near   = (
        curr['MA20'] > curr['MA40'] and
        gap_ratio < 0.03 and
        curr['MA20'] > df['MA20'].iloc[-3]
    )

    slope_5ago  = df['MA20_slope'].shift(5).iloc[-1]
    ma20_rising = curr['MA20_slope'] > 0
    ma40_rising = curr['MA40_slope'] > -0.05
    ma20_accel  = pd.notna(slope_5ago) and curr['MA20_slope'] > slope_5ago

    jongbe_ok = (
        (cross_recent or cross_near) and
        ma20_rising and ma40_rising and
        ma20_accel and
        curr['Close'] > curr['MA20']
    )

    # ── ✅ DMI 조건
    dmi_cross = curr['plus_di'] > curr['minus_di'] and prev['plus_di'] <= prev['minus_di']
    adx_ok    = curr['adx'] > 20 and curr['adx'] > prev['adx']
    dmi_ok    = dmi_cross and adx_ok

    # ── 삼각수렴 + DNA
    tri          = analyze_triangle_convergence_pivot_v2(df)
    has_triangle = tri is not None
    tri_safe     = tri or {}
    dna_score    = analyze_support_dna(df, 'MA20')

    # ── 점수 계산
    score = 0

    if jongbe_ok:
        score += 30

    if dmi_ok:                # ✅ DMI 가중치
        score += 10

    if has_triangle:
        if tri['is_triangle']:
            score += 20

        pattern_bonus = {
            'Symmetrical': 15,
            'Ascending':   10,
            'Descending':   5,
            'Unknown':      0
        }
        score += pattern_bonus.get(tri['pattern'], 0)

        if tri['confidence'] == 'HIGH':
            score += 5

        if tri['bars_to_apex'] is not None:
            if 0 <= tri['bars_to_apex'] <= 5:
                score += 10
            elif tri['bars_to_apex'] < 0:
                score -= 10

        if tri.get('lines_crossed'):
            score -= 15

        if tri['breakout_up']:
            score += 15
        if tri.get('breakout_down'):
            score -= 25

    if dna_score >= 0.7:
        score += 10

    score = max(min(score, 100), 0)

    # ── 등급
    if   score >= 85: grade = 'S (🏆LEGEND)'
    elif score >= 70: grade = 'A (🔥KING WATERMELON)'
    elif score >= 50: grade = 'B (👀WATCHING)'
    else:             grade = 'C (❄️PASS)'

    date_str = (
        str(df.index[-1].date())
        if isinstance(df.index, pd.DatetimeIndex) else 'N/A'
    )

    return {
        'date':             date_str,
        'pass':             score >= 70,
        'grade':            grade,
        'score':            score,
        'jongbe':           jongbe_ok,
        'has_triangle':     has_triangle,
        'ma20_dna':         f"{round(dna_score * 100)}%",
        'triangle_pattern': tri_safe.get('pattern', 'None'),
        'convergence_pct':  tri_safe.get('convergence_pct', 0),
        'apex_remain':      tri_safe.get('bars_to_apex', None),
        'is_breakout':      tri_safe.get('breakout_up', False),
        'lines_crossed':    tri_safe.get('lines_crossed', False),
        'triangle':         tri_safe,
        'jongbe_detail': {
            'cross_recent': bool(cross_recent),
            'cross_near':   cross_near,
            'ma20_rising':  ma20_rising,
            'ma40_rising':  ma40_rising,
            'ma20_accel':   ma20_accel,
            'dmi_cross':    bool(dmi_cross),   # ✅ 추가
            'adx_ok':       bool(adx_ok),      # ✅ 추가
            'dmi_ok':       bool(dmi_ok),      # ✅ 추가
        }
    }
