# -*- coding: utf-8 -*-
"""
단테 3박자 V4 모듈
- 기간대칭 + 파동에너지 + 수박 상태머신
- 기존 main7_bugfix_2.py 에 붙여 넣거나 import 해서 사용 가능

사용법 예시:
    import pandas as pd
    from dante_3phase_v4_module import apply_dante_v4

    df = ...  # OHLCV + MA/BB/OBV/MFI/ADX/MACD_Hist 등이 포함된 DataFrame
    df = apply_dante_v4(df)
    row = df.iloc[-1]
    print(row['DANTE_FINAL_FIRE'], row['DANTE_3PHASE_SCORE'])

권장 입력 컬럼:
    Open, High, Low, Close, Volume
선택 입력 컬럼(없으면 일부 내부 계산):
    MA5, MA20, MA40, MA60, BB40_Width, Disparity,
    OBV, OBV_MA10, OBV_Slope, MFI, VMA20,
    ADX, pDI, mDI, MACD_Hist
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

DANTE_V4_VERSION = 'v4.3-one-module-pre-dolbanji'

# ============================================================
# 공용 유틸
# ============================================================

def safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.replace(',', '').strip()
            if not x:
                return default
        v = float(x)
        if np.isnan(v):
            return default
        return v
    except Exception:
        return default


def safe_int(x, default: int = 0) -> int:
    try:
        return int(round(safe_float(x, default)))
    except Exception:
        return default


# ============================================================
# 보조 계산
# ============================================================

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_gain = up.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = down.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)


def mfi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    tp = (df['High'] + df['Low'] + df['Close']) / 3.0
    mf = tp * df['Volume']
    diff = tp.diff()
    pos = pd.Series(0.0, index=df.index)
    neg = pd.Series(0.0, index=df.index)
    pos[diff > 0] = mf[diff > 0]
    neg[diff < 0] = mf[diff < 0]
    pos_sum = pos.rolling(period).sum()
    neg_sum = neg.rolling(period).sum().replace(0, np.nan)
    ratio = pos_sum / neg_sum
    out = 100 - (100 / (1 + ratio))
    return out.fillna(50)


def calc_upper_wick_total_pct(row: pd.Series) -> float:
    high_p = safe_float(row.get('High', 0))
    low_p = safe_float(row.get('Low', 0))
    open_p = safe_float(row.get('Open', 0))
    close_p = safe_float(row.get('Close', 0))
    total_range = max(high_p - low_p, 1e-9)
    body_top = max(open_p, close_p)
    upper_wick = max(0.0, high_p - body_top)
    return round((upper_wick / total_range) * 100, 1)


def ensure_supporting_columns(df: pd.DataFrame) -> pd.DataFrame:
    """필수 컬럼이 없으면 최소한만 내부 계산한다."""
    out = df.copy()
    required = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required:
        if col not in out.columns:
            raise ValueError(f'필수 컬럼 누락: {col}')

    close = out['Close']
    high = out['High']
    low = out['Low']
    vol = out['Volume']

    for n in [5, 20, 40, 60]:
        if f'MA{n}' not in out.columns:
            out[f'MA{n}'] = close.rolling(n, min_periods=max(2, n // 2)).mean()
    if 'VMA20' not in out.columns:
        out['VMA20'] = vol.rolling(20, min_periods=5).mean()
    if 'Disparity' not in out.columns:
        out['Disparity'] = (close / out['MA20'].replace(0, np.nan) * 100).fillna(100)

    if 'BB40_Width' not in out.columns:
        ma40 = out['MA40']
        std40 = close.rolling(40, min_periods=10).std(ddof=0)
        bb40_up = ma40 + std40 * 2
        bb40_dn = ma40 - std40 * 2
        out['BB40_Width'] = ((bb40_up - bb40_dn) / ma40.replace(0, np.nan) * 100).fillna(999)

    if 'MACD_Hist' not in out.columns:
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        out['MACD_Hist'] = macd - signal

    if 'OBV' not in out.columns:
        direction = np.sign(close.diff().fillna(0))
        obv_delta = pd.Series(0.0, index=out.index)
        obv_delta[direction > 0] = vol[direction > 0]
        obv_delta[direction < 0] = -vol[direction < 0]
        out['OBV'] = obv_delta.cumsum()
    if 'OBV_MA10' not in out.columns:
        out['OBV_MA10'] = out['OBV'].rolling(10, min_periods=3).mean()
    if 'OBV_Slope' not in out.columns:
        out['OBV_Slope'] = ((out['OBV'] - out['OBV'].shift(5)) / out['OBV'].shift(5).abs().replace(0, np.nan) * 100).fillna(0)

    if 'MFI' not in out.columns:
        out['MFI'] = mfi(out, 14)
    if 'ADX' not in out.columns or 'pDI' not in out.columns or 'mDI' not in out.columns:
        tr = pd.concat([
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ], axis=1).max(axis=1)
        dm_plus = (high - high.shift(1)).clip(lower=0)
        dm_minus = (low.shift(1) - low).clip(lower=0)
        tr14 = tr.rolling(14, min_periods=5).sum().replace(0, np.nan)
        out['pDI'] = (dm_plus.rolling(14, min_periods=5).sum() / tr14 * 100).fillna(0)
        out['mDI'] = (dm_minus.rolling(14, min_periods=5).sum() / tr14 * 100).fillna(0)
        out['ADX'] = (((out['pDI'] - out['mDI']).abs() / (out['pDI'] + out['mDI']).replace(0, np.nan)) * 100).rolling(14, min_periods=5).mean().fillna(0)

    out['Upper_Wick_Total_Pct_V4'] = out.apply(calc_upper_wick_total_pct, axis=1)
    out['TurnoverEok_V4'] = (out['Close'] * out['Volume'] / 100_000_000).fillna(0)
    out['RecentHigh3_V4'] = out['High'].rolling(3, min_periods=1).max()
    out['RecentHigh5_V4'] = out['High'].rolling(5, min_periods=1).max()
    out['MA5_Slope_Pos_V4'] = (out['MA5'] >= out['MA5'].shift(1)).fillna(False)
    return out


# ============================================================
# 파동 인식
# ============================================================

def find_pivot_highs(series: pd.Series, left: int = 3, right: int = 3) -> List[int]:
    vals = series.astype(float).tolist()
    out: List[int] = []
    for i in range(left, len(vals) - right):
        cur = vals[i]
        if all(cur >= vals[j] for j in range(i - left, i + right + 1)):
            out.append(i)
    return out


def find_pivot_lows(series: pd.Series, left: int = 3, right: int = 3) -> List[int]:
    vals = series.astype(float).tolist()
    out: List[int] = []
    for i in range(left, len(vals) - right):
        cur = vals[i]
        if all(cur <= vals[j] for j in range(i - left, i + right + 1)):
            out.append(i)
    return out


@dataclass
class WaveSegments:
    a_start: int
    a_end: int
    b_end: int
    c_end: int
    a_len: int
    b_len: int
    c_len: int
    a_ret_pct: float
    b_ret_pct: float
    c_ret_pct: float
    valid: bool
    reason: str = ''


def detect_wave_segments(df: pd.DataFrame, lookback: int = 60) -> WaveSegments:
    sub = df.tail(lookback).copy().reset_index(drop=False)
    if len(sub) < 20:
        return WaveSegments(0, 0, 0, len(sub) - 1, 0, 0, 0, 0, 0, 0, False, '데이터 부족')

    lows = find_pivot_lows(sub['Low'], 2, 2)
    highs = find_pivot_highs(sub['High'], 2, 2)
    cur = len(sub) - 1

    if not lows or not highs:
        return WaveSegments(0, 0, 0, cur, 0, 0, 0, 0, 0, 0, False, '피벗 부족')

    # 현재 이전의 최근 저점(B_END) 선택
    b_candidates = [i for i in lows if i <= cur - 2]
    if not b_candidates:
        return WaveSegments(0, 0, 0, cur, 0, 0, 0, 0, 0, 0, False, 'B 저점 없음')
    b_end = b_candidates[-1]

    # B_END 이전의 최근 의미 있는 고점(A_END)
    a_end_candidates = [i for i in highs if i < b_end]
    if not a_end_candidates:
        return WaveSegments(0, 0, b_end, cur, 0, 0, cur - b_end, 0, 0, 0, False, 'A 고점 없음')
    a_end = a_end_candidates[-1]

    # A_END 이전의 최근 저점(A_START)
    a_start_candidates = [i for i in lows if i < a_end]
    if not a_start_candidates:
        return WaveSegments(0, a_end, b_end, cur, a_end, b_end - a_end, cur - b_end, 0, 0, 0, False, 'A 시작 저점 없음')
    a_start = a_start_candidates[-1]

    a_len = max(a_end - a_start, 0)
    b_len = max(b_end - a_end, 0)
    c_len = max(cur - b_end, 0)

    a_start_p = safe_float(sub.loc[a_start, 'Low'])
    a_end_p = safe_float(sub.loc[a_end, 'High'])
    b_end_p = safe_float(sub.loc[b_end, 'Low'])
    c_end_p = safe_float(sub.loc[cur, 'Close'])

    a_ret = ((a_end_p - a_start_p) / a_start_p * 100) if a_start_p > 0 else 0.0
    b_ret = ((b_end_p - a_end_p) / a_end_p * 100) if a_end_p > 0 else 0.0
    c_ret = ((c_end_p - b_end_p) / b_end_p * 100) if b_end_p > 0 else 0.0

    valid = (a_len >= 3 and b_len >= 2 and c_len >= 2 and a_ret > 0 and b_ret < 0)
    reason = '' if valid else '파동 길이 또는 수익률 구조 미흡'

    return WaveSegments(
        a_start=a_start,
        a_end=a_end,
        b_end=b_end,
        c_end=cur,
        a_len=a_len,
        b_len=b_len,
        c_len=c_len,
        a_ret_pct=round(a_ret, 1),
        b_ret_pct=round(b_ret, 1),
        c_ret_pct=round(c_ret, 1),
        valid=valid,
        reason=reason,
    )


# ============================================================
# 기간대칭
# ============================================================

def calc_period_symmetry(seg: WaveSegments) -> Dict[str, object]:
    if not seg.valid or seg.a_len <= 0 or seg.b_len <= 0:
        return {
            'SYM_RATIO_BA': 0.0,
            'SYM_RATIO_CA': 0.0,
            'SYM_RATIO_CB': 0.0,
            'SYM_SCORE': 0,
            'SYM_PASS': False,
            'SYM_GRADE': 'F',
            'reason': seg.reason or '파동구조 미확정',
        }

    ratio_ba = seg.b_len / max(seg.a_len, 1)
    ratio_ca = seg.c_len / max(seg.a_len, 1)
    ratio_cb = seg.c_len / max(seg.b_len, 1)

    score = 0
    if 0.4 <= ratio_ba <= 1.2:
        score += 40
    elif 0.25 <= ratio_ba <= 1.5:
        score += 20

    if 0.3 <= ratio_ca <= 1.0:
        score += 30
    elif 0.2 <= ratio_ca <= 1.3:
        score += 15

    if 0.5 <= ratio_cb <= 1.5:
        score += 20
    elif 0.3 <= ratio_cb <= 1.8:
        score += 10

    if seg.a_len >= 3 and seg.b_len >= 2 and seg.c_len >= 2:
        score += 10

    grade = 'F'
    if score >= 80:
        grade = 'A'
    elif score >= 60:
        grade = 'B'
    elif score >= 40:
        grade = 'C'
    elif score >= 20:
        grade = 'D'

    return {
        'SYM_RATIO_BA': round(ratio_ba, 2),
        'SYM_RATIO_CA': round(ratio_ca, 2),
        'SYM_RATIO_CB': round(ratio_cb, 2),
        'SYM_SCORE': int(score),
        'SYM_PASS': score >= 60,
        'SYM_GRADE': grade,
        'reason': '',
    }


# ============================================================
# 파동에너지
# ============================================================

def calc_wave_energy(df: pd.DataFrame) -> Dict[str, object]:
    row = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else row

    bb_w = safe_float(row.get('BB40_Width', 999))
    bb_w_ma10 = safe_float(df['BB40_Width'].rolling(10, min_periods=3).mean().iloc[-1], 999)
    macd_hist = safe_float(row.get('MACD_Hist', 0))
    macd_hist_1 = safe_float(prev.get('MACD_Hist', 0))
    macd_hist_2 = safe_float(df['MACD_Hist'].iloc[-3] if len(df) >= 3 else macd_hist_1)
    obv = safe_float(row.get('OBV', 0))
    obv_ma10 = safe_float(row.get('OBV_MA10', 0))
    obv_slope = safe_float(row.get('OBV_Slope', 0))
    volume = safe_float(row.get('Volume', 0))
    vma20 = safe_float(row.get('VMA20', 0))
    turnover = safe_float(row.get('TurnoverEok_V4', 0))
    adx = safe_float(row.get('ADX', 0))
    pdi = safe_float(row.get('pDI', 0))
    mdi = safe_float(row.get('mDI', 0))

    bb_score = 0
    if bb_w <= 12:
        bb_score += 20
    if bb_w <= bb_w_ma10:
        bb_score += 10
    if bb_w > safe_float(prev.get('BB40_Width', bb_w)):
        bb_score += 10

    macd_score = 0
    if macd_hist > 0:
        macd_score += 15
    if macd_hist > macd_hist_1:
        macd_score += 10
    if macd_hist_1 > macd_hist_2:
        macd_score += 10

    obv_score = 0
    if obv > obv_ma10:
        obv_score += 15
    if obv_slope > 0:
        obv_score += 10
    if obv >= safe_float(df['OBV'].tail(5).max(), obv):
        obv_score += 10

    vol_score = 0
    if vma20 > 0 and volume >= vma20 * 1.0:
        vol_score += 10
    if vma20 > 0 and volume >= vma20 * 1.3:
        vol_score += 10
    if turnover >= 30:
        vol_score += 10

    adx_score = 0
    if adx >= 18:
        adx_score += 10
    if pdi > mdi:
        adx_score += 10

    total = bb_score + macd_score + obv_score + vol_score + adx_score

    grade = 'F'
    if total >= 80:
        grade = 'A'
    elif total >= 60:
        grade = 'B'
    elif total >= 40:
        grade = 'C'
    elif total >= 20:
        grade = 'D'

    return {
        'ENERGY_BB_SCORE': bb_score,
        'ENERGY_MACD_SCORE': macd_score,
        'ENERGY_OBV_SCORE': obv_score,
        'ENERGY_VOL_SCORE': vol_score,
        'ENERGY_ADX_SCORE': adx_score,
        'ENERGY_TOTAL': int(total),
        'ENERGY_PASS': total >= 60,
        'ENERGY_GRADE': grade,
    }


# ============================================================
# 수박 상태머신
# ============================================================

STATE_NONE = 0
STATE_GREEN_PREP = 1
STATE_RED_FIRE = 2
STATE_RED_HOLD = 3
STATE_END = 4

STATE_NAME_MAP = {
    STATE_NONE: 'NONE',
    STATE_GREEN_PREP: 'GREEN_PREP',
    STATE_RED_FIRE: 'RED_FIRE',
    STATE_RED_HOLD: 'RED_HOLD',
    STATE_END: 'END',
}


def _prep_score_row(row: pd.Series) -> int:
    score = 0
    if safe_float(row.get('BB40_Width', 999)) <= 18:
        score += 20
    if safe_float(row.get('Close', 0)) >= safe_float(row.get('MA20', 0)) * 0.98:
        score += 15
    if safe_float(row.get('Close', 0)) >= safe_float(row.get('MA40', 0)) * 0.96:
        score += 10
    disp = safe_float(row.get('Disparity', 100))
    if 97 <= disp <= 108:
        score += 15
    if safe_float(row.get('OBV_Slope', 0)) > 0:
        score += 20
    if safe_float(row.get('MFI', 50)) >= 45:
        score += 10
    if safe_float(row.get('TurnoverEok_V4', 0)) >= 30:
        score += 10
    return score


def _fire_score_row(row: pd.Series, prev_recent_high3: float) -> int:
    score = 0
    close = safe_float(row.get('Close', 0))
    ma20 = safe_float(row.get('MA20', 0))
    ma40 = safe_float(row.get('MA40', 0))
    open_p = safe_float(row.get('Open', 0))
    volume = safe_float(row.get('Volume', 0))
    vma20 = safe_float(row.get('VMA20', 0))
    wick = safe_float(row.get('Upper_Wick_Total_Pct_V4', 100))
    disp = safe_float(row.get('Disparity', 100))
    ma5_slope_pos = bool(row.get('MA5_Slope_Pos_V4', False))
    obv_slope = safe_float(row.get('OBV_Slope', 0))

    if close >= ma20:
        score += 15
    if close >= ma40:
        score += 15
    if close >= open_p:
        score += 10
    if vma20 > 0 and volume >= vma20 * 1.0:
        score += 15
    if wick <= 35:
        score += 10
    if 98 <= disp <= 110:
        score += 10
    if close >= prev_recent_high3 or (ma5_slope_pos and close >= ma20):
        score += 15
    if obv_slope > 0:
        score += 10
    return score


def _hold_ok(row: pd.Series, fire_score: int, prep_score: int) -> bool:
    conds = [
        prep_score >= 55,
        fire_score >= 55,
        safe_float(row.get('Close', 0)) >= safe_float(row.get('MA20', 0)),
        safe_float(row.get('OBV_Slope', 0)) > -3,
        safe_float(row.get('Upper_Wick_Total_Pct_V4', 100)) <= 45,
    ]
    return sum(1 for x in conds if x) >= 4


def _exit_count(row: pd.Series) -> int:
    cnt = 0
    if safe_float(row.get('Close', 0)) < safe_float(row.get('MA20', 0)):
        cnt += 1
    if safe_float(row.get('OBV_Slope', 0)) < 0:
        cnt += 1
    if safe_float(row.get('Upper_Wick_Total_Pct_V4', 0)) > 50:
        cnt += 1
    if safe_float(row.get('Disparity', 100)) > 112:
        cnt += 1
    if safe_float(row.get('BB40_Width', 0)) > 24:
        cnt += 1
    return cnt


def run_watermelon_state_machine(df: pd.DataFrame, sym_series: pd.Series, energy_series: pd.Series) -> pd.DataFrame:
    out = df.copy()
    prep_scores: List[int] = []
    fire_scores: List[int] = []
    states: List[int] = []
    hold_days: List[int] = []
    block_scores: List[float] = []

    state = STATE_NONE
    hold_day = 0

    for i in range(len(out)):
        row = out.iloc[i]
        prep_score = _prep_score_row(row)
        prev_recent_high3 = safe_float(out['RecentHigh3_V4'].iloc[i - 1], safe_float(row.get('Close', 0))) if i >= 1 else safe_float(row.get('Close', 0))
        fire_score = _fire_score_row(row, prev_recent_high3)
        prep_scores.append(prep_score)
        fire_scores.append(fire_score)

        prep_ready = False
        if i >= 2:
            recent = prep_scores[max(0, i - 2):i + 1]
            prep_ready = sum(1 for x in recent if x >= 65) >= 2
        else:
            prep_ready = prep_score >= 65

        sym_pass = bool(sym_series.iloc[i]) if len(sym_series) > i else False
        energy_pass = bool(energy_series.iloc[i]) if len(energy_series) > i else False
        fire_ready = fire_score >= 70 and sym_pass and energy_pass
        hold_ok = _hold_ok(row, fire_score, prep_score)
        exit_ready = _exit_count(row) >= 2

        if state == STATE_NONE:
            if prep_ready:
                state = STATE_GREEN_PREP
                hold_day = 0
        elif state == STATE_GREEN_PREP:
            if fire_ready:
                state = STATE_RED_FIRE
                hold_day = 1
            elif not prep_ready:
                state = STATE_NONE
                hold_day = 0
        elif state == STATE_RED_FIRE:
            if exit_ready:
                state = STATE_END
                hold_day = 0
            else:
                state = STATE_RED_HOLD
                hold_day = max(hold_day, 1)
        elif state == STATE_RED_HOLD:
            if exit_ready:
                state = STATE_END
                hold_day = 0
            elif hold_ok:
                state = STATE_RED_HOLD
                hold_day += 1
            else:
                state = STATE_GREEN_PREP if prep_ready else STATE_NONE
                hold_day = 0
        elif state == STATE_END:
            if prep_ready:
                state = STATE_GREEN_PREP
                hold_day = 0
            else:
                state = STATE_NONE
                hold_day = 0

        states.append(state)
        hold_days.append(hold_day)
        block_scores.append(round(prep_score * 0.45 + fire_score * 0.55, 1))

    out['WM_PREP_SCORE'] = prep_scores
    out['WM_FIRE_SCORE'] = fire_scores
    out['WM_STATE'] = states
    out['WM_STATE_NAME'] = out['WM_STATE'].map(STATE_NAME_MAP)
    out['WM_HOLD_DAYS'] = hold_days
    out['WM_BLOCK_SCORE'] = block_scores

    # V4 레거시 호환 컬럼
    out['Watermelon_Prepare_V4'] = out['WM_STATE'].eq(STATE_GREEN_PREP)
    out['Watermelon_First_Launch_V4'] = out['WM_STATE'].eq(STATE_RED_FIRE)
    out['Watermelon_Hold_V4'] = out['WM_STATE'].eq(STATE_RED_HOLD)
    out['Watermelon_Launch_V4'] = out['WM_STATE'].isin([STATE_RED_FIRE, STATE_RED_HOLD])
    return out


# ============================================================
# 최종 3박자 계산
# ============================================================

def apply_dante_v4(df: pd.DataFrame) -> pd.DataFrame:
    out = ensure_supporting_columns(df)

    # 파동 구간 / 기간대칭을 각 행에 rolling 근사 적용
    seg_rows = []
    sym_rows = []
    energy_rows = []
    for i in range(len(out)):
        sub = out.iloc[:i + 1].copy()
        seg = detect_wave_segments(sub, lookback=min(60, len(sub)))
        sym = calc_period_symmetry(seg)
        energy = calc_wave_energy(sub)
        seg_rows.append(seg)
        sym_rows.append(sym)
        energy_rows.append(energy)

    out['A_LEN'] = [s.a_len for s in seg_rows]
    out['B_LEN'] = [s.b_len for s in seg_rows]
    out['C_LEN'] = [s.c_len for s in seg_rows]
    out['A_RET_PCT'] = [s.a_ret_pct for s in seg_rows]
    out['B_RET_PCT'] = [s.b_ret_pct for s in seg_rows]
    out['C_RET_PCT'] = [s.c_ret_pct for s in seg_rows]

    for col in ['SYM_RATIO_BA', 'SYM_RATIO_CA', 'SYM_RATIO_CB', 'SYM_SCORE', 'SYM_PASS', 'SYM_GRADE']:
        out[col] = [x[col] for x in sym_rows]

    for col in ['ENERGY_BB_SCORE', 'ENERGY_MACD_SCORE', 'ENERGY_OBV_SCORE', 'ENERGY_VOL_SCORE', 'ENERGY_ADX_SCORE', 'ENERGY_TOTAL', 'ENERGY_PASS', 'ENERGY_GRADE']:
        out[col] = [x[col] for x in energy_rows]

    sym_series = out['SYM_PASS'].astype(bool)
    energy_series = out['ENERGY_PASS'].astype(bool)
    out = run_watermelon_state_machine(out, sym_series, energy_series)

    # 최종 종합점수
    out['DANTE_3PHASE_SCORE'] = (
        out['SYM_SCORE'] * 0.30 +
        out['ENERGY_TOTAL'] * 0.35 +
        out['WM_BLOCK_SCORE'] * 0.35
    ).round(1)

    def _grade(v: float) -> str:
        if v >= 80:
            return 'SSS'
        if v >= 70:
            return 'SS'
        if v >= 60:
            return 'S'
        if v >= 50:
            return 'A'
        if v >= 40:
            return 'B'
        return 'C'

    out['DANTE_3PHASE_GRADE'] = out['DANTE_3PHASE_SCORE'].map(_grade)
    out['DANTE_3PHASE_PASS'] = out['DANTE_3PHASE_SCORE'] >= 60

    out['DANTE_FINAL_PREP'] = (
        (out['SYM_SCORE'] >= 55) &
        (out['ENERGY_TOTAL'] >= 50) &
        (out['WM_STATE'] == STATE_GREEN_PREP)
    )
    out['DANTE_FINAL_FIRE'] = (
        (out['SYM_SCORE'] >= 60) &
        (out['ENERGY_TOTAL'] >= 60) &
        (out['WM_STATE'].isin([STATE_RED_FIRE, STATE_RED_HOLD]))
    )
    out['DANTE_FINAL_HOLD'] = (
        (out['WM_STATE'] == STATE_RED_HOLD) &
        (out['DANTE_3PHASE_SCORE'] >= 70)
    )

    # 기존 엔진과 호환되도록 수박 관련 별칭 제공
    out['WATERMELON_GREEN_SCORE_V4'] = out['WM_PREP_SCORE']
    out['WATERMELON_RED_SCORE_V4'] = out['WM_FIRE_SCORE']
    out['WATERMELON_QUALITY_V4'] = out['WM_BLOCK_SCORE']
    out['Watermelon_State_Name_V4'] = out['WM_STATE_NAME']
    out['WATERMELON_STATE_V4'] = out['WM_STATE']
    out['WATERMELON_PHASE_V4'] = out['WM_STATE_NAME']
    out['DANTE_V4_VERSION'] = DANTE_V4_VERSION

    # 추천 태그 텍스트
    tags = []
    for _, row in out.iterrows():
        t = []
        if bool(row.get('DANTE_FINAL_FIRE', False)):
            t.append('🍉3박자발사')
        elif bool(row.get('DANTE_FINAL_HOLD', False)):
            t.append('🍉3박자유지')
        elif bool(row.get('DANTE_FINAL_PREP', False)):
            t.append('🍈3박자준비')
        if safe_int(row.get('SYM_SCORE', 0)) >= 60:
            t.append(f"⏱대칭{safe_int(row.get('SYM_SCORE', 0))}")
        if safe_int(row.get('ENERGY_TOTAL', 0)) >= 60:
            t.append(f"⚡에너지{safe_int(row.get('ENERGY_TOTAL', 0))}")
        tags.append(' '.join(t))
    out['DANTE_3PHASE_TAGS'] = tags
    return out


# ============================================================
# 기존 main7_bugfix_2.py 에 연결하기 쉬운 helper
# ============================================================


def summarize_v4_row(row: pd.Series) -> str:
    phase = str(row.get('WM_STATE_NAME', 'NONE'))
    grade = str(row.get('DANTE_3PHASE_GRADE', 'C'))
    score = round(safe_float(row.get('DANTE_3PHASE_SCORE', 0)), 1)
    sym = safe_int(row.get('SYM_SCORE', 0))
    energy = safe_int(row.get('ENERGY_TOTAL', 0))
    quality = round(safe_float(row.get('WATERMELON_QUALITY_V4', 0)), 1)
    return f"{phase} | 등급 {grade} | 점수 {score} | 대칭 {sym} | 에너지 {energy} | 품질 {quality}"


def build_v4_signal_map(row: pd.Series) -> Dict[str, object]:
    """기존 콤보엔진 signals 딕셔너리에 주입하기 쉬운 맵."""
    return {
        'dante_v4_fire': bool(row.get('DANTE_FINAL_FIRE', False)),
        'dante_v4_prep': bool(row.get('DANTE_FINAL_PREP', False)),
        'dante_v4_hold': bool(row.get('DANTE_FINAL_HOLD', False)),
        'dante_v4_score': float(row.get('DANTE_3PHASE_SCORE', 0)),
        'dante_v4_grade': str(row.get('DANTE_3PHASE_GRADE', 'C')),
        'watermelon_prepare_v4': bool(row.get('Watermelon_Prepare_V4', False)),
        'watermelon_first_launch_v4': bool(row.get('Watermelon_First_Launch_V4', False)),
        'watermelon_hold_v4': bool(row.get('Watermelon_Hold_V4', False)),
        'watermelon_launch_v4': bool(row.get('Watermelon_Launch_V4', False)),
        'watermelon_quality_v4': float(row.get('WATERMELON_QUALITY_V4', 0)),
        'sym_score_v4': int(row.get('SYM_SCORE', 0)),
        'energy_total_v4': int(row.get('ENERGY_TOTAL', 0)),
        'wm_state_name_v4': str(row.get('WM_STATE_NAME', 'NONE')),
        'dante_v4_summary': summarize_v4_row(row),
        'dante_v4_version': str(row.get('DANTE_V4_VERSION', DANTE_V4_VERSION)),
    }


def apply_fear_and_quality_bonus(base_score: float, row: pd.Series) -> float:
    """사용자님의 공포흡수/종가배팅/돌반지 보정 아이디어를 V4 점수에 더하는 보조 함수."""
    score = float(base_score)
    if bool(row.get('공포매물흡수', False)):
        score += 8
    if str(row.get('closing_bet_grade', '')) in ('A', 'B'):
        score += 6
    if bool(row.get('Dolbanzi', False)) and bool(row.get('DANTE_FINAL_FIRE', False)):
        score += 10
    return round(score, 1)


if __name__ == '__main__':
    # 최소 문법/동작 확인용 예제
    idx = pd.date_range('2025-01-01', periods=120, freq='B')
    np.random.seed(7)
    base = np.linspace(100, 145, len(idx)) + np.sin(np.linspace(0, 8, len(idx))) * 4
    close = pd.Series(base + np.random.normal(0, 1.2, len(idx)), index=idx)
    open_ = close.shift(1).fillna(close.iloc[0]) + np.random.normal(0, 0.8, len(idx))
    high = pd.concat([open_, close], axis=1).max(axis=1) + abs(np.random.normal(1.2, 0.6, len(idx)))
    low = pd.concat([open_, close], axis=1).min(axis=1) - abs(np.random.normal(1.2, 0.6, len(idx)))
    volume = pd.Series(np.random.randint(100000, 400000, len(idx)), index=idx)

    sample = pd.DataFrame({
        'Open': open_.values,
        'High': high.values,
        'Low': low.values,
        'Close': close.values,
        'Volume': volume.values,
    }, index=idx)

    out = apply_dante_v4(sample)
    print(out[['DANTE_3PHASE_SCORE', 'DANTE_3PHASE_GRADE', 'WM_STATE_NAME', 'DANTE_FINAL_PREP', 'DANTE_FINAL_FIRE', 'DANTE_FINAL_HOLD']].tail(10))


# ============================================================
# 예비돌반지(통합형)
# ============================================================

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.replace(",", "").strip()
            if x == "":
                return default
        v = float(x)
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(round(safe_float(x, default)))
    except Exception:
        return default


def _has_required_columns(df: pd.DataFrame, cols: List[str]) -> bool:
    return all(c in df.columns for c in cols)


def _recent(df: pd.DataFrame, n: int) -> pd.DataFrame:
    return df.tail(n).copy()


def _pct_diff(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def _between(v: float, lo: float, hi: float) -> bool:
    return lo <= v <= hi


@dataclass
class PatternCheckResult:
    name: str
    passed: bool
    score: int
    max_score: int
    detail: Dict[str, Any]


def _pre_dolbanji_common(row: pd.Series, df: pd.DataFrame) -> Dict[str, Any]:
    close = safe_float(row.get("Close"))
    ma5 = safe_float(row.get("MA5"))
    ma20 = safe_float(row.get("MA20"))
    ma60 = safe_float(row.get("MA60"))
    ma112 = safe_float(row.get("MA112"))
    ma224 = safe_float(row.get("MA224"))
    ma448 = safe_float(row.get("MA448"))
    bb40u = safe_float(row.get("BB40_Upper"))
    bb40w = safe_float(row.get("BB40_Width"))
    obv = safe_float(row.get("OBV"))
    obv_ma10 = safe_float(row.get("OBV_MA10"))
    macd_hist = safe_float(row.get("MACD_Hist"))

    recent50 = _recent(df, 50)
    prev_close = recent50["Close"].shift(1) if "Close" in recent50.columns else pd.Series(dtype=float)
    surge_pct = ((recent50["High"] / prev_close) - 1.0) * 100 if "High" in recent50.columns else pd.Series(dtype=float)
    vol_ratio = (recent50["Volume"] / recent50["Volume"].shift(1)) if "Volume" in recent50.columns else pd.Series(dtype=float)

    ma224_below_448_days_50 = int(((recent50["MA224"] < recent50["MA448"]).fillna(False)).sum()) \
        if _has_required_columns(recent50, ["MA224", "MA448"]) else 0

    past_ma448_break = bool((recent50["High"] > recent50["MA448"]).fillna(False).any()) \
        if _has_required_columns(recent50, ["High", "MA448"]) else False

    past_ma448_near_break = bool((recent50["High"] > recent50["MA448"] * 0.98).fillna(False).any()) \
        if _has_required_columns(recent50, ["High", "MA448"]) else False

    info = {
        "close": close,
        "ma5": ma5,
        "ma20": ma20,
        "ma60": ma60,
        "ma112": ma112,
        "ma224": ma224,
        "ma448": ma448,
        "bb40_upper": bb40u,
        "bb40_width": bb40w,
        "obv": obv,
        "obv_ma10": obv_ma10,
        "macd_hist": macd_hist,
        "ma224_below_448_days_50": ma224_below_448_days_50,
        "past_surge_12_30": bool(surge_pct.between(12, 30).any()) if len(surge_pct) else False,
        "past_surge_10_35": bool(surge_pct.between(10, 35).any()) if len(surge_pct) else False,
        "past_surge_15_40": bool(surge_pct.between(15, 40).any()) if len(surge_pct) else False,
        "past_volume_3x": bool((vol_ratio >= 3.0).any()) if len(vol_ratio) else False,
        "past_volume_2_5x": bool((vol_ratio >= 2.5).any()) if len(vol_ratio) else False,
        "past_volume_4x": bool((vol_ratio >= 4.0).any()) if len(vol_ratio) else False,
        "past_ma448_break": past_ma448_break,
        "past_ma448_near_break": past_ma448_near_break,
        "recent30_obv_up": bool(obv > obv_ma10) if obv_ma10 > 0 else False,
        "recent30_close_above_ma112": bool(close > ma112) if ma112 > 0 else False,
        "recent30_ma20_gt_ma60": bool(ma20 > ma60) if ma20 > 0 and ma60 > 0 else False,
    }

    info["close_vs_ma224_pct"] = round(_pct_diff(close, ma224), 2) if ma224 > 0 else None
    info["close_vs_ma448_pct"] = round(_pct_diff(close, ma448), 2) if ma448 > 0 else None
    info["close_vs_bb40u_pct"] = round(_pct_diff(close, bb40u), 2) if bb40u > 0 else None
    return info


def check_pre_dolbanji_A(row: pd.Series, df: pd.DataFrame) -> PatternCheckResult:
    info = _pre_dolbanji_common(row, df)
    cond1 = info["ma224"] > 0 and info["ma448"] > 0 and info["ma224"] < info["ma448"]
    cond2 = info["ma224_below_448_days_50"] >= 35
    cond3 = info["close_vs_ma224_pct"] is not None and _between(info["close_vs_ma224_pct"], -3.0, 5.0)
    cond4 = info["close"] < info["ma448"] if info["ma448"] > 0 else False
    cond5 = info["ma5"] > info["ma112"] if info["ma112"] > 0 else False
    cond6 = info["close_vs_bb40u_pct"] is not None and _between(info["close_vs_bb40u_pct"], -7.0, 3.0)
    cond7 = info["past_surge_12_30"]
    cond8 = info["past_volume_4x"]
    cond9 = info["past_ma448_break"]
    conditions = [cond1, cond2, cond3, cond4, cond5, cond6, cond7, cond8, cond9]
    return PatternCheckResult("예비돌반지A", all(conditions), sum(1 for x in conditions if x), 9, info)


def check_pre_dolbanji_B(row: pd.Series, df: pd.DataFrame) -> PatternCheckResult:
    info = _pre_dolbanji_common(row, df)
    cond1 = info["ma224"] > 0 and info["ma448"] > 0 and info["ma224"] < info["ma448"]
    cond2 = info["ma224_below_448_days_50"] >= 25
    cond3 = info["close_vs_ma224_pct"] is not None and _between(info["close_vs_ma224_pct"], -5.0, 8.0)
    cond4 = info["close_vs_ma448_pct"] is not None and info["close_vs_ma448_pct"] <= 3.0
    cond5 = info["ma112"] > 0 and info["ma5"] >= info["ma112"] * 0.98
    cond6 = info["close_vs_bb40u_pct"] is not None and _between(info["close_vs_bb40u_pct"], -10.0, 5.0)
    cond7 = info["past_surge_10_35"]
    cond8 = info["past_volume_2_5x"]
    cond9 = info["past_ma448_near_break"]
    conditions = [cond1, cond2, cond3, cond4, cond5, cond6, cond7, cond8, cond9]
    score = sum(1 for x in conditions if x)
    return PatternCheckResult("예비돌반지B", score >= 7, score, 9, info)


def check_pre_dolbanji_C(row: pd.Series, df: pd.DataFrame) -> PatternCheckResult:
    info = _pre_dolbanji_common(row, df)
    cond1 = info["ma224"] > 0 and info["ma448"] > 0 and info["ma224"] < info["ma448"]
    cond2 = info["close_vs_ma224_pct"] is not None and _between(info["close_vs_ma224_pct"], -6.0, 10.0)
    cond3 = info["close_vs_ma448_pct"] is not None and info["close_vs_ma448_pct"] <= 5.0
    cond4 = info["recent30_close_above_ma112"]
    cond5 = info["past_surge_15_40"]
    cond6 = info["past_volume_4x"]
    cond7 = info["past_ma448_break"]
    cond8 = info["recent30_obv_up"]
    conditions = [cond1, cond2, cond3, cond4, cond5, cond6, cond7, cond8]
    score = sum(1 for x in conditions if x)
    return PatternCheckResult("예비돌반지C", score >= 6, score, 8, info)


def check_pre_dolbanji_D(row: pd.Series, df: pd.DataFrame) -> PatternCheckResult:
    info = _pre_dolbanji_common(row, df)
    cond1 = info["ma224"] > 0 and info["ma448"] > 0 and info["ma224"] < info["ma448"]
    cond2 = info["close_vs_ma448_pct"] is not None and info["close_vs_ma448_pct"] <= 2.0
    cond3 = info["recent30_ma20_gt_ma60"] or (info["ma112"] > 0 and info["ma5"] > info["ma112"])
    cond4 = info["bb40_width"] > 0 and info["bb40_width"] <= 18.0
    cond5 = info["recent30_obv_up"]
    cond6 = info["past_ma448_break"]
    cond7 = info["past_volume_3x"]
    conditions = [cond1, cond2, cond3, cond4, cond5, cond6, cond7]
    return PatternCheckResult("예비돌반지D", all(conditions), sum(1 for x in conditions if x), 7, info)


def check_trend_reversal_confirm(row: pd.Series, df: pd.DataFrame) -> PatternCheckResult:
    if len(df) < 5:
        return PatternCheckResult("구조전환확인", False, 0, 4, {"reason": "데이터 부족"})
    ma20_now = safe_float(row.get("MA20"))
    ma20_prev = safe_float(df["MA20"].iloc[-4]) if "MA20" in df.columns else 0.0
    macd_hist_now = safe_float(row.get("MACD_Hist"))
    macd_hist_prev = safe_float(df["MACD_Hist"].iloc[-2]) if "MACD_Hist" in df.columns else 0.0
    obv = safe_float(row.get("OBV"))
    obv_ma10 = safe_float(row.get("OBV_MA10"))
    close = safe_float(row.get("Close"))
    ma112 = safe_float(row.get("MA112"))
    c1 = ma20_now > ma20_prev if ma20_now > 0 and ma20_prev > 0 else False
    c2 = (macd_hist_now > 0) or (macd_hist_now > macd_hist_prev)
    c3 = obv > obv_ma10 if obv_ma10 > 0 else False
    c4 = close > ma112 if ma112 > 0 else False
    score = sum([c1, c2, c3, c4])
    return PatternCheckResult("구조전환확인", score >= 3, score, 4, {
        "ma20_slope_up": c1,
        "macd_turn_up": c2,
        "obv_confirm": c3,
        "close_above_ma112": c4,
    })


def evaluate_pre_dolbanji_suite(df: pd.DataFrame) -> Dict[str, Any]:
    required = [
        "Open", "High", "Low", "Close", "Volume",
        "MA5", "MA20", "MA60", "MA112", "MA224", "MA448",
        "BB40_Upper", "BB40_Width", "OBV", "OBV_MA10", "MACD_Hist",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return {"ok": False, "error": f"필수 컬럼 부족: {', '.join(missing)}", "patterns": []}
    if len(df) < 60:
        return {"ok": False, "error": "최소 60봉 이상 필요", "patterns": []}

    row = df.iloc[-1]
    patterns = [
        check_pre_dolbanji_A(row, df),
        check_pre_dolbanji_B(row, df),
        check_pre_dolbanji_C(row, df),
        check_pre_dolbanji_D(row, df),
    ]
    trend = check_trend_reversal_confirm(row, df)
    passed_patterns = [p for p in patterns if p.passed]
    tags = [f"💍{p.name}" for p in passed_patterns]
    score = sum(p.score * 10 for p in passed_patterns)
    if trend.passed:
        tags.append(f"🧭구조전환확인({trend.score}/4)")
        score += 40
    else:
        tags.append(f"🧭구조전환미완({trend.score}/4)")
    if any(p.name == "예비돌반지D" and p.passed for p in passed_patterns):
        tags.append("🚀재돌파임박")
        score += 20
    if any(p.name == "예비돌반지A" and p.passed for p in passed_patterns):
        tags.append("📚정석형")
        score += 15
    if any(p.name == "예비돌반지C" and p.passed for p in passed_patterns):
        tags.append("🐋세력흔적형")
        score += 15

    confirmed = bool(passed_patterns) and trend.passed
    grade = "없음"
    if confirmed and len(passed_patterns) >= 2 and trend.score >= 4:
        grade = "S"
    elif confirmed and len(passed_patterns) >= 1:
        grade = "A"
    elif len(passed_patterns) >= 1:
        grade = "B"

    return {
        "ok": True,
        "patterns": [asdict(p) for p in patterns],
        "trend_confirm": asdict(trend),
        "passed_names": [p.name for p in passed_patterns],
        "confirmed": confirmed,
        "score": int(score),
        "grade": grade,
        "tags": tags,
        "best_pattern": passed_patterns[0].name if passed_patterns else "",
    }


def build_pre_dolbanji_bundle(df: pd.DataFrame) -> Dict[str, Any]:
    suite = evaluate_pre_dolbanji_suite(df)
    if not suite.get("ok", False):
        return {
            "pre_dolbanji": False,
            "pre_dolbanji_confirmed": False,
            "pre_dolbanji_score": 0,
            "pre_dolbanji_grade": "없음",
            "pre_dolbanji_tags": [],
            "pre_dolbanji_best": "",
            "pre_dolbanji_detail": suite,
        }
    return {
        "pre_dolbanji": bool(suite["passed_names"]),
        "pre_dolbanji_confirmed": bool(suite["confirmed"]),
        "pre_dolbanji_score": int(suite["score"]),
        "pre_dolbanji_grade": suite["grade"],
        "pre_dolbanji_tags": suite["tags"],
        "pre_dolbanji_best": suite["best_pattern"],
        "pre_dolbanji_detail": suite,
    }


# ============================================================
# 예비돌반지 HTS 정확복제형 (스크린샷 기반)
# ============================================================

def _hts_exact_ensure_ma180(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "MA180" not in out.columns:
        if "Close" in out.columns:
            out["MA180"] = out["Close"].rolling(180, min_periods=30).mean()
        else:
            out["MA180"] = 0.0
    return out

def _hts_exact_ensure_bb40_upper(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "BB40_Upper" not in out.columns:
        close = out["Close"] if "Close" in out.columns else pd.Series(dtype=float)
        ma40 = close.rolling(40, min_periods=20).mean()
        std40 = close.rolling(40, min_periods=20).std()
        out["BB40_Upper"] = ma40 + std40 * 2.0
    return out

def _hts_exact_avg_turnover_prev10(df: pd.DataFrame) -> float:
    if "Close" not in df.columns or "Volume" not in df.columns or len(df) < 11:
        return 0.0
    prev10 = df.iloc[-11:-1].copy()
    return float((prev10["Close"] * prev10["Volume"]).mean())

def _hts_exact_cond_inverse_arrangement_recent10(df: pd.DataFrame) -> bool:
    if len(df) < 10:
        return False
    recent10 = df.tail(10)
    cond = (
        (recent10["Close"] < recent10["MA180"]) &
        (recent10["MA180"] < recent10["MA224"]) &
        (recent10["MA224"] < recent10["MA448"])
    )
    return bool(cond.any())

def _hts_exact_cond_ma224_lt_ma448_persist50(df: pd.DataFrame) -> bool:
    if len(df) < 50:
        return False
    recent50 = df.tail(50)
    return bool((recent50["MA224"] < recent50["MA448"]).all())

def _hts_exact_cond_close_vs_ma224(row: pd.Series) -> bool:
    close_p = safe_float(row.get("Close"))
    ma224 = safe_float(row.get("MA224"))
    if ma224 <= 0:
        return False
    pct = (close_p - ma224) / ma224 * 100.0
    return -3.0 <= pct <= 5.0

def _hts_exact_cond_close_lt_ma448(row: pd.Series) -> bool:
    close_p = safe_float(row.get("Close"))
    ma448 = safe_float(row.get("MA448"))
    return ma448 > 0 and close_p < ma448

def _hts_exact_cond_ma5_ge_ma112(row: pd.Series) -> bool:
    ma5 = safe_float(row.get("MA5"))
    ma112 = safe_float(row.get("MA112"))
    return ma5 > 0 and ma112 > 0 and ma5 >= ma112

def _hts_exact_cond_close_near_bb40_upper(row: pd.Series) -> bool:
    close_p = safe_float(row.get("Close"))
    bb40u = safe_float(row.get("BB40_Upper"))
    if bb40u <= 0:
        return False
    pct = (close_p - bb40u) / bb40u * 100.0
    return -7.0 <= pct <= 3.0

def _hts_exact_cond_past_surge_recent50(df: pd.DataFrame) -> bool:
    if len(df) < 51:
        return False
    recent50 = df.tail(50)
    prev_close = df["Close"].shift(1).loc[recent50.index].replace(0, np.nan)
    surge_pct = ((recent50["High"] / prev_close) - 1.0) * 100.0
    return bool(surge_pct.between(12.0, 30.0).fillna(False).any())

def _hts_exact_cond_past_volume_burst_recent50(df: pd.DataFrame) -> bool:
    if len(df) < 51:
        return False
    recent50 = df.tail(50)
    prev_vol = df["Volume"].shift(1).loc[recent50.index].replace(0, np.nan)
    vol_pct = (recent50["Volume"] / prev_vol) * 100.0
    return bool(vol_pct.between(300.0, 999999.0).fillna(False).any())

def _hts_exact_cond_past_ma448_high_break_recent50(df: pd.DataFrame) -> bool:
    if len(df) < 51:
        return False
    recent50 = df.tail(50)
    return bool((recent50["High"] > recent50["MA448"]).fillna(False).any())

def evaluate_pre_dolbanji_hts_exact(df: pd.DataFrame) -> Dict[str, Any]:
    work = _hts_exact_ensure_ma180(df)
    work = _hts_exact_ensure_bb40_upper(work)
    required_cols = ["Close", "High", "Volume", "MA5", "MA112", "MA224", "MA448", "MA180", "BB40_Upper"]
    missing = [c for c in required_cols if c not in work.columns]
    if missing or len(work) < 60:
        return {
            "passed": False,
            "score": 0,
            "max_score": 10,
            "tags": [],
            "detail": {"error": f"필수 컬럼/봉수 부족: {missing}, len={len(work)}"}
        }

    row = work.iloc[-1]
    conds = {
        "A_turnover_ma10_prev_between_3e8_and_max": _hts_exact_avg_turnover_prev10(work) >= 300_000_000,
        "B_inverse_arrangement_recent10": _hts_exact_cond_inverse_arrangement_recent10(work),
        "C_ma224_lt_ma448_persist50": _hts_exact_cond_ma224_lt_ma448_persist50(work),
        "D_close_vs_ma224_-3_to_5": _hts_exact_cond_close_vs_ma224(row),
        "E_close_lt_ma448_now": _hts_exact_cond_close_lt_ma448(row),
        "F_ma5_ge_ma112_now": _hts_exact_cond_ma5_ge_ma112(row),
        "G_close_near_bb40_upper": _hts_exact_cond_close_near_bb40_upper(row),
        "H_past_surge_recent50": _hts_exact_cond_past_surge_recent50(work),
        "I_past_volume_burst_recent50": _hts_exact_cond_past_volume_burst_recent50(work),
        "J_past_high_break_ma448_recent50": _hts_exact_cond_past_ma448_high_break_recent50(work),
    }
    tech_keys = list(conds.keys())
    tech_score = sum(1 for k in tech_keys if bool(conds[k]))
    tags: List[str] = []
    if tech_score == len(tech_keys):
        tags.append("💍HTS정확복제형")
    elif tech_score >= 8:
        tags.append("💍HTS유사강형")
    elif tech_score >= 6:
        tags.append("💍HTS유사약형")
    if conds["C_ma224_lt_ma448_persist50"]:
        tags.append("🧱224<448_50봉지속")
    if conds["G_close_near_bb40_upper"]:
        tags.append("🟣BB40상단근접")
    if conds["H_past_surge_recent50"] and conds["I_past_volume_burst_recent50"]:
        tags.append("🚀과거폭발흔적")
    if conds["J_past_high_break_ma448_recent50"]:
        tags.append("📈448상향돌파이력")
    return {
        "passed": tech_score == len(tech_keys),
        "score": int(tech_score),
        "max_score": len(tech_keys),
        "tags": tags,
        "detail": {**conds, "tech_score": tech_score, "tech_max": len(tech_keys)},
    }

def build_pre_dolbanji_hts_exact_bundle(df: pd.DataFrame) -> Dict[str, Any]:
    res = evaluate_pre_dolbanji_hts_exact(df)
    return {
        "pre_dolbanji_hts_exact": bool(res.get("passed", False)),
        "pre_dolbanji_hts_exact_score": int(res.get("score", 0)),
        "pre_dolbanji_hts_exact_max_score": int(res.get("max_score", 10)),
        "pre_dolbanji_hts_exact_tags": res.get("tags", []),
        "pre_dolbanji_hts_exact_detail": res.get("detail", {}),
    }
