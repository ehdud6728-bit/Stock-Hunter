from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .config import (
    BAND_FAMILIES,
    KKI_HIGH,
    KKI_IMPULSE_BODY_PCT,
    KKI_IMPULSE_VOL_MULT,
    KKI_LOOKBACK_DAYS,
    KKI_MEDIUM,
    KKI_PULLBACK_MAX_DAYS,
    KKI_RELAUNCH_MAX_DAYS,
    KKI_SHOW_MIN,
    KKI_SIDEWAYS_MAX_RANGE,
)
from .common import safe_float, safe_int, text_join


@dataclass
class KkiProfileResult:
    score: int
    tag: str
    best_band: str
    recurrence_summary: str
    current_state: str
    commentary: str
    show: bool

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    mapping = {
        'Open': ['Open', '시가'],
        'High': ['High', '고가'],
        'Low': ['Low', '저가'],
        'Close': ['Close', '종가'],
        'Volume': ['Volume', '거래량'],
    }
    for target, aliases in mapping.items():
        if target not in out.columns:
            for alias in aliases:
                if alias in out.columns:
                    out[target] = out[alias]
                    break
    return out


def _body_pct(row: pd.Series) -> float:
    o = safe_float(row.get('Open'))
    c = safe_float(row.get('Close'))
    return 0.0 if o <= 0 else abs(c - o) / o


def _upper_wick_pct(row: pd.Series) -> float:
    o = safe_float(row.get('Open'))
    c = safe_float(row.get('Close'))
    h = safe_float(row.get('High'))
    if max(o, c) <= 0:
        return 0.0
    return max(0.0, h - max(o, c)) / max(o, c)


def _is_bull_impulse(row: pd.Series, vol_ma: float) -> bool:
    o = safe_float(row.get('Open'))
    c = safe_float(row.get('Close'))
    if o <= 0 or c <= o:
        return False
    body = _body_pct(row)
    vol = safe_float(row.get('Volume'))
    wick = _upper_wick_pct(row)
    return body >= KKI_IMPULSE_BODY_PCT and vol >= (vol_ma * KKI_IMPULSE_VOL_MULT) and wick <= 0.06


def _add_bands(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = out['Close']
    for name, period, width in BAND_FAMILIES:
        ma = close.rolling(period).mean()
        if name.startswith('BB'):
            std = close.rolling(period).std(ddof=0)
            out[f'{name}_mid'] = ma
            out[f'{name}_upper'] = ma + (std * width)
            out[f'{name}_lower'] = ma - (std * width)
            out[f'{name}_width'] = ((out[f'{name}_upper'] - out[f'{name}_lower']) / ma.replace(0, np.nan)).fillna(0.0)
        else:
            out[f'{name}_mid'] = ma
            out[f'{name}_upper'] = ma * (1.0 + width)
            out[f'{name}_lower'] = ma * (1.0 - width)
            out[f'{name}_width'] = ((out[f'{name}_upper'] - out[f'{name}_lower']) / ma.replace(0, np.nan)).fillna(0.0)
    return out


def _count_pattern_hits(df: pd.DataFrame) -> Tuple[int, int, int]:
    # 장대양봉 -> 눌림 -> 재발사 / 연속 장대양봉 / 박스 횡보 후 재발사
    if len(df) < 60:
        return 0, 0, 0
    vol_ma20 = df['Volume'].rolling(20).mean().fillna(method='bfill')
    impulse_pull_relaunch = 0
    consecutive_impulse = 0
    sideways_relaunch = 0

    impulses = [i for i in range(len(df)) if _is_bull_impulse(df.iloc[i], safe_float(vol_ma20.iloc[i], 0.0))]

    for i in impulses:
        # 연속 장대양봉
        if i + 1 < len(df) and _is_bull_impulse(df.iloc[i + 1], safe_float(vol_ma20.iloc[i + 1], 0.0)):
            consecutive_impulse += 1

        # 눌림 후 재발사
        pull_end = min(len(df) - 1, i + KKI_PULLBACK_MAX_DAYS)
        relaunch_end = min(len(df) - 1, pull_end + KKI_RELAUNCH_MAX_DAYS)
        if i + 3 >= len(df):
            continue
        pull_df = df.iloc[i + 1: pull_end + 1]
        if pull_df.empty:
            continue
        peak = safe_float(df.iloc[i]['Close'])
        trough = safe_float(pull_df['Low'].min())
        pullback_ok = trough <= peak * 0.97 and trough >= peak * 0.82
        if pullback_ok:
            for j in range(pull_end, relaunch_end + 1):
                if _is_bull_impulse(df.iloc[j], safe_float(vol_ma20.iloc[j], 0.0)) and safe_float(df.iloc[j]['Close']) >= peak * 0.97:
                    impulse_pull_relaunch += 1
                    break

        # 횡보 후 재발사
        box_df = df.iloc[i + 1: pull_end + 1]
        if len(box_df) >= 4:
            hi = safe_float(box_df['High'].max())
            lo = safe_float(box_df['Low'].min())
            box_range = 0.0 if lo <= 0 else (hi - lo) / lo
            if box_range <= KKI_SIDEWAYS_MAX_RANGE:
                for j in range(pull_end, relaunch_end + 1):
                    if _is_bull_impulse(df.iloc[j], safe_float(vol_ma20.iloc[j], 0.0)):
                        sideways_relaunch += 1
                        break
    return impulse_pull_relaunch, consecutive_impulse, sideways_relaunch


def _band_fit_score(df: pd.DataFrame, name: str) -> int:
    if f'{name}_lower' not in df.columns:
        return 0
    close = df['Close']
    low = df['Low']
    high = df['High']
    lower = df[f'{name}_lower']
    upper = df[f'{name}_upper']
    width = df[f'{name}_width']
    score = 0

    # 하단 터치 후 반등
    lower_touch = (low <= lower * 1.01)
    rebound = (close.shift(-1) > close)
    score += int((lower_touch & rebound).sum()) * 2

    # 수축 후 상승
    squeeze = width <= width.rolling(40).quantile(0.3)
    breakout = close.shift(-1) > upper
    score += int((squeeze & breakout).sum()) * 2

    # 상단 터치 후 눌림 -> 재상승
    upper_touch = high >= upper * 0.995
    for i in range(len(df) - 6):
        if not upper_touch.iloc[i]:
            continue
        pull = close.iloc[i + 1:i + 4]
        relaunch = close.iloc[i + 4:i + 7]
        if len(pull) < 2 or len(relaunch) < 1:
            continue
        if pull.min() <= close.iloc[i] * 0.97 and relaunch.max() >= close.iloc[i] * 1.01:
            score += 3
    return int(score)


def _current_state_text(df: pd.DataFrame, best_band: str) -> str:
    if df.empty:
        return '데이터 부족'
    row = df.iloc[-1]
    close = safe_float(row.get('Close'))
    upper = safe_float(row.get(f'{best_band}_upper'))
    lower = safe_float(row.get(f'{best_band}_lower'))
    width = safe_float(row.get(f'{best_band}_width'))
    if upper > 0 and close >= upper * 0.995:
        return '상단 돌파/재발사 시험 구간'
    if lower > 0 and close <= lower * 1.02:
        return '하단 지지 확인 구간'
    if width and width <= df[f'{best_band}_width'].rolling(40).quantile(0.3).iloc[-1]:
        return '수축 후 방향 선택 직전 구간'
    return '중립 눌림/횡보 구간'


def build_kki_profile(df: pd.DataFrame) -> Dict[str, object]:
    if df is None or len(df) < 80:
        return KkiProfileResult(0, '', '', '데이터 부족', '데이터 부족', '', False).to_dict()

    work = _ensure_columns(df.tail(KKI_LOOKBACK_DAYS)).dropna(subset=['Open','High','Low','Close','Volume']).copy()
    if len(work) < 80:
        return KkiProfileResult(0, '', '', '데이터 부족', '데이터 부족', '', False).to_dict()
    work = _add_bands(work)

    pattern1, pattern2, pattern3 = _count_pattern_hits(work)
    band_scores = {name: _band_fit_score(work, name) for name, _, _ in BAND_FAMILIES}
    best_band = max(band_scores, key=band_scores.get)

    score = 0
    score += min(pattern1 * 10, 35)
    score += min(pattern2 * 6, 18)
    score += min(pattern3 * 7, 21)
    score += min(band_scores[best_band], 26)
    score = min(score, 100)

    if score >= KKI_HIGH:
        tag = '🔥끼강'
    elif score >= KKI_MEDIUM:
        tag = '🟡끼보통'
    elif score > 0:
        tag = '😴끼약'
    else:
        tag = ''

    current_state = _current_state_text(work, best_band)
    recurrence_parts = []
    if pattern1:
        recurrence_parts.append(f'장대양봉→눌림→재발사 {pattern1}회')
    if pattern2:
        recurrence_parts.append(f'연속 장대양봉 {pattern2}회')
    if pattern3:
        recurrence_parts.append(f'횡보 후 재발사 {pattern3}회')
    if band_scores[best_band]:
        recurrence_parts.append(f'{best_band} 적합도 {band_scores[best_band]}점')
    recurrence_summary = text_join(recurrence_parts, sep=' | ') or '뚜렷한 과거 재현 패턴은 약함'

    if score >= KKI_HIGH:
        commentary = f'과거에 비슷한 재발사 패턴이 반복된 편이며, 현재도 {current_state}이라 2차 상승 재현 가능성을 함께 볼 수 있습니다.'
    elif score >= KKI_MEDIUM:
        commentary = f'과거 유사 패턴이 어느 정도 관찰되며, 현재는 {current_state}이라 확인 신호가 붙을 때 효율이 좋습니다.'
    elif score > 0:
        commentary = f'과거 반복성은 약한 편이고, 현재는 {current_state}입니다. 단독 신뢰보다 다른 구조 확인이 먼저입니다.'
    else:
        commentary = ''

    show = (score >= KKI_SHOW_MIN)
    return KkiProfileResult(score, tag, best_band, recurrence_summary, current_state, commentary, show).to_dict()
