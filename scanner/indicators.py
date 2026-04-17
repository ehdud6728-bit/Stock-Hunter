from __future__ import annotations

import os
import pandas as pd

"""실제 로직 이관형 indicators 모듈.
기존 main7_bugfix_2.py의 기술지표/분류 함수 일부를 분리했다.
"""

def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, str(default))).strip().lower()
    return raw in ('1', 'true', 't', 'yes', 'y', 'on')

def _env_str(name: str, default: str = '') -> str:
    return str(os.environ.get(name, default)).strip()

def check_bb40_second_wave(curr: pd.Series, past: pd.DataFrame):
    """
    BB40 재안착 후 2차 파동:
    - 과거 BB40 하단 이탈 후 복귀 이력
    - 현재 BB40 중심선 위 or 상단밴드 방향
    - OBV/RSI가 1차 반등보다 강해짐
    """
    if past.empty or len(past) < 15:
        return False, "데이터 부족"

    bb40_break = (past['Low'] < past['BB40_Lower']).any()
    bb40_reclaim = (past['Close'] > past['BB40_Lower']).any()

    above_mid = curr['Close'] > curr['MA40']
    bb_expand = curr['BB40_Width'] > past['BB40_Width'].tail(5).mean()
    obv_up = curr['OBV'] > past['OBV'].tail(5).max()
    rsi_up = curr['RSI'] > past['RSI'].tail(5).max()

    passed = bb40_break and bb40_reclaim and above_mid and (obv_up or rsi_up) and bb_expand
    return passed, f"BB40이탈:{bb40_break}, 복귀:{bb40_reclaim}, 중심선위:{above_mid}, OBV상승:{obv_up}, RSI상승:{rsi_up}"

def check_obv_acc_breakout(curr: pd.Series, past: pd.DataFrame):
    """
    OBV 매집 후 돌파:
    - 최근 박스권/수렴
    - OBV는 미리 상승
    - 현재 가격/거래량 돌파
    """
    if past.empty or len(past) < 20:
        return False, "데이터 부족"

    box_range = (past['High'].max() / (past['Low'].min() + 1e-9)) <= 1.18
    obv_acc = curr['OBV'] > past['OBV'].tail(10).max()
    price_break = curr['Close'] > past['High'].tail(10).max()
    vol_break = curr['Volume'] > curr['Vol_Avg'] * 1.5

    passed = box_range and obv_acc and price_break and vol_break
    return passed, f"박스권:{box_range}, OBV매집:{obv_acc}, 가격돌파:{price_break}, 거래량:{vol_break}"

def classify_bb_state(row) -> str:
    """볼린저밴드 상태 분류"""
    bb40w  = float(row.get('BB40_Width', 99) or 99)
    bb20w  = float(row.get('BB20_Width', 99) or 99)
    pct_b  = float(row.get('BB40_PercentB', 0.5) or 0.5)

    if bb40w <= 3:   return "💎극강응축(BB40≤3)"
    if bb40w <= 5:   return "💎강응축(BB40≤5)"
    if bb40w <= 10:  return "🔋응축중(BB40≤10)"
    if pct_b >= 0.9: return "🚀BB상단돌파권"
    if pct_b <= 0.1: return "📍BB하단근접"
    return f"➖BB보통({bb40w:.1f})"

def classify_obv_trend(row) -> str:
    """OBV 추세 분류"""
    slope = float(row.get('OBV_Slope', 0) or 0)
    obv_r = bool(row.get('OBV_Rising', False))
    obv_b = bool(row.get('OBV_Bullish', False))
    if slope > 20 and obv_r and obv_b:  return "📊OBV강매집(3중확인)"
    if slope > 5 and obv_r:             return "📊OBV매집중"
    if slope > 0:                        return "📊OBV소폭상승"
    if slope < -10:                      return "📉OBV강분산"
    if slope < 0:                        return "📉OBV분산중"
    return "➖OBV보합"

def classify_supply_state(row) -> str:
    """수급 상태 분류 (enrich 후 사용)"""
    supply = str(row.get('수급', '') or '')
    maejip = int(row.get('매집', '0/5').split('/')[0] if '/' in str(row.get('매집','0/5')) else 0)
    if '쌍끌' in supply:      return "🤝쌍끌매수"
    if '기관' in supply:      return "🔴기관매수"
    if '외인' in supply:      return "🔵외인매수"
    if maejip >= 4:            return "🐋세력매집강"
    if maejip >= 3:            return "🐋세력매집"
    return "➖수급보통"

def calc_atr_targets(row: pd.Series, close: float) -> dict:
    """
    ATR 기반 동적 목표가/손절가 계산.
    1차 목표: 현재가 + ATR × 2
    2차 목표: 현재가 + ATR × 3.5
    손절:     현재가 - ATR × 1.5  (역매공파 -5% 기준과 취사선택)
    """
    atr = float(row.get('ATR', 0) or 0)
    if atr <= 0:
        return {}

    return {
        'atr_val':    round(atr),
        'target_1':   round(close + atr * 2),     # 1차 목표
        'target_2':   round(close + atr * 3.5),   # 2차 목표
        'stop_atr':   round(close - atr * 1.5),   # ATR 손절
        'risk_reward': round((atr * 2) / (atr * 1.5), 1),  # 기본 RR비율
    }
