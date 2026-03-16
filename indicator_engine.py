import json
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import mplfinance as mpf
import matplotlib.pyplot as plt
import os, re, time, pytz
from bs4 import BeautifulSoup
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from bs4 import BeautifulSoup 
import pytz
from tactics_engine import get_global_and_leader_status, analyze_all_narratives, get_dynamic_sector_leaders, calculate_dante_symmetry, watermelon_indicator_complete, judge_yeok_break_sequence_v2
from triangle_combo_analyzer import jongbe_triangle_combo_v3
import traceback
from pykrx import stock
import pandas as pd
from datetime import datetime
from auto_theme_news import analyze_market_issues
from functools import lru_cache  # ✅ FIX 1: 캐시용

import io
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

RECENT_AVG_AMOUNT_1 = 150
RECENT_AVG_AMOUNT_2 = 350
ROSS_BAND_TOLERANCE = 1.03
RSI_LOW_TOLERANCE   = 1.03

def check_ross(curr: pd.Series, past: pd.DataFrame):
    if past.empty or past['BB_LOW'].isna().all():
        return False, "과거 데이터 부족"
    bb_low = past['BB_LOW']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False, "1차 저점 없음"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]
    rebound = (after_first['Close'] > after_first['BB_LOW']).any()
    near_band = curr['Low'] <= curr['BB_LOW'] * ROSS_BAND_TOLERANCE
    close_above = curr['Close'] > curr['BB_LOW']
    passed = rebound and near_band and close_above
    return passed, f"반등:{rebound}, 저가밴드근접:{near_band}, 종가밴드위:{close_above}"

def check_rsi_div(curr: pd.Series, past: pd.DataFrame):
    if past['RSI'].isna().all() or pd.isna(curr['RSI']):
        return False, "RSI 데이터 부족"
    min_price_past = past['Low'].min()
    min_rsi_past = past['RSI'].min()
    price_similar = curr['Low'] <= min_price_past * RSI_LOW_TOLERANCE
    rsi_higher = curr['RSI'] > min_rsi_past
    return price_similar and rsi_higher, f"주가저점:{curr['Low']:.0f}(과거:{min_price_past:.0f}), RSI:{curr['RSI']:.1f}(과거:{min_rsi_past:.1f})"

def check_bb40_ross(curr: pd.Series, past: pd.DataFrame):
    """
    BB40 하단 이탈 후 재안착 판단
    - 과거 구간에서 BB40_Lower 하향 이탈이 있었는지
    - 이후 다시 BB40_Lower 위로 복귀한 적이 있는지
    - 현재봉이 BB40_Lower 근처에서 종가 기준 위에 안착했는지
    """
    if past.empty or 'BB40_Lower' not in past.columns or past['BB40_Lower'].isna().all():
        return False, "BB40 데이터 부족"

    bb40_low = past['BB40_Lower']
    outside_mask = past['Low'] < bb40_low

    if not outside_mask.any():
        return False, "BB40 1차 저점 없음"

    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]

    if after_first.empty:
        return False, "BB40 반등 확인 구간 부족"

    rebound = (after_first['Close'] > after_first['BB40_Lower']).any()
    near_band = curr['Low'] <= curr['BB40_Lower'] * ROSS_BAND_TOLERANCE
    close_above = curr['Close'] > curr['BB40_Lower']

    passed = rebound and near_band and close_above
    return passed, f"BB40반등:{rebound}, 저가밴드근접:{near_band}, 종가밴드위:{close_above}"


def check_bb40_rsi_div(curr: pd.Series, past: pd.DataFrame):
    """
    BB40 관점 RSI 다이버전스
    - 과거 BB40 하단 이탈 봉들만 후보로 봄
    - 현재 저점이 과거 저점 부근이거나 더 낮고
    - RSI는 과거보다 높으면 다이버전스로 판단
    """
    if past.empty or 'BB40_Lower' not in past.columns or past['RSI'].isna().all() or pd.isna(curr['RSI']):
        return False, "RSI 데이터 부족"

    bb40_break_df = past[past['Low'] < past['BB40_Lower']].copy()

    if bb40_break_df.empty:
        return False, "BB40 하단 이탈 이력 없음"

    min_price_idx = bb40_break_df['Low'].idxmin()
    min_price_past = bb40_break_df.loc[min_price_idx, 'Low']
    min_rsi_past = bb40_break_df.loc[min_price_idx, 'RSI']

    if pd.isna(min_rsi_past):
        min_rsi_past = bb40_break_df['RSI'].min()

    price_similar = curr['Low'] <= min_price_past * RSI_LOW_TOLERANCE
    rsi_higher = curr['RSI'] > min_rsi_past

    passed = price_similar and rsi_higher
    return passed, f"BB40저점:{curr['Low']:.0f}(과거:{min_price_past:.0f}), RSI:{curr['RSI']:.1f}(과거:{min_rsi_past:.1f})"


def check_bb40_reclaim_rsi_div(curr: pd.Series, past: pd.DataFrame):
    """
    최종 결합형:
    BB40 하단 이탈 후 재안착 + RSI DIV
    """
    bb40_ross, ross_msg = check_bb40_ross(curr, past)
    bb40_div, div_msg = check_bb40_rsi_div(curr, past)

    passed = bb40_ross and bb40_div
    return passed, f"[BB40_Ross] {ross_msg} | [BB40_RSI_DIV] {div_msg}"

def check_good_ma_convergence(curr: pd.Series, past: pd.DataFrame):
    """
    좋은 MA 수렴:
    1. MA20/40/60 서로 가깝다
    2. MA20/40 기울기가 꺾이지 않음
    3. 종가가 수렴대 너무 아래에 있지 않음
    4. BB40 수축 동반
    """
    try:
        ma20 = curr['MA20']
        ma40 = curr['MA40']
        ma60 = curr['MA60']

        if pd.isna(ma20) or pd.isna(ma40) or pd.isna(ma60):
            return False, {"score": 0, "msg": "MA 데이터 부족"}

        gap_20_40 = abs(ma20 - ma40) / (ma40 + 1e-9)
        gap_40_60 = abs(ma40 - ma60) / (ma60 + 1e-9)
        gap_20_60 = abs(ma20 - ma60) / (ma60 + 1e-9)
        cond_gap = (gap_20_40 <= 0.025) and (gap_40_60 <= 0.025) and (gap_20_60 <= 0.035)

        if len(past) >= 5:
            ma20_prev = past['MA20'].iloc[-5]
            ma40_prev = past['MA40'].iloc[-5]
        else:
            ma20_prev, ma40_prev = ma20, ma40

        ma20_slope = (ma20 - ma20_prev) / (ma20_prev + 1e-9)
        ma40_slope = (ma40 - ma40_prev) / (ma40_prev + 1e-9)
        cond_slope = (ma20_slope >= -0.003) and (ma40_slope >= -0.003)

        close = curr['Close']
        convergence_bottom = min(ma20, ma40, ma60)
        convergence_top = max(ma20, ma40, ma60)

        cond_price_zone = close >= convergence_bottom * 0.98
        cond_not_too_far = close <= convergence_top * 1.06

        bb40_width = curr['BB40_Width'] if 'BB40_Width' in curr.index else 999
        cond_bb_squeeze = bb40_width <= 14

        score = 0
        if cond_gap:
            score += 35
        if cond_slope:
            score += 20
        if cond_price_zone:
            score += 15
        if cond_not_too_far:
            score += 10
        if cond_bb_squeeze:
            score += 20
        if bb40_width <= 10:
            score += 10

        passed = cond_gap and cond_slope and cond_price_zone and cond_not_too_far and cond_bb_squeeze

        msg = (
            f"gap20_40:{gap_20_40:.3f} gap40_60:{gap_40_60:.3f} gap20_60:{gap_20_60:.3f} | "
            f"ma20기울기:{ma20_slope:+.4f} ma40기울기:{ma40_slope:+.4f} | "
            f"종가:{close:.0f} 수렴하단:{convergence_bottom:.0f} 수렴상단:{convergence_top:.0f} | "
            f"BB40:{bb40_width:.1f}"
        )

        return passed, {"score": score, "msg": msg}

    except Exception as e:
        return False, {"score": 0, "msg": f"오류:{e}"}

def check_ma_convergence_break_ready(curr: pd.Series, past: pd.DataFrame):
    """
    폭발직전 수렴:
    1. 좋은 수렴이 이미 형성
    2. 종가가 수렴대 상단 근처 또는 그 위
    3. BB40 수축이 유지
    4. 거래량이 너무 죽지 않음
    """
    try:
        good_conv, good_info = check_good_ma_convergence(curr, past)

        ma20 = curr['MA20']
        ma40 = curr['MA40']
        ma60 = curr['MA60']
        close = curr['Close']
        volume = curr['Volume']
        vol_avg = curr['Vol_Avg']

        convergence_top = max(ma20, ma40, ma60)

        cond_price_break_ready = close >= convergence_top * 0.995
        cond_not_overheat = close <= convergence_top * 1.05

        bb40_width = curr['BB40_Width'] if 'BB40_Width' in curr.index else 999
        cond_bb_squeeze = bb40_width <= 12

        cond_volume_alive = volume >= vol_avg * 0.8

        score = 0
        if good_conv:
            score += int(good_info.get("score", 0) * 0.5)
        if cond_price_break_ready:
            score += 25
        if cond_not_overheat:
            score += 10
        if cond_bb_squeeze:
            score += 20
        if cond_volume_alive:
            score += 10
        if bb40_width <= 8:
            score += 10

        passed = good_conv and cond_price_break_ready and cond_not_overheat and cond_bb_squeeze

        msg = (
            f"좋은수렴:{good_conv} | "
            f"종가:{close:.0f} 수렴상단:{convergence_top:.0f} | "
            f"돌파준비:{cond_price_break_ready} 과열아님:{cond_not_overheat} | "
            f"BB40:{bb40_width:.1f} 거래량유지:{cond_volume_alive}"
        )

        return passed, {"score": score, "msg": msg}

    except Exception as e:
        return False, {"score": 0, "msg": f"오류:{e}"}

# ---------------------------------------------------------
# 📈 [4] 기술적 분석 지표
# ---------------------------------------------------------
def get_indicators(df):
    df = df.copy()
    count = len(df)

    recent_avg_amount = (df['Close'] * df['Volume']).tail(5).mean() / 100_000_000
    ma20_amount       = (df['Close'] * df['Volume']).tail(20).mean() / 100_000_000

    amount_ok = (
        (recent_avg_amount >= RECENT_AVG_AMOUNT_1 and recent_avg_amount >= ma20_amount * 1.5)
        or recent_avg_amount >= RECENT_AVG_AMOUNT_2
    )
    if not amount_ok:
        return None

    high  = df['High']
    low   = df['Low']
    close = df['Close']

    for n in [5, 10, 20, 40, 60, 112, 224, 448]:
        df[f'MA{n}']    = close.rolling(window=min(count, n)).mean()
        df[f'VMA{n}']   = df['Volume'].rolling(window=min(count, n)).mean()
        df[f'Slope{n}'] = (df[f'MA{n}'] - df[f'MA{n}'].shift(3)) / df[f'MA{n}'].shift(3) * 100

    df['MA20_slope'] = (df['MA20'] - df['MA20'].shift(5)) / (df['MA20'].shift(5) + 1e-9) * 100
    df['MA40_slope'] = (df['MA40'] - df['MA40'].shift(5)) / (df['MA40'].shift(5) + 1e-9) * 100

    std20 = close.rolling(20).std()
    std40 = close.rolling(40).std()

    df['BB_Upper']      = df['MA20'] + std20 * 2
    df['BB_Lower']      = df['MA20'] - std20 * 2
    df['BB20_Width']    = std20 * 4 / df['MA20'] * 100
    df['BB40_Upper']    = df['MA40'] + std40 * 2
    df['BB40_Lower']    = df['MA40'] - std40 * 2
    df['BB40_Width']    = std40 * 4 / df['MA40'] * 100
    df['BB40_PercentB'] = (close - df['BB40_Lower']) / (df['BB40_Upper'] - df['BB40_Lower'])
    df['BB_UP']  = df['BB40_Upper']
    df['BB_LOW'] = df['BB_Lower']

    df['Disparity']      = (close / df['MA20']) * 100
    df['MA_Convergence'] = abs(df['MA20'] - df['MA60']) / df['MA60'] * 100
    df['Box_Range']      = high.rolling(10).max() / low.rolling(10).min()

    tr = pd.concat([
        high - low,
        abs(high - close.shift(1)),
        abs(low  - close.shift(1))
    ], axis=1).max(axis=1)

    dm_plus  = (high - high.shift(1)).clip(lower=0)
    dm_minus = (low.shift(1) - low).clip(lower=0)
    tr14     = tr.rolling(14).sum()

    df['pDI'] = dm_plus.rolling(14).sum()  / tr14 * 100
    df['mDI'] = dm_minus.rolling(14).sum() / tr14 * 100
    df['ADX'] = ((abs(df['pDI'] - df['mDI']) / (df['pDI'] + df['mDI'])) * 100).rolling(14).mean()

    df['ATR']            = tr.rolling(14).mean()
    df['ATR_MA20']       = df['ATR'].rolling(20).mean()
    df['ATR_Below_MA']   = (df['ATR'] < df['ATR_MA20']).astype(int)
    df['ATR_Below_Days'] = df['ATR_Below_MA'].rolling(10).sum()

    df['Tenkan_sen'] = (high.rolling(9).max()  + low.rolling(9).min())  / 2
    df['Kijun_sen']  = (high.rolling(26).max() + low.rolling(26).min()) / 2
    df['Span_A']     = ((df['Tenkan_sen'] + df['Kijun_sen']) / 2).shift(26)
    df['Span_B']     = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    df['Cloud_Top']  = df[['Span_A', 'Span_B']].max(axis=1)

    l_min, h_max = low.rolling(12).min(), high.rolling(12).max()
    df['Sto_K']  = (close - l_min) / (h_max - l_min) * 100
    df['Sto_D']  = df['Sto_K'].rolling(5).mean()
    df['Sto_SD'] = df['Sto_D'].rolling(5).mean()

    ema12             = close.ewm(span=12).mean()
    ema26             = close.ewm(span=26).mean()
    df['MACD']        = ema12 - ema26
    df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
    df['MACD_Hist']   = df['MACD'] - df['MACD_Signal']

    df['OBV']         = (np.sign(close.diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_MA10']    = df['OBV'].rolling(10).mean()
    df['OBV_Rising']  = df['OBV'] > df['OBV_MA10']
    df['OBV_Slope']   = (df['OBV'] - df['OBV'].shift(5)) / df['OBV'].shift(5).abs() * 100
    df['OBV_Bullish'] = df['OBV_MA10'] > df['OBV_MA10'].shift(1)
    df['Base_Line']   = close.rolling(20).min().shift(5)

    delta      = close.diff()
    gain       = delta.where(delta > 0, 0).ewm(com=13, adjust=False).mean()
    loss       = (-delta.where(delta < 0, 0)).ewm(com=13, adjust=False).mean()
    df['RSI']  = 100 - (100 / (1 + gain / loss))

    typical_price     = (high + low + close) / 3
    money_flow        = typical_price * df['Volume']
    pos_flow          = money_flow.where(typical_price > typical_price.shift(1), 0).rolling(14).sum()
    neg_flow          = money_flow.where(typical_price < typical_price.shift(1), 0).rolling(14).sum()
    df['MFI']             = 100 - (100 / (1 + pos_flow / neg_flow))
    df['MFI_Strong']      = df['MFI'] > 50
    df['MFI_Prev5']       = df['MFI'].shift(5)
    df['MFI_Above50']     = df['MFI_Strong'].astype(int)
    df['MFI_Strong_Days'] = df['MFI_Above50'].rolling(10).sum()
    df['MFI_10d_ago']     = df['MFI'].shift(10)

    df['Buy_Power']       = df['Volume'] * (close - df['Open'])
    df['Buy_Power_MA']    = df['Buy_Power'].rolling(10).mean()
    df['Buying_Pressure'] = df['Buy_Power'] > df['Buy_Power_MA']

    df['Vol_Avg'] = df['Volume'].rolling(20).mean()
    vol_avg20     = df['Vol_Avg']

    df['MA60_Slope']      = df['MA60'].diff()
    df['MA112_Slope']     = df['MA112'].diff()
    df['Dist_to_MA112']   = (df['MA112'] - close) / close
    df['Near_MA112']      = abs(close - df['MA112']) / df['MA112'] * 100
    df['Below_MA112']     = (df['Close'] < df['MA112']).astype(int)
    df['Below_MA112_60d'] = df['Below_MA112'].rolling(60).sum()

    df['MA224'] = df['MA224'].ffill().fillna(0)

    is_above_series       = close > df['MA224']
    df['Trend_Group']     = is_above_series.astype(int).diff().fillna(0).ne(0).cumsum()
    df['Below_MA224']     = (~is_above_series).astype(int)
    df['Below_MA224_60d'] = df['Below_MA224'].rolling(60).sum()

    vol_power_series = df['Volume'] / vol_avg20
    is_above_ma224   = close > df['MA224']

    near_band_low = (low - df['MA224']).abs() / df['MA224'] < 0.03
    local_min     = low == low.rolling(5, center=True, min_periods=1).min()
    double_bottom_series = (near_band_low & local_min).rolling(30).sum() >= 2

    df['Dolbanzi']       = (vol_power_series >= 3.0) & is_above_ma224 & double_bottom_series
    df['Dolbanzi_Count'] = df.groupby('Trend_Group')['Dolbanzi'].cumsum()

    df['VWMA40']           = (close * df['Volume']).rolling(40).mean() / df['Volume'].rolling(40).mean()
    df['Vol_Accel']        = df['Volume'] / df['Volume'].rolling(5).mean()
    df['Watermelon_Fire']  = (close / df['VWMA40'] - 1) * 100 * df['Vol_Accel']
    df['Watermelon_Green'] = (close > df['VWMA40']) & (df['BB40_Width'] < 10)
    df['Watermelon_Red']   = df['Watermelon_Green'] & (df['Watermelon_Fire'] > 5.0)
    df['Watermelon_Red2']  = (close > df['VWMA40']) & (close >= df['Open'])

    red_score = (
        df['OBV_Rising'].astype(int) +
        df['MFI_Strong'].astype(int) +
        df['Buying_Pressure'].astype(int)
    )
    df['Watermelon_Score'] = red_score
    df['Watermelon_Color'] = np.where(red_score >= 2, 'red', 'green')

    color_change            = (df['Watermelon_Color'] == 'red') & (df['Watermelon_Color'].shift(1) == 'green')
    df['Green_Days_10']     = (df['Watermelon_Color'].shift(1) == 'green').rolling(10).sum()
    volume_surge            = df['Volume'] >= vol_avg20 * 1.2
    df['Watermelon_Signal'] = color_change & (df['Green_Days_10'] >= 7) & volume_surge

    for col in [
        'BB_Ross', 'RSI_DIV',
        'BB40_Ross', 'BB40_RSI_DIV', 'BB40_Reclaim_RSI_DIV',
        'Force_Pullback', 'BB40_Second_Wave', 'Watermelon_Relaunch', 'OBV_Acc_Breakout',
        'Was_Panic', 'Is_bb_low_Stable', 'Has_Accumulation', 'Is_Rsi_Divergence'
    ]:
        df[col] = False

    df_signal = df.dropna(subset=['BB_UP', 'BB_LOW', 'BB40_Lower', 'RSI']).copy()
    if len(df_signal) > 51:
        curr_s  = df_signal.iloc[-1]
        past    = df_signal.iloc[-21:-1]
        past_50 = df_signal.iloc[-51:-1]

        ross, _ = check_ross(curr_s, past)
        rsi_div, _ = check_rsi_div(curr_s, past)

        bb40_ross, _ = check_bb40_ross(curr_s, past)
        bb40_rsi_div, _ = check_bb40_rsi_div(curr_s, past)
        bb40_combo, _ = check_bb40_reclaim_rsi_div(curr_s, past)
        force_pullback, _ = check_force_pullback(curr_s, past_50)
        bb40_second_wave, _ = check_bb40_second_wave(curr_s, past_50)
        watermelon_relaunch, _ = check_watermelon_relaunch(curr_s, past_50)
        obv_acc_breakout, _ = check_obv_acc_breakout(curr_s, past_50)

        was_panic         = (past_50['Low'] < past_50['BB_LOW']).any()
        is_bb_low_stable  = curr_s['Low'] > curr_s['BB_LOW']
        is_rsi_divergence = curr_s['RSI'] > past_50['RSI'].min()
        has_accumulation  = (past_50['Volume'] > (past_50['Vol_Avg'] * 3)).any()

        idx = df.index[-1]
        df.at[idx, 'BB_Ross']              = ross
        df.at[idx, 'RSI_DIV']              = rsi_div
        df.at[idx, 'BB40_Ross']            = bb40_ross
        df.at[idx, 'BB40_RSI_DIV']         = bb40_rsi_div
        df.at[idx, 'BB40_Reclaim_RSI_DIV'] = bb40_combo
        df.at[idx, 'Was_Panic']            = was_panic
        df.at[idx, 'Is_bb_low_Stable']     = is_bb_low_stable
        df.at[idx, 'Is_Rsi_Divergence']    = is_rsi_divergence
        df.at[idx, 'Has_Accumulation']     = has_accumulation
        df.at[idx, 'Force_Pullback']      = force_pullback
        df.at[idx, 'BB40_Second_Wave']    = bb40_second_wave
        df.at[idx, 'Watermelon_Relaunch'] = watermelon_relaunch
        df.at[idx, 'OBV_Acc_Breakout']    = obv_acc_breakout


    prev = df.iloc[-2]
    curr = df.iloc[-1]

    cond_golden_cross = (prev['MA5'] < prev['MA112']) and (curr['MA5'] >= curr['MA112'])
    cond_approaching  = (prev['MA5'] < prev['MA112']) and (curr['MA112'] * 0.98 <= curr['MA5'] <= curr['MA112'] * 1.03)
    cond_cross        = cond_golden_cross or cond_approaching

    cond_inverse_mid = curr['MA112'] < curr['MA224']
    cond_below_448   = curr['Close'] < curr['MA448']
    cond_ma224_range = -3 <= ((curr['Close'] - curr['MA224']) / curr['MA224']) * 100 <= 5
    cond_bb40_range  = -7 <= ((curr['Close'] - curr['BB40_Upper']) / curr['BB40_Upper']) * 100 <= 3

    vol_ratio      = df['Volume'] / df['Volume'].shift(1).replace(0, np.nan)
    cond_vol_300   = (vol_ratio >= 3.0).iloc[-50:].any()
    cond_break_448 = (df['High'] > df['MA448']).iloc[-50:].any()

    df['Is_Real_Watermelon'] = False
    if cond_cross and cond_inverse_mid and cond_below_448 and cond_ma224_range and cond_bb40_range and cond_break_448 and cond_vol_300:
        df.at[df.index[-1], 'Is_Real_Watermelon'] = True

    resistances = df[['BB_Upper', 'BB40_Upper', 'MA60', 'MA112']]
    touch_count = pd.DataFrame({
        col: (close < df[col]) & (high >= df[col] * 0.995)
        for col in ['BB_Upper', 'BB40_Upper', 'MA60', 'MA112']
        if col in df.columns
    }).sum(axis=1)
    df['Daily_Touch']     = touch_count
    df['Total_hammering'] = df['Daily_Touch'].rolling(20).sum().fillna(0).astype(int)

    current_res_max = max(curr['BB_Upper'], curr['BB40_Upper'], curr['MA60'], curr['MA112'])
    df['Is_resistance_break'] = curr['Close'] > current_res_max

    df['Is_Maejip'] = (
        (df['Volume'] > df['Volume'].shift(1) * 2) &
        (df['Close'] > df['Open']) &
        (df['Close'] > df['Close'].shift(1))
    )
    df['Maejip_Count'] = df['Is_Maejip'].rolling(20).sum().fillna(0).astype(int)

    max_ma      = df[['MA5', 'MA10', 'MA20']].max(axis=1)
    min_ma      = df[['MA5', 'MA10', 'MA20']].min(axis=1)
    is_squeezed = (max_ma - min_ma) / min_ma <= 0.03

    was_below_20 = (close < df['MA20']).astype(int).rolling(10).max() == 1
    is_slope_up  = df['MA5'] > df['MA5'].shift(1)
    is_head_up   = is_slope_up & (df['MA5'] >= df['MA20'] * 0.99)

    df['Viper_Hook'] = is_squeezed & was_below_20 & is_head_up

    is_heading_ceiling     = (close < df['MA112']) & (df['MA112_Slope'] < 0) & (df['Dist_to_MA112'] <= 0.04)
    df['is_not_blocked']   = ~is_heading_ceiling
    df['is_not_waterfall'] = df['MA112'] >= df['MA224'] * 0.9
    df['is_ma60_safe']     = df['MA60_Slope'] >= 0

    df['Dist_from_MA5']  = (close - df['MA5']) / df['MA5']
    df['is_hugging_ma5'] = df['Dist_from_MA5'] < 0.08

    df['recent_high_10d'] = df['High'].rolling(10).max().shift(1)
    is_hitting_wall       = abs(df['recent_high_10d'] - close) / close < 0.02
    is_breaking_high      = close > df['recent_high_10d']
    df['is_not_double_top'] = ~(is_hitting_wall & ~is_breaking_high)

    df['Real_Viper_Hook'] = (
        df['Viper_Hook'] &
        df['is_not_blocked'] &
        df['is_not_waterfall'] &
        df['is_ma60_safe'] &
        df['is_hugging_ma5'] &
        df['is_not_double_top']
    )

    df['was_broken_20']  = (close < df['MA20']).rolling(5).max() == 1
    df['lowest_vol_5d']  = df['Volume'].rolling(5).min()
    df['is_fake_drop']   = df['lowest_vol_5d'] < (vol_avg20 * 0.5)
    df['obv_divergence'] = (close < close.shift(5)) & (df['OBV'] >= df['OBV'].shift(5))
    df['reclaim_20']     = (close > df['MA20']) & (close > df['Open']) & (df['Volume'] > df['Volume'].shift(1))

    df['Golpagi_Trap'] = (
        df['was_broken_20'] &
        (df['is_fake_drop'] & df['obv_divergence']) &
        df['reclaim_20']
    )

    gap_ratio    = abs(curr['MA20'] - curr['MA40']) / (curr['MA40'] + 1e-9)
    cross_series = (df['MA20'] > df['MA40']) & (df['MA20'].shift(1) <= df['MA40'].shift(1))
    cross_recent = cross_series.iloc[-5:].any()
    cross_near   = (curr['MA20'] > curr['MA40']) and (gap_ratio < 0.03)

    ma20_rising = curr['MA20_slope'] > 0
    ma40_rising = curr['MA40_slope'] > -0.05
    ma20_accel  = curr['MA20_slope'] > df['MA20_slope'].rolling(3).mean().iloc[-2]

    jongbe_value = (
        (cross_recent or cross_near) and
        ma20_rising and
        ma40_rising and
        ma20_accel and
        curr['Close'] > curr['MA20']
    )
    df['Jongbe_Break'] = False
    df.at[df.index[-1], 'Jongbe_Break'] = jongbe_value

    print("✅ 최종판독 완료")
    return df
