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
from news_sentiment import get_news_sentiment
from pykrx import stock
import pandas as pd
from datetime import datetime
from auto_theme_news import analyze_market_issues
from functools import lru_cache  # ✅ FIX 1: 캐시용
from Watermelonchart import create_watermelon_charts_for_hits
try: from openai import OpenAI
except: OpenAI = None

from google_sheet_manager import update_google_sheet, update_ai_briefing_sheet
import io
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

RECENT_AVG_AMOUNT_1 = 150
RECENT_AVG_AMOUNT_2 = 350
ROSS_BAND_TOLERANCE = 1.03
RSI_LOW_TOLERANCE   = 1.03

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
