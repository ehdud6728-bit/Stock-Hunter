# -*- coding: utf-8 -*-
# main7 원본 엔진 연동 최종본: 단일종목 분석기가 main7 검색엔진을 직접 참조합니다.
import argparse
import html
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import FinanceDataReader as fdr
import numpy as np
import pandas as pd
import pytz

try:
    from dante_3phase_v4_module import (
        apply_dante_v4,
        build_v4_signal_map,
        apply_fear_and_quality_bonus,
        build_pre_dolbanji_bundle,
        build_pre_dolbanji_lite_bundle,
        build_pre_dolbanji_hts_exact_bundle,
    )
except Exception:
    def apply_dante_v4(df: pd.DataFrame) -> pd.DataFrame:
        return df

    def build_v4_signal_map(row: pd.Series) -> Dict[str, object]:
        return {}

    def apply_fear_and_quality_bonus(base_score: float, row: pd.Series) -> float:
        return float(base_score)

    def build_pre_dolbanji_bundle(df: pd.DataFrame) -> Dict[str, object]:
        return {
            "pre_dolbanji": False,
            "pre_dolbanji_confirmed": False,
            "pre_dolbanji_score": 0,
            "pre_dolbanji_grade": "없음",
            "pre_dolbanji_tags": [],
            "pre_dolbanji_best": "",
            "pre_dolbanji_detail": {},
        }

    def build_pre_dolbanji_lite_bundle(df: pd.DataFrame) -> Dict[str, object]:
        return {
            "pre_dolbanji_lite": False,
            "pre_dolbanji_lite_confirmed": False,
            "pre_dolbanji_lite_score": 0,
            "pre_dolbanji_lite_grade": "없음",
            "pre_dolbanji_lite_tags": [],
            "pre_dolbanji_lite_best": "",
            "pre_dolbanji_lite_detail": {},
        }

    def build_pre_dolbanji_hts_exact_bundle(df: pd.DataFrame) -> Dict[str, object]:
        return {
            "pre_dolbanji_hts_exact": False,
            "pre_dolbanji_hts_exact_score": 0,
            "pre_dolbanji_hts_exact_max_score": 10,
            "pre_dolbanji_hts_exact_tags": [],
            "pre_dolbanji_hts_exact_detail": {},
        }

try:
    from main7_bugfix_2 import (
        get_indicators as main7_get_indicators,
        build_ma_convergence_comment_from_row as main7_build_ma_convergence_comment_from_row,
        analyze_single_stock_with_main7_engine,
    )
except Exception:
    main7_get_indicators = None
    main7_build_ma_convergence_comment_from_row = None

    def analyze_single_stock_with_main7_engine(ticker: str, name: str, end_date: str = "", context=None):
        return []

KST = pytz.timezone("Asia/Seoul")


MIN_PRICE = 3_000
MIN_AMOUNT_B = 30.0

NEAR_HIGH20_MIN = 85.0
NEAR_HIGH20_MAX = 100.0
UPPER_WICK_BODY_MAX = 20.0
VOL_MULT = 2.0
DISPARITY_MIN = 98.0
DISPARITY_MAX = 112.0

ENV20_PCT = 20.0
ENV40_PCT = 40.0
ENV20_NEAR_MIN = -2.0
ENV20_NEAR_MAX = 2.0
ENV40_NEAR_MIN = -10.0
ENV40_NEAR_MAX = 10.0

ULTRA_MA_CONV_MAX = 3.5
SHORT_MA_CONV_MAX = 4.5
STRUCT_MA_CONV_MAX = 8.0
BRIDGE_MA_CONV_MAX = 5.5
CONNECT_MA_CONV_MAX = 6.5

MODE_CHOICES = {
    "all",
    "closing_bet",
    "envelope_bet",
    "dolbanji",
    "watermelon",
    "pre_dolbanji",
    "pre_dolbanji_lite",
    "pre_dolbanji_hts_exact",
    "viper",
    "yeokmae",
    "double_bottom",
}


@dataclass
class CheckRow:
    strategy: str
    label: str
    current: str
    target: str
    ok: bool
    reason: str


@dataclass
class PatternResult:
    key: str
    name: str
    status: str
    score: int
    max_score: int
    subtitle: str
    comment: str
    checks: List[CheckRow]


STATUS_CLASS = {
    "해당": "pass",
    "유사": "warn",
    "미해당": "fail",
    "데이터부족": "na",
}


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, float) and math.isnan(v):
            return default
        return float(v)
    except Exception:
        return default


def safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(round(float(v)))
    except Exception:
        return default


def fmt_int(v: Any) -> str:
    return f"{safe_int(v):,}"


def fmt_float(v: Any, digits: int = 1) -> str:
    try:
        return f"{float(v):.{digits}f}"
    except Exception:
        return str(v)


def escape(v: Any) -> str:
    return html.escape(str(v))


def normalize_code(code: str) -> str:
    digits = re.sub(r"\D", "", code or "")
    return digits.zfill(6)


def detect_name(code: str, given_name: str = "") -> str:
    if given_name and given_name.strip():
        return given_name.strip()
    try:
        from pykrx import stock as pk
        name = pk.get_market_ticker_name(code)
        if name:
            return str(name)
    except Exception:
        pass
    return code


def resolve_analysis_window(
    start_date: str = "",
    end_date: str = "",
    days: int = 450,
    warmup_days: int = 320,
) -> Dict[str, str]:
    """
    target_start/target_end:
      - 사용자가 결과를 보고 싶은 구간
    fetch_start/fetch_end:
      - 실제 지표 계산용 과거 데이터 조회 구간
    """
    target_end = pd.Timestamp(end_date).normalize() if end_date else pd.Timestamp(datetime.now(KST).date())
    target_start = pd.Timestamp(start_date).normalize() if start_date else target_end

    if target_start > target_end:
        raise RuntimeError("start_date는 end_date보다 늦을 수 없습니다.")

    fetch_span_days = max(days, warmup_days)
    fetch_start = target_end - pd.Timedelta(days=fetch_span_days)

    return {
        "target_start": target_start.strftime("%Y-%m-%d"),
        "target_end": target_end.strftime("%Y-%m-%d"),
        "fetch_start": fetch_start.strftime("%Y-%m-%d"),
        "fetch_end": target_end.strftime("%Y-%m-%d"),
    }


def load_price_history(
    code: str,
    start_date: str = "",
    end_date: str = "",
    days: int = 450,
    min_bars: int = 40,
) -> tuple[pd.DataFrame, Dict[str, str]]:
    window = resolve_analysis_window(
        start_date=start_date,
        end_date=end_date,
        days=days,
        warmup_days=320,
    )

    df = fdr.DataReader(code, window["fetch_start"], window["fetch_end"])

    if df is None or df.empty:
        raise RuntimeError(
            f"가격 데이터를 불러오지 못했습니다: {code} "
            f"({window['fetch_start']} ~ {window['fetch_end']})"
        )

    df = df.rename(columns={c: c.capitalize() for c in df.columns})
    needed = ["Open", "High", "Low", "Close", "Volume"]
    for col in needed:
        if col not in df.columns:
            raise RuntimeError(f"필수 컬럼 누락: {col}")

    df = df.dropna(subset=needed).copy()

    # 사용자가 보고 싶은 기준일 이하 데이터만 최종 판정에 사용
    target_end_ts = pd.Timestamp(window["target_end"])
    df = df[df.index <= target_end_ts].copy()

    if df.empty:
        raise RuntimeError(f"기준일({window['target_end']}) 이전 거래 데이터가 없습니다.")

    if len(df) < min_bars:
        raise RuntimeError(
            f"지표 계산에 필요한 최소 거래봉이 부족합니다. "
            f"현재 {len(df)}봉 / 필요 {min_bars}봉 "
            f"(실제 조회: {window['fetch_start']} ~ {window['fetch_end']})"
        )

    return df, window


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
    tp = (df["High"] + df["Low"] + df["Close"]) / 3
    mf = tp * df["Volume"]
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




def _apply_main7_aliases(out: pd.DataFrame) -> pd.DataFrame:
    out = out.copy()
    alias_map = {
        'BB_Upper': 'BB20_UP',
        'BB_Lower': 'BB20_DN',
        'BB20_Width': 'BB20_WIDTH',
        'BB40_Upper': 'BB40_UP',
        'BB40_Lower': 'BB40_DN',
        'BB40_Width': 'BB40_WIDTH',
        'BB40_PercentB': 'BB40_PCTB',
        'MACD_Signal': 'MACD_SIGNAL',
        'MACD_Hist': 'MACD_HIST',
        'RSI': 'RSI14',
        'MFI': 'MFI14',
        'pDI': 'PLUS_DI',
        'mDI': 'MINUS_DI',
    }
    for src, dst in alias_map.items():
        if src in out.columns and dst not in out.columns:
            out[dst] = out[src]

    if 'MA20' in out.columns and 'BB20_MID' not in out.columns:
        out['BB20_MID'] = out['MA20']
    if 'MA40' in out.columns and 'BB40_MID' not in out.columns:
        out['BB40_MID'] = out['MA40']

    if 'OBV_Slope' in out.columns:
        if 'OBV_SLOPE_5' not in out.columns:
            out['OBV_SLOPE_5'] = out['OBV_Slope']
        if 'OBV_SLOPE_10' not in out.columns:
            out['OBV_SLOPE_10'] = out['OBV_Slope']

    for n in [5, 10, 20, 40, 60]:
        slope_src = f'Slope{n}'
        slope_dst = f'MA{n}_SLOPE'
        if slope_src in out.columns and slope_dst not in out.columns:
            out[slope_dst] = out[slope_src]

    if 'MA200' not in out.columns and 'MA224' in out.columns:
        out['MA200'] = out['MA224']
    if 'Amount' not in out.columns and 'Close' in out.columns and 'Volume' in out.columns:
        out['Amount'] = out['Close'] * out['Volume']
    if 'AmountB' not in out.columns and 'Amount' in out.columns:
        out['AmountB'] = (out['Amount'] / 1e8).round(1)

    if 'Watermelon_Color' in out.columns:
        if 'WATERMELON_GREEN' not in out.columns:
            out['WATERMELON_GREEN'] = (out['Watermelon_Color'] == 'green').astype(int)
        if 'WATERMELON_RED' not in out.columns:
            out['WATERMELON_RED'] = (out['Watermelon_Color'] == 'red').astype(int)

    return out

def _add_indicators_legacy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for n in [5, 10, 20, 40, 60, 112, 200, 224]:
        out[f"MA{n}"] = out["Close"].rolling(n).mean()

    out["VMA5"] = out["Volume"].rolling(5).mean()
    out["VMA20"] = out["Volume"].rolling(20).mean()

    out["Amount"] = out["Close"] * out["Volume"]
    out["AmountB"] = (out["Amount"] / 1e8).round(1)

    out["High20"] = out["High"].rolling(20).max()
    out["High60"] = out["High"].rolling(60).max()
    out["Low20"] = out["Low"].rolling(20).min()
    out["Low60"] = out["Low"].rolling(60).min()

    out["Disparity"] = (out["Close"] / out["MA20"] * 100).round(1)
    out["NearHigh20_Pct"] = (out["Close"] / out["High20"] * 100).round(1)
    out["NearHigh60_Pct"] = (out["Close"] / out["High60"] * 100).round(1)

    tr1 = out["High"] - out["Low"]
    tr2 = (out["High"] - out["Close"].shift(1)).abs()
    tr3 = (out["Low"] - out["Close"].shift(1)).abs()
    out["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out["ATR"] = out["TR"].rolling(14).mean()
    out["ATR_MA20"] = out["ATR"].rolling(20).mean()

    std20 = out["Close"].rolling(20).std(ddof=0)
    std40 = out["Close"].rolling(40).std(ddof=0)

    out["BB20_MID"] = out["MA20"]
    out["BB20_UP"] = out["MA20"] + std20 * 2
    out["BB20_DN"] = out["MA20"] - std20 * 2

    out["BB40_MID"] = out["MA40"]
    out["BB40_UP"] = out["MA40"] + std40 * 2
    out["BB40_DN"] = out["MA40"] - std40 * 2

    out["BB20_WIDTH"] = ((out["BB20_UP"] - out["BB20_DN"]) / out["BB20_MID"] * 100).round(1)
    out["BB40_WIDTH"] = ((out["BB40_UP"] - out["BB40_DN"]) / out["BB40_MID"] * 100).round(1)

    width40_raw = (out["BB40_UP"] - out["BB40_DN"]).replace(0, np.nan)
    out["BB40_PCTB"] = ((out["Close"] - out["BB40_DN"]) / width40_raw).round(3)

    direction = out["Close"].diff().fillna(0)
    obv_delta = pd.Series(0, index=out.index, dtype="float64")
    obv_delta[direction > 0] = out.loc[direction > 0, "Volume"]
    obv_delta[direction < 0] = -out.loc[direction < 0, "Volume"]
    out["OBV"] = obv_delta.cumsum()

    vol5 = out["Volume"].rolling(5).sum().replace(0, np.nan)
    vol10 = out["Volume"].rolling(10).sum().replace(0, np.nan)
    out["OBV_SLOPE_5"] = ((out["OBV"].diff(5) / vol5) * 100).round(2)
    out["OBV_SLOPE_10"] = ((out["OBV"].diff(10) / vol10) * 100).round(2)

    out["RSI14"] = rsi(out["Close"], 14).round(1)
    out["MFI14"] = mfi(out, 14).round(1)

    ema12 = out["Close"].ewm(span=12, adjust=False).mean()
    ema26 = out["Close"].ewm(span=26, adjust=False).mean()
    out["MACD"] = ema12 - ema26
    out["MACD_SIGNAL"] = out["MACD"].ewm(span=9, adjust=False).mean()
    out["MACD_HIST"] = out["MACD"] - out["MACD_SIGNAL"]

    up_move = out["High"].diff()
    down_move = -out["Low"].diff()

    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=out.index,
        dtype="float64",
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=out.index,
        dtype="float64",
    )

    atr14 = out["TR"].rolling(14).mean().replace(0, np.nan)
    out["PLUS_DI"] = (100 * plus_dm.rolling(14).mean() / atr14).fillna(0).round(2)
    out["MINUS_DI"] = (100 * minus_dm.rolling(14).mean() / atr14).fillna(0).round(2)

    dx = ((out["PLUS_DI"] - out["MINUS_DI"]).abs() / (out["PLUS_DI"] + out["MINUS_DI"]).replace(0, np.nan) * 100)
    out["ADX"] = dx.rolling(14).mean().fillna(0).round(2)

    total_range = (out["High"] - out["Low"]).replace(0, np.nan)
    upper_wick_total = (out["High"] - out[["Open", "Close"]].max(axis=1)).clip(lower=0)
    out["UPPER_WICK_TOTAL_PCT"] = (upper_wick_total / total_range).fillna(0).mul(100).round(1)

    out["Green"] = (out["Close"] >= out["Open"]).astype(int)
    out["Green_Days_10"] = out["Green"].rolling(10).sum()

    out["MA5_SLOPE"] = (((out["MA5"] - out["MA5"].shift(3)) / out["MA5"].shift(3)) * 100).round(2)
    out["MA10_SLOPE"] = (((out["MA10"] - out["MA10"].shift(3)) / out["MA10"].shift(3)) * 100).round(2)
    out["MA20_SLOPE"] = (((out["MA20"] - out["MA20"].shift(3)) / out["MA20"].shift(3)) * 100).round(2)
    out["MA60_SLOPE"] = (((out["MA60"] - out["MA60"].shift(5)) / out["MA60"].shift(5)) * 100).round(2)

    out["MA200_GAP_PCT"] = ((out["Close"] - out["MA200"]) / out["MA200"] * 100).round(1)

    mfi_up = (out["MFI14"] > out["MFI14"].shift(1)).fillna(False).astype(int)
    macd_hist_up = (out["MACD_HIST"] > out["MACD_HIST"].shift(1)).fillna(False).astype(int)
    breakout_3d = (out["Close"] > out["High"].shift(1).rolling(3).max()).fillna(False).astype(int)
    vol_ok = (out["Volume"] >= out["VMA20"] * 0.8).fillna(False).astype(int)
    vol_trigger = (out["Volume"] >= out["VMA20"] * 0.9).fillna(False).astype(int)
    atr_quiet = (out["ATR"] <= out["ATR_MA20"]).fillna(False).astype(int)

    green_score = (
        (out["OBV_SLOPE_10"] > 0).astype(int) +
        (out["MFI14"] >= 50).astype(int) +
        mfi_up +
        (out["Close"] >= out["MA20"]).astype(int) +
        (out["Close"] >= out["BB40_MID"]).astype(int) +
        vol_ok +
        atr_quiet
    )

    red_score = (
        macd_hist_up +
        (out["MACD"] >= out["MACD_SIGNAL"]).astype(int) +
        (out["PLUS_DI"] >= out["MINUS_DI"]).astype(int) +
        breakout_3d +
        (out["MA5"] >= out["MA20"]).astype(int) +
        (out["UPPER_WICK_TOTAL_PCT"] <= 35).astype(int) +
        vol_trigger
    )

    green_filter = (
        (out["Disparity"] >= 97) &
        (out["Disparity"] <= 108) &
        (out["BB40_WIDTH"] >= 3) &
        (out["BB40_WIDTH"] <= 24) &
        (out["Close"] >= out["BB40_DN"] * 0.98) &
        (out["Close"] <= out["BB40_MID"] * 1.05)
    ).fillna(False)

    red_filter = (
        (out["Disparity"] >= 98) &
        (out["Disparity"] <= 110) &
        (out["Close"] >= out["BB40_MID"] * 0.98) &
        (out["Close"] <= out["BB40_UP"] * 1.02)
    ).fillna(False)

    out["WATERMELON_GREEN_SCORE"] = green_score.fillna(0).astype(int)
    out["WATERMELON_RED_SCORE"] = red_score.fillna(0).astype(int)

    out["WATERMELON_GREEN"] = ((out["WATERMELON_GREEN_SCORE"] >= 4) & green_filter).astype(int)
    out["WATERMELON_RED"] = (
        (out["WATERMELON_GREEN"] == 1) &
        (out["WATERMELON_RED_SCORE"] >= 4) &
        red_filter
    ).astype(int)

    out["WATERMELON_VALUE"] = 0
    out.loc[out["WATERMELON_GREEN"] == 1, "WATERMELON_VALUE"] = 1
    out.loc[out["WATERMELON_RED"] == 1, "WATERMELON_VALUE"] = 2

    green_days = []
    red_days = []
    g_cnt = 0
    r_cnt = 0
    for g, r in zip(out["WATERMELON_GREEN"].tolist(), out["WATERMELON_RED"].tolist()):
        g_cnt = g_cnt + 1 if g == 1 else 0
        r_cnt = r_cnt + 1 if r == 1 else 0
        green_days.append(g_cnt)
        red_days.append(r_cnt)

    out["WATERMELON_GREEN_DAYS"] = green_days
    out["WATERMELON_RED_DAYS"] = red_days

    out["WATERMELON_GREEN_NEW"] = (
        (out["WATERMELON_GREEN"] == 1) &
        (out["WATERMELON_GREEN"].shift(1).fillna(0) == 0)
    ).astype(int)

    out["WATERMELON_RED_NEW"] = (
        (out["WATERMELON_RED"] == 1) &
        (out["WATERMELON_RED"].shift(1).fillna(0) == 0)
    ).astype(int)

    out["WATERMELON_QUALITY"] = (
        out["WATERMELON_GREEN_SCORE"] * 0.4 +
        out["WATERMELON_RED_SCORE"] * 0.6 +
        np.where(out["WATERMELON_RED_NEW"] == 1, 1.5, 0.0) +
        np.where(out["OBV_SLOPE_10"] > 0, 0.5, 0.0) +
        np.where(out["MFI14"] >= 55, 0.5, 0.0) +
        np.where(out["PLUS_DI"] > out["MINUS_DI"], 0.5, 0.0)
    ).round(2)

    return out




def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if main7_get_indicators is not None:
        try:
            out = main7_get_indicators(df.copy())
            if out is None or out.empty:
                raise RuntimeError('main7_get_indicators empty')
            out = apply_dante_v4(out)
            out = _apply_main7_aliases(out)
            return out
        except Exception:
            pass

    out = _add_indicators_legacy(df)
    out = apply_dante_v4(out)
    return out

def calc_envelope(df: pd.DataFrame, period: int, pct: float) -> Dict[str, pd.Series]:
    ma = df["Close"].rolling(period).mean()
    upper = ma * (1 + pct / 100)
    lower = ma * (1 - pct / 100)
    return {"ma": ma, "upper": upper, "lower": lower}


def calc_upper_wick_body_pct(row: pd.Series) -> float:
    high_p = safe_float(row.get("High"))
    open_p = safe_float(row.get("Open"))
    close_p = safe_float(row.get("Close"))
    body_top = max(open_p, close_p)
    body_size = max(abs(close_p - open_p), 1e-9)
    upper_wick = max(0.0, high_p - body_top)
    return round(upper_wick / body_size * 100, 1)


def calc_lower_wick_body_pct(row: pd.Series) -> float:
    low_p = safe_float(row.get("Low"))
    open_p = safe_float(row.get("Open"))
    close_p = safe_float(row.get("Close"))
    body_bot = min(open_p, close_p)
    body_size = max(abs(close_p - open_p), 1e-9)
    lower_wick = max(0.0, body_bot - low_p)
    return round(lower_wick / body_size * 100, 1)


def check_envelope_bottom(row: pd.Series, df: pd.DataFrame) -> Dict[str, Any]:
    close = safe_float(row.get("Close"))
    env20 = calc_envelope(df, 20, ENV20_PCT)
    env40 = calc_envelope(df, 40, ENV40_PCT)

    lower20 = safe_float(env20["lower"].iloc[-1])
    lower40 = safe_float(env40["lower"].iloc[-1])

    env20_pct = round((close - lower20) / lower20 * 100, 1) if lower20 > 0 else 0.0
    env40_pct = round((close - lower40) / lower40 * 100, 1) if lower40 > 0 else 0.0

    return {
        "env20_pct": env20_pct,
        "env40_pct": env40_pct,
        "env20_near": ENV20_NEAR_MIN <= env20_pct <= ENV20_NEAR_MAX,
        "env40_near": ENV40_NEAR_MIN <= env40_pct <= ENV40_NEAR_MAX,
        "lower20": round(lower20),
        "lower40": round(lower40),
    }


def find_double_bottom(df: pd.DataFrame, lookback: int = 120) -> Dict[str, Any]:
    sub = df.tail(lookback).copy().reset_index()
    date_col = sub.columns[0]
    if len(sub) < 50:
        return {"found": False}

    lows = []
    for i in range(3, len(sub) - 3):
        win = sub.loc[i - 3:i + 3, "Low"]
        cur = safe_float(sub.loc[i, "Low"])
        if cur <= safe_float(win.min()):
            lows.append(i)

    best = None
    for i in range(len(lows)):
        for j in range(i + 1, len(lows)):
            a, b = lows[i], lows[j]
            gap = b - a
            if gap < 12 or gap > 60:
                continue

            low1 = safe_float(sub.loc[a, "Low"])
            low2 = safe_float(sub.loc[b, "Low"])
            if low1 <= 0 or low2 <= 0:
                continue

            diff_pct = abs(low2 - low1) / min(low1, low2) * 100
            if diff_pct > 8.0:
                continue

            neckline = safe_float(sub.loc[a:b, "High"].max())
            cur_close = safe_float(sub.loc[len(sub) - 1, "Close"])
            strength = 100 - diff_pct - abs(gap - 25) * 0.7

            cand = {
                "found": True,
                "left_idx": a,
                "right_idx": b,
                "left_date": str(pd.to_datetime(sub.loc[a, date_col]).date()),
                "right_date": str(pd.to_datetime(sub.loc[b, date_col]).date()),
                "low1": round(low1),
                "low2": round(low2),
                "diff_pct": round(diff_pct, 1),
                "gap_days": int(gap),
                "neckline": round(neckline),
                "neckline_break": cur_close >= neckline * 0.985,
                "neckline_distance_pct": round((cur_close - neckline) / neckline * 100, 1) if neckline > 0 else 0.0,
                "strength": round(strength, 1),
            }
            if best is None or cand["strength"] > best["strength"]:
                best = cand

    return best or {"found": False}


def calc_triplet_convergence(*values: float) -> float:
    valid = [safe_float(v, -1.0) for v in values]
    valid = [v for v in valid if v > 0]
    if len(valid) < 3:
        return 999.0
    return round((max(valid) - min(valid)) / max(valid) * 100, 2)



def build_ma_convergence_comment(price: Dict[str, Any]) -> str:
    if main7_build_ma_convergence_comment_from_row is not None:
        try:
            proxy = {
                'Is_UltraShort_MA_Conv': bool(price.get('ultra_ma_conv')),
                'Is_Short_MA_Conv': bool(price.get('short_ma_conv')),
                'Is_Structure_MA_Conv': bool(price.get('struct_ma_conv')),
                'Is_Bridge_MA_Conv': bool(price.get('bridge_ma_conv')),
                'Is_Structure_Link_MA_Conv': bool(price.get('connect_ma_conv')),
                'Is_Super_MA_Conv': bool(price.get('super_ma_conv')),
            }
            return main7_build_ma_convergence_comment_from_row(proxy)
        except Exception:
            pass

    comments: List[str] = []

    if price.get("super_ma_conv"):
        comments.append(
            "단기 수렴과 구조 수렴이 함께 잡히는 초강력 응축 구간입니다. 거래량 확산과 BB40 상단 돌파가 붙으면 가장 강한 발사 구간으로 볼 수 있습니다."
        )
    else:
        if price.get("ultra_ma_conv"):
            comments.append(
                "초단기 MA수렴(5/10/20)이 확인됩니다. 아주 짧은 호흡의 응축이라 독사·종가배팅·수박 준비형처럼 빠른 전개에 유리합니다."
            )
        if price.get("short_ma_conv"):
            comments.append(
                "단기 MA수렴(5/20/60)이 확인됩니다. 단기 에너지 응축이 살아 있어 돌파 직전의 정돈된 흐름으로 해석할 수 있습니다."
            )
        if price.get("struct_ma_conv"):
            comments.append(
                "구조 MA수렴(20/60/112)이 확인됩니다. 중기 바닥 구조가 정리되는 단계라 역매공파나 스윙 준비형 해석에 적합합니다."
            )
        if price.get("bridge_ma_conv"):
            comments.append(
                "브릿지 MA수렴(5/20/112)이 확인됩니다. 단기 흐름이 구조선으로 재합류하는 형태라 MA112 부근 재출발형 해석에 의미가 있습니다."
            )
        if price.get("connect_ma_conv"):
            comments.append(
                "구조접속 MA수렴(5/60/112)이 확인됩니다. 초기 구조 회복형에 가깝고, MA20 정렬이 아직 약할 수 있으므로 거래량과 종가 위치를 함께 보는 편이 좋습니다."
            )

    if not comments:
        return "현재는 의미 있는 MA수렴이 약합니다. 이평 응축보다는 개별 패턴이나 수급, 거래대금 중심으로 해석하는 편이 적절합니다."

    return " ".join(dict.fromkeys(comments))

def build_snapshot(df: pd.DataFrame, window: Dict[str, str]) -> Dict[str, Any]:
    row = df.iloc[-1]
    close = safe_float(row["Close"])
    open_p = safe_float(row["Open"])
    high = safe_float(row["High"])
    low = safe_float(row["Low"])
    volume = safe_float(row["Volume"])
    vma20 = safe_float(row.get("VMA20"))
    atr = safe_float(row.get("ATR"))
    total = max(high - low, 1e-9)

    env = check_envelope_bottom(row, df)
    double_bottom = find_double_bottom(df)

    ma5 = safe_float(row.get("MA5"))
    ma10 = safe_float(row.get("MA10"))
    ma20 = safe_float(row.get("MA20"))
    ma40 = safe_float(row.get("MA40"))
    ma60 = safe_float(row.get("MA60"))
    ma112 = safe_float(row.get("MA112"))
    ma200 = safe_float(row.get("MA200"))

    ultra_ma_conv_pct = calc_triplet_convergence(ma5, ma10, ma20)
    short_ma_conv_pct = calc_triplet_convergence(ma5, ma20, ma60)
    struct_ma_conv_pct = calc_triplet_convergence(ma20, ma60, ma112)
    bridge_ma_conv_pct = calc_triplet_convergence(ma5, ma20, ma112)
    connect_ma_conv_pct = calc_triplet_convergence(ma5, ma60, ma112)

    ultra_ma_conv = ultra_ma_conv_pct <= ULTRA_MA_CONV_MAX
    short_ma_conv = short_ma_conv_pct <= SHORT_MA_CONV_MAX
    struct_ma_conv = struct_ma_conv_pct <= STRUCT_MA_CONV_MAX
    bridge_ma_conv = bridge_ma_conv_pct <= BRIDGE_MA_CONV_MAX
    connect_ma_conv = connect_ma_conv_pct <= CONNECT_MA_CONV_MAX
    super_ma_conv = (short_ma_conv and struct_ma_conv) or (ultra_ma_conv and struct_ma_conv)

    bb40_mid = safe_float(row.get("BB40_MID"))
    bb40_up = safe_float(row.get("BB40_UP"))
    bb40_dn = safe_float(row.get("BB40_DN"))

    pre_bundle = build_pre_dolbanji_bundle(df)
    pre_detail = pre_bundle.get("pre_dolbanji_detail", {}) if isinstance(pre_bundle, dict) else {}
    pre_trend = pre_detail.get("trend_confirm", {}) if isinstance(pre_detail, dict) else {}
    pre_hts_bundle = build_pre_dolbanji_hts_exact_bundle(df)
    pre_hts_detail = pre_hts_bundle.get("pre_dolbanji_hts_exact_detail", {}) if isinstance(pre_hts_bundle, dict) else {}

    prep_zone = bb40_dn * 0.98 <= close <= bb40_mid * 1.02 if bb40_dn > 0 and bb40_mid > 0 else False
    launch_zone = bb40_mid * 0.98 <= close <= bb40_up * 1.02 if bb40_mid > 0 and bb40_up > 0 else False

    watermelon_phase = "비정형"
    if prep_zone and close < bb40_mid:
        watermelon_phase = "준비형"
    elif launch_zone:
        watermelon_phase = "발사형"

    return {
        "date": str(pd.to_datetime(df.index[-1]).date()),
        "target_start_date": window["target_start"],
        "target_end_date": window["target_end"],
        "fetch_start_date": window["fetch_start"],
        "fetch_end_date": window["fetch_end"],
        "bars": len(df),
        "has_ma112": len(df) >= 112,
        "has_ma200": len(df) >= 200,
        "has_ma224": len(df) >= 224,
        "open": round(open_p),
        "close": round(close),
        "high": round(high),
        "low": round(low),
        "amount_b": round(close * volume / 1e8, 1),
        "vol_ratio": round(volume / vma20, 1) if vma20 > 0 else 0.0,
        "vma20": round(vma20, 1),
        "atr": round(atr, 1),
        "disparity": round(safe_float(row.get("Disparity")), 1),
        "near_high20_pct": round(safe_float(row.get("NearHigh20_Pct")), 1),
        "near_high60_pct": round(safe_float(row.get("NearHigh60_Pct")), 1),
        "upper_wick_body_pct": calc_upper_wick_body_pct(row),
        "lower_wick_body_pct": calc_lower_wick_body_pct(row),
        "upper_wick_total_pct": round(max(0.0, high - max(open_p, close)) / total * 100, 1),
        "rsi14": round(safe_float(row.get("RSI14")), 1),
        "mfi14": round(safe_float(row.get("MFI14")), 1),
        "bb20_width": round(safe_float(row.get("BB20_WIDTH")), 1),
        "bb40_width": round(safe_float(row.get("BB40_WIDTH")), 1),
        "bb40_pctb": round(safe_float(row.get("BB40_PCTB")), 3),
        "obv_slope_5": round(safe_float(row.get("OBV_SLOPE_5")), 2),
        "obv_slope_10": round(safe_float(row.get("OBV_SLOPE_10")), 2),
        "green_days_10": safe_int(row.get("Green_Days_10")),
        "ma5": round(ma5, 1),
        "ma10": round(ma10, 1),
        "ma20": round(ma20, 1),
        "ma40": round(ma40, 1),
        "ma60": round(ma60, 1),
        "ma112": round(ma112, 1),
        "ma200": round(ma200, 1),
        "ma5_slope": round(safe_float(row.get("MA5_SLOPE")), 2),
        "ma10_slope": round(safe_float(row.get("MA10_SLOPE")), 2),
        "ma20_slope": round(safe_float(row.get("MA20_SLOPE")), 2),
        "ma60_slope": round(safe_float(row.get("MA60_SLOPE")), 2),
        "ma200_gap_pct": round(safe_float(row.get("MA200_GAP_PCT")), 1),
        "ultra_ma_conv_pct": ultra_ma_conv_pct,
        "short_ma_conv_pct": short_ma_conv_pct,
        "struct_ma_conv_pct": struct_ma_conv_pct,
        "bridge_ma_conv_pct": bridge_ma_conv_pct,
        "connect_ma_conv_pct": connect_ma_conv_pct,
        "ultra_ma_conv": ultra_ma_conv,
        "short_ma_conv": short_ma_conv,
        "struct_ma_conv": struct_ma_conv,
        "bridge_ma_conv": bridge_ma_conv,
        "connect_ma_conv": connect_ma_conv,
        "super_ma_conv": super_ma_conv,
        "env20_pct": env["env20_pct"],
        "env40_pct": env["env40_pct"],
        "env20_near": env["env20_near"],
        "env40_near": env["env40_near"],
        "env20_lower": env["lower20"],
        "env40_lower": env["lower40"],
        "double_bottom": double_bottom,
        "macd": round(safe_float(row.get("MACD")), 4),
        "macd_signal": round(safe_float(row.get("MACD_SIGNAL")), 4),
        "macd_hist": round(safe_float(row.get("MACD_HIST")), 4),
        "plus_di": round(safe_float(row.get("PLUS_DI")), 2),
        "minus_di": round(safe_float(row.get("MINUS_DI")), 2),
        "adx": round(safe_float(row.get("ADX")), 2),
        "watermelon_green": bool(safe_int(row.get("WATERMELON_GREEN"))),
        "watermelon_red": bool(safe_int(row.get("WATERMELON_RED"))),
        "watermelon_green_new": bool(safe_int(row.get("WATERMELON_GREEN_NEW"))),
        "watermelon_red_new": bool(safe_int(row.get("WATERMELON_RED_NEW"))),
        "watermelon_green_score": safe_int(row.get("WATERMELON_GREEN_SCORE")),
        "watermelon_red_score": safe_int(row.get("WATERMELON_RED_SCORE")),
        "watermelon_green_days": safe_int(row.get("WATERMELON_GREEN_DAYS")),
        "watermelon_red_days": safe_int(row.get("WATERMELON_RED_DAYS")),
        "watermelon_value": safe_int(row.get("WATERMELON_VALUE")),
        "watermelon_quality": round(safe_float(row.get("WATERMELON_QUALITY")), 2),
        "watermelon_phase": watermelon_phase,
        "dante_v4_score": round(safe_float(row.get("DANTE_3PHASE_SCORE")), 1),
        "dante_v4_grade": str(row.get("DANTE_3PHASE_GRADE", "C")),
        "dante_v4_fire": bool(safe_int(row.get("DANTE_FINAL_FIRE"))),
        "dante_v4_prep": bool(safe_int(row.get("DANTE_FINAL_PREP"))),
        "dante_v4_hold": bool(safe_int(row.get("DANTE_FINAL_HOLD"))),
        "sym_score_v4": safe_int(row.get("SYM_SCORE")),
        "energy_total_v4": round(safe_float(row.get("ENERGY_TOTAL")), 1),
        "watermelon_phase_v4": str(row.get("WM_STATE_NAME", "NONE")),
        "watermelon_quality_v4": round(safe_float(row.get("WATERMELON_QUALITY_V4")), 1),
        "watermelon_prepare_v4": bool(safe_int(row.get("Watermelon_Prepare_V4"))),
        "watermelon_first_launch_v4": bool(safe_int(row.get("Watermelon_First_Launch_V4"))),
        "watermelon_hold_v4": bool(safe_int(row.get("Watermelon_Hold_V4"))),
        "watermelon_launch_v4": bool(safe_int(row.get("Watermelon_Launch_V4"))),
        "pre_dolbanji": bool(pre_bundle.get("pre_dolbanji", False)),
        "pre_dolbanji_confirmed": bool(pre_bundle.get("pre_dolbanji_confirmed", False)),
        "pre_dolbanji_score": safe_int(pre_bundle.get("pre_dolbanji_score", 0)),
        "pre_dolbanji_grade": str(pre_bundle.get("pre_dolbanji_grade", "없음")),
        "pre_dolbanji_best": str(pre_bundle.get("pre_dolbanji_best", "")),
        "pre_dolbanji_tags": " ".join(pre_bundle.get("pre_dolbanji_tags", [])) if isinstance(pre_bundle, dict) else "",
        "pre_dolbanji_trend_score": safe_int(pre_trend.get("score", 0)),
        "pre_dolbanji_lite": bool(pre_lite_bundle.get("pre_dolbanji_lite", False)),
        "pre_dolbanji_lite_confirmed": bool(pre_lite_bundle.get("pre_dolbanji_lite_confirmed", False)),
        "pre_dolbanji_lite_score": safe_int(pre_lite_bundle.get("pre_dolbanji_lite_score", 0)),
        "pre_dolbanji_lite_grade": str(pre_lite_bundle.get("pre_dolbanji_lite_grade", "없음")),
        "pre_dolbanji_lite_best": str(pre_lite_bundle.get("pre_dolbanji_lite_best", "")),
        "pre_dolbanji_lite_tags": " ".join(pre_lite_bundle.get("pre_dolbanji_lite_tags", [])) if isinstance(pre_lite_bundle, dict) else "",
        "pre_dolbanji_lite_trend_score": safe_int((pre_lite_detail.get("trend_confirm", {}) or {}).get("score", 0)),
        "pre_dolbanji_hts_exact": bool(pre_hts_bundle.get("pre_dolbanji_hts_exact", False)),
        "pre_dolbanji_hts_exact_score": safe_int(pre_hts_bundle.get("pre_dolbanji_hts_exact_score", 0)),
        "pre_dolbanji_hts_exact_max_score": safe_int(pre_hts_bundle.get("pre_dolbanji_hts_exact_max_score", 10)),
        "pre_dolbanji_hts_exact_tags": " ".join(pre_hts_bundle.get("pre_dolbanji_hts_exact_tags", [])) if isinstance(pre_hts_bundle, dict) else "",
        "pre_dolbanji_hts_exact_tech_score": safe_int(pre_hts_detail.get("tech_score", 0)),
    }


def add_check(rows: List[CheckRow], strategy: str, label: str, current: str, target: str, ok: bool, reason: str) -> None:
    rows.append(CheckRow(strategy, label, current, target, ok, reason))


def decide_status(score: int, max_score: int) -> str:
    if max_score <= 0:
        return "미해당"
    ratio = score / max_score
    if ratio >= 0.8:
        return "해당"
    if ratio >= 0.5:
        return "유사"
    return "미해당"


def build_closing_bet(price: Dict[str, Any]) -> PatternResult:
    rows: List[CheckRow] = []
    s = "종가배팅"

    c1 = price["close"] >= MIN_PRICE
    add_check(rows, s, "최소 주가", f"{fmt_int(price['close'])}", f">= {fmt_int(MIN_PRICE)}", c1, "가격 조건 충족" if c1 else "저가주 구간")

    c2 = NEAR_HIGH20_MIN <= price["near_high20_pct"] <= NEAR_HIGH20_MAX
    add_check(rows, s, "20일 전고점 근접도", f"{price['near_high20_pct']}%", f"{NEAR_HIGH20_MIN}~{NEAR_HIGH20_MAX}%", c2, "전고점 부근" if c2 else "전고점과 거리 있음")

    c3 = price["upper_wick_body_pct"] <= UPPER_WICK_BODY_MAX
    add_check(rows, s, "윗꼬리(몸통 기준)", f"{price['upper_wick_body_pct']}%", f"<= {UPPER_WICK_BODY_MAX}%", c3, "강봉 마감" if c3 else "윗꼬리 길어 종가 힘 약함")

    c4 = price["vol_ratio"] >= VOL_MULT
    add_check(rows, s, "거래량 배수", f"{price['vol_ratio']}배", f">= {VOL_MULT}배", c4, "거래량 폭발" if c4 else "거래량 확산 부족")

    c5 = DISPARITY_MIN <= price["disparity"] <= DISPARITY_MAX
    add_check(rows, s, "이격도", f"{price['disparity']}", f"{DISPARITY_MIN}~{DISPARITY_MAX}", c5, "적정 이격" if c5 else "과열 또는 힘 부족")

    c6 = price["close"] >= price["ma20"]
    add_check(rows, s, "MA20 위 마감", f"{fmt_int(price['close'])} / MA20 {fmt_float(price['ma20'])}", "종가 >= MA20", c6, "추세선 위" if c6 else "MA20 아래")

    c7 = price["amount_b"] >= MIN_AMOUNT_B
    add_check(rows, s, "거래대금", f"{price['amount_b']}억", f">= {MIN_AMOUNT_B}억", c7, "유동성 충분" if c7 else "거래대금 약함")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))
    if status == "해당":
        comment = "종가배팅 관점에서 구조가 비교적 잘 맞습니다. 전고점 부근 강한 마감으로 해석 가능합니다."
    elif status == "유사":
        comment = "큰 틀은 비슷하지만 몇 가지 핵심 수치가 부족합니다. 다음 봉 확인이 필요합니다."
    else:
        comment = "현재는 종가배팅 완성형으로 보기 어렵습니다. 힘이 모자라거나 자리 자체가 다를 가능성이 큽니다."
    return PatternResult("closing_bet", s, status, score, len(rows), "전고점 부근 강한 종가 마감형", comment, rows)


def build_envelope_bet(price: Dict[str, Any]) -> PatternResult:
    rows: List[CheckRow] = []
    s = "엔벨로프"

    c1 = price["env20_near"]
    add_check(rows, s, "Envelope20 하단 근접", f"{price['env20_pct']}%", f"{ENV20_NEAR_MIN}~{ENV20_NEAR_MAX}%", c1, "하단 근접" if c1 else "20일 하단선과 거리 있음")

    c2 = price["env40_near"]
    add_check(rows, s, "Envelope40 하단 근접", f"{price['env40_pct']}%", f"{ENV40_NEAR_MIN}~{ENV40_NEAR_MAX}%", c2, "하단 근접" if c2 else "40일 하단선과 거리 있음")

    c3 = price["close"] >= MIN_PRICE
    add_check(rows, s, "최소 주가", f"{fmt_int(price['close'])}", f">= {fmt_int(MIN_PRICE)}", c3, "가격 조건 충족" if c3 else "저가주")

    c4 = price["lower_wick_body_pct"] >= 20
    add_check(rows, s, "아랫꼬리(몸통 기준)", f"{price['lower_wick_body_pct']}%", ">= 20%", c4, "하단 지지 흔적" if c4 else "받아올림 약함")

    c5 = price["rsi14"] <= 45
    add_check(rows, s, "RSI 과열 여부", f"{price['rsi14']}", "<= 45", c5, "과열 아님" if c5 else "이미 많이 반등")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))
    if status == "해당":
        comment = "엔벨로프 하단 근처의 되돌림 자리로 해석할 수 있습니다."
    elif status == "유사":
        comment = "엔벨로프 관점은 일부 맞지만 핵심 하단 근접도가 다소 아쉽습니다."
    else:
        comment = "엔벨로프 하단 매매 자리로 보기엔 거리나 캔들 구조가 부족합니다."
    return PatternResult("envelope_bet", s, status, score, len(rows), "하단 근접 반등 후보", comment, rows)


def build_dolbanji(price: Dict[str, Any], df: pd.DataFrame) -> PatternResult:
    if not price["has_ma200"]:
        rows = [CheckRow("돌반지", "데이터 길이", f'{price["bars"]}봉', ">= 200봉", False, "상장 기간이 짧아 MA200 기반 돌반지 판정 불가")]
        return PatternResult("dolbanji", "돌반지", "데이터부족", 0, 1, "200일선 돌파 직후 응축형", "신규상장주라 200일 이동평균선이 아직 형성되지 않아 돌반지 패턴은 판정 보류입니다.", rows)

    rows: List[CheckRow] = []
    s = "돌반지"
    recent20 = df.tail(20)
    recent_below_200 = bool((recent20["Close"] < recent20["MA200"]).fillna(False).any())

    c1 = price["close"] >= price["ma200"]
    add_check(rows, s, "200일선 위", f"{fmt_int(price['close'])} / MA200 {fmt_float(price['ma200'])}", "종가 >= MA200", c1, "200일선 돌파/안착" if c1 else "아직 200일선 아래")

    c2 = recent_below_200
    add_check(rows, s, "최근 20봉 내 200일선 하단 이력", "있음" if c2 else "없음", "있음", c2, "최근 돌파형 가능" if c2 else "너무 오래 위에 있었음")

    c3 = -3.0 <= price["ma200_gap_pct"] <= 8.0
    add_check(rows, s, "200일선 이격", f"{price['ma200_gap_pct']}%", "-3% ~ +8%", c3, "돌파 직후 적정 이격" if c3 else "이격이 너무 큼 또는 너무 아래")

    c4 = price["vol_ratio"] >= 1.2
    add_check(rows, s, "거래량 배수", f"{price['vol_ratio']}배", ">= 1.2배", c4, "거래량 보강" if c4 else "거래량 약함")

    c5 = price["obv_slope_10"] > 0
    add_check(rows, s, "OBV 기울기", f"{price['obv_slope_10']}%", "> 0%", c5, "매집 우위" if c5 else "수급 기울기 약함")

    c6 = price["ma60_slope"] >= 0
    add_check(rows, s, "MA60 기울기", f"{price['ma60_slope']}%", ">= 0%", c6, "중기 추세 꺾임 완화" if c6 else "중기 추세 아직 하방")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))
    if status == "해당":
        comment = "200일선 돌파형에 가깝습니다. 돌반지 성격으로 해석할 수 있습니다."
    elif status == "유사":
        comment = "돌반지 느낌은 있지만 200일선 돌파 직후의 응축감은 조금 더 필요합니다."
    else:
        comment = "현재는 돌반지 핵심 구조가 약합니다."
    return PatternResult("dolbanji", s, status, score, len(rows), "200일선 돌파 직후 응축형", comment, rows)



def build_pre_dolbanji(price: Dict[str, Any], df: pd.DataFrame) -> PatternResult:
    rows: List[CheckRow] = []
    s = "예비돌반지"

    c1 = bool(price.get("pre_dolbanji", False))
    add_check(rows, s, "후보 감지", "감지" if c1 else "미감지", "감지", c1, "예비돌반지 계열 조건 포착" if c1 else "현재는 예비돌반지 계열 미감지")

    c2 = str(price.get("pre_dolbanji_grade", "없음")) in ("A", "S", "B")
    add_check(rows, s, "등급", str(price.get("pre_dolbanji_grade", "없음")), "B 이상", c2, "등급 부여됨" if c2 else "등급 낮음")

    c3 = safe_int(price.get("pre_dolbanji_score", 0)) >= 60
    add_check(rows, s, "종합 점수", f"{safe_int(price.get('pre_dolbanji_score', 0))}", ">= 60", c3, "예비돌반지 점수 양호" if c3 else "점수 부족")

    c4 = str(price.get("pre_dolbanji_best", "")).strip() != ""
    add_check(rows, s, "대표 변형", str(price.get("pre_dolbanji_best", "") or "없음"), "A/B/C/D 중 1개", c4, "대표 변형 확인" if c4 else "대표 변형 없음")

    c5 = safe_int(price.get("pre_dolbanji_trend_score", 0)) >= 3
    add_check(rows, s, "구조전환 확인", f"{safe_int(price.get('pre_dolbanji_trend_score', 0))}/4", ">= 3/4", c5, "추세전환 확인" if c5 else "아직 추세전환 미완")

    c6 = bool(price.get("pre_dolbanji_confirmed", False))
    add_check(rows, s, "확인형 여부", "확인" if c6 else "대기", "확인", c6, "예비돌반지 확인형" if c6 else "아직 감시형 단계")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))
    if c6:
        status = "해당"
        comment = "장기 역배열 구간에서 과거 폭발 흔적 이후 재정비를 거쳐 구조전환 확인까지 동반된 예비돌반지 확인형입니다."
    elif c1 and c5:
        comment = "예비돌반지 감시형으로는 꽤 괜찮지만, 완전한 돌반지보다는 재돌파 대기형에 가깝습니다."
    elif c1:
        comment = "예비돌반지 계열 흔적은 있으나 아직 추세전환 확인이 덜 붙었습니다. 감시목록 성격으로 보는 편이 좋습니다."
    else:
        comment = "현재는 예비돌반지 계열 패턴으로 보기 어렵습니다."

    subtitle = f"대표 {price.get('pre_dolbanji_best', '없음')} · 등급 {price.get('pre_dolbanji_grade', '없음')} · 점수 {safe_int(price.get('pre_dolbanji_score', 0))} · 구조전환 {safe_int(price.get('pre_dolbanji_trend_score', 0))}/4"
    return PatternResult("pre_dolbanji", s, status, score, len(rows), subtitle, comment, rows)




def build_pre_dolbanji_lite(price: Dict[str, Any], df: pd.DataFrame) -> PatternResult:
    rows: List[CheckRow] = []
    s = "신규예비돌반지 Lite"

    c1 = bool(price.get("pre_dolbanji_lite", False))
    add_check(rows, s, "후보 감지", "감지" if c1 else "미감지", "감지", c1, "신규상장/장기이평 부족 종목용 Lite 패턴 포착" if c1 else "현재는 Lite 패턴 미감지")

    c2 = str(price.get("pre_dolbanji_lite_grade", "없음")) in ("A", "S", "B")
    add_check(rows, s, "등급", str(price.get("pre_dolbanji_lite_grade", "없음")), "B 이상", c2, "Lite 등급 부여" if c2 else "등급 낮음")

    c3 = safe_int(price.get("pre_dolbanji_lite_score", 0)) >= 30
    add_check(rows, s, "종합 점수", f"{safe_int(price.get('pre_dolbanji_lite_score', 0))}", ">= 30", c3, "Lite 점수 양호" if c3 else "점수 부족")

    c4 = str(price.get("pre_dolbanji_lite_best", "")).strip() != ""
    add_check(rows, s, "대표 변형", str(price.get("pre_dolbanji_lite_best", "") or "없음"), "LiteA/B 중 1개", c4, "대표 Lite 변형 확인" if c4 else "대표 Lite 변형 없음")

    c5 = safe_int(price.get("pre_dolbanji_lite_trend_score", 0)) >= 2
    add_check(rows, s, "구조전환 확인", f"{safe_int(price.get('pre_dolbanji_lite_trend_score', 0))}/4", ">= 2/4", c5, "신규 구조전환 확인" if c5 else "아직 구조전환 미완")

    c6 = bool(price.get("pre_dolbanji_lite_confirmed", False))
    add_check(rows, s, "확인형 여부", "확인" if c6 else "대기", "확인", c6, "Lite 확인형" if c6 else "감시형 단계")

    score = sum(1 for r in rows if r.ok)
    if c6 and score >= 5:
        status = "좋음"
        comment = "224/448 장기선이 없는 신규상장/짧은 이력 종목에서 기존 예비돌반지를 대체할 수 있는 Lite 확인형입니다."
    elif c1 and score >= 4:
        status = "보통"
        comment = "신규상장 감시형으로는 괜찮습니다. 장기선이 생기기 전까지는 Lite 패턴으로 추적하는 편이 좋습니다."
    elif c1:
        status = "약함"
        comment = "Lite 흔적은 있지만 아직 초기 구조전환이 덜 붙었습니다."
    else:
        status = "미충족"
        comment = "현재는 신규예비돌반지 Lite 패턴으로 보기 어렵습니다."

    subtitle = f"대표 {price.get('pre_dolbanji_lite_best', '없음')} · 등급 {price.get('pre_dolbanji_lite_grade', '없음')} · 점수 {safe_int(price.get('pre_dolbanji_lite_score', 0))} · 구조전환 {safe_int(price.get('pre_dolbanji_lite_trend_score', 0))}/4"
    return PatternResult("pre_dolbanji_lite", s, status, score, len(rows), subtitle, comment, rows)

def build_pre_dolbanji_hts_exact(price: Dict[str, Any], df: pd.DataFrame) -> PatternResult:
    rows: List[CheckRow] = []
    s = "예비돌반지(HTS정확복제)"

    c1 = bool(price.get("pre_dolbanji_hts_exact", False))
    add_check(rows, s, "정확복제형 통과", "통과" if c1 else "미통과", "통과", c1, "스크린샷 조건을 전부 통과" if c1 else "정확복제형은 아직 미통과")

    c2 = safe_int(price.get("pre_dolbanji_hts_exact_score", 0)) >= 8
    add_check(rows, s, "기술 점수", f"{safe_int(price.get('pre_dolbanji_hts_exact_score', 0))}/{safe_int(price.get('pre_dolbanji_hts_exact_max_score', 10))}", ">= 8/10", c2, "기술조건 대부분 충족" if c2 else "기술조건 충족 수 부족")

    tags_now = str(price.get("pre_dolbanji_hts_exact_tags", ""))
    c3 = "🧱224<448_50봉지속" in tags_now
    add_check(rows, s, "224<448 50봉 지속", "해당" if c3 else "미해당", "해당", c3, "장기 역배열 구조 유지" if c3 else "224<448 장기구조 부족")

    c4 = "🟣BB40상단근접" in tags_now
    add_check(rows, s, "BB40 상단 근접", "해당" if c4 else "미해당", "해당", c4, "상단선 근접" if c4 else "BB40 상단 근접 아님")

    c5 = "🚀과거폭발흔적" in tags_now
    add_check(rows, s, "과거 폭발 흔적", "해당" if c5 else "미해당", "해당", c5, "급등+거래량 폭증 이력" if c5 else "과거 폭발 흔적 부족")

    c6 = "📈448상향돌파이력" in tags_now
    add_check(rows, s, "448 상향 돌파 이력", "해당" if c6 else "미해당", "해당", c6, "과거 448 돌파 이력 존재" if c6 else "448 돌파 이력 부족")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))
    if c1:
        status = "해당"
        comment = "사용자가 제시한 영웅문 HTS 스크린샷 조건을 기술적으로 거의 그대로 통과한 엄격형 패턴입니다."
    elif c2 and c3 and (c5 or c6):
        comment = "정확복제형에 꽤 가깝습니다. 기존 예비돌반지보다 더 엄격하게 보는 확인용 패턴으로 적합합니다."
    elif c2:
        comment = "일부 핵심 조건은 붙었지만 정확복제형으로 보기엔 아직 1~2개가 부족합니다."
    else:
        comment = "HTS 정확복제형 기준으로는 현재 구조가 부족합니다."

    subtitle = f"기술 {safe_int(price.get('pre_dolbanji_hts_exact_score', 0))}/{safe_int(price.get('pre_dolbanji_hts_exact_max_score', 10))} · 태그 {tags_now or '없음'}"
    return PatternResult("pre_dolbanji_hts_exact", s, status, score, len(rows), subtitle, comment, rows)


def build_watermelon(price: Dict[str, Any], df: pd.DataFrame) -> PatternResult:
    rows: List[CheckRow] = []
    s = "수박"

    conv_tags = []
    if price["ultra_ma_conv"]:
        conv_tags.append("5/10/20")
    if price["short_ma_conv"]:
        conv_tags.append("5/20/60")
    if price["struct_ma_conv"]:
        conv_tags.append("20/60/112")
    if price["bridge_ma_conv"]:
        conv_tags.append("5/20/112")
    if price["connect_ma_conv"]:
        conv_tags.append("5/60/112")
    conv_ok = len(conv_tags) > 0

    v4_phase = price.get("watermelon_phase_v4", "NONE")
    display_phase = v4_phase if v4_phase and v4_phase != "NONE" else price.get("watermelon_phase", "비정형")

    c1 = price.get("watermelon_prepare_v4", False) or price["watermelon_green"]
    add_check(rows, s, "준비형 축적", "점등" if c1 else "꺼짐", "점등", c1, "수박 준비 상태 형성" if c1 else "준비형 축적 부족")

    c2 = price.get("sym_score_v4", 0) >= 60
    add_check(rows, s, "기간대칭 점수", f"{price.get('sym_score_v4', 0)}", ">= 60", c2, "시간 구조 적정" if c2 else "시간 대칭성 부족")

    c3 = price.get("energy_total_v4", 0) >= 60
    add_check(rows, s, "파동에너지 점수", f"{price.get('energy_total_v4', 0)}", ">= 60", c3, "발사 에너지 양호" if c3 else "파동 에너지 부족")

    c4 = price.get("watermelon_first_launch_v4", False) or price.get("dante_v4_fire", False)
    add_check(rows, s, "1차 발사형 V4", "해당" if c4 else "미해당", "해당", c4, "신규 점화 구간" if c4 else "신규 발사형 아님")

    c5 = price.get("watermelon_hold_v4", False) or price.get("dante_v4_hold", False)
    add_check(rows, s, "유지형 V4", "해당" if c5 else "미해당", "해당", c5, "발사 후 유지 상태" if c5 else "유지형 아님")

    c6 = price["bb40_width"] <= 18.0
    add_check(rows, s, "BB40 폭", f"{price['bb40_width']}", "<= 18", c6, "응축 구간" if c6 else "밴드 폭 넓음")

    c7 = price["obv_slope_10"] > 0
    add_check(rows, s, "OBV 기울기", f"{price['obv_slope_10']}%", "> 0%", c7, "매집 우위" if c7 else "매집 신호 약함")

    c8 = price["mfi14"] >= 50
    add_check(rows, s, "MFI", f"{price['mfi14']}", ">= 50", c8, "자금 흐름 양호" if c8 else "자금 흐름 약함")

    c9 = price["upper_wick_total_pct"] <= 35.0
    add_check(rows, s, "윗꼬리", f"{price['upper_wick_total_pct']}%", "<= 35%", c9, "종가 품질 양호" if c9 else "윗꼬리 길어 힘 분산")

    c10 = conv_ok
    add_check(rows, s, "MA수렴 동반", ", ".join(conv_tags) if conv_tags else "없음", "1개 이상", c10, "응축 정렬 동반" if c10 else "이평 응축 부족")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))

    if price.get("dante_v4_fire", False):
        status = "해당"
        comment = "기간대칭과 파동에너지를 통과한 뒤 수박이 새롭게 점화된 V4 발사형입니다. 기존 수박 단독 신호보다 신뢰도를 높여 해석할 수 있습니다."
    elif price.get("dante_v4_hold", False):
        status = "해당" if score >= 6 else "유사"
        comment = "V4 기준으로 이미 발사 후 유지 상태에 들어온 구간입니다. 첫 점화봉보다는 보수적으로 보되 추세 지속 여부를 확인할 만합니다."
    elif price.get("dante_v4_prep", False):
        comment = "기간대칭과 파동에너지가 어느 정도 갖춰진 준비형입니다. 수박이 바로 점화되기 전의 감시 구간으로 해석하는 편이 좋습니다."
    elif status == "유사":
        comment = "수박형 느낌은 있지만 3박자 구조나 수급 품질이 아직 완전히 맞지 않습니다. 준비형과 발사형의 경계 구간으로 볼 수 있습니다."
    else:
        comment = "현재는 수박 패턴으로 보기엔 응축·수급·3박자 구조가 모두 충분하지 않습니다."

    subtitle = f"수박 {display_phase} · V4 {price.get('dante_v4_grade', 'C')} · 품질 {price.get('watermelon_quality_v4', price['watermelon_quality'])} · MA수렴 {', '.join(conv_tags) if conv_tags else '없음'}"
    return PatternResult("watermelon", s, status, score, len(rows), subtitle, comment, rows)


def build_viper(price: Dict[str, Any], df: pd.DataFrame) -> PatternResult:
    rows: List[CheckRow] = []
    s = "독사"
    recent = df.tail(6)
    recent_cross = False
    if len(recent) >= 2:
        prev = recent.iloc[:-1]
        last = recent.iloc[-1]
        recent_cross = bool(((prev["MA5"] <= prev["MA20"]).fillna(False)).any() and safe_float(last["MA5"]) > safe_float(last["MA20"]))

    c1 = price["ma5"] > price["ma20"]
    add_check(rows, s, "MA5 > MA20", f"{fmt_float(price['ma5'])} / {fmt_float(price['ma20'])}", "MA5 > MA20", c1, "단기 우상향" if c1 else "아직 단기선 약함")

    c2 = recent_cross
    add_check(rows, s, "최근 5봉 내 5-20 교차", "있음" if c2 else "없음", "있음", c2, "훅 출현" if c2 else "최근 교차 흔적 약함")

    c3 = price["ma5_slope"] > 0
    add_check(rows, s, "MA5 기울기", f"{price['ma5_slope']}%", "> 0%", c3, "단기 기울기 양호" if c3 else "기울기 약함")

    c4 = price["close"] >= price["ma20"]
    add_check(rows, s, "MA20 위 종가", f"{fmt_int(price['close'])} / {fmt_float(price['ma20'])}", "종가 >= MA20", c4, "추세선 위" if c4 else "MA20 아래")

    c5 = price["upper_wick_body_pct"] <= 35.0
    add_check(rows, s, "윗꼬리", f"{price['upper_wick_body_pct']}%", "<= 35%", c5, "종가 밀림 약함" if c5 else "윗꼬리 길어 힘 분산")

    c6 = price["vol_ratio"] >= 1.0
    add_check(rows, s, "거래량 배수", f"{price['vol_ratio']}배", ">= 1.0배", c6, "거래량 최소 확인" if c6 else "거래량 부족")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))
    if status == "해당":
        comment = "5-20 독사 훅 패턴에 비교적 잘 맞습니다."
    elif status == "유사":
        comment = "독사 느낌은 있지만 훅의 선명도나 거래량이 조금 약합니다."
    else:
        comment = "현재는 독사 훅 패턴으로 보기 어렵습니다."
    return PatternResult("viper", s, status, score, len(rows), "5일선-20일선 훅형", comment, rows)


def build_yeokmae(price: Dict[str, Any], df: pd.DataFrame) -> PatternResult:
    if not price["has_ma112"]:
        rows = [CheckRow("역매공파", "데이터 길이", f'{price["bars"]}봉', ">= 112봉", False, "상장 기간이 짧아 MA112 기반 역매공파 판정 불가")]
        return PatternResult("yeokmae", "역매공파", "데이터부족", 0, 1, "수렴 → 공구리 → 상단 돌파형", "신규상장주라 MA112가 아직 충분히 형성되지 않아 역매공파는 판정 보류입니다.", rows)

    rows: List[CheckRow] = []
    s = "역매공파"
    close = safe_float(df["Close"].iloc[-1])
    bb40_up = safe_float(df["BB40_UP"].iloc[-1])

    vals = [price["ma20"], price["ma60"], price["ma112"]]
    valid_vals = [v for v in vals if v > 0]
    ma_conv = 999.0
    if valid_vals:
        ma_conv = (max(valid_vals) - min(valid_vals)) / max(valid_vals) * 100

    c1 = ma_conv <= 8.0
    add_check(rows, s, "MA20/60/112 수렴도", f"{fmt_float(ma_conv)}%", "<= 8%", c1, "이평 수렴" if c1 else "이평 벌어짐")

    c2 = price["bb40_width"] <= 20.0
    add_check(rows, s, "BB40 폭", f"{price['bb40_width']}", "<= 20", c2, "응축 구간" if c2 else "응축 부족")

    c3 = price["close"] >= price["ma112"]
    add_check(rows, s, "공구리(MA112) 돌파", f"{fmt_int(price['close'])} / {fmt_float(price['ma112'])}", "종가 >= MA112", c3, "공구리 돌파" if c3 else "아직 공구리 아래")

    c4 = close >= bb40_up * 0.98
    add_check(rows, s, "BB40 상단 근접/돌파", f"종가 {fmt_int(close)} / 상단 {fmt_float(bb40_up)}", "종가 >= 상단의 98%", c4, "상단 돌파권" if c4 else "상단 돌파 전")

    c5 = price["vol_ratio"] >= 1.5
    add_check(rows, s, "거래량 배수", f"{price['vol_ratio']}배", ">= 1.5배", c5, "거래량 확산" if c5 else "거래량 부족")

    c6 = 98.0 <= price["disparity"] <= 110.0
    add_check(rows, s, "이격도", f"{price['disparity']}", "98~110", c6, "안전 이격" if c6 else "이격 과열/부족")

    c7 = price["obv_slope_10"] > 0
    add_check(rows, s, "OBV 기울기", f"{price['obv_slope_10']}%", "> 0%", c7, "매집 우위" if c7 else "수급 약함")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))
    if status == "해당":
        comment = "역매공파 구조와 꽤 닮아 있습니다. 수렴 → 공구리 → 상단 돌파 흐름으로 볼 수 있습니다."
    elif status == "유사":
        comment = "역매공파 느낌은 있지만 공구리/상단돌파/거래량 중 일부가 약합니다."
    else:
        comment = "현재는 역매공파 완성 구조로 보기 어렵습니다."
    return PatternResult("yeokmae", s, status, score, len(rows), "수렴 → 공구리 → 상단 돌파형", comment, rows)


def build_double_bottom(price: Dict[str, Any]) -> PatternResult:
    rows: List[CheckRow] = []
    s = "쌍바닥"
    db = price["double_bottom"]
    found = bool(db.get("found"))

    c1 = found
    add_check(rows, s, "쌍바닥 감지", "감지" if found else "미감지", "감지", c1, "두 저점 구조 확인" if c1 else "의미 있는 두 바닥 미발견")

    c2 = found and safe_float(db.get("diff_pct")) <= 5.0
    add_check(rows, s, "두 바닥 가격 차이", f"{db.get('diff_pct', '-')}%", "<= 5%", c2, "두 바닥 유사" if c2 else "두 바닥 차이 큼")

    c3 = found and 15 <= safe_int(db.get("gap_days")) <= 45
    add_check(rows, s, "두 바닥 간격", f"{db.get('gap_days', '-')}일", "15~45일", c3, "이상적 간격" if c3 else "간격이 너무 짧거나 김")

    c4 = found and bool(db.get("neckline_break"))
    add_check(rows, s, "넥라인 근접/돌파", f"{db.get('neckline_distance_pct', '-')}%", "넥라인 -1.5% 이내 또는 돌파", c4, "넥라인 돌파권" if c4 else "넥라인 아직 멀음")

    c5 = price["close"] >= price["ma20"]
    add_check(rows, s, "MA20 위 위치", f"{fmt_int(price['close'])} / {fmt_float(price['ma20'])}", "종가 >= MA20", c5, "회복 흐름" if c5 else "추세 회복 미흡")

    c6 = price["vol_ratio"] >= 1.2
    add_check(rows, s, "거래량 배수", f"{price['vol_ratio']}배", ">= 1.2배", c6, "거래량 보강" if c6 else "거래량 부족")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))
    if status == "해당":
        comment = "쌍바닥 구조가 비교적 선명합니다. 넥라인 돌파 여부를 핵심으로 보면 좋습니다."
    elif status == "유사":
        comment = "쌍바닥 유사 구조는 있으나 넥라인/거래량 확인이 조금 더 필요합니다."
    else:
        comment = "현재는 쌍바닥으로 보기엔 구조적 근거가 약합니다."

    subtitle = "두 저점 + 넥라인 회복형"
    if found:
        subtitle = f"좌 {db.get('left_date')} / 우 {db.get('right_date')} · 넥라인 {db.get('neckline')}"
    return PatternResult("double_bottom", s, status, score, len(rows), subtitle, comment, rows)


def build_patterns(price: Dict[str, Any], df: pd.DataFrame) -> List[PatternResult]:
    patterns = [
        build_closing_bet(price),
        build_envelope_bet(price),
        build_dolbanji(price, df),
        build_pre_dolbanji(price, df),
        build_pre_dolbanji_lite(price, df),
        build_pre_dolbanji_hts_exact(price, df),
        build_watermelon(price, df),
        build_viper(price, df),
        build_yeokmae(price, df),
        build_double_bottom(price),
    ]
    priority = {"해당": 4, "유사": 3, "미해당": 2, "데이터부족": 1}
    patterns.sort(key=lambda x: (priority.get(x.status, 0), x.score), reverse=True)
    return patterns


def patterns_for_mode(patterns: List[PatternResult], mode: str) -> List[PatternResult]:
    if mode == "all":
        return patterns
    return [p for p in patterns if p.key == mode]


def build_summary(name: str, patterns: List[PatternResult]) -> str:
    if not patterns:
        return f"{name}은 현재 선택한 모드에서 표시할 패턴이 없습니다."
    good = [p for p in patterns if p.status == "해당"]
    near = [p for p in patterns if p.status == "유사"]
    na = [p for p in patterns if p.status == "데이터부족"]

    parts = []
    if good:
        parts.append(f"{name}은 {', '.join(p.name for p in good)} 패턴에 해당합니다.")
    if near:
        parts.append(f"유사 패턴은 {', '.join(p.name for p in near)} 입니다.")
    if na:
        parts.append(f"데이터 부족으로 판정 보류된 패턴은 {', '.join(p.name for p in na)} 입니다.")
    if not good and not near and not na:
        parts.append(f"{name}은 현재 뚜렷하게 맞는 패턴이 적습니다.")
    return " ".join(parts)


def build_smart_comment(price: Dict[str, Any], patterns: List[PatternResult], name: str) -> str:
    if not patterns:
        return f"{name}은 현재 분석할 패턴 결과가 없습니다."

    best = patterns[0]
    comments = [f"가장 가까운 구조는 {best.name}입니다."]

    if best.status == "해당":
        comments.append("완성도가 높은 편이라 해당 패턴 기준으로 추적하기 좋습니다.")
    elif best.status == "유사":
        comments.append("뼈대는 있으나 아직 핵심 한두 조건이 부족합니다.")
    elif best.status == "데이터부족":
        comments.append("상장 기간이 짧아 장기이평 기반 패턴은 판정 보류입니다.")
    else:
        comments.append("어느 한 패턴으로 강하게 단정할 정도는 아닙니다.")

    comments.append(build_ma_convergence_comment(price))

    if price["bars"] < 112:
        comments.append("신규상장주 구간이라 장기 패턴보다 20/40일선 중심 패턴 해석이 더 중요합니다.")
    if price["upper_wick_body_pct"] > 30:
        comments.append("윗꼬리가 길어 종가 힘이 분산된 점은 보수적으로 봐야 합니다.")
    if price["vol_ratio"] < 1.0:
        comments.append("거래량이 평균 대비 약해 패턴 신뢰도가 떨어질 수 있습니다.")
    if price["disparity"] > 112:
        comments.append("이격도가 높아 추격 관점은 불리할 수 있습니다.")
    elif price["disparity"] < 98:
        comments.append("이격이 낮아 돌파 탄력은 아직 약할 수 있습니다.")
    if price["obv_slope_10"] < 0:
        comments.append("OBV 기울기가 음수라 최근 구간은 분산 우위 해석이 가능합니다.")
    if price["has_ma200"] and price["ma60_slope"] < 0:
        comments.append("MA60 기울기가 음수면 중기 추세는 아직 완전히 돌아섰다고 보기 어렵습니다.")
    if price["double_bottom"].get("found"):
        comments.append(f"쌍바닥은 감지되었고 넥라인과의 거리는 {price['double_bottom'].get('neckline_distance_pct')}% 수준입니다.")
    if price.get("watermelon_green"):
        comments.append(f"수박 매집 신호는 켜져 있고 현재 단계는 {price.get('watermelon_phase')}입니다.")
    if price.get("watermelon_red_new"):
        comments.append("수박 빨강 신규 점등 구간이면 진입 타점 초기 가능성을 체크할 만합니다.")
    if price.get("pre_dolbanji"):
        comments.append(f"예비돌반지 계열이 감지되었고 대표 변형은 {price.get('pre_dolbanji_best', '') or '없음'} 입니다. 구조전환 점수는 {price.get('pre_dolbanji_trend_score', 0)}/4 입니다.")
    if price.get("pre_dolbanji_confirmed"):
        comments.append("예비돌반지 확인형이면 단순 감시목록이 아니라 재돌파 관찰 우선순위를 높일 수 있습니다.")
    if price.get("pre_dolbanji_lite"):
        comments.append(f"신규예비돌반지 Lite가 감지되었고 대표 변형은 {price.get('pre_dolbanji_lite_best', '') or '없음'} 입니다. 구조전환 점수는 {price.get('pre_dolbanji_lite_trend_score', 0)}/4 입니다.")
    if price.get("pre_dolbanji_lite_confirmed"):
        comments.append("신규예비돌반지 Lite 확인형이면 장기이평이 형성되기 전의 대체 감시 패턴으로 우선순위를 높일 수 있습니다.")
    if price.get("pre_dolbanji_hts_exact"):
        comments.append("HTS 정확복제형까지 동시에 통과하면 기존 예비돌반지보다 더 엄격한 교차검증을 통과한 상태로 볼 수 있습니다.")
    if price.get("dante_v4_prep"):
        comments.append(f"V4 3박자 기준으로는 준비형이며 기간대칭 {price.get('sym_score_v4')}점, 파동에너지 {price.get('energy_total_v4')}점 수준입니다.")
    if price.get("dante_v4_fire"):
        comments.append(f"V4 3박자 최종 발사형이 켜져 있고 수박 상태는 {price.get('watermelon_phase_v4')}입니다.")
    elif price.get("dante_v4_hold"):
        comments.append("V4 기준 발사 후 유지형 구간이며 과열보다 유지력 확인이 중요합니다.")

    return " ".join(dict.fromkeys(comments))



def scan_pattern_history(df: pd.DataFrame, window: Dict[str, str], mode: str, min_bars: int = 40) -> List[Dict[str, Any]]:
    target_start = pd.Timestamp(window["target_start"])
    target_end = pd.Timestamp(window["target_end"])

    work_idx = [idx for idx in df.index if target_start <= pd.Timestamp(idx).normalize() <= target_end]
    hits: List[Dict[str, Any]] = []

    for current_date in work_idx:
        subdf = df[df.index <= current_date].copy()
        if len(subdf) < min_bars:
            continue

        sub_window = {
            "target_start": window["target_start"],
            "target_end": str(pd.Timestamp(current_date).date()),
            "fetch_start": window["fetch_start"],
            "fetch_end": str(pd.Timestamp(current_date).date()),
        }
        day_price = build_snapshot(subdf, sub_window)
        day_patterns = build_patterns(day_price, subdf)
        day_selected = patterns_for_mode(day_patterns, mode)

        for p in day_selected:
            if p.status not in ("해당", "유사"):
                continue
            hits.append({
                "date": str(pd.Timestamp(current_date).date()),
                "pattern_key": p.key,
                "pattern_name": p.name,
                "status": p.status,
                "score": p.score,
                "max_score": p.max_score,
                "subtitle": p.subtitle,
                "comment": p.comment,
                "close": safe_int(day_price.get("close")),
                "amount_b": safe_float(day_price.get("amount_b")),
                "watermelon_value": safe_int(day_price.get("watermelon_value")),
                "watermelon_quality": safe_float(day_price.get("watermelon_quality")),
            })

    hits.sort(key=lambda x: (x["date"], x["score"], x["pattern_name"]))
    return hits


def summarize_pattern_hits(hits: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not hits:
        return {
            "total_hits": 0,
            "dates_count": 0,
            "first_hit_date": "",
            "last_hit_date": "",
            "top_pattern_name": "",
            "top_pattern_hits": 0,
        }

    unique_dates = sorted({h["date"] for h in hits})
    counts: Dict[str, int] = {}
    for h in hits:
        counts[h["pattern_name"]] = counts.get(h["pattern_name"], 0) + 1

    top_pattern_name, top_pattern_hits = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0]
    return {
        "total_hits": len(hits),
        "dates_count": len(unique_dates),
        "first_hit_date": unique_dates[0],
        "last_hit_date": unique_dates[-1],
        "top_pattern_name": top_pattern_name,
        "top_pattern_hits": top_pattern_hits,
    }


def build_pattern_hits_table(hits: List[Dict[str, Any]]) -> str:
    if not hits:
        return '<div class="muted">요청구간 내 해당/유사 패턴 발생 이력이 없습니다.</div>'

    rows = []
    for hit in hits:
        status_cls = STATUS_CLASS.get(hit["status"], "fail")
        rows.append(
            f"<tr>"
            f"<td>{escape(hit['date'])}</td>"
            f"<td>{escape(hit['pattern_name'])}</td>"
            f"<td class='{status_cls}'>{escape(hit['status'])}</td>"
            f"<td class='mono'>{hit['score']}/{hit['max_score']}</td>"
            f"<td class='mono'>{fmt_int(hit['close'])}</td>"
            f"<td class='mono'>{fmt_float(hit['amount_b'], 1)}억</td>"
            f"<td class='mono'>{fmt_float(hit['watermelon_quality'], 1)}</td>"
            f"<td>{escape(hit['subtitle'])}</td>"
            f"</tr>"
        )
    return (
        "<div class='table-wrap'><table>"
        "<thead><tr><th>날짜</th><th>패턴</th><th>상태</th><th>점수</th><th>종가</th><th>거래대금</th><th>수박품질</th><th>설명</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
    )



PATTERN_VIZ = {
    "closing_bet": {"abbr": "종", "color": "#f59e0b", "name": "종가배팅"},
    "envelope_bet": {"abbr": "엔", "color": "#10b981", "name": "엔벨로프"},
    "dolbanji": {"abbr": "돌", "color": "#a78bfa", "name": "돌반지"},
    "pre_dolbanji": {"abbr": "예", "color": "#8b5cf6", "name": "예비돌반지"},
    "pre_dolbanji_lite": {"abbr": "라", "color": "#06b6d4", "name": "신규예비Lite"},
    "watermelon": {"abbr": "수", "color": "#f43f5e", "name": "수박"},
    "viper": {"abbr": "독", "color": "#22c55e", "name": "독사"},
    "yeokmae": {"abbr": "역", "color": "#38bdf8", "name": "역매공파"},
    "double_bottom": {"abbr": "쌍", "color": "#f97316", "name": "쌍바닥"},
}


def get_pattern_viz(pattern_key: str) -> Dict[str, str]:
    return PATTERN_VIZ.get(pattern_key, {"abbr": "?", "color": "#94a3b8", "name": pattern_key})


DISPLAY_PATTERN_KEYS = ("watermelon", "pre_dolbanji", "pre_dolbanji_lite", "envelope_bet", "dolbanji", "closing_bet")
DISPLAY_PATTERN_SET = set(DISPLAY_PATTERN_KEYS)
STATUS_PRIORITY = {"해당": 3, "유사": 2, "미해당": 1, "데이터부족": 0}


def visible_pattern_keys_for_mode(mode: str) -> List[str]:
    if mode == "all":
        return list(DISPLAY_PATTERN_KEYS)
    return [mode]


def filter_display_patterns(patterns: List[PatternResult], mode: str) -> List[PatternResult]:
    visible_keys = set(visible_pattern_keys_for_mode(mode))
    return [p for p in patterns if p.key in visible_keys]


def filter_display_hits(hits: List[Dict[str, Any]], mode: str) -> List[Dict[str, Any]]:
    visible_keys = set(visible_pattern_keys_for_mode(mode))
    return [h for h in hits if h.get("pattern_key") in visible_keys]


def build_pattern_line_legend(mode: str = "all") -> str:
    items = []
    keys = visible_pattern_keys_for_mode(mode)
    for key in keys:
        viz = get_pattern_viz(key)
        items.append(
            f'<span style="display:inline-flex;align-items:center;gap:6px;margin-right:12px;white-space:nowrap;">'
            f'<span style="display:inline-block;width:12px;height:3px;background:{viz["color"]};border-radius:99px;"></span>'
            f'<span>{escape(viz["abbr"])}={escape(viz["name"])}'
            f'</span>'
        )
    return ''.join(items)



def sparkline_svg(
    df: pd.DataFrame,
    pattern_hits: List[Dict[str, Any]] | None = None,
    mode: str = "all",
    width: int = 960,
    height: int = 500,
) -> str:
    sub = df.tail(120).copy()
    env20 = calc_envelope(df, 20, ENV20_PCT)
    env40 = calc_envelope(df, 40, ENV40_PCT)

    sub["ENV20_LOWER"] = env20["lower"].tail(len(sub)).values
    sub["ENV40_LOWER"] = env40["lower"].tail(len(sub)).values

    price_top = 24
    price_bottom = 318
    score_top = 356
    score_bottom = height - 38
    left, right = 22, width - 22

    price_cols = ["Close", "MA20", "MA60", "MA200", "BB40_UP", "BB40_DN", "ENV20_LOWER", "ENV40_LOWER"]
    values = []
    for col in price_cols:
        if col in sub.columns:
            values.extend(pd.to_numeric(sub[col], errors="coerce").dropna().tolist())
    if not values:
        return ""

    min_v = min(values)
    max_v = max(values)
    price_rng = max(max_v - min_v, 1e-9)

    score_vals = pd.to_numeric(sub.get("WATERMELON_QUALITY", pd.Series([], dtype="float64")), errors="coerce").fillna(0)
    score_max = max(8.0, float(score_vals.max()) + 1.0)

    def x_pos(i: int, count: int) -> float:
        if count <= 1:
            return left
        return left + (right - left) * i / (count - 1)

    def price_y(v: float) -> float:
        return price_top + (price_bottom - price_top) * (1 - (v - min_v) / price_rng)

    def score_y(v: float) -> float:
        return score_top + (score_bottom - score_top) * (1 - (v / score_max))

    def series_points(series: pd.Series, mapper) -> List[str]:
        pts = []
        arr = pd.to_numeric(series, errors="coerce").tolist()
        for i, v in enumerate(arr):
            if pd.isna(v):
                pts.append("")
            else:
                pts.append(f"{x_pos(i, len(arr)):.1f},{mapper(float(v)):.1f}")
        return pts

    def polyline(points: List[str], color: str, stroke_width: int = 2, dash: str = "", opacity: float = 1.0) -> str:
        segs, cur = [], []
        for p in points:
            if p:
                cur.append(p)
            else:
                if len(cur) >= 2:
                    segs.append(cur)
                cur = []
        if len(cur) >= 2:
            segs.append(cur)
        lines = []
        for seg in segs:
            dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
            lines.append(
                f'<polyline points="{" ".join(seg)}" fill="none" stroke="{color}" '
                f'stroke-width="{stroke_width}" stroke-linejoin="round" stroke-linecap="round" '
                f'opacity="{opacity}"{dash_attr}/>'
            )
        return "".join(lines)

    close_points = series_points(sub["Close"], price_y)
    ma20_points = series_points(sub["MA20"], price_y)
    ma60_points = series_points(sub["MA60"], price_y)
    ma200_points = series_points(sub["MA200"], price_y)
    bb40_up_points = series_points(sub["BB40_UP"], price_y)
    bb40_dn_points = series_points(sub["BB40_DN"], price_y)
    env20_points = series_points(sub["ENV20_LOWER"], price_y)
    env40_points = series_points(sub["ENV40_LOWER"], price_y)
    wm_quality_points = series_points(sub["WATERMELON_QUALITY"], score_y)

    last_close = safe_float(sub["Close"].iloc[-1])
    first_close = safe_float(sub["Close"].iloc[0])
    close_color = "#22c55e" if last_close >= first_close else "#ef4444"

    bb_up = pd.to_numeric(sub["BB40_UP"], errors="coerce").tolist()
    bb_dn = pd.to_numeric(sub["BB40_DN"], errors="coerce").tolist()
    band_up, band_dn = [], []
    for i, v in enumerate(bb_up):
        if pd.notna(v):
            band_up.append(f"{x_pos(i, len(bb_up)):.1f},{price_y(float(v)):.1f}")
    for i in range(len(bb_dn) - 1, -1, -1):
        v = bb_dn[i]
        if pd.notna(v):
            band_dn.append(f"{x_pos(i, len(bb_dn)):.1f},{price_y(float(v)):.1f}")

    band_polygon = ""
    if len(band_up) >= 2 and len(band_dn) >= 2:
        band_polygon = f'<polygon points="{" ".join(band_up + band_dn)}" fill="rgba(99,102,241,0.10)" stroke="none"/>'

    def legend_item(x: int, y: int, color: str, label: str, dash: str = "") -> str:
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        return (
            f'<line x1="{x}" y1="{y}" x2="{x+18}" y2="{y}" stroke="{color}" stroke-width="3"{dash_attr}/>'
            f'<text x="{x+24}" y="{y+4}" fill="#d9e6f7" font-size="12">{escape(label)}</text>'
        )

    score_markers = []
    for i, (_, row) in enumerate(sub.iterrows()):
        x = x_pos(i, len(sub))
        q = safe_float(row.get("WATERMELON_QUALITY"))
        y = score_y(q)
        if safe_int(row.get("WATERMELON_GREEN")) == 1:
            score_markers.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.4" fill="#22c55e" opacity="0.9"/>')
        if safe_int(row.get("WATERMELON_RED")) == 1:
            score_markers.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.6" fill="#ef4444" opacity="0.95"/>')
        if safe_int(row.get("WATERMELON_RED_NEW")) == 1:
            score_markers.append(f'<line x1="{x:.1f}" y1="{score_bottom:.1f}" x2="{x:.1f}" y2="{y:.1f}" stroke="#ef4444" stroke-width="1.5" opacity="0.6" stroke-dasharray="4 3"/>')

    # 패턴 세로선 + 상단 약어 라벨
    vertical_markers = []
    hit_dates: Dict[str, List[Dict[str, Any]]] = {}
    visible_hits = filter_display_hits(pattern_hits or [], mode)
    if visible_hits:
        sub_dates = {str(pd.Timestamp(idx).date()) for idx in sub.index}
        for hit in visible_hits:
            d = str(hit.get("date", ""))
            if d in sub_dates:
                hit_dates.setdefault(d, []).append(hit)

    label_levels = [price_top + 10, price_top + 26, price_top + 42]
    for i, dt in enumerate([str(pd.Timestamp(idx).date()) for idx in sub.index]):
        hits = hit_dates.get(dt, [])
        if not hits:
            continue
        x = x_pos(i, len(sub))
        ordered_hits = sorted(
            hits,
            key=lambda h: (STATUS_PRIORITY.get(h.get("status", "미해당"), 0), h.get("score", 0), h.get("pattern_name", "")),
            reverse=True,
        )
        unique_keys: List[str] = []
        for hit in ordered_hits:
            key = str(hit.get("pattern_key", ""))
            if key and key not in unique_keys:
                unique_keys.append(key)

        lead_key = unique_keys[0]
        line_color = get_pattern_viz(lead_key)["color"]
        vertical_markers.append(
            f'<line x1="{x:.1f}" y1="{price_top:.1f}" x2="{x:.1f}" y2="{score_bottom:.1f}" '
            f'stroke="{line_color}" stroke-width="1.3" opacity="0.5" stroke-dasharray="4 4"/>'
        )

        show_keys = unique_keys[:3]
        if len(unique_keys) > 3:
            show_keys.append(f'+{len(unique_keys)-3}')
        for j, key in enumerate(show_keys):
            y = label_levels[j % len(label_levels)]
            if key.startswith('+'):
                label = key
                color = line_color
            else:
                viz = get_pattern_viz(key)
                label = viz["abbr"]
                color = viz["color"]
            vertical_markers.append(
                f'<rect x="{x-8:.1f}" y="{y-10:.1f}" width="16" height="12" rx="4" fill="{color}" opacity="0.95"/>'
                f'<text x="{x:.1f}" y="{y-1:.1f}" fill="#07101d" font-size="9" font-weight="800" text-anchor="middle">{escape(label)}</text>'
            )

    pattern_legends = []
    base_x = 370
    legend_y = 334
    legend_gap = 72
    legend_keys = visible_pattern_keys_for_mode(mode)
    for idx, key in enumerate(legend_keys[:7]):
        viz = get_pattern_viz(key)
        pattern_legends.append(legend_item(base_x + idx * legend_gap, legend_y, viz["color"], f'{viz["abbr"]}={viz["name"]}'))

    legends = [
        legend_item(26, 24, close_color, "종가"),
        legend_item(120, 24, "#f59e0b", "MA20"),
        legend_item(210, 24, "#38bdf8", "MA60"),
        legend_item(300, 24, "#a78bfa", "MA200"),
        legend_item(400, 24, "#818cf8", "BB40 상단"),
        legend_item(510, 24, "#6366f1", "BB40 하단"),
        legend_item(620, 24, "#10b981", "Env20 하단", "6 4"),
        legend_item(760, 24, "#f97316", "Env40 하단", "6 4"),
        legend_item(26, 334, "#f43f5e", "수박 품질"),
        legend_item(140, 334, "#22c55e", "초록 점등"),
        legend_item(255, 334, "#ef4444", "빨강 점등"),
    ]
    legends.extend(pattern_legends)

    score_last = safe_float(sub["WATERMELON_QUALITY"].iloc[-1]) if "WATERMELON_QUALITY" in sub.columns else 0.0

    return f"""
    <svg class="chart-svg" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none">
      <rect x="0" y="0" width="{width}" height="{height}" fill="transparent"/>
      <line x1="{left}" y1="{price_bottom}" x2="{right}" y2="{price_bottom}" stroke="#24385d" stroke-width="1"/>
      <line x1="{left}" y1="{price_top}" x2="{left}" y2="{price_bottom}" stroke="#24385d" stroke-width="1"/>
      <line x1="{left}" y1="{score_bottom}" x2="{right}" y2="{score_bottom}" stroke="#24385d" stroke-width="1"/>
      <line x1="{left}" y1="{score_top}" x2="{left}" y2="{score_bottom}" stroke="#24385d" stroke-width="1"/>
      <line x1="{left}" y1="{score_y(4):.1f}" x2="{right}" y2="{score_y(4):.1f}" stroke="#3b4e74" stroke-width="1" stroke-dasharray="5 4"/>
      <line x1="{left}" y1="{score_y(7):.1f}" x2="{right}" y2="{score_y(7):.1f}" stroke="#5b4968" stroke-width="1" stroke-dasharray="5 4"/>
      {band_polygon}
      {''.join(vertical_markers)}
      {polyline(bb40_up_points, "#818cf8", 1)}
      {polyline(bb40_dn_points, "#6366f1", 1)}
      {polyline(env20_points, "#10b981", 2, "6 4", 0.95)}
      {polyline(env40_points, "#f97316", 2, "6 4", 0.95)}
      {polyline(ma200_points, "#a78bfa", 2)}
      {polyline(ma60_points, "#38bdf8", 2)}
      {polyline(ma20_points, "#f59e0b", 2)}
      {polyline(close_points, close_color, 3)}
      {polyline(wm_quality_points, "#f43f5e", 2.4)}
      {''.join(score_markers)}
      <text x="{left}" y="{price_bottom + 18}" fill="#9db2d2" font-size="12">최근 120봉 · 종가 / MA / BB40 / Envelope 하단 · 세로선 상단 약어로 패턴 구분</text>
      <text x="{left}" y="{score_bottom + 18}" fill="#9db2d2" font-size="12">수박 품질 점수 · 4점/7점 기준선 · 초록/빨강 점등 마커</text>
      <text x="{width-24}" y="24" fill="#f8fbff" font-size="12" text-anchor="end">종가 {fmt_int(last_close)}</text>
      <text x="{width-24}" y="334" fill="#f8fbff" font-size="12" text-anchor="end">수박품질 {fmt_float(score_last, 1)}</text>
      <text x="6" y="{price_bottom:.1f}" fill="#8aa0bf" font-size="11">{fmt_int(min_v)}</text>
      <text x="6" y="{price_top+4:.1f}" fill="#8aa0bf" font-size="11">{fmt_int(max_v)}</text>
      <text x="6" y="{score_bottom:.1f}" fill="#8aa0bf" font-size="11">0</text>
      <text x="6" y="{score_y(4):.1f}" fill="#8aa0bf" font-size="11">4</text>
      <text x="6" y="{score_y(7):.1f}" fill="#8aa0bf" font-size="11">7</text>
      <text x="6" y="{score_top+4:.1f}" fill="#8aa0bf" font-size="11">{fmt_float(score_max, 0)}</text>
      {''.join(legends)}
    </svg>
    """


def render_html(result: Dict[str, Any]) -> str:
    price = result["price"]
    patterns: List[PatternResult] = result["patterns"]
    df = result["df"]
    pattern_hits: List[Dict[str, Any]] = result.get("pattern_hits", [])
    visible_mode = result.get("mode", "all")
    visible_patterns = filter_display_patterns(patterns, visible_mode)
    visible_hits = filter_display_hits(pattern_hits, visible_mode)
    hit_summary: Dict[str, Any] = summarize_pattern_hits(visible_hits)

    nav = "".join(f'<button class="nav-chip" onclick="scrollToId(\'sec-{escape(p.key)}\')">{escape(p.name)}</button>' for p in visible_patterns)

    display_summary = build_summary(result["name"], visible_patterns)
    display_smart_comment = build_smart_comment(price, visible_patterns, result["name"])

    research_panel = f"""
    <section class="card">
      <div class="section-title">다음 종목 바로 재검색</div>
      <div class="muted" style="margin-bottom:10px;">
        현재 결과 페이지에서 바로 다른 종목 분석을 다시 요청할 수 있습니다.
      </div>

      <div class="research-grid">
        <input id="reCode" placeholder="종목코드 예: 005930" inputmode="numeric" />
        <input id="reName" placeholder="종목명 예: 삼성전자" />
      </div>

      <select id="reMode" style="margin-top:10px;">
        <option value="all">all</option>
        <option value="closing_bet">closing_bet</option>
        <option value="envelope_bet">envelope_bet</option>
        <option value="dolbanji">dolbanji</option>
        <option value="pre_dolbanji">pre_dolbanji</option>
        <option value="pre_dolbanji_lite">pre_dolbanji_lite</option>
        <option value="pre_dolbanji_hts_exact">pre_dolbanji_hts_exact</option>
        <option value="watermelon">watermelon</option>
        <option value="viper">viper</option>
        <option value="yeokmae">yeokmae</option>
        <option value="double_bottom">double_bottom</option>
      </select>

      <div class="research-actions">
        <button class="action-btn" onclick="runFromResultPage()">바로 재검색 실행</button>
        <button class="action-btn secondary" onclick="history.back()">이전 페이지로</button>
      </div>

      <div id="reStatus" class="muted" style="margin-top:10px;">대기 중</div>
    </section>
    """

    metric_items = [
        ("현재가", fmt_int(price["close"])),
        ("상장 후 데이터", f'{price["bars"]}봉'),
        ("거래대금", f'{price["amount_b"]}억'),
        ("거래량 배수", f'{price["vol_ratio"]}배'),
        ("이격도", f'{price["disparity"]}'),
        ("20일 전고점 근접도", f'{price["near_high20_pct"]}%'),
        ("윗꼬리(몸통)", f'{price["upper_wick_body_pct"]}%'),
        ("RSI14", f'{price["rsi14"]}'),
        ("MFI14", f'{price["mfi14"]}'),
        ("OBV 기울기(10)", f'{price["obv_slope_10"]}%'),
        ("MACD HIST", f'{price["macd_hist"]}'),
        ("+DI / -DI", f'{price["plus_di"]} / {price["minus_di"]}'),
        ("ADX", f'{price["adx"]}'),
        ("V4 3박자 점수", f'{price.get("dante_v4_score", 0)}'),
        ("V4 등급", f'{price.get("dante_v4_grade", "C")}'),
        ("예비돌반지 점수", f'{price.get("pre_dolbanji_score", 0)}'),
        ("예비돌반지 등급", f'{price.get("pre_dolbanji_grade", "없음")}'),
        ("예비돌반지 대표형", f'{price.get("pre_dolbanji_best", "") or "없음"}'),
        ("예비돌반지 구조전환", f'{price.get("pre_dolbanji_trend_score", 0)}/4'),
        ("신규예비Lite", "통과" if price.get("pre_dolbanji_lite", False) else "미통과"),
        ("신규예비Lite 점수", f'{price.get("pre_dolbanji_lite_score", 0)}'),
        ("신규예비Lite 등급", f'{price.get("pre_dolbanji_lite_grade", "없음")}'),
        ("신규예비Lite 대표형", f'{price.get("pre_dolbanji_lite_best", "") or "없음"}'),
        ("신규예비Lite 구조전환", f'{price.get("pre_dolbanji_lite_trend_score", 0)}/4'),
        ("예비돌반지 HTS정확복제", "통과" if price.get("pre_dolbanji_hts_exact", False) else "미통과"),
        ("예비돌반지 HTS점수", f'{price.get("pre_dolbanji_hts_exact_score", 0)}/{price.get("pre_dolbanji_hts_exact_max_score", 10)}'),
        ("예비돌반지 HTS태그", f'{price.get("pre_dolbanji_hts_exact_tags", "") or "없음"}'),
        ("main7 엔진 N등급", f'{price.get("main7_engine_grade", "-")}'),
        ("main7 엔진 조합", f'{price.get("main7_engine_combo", "-")}'),
        ("main7 엔진 N점수", f'{price.get("main7_engine_score", 0)}'),
        ("main7 엔진 안전점수", f'{price.get("main7_engine_safe_score", 0)}'),
        ("main7 엔진 단계", f'{price.get("main7_engine_stage", "-")}'),
        ("기간대칭 점수", f'{price.get("sym_score_v4", 0)}'),
        ("파동에너지 점수", f'{price.get("energy_total_v4", 0)}'),
        ("수박 V4 상태", f'{price.get("watermelon_phase_v4", "NONE")}'),
        ("수박 V4 품질", f'{price.get("watermelon_quality_v4", 0)}'),
        ("수박 품질", f'{price["watermelon_quality"]}'),
        ("수박 단계", f'{price["watermelon_phase"]}'),
        ("수박 값", f'{price["watermelon_value"]}'),
        ("초록 점수", f'{price["watermelon_green_score"]}'),
        ("빨강 점수", f'{price["watermelon_red_score"]}'),
        ("BB40 폭", f'{price["bb40_width"]}'),
        ("MA200 이격", f'{price["ma200_gap_pct"]}%'),
        ("Env20 하단 괴리", f'{price["env20_pct"]}%'),
        ("Env40 하단 괴리", f'{price["env40_pct"]}%'),
        ("초단기수렴 5/10/20", f'{price["ultra_ma_conv_pct"]}% / {"해당" if price["ultra_ma_conv"] else "미달"}'),
        ("단기수렴 5/20/60", f'{price["short_ma_conv_pct"]}% / {"해당" if price["short_ma_conv"] else "미달"}'),
        ("구조수렴 20/60/112", f'{price["struct_ma_conv_pct"]}% / {"해당" if price["struct_ma_conv"] else "미달"}'),
        ("브릿지수렴 5/20/112", f'{price["bridge_ma_conv_pct"]}% / {"해당" if price["bridge_ma_conv"] else "미달"}'),
        ("구조접속 5/60/112", f'{price["connect_ma_conv_pct"]}% / {"해당" if price["connect_ma_conv"] else "미달"}'),
        ("초강력MA수렴", "해당" if price["super_ma_conv"] else "미달"),
    ]
    metric_cards = "".join(
        f'<div class="metric"><div class="metric-label">{escape(k)}</div><div class="metric-value">{escape(v)}</div></div>'
        for k, v in metric_items
    )

    hit_summary_cards = ""
    if hit_summary.get("total_hits", 0) > 0:
        hit_summary_items = [
            ("발생건수", str(hit_summary.get("total_hits", 0))),
            ("발생일수", str(hit_summary.get("dates_count", 0))),
            ("첫 발생일", hit_summary.get("first_hit_date", "-") or "-"),
            ("마지막 발생일", hit_summary.get("last_hit_date", "-") or "-"),
            ("가장 자주 나온 패턴", hit_summary.get("top_pattern_name", "-") or "-"),
            ("그 패턴 횟수", str(hit_summary.get("top_pattern_hits", 0))),
        ]
        hit_summary_cards = "".join(
            f'<div class="metric"><div class="metric-label">{escape(k)}</div><div class="metric-value">{escape(v)}</div></div>'
            for k, v in hit_summary_items
        )

    pattern_blocks = []
    all_rows = []

    for p in visible_patterns:
        check_rows = []
        for c in p.checks:
            cls = "pass" if c.ok else "fail"
            text = "통과" if c.ok else "미달"
            check_rows.append(
                f"<tr><td>{escape(c.label)}</td><td class='mono'>{escape(c.current)}</td><td class='mono'>{escape(c.target)}</td><td class='{cls}'>{text}</td><td>{escape(c.reason)}</td></tr>"
            )
            all_rows.append(
                f"<tr><td>{escape(p.name)}</td><td>{escape(c.label)}</td><td class='mono'>{escape(c.current)}</td><td class='mono'>{escape(c.target)}</td><td class='{cls}'>{text}</td><td>{escape(c.reason)}</td></tr>"
            )

        pattern_blocks.append(
            f"""
            <section class="card" id="sec-{escape(p.key)}">
              <div class="pattern-head">
                <div>
                  <div class="pattern-title">{escape(p.name)}</div>
                  <div class="muted">{escape(p.subtitle)}</div>
                </div>
                <div class="badge {STATUS_CLASS.get(p.status, 'fail')}">{escape(p.status)}</div>
              </div>
              <div class="score-line">완성도 {p.score}/{p.max_score}</div>
              <p class="pattern-comment">{escape(p.comment)}</p>
              <details open>
                <summary>조건 수치 보기</summary>
                <div class="table-wrap">
                  <table>
                    <thead><tr><th>조건</th><th>현재값</th><th>기준</th><th>결과</th><th>사유</th></tr></thead>
                    <tbody>{''.join(check_rows)}</tbody>
                  </table>
                </div>
              </details>
            </section>
            """
        )

    hits_table = build_pattern_hits_table(visible_hits)

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <title>{escape(result['name'])}({escape(result['code'])}) 멀티패턴 진단</title>
  <style>
    :root {{ --line:#223556; --text:#e5ecf6; --muted:#8aa0bf; --green:#22c55e; --red:#ef4444; --amber:#f59e0b; }}
    * {{ box-sizing:border-box; }}
    html, body {{ margin:0; padding:0; background:linear-gradient(180deg,#07101d 0%,#0b1325 100%); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Noto Sans KR',sans-serif; }}
    body {{ padding-bottom:24px; }}
    .hero {{ position:sticky; top:0; z-index:10; backdrop-filter: blur(14px); background:rgba(8,17,31,.9); border-bottom:1px solid rgba(34,53,86,.7); padding:14px 0 10px; margin-bottom:12px; }}
    .hero-inner, .app {{ max-width:1024px; margin:0 auto; padding:0 16px; }}
    .title {{ font-size:24px; font-weight:900; margin:0; }}
    .sub {{ color:var(--muted); font-size:13px; margin-top:4px; line-height:1.55; }}
    .pill-row, .nav-row {{ display:flex; gap:8px; overflow:auto; white-space:nowrap; padding:10px 0 2px; scrollbar-width:none; }}
    .pill-row::-webkit-scrollbar, .nav-row::-webkit-scrollbar {{ display:none; }}
    .pill, .nav-chip {{ border:1px solid var(--line); background:#12203a; color:var(--text); padding:8px 12px; border-radius:999px; font-size:13px; }}
    .nav-chip {{ cursor:pointer; }}
    .card {{ background:linear-gradient(180deg,rgba(17,31,57,.98),rgba(12,24,44,.98)); border:1px solid var(--line); border-radius:22px; padding:16px; margin-bottom:14px; }}
    .section-title {{ font-size:18px; font-weight:800; margin:0 0 12px; }}
    .metric-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }}
    .metric {{ background:#0b1730; border:1px solid var(--line); border-radius:18px; padding:12px; min-height:84px; }}
    .metric-label {{ color:var(--muted); font-size:12px; }}
    .metric-value {{ font-size:24px; font-weight:900; margin-top:6px; line-height:1.2; word-break:keep-all; }}
    .chart-svg {{ width:100%; height:360px; display:block; }}
    .pattern-head {{ display:flex; justify-content:space-between; gap:12px; align-items:flex-start; }}
    .pattern-title {{ font-size:20px; font-weight:900; }}
    .badge {{ min-width:62px; text-align:center; padding:8px 12px; border-radius:999px; font-weight:900; font-size:13px; }}
    .pass {{ color:var(--green); }} .fail {{ color:var(--red); }} .warn {{ color:var(--amber); }} .na {{ color:#94a3b8; }}
    .badge.pass {{ background:rgba(34,197,94,.12); border:1px solid rgba(34,197,94,.28); }}
    .badge.fail {{ background:rgba(239,68,68,.12); border:1px solid rgba(239,68,68,.28); }}
    .badge.warn {{ background:rgba(245,158,11,.12); border:1px solid rgba(245,158,11,.28); }}
    .badge.na {{ background:rgba(148,163,184,.12); border:1px solid rgba(148,163,184,.28); }}
    .score-line {{ margin-top:8px; color:#c9d6ea; font-size:14px; }}
    .pattern-comment {{ margin-top:10px; line-height:1.65; color:#dde6f6; }}
    details summary {{ cursor:pointer; color:#bdd0ec; font-weight:700; margin:8px 0 12px; }}
    .table-wrap {{ overflow:auto; border-radius:16px; border:1px solid var(--line); }}
    table {{ width:100%; border-collapse:collapse; min-width:760px; background:#0b1730; }}
    th, td {{ padding:11px 10px; border-bottom:1px solid #1f3150; text-align:left; vertical-align:top; font-size:13px; }}
    th {{ color:#c7d5ea; background:#0d1a33; position:sticky; top:0; }}
    .mono {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }}
    .muted {{ color:var(--muted); }}
    .research-grid{{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
    .research-actions{{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-top:10px; }}
    .action-btn{{ width:100%; border-radius:16px; padding:14px 14px; font-weight:800; background:linear-gradient(180deg,#3b82f6,#2463d7); color:#fff; border:none; cursor:pointer; }}
    .action-btn.secondary{{ background:#13203b; border:1px solid #243759; }}
    @media (max-width:720px){{ .research-grid, .research-actions{{ grid-template-columns:1fr; }} }}
    @media (min-width:768px) {{ .metric-grid {{ grid-template-columns:repeat(4,minmax(0,1fr)); }} .chart-svg {{ height:430px; }} }}
  </style>
  <script>
    const DEFAULT_WORKER_URL = "https://stock-hunter-trigger.ehdud6728.workers.dev";

    function scrollToId(id) {{
      const el = document.getElementById(id);
      if (el) el.scrollIntoView({{ behavior:'smooth', block:'start' }});
    }}

    function getWorkerUrl() {{
      return DEFAULT_WORKER_URL.replace(/\\/+$/, "");
    }}

    async function runFromResultPage() {{
      const code = (document.getElementById("reCode")?.value || "").trim();
      const name = (document.getElementById("reName")?.value || "").trim();
      const mode = (document.getElementById("reMode")?.value || "all").trim();
      const statusEl = document.getElementById("reStatus");

      if (!code) {{
        statusEl.textContent = "종목코드를 입력하세요.";
        return;
      }}

      const workerUrl = getWorkerUrl();
      statusEl.textContent = "실행 요청 중...";

      try {{
        const resp = await fetch(workerUrl, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            stock_code: code,
            stock_name: name,
            mode: mode
          }})
        }});

        let data = null;
        try {{
          data = await resp.json();
        }} catch (e) {{
          data = null;
        }}

        if (!resp.ok) {{
          statusEl.textContent = `실행 실패: HTTP ${{resp.status}}`;
          console.log(data);
          return;
        }}

        if (!data || !data.ok) {{
          statusEl.textContent = "실행 실패: 응답값을 확인하세요.";
          console.log(data);
          return;
        }}

        const resultUrl = data.page_url || data.pages_url || data.result_url || data.report_url || "";
        if (resultUrl) {{
          statusEl.textContent = "실행 성공. 새 결과 페이지를 엽니다.";
          window.open(resultUrl, "_blank", "noopener");
        }} else if (data.html_url) {{
          statusEl.textContent = "실행 성공. GitHub Actions 페이지를 엽니다. 완료 후 이 페이지를 새로고침하세요.";
          window.open(data.html_url, "_blank", "noopener");
        }} else if (data.run_url) {{
          statusEl.textContent = "실행 성공. Actions 실행 페이지를 엽니다.";
          window.open(data.run_url, "_blank", "noopener");
        }} else {{
          statusEl.textContent = "실행 성공. 결과 URL이 아직 준비되지 않았습니다.";
        }}

      }} catch (err) {{
        statusEl.textContent = "Worker 연결 실패";
        console.log(err);
      }}
    }}

    document.addEventListener("DOMContentLoaded", () => {{
      const reCode = document.getElementById("reCode");
      const reName = document.getElementById("reName");
      const reMode = document.getElementById("reMode");

      if (reCode) reCode.value = "";
      if (reName) reName.value = "";
      if (reMode) reMode.value = "all";
    }});
  </script></head>
<body>
  <div class="hero"><div class="hero-inner">
      <h1 class="title">{escape(result['name'])} <span style="color:var(--muted)">({escape(result['code'])})</span></h1>
      <div class="sub">
        생성시각 {escape(result['generated_at'])} · 기준봉 {escape(price['date'])}
        · 요청구간 {escape(price['target_start_date'])} ~ {escape(price['target_end_date'])}
        · 계산데이터 {escape(price['fetch_start_date'])} ~ {escape(price['fetch_end_date'])}
      </div>
      <div class="pill-row">
        <div class="pill">분석모드 {escape(result['mode'])}</div>
        <div class="pill">현재가 {fmt_int(price['close'])}</div>
        <div class="pill">데이터 {price['bars']}봉</div>
        <div class="pill">거래대금 {price['amount_b']}억</div>
      </div>
      <div class="nav-row">{nav}</div>
  </div></div>
  <div class="app">
    {research_panel}
    <section class="card"><div class="section-title">핵심 요약</div><p>{escape(display_summary)}</p><p><strong>종합 코멘트</strong><br>{escape(display_smart_comment)}</p><p><strong>MA수렴 해석</strong><br>{escape(build_ma_convergence_comment(price))}</p></section>
    <section class="card"><div class="section-title">가격 구조 차트 + 수박 점수 + 패턴 세로선</div><div class="muted" style="margin-bottom:10px;">세로선 상단 약어로 어떤 패턴이 발생했는지 구분합니다. 같은 날짜에 여러 패턴이 겹치면 대표 패턴 색 세로선과 복수 약어가 함께 표시됩니다.</div>{sparkline_svg(df, visible_hits, result.get("mode", "all"))}</section>
    <section class="card"><div class="section-title">기본 수치</div><div class="metric-grid">{metric_cards}</div></section>
    <section class="card">
      <div class="section-title">요청구간 패턴 발생 이력</div>
      <div class="muted" style="margin-bottom:10px;">요청구간 안에서 날짜별로 해당/유사 판정이 나온 패턴만 뽑았습니다. mode=all이면 여러 패턴이 같은 날짜에 함께 나올 수 있습니다.</div>
      {'<div class="metric-grid" style="margin-bottom:12px;">' + hit_summary_cards + '</div>' if hit_summary_cards else ''}
      {hits_table}
    </section>
    {''.join(pattern_blocks)}
    <section class="card"><div class="section-title">전체 조건표</div><div class="table-wrap"><table><thead><tr><th>패턴</th><th>조건</th><th>현재값</th><th>기준</th><th>결과</th><th>사유</th></tr></thead><tbody>{''.join(all_rows)}</tbody></table></div></section>
  </div>
</body>
</html>"""

def pattern_results_to_json(patterns: List[PatternResult]) -> List[Dict[str, Any]]:
    return [
        {
            "key": p.key,
            "name": p.name,
            "status": p.status,
            "score": p.score,
            "max_score": p.max_score,
            "subtitle": p.subtitle,
            "comment": p.comment,
            "checks": [c.__dict__ for c in p.checks],
        }
        for p in patterns
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", required=True)
    ap.add_argument("--name", default="")
    ap.add_argument("--mode", default="all")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--days", type=int, default=450, help="계산용 과거 데이터 조회 일수")
    ap.add_argument("--start-date", default="", help="결과를 보고 싶은 시작일 YYYY-MM-DD")
    ap.add_argument("--end-date", default="", help="결과를 보고 싶은 기준일 YYYY-MM-DD")
    ap.add_argument("--min-bars", type=int, default=40, help="지표 계산에 필요한 최소 거래봉 수")
    ap.add_argument("--output-json", default="reports/latest_result.json")
    ap.add_argument("--output-html", default="site/index.html")
    args = ap.parse_args()

    if args.mode not in MODE_CHOICES:
        raise SystemExit(f"지원하지 않는 mode 입니다: {args.mode}")

    code = normalize_code(args.code)
    name = detect_name(code, args.name)

    df, window = load_price_history(
        code=code,
        start_date=args.start_date,
        end_date=args.end_date,
        days=args.days,
        min_bars=args.min_bars,
    )
    df = add_indicators(df)
    df = apply_dante_v4(df)


price = build_snapshot(df, window)

engine_hits = []
engine_hit = {}
try:
    engine_hits = analyze_single_stock_with_main7_engine(code, name, end_date=window["target_end"])
    engine_hit = engine_hits[0] if engine_hits else {}
except Exception:
    engine_hits = []
    engine_hit = {}

if engine_hit:
    price["main7_engine_grade"] = str(engine_hit.get("N등급", ""))
    price["main7_engine_combo"] = str(engine_hit.get("N조합", ""))
    price["main7_engine_score"] = safe_int(engine_hit.get("N점수", 0))
    price["main7_engine_safe_score"] = safe_int(engine_hit.get("안전점수", 0))
    price["main7_engine_stage"] = str(engine_hit.get("단계상태", ""))
    price["main7_engine_tags"] = str(engine_hit.get("N구분", ""))
    patterns = build_patterns(price, df)
    selected = patterns_for_mode(patterns, args.mode)

    summary = build_summary(name, selected)
    smart_comment = build_smart_comment(price, selected, name)
    pattern_hits = scan_pattern_history(df, window, args.mode, min_bars=args.min_bars)
    pattern_hit_summary = summarize_pattern_hits(pattern_hits)

    result = {
        "code": code,
        "name": name,
        "mode": args.mode,
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        "price": price,
        "summary": summary,
        "smart_comment": smart_comment,
        "patterns": selected,
        "df": df,
        "pattern_hits": pattern_hits,
        "pattern_hit_summary": pattern_hit_summary,
        "main7_engine_result": engine_hit,
    }

    out_json = Path(args.output_json)
    out_html = Path(args.output_html)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_html.parent.mkdir(parents=True, exist_ok=True)

    json_payload = {
        "code": code,
        "name": name,
        "mode": args.mode,
        "generated_at": result["generated_at"],
        "price": price,
        "summary": summary,
        "smart_comment": smart_comment,
        "patterns": pattern_results_to_json(selected),
        "pattern_hits": pattern_hits,
        "pattern_hit_summary": pattern_hit_summary,
        "main7_engine_result": engine_hit,
    }

    out_json.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_html.write_text(render_html(result), encoding="utf-8")

    print(f"saved: {out_json}")
    print(f"saved: {out_html}")


if __name__ == "__main__":
    main()
