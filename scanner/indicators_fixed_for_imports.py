from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd


REQUIRED_OHLCV = ("Open", "High", "Low", "Close", "Volume")


def _pick_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def standardize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """한글/영문 혼용 OHLCV 컬럼을 영문 표준으로 맞춘다."""
    if df is None or df.empty:
        return pd.DataFrame(columns=list(REQUIRED_OHLCV))

    src = df.copy()
    alias = {
        "Open": ["Open", "open", "시가"],
        "High": ["High", "high", "고가"],
        "Low": ["Low", "low", "저가"],
        "Close": ["Close", "close", "종가", "현재가"],
        "Volume": ["Volume", "volume", "거래량"],
        "Amount": ["Amount", "amount", "거래대금"],
    }
    out = pd.DataFrame(index=src.index)
    for target, candidates in alias.items():
        col = _pick_col(src, *candidates)
        if col is not None:
            out[target] = pd.to_numeric(
                src[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )
    for col in REQUIRED_OHLCV:
        if col not in out.columns:
            out[col] = np.nan
    if "Amount" in out.columns:
        out["Amount"] = out["Amount"].fillna(out["Close"] * out["Volume"])
    else:
        out["Amount"] = out["Close"] * out["Volume"]
    return out


def ma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(period, min_periods=period).mean()


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def slope_pct(series: pd.Series, lookback: int = 5) -> pd.Series:
    base = series.shift(lookback)
    return np.where(base.abs() > 1e-9, (series - base) / base * 100.0, np.nan)


def true_range(df: pd.DataFrame) -> pd.Series:
    high = df["High"]
    low = df["Low"]
    prev_close = df["Close"].shift(1)
    return pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    return true_range(df).rolling(period, min_periods=period).mean()


def obv(df: pd.DataFrame) -> pd.Series:
    close = df["Close"]
    vol = df["Volume"].fillna(0)
    direction = np.sign(close.diff().fillna(0))
    return (direction * vol).cumsum()


def bollinger(close: pd.Series, period: int, std_mul: float = 2.0) -> pd.DataFrame:
    mid = ma(close, period)
    std = close.rolling(period, min_periods=period).std(ddof=0)
    upper = mid + std * std_mul
    lower = mid - std * std_mul
    width = np.where(mid.abs() > 1e-9, (upper - lower) / mid * 100.0, np.nan)
    return pd.DataFrame(
        {
            f"bb{period}_mid": mid,
            f"bb{period}_upper": upper,
            f"bb{period}_lower": lower,
            f"bb{period}_width": width,
        },
        index=close.index,
    )


def envelope(close: pd.Series, period: int, pct: float = 2.0) -> pd.DataFrame:
    mid = ma(close, period)
    upper = mid * (1.0 + pct / 100.0)
    lower = mid * (1.0 - pct / 100.0)
    width = np.where(mid.abs() > 1e-9, (upper - lower) / mid * 100.0, np.nan)
    return pd.DataFrame(
        {
            f"env{period}_mid": mid,
            f"env{period}_upper": upper,
            f"env{period}_lower": lower,
            f"env{period}_width": width,
        },
        index=close.index,
    )


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    diff = close.diff()
    up = diff.clip(lower=0)
    down = -diff.clip(upper=0)
    avg_up = up.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_down = down.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_up / avg_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


@dataclass
class BandSnapshot:
    name: str
    mid: float
    upper: float
    lower: float
    width: float
    close: float
    low: float
    high: float

    @property
    def lower_touch(self) -> bool:
        return self.low <= self.lower * 1.01

    @property
    def upper_touch(self) -> bool:
        return self.high >= self.upper * 0.995

    @property
    def below_mid(self) -> bool:
        return self.close < self.mid

    @property
    def above_mid(self) -> bool:
        return self.close >= self.mid


BAND_MAP = {
    "BB20": ("bb20_mid", "bb20_upper", "bb20_lower", "bb20_width"),
    "BB40": ("bb40_mid", "bb40_upper", "bb40_lower", "bb40_width"),
    "Env20": ("env20_mid", "env20_upper", "env20_lower", "env20_width"),
    "Env40": ("env40_mid", "env40_upper", "env40_lower", "env40_width"),
}


def build_band_snapshot(df: pd.DataFrame, band_name: str) -> Optional[BandSnapshot]:
    cols = BAND_MAP.get(band_name)
    if cols is None or df is None or df.empty:
        return None
    mid_col, upper_col, lower_col, width_col = cols
    needed = [mid_col, upper_col, lower_col, width_col, "Close", "Low", "High"]
    if any(col not in df.columns for col in needed):
        return None
    row = df.iloc[-1]
    return BandSnapshot(
        name=band_name,
        mid=float(row[mid_col] or 0),
        upper=float(row[upper_col] or 0),
        lower=float(row[lower_col] or 0),
        width=float(row[width_col] or 0),
        close=float(row["Close"] or 0),
        low=float(row["Low"] or 0),
        high=float(row["High"] or 0),
    )


def detect_squeeze(width: pd.Series, lookback: int = 120, q: float = 0.25) -> pd.Series:
    base = width.rolling(lookback, min_periods=max(20, lookback // 3)).quantile(q)
    return width <= base


def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """끼/파동 분석용 기본 지표를 한 번에 붙인다."""
    px = standardize_ohlcv(df)
    if px.empty:
        return px

    close = px["Close"]
    high = px["High"]
    low = px["Low"]
    volume = px["Volume"]

    px["ma5"] = ma(close, 5)
    px["ma10"] = ma(close, 10)
    px["ma20"] = ma(close, 20)
    px["ma40"] = ma(close, 40)
    px["ma60"] = ma(close, 60)
    px["ma112"] = ma(close, 112)
    px["ma224"] = ma(close, 224)
    px["ema20"] = ema(close, 20)
    px["atr14"] = atr(px, 14)
    px["atr20"] = atr(px, 20)
    px["obv"] = obv(px)
    px["obv_slope5"] = slope_pct(px["obv"], 5)
    px["obv_slope10"] = slope_pct(px["obv"], 10)
    px["rsi14"] = rsi(close, 14)
    px["ret1"] = close.pct_change(1) * 100.0
    px["ret3"] = close.pct_change(3) * 100.0
    px["ret5"] = close.pct_change(5) * 100.0
    px["ret10"] = close.pct_change(10) * 100.0
    px["body_pct"] = np.where(
        px["Open"].abs() > 1e-9,
        (close - px["Open"]) / px["Open"] * 100.0,
        np.nan,
    )
    px["upper_wick_pct"] = np.where(
        (close - px["Open"]).abs() > 1e-9,
        (high - np.maximum(close, px["Open"])) / (close - px["Open"]).abs() * 100.0,
        np.nan,
    )
    px["lower_wick_pct"] = np.where(
        (close - px["Open"]).abs() > 1e-9,
        (np.minimum(close, px["Open"]) - low) / (close - px["Open"]).abs() * 100.0,
        np.nan,
    )
    px["vol_ma5"] = ma(volume, 5)
    px["vol_ma20"] = ma(volume, 20)
    px["vol_ratio20"] = np.where(px["vol_ma20"] > 0, volume / px["vol_ma20"], np.nan)
    px["disparity20"] = np.where(px["ma20"] > 0, close / px["ma20"] * 100.0, np.nan)
    px["disparity40"] = np.where(px["ma40"] > 0, close / px["ma40"] * 100.0, np.nan)

    bb20 = bollinger(close, 20, 2.0)
    bb40 = bollinger(close, 40, 2.0)
    env20 = envelope(close, 20, 2.0)
    env40 = envelope(close, 40, 2.0)
    px = pd.concat([px, bb20, bb40, env20, env40], axis=1)

    px["bb20_squeeze"] = detect_squeeze(px["bb20_width"])
    px["bb40_squeeze"] = detect_squeeze(px["bb40_width"])
    px["env20_squeeze"] = detect_squeeze(px["env20_width"])
    px["env40_squeeze"] = detect_squeeze(px["env40_width"])

    px["bb20_lower_touch"] = low <= px["bb20_lower"] * 1.01
    px["bb20_upper_touch"] = high >= px["bb20_upper"] * 0.995
    px["bb40_lower_touch"] = low <= px["bb40_lower"] * 1.01
    px["bb40_upper_touch"] = high >= px["bb40_upper"] * 0.995
    px["env20_lower_touch"] = low <= px["env20_lower"] * 1.01
    px["env20_upper_touch"] = high >= px["env20_upper"] * 0.995
    px["env40_lower_touch"] = low <= px["env40_lower"] * 1.01
    px["env40_upper_touch"] = high >= px["env40_upper"] * 0.995

    px["range_pct"] = np.where(close.shift(1).abs() > 1e-9, (high - low) / close.shift(1) * 100.0, np.nan)
    px["impulse_day"] = (
        (px["body_pct"] >= 6.0)
        | (px["ret1"] >= 8.0)
        | ((px["vol_ratio20"] >= 2.2) & (px["ret1"] >= 5.0))
    )
    px["bullish_close"] = close >= px["Open"]
    px["recent_20d_high"] = high.rolling(20, min_periods=10).max()
    px["recent_20d_low"] = low.rolling(20, min_periods=10).min()
    px["from_20d_high_pct"] = np.where(
        px["recent_20d_high"] > 0,
        (close / px["recent_20d_high"] - 1.0) * 100.0,
        np.nan,
    )
    px["from_20d_low_pct"] = np.where(
        px["recent_20d_low"] > 0,
        (close / px["recent_20d_low"] - 1.0) * 100.0,
        np.nan,
    )
    return px


def recent_box(high: pd.Series, low: pd.Series, window: int = 20) -> Tuple[float, float, float]:
    sub_high = high.tail(window)
    sub_low = low.tail(window)
    if sub_high.empty or sub_low.empty:
        return 0.0, 0.0, 0.0
    hi = float(sub_high.max())
    lo = float(sub_low.min())
    mid = (hi + lo) / 2.0
    return lo, mid, hi


def regression_angle(series: pd.Series, window: int = 10) -> float:
    s = series.dropna().tail(window)
    if len(s) < max(3, window // 2):
        return 0.0
    y = s.to_numpy(dtype=float)
    x = np.arange(len(y), dtype=float)
    slope = np.polyfit(x, y, 1)[0]
    base = np.nanmean(np.abs(y)) or 1.0
    norm = slope / base * 100.0
    return float(norm)


import os


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, str(default))).strip().lower()
    return raw in ("1", "true", "t", "yes", "y", "on")


def _env_str(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default)).strip()


def _row_get(row, *keys, default=0):
    for k in keys:
        try:
            if hasattr(row, "get"):
                v = row.get(k, None)
                if v is not None:
                    return v
        except Exception:
            pass
    return default


def check_bb40_second_wave(curr: pd.Series, past: pd.DataFrame):
    """
    BB40 재안착 후 2차 파동:
    - 과거 BB40 하단 이탈 후 복귀 이력
    - 현재 BB40 중심선 위 or 상단밴드 방향
    - OBV/RSI가 1차 반등보다 강해짐
    """
    if past is None or past.empty or len(past) < 15:
        return False, "데이터 부족"

    bb40_lower_col = "BB40_Lower" if "BB40_Lower" in past.columns else "bb40_lower"
    bb40_width_col = "BB40_Width" if "BB40_Width" in past.columns else "bb40_width"
    ma40_col = "MA40" if "MA40" in curr.index else "ma40"
    obv_col = "OBV" if "OBV" in past.columns else "obv"
    rsi_col = "RSI" if "RSI" in past.columns else "rsi14"

    if bb40_lower_col not in past.columns or bb40_width_col not in past.columns:
        return False, "BB40 컬럼 부족"

    bb40_break = (past["Low"] < past[bb40_lower_col]).any()
    bb40_reclaim = (past["Close"] > past[bb40_lower_col]).any()

    above_mid = float(curr.get("Close", 0)) > float(curr.get(ma40_col, 0) or 0)
    bb_expand = float(curr.get(bb40_width_col, 0) or 0) > float(past[bb40_width_col].tail(5).mean() or 0)
    obv_up = obv_col in past.columns and float(curr.get(obv_col, 0) or 0) > float(past[obv_col].tail(5).max() or 0)
    rsi_up = rsi_col in past.columns and float(curr.get(rsi_col, 0) or 0) > float(past[rsi_col].tail(5).max() or 0)

    passed = bb40_break and bb40_reclaim and above_mid and (obv_up or rsi_up) and bb_expand
    return passed, f"BB40이탈:{bb40_break}, 복귀:{bb40_reclaim}, 중심선위:{above_mid}, OBV상승:{obv_up}, RSI상승:{rsi_up}"


def check_obv_acc_breakout(curr: pd.Series, past: pd.DataFrame):
    """
    OBV 매집 후 돌파:
    - 최근 박스권/수렴
    - OBV는 미리 상승
    - 현재 가격/거래량 돌파
    """
    if past is None or past.empty or len(past) < 20:
        return False, "데이터 부족"

    obv_col = "OBV" if "OBV" in past.columns else "obv"
    vol_avg = (
        float(curr.get("Vol_Avg", 0) or 0)
        or float(curr.get("vol_ma20", 0) or 0)
        or float(curr.get("Volume", 0) or 0)
    )

    box_range = (float(past["High"].max() or 0) / (float(past["Low"].min() or 0) + 1e-9)) <= 1.18
    obv_acc = obv_col in past.columns and float(curr.get(obv_col, 0) or 0) > float(past[obv_col].tail(10).max() or 0)
    price_break = float(curr.get("Close", 0) or 0) > float(past["High"].tail(10).max() or 0)
    vol_break = float(curr.get("Volume", 0) or 0) > vol_avg * 1.5 if vol_avg > 0 else False

    passed = box_range and obv_acc and price_break and vol_break
    return passed, f"박스권:{box_range}, OBV매집:{obv_acc}, 가격돌파:{price_break}, 거래량:{vol_break}"


def classify_bb_state(row) -> str:
    """볼린저밴드 상태 분류"""
    bb40w = float(_row_get(row, "BB40_Width", "bb40_width", default=99) or 99)
    pct_b = float(_row_get(row, "BB40_PercentB", "bb40_percent_b", default=0.5) or 0.5)

    if bb40w <= 3:
        return "💎극강응축(BB40≤3)"
    if bb40w <= 5:
        return "💎강응축(BB40≤5)"
    if bb40w <= 10:
        return "🔋응축중(BB40≤10)"
    if pct_b >= 0.9:
        return "🚀BB상단돌파권"
    if pct_b <= 0.1:
        return "📍BB하단근접"
    return f"➖BB보통({bb40w:.1f})"


def classify_obv_trend(row) -> str:
    """OBV 추세 분류"""
    slope = float(_row_get(row, "OBV_Slope", "obv_slope5", default=0) or 0)
    obv_r = bool(_row_get(row, "OBV_Rising", default=False))
    obv_b = bool(_row_get(row, "OBV_Bullish", default=False))
    if slope > 20 and obv_r and obv_b:
        return "📊OBV강매집(3중확인)"
    if slope > 5 and obv_r:
        return "📊OBV매집중"
    if slope > 0:
        return "📊OBV소폭상승"
    if slope < -10:
        return "📉OBV강분산"
    if slope < 0:
        return "📉OBV분산중"
    return "➖OBV보합"


def classify_supply_state(row) -> str:
    """수급 상태 분류 (enrich 후 사용)"""
    supply = str(_row_get(row, "수급", default="") or "")
    raw_maejip = str(_row_get(row, "매집", default="0/5"))
    try:
        maejip = int(raw_maejip.split("/")[0]) if "/" in raw_maejip else int(float(raw_maejip))
    except Exception:
        maejip = 0

    if "쌍끌" in supply:
        return "🤝쌍끌매수"
    if "기관" in supply:
        return "🔴기관매수"
    if "외인" in supply:
        return "🔵외인매수"
    if maejip >= 4:
        return "🐋세력매집강"
    if maejip >= 3:
        return "🐋세력매집"
    return "➖수급보통"


def calc_atr_targets(row: pd.Series, close: float) -> dict:
    """
    ATR 기반 동적 목표가/손절가 계산.
    1차 목표: 현재가 + ATR × 2
    2차 목표: 현재가 + ATR × 3.5
    손절: 현재가 - ATR × 1.5
    """
    atr_val = float(_row_get(row, "ATR", "atr14", "atr20", default=0) or 0)
    if atr_val <= 0:
        return {}

    return {
        "atr_val": round(atr_val),
        "target_1": round(close + atr_val * 2),
        "target_2": round(close + atr_val * 3.5),
        "stop_atr": round(close - atr_val * 1.5),
        "risk_reward": round((atr_val * 2) / (atr_val * 1.5), 1),
    }
