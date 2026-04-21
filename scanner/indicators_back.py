from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd


REQUIRED_OHLCV = ("Open", "High", "Low", "Close", "Volume")

def _env_flag(value, low=None, high=None):
    try:
        v = float(value)
    except Exception:
        return False

    if low is not None and v < float(low):
        return False
    if high is not None and v > float(high):
        return False
    return True

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
