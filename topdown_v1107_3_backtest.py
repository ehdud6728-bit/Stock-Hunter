# -*- coding: utf-8 -*-
"""
V1107_3_TOPDOWN_OPERATION_FILTER_BACKTEST_20260525

주봉 → 일봉 → 60분봉 프록시 탑다운 백테스트
+ 지수/섹터/종목 눌림 원인 분리
+ T-CORE / T-PRIME / T-WATCH / R-PRIME / R-WAIT / R-RISK / T-FAIL 분리
+ TD-CORE / TD-PRIME / TD-RECOVER / TD-WATCH / TD-RISK 운용형 압축
+ 기준일별 후보 수 제한 + 동일 종목 10거래일 중복 제거
+ 종가진입 / 다음날 시가 / 다음날 전일고점 돌파확인 진입 성과 비교
+ 평균 왜곡 방지: 중앙값, 10% 절사평균, 최고·최저 제외 평균

실행 예:
  python topdown_v1107_3_backtest.py --send-telegram
  python topdown_v1107_3_backtest.py --weeks 52 --universe-top 150

환경변수:
  TOPDOWN_WEEKS=52
  TOPDOWN_UNIVERSE_TOP=150
  TOPDOWN_REF_DATES=2026-05-08,2026-05-15
  TOPDOWN_OUT_DIR=topdown_v1107_3_logs
  TELEGRAM_BACKTEST_TOKEN / TELEGRAM_BACKTEST_CHAT_ID
  TELEGRAM_TOKEN / TELEGRAM_CHAT_ID fallback
"""

from __future__ import annotations

import argparse
import html
import math
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

try:
    import FinanceDataReader as fdr
except Exception:  # pragma: no cover
    fdr = None

try:
    import pytz
except Exception:  # pragma: no cover
    pytz = None

VERSION = "V1107_3_TOPDOWN_OPERATION_FILTER_BACKTEST_20260525"
KST = pytz.timezone("Asia/Seoul") if pytz else None

BAD_NAME_PAT = re.compile(
    r"(ETF|ETN|스팩|SPAC|리츠|REIT|우선주|우\b|우\)|인버스|레버리지|선물|채권|액티브|합성|TR|커버드콜)",
    re.IGNORECASE,
)

# 대표 섹터 키워드 매핑. 없는 종목은 미분류로 두고, 추후 repo 내부 테마맵이 있으면 이 함수만 교체하면 됩니다.
SECTOR_KEYWORDS: List[Tuple[str, str]] = [
    ("반도체", "삼성전자|SK하이닉스|DB하이텍|한미반도체|주성엔지니어링|테크윙|이수페타시스|하나마이크론|리노공업|동진쎄미켐|코리아써키트|SFA반도체|제주반도체|피델릭스|텔레칩스|아이텍"),
    ("로봇/자동화", "두산로보틱스|레인보우로보틱스|로보티즈|휴림로봇|에스피지|라온로보틱스|제닉스|제닉스로보틱스|유일로보틱스|티로보틱스|고영"),
    ("AI전력/전력설비", "HD현대일렉트릭|LS ELECTRIC|LS일렉트릭|LS|산일전기|제룡전기|효성중공업|일진전기|대한전선|가온전선|비나텍"),
    ("원전/우라늄", "두산에너빌리티|한전기술|한전산업|우리기술|비에이치아이|보성파워텍|우진|일진파워"),
    ("바이오/헬스케어", "셀트리온|삼성바이오로직스|에이비엘바이오|한미약품|녹십자|펩트론|알테오젠|리가켐|리가켐바이오|바이오니아|바이오플러스|현대바이오|한올바이오파마|원텍|클래시스"),
    ("금융/은행", "KB금융|신한지주|하나금융지주|우리금융지주|메리츠금융지주|기업은행|한화생명|한화손해보험|DB손해보험|삼성생명|미래에셋증권"),
    ("자동차/부품", "현대차|기아|현대모비스|HL만도|현대글로비스|성우하이텍|화신|에스엘"),
    ("조선/해운", "HD현대중공업|HD한국조선해양|한화오션|삼성중공업|현대미포|HMM|팬오션|흥아해운|STX그린로지스"),
    ("항공/운송", "대한항공|진에어|제주항공|티웨이항공|아시아나항공|현대글로비스|CJ대한통운|한진"),
    ("소프트웨어/클라우드", "NAVER|카카오|더존비즈온|안랩|한글과컴퓨터|크래프톤|하이브|엔씨소프트|넷마블"),
    ("정유/에너지", "SK이노베이션|S-Oil|GS|HD현대|흥구석유|중앙에너비스|한국석유|대성에너지"),
]


STRONG_SECTORS = {"반도체", "AI전력/전력설비", "로봇/자동화", "원전/우라늄", "자동차/부품"}
WEAK_SECTORS = {"항공/운송", "소프트웨어/클라우드", "조선/해운"}
OP_LABEL_ORDER = ["🟢 TD-CORE", "🔥 TD-PRIME", "🟠 TD-RECOVER", "🟡 TD-WATCH", "🔴 TD-RISK"]
TD_LABEL_ORDER = ["🟢 T-CORE", "🔥 T-PRIME", "🟡 T-WATCH", "🟠 R-PRIME", "🟠 R-WAIT", "🟤 R-RISK", "🔴 T-FAIL"]


def now_kst() -> datetime:
    return datetime.now(KST) if KST else datetime.now()


def env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def parse_bool(x: Any) -> bool:
    return str(x).strip().lower() in {"1", "true", "y", "yes", "on"}


def safe_num(s: Any) -> pd.Series:
    """pandas nullable NA/object가 rolling에서 터지는 문제를 막기 위한 숫자 변환."""
    if isinstance(s, pd.Series):
        return pd.to_numeric(s, errors="coerce").astype("float64")
    return pd.to_numeric(pd.Series(s), errors="coerce").astype("float64")


def pct(a: float, b: float) -> float:
    if b is None or pd.isna(b) or b == 0:
        return np.nan
    return (a / b - 1.0) * 100.0


def fmt_pct(x: Any, nd: int = 2) -> str:
    try:
        if pd.isna(x):
            return "-"
        return f"{float(x):+.{nd}f}%"
    except Exception:
        return "-"


def fmt_num(x: Any, nd: int = 1) -> str:
    try:
        if pd.isna(x):
            return "-"
        return f"{float(x):.{nd}f}"
    except Exception:
        return "-"


def clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    rename_map = {
        "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume", "Change": "change",
        "시가": "open", "고가": "high", "저가": "low", "종가": "close", "거래량": "volume", "등락률": "change",
    }
    out = out.rename(columns={c: rename_map.get(c, c) for c in out.columns})
    need = ["open", "high", "low", "close", "volume"]
    for c in need:
        if c not in out.columns:
            out[c] = np.nan
        out[c] = safe_num(out[c])

    out = out[need].replace([np.inf, -np.inf], np.nan)
    out = out.dropna(subset=["close"])
    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out


def macd_hist(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    c = safe_num(close)
    ema_fast = c.ewm(span=fast, adjust=False, min_periods=fast).mean()
    ema_slow = c.ewm(span=slow, adjust=False, min_periods=slow).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False, min_periods=signal).mean()
    return (macd - sig).astype("float64")


def stochastic_kd(df: pd.DataFrame, n: int = 14, k_smooth: int = 3, d_smooth: int = 3) -> Tuple[pd.Series, pd.Series]:
    high = safe_num(df["high"])
    low = safe_num(df["low"])
    close = safe_num(df["close"])
    hh = high.rolling(n, min_periods=1).max()
    ll = low.rolling(n, min_periods=1).min()
    denom = (hh - ll).replace(0, np.nan)
    raw_k = ((close - ll) / denom * 100.0).replace([np.inf, -np.inf], np.nan).astype("float64")
    raw_k = raw_k.ffill().fillna(50.0).astype("float64")
    k = raw_k.rolling(k_smooth, min_periods=1).mean().astype("float64")
    d = k.rolling(d_smooth, min_periods=1).mean().astype("float64")
    return k, d


def assign_sector(name: str) -> str:
    name = str(name or "")
    for sector, pat in SECTOR_KEYWORDS:
        if re.search(pat, name, re.IGNORECASE):
            return sector
    return "미분류"


def load_krx_listing(universe_top: int) -> pd.DataFrame:
    if fdr is None:
        raise RuntimeError("FinanceDataReader가 설치되어 있지 않습니다. requirements.txt 또는 workflow에 finance-datareader를 추가하세요. import 이름은 FinanceDataReader입니다.")

    frames = []
    for market in ["KOSPI", "KOSDAQ"]:
        try:
            x = fdr.StockListing(market)
            if x is not None and not x.empty:
                x["market"] = market
                frames.append(x)
        except Exception as e:
            print(f"⚠️ listing load fail {market}: {e}")

    if not frames:
        raise RuntimeError("KRX listing을 불러오지 못했습니다.")

    listing = pd.concat(frames, ignore_index=True)
    colmap = {}
    for c in listing.columns:
        lc = str(c).lower()
        if c in ["Code", "Symbol", "종목코드"]:
            colmap[c] = "code"
        elif c in ["Name", "종목명"]:
            colmap[c] = "name"
        elif c in ["Marcap", "MarketCap", "시가총액"]:
            colmap[c] = "marcap"
    listing = listing.rename(columns=colmap)

    if "code" not in listing.columns or "name" not in listing.columns:
        raise RuntimeError(f"listing columns 확인 필요: {list(listing.columns)}")

    listing["code"] = listing["code"].astype(str).str.zfill(6)
    listing["name"] = listing["name"].astype(str)
    listing = listing[~listing["name"].str.contains(BAD_NAME_PAT, na=False, regex=True)].copy()

    if "marcap" in listing.columns:
        listing["marcap"] = safe_num(listing["marcap"])
        listing = listing.sort_values("marcap", ascending=False)
    else:
        listing["marcap"] = np.nan

    listing["sector"] = listing["name"].map(assign_sector)
    listing = listing.drop_duplicates("code", keep="first")
    return listing.head(universe_top).reset_index(drop=True)


def fetch_price(code: str, start: str, end: str, retry: int = 2) -> pd.DataFrame:
    if fdr is None:
        return pd.DataFrame()
    last_err = None
    for i in range(retry + 1):
        try:
            df = fdr.DataReader(str(code).zfill(6), start, end)
            return clean_ohlcv(df)
        except Exception as e:
            last_err = e
            time.sleep(0.15 * (i + 1))
    print(f"⚠️ price load fail {code}: {last_err}")
    return pd.DataFrame()


def fetch_index(symbol: str, start: str, end: str) -> pd.DataFrame:
    # FDR에서 KS11/KQ11이 실패할 때가 있어 다중 시도
    if fdr is None:
        return pd.DataFrame()
    for s in [symbol, "KS11", "KQ11"]:
        try:
            df = fdr.DataReader(s, start, end)
            out = clean_ohlcv(df)
            if not out.empty:
                return out
        except Exception:
            continue
    return pd.DataFrame()


def make_ref_dates(end_date: datetime, weeks: int, explicit: str = "") -> List[pd.Timestamp]:
    if explicit:
        dates = []
        for x in explicit.split(","):
            x = x.strip()
            if not x:
                continue
            dates.append(pd.Timestamp(x))
        return sorted(dates)

    # 금요일 기준. 실행일이 주중이어도 마지막 금요일까지 52개.
    end_ts = pd.Timestamp(end_date.date())
    # 가장 가까운 과거 금요일
    offset = (end_ts.weekday() - 4) % 7
    last_fri = end_ts - pd.Timedelta(days=offset)
    return [last_fri - pd.Timedelta(weeks=i) for i in range(weeks - 1, -1, -1)]


@dataclass
class WeeklySignal:
    pass_weekly: bool
    above_20w: bool
    ma20w_up: bool
    macd_hist_up: bool
    close: float
    ma20w: float
    hist: float


@dataclass
class DailySignal:
    ready: bool
    watch: bool
    near_ma20: bool
    stoch_exit: bool
    macd_div: bool
    close: float
    ma20: float
    stoch_k: float
    stoch_d: float
    hist: float


@dataclass
class ProxySignal:
    yes: bool
    breakout: bool
    volume_ok: bool
    ma5_up: bool
    macd_gc_or_up: bool
    vol_ratio: float
    prev_high: float


def calc_weekly_signal(df: pd.DataFrame) -> WeeklySignal:
    if df is None or len(df) < 120:
        return WeeklySignal(False, False, False, False, np.nan, np.nan, np.nan)

    wk = df.resample("W-FRI").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna(subset=["close"])
    if len(wk) < 35:
        return WeeklySignal(False, False, False, False, np.nan, np.nan, np.nan)

    close = safe_num(wk["close"])
    ma20w = close.rolling(20, min_periods=10).mean()
    hist = macd_hist(close)

    c = float(close.iloc[-1])
    m = float(ma20w.iloc[-1]) if pd.notna(ma20w.iloc[-1]) else np.nan
    h = float(hist.iloc[-1]) if pd.notna(hist.iloc[-1]) else np.nan

    above = bool(pd.notna(m) and c > m)
    slope = bool(pd.notna(ma20w.iloc[-1]) and pd.notna(ma20w.iloc[-4]) and ma20w.iloc[-1] > ma20w.iloc[-4])
    hist_up = bool(pd.notna(hist.iloc[-1]) and pd.notna(hist.iloc[-2]) and hist.iloc[-1] > 0 and hist.iloc[-1] >= hist.iloc[-2])
    return WeeklySignal(above and slope and hist_up, above, slope, hist_up, c, m, h)


def calc_daily_signal(df: pd.DataFrame) -> DailySignal:
    if df is None or len(df) < 60:
        return DailySignal(False, False, False, False, False, np.nan, np.nan, np.nan, np.nan, np.nan)

    close = safe_num(df["close"])
    ma20 = close.rolling(20, min_periods=10).mean()
    hist = macd_hist(close)
    k, d = stochastic_kd(df)

    c = float(close.iloc[-1])
    m20 = float(ma20.iloc[-1]) if pd.notna(ma20.iloc[-1]) else np.nan
    k0 = float(k.iloc[-1]) if pd.notna(k.iloc[-1]) else np.nan
    d0 = float(d.iloc[-1]) if pd.notna(d.iloc[-1]) else np.nan
    h0 = float(hist.iloc[-1]) if pd.notna(hist.iloc[-1]) else np.nan

    dist20 = pct(c, m20) if pd.notna(m20) else np.nan
    near_ma20 = bool(pd.notna(dist20) and -6.0 <= dist20 <= 5.0)

    recent_k_min = k.iloc[-7:].min() if len(k) >= 7 else k.min()
    stoch_cross = bool(len(k) >= 2 and pd.notna(k.iloc[-2]) and pd.notna(d.iloc[-2]) and k.iloc[-2] <= d.iloc[-2] and k.iloc[-1] > d.iloc[-1])
    stoch_exit = bool((pd.notna(recent_k_min) and recent_k_min <= 35 and k.iloc[-1] > d.iloc[-1]) or (stoch_cross and k.iloc[-1] <= 55))

    # 단순 강세 다이버전스 프록시: 가격 저점은 비슷/낮지만 MACD hist 저점은 개선 + 최근 hist 상승
    macd_div = False
    if len(close) >= 45 and hist.notna().sum() >= 20:
        c_recent_low = close.iloc[-12:].min()
        c_prev_low = close.iloc[-35:-12].min()
        h_recent_low = hist.iloc[-12:].min()
        h_prev_low = hist.iloc[-35:-12].min()
        hist_rising = hist.iloc[-1] > hist.iloc[-3] if pd.notna(hist.iloc[-3]) and pd.notna(hist.iloc[-1]) else False
        macd_div = bool(
            pd.notna(c_recent_low) and pd.notna(c_prev_low) and pd.notna(h_recent_low) and pd.notna(h_prev_low)
            and c_recent_low <= c_prev_low * 1.03
            and h_recent_low > h_prev_low
            and hist_rising
        )

    score = int(near_ma20) + int(stoch_exit) + int(macd_div)
    ready = bool(score >= 2 and near_ma20 and (stoch_exit or macd_div))
    watch = bool(score >= 1 and near_ma20)
    return DailySignal(ready, watch, near_ma20, stoch_exit, macd_div, c, m20, k0, d0, h0)


def calc_60m_proxy_signal(df: pd.DataFrame) -> ProxySignal:
    """분봉 데이터가 없을 때 쓰는 일봉 기반 60분 돌파 프록시.
    실제 live 적용 시에는 이 부분만 60분 OHLCV로 교체하면 됩니다.
    """
    if df is None or len(df) < 40:
        return ProxySignal(False, False, False, False, False, np.nan, np.nan)

    close = safe_num(df["close"])
    high = safe_num(df["high"])
    volume = safe_num(df["volume"])
    ma5 = close.rolling(5, min_periods=3).mean()
    v20 = volume.rolling(20, min_periods=10).mean()
    hist = macd_hist(close)

    prev_high = high.shift(1).rolling(5, min_periods=3).max().iloc[-1]
    breakout = bool(pd.notna(prev_high) and close.iloc[-1] > prev_high)
    vol_ratio = float(volume.iloc[-1] / v20.iloc[-1]) if pd.notna(v20.iloc[-1]) and v20.iloc[-1] > 0 else np.nan
    volume_ok = bool(pd.notna(vol_ratio) and vol_ratio >= 1.2)
    ma5_up = bool(pd.notna(ma5.iloc[-1]) and pd.notna(ma5.iloc[-3]) and ma5.iloc[-1] > ma5.iloc[-3])
    macd_gc = bool(len(hist) >= 2 and pd.notna(hist.iloc[-1]) and pd.notna(hist.iloc[-2]) and (hist.iloc[-1] > 0 or hist.iloc[-1] > hist.iloc[-2]))

    # 거래량 없는 돌파는 가짜로 처리. 다만 종가가 5일고점 근처 + 거래량 1.5x면 돌파 인정.
    yes = bool((breakout and volume_ok and (ma5_up or macd_gc)) or (close.iloc[-1] >= high.iloc[-5:].max() * 0.985 and pd.notna(vol_ratio) and vol_ratio >= 1.5 and ma5_up))
    return ProxySignal(yes, breakout, volume_ok, ma5_up, macd_gc, vol_ratio, float(prev_high) if pd.notna(prev_high) else np.nan)


def ret_n(df: pd.DataFrame, ref_date: pd.Timestamp, n: int) -> float:
    if df is None or df.empty:
        return np.nan
    x = df[df.index <= ref_date]
    y = df[df.index > ref_date]
    if x.empty or len(y) < n:
        return np.nan
    entry = float(x["close"].iloc[-1])
    exitp = float(y["close"].iloc[n - 1])
    return pct(exitp, entry)


def recent_return(df: pd.DataFrame, ref_date: pd.Timestamp, days: int = 5) -> float:
    x = df[df.index <= ref_date]
    if len(x) <= days:
        return np.nan
    c0 = float(x["close"].iloc[-1])
    c1 = float(x["close"].iloc[-1 - days])
    return pct(c0, c1)


def classify_pullback(
    stock_df: pd.DataFrame,
    market_df: pd.DataFrame,
    sector_ret5: float,
    ref_date: pd.Timestamp,
) -> Tuple[str, Dict[str, float]]:
    x = stock_df[stock_df.index <= ref_date]
    if len(x) < 30:
        return "⚪ MIXED_PULLBACK", {}

    close = safe_num(x["close"])
    volume = safe_num(x["volume"])
    ma20 = close.rolling(20, min_periods=10).mean()
    v20 = volume.rolling(20, min_periods=10).mean()

    stock_r5 = recent_return(stock_df, ref_date, 5)
    market_r5 = recent_return(market_df, ref_date, 5) if market_df is not None and not market_df.empty else np.nan
    vol_ratio = float(volume.iloc[-1] / v20.iloc[-1]) if pd.notna(v20.iloc[-1]) and v20.iloc[-1] > 0 else np.nan
    c = float(close.iloc[-1])
    m20 = float(ma20.iloc[-1]) if pd.notna(ma20.iloc[-1]) else np.nan
    below_ma20 = bool(pd.notna(m20) and c < m20 * 0.97)
    near_or_above_ma20 = bool(pd.notna(m20) and c >= m20 * 0.97)

    info = {
        "stock_ret5": stock_r5,
        "market_ret5": market_r5,
        "sector_ret5": sector_ret5,
        "vol_ratio": vol_ratio,
        "ma20_dist": pct(c, m20) if pd.notna(m20) else np.nan,
    }

    # 종목 단독 붕괴: 지수/섹터보다 훨씬 약하고, 거래량이 붙은 하락 또는 20일선 훼손
    if pd.notna(stock_r5):
        sector_ok = pd.isna(sector_ret5) or stock_r5 <= sector_ret5 - 4.0
        market_ok = pd.isna(market_r5) or stock_r5 <= market_r5 - 4.0
        if stock_r5 <= -7.0 and sector_ok and market_ok and (below_ma20 or (pd.notna(vol_ratio) and vol_ratio >= 1.4)):
            return "🔴 STOCK_BREAKDOWN", info

    # 섹터 동반 조정: 종목보다 섹터 자체가 빠지는 중
    if pd.notna(sector_ret5) and sector_ret5 <= -3.5 and (pd.isna(market_r5) or sector_ret5 <= market_r5 - 1.0):
        return "🟠 SECTOR_PULLBACK", info

    # 지수 조정: 시장이 빠지는데 종목/섹터가 상대적으로 덜 훼손
    if pd.notna(market_r5) and market_r5 <= -2.0:
        if pd.isna(stock_r5) or stock_r5 >= market_r5 - 2.5:
            return "🟡 INDEX_PULLBACK", info

    # 건강한 눌림: 20일선 근처/위, 종목 자체가 과도하게 깨지지 않음
    if pd.notna(stock_r5) and -8.0 <= stock_r5 <= 3.0 and near_or_above_ma20:
        return "🟢 HEALTHY_PULLBACK", info

    # 일반 눌림
    if pd.notna(stock_r5) and -10.0 <= stock_r5 <= 6.0:
        return "🟡 NORMAL_PULLBACK", info

    return "⚪ MIXED_PULLBACK", info


def classify_topdown(w: WeeklySignal, d: DailySignal, p: ProxySignal, pullback: str) -> Tuple[str, str, str]:
    dangerous = ("STOCK_BREAKDOWN" in pullback) or ("SECTOR_PULLBACK" in pullback)
    good_pullback = any(x in pullback for x in ["HEALTHY_PULLBACK", "INDEX_PULLBACK", "NORMAL_PULLBACK"])

    if w.pass_weekly and d.ready and p.yes and not dangerous:
        return "🟢 T-CORE", "안정형", "주봉 PASS + 일봉 READY + 60분프록시 YES + 붕괴 아님"

    if w.pass_weekly and p.yes and good_pullback and not dangerous:
        return "🔥 T-PRIME", "공격형", "주봉 PASS + 60분프록시 YES + 정상/지수/건강한 눌림"

    if w.pass_weekly and (d.ready or d.watch or good_pullback) and (not p.yes) and not dangerous:
        return "🟡 T-WATCH", "관찰형", "주봉은 살아 있으나 60분프록시 없음 — 거래량 재돌파 대기"

    if w.pass_weekly and p.yes and not dangerous:
        return "🟠 R-PRIME", "회복공격", "주봉 PASS + 60분프록시 YES이나 일봉 타점/눌림 분류 미완성"

    if w.pass_weekly and dangerous:
        return "🟤 R-RISK", "회복위험", "주봉은 살아 있으나 섹터/종목 훼손 가능성"

    if w.pass_weekly:
        return "🟠 R-WAIT", "회복대기", "주봉은 살아 있으나 일봉/60분 타점 대기"

    return "🔴 T-FAIL", "탈락", "주봉 PASS 실패 또는 구조 훼손"


def sector_grade(sector: str, sector_ret5: float) -> str:
    """섹터 자체가 버티는지/무너지는지 단순 등급화."""
    if pd.notna(sector_ret5) and sector_ret5 <= -3.5:
        return "WEAK_SECTOR"
    if sector in STRONG_SECTORS and (pd.isna(sector_ret5) or sector_ret5 >= -2.0):
        return "STRONG_SECTOR"
    if sector in WEAK_SECTORS:
        return "WEAK_SECTOR"
    return "NEUTRAL_SECTOR"


def calc_operation_score(
    w: WeeklySignal,
    d: DailySignal,
    p: ProxySignal,
    pullback: str,
    sector: str,
    sector_ret5: float,
    stop_pct: float,
    trade_value_b: float,
) -> float:
    """미래 수익률을 쓰지 않는 실시간 운용용 점수."""
    score = 0.0
    score += 24 if w.pass_weekly else 0
    score += 7 if w.above_20w else 0
    score += 7 if w.ma20w_up else 0
    score += 8 if w.macd_hist_up else 0
    score += 20 if d.ready else (9 if d.watch else 0)
    score += 21 if p.yes else 0
    score += 5 if p.volume_ok else 0
    score += 4 if p.ma5_up else 0
    score += 4 if p.macd_gc_or_up else 0

    if "HEALTHY_PULLBACK" in pullback:
        score += 14
    elif "INDEX_PULLBACK" in pullback:
        score += 12
    elif "NORMAL_PULLBACK" in pullback:
        score += 10
    elif "MIXED_PULLBACK" in pullback:
        score += 3
    elif "SECTOR_PULLBACK" in pullback:
        score -= 16
    elif "STOCK_BREAKDOWN" in pullback:
        score -= 30

    sg = sector_grade(sector, sector_ret5)
    if sg == "STRONG_SECTOR":
        score += 10
    elif sg == "WEAK_SECTOR":
        score -= 8

    if pd.notna(stop_pct):
        if -5.0 <= stop_pct <= -2.0:
            score += 8
        elif -8.0 <= stop_pct < -5.0:
            score += 4
        elif stop_pct < -10.0:
            score -= 10

    if pd.notna(trade_value_b):
        if trade_value_b >= 500:
            score += 4
        elif trade_value_b >= 100:
            score += 2
        elif trade_value_b < 30:
            score -= 5

    if pd.notna(p.vol_ratio):
        # 1.2~2.5배를 가장 좋게 보고, 3배 이상은 추격주의로 살짝 감점
        if 1.2 <= p.vol_ratio <= 2.5:
            score += 5
        elif 2.5 < p.vol_ratio <= 4.0:
            score += 1
        elif p.vol_ratio > 4.0:
            score -= 3

    return round(float(score), 2)


def classify_operation_label(
    td_label: str,
    w: WeeklySignal,
    d: DailySignal,
    p: ProxySignal,
    pullback: str,
    sector: str,
    sector_ret5: float,
    stop_pct: float,
    op_score: float,
) -> Tuple[str, str]:
    """백테스트 라벨을 실제 운용 라벨로 압축."""
    sg = sector_grade(sector, sector_ret5)
    stock_break = "STOCK_BREAKDOWN" in pullback
    sector_break = "SECTOR_PULLBACK" in pullback
    stop_too_wide = bool(pd.notna(stop_pct) and stop_pct < -10.0)

    if td_label == "🔴 T-FAIL" or stock_break or stop_too_wide:
        return "🔴 TD-RISK", "주봉 실패/종목 단독 훼손/손절거리 과다 — 실전 제외"

    if sector_break:
        return "🔴 TD-RISK", "섹터 동반 조정 — 세력 눌림목으로 단정 금지, 섹터 회복 확인 전 제외"

    if td_label == "🟢 T-CORE" and sg != "WEAK_SECTOR" and op_score >= 75:
        return "🟢 TD-CORE", "주봉·일봉·60분프록시가 모두 맞는 안정형 후보"

    if td_label in {"🔥 T-PRIME", "🟢 T-CORE"} and p.yes and sg != "WEAK_SECTOR" and op_score >= 65:
        return "🔥 TD-PRIME", "주봉 PASS + 60분프록시 확인. 공격형 보강 후보"

    if td_label == "🟠 R-PRIME" and p.yes and sg != "WEAK_SECTOR" and op_score >= 55:
        return "🟠 TD-RECOVER", "주봉과 분봉 프록시는 살아 있으나 일봉 타점은 확인형"

    if w.pass_weekly and sg != "WEAK_SECTOR":
        return "🟡 TD-WATCH", "주봉/섹터는 살아 있으나 60분 거래량 재돌파 또는 일봉 타점 대기"

    return "🔴 TD-RISK", "조건 미달 또는 섹터 약세 — 실전 TOP 제외"


def calc_stop_price(df: pd.DataFrame) -> Tuple[float, float, str]:
    if df is None or len(df) < 10:
        return np.nan, np.nan, "기본"
    c = float(df["close"].iloc[-1])
    low10 = float(safe_num(df["low"]).iloc[-10:].min())
    low5 = float(safe_num(df["low"]).iloc[-5:].min())
    swing = min(low5, low10) * 0.995
    hard = c * 0.90
    stop = max(swing, hard) if pd.notna(swing) else hard
    stop_pct = pct(stop, c)
    return stop, stop_pct, "swing10_or_-10"


def evaluate_path(df: pd.DataFrame, ref_date: pd.Timestamp, stop_price: float) -> Dict[str, Any]:
    x = df[df.index <= ref_date]
    future = df[df.index > ref_date].head(10)
    if x.empty or future.empty:
        return {}
    entry = float(x["close"].iloc[-1])
    out: Dict[str, Any] = {}
    for n in [1, 3, 5, 10]:
        if len(future) >= n:
            out[f"ret{n}"] = pct(float(future["close"].iloc[n - 1]), entry)
        else:
            out[f"ret{n}"] = np.nan
    out["max_up10"] = pct(float(future["high"].max()), entry) if "high" in future else np.nan
    out["max_dn10"] = pct(float(future["low"].min()), entry) if "low" in future else np.nan

    hit3_day = hit5_day = stop_day = None
    for i, (_, row) in enumerate(future.iterrows(), start=1):
        hi = float(row["high"])
        lo = float(row["low"])
        if hit3_day is None and hi >= entry * 1.03:
            hit3_day = i
        if hit5_day is None and hi >= entry * 1.05:
            hit5_day = i
        if stop_day is None and pd.notna(stop_price) and lo <= stop_price:
            stop_day = i

    out["hit3_first"] = bool(hit3_day is not None and (stop_day is None or hit3_day <= stop_day))
    out["hit5_first"] = bool(hit5_day is not None and (stop_day is None or hit5_day <= stop_day))
    out["stop_first"] = bool(stop_day is not None and ((hit3_day is None) or stop_day < hit3_day))
    out["hit3_day"] = hit3_day
    out["hit5_day"] = hit5_day
    out["stop_day"] = stop_day
    return out


def evaluate_from_entry(df: pd.DataFrame, entry_pos: int, entry_price: float, stop_price: float, prefix: str) -> Dict[str, Any]:
    """entry_pos부터 10거래일 경로 평가. entry_price는 실제 진입가격."""
    future = df.iloc[entry_pos: entry_pos + 10]
    out: Dict[str, Any] = {}
    if future.empty or pd.isna(entry_price) or entry_price <= 0:
        for n in [1, 3, 5, 10]:
            out[f"{prefix}_ret{n}"] = np.nan
        out[f"{prefix}_max_up10"] = np.nan
        out[f"{prefix}_max_dn10"] = np.nan
        out[f"{prefix}_hit3_first"] = False
        out[f"{prefix}_hit5_first"] = False
        out[f"{prefix}_stop_first"] = False
        return out

    for n in [1, 3, 5, 10]:
        if len(future) >= n:
            out[f"{prefix}_ret{n}"] = pct(float(future["close"].iloc[n - 1]), entry_price)
        else:
            out[f"{prefix}_ret{n}"] = np.nan
    out[f"{prefix}_max_up10"] = pct(float(future["high"].max()), entry_price)
    out[f"{prefix}_max_dn10"] = pct(float(future["low"].min()), entry_price)

    hit3_day = hit5_day = stop_day = None
    for i, (_, row) in enumerate(future.iterrows(), start=1):
        hi = float(row["high"])
        lo = float(row["low"])
        if hit3_day is None and hi >= entry_price * 1.03:
            hit3_day = i
        if hit5_day is None and hi >= entry_price * 1.05:
            hit5_day = i
        if stop_day is None and pd.notna(stop_price) and lo <= stop_price:
            stop_day = i
    out[f"{prefix}_hit3_first"] = bool(hit3_day is not None and (stop_day is None or hit3_day <= stop_day))
    out[f"{prefix}_hit5_first"] = bool(hit5_day is not None and (stop_day is None or hit5_day <= stop_day))
    out[f"{prefix}_stop_first"] = bool(stop_day is not None and ((hit3_day is None) or stop_day < hit3_day))
    return out


def evaluate_entry_modes(df: pd.DataFrame, ref_date: pd.Timestamp, ref_high: float, stop_price: float) -> Dict[str, Any]:
    """A 종가, B 다음날 시가, C 다음날 전일고점 돌파확인 진입 비교."""
    out: Dict[str, Any] = {}
    x = df[df.index <= ref_date]
    future = df[df.index > ref_date]
    if x.empty or future.empty:
        return out

    close_entry = float(x["close"].iloc[-1])
    out.update(evaluate_from_entry(future, 0, close_entry, stop_price, "close_entry"))

    next_open = float(future["open"].iloc[0]) if pd.notna(future["open"].iloc[0]) else float(future["close"].iloc[0])
    out["next_open_price"] = next_open
    out.update(evaluate_from_entry(future, 0, next_open, stop_price, "next_open"))

    # 다음날 전일고점 돌파 확인 진입. 미돌파면 거래 없음으로 처리.
    first = future.iloc[0]
    trigger = bool(pd.notna(ref_high) and float(first["high"]) >= float(ref_high) * 1.001)
    out["breakout_trigger"] = trigger
    if trigger:
        break_entry = max(float(ref_high) * 1.001, float(first["open"]) if pd.notna(first["open"]) else float(ref_high) * 1.001)
        out["breakout_entry_price"] = break_entry
        out.update(evaluate_from_entry(future, 0, break_entry, stop_price, "breakout_entry"))
    else:
        out["breakout_entry_price"] = np.nan
        out.update(evaluate_from_entry(future, 0, np.nan, stop_price, "breakout_entry"))
    return out


def trim_mean(s: pd.Series, proportion: float = 0.1) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    n = len(x)
    if n == 0:
        return np.nan
    k = int(math.floor(n * proportion))
    if n - 2 * k <= 0:
        return float(x.mean())
    return float(x.iloc[k:n - k].mean())


def excl_extreme_mean(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    if len(x) <= 2:
        return float(x.mean()) if len(x) else np.nan
    return float(x.iloc[1:-1].mean())


def stat_line(df: pd.DataFrame, label: str) -> str:
    if df is None or df.empty:
        return f"- {label}: 데이터 없음"
    n = len(df)
    return (
        f"- {label}: {n}개 | "
        f"3일 {fmt_pct(df['ret3'].mean())} / 5일 {fmt_pct(df['ret5'].mean())} / 10일 {fmt_pct(df['ret10'].mean())} | "
        f"5일중앙 {fmt_pct(df['ret5'].median())} / 절사 {fmt_pct(trim_mean(df['ret5']))} / 극단제외 {fmt_pct(excl_extreme_mean(df['ret5']))} | "
        f"10일최고 {fmt_pct(df['max_up10'].mean())} | "
        f"+3먼저 {df['hit3_first'].mean() * 100:.1f}% | +5먼저 {df['hit5_first'].mean() * 100:.1f}% | "
        f"손절먼저 {df['stop_first'].mean() * 100:.1f}%"
    )


def group_report(df: pd.DataFrame, col: str, title: str, min_n: int = 1, order: Optional[List[str]] = None) -> List[str]:
    lines = [f"\n[{title}]"]
    if df.empty or col not in df.columns:
        lines.append("- 데이터 없음")
        return lines
    groups = []
    for k, g in df.groupby(col, dropna=False):
        if len(g) >= min_n:
            groups.append((str(k), g))
    if order:
        order_map = {v: i for i, v in enumerate(order)}
        groups.sort(key=lambda x: order_map.get(x[0], 999))
    else:
        groups.sort(key=lambda x: (-len(x[1]), x[0]))
    for k, g in groups:
        lines.append(stat_line(g, k))
    return lines


def format_examples(df: pd.DataFrame, title: str, n: int = 10, label_filter: Optional[Iterable[str]] = None, sort_col: str = "ret10") -> List[str]:
    lines = [f"\n[{title}]"]
    if df.empty:
        lines.append("- 데이터 없음")
        return lines
    x = df.copy()
    if label_filter:
        filters = list(label_filter)
        x = x[x["td_label"].isin(filters)]
    if x.empty:
        lines.append("- 데이터 없음")
        return lines
    x = x.sort_values(sort_col, ascending=False).head(n)
    for _, r in x.iterrows():
        lines.append(
            f"- {r['ref_date']} {r['name']}({r['code']}) | {r['td_label']} | {r['pullback_cause']} | "
            f"주봉:{'PASS' if r['weekly_pass'] else 'FAIL'} / 일봉:{'READY' if r['daily_ready'] else ('WATCH' if r['daily_watch'] else 'WAIT')} / 프록시:{'YES' if r['proxy_yes'] else 'NO'} | "
            f"5일 {fmt_pct(r['ret5'])} / 10일 {fmt_pct(r['ret10'])} | 손절 {fmt_pct(r['stop_pct'])} | {r.get('td_reason','')}"
        )
    return lines


def to_html_report(df: pd.DataFrame, summary_text: str, path: Path) -> None:
    esc = html.escape(summary_text).replace("\n", "<br>\n")
    table = df.head(5000).to_html(index=False, escape=False) if not df.empty else "<p>데이터 없음</p>"
    content = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{VERSION}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:18px;line-height:1.45;background:#fafafa;color:#111}}
h1{{font-size:22px}} .box{{background:white;border:1px solid #ddd;border-radius:14px;padding:14px;margin:12px 0;box-shadow:0 1px 3px rgba(0,0,0,.05)}}
table{{border-collapse:collapse;width:100%;font-size:12px;background:white}} th,td{{border:1px solid #ddd;padding:6px;white-space:nowrap}} th{{background:#f2f2f2;position:sticky;top:0}}
.good{{color:#0a7a2f;font-weight:700}} .bad{{color:#b00020;font-weight:700}}
</style></head><body>
<h1>{VERSION}</h1>
<div class="box">{esc}</div>
<div class="box"><h2>상세 데이터</h2>{table}</div>
</body></html>"""
    path.write_text(content, encoding="utf-8")


def telegram_send(text: str) -> None:
    token = os.getenv("TELEGRAM_BACKTEST_TOKEN") or os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_BACKTEST_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("⚠️ Telegram token/chat_id 없음: 전송 생략")
        return
    chunks = split_telegram(text, 3600)
    for i, ch in enumerate(chunks, start=1):
        prefix = f"({i}/{len(chunks)})\n" if len(chunks) > 1 else ""
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={"chat_id": chat_id, "text": prefix + ch, "disable_web_page_preview": True},
                timeout=15,
            )
            if r.status_code >= 300:
                print(f"⚠️ Telegram send fail {r.status_code}: {r.text[:300]}")
        except Exception as e:
            print(f"⚠️ Telegram send error: {e}")
        time.sleep(0.3)


def split_telegram(text: str, limit: int = 3600) -> List[str]:
    if len(text) <= limit:
        return [text]
    parts: List[str] = []
    buf: List[str] = []
    size = 0
    for line in text.splitlines():
        add = len(line) + 1
        if size + add > limit and buf:
            parts.append("\n".join(buf))
            buf = []
            size = 0
        buf.append(line)
        size += add
    if buf:
        parts.append("\n".join(buf))
    return parts


def build_sector_returns(rows: Dict[str, pd.DataFrame], listing: pd.DataFrame, ref_date: pd.Timestamp) -> Dict[str, float]:
    vals: Dict[str, List[float]] = {}
    meta = listing.set_index("code")
    for code, df in rows.items():
        if code not in meta.index:
            continue
        sec = str(meta.loc[code, "sector"])
        r = recent_return(df, ref_date, 5)
        if pd.notna(r):
            vals.setdefault(sec, []).append(float(r))
    return {k: float(np.nanmean(v)) for k, v in vals.items() if v}


def analyze_one_signal(
    code: str,
    name: str,
    sector: str,
    df: pd.DataFrame,
    market_df: pd.DataFrame,
    sector_ret5: float,
    ref_date: pd.Timestamp,
) -> Optional[Dict[str, Any]]:
    cut = df[df.index <= ref_date].copy()
    if len(cut) < 160:
        return None
    future = df[df.index > ref_date]
    if len(future) < 3:
        return None

    w = calc_weekly_signal(cut)
    d = calc_daily_signal(cut)
    p = calc_60m_proxy_signal(cut)
    pullback, pb_info = classify_pullback(cut, market_df, sector_ret5, ref_date)
    td_label, td_group, td_reason = classify_topdown(w, d, p, pullback)
    stop_price, stop_pct, stop_src = calc_stop_price(cut)
    last_close = float(cut["close"].iloc[-1])
    last_volume = float(cut["volume"].iloc[-1]) if pd.notna(cut["volume"].iloc[-1]) else np.nan
    trade_value_b = (last_close * last_volume / 100_000_000.0) if pd.notna(last_volume) else np.nan
    op_score = calc_operation_score(w, d, p, pullback, sector, sector_ret5, stop_pct, trade_value_b)
    op_label, op_reason = classify_operation_label(td_label, w, d, p, pullback, sector, sector_ret5, stop_pct, op_score)
    evals = evaluate_path(df, ref_date, stop_price)
    if not evals:
        return None
    entry_modes = evaluate_entry_modes(df, ref_date, float(cut["high"].iloc[-1]), stop_price)

    row = {
        "ref_date": ref_date.strftime("%Y-%m-%d"),
        "code": code,
        "name": name,
        "sector": sector,
        "td_label": td_label,
        "td_group": td_group,
        "td_reason": td_reason,
        "op_label": op_label,
        "op_reason": op_reason,
        "op_score": op_score,
        "sector_grade": sector_grade(sector, sector_ret5),
        "pullback_cause": pullback,
        "weekly_pass": w.pass_weekly,
        "weekly_above_20w": w.above_20w,
        "weekly_ma20w_up": w.ma20w_up,
        "weekly_macd_hist_up": w.macd_hist_up,
        "daily_ready": d.ready,
        "daily_watch": d.watch,
        "daily_near_ma20": d.near_ma20,
        "daily_stoch_exit": d.stoch_exit,
        "daily_macd_div": d.macd_div,
        "proxy_yes": p.yes,
        "proxy_breakout": p.breakout,
        "proxy_volume_ok": p.volume_ok,
        "proxy_ma5_up": p.ma5_up,
        "proxy_macd_ok": p.macd_gc_or_up,
        "proxy_vol_ratio": p.vol_ratio,
        "entry_close": last_close,
        "trade_value_b": trade_value_b,
        "stop_price": stop_price,
        "stop_pct": stop_pct,
        "stop_src": stop_src,
        "stock_ret5": pb_info.get("stock_ret5", np.nan),
        "market_ret5": pb_info.get("market_ret5", np.nan),
        "sector_ret5": pb_info.get("sector_ret5", np.nan),
        "ma20_dist": pb_info.get("ma20_dist", np.nan),
    }
    row.update(evals)
    row.update(entry_modes)
    return row


def select_operation_candidates(result: pd.DataFrame, per_bucket_limit: int = 5, dedup_days: int = 10) -> pd.DataFrame:
    """기준일별 TD-CORE/PRIME/RECOVER/WATCH 상위 후보만 남김 + 동일 종목 중복 제거."""
    if result.empty or "op_label" not in result.columns:
        return result.copy()

    x = result.copy()
    x["_ref_ts"] = pd.to_datetime(x["ref_date"], errors="coerce")
    x["_op_rank"] = x["op_label"].map({v: i for i, v in enumerate(OP_LABEL_ORDER)}).fillna(99)
    selected: List[pd.DataFrame] = []
    last_pick: Dict[str, pd.Timestamp] = {}
    cal_gap = max(1, int(dedup_days * 1.6))

    for ref_date, day in x.sort_values(["_ref_ts", "_op_rank", "op_score"], ascending=[True, True, False]).groupby("_ref_ts"):
        used_today = set()
        for bucket in OP_LABEL_ORDER:
            if bucket == "🔴 TD-RISK":
                continue
            b = day[day["op_label"] == bucket].sort_values(["op_score", "trade_value_b"], ascending=[False, False])
            keep_rows = []
            for idx, r in b.iterrows():
                code = str(r["code"])
                if code in used_today:
                    continue
                prev = last_pick.get(code)
                if prev is not None and pd.notna(ref_date) and (ref_date - prev).days <= cal_gap:
                    continue
                keep_rows.append(idx)
                used_today.add(code)
                last_pick[code] = ref_date
                if len(keep_rows) >= per_bucket_limit:
                    break
            if keep_rows:
                selected.append(x.loc[keep_rows])

    if not selected:
        return x.iloc[0:0].drop(columns=["_ref_ts", "_op_rank"], errors="ignore")
    out = pd.concat(selected, ignore_index=True)
    out = out.sort_values(["_ref_ts", "_op_rank", "op_score"], ascending=[False, True, False])
    return out.drop(columns=["_ref_ts", "_op_rank"], errors="ignore")


def mode_stat_line(df: pd.DataFrame, label: str, prefix: str, trigger_col: Optional[str] = None) -> str:
    if df is None or df.empty:
        return f"- {label}: 데이터 없음"
    x = df.copy()
    if trigger_col:
        total = len(x)
        x = x[x[trigger_col] == True]
        trigger_note = f" | 체결 {len(x)}/{total}개"
    else:
        trigger_note = f" | 체결 {len(x)}개"
    if x.empty:
        return f"- {label}: 체결 없음"
    r5 = pd.to_numeric(x.get(f"{prefix}_ret5"), errors="coerce")
    r10 = pd.to_numeric(x.get(f"{prefix}_ret10"), errors="coerce")
    hit3 = x.get(f"{prefix}_hit3_first")
    hit5 = x.get(f"{prefix}_hit5_first")
    stop = x.get(f"{prefix}_stop_first")
    return (
        f"- {label}{trigger_note} | 5일 {fmt_pct(r5.mean())} | 10일 {fmt_pct(r10.mean())} | "
        f"5일중앙 {fmt_pct(r5.median())} | +3먼저 {pd.Series(hit3).mean()*100:.1f}% | "
        f"+5먼저 {pd.Series(hit5).mean()*100:.1f}% | 손절먼저 {pd.Series(stop).mean()*100:.1f}%"
    )


def entry_mode_report(df: pd.DataFrame, title: str) -> List[str]:
    lines = [f"\n[{title}]"]
    if df.empty:
        lines.append("- 데이터 없음")
        return lines
    lines.append(mode_stat_line(df, "A. 기준일 종가 진입", "close_entry"))
    lines.append(mode_stat_line(df, "B. 다음날 시가 진입", "next_open"))
    lines.append(mode_stat_line(df, "C. 다음날 전일고점 돌파확인", "breakout_entry", "breakout_trigger"))
    return lines


def format_operation_examples(df: pd.DataFrame, title: str, n: int = 10, op_filter: Optional[Iterable[str]] = None) -> List[str]:
    lines = [f"\n[{title}]"]
    if df.empty:
        lines.append("- 데이터 없음")
        return lines
    x = df.copy()
    if op_filter:
        x = x[x["op_label"].isin(list(op_filter))]
    if x.empty:
        lines.append("- 데이터 없음")
        return lines
    x = x.sort_values(["op_score", "ret5"], ascending=[False, False]).head(n)
    for _, r in x.iterrows():
        lines.append(
            f"- {r['ref_date']} {r['name']}({r['code']}) | {r['op_label']} / {r['td_label']} | 점수 {fmt_num(r['op_score'],1)} | "
            f"{r['sector']}·{r['sector_grade']} | {r['pullback_cause']} | "
            f"주봉:{'PASS' if r['weekly_pass'] else 'FAIL'} / 일봉:{'READY' if r['daily_ready'] else ('WATCH' if r['daily_watch'] else 'WAIT')} / 프록시:{'YES' if r['proxy_yes'] else 'NO'} | "
            f"5일 {fmt_pct(r['ret5'])} / 10일 {fmt_pct(r['ret10'])} | 손절 {fmt_pct(r['stop_pct'])} | {r.get('op_reason','')}"
        )
    return lines


def build_summary(result: pd.DataFrame, ref_dates: List[pd.Timestamp], universe_n: int, out_dir: Path, selected: Optional[pd.DataFrame] = None) -> str:
    lines: List[str] = []
    lines.append(f"🧪 [{VERSION}]")
    lines.append("- 방식: 주봉→일봉→60분프록시 + 눌림 원인 분리 + PRIME/RECOVER 세분화 + 운용형 후보 압축")
    lines.append(f"- 기준일 수: {len(ref_dates)}개 | 유니버스: {universe_n}개 | 검증신호: {len(result)}개")
    lines.append("- 기준일: " + ", ".join([d.strftime("%Y-%m-%d") for d in ref_dates]))
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    if result.empty:
        lines.append("\n[전체]\n- 검증신호 없음")
        return "\n".join(lines)

    lines.append("\n[전체]")
    lines.append(
        f"- 검증 {len(result)}개 | 1일 {fmt_pct(result['ret1'].mean())} | 3일 {fmt_pct(result['ret3'].mean())} | "
        f"5일 {fmt_pct(result['ret5'].mean())} | 10일 {fmt_pct(result['ret10'].mean())}"
    )
    lines.append(
        f"- 5일 중앙값 {fmt_pct(result['ret5'].median())} | 10% 절사평균 {fmt_pct(trim_mean(result['ret5']))} | "
        f"최고·최저 제외평균 {fmt_pct(excl_extreme_mean(result['ret5']))}"
    )
    lines.append(
        f"- +3먼저 {result['hit3_first'].mean()*100:.1f}% | +5먼저 {result['hit5_first'].mean()*100:.1f}% | "
        f"손절먼저 {result['stop_first'].mean()*100:.1f}% | 평균최대상승 {fmt_pct(result['max_up10'].mean())} | 평균최대하락 {fmt_pct(result['max_dn10'].mean())}"
    )

    order = ["🟢 T-CORE", "🔥 T-PRIME", "🟡 T-WATCH", "🟠 R-PRIME", "🟠 R-WAIT", "🟤 R-RISK", "🔴 T-FAIL"]
    lines += group_report(result, "td_label", "V1107.3 원본 TOPDOWN 라벨별 성과", order=order)

    if selected is not None and not selected.empty:
        lines.append("\n[운용형 압축 후보 성과 — 날짜별 상위 제한/중복제거 후]")
        lines.append(f"- 원본 {len(result)}개 → 운용형 선택 {len(selected)}개")
        lines += group_report(selected, "op_label", "TD 운용라벨별 성과", order=OP_LABEL_ORDER)
        lines += group_report(selected, "sector_grade", "섹터 강약별 성과")
        lines += entry_mode_report(selected, "진입 방식별 성과 비교 — 운용형 선택 후보")
        for bucket in ["🟢 TD-CORE", "🔥 TD-PRIME", "🟠 TD-RECOVER"]:
            sub = selected[selected["op_label"] == bucket]
            if not sub.empty:
                lines += entry_mode_report(sub, f"진입 방식별 성과 — {bucket}")
    else:
        lines.append("\n[운용형 압축 후보 성과]\n- 선택 후보 없음")

    lines += group_report(result, "pullback_cause", "눌림 원인별 성과")
    lines += group_report(result, "weekly_pass", "주봉 PASS 여부별 성과")
    lines += group_report(result, "daily_ready", "일봉 READY 여부별 성과")
    lines += group_report(result, "proxy_yes", "60분봉 프록시 여부별 성과")
    lines += group_report(result, "sector", "섹터별 성과", min_n=3)

    lines.append("\n[V1107.3 조건별 진단]")
    conds = [
        ("주가 20주선 위", "weekly_above_20w"),
        ("20주선 우상향", "weekly_ma20w_up"),
        ("주봉 MACD 히스토그램 상승", "weekly_macd_hist_up"),
        ("일봉 20일선 눌림", "daily_near_ma20"),
        ("스토캐스틱 30 이하 탈출", "daily_stoch_exit"),
        ("일봉 MACD 강세 다이버전스", "daily_macd_div"),
        ("60분프록시 거래량 돌파", "proxy_yes"),
    ]
    for title, col in conds:
        yes = result[result[col] == True]
        no = result[result[col] != True]
        if not yes.empty:
            lines.append(f"- {title}: {len(yes)}개 | 5일 {fmt_pct(yes['ret5'].mean())} | 손절먼저 {yes['stop_first'].mean()*100:.1f}%")
        if not no.empty:
            lines.append(f"  ↳ 미충족: {len(no)}개 | 5일 {fmt_pct(no['ret5'].mean())} | 손절먼저 {no['stop_first'].mean()*100:.1f}%")

    if selected is not None and not selected.empty:
        lines += format_operation_examples(selected, "TD-CORE/PRIME/RECOVER 운용 후보 예시", 12, ["🟢 TD-CORE", "🔥 TD-PRIME", "🟠 TD-RECOVER"])
        lines += format_operation_examples(selected, "TD-WATCH 관찰 후보 예시", 8, ["🟡 TD-WATCH"])

    lines += format_examples(result, "T-CORE 안정형 예시", 8, ["🟢 T-CORE"])
    lines += format_examples(result, "T-PRIME 공격형 예시", 10, ["🔥 T-PRIME"])
    lines += format_examples(result, "T-WATCH 관찰 예시", 8, ["🟡 T-WATCH"])
    lines += format_examples(result, "R-PRIME 회복공격 예시", 8, ["🟠 R-PRIME"])
    lines += format_examples(result, "T-FAIL 예외 상승 예시", 8, ["🔴 T-FAIL"])

    bad = result[result["pullback_cause"].str.contains("STOCK_BREAKDOWN|SECTOR_PULLBACK", na=False)].copy()
    lines += format_examples(bad, "STOCK/SECTOR 훼손 예시", 8, None, sort_col="ret5")

    lines.append("\n[V1107.3 적용 가이드]")
    lines.append("- 🟢 TD-CORE: T-CORE 중 손절거리·섹터·눌림원인까지 좋은 안정형 후보입니다.")
    lines.append("- 🔥 TD-PRIME: 주봉 PASS + 60분프록시 YES + 정상/지수/건강한 눌림. 공격형 보강 후보입니다.")
    lines.append("- 🟡 TD-WATCH: 주봉/눌림은 양호하지만 60분프록시 또는 일봉 타점이 부족합니다. 거래량 재돌파 전까지 관찰입니다.")
    lines.append("- 🟠 TD-RECOVER: R-PRIME 중 섹터/종목 훼손이 없는 회복 후보입니다. 소액/확인형입니다.")
    lines.append("- 🔴 TD-RISK: STOCK_BREAKDOWN, 섹터 붕괴, 주봉 실패, 손절거리 과다 후보입니다. 실전 TOP과 분리합니다.")
    lines.append("- 🔴 T-FAIL: 주봉 PASS 실패 또는 구조 미완성입니다. 대박 예외가 있어도 실전 TOP과 분리합니다.")
    lines.append("- INDEX_PULLBACK은 지수 영향 눌림으로 유지, HEALTHY_PULLBACK은 우선 관찰, SECTOR_PULLBACK/STOCK_BREAKDOWN은 회복 확인 전 실전 제외가 기본입니다. C. 다음날 전일고점 돌파확인 진입이 종가진입보다 좋은지 같이 확인합니다.")
    lines.append(f"\n📁 상세 CSV/HTML 저장 위치: {out_dir}")
    return "\n".join(lines)


def run_backtest(args: argparse.Namespace) -> Tuple[pd.DataFrame, str, Path]:
    weeks = int(args.weeks)
    universe_top = int(args.universe_top)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    today = now_kst()
    ref_dates = make_ref_dates(today, weeks, args.ref_dates)
    # 보유 10거래일 평가를 위해 미래 여유를 둡니다. 실제 미래 데이터가 없으면 평가 가능한 만큼만 계산됩니다.
    start = (min(ref_dates).to_pydatetime() - timedelta(days=520)).strftime("%Y-%m-%d")
    end = (max(ref_dates).to_pydatetime() + timedelta(days=35)).strftime("%Y-%m-%d")

    print(f"✅ {VERSION} 시작 | {today.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 데이터 범위: {start} ~ {end}")

    listing = load_krx_listing(universe_top)
    print(f"📌 유니버스 {len(listing)}개 로드")

    price_map: Dict[str, pd.DataFrame] = {}
    for i, r in listing.iterrows():
        code = str(r["code"]).zfill(6)
        df = fetch_price(code, start, end)
        if not df.empty:
            price_map[code] = df
        if (i + 1) % 25 == 0:
            print(f"  - 데이터 수집 {i + 1}/{len(listing)}")

    market_df = fetch_index("KS11", start, end)
    if market_df.empty and price_map:
        # fallback: 유니버스 평균 close로 시장 프록시 생성
        all_close = []
        for c, df in price_map.items():
            all_close.append(safe_num(df["close"]).rename(c))
        m = pd.concat(all_close, axis=1).mean(axis=1).dropna()
        market_df = pd.DataFrame({"open": m, "high": m, "low": m, "close": m, "volume": 0.0})

    rows: List[Dict[str, Any]] = []
    meta = listing.set_index("code")
    for rd in ref_dates:
        print(f"🔎 기준일 분석: {rd.strftime('%Y-%m-%d')}")
        sector_rets = build_sector_returns(price_map, listing, rd)
        for code, df in price_map.items():
            if code not in meta.index:
                continue
            name = str(meta.loc[code, "name"])
            sector = str(meta.loc[code, "sector"])
            sig = analyze_one_signal(code, name, sector, df, market_df, sector_rets.get(sector, np.nan), rd)
            if sig:
                rows.append(sig)

    result = pd.DataFrame(rows)
    ts = now_kst().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"topdown_v1107_3_all_detail_{ts}.csv"
    selected_csv_path = out_dir / f"topdown_v1107_3_operation_selected_{ts}.csv"
    html_path = out_dir / f"topdown_v1107_3_report_{ts}.html"

    selected = pd.DataFrame()
    if not result.empty:
        # 보기 좋은 정렬: 운용형 우선, 점수/수익률 보조
        rank_map = {v: i for i, v in enumerate(OP_LABEL_ORDER)}
        result["_rank"] = result["op_label"].map(rank_map).fillna(99)
        result = result.sort_values(["_rank", "ref_date", "op_score", "ret10"], ascending=[True, False, False, False]).drop(columns=["_rank"])
        selected = select_operation_candidates(result, args.per_bucket_limit, args.dedup_days)
        result.to_csv(csv_path, index=False, encoding="utf-8-sig")
        selected.to_csv(selected_csv_path, index=False, encoding="utf-8-sig")
    else:
        result.to_csv(csv_path, index=False, encoding="utf-8-sig")
        selected.to_csv(selected_csv_path, index=False, encoding="utf-8-sig")

    summary = build_summary(result, ref_dates, len(listing), out_dir, selected=selected)
    to_html_report(selected if not selected.empty else result, summary, html_path)
    print(summary)
    print(f"📁 ALL CSV: {csv_path}")
    print(f"📁 SELECTED CSV: {selected_csv_path}")
    print(f"📁 HTML: {html_path}")

    return selected if not selected.empty else result, summary, out_dir


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=VERSION)
    p.add_argument("--weeks", type=int, default=env_int("TOPDOWN_WEEKS", 52))
    p.add_argument("--universe-top", type=int, default=env_int("TOPDOWN_UNIVERSE_TOP", 150))
    p.add_argument("--ref-dates", type=str, default=os.getenv("TOPDOWN_REF_DATES", ""))
    p.add_argument("--out-dir", type=str, default=os.getenv("TOPDOWN_OUT_DIR", "topdown_v1107_3_logs"))
    p.add_argument("--per-bucket-limit", type=int, default=env_int("TOPDOWN_PER_BUCKET_LIMIT", 5))
    p.add_argument("--dedup-days", type=int, default=env_int("TOPDOWN_DEDUP_DAYS", 10))
    p.add_argument("--send-telegram", action="store_true", default=parse_bool(os.getenv("TOPDOWN_SEND_TELEGRAM", "0")))
    return p


def main() -> None:
    args = build_argparser().parse_args()
    try:
        _, summary, _ = run_backtest(args)
        if args.send_telegram:
            telegram_send(summary)
    except Exception as e:
        msg = f"🚨 {VERSION} 실패: {e}\n{traceback.format_exc()}"
        print(msg)
        if args.send_telegram:
            telegram_send(msg[:3500])
        sys.exit(1)


if __name__ == "__main__":
    main()
