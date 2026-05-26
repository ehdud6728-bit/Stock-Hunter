# -*- coding: utf-8 -*-
"""
V1107_4_3_TOPDOWN_OUTPUT_SYNC_BACKTEST_20260526

주봉 → 일봉 → 60분봉 프록시 탑다운 백테스트
+ 지수/섹터/종목 눌림 원인 분리
+ TD-PRIME / TD-CORE / TD-RECOVER / TD-WATCH / TD-RISK 운용형 압축
+ V1107.4.3 보정:
  1) TD-RISK 선분류 우선 적용
  2) 운용형 후보만 따로 압축해 실전 후보 수 과대표시 방지
  3) TD-PRIME 전일고점 돌파확인 진입 성과 별도 비교
  4) 다음날 갭 +2% 이상 추격금지 조건 비교
  5) 섹터 강약/눌림 원인/라벨별 손절먼저 비율 재검증

실행 예:
  python topdown_v1107_4_3_backtest.py --send-telegram
  python topdown_v1107_4_3_backtest.py --weeks 52 --universe-top 150

환경변수:
  TOPDOWN_WEEKS=52
  TOPDOWN_UNIVERSE_TOP=150
  TOPDOWN_REF_DATES=2026-05-08,2026-05-15
  TOPDOWN_OUT_DIR=topdown_v1107_4_3_logs
  TOPDOWN_MAX_PER_DAY=10
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
from dataclasses import dataclass, asdict
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

VERSION = "V1107_4_3_TOPDOWN_OUTPUT_SYNC_BACKTEST_20260526"
KST = pytz.timezone("Asia/Seoul") if pytz else None

BAD_NAME_PAT = re.compile(
    r"(?:ETF|ETN|스팩|SPAC|리츠|REIT|우선주|우\b|우\)|인버스|레버리지|선물|채권|액티브|합성|TR|커버드콜)",
    re.IGNORECASE,
)

SECTOR_KEYWORDS: List[Tuple[str, str]] = [
    ("반도체", "삼성전자|SK하이닉스|DB하이텍|한미반도체|주성엔지니어링|테크윙|이수페타시스|하나마이크론|리노공업|동진쎄미켐|코리아써키트|SFA반도체|제주반도체|피델릭스|텔레칩스|아이텍|인텍플러스|심텍|HPSP|대덕전자"),
    ("로봇/자동화", "두산로보틱스|레인보우로보틱스|로보티즈|휴림로봇|에스피지|라온로보틱스|제닉스|유일로보틱스|티로보틱스|고영|코스모로보틱스"),
    ("AI전력/전력설비", "HD현대일렉트릭|LS ELECTRIC|LS일렉트릭|LS|산일전기|제룡전기|효성중공업|일진전기|대한전선|가온전선|비나텍|두산에너빌리티"),
    ("원전/우라늄", "두산에너빌리티|한전기술|한전산업|우리기술|비에이치아이|보성파워텍|우진|일진파워"),
    ("바이오/헬스케어", "셀트리온|삼성바이오로직스|에이비엘바이오|한미약품|녹십자|펩트론|알테오젠|리가켐|리가켐바이오|바이오니아|바이오플러스|현대바이오|한올바이오파마|원텍|클래시스|올릭스|삼천당제약"),
    ("금융/은행", "KB금융|신한지주|하나금융지주|우리금융지주|메리츠금융지주|기업은행|한화생명|한화손해보험|DB손해보험|삼성생명|미래에셋증권|키움증권|BNK금융지주"),
    ("자동차/부품", "현대차|기아|현대모비스|HL만도|현대글로비스|성우하이텍|화신|에스엘|한국타이어"),
    ("조선/해운", "HD현대중공업|HD한국조선해양|한화오션|삼성중공업|현대미포|HMM|팬오션|흥아해운|STX그린로지스|HD현대마린솔루션"),
    ("항공/운송", "대한항공|진에어|제주항공|티웨이항공|아시아나항공|현대글로비스|CJ대한통운|한진"),
    ("소프트웨어/클라우드", "NAVER|카카오|더존비즈온|안랩|한글과컴퓨터|크래프톤|하이브|엔씨소프트|넷마블|삼성에스디에스|현대오토에버"),
    ("정유/에너지", "SK이노베이션|S-Oil|GS|HD현대|흥구석유|중앙에너비스|한국석유|대성에너지|OCI홀딩스|한화솔루션"),
    ("2차전지/EV", "LG에너지솔루션|삼성SDI|포스코퓨처엠|에코프로|에코프로머티|엘앤에프|천보|금양|포스코DX"),
    ("우주항공/방산", "한화에어로스페이스|한국항공우주|LIG넥스원|LIG디펜스|현대로템|한화시스템|AP위성|쎄트렉아이"),
]

STRONG_SECTORS = {"반도체", "AI전력/전력설비", "로봇/자동화", "원전/우라늄", "자동차/부품"}
WEAK_SECTORS = {"항공/운송", "소프트웨어/클라우드", "조선/해운"}

TD_LABEL_ORDER = ["🟢 T-CORE", "🔥 T-PRIME", "🟡 T-WATCH", "🟠 R-PRIME", "🟠 R-WAIT", "🟤 R-RISK", "🔴 T-FAIL"]
OP_LABEL_ORDER = ["🟢 TD-CORE", "🔥 TD-PRIME", "🟠 TD-RECOVER", "🟡 TD-WATCH", "🔴 TD-RISK"]


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


def pct(a: Any, b: Any) -> float:
    try:
        a = float(a)
        b = float(b)
        if b == 0 or not np.isfinite(a) or not np.isfinite(b):
            return np.nan
        return (a / b - 1.0) * 100.0
    except Exception:
        return np.nan


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
    for c in ["open", "high", "low", "close", "volume"]:
        if c not in out.columns:
            out[c] = np.nan
        out[c] = safe_num(out[c])
    out = out[["open", "high", "low", "close", "volume"]].replace([np.inf, -np.inf], np.nan)
    out = out.dropna(subset=["close"]).sort_index()
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


def sector_strength(sector: str) -> str:
    if sector in STRONG_SECTORS:
        return "STRONG_SECTOR"
    if sector in WEAK_SECTORS:
        return "WEAK_SECTOR"
    return "NEUTRAL_SECTOR"


def load_krx_listing(universe_top: int) -> pd.DataFrame:
    if fdr is None:
        raise RuntimeError(
            "FinanceDataReader가 설치되어 있지 않습니다. workflow pip 명령은 'finance-datareader'로 설치하세요."
        )
    frames: List[pd.DataFrame] = []
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
    colmap: Dict[str, str] = {}
    for c in listing.columns:
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
    listing["sector_strength"] = listing["sector"].map(sector_strength)
    listing = listing.drop_duplicates("code", keep="first")
    return listing.head(universe_top).reset_index(drop=True)


def fetch_price(code: str, start: str, end: str, retry: int = 2) -> pd.DataFrame:
    if fdr is None:
        return pd.DataFrame()
    last_err: Optional[Exception] = None
    for i in range(retry + 1):
        try:
            return clean_ohlcv(fdr.DataReader(str(code).zfill(6), start, end))
        except Exception as e:
            last_err = e
            time.sleep(0.15 * (i + 1))
    print(f"⚠️ price load fail {code}: {last_err}")
    return pd.DataFrame()


def fetch_index(symbol: str, start: str, end: str) -> pd.DataFrame:
    if fdr is None:
        return pd.DataFrame()
    for s in [symbol, "KS11", "KQ11"]:
        try:
            out = clean_ohlcv(fdr.DataReader(s, start, end))
            if not out.empty:
                return out
        except Exception:
            continue
    return pd.DataFrame()


def make_ref_dates(end_date: datetime, weeks: int, explicit: str = "") -> List[pd.Timestamp]:
    if explicit:
        return sorted(pd.Timestamp(x.strip()) for x in explicit.split(",") if x.strip())
    end_ts = pd.Timestamp(end_date.date())
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
    stoch_cross = bool(
        len(k) >= 2
        and pd.notna(k.iloc[-2]) and pd.notna(d.iloc[-2])
        and k.iloc[-2] <= d.iloc[-2]
        and k.iloc[-1] > d.iloc[-1]
    )
    stoch_exit = bool((pd.notna(recent_k_min) and recent_k_min <= 35 and k.iloc[-1] > d.iloc[-1]) or (stoch_cross and k.iloc[-1] <= 55))

    macd_div = False
    if len(close) >= 45 and hist.notna().sum() >= 20:
        c_recent_low = close.iloc[-12:].min()
        c_prev_low = close.iloc[-35:-12].min()
        h_recent_low = hist.iloc[-12:].min()
        h_prev_low = hist.iloc[-35:-12].min()
        hist_rising = hist.iloc[-1] > hist.iloc[-3] if pd.notna(hist.iloc[-3]) and pd.notna(hist.iloc[-1]) else False
        macd_div = bool(
            pd.notna(c_recent_low) and pd.notna(c_prev_low)
            and pd.notna(h_recent_low) and pd.notna(h_prev_low)
            and c_recent_low <= c_prev_low * 1.03
            and h_recent_low > h_prev_low
            and hist_rising
        )
    score = int(near_ma20) + int(stoch_exit) + int(macd_div)
    ready = bool(score >= 2 and near_ma20 and (stoch_exit or macd_div))
    watch = bool(score >= 1 and near_ma20)
    return DailySignal(ready, watch, near_ma20, stoch_exit, macd_div, c, m20, k0, d0, h0)


def calc_60m_proxy_signal(df: pd.DataFrame) -> ProxySignal:
    """분봉 데이터가 없을 때 쓰는 일봉 기반 60분 돌파 프록시. 실제 live에서는 60분 OHLCV로 교체 가능."""
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
    macd_gc_or_up = bool(len(hist) >= 2 and pd.notna(hist.iloc[-1]) and pd.notna(hist.iloc[-2]) and (hist.iloc[-1] > 0 or hist.iloc[-1] > hist.iloc[-2]))
    near_high = bool(pd.notna(prev_high) and close.iloc[-1] >= prev_high * 0.985)
    yes = bool((breakout and volume_ok and (ma5_up or macd_gc_or_up)) or (near_high and volume_ok and ma5_up and macd_gc_or_up))
    return ProxySignal(yes, breakout, volume_ok, ma5_up, macd_gc_or_up, vol_ratio, float(prev_high) if pd.notna(prev_high) else np.nan)


def get_ret_at_or_before(df: pd.DataFrame, ref_date: pd.Timestamp, days: int = 5) -> float:
    if df is None or df.empty:
        return np.nan
    idx = df.index.searchsorted(ref_date, side="right") - 1
    if idx < days or idx >= len(df):
        return np.nan
    return pct(df["close"].iloc[idx], df["close"].iloc[idx - days])


def calc_sector_returns(price_map: Dict[str, pd.DataFrame], listing: pd.DataFrame, ref_dates: List[pd.Timestamp], days: int = 5) -> Dict[Tuple[pd.Timestamp, str], float]:
    sector_by_code = dict(zip(listing["code"], listing["sector"]))
    out: Dict[Tuple[pd.Timestamp, str], float] = {}
    for rd in ref_dates:
        vals: Dict[str, List[float]] = {}
        for code, df in price_map.items():
            sec = sector_by_code.get(code, "미분류")
            r = get_ret_at_or_before(df, rd, days=days)
            if pd.notna(r):
                vals.setdefault(sec, []).append(float(r))
        for sec, arr in vals.items():
            if arr:
                out[(rd, sec)] = float(np.nanmean(arr))
    return out


def classify_pullback(
    stock_ret5: float,
    index_ret5: float,
    sector_ret5: float,
    cut: pd.DataFrame,
) -> str:
    volume = safe_num(cut["volume"])
    close = safe_num(cut["close"])
    ma20 = close.rolling(20, min_periods=10).mean()
    v20 = volume.rolling(20, min_periods=10).mean()
    vol_cooling = False
    ma20_alive = False
    try:
        vol_cooling = bool(pd.notna(v20.iloc[-1]) and v20.iloc[-1] > 0 and volume.iloc[-1] <= v20.iloc[-1] * 1.25)
        ma20_alive = bool(pd.notna(ma20.iloc[-1]) and close.iloc[-1] >= ma20.iloc[-1] * 0.96)
    except Exception:
        pass

    if pd.notna(stock_ret5) and stock_ret5 <= -8.0 and (pd.isna(index_ret5) or index_ret5 > -3.0) and (pd.isna(sector_ret5) or sector_ret5 > -4.0):
        return "🔴 STOCK_BREAKDOWN"
    if pd.notna(sector_ret5) and sector_ret5 <= -4.0 and (pd.isna(index_ret5) or index_ret5 > -3.5):
        return "🟠 SECTOR_PULLBACK"
    if pd.notna(index_ret5) and index_ret5 <= -3.0:
        return "🟡 INDEX_PULLBACK"
    if pd.notna(stock_ret5) and -8.0 < stock_ret5 <= -1.0 and vol_cooling and ma20_alive:
        return "🟢 HEALTHY_PULLBACK"
    if pd.notna(stock_ret5) and -8.0 < stock_ret5 <= 3.0:
        return "🟡 NORMAL_PULLBACK"
    return "⚪ MIXED_PULLBACK"


def label_topdown(weekly: WeeklySignal, daily: DailySignal, proxy: ProxySignal, pullback: str) -> Tuple[str, str]:
    broken = pullback.startswith("🔴 STOCK_BREAKDOWN")
    sector_break = pullback.startswith("🟠 SECTOR_PULLBACK")
    if not weekly.pass_weekly:
        return "🔴 T-FAIL", "주봉 PASS 실패 또는 구조 미완성"
    if broken or sector_break:
        if proxy.yes:
            return "🟤 R-RISK", "주봉은 살아 있으나 섹터/종목 훼손 가능성"
        return "🟤 R-RISK", "섹터/종목 훼손 가능성"
    if daily.ready and proxy.yes:
        return "🟢 T-CORE", "주봉 PASS + 일봉 READY + 60분프록시 YES + 붕괴 아님"
    if proxy.yes and pullback in {"🟢 HEALTHY_PULLBACK", "🟡 INDEX_PULLBACK", "🟡 NORMAL_PULLBACK"}:
        return "🔥 T-PRIME", "주봉 PASS + 60분프록시 YES + 정상/지수/건강한 눌림"
    if daily.watch or daily.ready:
        return "🟡 T-WATCH", "주봉/눌림은 양호하지만 60분프록시 없음"
    if proxy.yes:
        return "🟠 R-PRIME", "주봉 PASS + 60분프록시 YES이나 일봉 타점/눌림 분류 미완성"
    return "🟠 R-WAIT", "주봉은 살아 있으나 일봉/분봉 타점 대기"


def calc_stop_pct(cut: pd.DataFrame, entry: float) -> float:
    if entry <= 0 or cut is None or cut.empty:
        return -10.0
    lows = safe_num(cut["low"]).iloc[-10:]
    recent_low = float(lows.min()) if len(lows) and pd.notna(lows.min()) else entry * 0.9
    stop_pct = pct(recent_low, entry)
    if pd.isna(stop_pct):
        return -10.0
    # 너무 먼 손절은 -10%로 캡, 너무 가까운 손절은 -3% 최소폭 부여
    stop_pct = max(float(stop_pct), -10.0)
    stop_pct = min(stop_pct, -3.0)
    return stop_pct


def eval_entry_outcome(
    df: pd.DataFrame,
    sig_pos: int,
    entry_price: float,
    stop_pct: float,
    start_pos: int,
    hold_days: int = 10,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"filled": False}
    if df is None or df.empty or entry_price <= 0 or start_pos >= len(df):
        return out
    out["filled"] = True
    out["entry"] = float(entry_price)
    out["stop_pct"] = float(stop_pct)
    stop_price = entry_price * (1.0 + stop_pct / 100.0)

    for n in [1, 3, 5, 10]:
        pos = start_pos + n - 1
        if pos < len(df):
            out[f"ret{n}d"] = pct(df["close"].iloc[pos], entry_price)
        else:
            out[f"ret{n}d"] = np.nan

    future = df.iloc[start_pos : min(len(df), start_pos + hold_days)].copy()
    if future.empty:
        out["max_up"] = np.nan
        out["max_down"] = np.nan
        out["hit3_first"] = False
        out["hit5_first"] = False
        out["stop_first"] = False
        return out

    out["max_up"] = pct(safe_num(future["high"]).max(), entry_price)
    out["max_down"] = pct(safe_num(future["low"]).min(), entry_price)

    hit3 = False
    hit5 = False
    stop_first = False
    resolved3 = False
    resolved5 = False
    for _, row in future.iterrows():
        hi = float(row["high"])
        lo = float(row["low"])
        if not resolved3:
            if lo <= stop_price:
                stop_first = True
                resolved3 = True
            elif hi >= entry_price * 1.03:
                hit3 = True
                resolved3 = True
        if not resolved5:
            if lo <= stop_price:
                resolved5 = True
            elif hi >= entry_price * 1.05:
                hit5 = True
                resolved5 = True
    out["hit3_first"] = bool(hit3)
    out["hit5_first"] = bool(hit5)
    out["stop_first"] = bool(stop_first)
    return out


def eval_all_entries(df: pd.DataFrame, cut: pd.DataFrame, sig_pos: int, proxy: ProxySignal) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if sig_pos < 0 or sig_pos >= len(df) - 1:
        return out
    close_entry = float(df["close"].iloc[sig_pos])
    stop_pct_a = calc_stop_pct(cut, close_entry)
    out["A_close"] = eval_entry_outcome(df, sig_pos, close_entry, stop_pct_a, sig_pos + 1)

    next_pos = sig_pos + 1
    if next_pos < len(df):
        open_entry = float(df["open"].iloc[next_pos])
        stop_pct_b = calc_stop_pct(cut, open_entry)
        out["B_next_open"] = eval_entry_outcome(df, sig_pos, open_entry, stop_pct_b, next_pos)

        prev_high = float(df["high"].iloc[sig_pos])
        if pd.notna(prev_high) and float(df["high"].iloc[next_pos]) >= prev_high:
            if float(df["open"].iloc[next_pos]) > prev_high:
                c_entry = float(df["open"].iloc[next_pos])
            else:
                c_entry = prev_high
            stop_pct_c = calc_stop_pct(cut, c_entry)
            c = eval_entry_outcome(df, sig_pos, c_entry, stop_pct_c, next_pos)
            c["triggered"] = True
            c["gap_pct"] = pct(float(df["open"].iloc[next_pos]), close_entry)
            out["C_prev_high_break"] = c
            # V1107.4.3: 다음날 시가 갭 +2% 이상은 추격금지 버전도 별도 검증
            if pd.notna(c["gap_pct"]) and float(c["gap_pct"]) <= 2.0:
                nc = dict(c)
                nc["filled"] = True
                nc["triggered"] = True
                out["D_prev_high_no_chase"] = nc
            else:
                out["D_prev_high_no_chase"] = {"filled": False, "triggered": False, "gap_pct": c.get("gap_pct")}
        else:
            out["C_prev_high_break"] = {"filled": False, "triggered": False, "gap_pct": pct(float(df["open"].iloc[next_pos]), close_entry)}
            out["D_prev_high_no_chase"] = {"filled": False, "triggered": False, "gap_pct": pct(float(df["open"].iloc[next_pos]), close_entry)}
    return out


def score_signal(td_label: str, op_label: str, sector_strength_label: str, pullback: str, weekly: WeeklySignal, daily: DailySignal, proxy: ProxySignal, stop_pct: float) -> float:
    score = 0.0
    score += {"🟢 T-CORE": 60, "🔥 T-PRIME": 54, "🟠 R-PRIME": 46, "🟡 T-WATCH": 38, "🟠 R-WAIT": 25, "🟤 R-RISK": -30, "🔴 T-FAIL": -50}.get(td_label, 0)
    score += {"🟢 TD-CORE": 30, "🔥 TD-PRIME": 28, "🟠 TD-RECOVER": 22, "🟡 TD-WATCH": 12, "🔴 TD-RISK": -50}.get(op_label, 0)
    if sector_strength_label == "STRONG_SECTOR":
        score += 8
    elif sector_strength_label == "WEAK_SECTOR":
        score -= 5
    if pullback.startswith("🟢"):
        score += 8
    elif pullback.startswith("🟡 INDEX"):
        score += 6
    elif pullback.startswith("🟡 NORMAL"):
        score += 4
    elif pullback.startswith("⚪"):
        score += 1
    elif pullback.startswith("🟠"):
        score -= 10
    elif pullback.startswith("🔴"):
        score -= 25
    score += 4 if weekly.pass_weekly else -10
    score += 5 if daily.ready else (2 if daily.watch else 0)
    score += 8 if proxy.yes else 0
    if pd.notna(stop_pct):
        if stop_pct >= -6:
            score += 8
        elif stop_pct >= -8:
            score += 4
        elif stop_pct <= -10:
            score -= 3
    return float(score)


def op_label_from(td_label: str, pullback: str, sector_strength_label: str, daily: DailySignal, proxy: ProxySignal, stop_pct: float) -> Tuple[str, str]:
    """V1107.4.3: TD-RISK 선분류를 우선 적용한 운용 라벨."""
    if td_label in {"🔴 T-FAIL", "🟤 R-RISK"} or pullback.startswith("🔴 STOCK_BREAKDOWN"):
        return "🔴 TD-RISK", "TD-RISK 선분류: 주봉 실패/종목 훼손/구조 훼손 가능성"
    if pullback.startswith("🟠 SECTOR_PULLBACK") and not proxy.yes:
        return "🔴 TD-RISK", "섹터 눌림 + 프록시 미확인: 회복 확인 전 실전 제외"
    if pd.notna(stop_pct) and stop_pct <= -10.0 and not proxy.yes:
        return "🔴 TD-RISK", "손절거리 과다 + 60분프록시 미확인"
    if td_label == "🟢 T-CORE" and daily.ready and proxy.yes:
        return "🟢 TD-CORE", "일봉 READY + 60분프록시 확인. 기준일 종가/다음날 시가 소액 가능"
    if td_label == "🔥 T-PRIME" and proxy.yes and pullback in {"🟢 HEALTHY_PULLBACK", "🟡 INDEX_PULLBACK", "🟡 NORMAL_PULLBACK"}:
        return "🔥 TD-PRIME", "주도/정상 눌림 + 60분프록시. 전일고점 돌파확인 우선"
    if td_label in {"🟠 R-PRIME"} and proxy.yes and not pullback.startswith("🔴"):
        return "🟠 TD-RECOVER", "회복 후보. 소액/확인형, 회복선 또는 전일고점 돌파 후 추가"
    if td_label in {"🟡 T-WATCH", "🟠 R-WAIT"}:
        return "🟡 TD-WATCH", "주봉/눌림은 양호하지만 60분프록시 또는 일봉 타점 대기"
    return "🔴 TD-RISK", "운용 기준 미충족"


def analyze_one_signal(
    code: str,
    name: str,
    sector: str,
    sector_strength_label: str,
    df: pd.DataFrame,
    ref_date: pd.Timestamp,
    index_df: pd.DataFrame,
    sector_ret_map: Dict[Tuple[pd.Timestamp, str], float],
) -> Optional[Dict[str, Any]]:
    if df is None or df.empty:
        return None
    sig_pos = df.index.searchsorted(ref_date, side="right") - 1
    if sig_pos < 130 or sig_pos >= len(df) - 1:
        return None
    cut = df.iloc[: sig_pos + 1].copy()
    weekly = calc_weekly_signal(cut)
    daily = calc_daily_signal(cut)
    proxy = calc_60m_proxy_signal(cut)
    stock_ret5 = get_ret_at_or_before(df, ref_date, 5)
    index_ret5 = get_ret_at_or_before(index_df, ref_date, 5)
    sector_ret5 = sector_ret_map.get((ref_date, sector), np.nan)
    pullback = classify_pullback(stock_ret5, index_ret5, sector_ret5, cut)
    td_label, td_reason = label_topdown(weekly, daily, proxy, pullback)
    entries = eval_all_entries(df, cut, sig_pos, proxy)
    close_entry = entries.get("A_close", {}).get("entry", float(cut["close"].iloc[-1]))
    stop_pct = entries.get("A_close", {}).get("stop_pct", calc_stop_pct(cut, close_entry))
    op_label, op_reason = op_label_from(td_label, pullback, sector_strength_label, daily, proxy, stop_pct)
    score = score_signal(td_label, op_label, sector_strength_label, pullback, weekly, daily, proxy, stop_pct)

    row: Dict[str, Any] = {
        "ref_date": str(pd.Timestamp(df.index[sig_pos]).date()),
        "code": code,
        "name": name,
        "sector": sector,
        "sector_strength": sector_strength_label,
        "td_label": td_label,
        "td_reason": td_reason,
        "op_label": op_label,
        "op_reason": op_reason,
        "pullback": pullback,
        "score": score,
        "close": float(cut["close"].iloc[-1]),
        "stock_ret5": stock_ret5,
        "index_ret5": index_ret5,
        "sector_ret5": sector_ret5,
        "weekly_pass": weekly.pass_weekly,
        "weekly_above20": weekly.above_20w,
        "weekly_ma20_up": weekly.ma20w_up,
        "weekly_macd_up": weekly.macd_hist_up,
        "daily_ready": daily.ready,
        "daily_watch": daily.watch,
        "daily_near_ma20": daily.near_ma20,
        "daily_stoch_exit": daily.stoch_exit,
        "daily_macd_div": daily.macd_div,
        "proxy_yes": proxy.yes,
        "proxy_breakout": proxy.breakout,
        "proxy_volume_ok": proxy.volume_ok,
        "proxy_vol_ratio": proxy.vol_ratio,
        "prev_high_proxy": proxy.prev_high,
        "stop_pct": stop_pct,
    }
    for style, e in entries.items():
        prefix = style
        for k, v in e.items():
            row[f"{prefix}_{k}"] = v
    return row


def summarize_group(df: pd.DataFrame, group_col: str, title: str, entry_prefix: str = "A_close", order: Optional[List[str]] = None) -> List[str]:
    lines: List[str] = [f"[{title}]"]
    if df.empty or group_col not in df.columns:
        lines.append("- 데이터 없음")
        return lines
    data = df.copy()
    if f"{entry_prefix}_filled" in data.columns:
        data = data[data[f"{entry_prefix}_filled"] == True].copy()
    if data.empty:
        lines.append("- 체결 데이터 없음")
        return lines
    groups = []
    if order:
        for key in order:
            g = data[data[group_col] == key]
            if not g.empty:
                groups.append((key, g))
        others = [x for x in data[group_col].dropna().unique().tolist() if x not in order]
        for key in sorted(others):
            groups.append((key, data[data[group_col] == key]))
    else:
        for key, g in data.groupby(group_col):
            groups.append((key, g))
    for key, g in groups:
        n = len(g)
        r3 = g.get(f"{entry_prefix}_ret3d", pd.Series(dtype=float)).mean()
        r5 = g.get(f"{entry_prefix}_ret5d", pd.Series(dtype=float)).mean()
        r10 = g.get(f"{entry_prefix}_ret10d", pd.Series(dtype=float)).mean()
        med5 = g.get(f"{entry_prefix}_ret5d", pd.Series(dtype=float)).median()
        maxup = g.get(f"{entry_prefix}_max_up", pd.Series(dtype=float)).mean()
        hit3 = g.get(f"{entry_prefix}_hit3_first", pd.Series(dtype=bool)).mean() * 100
        hit5 = g.get(f"{entry_prefix}_hit5_first", pd.Series(dtype=bool)).mean() * 100
        stop = g.get(f"{entry_prefix}_stop_first", pd.Series(dtype=bool)).mean() * 100
        lines.append(
            f"- {key}: {n}개 | 3일 {fmt_pct(r3)} / 5일 {fmt_pct(r5)} / 10일 {fmt_pct(r10)} "
            f"| 5일중앙 {fmt_pct(med5)} | 10일최고 {fmt_pct(maxup)} | +3먼저 {fmt_num(hit3,1)}% | +5먼저 {fmt_num(hit5,1)}% | 손절먼저 {fmt_num(stop,1)}%"
        )
    return lines


def summarize_entry_styles(df: pd.DataFrame, title: str) -> List[str]:
    lines = [f"[{title}]"]
    if df.empty:
        lines.append("- 데이터 없음")
        return lines
    styles = [
        ("A_close", "A. 기준일 종가 진입"),
        ("B_next_open", "B. 다음날 시가 진입"),
        ("C_prev_high_break", "C. 다음날 전일고점 돌파확인"),
        ("D_prev_high_no_chase", "D. 전일고점 돌파확인 + 갭2%↑ 추격금지"),
    ]
    total = len(df)
    for prefix, label in styles:
        filled_col = f"{prefix}_filled"
        if filled_col not in df.columns:
            lines.append(f"- {label}: 데이터 없음")
            continue
        g = df[df[filled_col] == True].copy()
        n = len(g)
        if n == 0:
            lines.append(f"- {label}: 체결 0/{total}개")
            continue
        r5 = g.get(f"{prefix}_ret5d", pd.Series(dtype=float)).mean()
        r10 = g.get(f"{prefix}_ret10d", pd.Series(dtype=float)).mean()
        med5 = g.get(f"{prefix}_ret5d", pd.Series(dtype=float)).median()
        hit3 = g.get(f"{prefix}_hit3_first", pd.Series(dtype=bool)).mean() * 100
        hit5 = g.get(f"{prefix}_hit5_first", pd.Series(dtype=bool)).mean() * 100
        stop = g.get(f"{prefix}_stop_first", pd.Series(dtype=bool)).mean() * 100
        lines.append(
            f"- {label} | 체결 {n}/{total}개 | 5일 {fmt_pct(r5)} | 10일 {fmt_pct(r10)} | 5일중앙 {fmt_pct(med5)} "
            f"| +3먼저 {fmt_num(hit3,1)}% | +5먼저 {fmt_num(hit5,1)}% | 손절먼저 {fmt_num(stop,1)}%"
        )
    return lines


def select_operation_candidates(rows: pd.DataFrame, max_per_day: int = 10) -> pd.DataFrame:
    """날짜별 상위 제한 + 동일 종목 10거래일 중복 제거를 흉내내는 운용형 압축."""
    if rows.empty:
        return rows.copy()
    usable = rows[rows["op_label"] != "🔴 TD-RISK"].copy()
    if usable.empty:
        return usable
    label_limits = {"🔥 TD-PRIME": 3, "🟢 TD-CORE": 3, "🟠 TD-RECOVER": 3, "🟡 TD-WATCH": 4}
    selected: List[pd.DataFrame] = []
    last_selected_date_by_code: Dict[str, pd.Timestamp] = {}
    usable["ref_ts"] = pd.to_datetime(usable["ref_date"])
    for rd, day in usable.sort_values(["ref_ts", "score"], ascending=[True, False]).groupby("ref_ts", sort=True):
        day_selected: List[pd.Series] = []
        used_label_count: Dict[str, int] = {k: 0 for k in label_limits}
        day_sorted = day.sort_values("score", ascending=False)
        for _, r in day_sorted.iterrows():
            op = r["op_label"]
            if used_label_count.get(op, 0) >= label_limits.get(op, 0):
                continue
            code = str(r["code"])
            last_dt = last_selected_date_by_code.get(code)
            if last_dt is not None and (rd - last_dt).days < 10:
                continue
            day_selected.append(r)
            used_label_count[op] = used_label_count.get(op, 0) + 1
            last_selected_date_by_code[code] = rd
            if len(day_selected) >= max_per_day:
                break
        if day_selected:
            selected.append(pd.DataFrame(day_selected))
    if not selected:
        return usable.iloc[0:0].copy()
    return pd.concat(selected, ignore_index=True)


def export_html(df: pd.DataFrame, path: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    show_cols = [
        "ref_date", "code", "name", "sector", "sector_strength", "op_label", "td_label", "pullback", "score",
        "A_close_ret5d", "A_close_ret10d", "A_close_stop_first", "C_prev_high_break_filled", "C_prev_high_break_ret5d", "D_prev_high_no_chase_filled", "D_prev_high_no_chase_ret5d",
        "weekly_pass", "daily_ready", "proxy_yes", "stop_pct", "op_reason",
    ]
    cols = [c for c in show_cols if c in df.columns]
    body = df[cols].sort_values(["ref_date", "score"], ascending=[False, False]).to_html(index=False, escape=False)
    html_text = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:16px;line-height:1.45}}
table{{border-collapse:collapse;width:100%;font-size:13px}} th,td{{border:1px solid #ddd;padding:6px;vertical-align:top}} th{{background:#f5f5f5;position:sticky;top:0}}
</style></head><body><h1>{html.escape(title)}</h1>{body}</body></html>"""
    path.write_text(html_text, encoding="utf-8")


def send_telegram(text: str) -> None:
    token = os.getenv("TELEGRAM_BACKTEST_TOKEN") or os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_BACKTEST_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("⚠️ Telegram token/chat_id 없음: 콘솔 출력만 진행")
        return
    chunks: List[str] = []
    cur = ""
    for line in text.splitlines():
        if len(cur) + len(line) + 1 > 3600:
            chunks.append(cur)
            cur = line
        else:
            cur += ("\n" if cur else "") + line
    if cur:
        chunks.append(cur)
    for i, chunk in enumerate(chunks, 1):
        prefix = f"({i}/{len(chunks)})\n" if len(chunks) > 1 else ""
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={"chat_id": chat_id, "text": prefix + chunk, "disable_web_page_preview": True},
                timeout=15,
            )
            time.sleep(0.2)
        except Exception as e:
            print(f"⚠️ Telegram send fail: {e}")


def build_report(all_df: pd.DataFrame, op_df: pd.DataFrame, ref_dates: List[pd.Timestamp], universe_n: int, out_dir: Path) -> str:
    lines: List[str] = []
    lines.append(f"🧪 [{VERSION}]")
    lines.append("- 방식: 주봉→일봉→60분프록시 + 눌림 원인 분리 + V1107.4.3 운용형 출력/진입 검증")
    lines.append(f"- 기준일 수: {len(ref_dates)}개 | 유니버스: {universe_n}개 | 검증신호: {len(all_df)}개")
    lines.append(f"- 기준일: {', '.join(str(d.date()) for d in ref_dates)}")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")

    if all_df.empty:
        lines.append("검증 신호가 없습니다.")
        return "\n".join(lines)

    lines.extend(summarize_entry_styles(all_df, "전체 진입 방식별 성과"))
    lines.append("")
    lines.extend(summarize_group(all_df, "td_label", "원본 TOPDOWN 라벨별 성과", "A_close", TD_LABEL_ORDER))
    lines.append("")
    lines.extend(summarize_group(all_df, "pullback", "눌림 원인별 성과", "A_close"))
    lines.append("")
    lines.extend(summarize_group(all_df, "sector", "섹터별 성과", "A_close"))
    lines.append("")

    lines.append("[운용형 압축 후보 성과 — 날짜별 상위 제한/중복제거 후]")
    lines.append(f"- 원본 {len(all_df)}개 → 운용형 선택 {len(op_df)}개")
    lines.append("")
    lines.extend(summarize_group(op_df, "op_label", "TD 운용라벨별 성과", "A_close", OP_LABEL_ORDER))
    lines.append("")
    lines.extend(summarize_group(op_df, "sector_strength", "섹터 강약별 성과", "A_close", ["STRONG_SECTOR", "NEUTRAL_SECTOR", "WEAK_SECTOR"]))
    lines.append("")
    lines.extend(summarize_entry_styles(op_df, "진입 방식별 성과 비교 — 운용형 선택 후보"))
    lines.append("")

    for label in ["🔥 TD-PRIME", "🟢 TD-CORE", "🟠 TD-RECOVER", "🟡 TD-WATCH"]:
        sub = op_df[op_df["op_label"] == label]
        if not sub.empty:
            lines.extend(summarize_entry_styles(sub, f"진입 방식별 성과 — {label}"))
            lines.append("")

    lines.append("[V1107.4.3 조건별 진단]")
    conditions = [
        ("주가 20주선 위", "weekly_above20"),
        ("20주선 우상향", "weekly_ma20_up"),
        ("주봉 MACD 히스토그램 상승", "weekly_macd_up"),
        ("일봉 20일선 눌림", "daily_near_ma20"),
        ("스토캐스틱 30 이하 탈출", "daily_stoch_exit"),
        ("일봉 MACD 강세 다이버전스", "daily_macd_div"),
        ("60분프록시 거래량 돌파", "proxy_yes"),
    ]
    for label, col in conditions:
        if col not in all_df.columns:
            continue
        yes = all_df[all_df[col] == True]
        no = all_df[all_df[col] != True]
        yes_r5 = yes["A_close_ret5d"].mean() if not yes.empty else np.nan
        no_r5 = no["A_close_ret5d"].mean() if not no.empty else np.nan
        yes_stop = yes["A_close_stop_first"].mean() * 100 if not yes.empty else np.nan
        no_stop = no["A_close_stop_first"].mean() * 100 if not no.empty else np.nan
        lines.append(f"- {label}: {len(yes)}개 | 5일 {fmt_pct(yes_r5)} | 손절먼저 {fmt_num(yes_stop,1)}%")
        lines.append(f"  ↳ 미충족: {len(no)}개 | 5일 {fmt_pct(no_r5)} | 손절먼저 {fmt_num(no_stop,1)}%")
    lines.append("")

    lines.append("[운용 후보 예시 — 점수 상위]")
    if not op_df.empty:
        sample = op_df.sort_values("score", ascending=False).head(12)
        for _, r in sample.iterrows():
            lines.append(
                f"- {r['ref_date']} {r['name']}({r['code']}) | {r['op_label']} / {r['td_label']} | 점수 {fmt_num(r['score'],1)} "
                f"| {r['sector']}·{r['sector_strength']} | {r['pullback']} | "
                f"주봉:{'PASS' if r['weekly_pass'] else 'FAIL'} / 일봉:{'READY' if r['daily_ready'] else ('WATCH' if r['daily_watch'] else 'WAIT')} / 프록시:{'YES' if r['proxy_yes'] else 'NO'} | "
                f"A 5일 {fmt_pct(r.get('A_close_ret5d'))} / C체결:{'Y' if r.get('C_prev_high_break_filled') else 'N'} C5일 {fmt_pct(r.get('C_prev_high_break_ret5d'))} | 손절 {fmt_pct(r.get('stop_pct'))} | {r['op_reason']}"
            )
    else:
        lines.append("- 운용 후보 없음")
    lines.append("")

    lines.append("[V1107.4.3 적용 가이드]")
    lines.append("- 🔥 TD-PRIME: 전일고점 돌파확인 진입 성과를 최우선으로 확인합니다. 갭 +2% 이상이면 D버전에서 추격금지 효과를 봅니다.")
    lines.append("- 🟢 TD-CORE: 기준일 종가/다음날 시가 소액 가능 후보지만, 실제 출력에서는 즉시진입 0개일 때 관찰 TOP으로만 표시합니다.")
    lines.append("- 🟠 TD-RECOVER: 회복 후보입니다. C/D 진입에서 손절먼저가 줄어드는지 확인합니다.")
    lines.append("- 🔴 TD-RISK: STOCK_BREAKDOWN, 섹터 붕괴, 주봉 실패, 손절거리 과다 후보입니다. 실전 TOP·AI Pick에서 제외해야 합니다.")
    lines.append("- 이번 백테스트의 핵심은 A/B/C/D 중 어떤 진입 방식이 TD-PRIME과 TD-RECOVER에서 손절먼저를 가장 낮추는지 확인하는 것입니다.")
    lines.append("")
    lines.append(f"📁 상세 CSV/HTML 저장 위치: {out_dir}")
    return "\n".join(lines)


def run_backtest(args: argparse.Namespace) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    weeks = args.weeks or env_int("TOPDOWN_WEEKS", 52)
    universe_top = args.universe_top or env_int("TOPDOWN_UNIVERSE_TOP", 150)
    ref_dates_env = args.ref_dates or os.getenv("TOPDOWN_REF_DATES", "")
    out_dir = Path(args.out_dir or os.getenv("TOPDOWN_OUT_DIR", "topdown_v1107_4_3_logs"))
    max_per_day = args.max_per_day or env_int("TOPDOWN_MAX_PER_DAY", 10)

    today = now_kst()
    # 최근 기준일의 10거래일 후 성과를 가능하면 평가하기 위해 약간 여유를 둠.
    end_dt = today + timedelta(days=47)
    start_dt = today - timedelta(days=900)
    start_s = start_dt.strftime("%Y-%m-%d")
    end_s = end_dt.strftime("%Y-%m-%d")

    print(f"✅ {VERSION} 시작 | {today.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 데이터 범위: {start_s} ~ {end_s}")

    listing = load_krx_listing(universe_top)
    print(f"📌 유니버스 {len(listing)}개 로드")

    ref_dates = make_ref_dates(today, weeks, ref_dates_env)
    price_map: Dict[str, pd.DataFrame] = {}
    for i, row in listing.iterrows():
        code = str(row["code"]).zfill(6)
        price_map[code] = fetch_price(code, start_s, end_s)
        if (i + 1) % 25 == 0:
            print(f"  - 데이터 수집 {i + 1}/{len(listing)}")

    index_df = fetch_index("KS11", start_s, end_s)
    sector_ret_map = calc_sector_returns(price_map, listing, ref_dates, days=5)

    rows: List[Dict[str, Any]] = []
    meta = listing.set_index("code").to_dict("index")
    for rd in ref_dates:
        print(f"🔎 기준일 분석: {rd.date()}")
        for code, df in price_map.items():
            if df is None or df.empty:
                continue
            m = meta.get(code, {})
            sig = analyze_one_signal(
                code=code,
                name=str(m.get("name", code)),
                sector=str(m.get("sector", "미분류")),
                sector_strength_label=str(m.get("sector_strength", "NEUTRAL_SECTOR")),
                df=df,
                ref_date=rd,
                index_df=index_df,
                sector_ret_map=sector_ret_map,
            )
            if sig is not None:
                rows.append(sig)

    all_df = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    detail_csv = out_dir / "topdown_v1107_4_3_detail.csv"
    all_df.to_csv(detail_csv, index=False, encoding="utf-8-sig")

    op_df = select_operation_candidates(all_df, max_per_day=max_per_day)
    op_csv = out_dir / "topdown_v1107_4_3_operation_selected.csv"
    op_df.to_csv(op_csv, index=False, encoding="utf-8-sig")

    export_html(all_df, out_dir / "topdown_v1107_4_3_detail.html", "V1107.4.3 TOPDOWN 전체 상세")
    export_html(op_df, out_dir / "topdown_v1107_4_3_operation_selected.html", "V1107.4.3 운용형 선택 후보")

    report = build_report(all_df, op_df, ref_dates, len(listing), out_dir)
    report_path = out_dir / "topdown_v1107_4_3_report.txt"
    report_path.write_text(report, encoding="utf-8")
    print(report)
    return all_df, op_df, report


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--weeks", type=int, default=None)
    p.add_argument("--universe-top", type=int, default=None)
    p.add_argument("--ref-dates", type=str, default="")
    p.add_argument("--out-dir", type=str, default="")
    p.add_argument("--max-per-day", type=int, default=None)
    p.add_argument("--send-telegram", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        _, _, report = run_backtest(args)
        if args.send_telegram:
            send_telegram(report)
        print(f"✅ {VERSION} 완료")
        return 0
    except Exception as e:
        print(f"🚨 {VERSION} 실패: {e}")
        traceback.print_exc()
        if args.send_telegram:
            send_telegram(f"🚨 {VERSION} 실패\n{e}\n\n{traceback.format_exc()[-3000:]}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
