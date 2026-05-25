# -*- coding: utf-8 -*-
"""
V1107 TOPDOWN MARKET PULLBACK BACKTEST
──────────────────────────────────────────────────────────────────────────────
Stock-Hunter 빠른 검증용 독립 실행 파일

핵심 목적
1) 주봉 20주선/20주선 기울기/주봉 MACD 히스토그램으로 큰 흐름을 확인
2) 일봉 20일선 눌림/스토캐스틱 30 이하 탈출/MACD 오실레이터 강세 다이버전스로 눌림 타점 확인
3) 지수·섹터·주도주가 같이 빠진 것인지, 종목만 무너진 것인지 구분
4) 60분봉이 없어도 일봉 프록시로 직전 고점 돌파+거래량 증가를 1차 재현

실행 예시
python topdown_v1107_backtest.py --weeks 8 --universe-top 150
python topdown_v1107_backtest.py --ref-dates "2026-03-20,2026-03-27,2026-04-03" --universe-top 150
python topdown_v1107_backtest.py --universe-csv topdown_universe.csv --weeks 8 --send-telegram

선택 CSV 형식
code,name,sector,market
005930,삼성전자,반도체,KOSPI
000660,SK하이닉스,반도체,KOSPI
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import html
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

VERSION = "V1107_TOPDOWN_MARKET_PULLBACK_BACKTEST_20260525"
SEP = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
THIN = "────────────────────────────"

try:
    import FinanceDataReader as fdr  # type: ignore
except Exception:  # pragma: no cover
    fdr = None

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None


# ─────────────────────────────────────────────────────────────────────────────
# 기본 유틸
# ─────────────────────────────────────────────────────────────────────────────

def now_kst_str() -> str:
    try:
        import pytz  # type: ignore
        return datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.replace(",", "").strip()
            if not x:
                return default
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    return int(round(safe_float(x, float(default))))


def pct(a: float, b: float) -> float:
    if b == 0 or math.isnan(b):
        return 0.0
    return (a / b - 1.0) * 100.0


def fmt_pct(v: Any, digits: int = 2) -> str:
    f = safe_float(v, 0.0)
    return f"{f:+.{digits}f}%"


def fmt_price(v: Any) -> str:
    f = safe_float(v, 0.0)
    if abs(f) >= 1000:
        return f"{f:,.0f}"
    return f"{f:,.2f}"


def norm_code(code: Any) -> str:
    s = str(code or "").strip().replace(".0", "")
    s = re.sub(r"\D", "", s)
    return s.zfill(6) if s else ""


def log(msg: str) -> None:
    print(msg, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# 데이터 정규화
# ─────────────────────────────────────────────────────────────────────────────

COL_MAP = {
    "Open": ["Open", "시가", "open"],
    "High": ["High", "고가", "high"],
    "Low": ["Low", "저가", "low"],
    "Close": ["Close", "종가", "close"],
    "Volume": ["Volume", "거래량", "volume"],
    "Amount": ["Amount", "거래대금", "amount"],
}


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame(index=pd.to_datetime(df.index))
    for std, names in COL_MAP.items():
        found = None
        for n in names:
            if n in df.columns:
                found = n
                break
        if found is not None:
            out[std] = pd.to_numeric(df[found], errors="coerce")
    need = ["Open", "High", "Low", "Close", "Volume"]
    if not all(c in out.columns for c in need):
        return pd.DataFrame()
    if "Amount" not in out.columns:
        out["Amount"] = out["Close"] * out["Volume"]
    out = out.dropna(subset=["Open", "High", "Low", "Close"])
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out


def fetch_ohlcv(code: str, start: str, end: str) -> pd.DataFrame:
    if fdr is None:
        raise RuntimeError("FinanceDataReader가 설치되어 있지 않습니다. GitHub Actions에서 finance-datareader 설치가 필요합니다.")
    code = norm_code(code)
    raw = fdr.DataReader(code, start, end)
    return normalize_ohlcv(raw)


def fetch_index(symbol: str, start: str, end: str) -> pd.DataFrame:
    if fdr is None:
        return pd.DataFrame()
    try:
        raw = fdr.DataReader(symbol, start, end)
        return normalize_ohlcv(raw)
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# 유니버스
# ─────────────────────────────────────────────────────────────────────────────

BAD_NAME_PAT = re.compile(r"(ETF|ETN|스팩|SPAC|리츠|우선주|우\b|선물|인버스|레버리지|KODEX|TIGER|ACE|SOL|HANARO|KBSTAR)", re.I)

SECTOR_KEYWORDS = [
    ("반도체", ["하이닉스", "삼성전자", "반도체", "테크윙", "DB하이텍", "동진쎄미켐", "코리아써키트", "SFA반도체", "텔레칩스", "리노공업", "한미반도체"]),
    ("로봇/자동화", ["로봇", "에스피지", "고영", "로보", "휴림", "두산로보틱스", "라온", "티로보틱스"]),
    ("AI전력/전력설비", ["전기", "일렉", "산일", "효성중공업", "LS", "HD현대일렉트릭", "제룡", "대한전선", "가온전선"]),
    ("원전/우라늄", ["두산에너빌리티", "한전기술", "한전산업", "우리기술", "비에이치아이", "보성파워텍"]),
    ("바이오/헬스케어", ["바이오", "셀트리온", "한미약품", "녹십자", "펩트론", "에이비엘", "삼성바이오", "HLB", "제약", "헬스"]),
    ("금융/은행", ["금융", "지주", "은행", "보험", "증권", "생명", "KB", "신한", "하나", "우리", "메리츠", "한화생명"]),
    ("항공/운송", ["항공", "대한항공", "진에어", "제주항공", "글로비스", "CJ대한통운", "팬오션", "HMM"]),
    ("조선/해운", ["조선", "중공업", "해양", "HMM", "팬오션", "HD현대중공업", "삼성중공업", "한화오션"]),
    ("자동차/부품", ["현대차", "기아", "만도", "모비스", "HL만도", "성우하이텍", "화신"]),
    ("소프트웨어/클라우드", ["NAVER", "카카오", "더존", "소프트", "시큐리티", "드림시큐리티", "안랩"]),
]


def infer_sector(name: str) -> str:
    n = str(name or "")
    for sector, keys in SECTOR_KEYWORDS:
        if any(k.lower() in n.lower() for k in keys):
            return sector
    return "미분류"


def load_universe(args: argparse.Namespace) -> pd.DataFrame:
    if args.universe_csv:
        path = Path(args.universe_csv)
        if not path.exists():
            raise FileNotFoundError(f"유니버스 CSV를 찾을 수 없습니다: {path}")
        u = pd.read_csv(path, dtype={"code": str, "Symbol": str, "종목코드": str})
        if "code" not in u.columns:
            for c in ["Symbol", "Code", "종목코드", "ticker"]:
                if c in u.columns:
                    u["code"] = u[c]
                    break
        if "name" not in u.columns:
            for c in ["Name", "종목명"]:
                if c in u.columns:
                    u["name"] = u[c]
                    break
        if "sector" not in u.columns:
            u["sector"] = u["name"].map(infer_sector)
        if "market" not in u.columns:
            u["market"] = "KRX"
        u["code"] = u["code"].map(norm_code)
        u = u.dropna(subset=["code", "name"]).drop_duplicates("code")
        return u[["code", "name", "sector", "market"]].head(args.universe_top).reset_index(drop=True)

    if fdr is None:
        raise RuntimeError("FinanceDataReader가 필요합니다. 또는 --universe-csv를 지정해 주세요.")

    listing = fdr.StockListing("KRX")
    listing = listing.copy()
    rename_map = {}
    if "Symbol" in listing.columns:
        rename_map["Symbol"] = "code"
    if "Code" in listing.columns:
        rename_map["Code"] = "code"
    if "Name" in listing.columns:
        rename_map["Name"] = "name"
    if "Market" in listing.columns:
        rename_map["Market"] = "market"
    listing = listing.rename(columns=rename_map)
    if "code" not in listing.columns or "name" not in listing.columns:
        raise RuntimeError("FDR StockListing 컬럼 구조를 인식하지 못했습니다.")

    listing["code"] = listing["code"].map(norm_code)
    listing["name"] = listing["name"].astype(str)
    if "market" not in listing.columns:
        listing["market"] = "KRX"
    listing = listing[~listing["name"].str.contains(BAD_NAME_PAT, na=False)]
    if args.min_price > 0 and "Close" in listing.columns:
        listing = listing[pd.to_numeric(listing["Close"], errors="coerce").fillna(0) >= args.min_price]

    sort_col = None
    for c in ["Amount", "Marcap", "MarketCap", "Volume"]:
        if c in listing.columns:
            sort_col = c
            break
    if sort_col:
        listing[sort_col] = pd.to_numeric(listing[sort_col], errors="coerce").fillna(0)
        listing = listing.sort_values(sort_col, ascending=False)
    listing["sector"] = listing["name"].map(infer_sector)
    return listing[["code", "name", "sector", "market"]].drop_duplicates("code").head(args.universe_top).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# 지표 계산
# ─────────────────────────────────────────────────────────────────────────────

def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=max(2, span // 2)).mean()


def macd_hist(close: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
    macd = ema(close, 12) - ema(close, 26)
    sig = ema(macd, 9)
    hist = macd - sig
    return macd, sig, hist


def stochastic_kd(df: pd.DataFrame, n: int = 14, k_smooth: int = 3, d_smooth: int = 3) -> Tuple[pd.Series, pd.Series]:
    low_n = df["Low"].rolling(n, min_periods=n // 2).min()
    high_n = df["High"].rolling(n, min_periods=n // 2).max()
    raw_k = (df["Close"] - low_n) / (high_n - low_n).replace(0, pd.NA) * 100.0
    k = raw_k.rolling(k_smooth, min_periods=1).mean()
    d = k.rolling(d_smooth, min_periods=1).mean()
    return k.fillna(50.0), d.fillna(50.0)


def weekly_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    o = df["Open"].resample("W-FRI").first()
    h = df["High"].resample("W-FRI").max()
    l = df["Low"].resample("W-FRI").min()
    c = df["Close"].resample("W-FRI").last()
    v = df["Volume"].resample("W-FRI").sum()
    out = pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c, "Volume": v}).dropna(subset=["Close"])
    return out


def calc_weekly_signal(df: pd.DataFrame) -> Dict[str, Any]:
    w = weekly_frame(df)
    if len(w) < 35:
        return {"weekly_pass": False, "w_above20": False, "w_ma20_up": False, "w_macd_hist_up": False, "w_ma20": 0.0, "w_hist": 0.0}
    close = w["Close"]
    ma20 = close.rolling(20).mean()
    _, _, hist = macd_hist(close)
    cur_close = safe_float(close.iloc[-1])
    cur_ma = safe_float(ma20.iloc[-1])
    prev_ma = safe_float(ma20.iloc[-4] if len(ma20) >= 4 else ma20.iloc[-2])
    cur_hist = safe_float(hist.iloc[-1])
    prev_hist = safe_float(hist.iloc[-2])
    above20 = cur_close > cur_ma > 0
    ma20_up = cur_ma > prev_ma
    hist_up = cur_hist > 0 and cur_hist > prev_hist
    return {
        "weekly_pass": bool(above20 and ma20_up and hist_up),
        "w_above20": bool(above20),
        "w_ma20_up": bool(ma20_up),
        "w_macd_hist_up": bool(hist_up),
        "w_ma20": cur_ma,
        "w_hist": cur_hist,
        "w_close_vs_ma20": pct(cur_close, cur_ma) if cur_ma else 0.0,
    }


def has_daily_macd_bull_divergence(df: pd.DataFrame) -> Tuple[bool, float]:
    if len(df) < 50:
        return False, 0.0
    close = df["Close"]
    low = df["Low"]
    _, _, hist = macd_hist(close)
    # 최근 5~8일 저점과 그 이전 8~18일 저점을 비교하는 빠른 프록시
    recent_low = safe_float(low.iloc[-6:].min())
    prev_low = safe_float(low.iloc[-20:-6].min()) if len(low) >= 20 else safe_float(low.iloc[:-6].min())
    recent_hist_low = safe_float(hist.iloc[-6:].min())
    prev_hist_low = safe_float(hist.iloc[-20:-6].min()) if len(hist) >= 20 else safe_float(hist.iloc[:-6].min())
    if prev_low <= 0:
        return False, 0.0
    price_lower_or_flat = recent_low <= prev_low * 1.015
    hist_higher = recent_hist_low > prev_hist_low
    score = (recent_hist_low - prev_hist_low)
    return bool(price_lower_or_flat and hist_higher), score


def calc_daily_signal(df: pd.DataFrame) -> Dict[str, Any]:
    if len(df) < 60:
        return {"daily_ready": False}
    cur = df.iloc[-1]
    close = df["Close"]
    vol = df["Volume"]
    ma20 = close.rolling(20).mean()
    cur_close = safe_float(cur["Close"])
    cur_low = safe_float(cur["Low"])
    cur_open = safe_float(cur["Open"])
    cur_high = safe_float(cur["High"])
    cur_ma20 = safe_float(ma20.iloc[-1])
    ma20_dist = pct(cur_close, cur_ma20) if cur_ma20 else 0.0
    pullback20 = False
    if cur_ma20 > 0:
        pullback20 = (-3.5 <= ma20_dist <= 4.0) or (cur_low <= cur_ma20 * 1.025 and cur_close >= cur_ma20 * 0.965)

    k, d = stochastic_kd(df)
    k_now, d_now = safe_float(k.iloc[-1]), safe_float(d.iloc[-1])
    k_prev, d_prev = safe_float(k.iloc[-2]), safe_float(d.iloc[-2])
    recent_oversold = bool((k.iloc[-5:] <= 30).any() or (d.iloc[-5:] <= 30).any())
    stoch_cross_up = bool(k_prev <= d_prev and k_now > d_now and recent_oversold)
    stoch_recover = bool(k_now > d_now and recent_oversold and k_now <= 55)

    div_ok, div_score = has_daily_macd_bull_divergence(df)

    vol20 = safe_float(vol.rolling(20).mean().iloc[-1])
    vol5 = safe_float(vol.rolling(5).mean().iloc[-1])
    cur_vol = safe_float(cur["Volume"])
    vol_ratio20 = cur_vol / vol20 if vol20 else 0.0
    vol_dry = bool(vol5 <= vol20 * 0.85 if vol20 else False)
    bearish_high_vol = bool(cur_close < cur_open and vol_ratio20 >= 1.5 and (cur_close - cur_low) / max(cur_high - cur_low, 1) < 0.4)

    prev_high5 = safe_float(df["High"].iloc[-6:-1].max())
    close_loc = (cur_close - cur_low) / max(cur_high - cur_low, 1) * 100.0
    breakout_proxy = bool(cur_high >= prev_high5 * 1.003 and vol_ratio20 >= 1.15 and close_loc >= 55)
    breakout_close_proxy = bool(cur_close >= prev_high5 * 0.998 and vol_ratio20 >= 1.05 and close_loc >= 60)

    daily_ready = bool(pullback20 and (stoch_cross_up or stoch_recover or div_ok))
    return {
        "daily_ready": daily_ready,
        "d_pullback20": bool(pullback20),
        "d_stoch_recover": bool(stoch_cross_up or stoch_recover),
        "d_stoch_cross_up": bool(stoch_cross_up),
        "d_macd_div": bool(div_ok),
        "d_macd_div_score": div_score,
        "d_ma20": cur_ma20,
        "d_ma20_dist": ma20_dist,
        "d_k": k_now,
        "d_d": d_now,
        "d_vol_ratio20": vol_ratio20,
        "d_vol_dry": vol_dry,
        "d_bearish_high_vol": bearish_high_vol,
        "d_breakout_proxy": bool(breakout_proxy or breakout_close_proxy),
        "d_breakout_intraday_proxy": bool(breakout_proxy),
        "d_close_loc": close_loc,
        "d_prev_high5": prev_high5,
    }


def ret_n(df: pd.DataFrame, ref_dt: pd.Timestamp, n: int, col: str = "Close") -> float:
    d = df.loc[:ref_dt]
    if len(d) < n + 1:
        return 0.0
    return pct(safe_float(d[col].iloc[-1]), safe_float(d[col].iloc[-1 - n]))


# ─────────────────────────────────────────────────────────────────────────────
# 시장/섹터/주도주 눌림 원인 분류
# ─────────────────────────────────────────────────────────────────────────────

def classify_pullback_cause(row: Dict[str, Any]) -> str:
    idx_r5 = safe_float(row.get("index_ret5"))
    sec_r5 = safe_float(row.get("sector_ret5"))
    lead_r5 = safe_float(row.get("leader_ret5"))
    stock_r5 = safe_float(row.get("stock_ret5"))
    vol_ratio = safe_float(row.get("d_vol_ratio20"))
    bearish_high_vol = bool(row.get("d_bearish_high_vol"))
    vol_dry = bool(row.get("d_vol_dry"))

    if (idx_r5 > -1.5 and sec_r5 > -1.5) and (stock_r5 <= -5.0 or bearish_high_vol):
        return "🔴 STOCK_BREAKDOWN"
    if sec_r5 <= -2.5 and lead_r5 <= -2.0:
        return "🟠 SECTOR_PULLBACK"
    if idx_r5 <= -2.0 and sec_r5 > -3.5:
        return "🟡 INDEX_PULLBACK"
    if idx_r5 > -1.5 and sec_r5 > -1.5 and (vol_dry or vol_ratio <= 0.9):
        return "🟢 HEALTHY_PULLBACK"
    if idx_r5 > -1.0 and sec_r5 > -2.0:
        return "🟡 NORMAL_PULLBACK"
    return "⚪ MIXED_PULLBACK"


def classify_topdown(row: Dict[str, Any]) -> str:
    weekly_pass = bool(row.get("weekly_pass"))
    daily_ready = bool(row.get("daily_ready"))
    proxy = bool(row.get("d_breakout_proxy"))
    cause = str(row.get("pullback_cause", ""))
    sector_bad = cause.startswith("🟠 SECTOR")
    stock_bad = cause.startswith("🔴 STOCK")

    if not weekly_pass or sector_bad or stock_bad:
        return "🔴 T-FAIL"
    if weekly_pass and daily_ready and proxy and not sector_bad and not stock_bad:
        return "🟢 T-CORE"
    if weekly_pass and daily_ready and not proxy:
        return "🟡 T-WATCH"
    if weekly_pass and not daily_ready:
        return "🟠 T-RECOVER"
    return "⚪ T-OTHER"


# ─────────────────────────────────────────────────────────────────────────────
# 평가
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_future(df: pd.DataFrame, ref_dt: pd.Timestamp, hold_days: int = 10) -> Dict[str, Any]:
    past = df.loc[:ref_dt]
    future = df.loc[df.index > ref_dt].head(max(hold_days, 10))
    if past.empty or future.empty:
        return {"eval_ok": False}
    entry = safe_float(past["Close"].iloc[-1])
    if entry <= 0:
        return {"eval_ok": False}
    recent_low = safe_float(past["Low"].tail(6).min())
    stop_price = max(recent_low, entry * 0.90)  # 너무 먼 저점은 10%로 제한
    out: Dict[str, Any] = {
        "eval_ok": True,
        "entry": entry,
        "stop_price": stop_price,
        "stop_dist_pct": pct(stop_price, entry),
    }
    for n in [1, 3, 5, 10]:
        if len(future) >= n:
            out[f"ret{n}"] = pct(safe_float(future["Close"].iloc[n - 1]), entry)
        else:
            out[f"ret{n}"] = pct(safe_float(future["Close"].iloc[-1]), entry)
    h = future.head(hold_days)
    out["max_up"] = pct(safe_float(h["High"].max()), entry)
    out["max_down"] = pct(safe_float(h["Low"].min()), entry)

    plus3_first = False
    plus5_first = False
    stop_first = False
    event = "NONE"
    for _, r in h.iterrows():
        low = safe_float(r["Low"])
        high = safe_float(r["High"])
        if low <= stop_price:
            stop_first = True
            event = "STOP_FIRST"
            break
        if high >= entry * 1.05:
            plus5_first = True
            plus3_first = True
            event = "+5_FIRST"
            break
        if high >= entry * 1.03:
            plus3_first = True
            event = "+3_FIRST"
            break
    out["plus3_first"] = plus3_first
    out["plus5_first"] = plus5_first
    out["stop_first"] = stop_first
    out["first_event"] = event
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 기준일 생성
# ─────────────────────────────────────────────────────────────────────────────

def make_ref_dates(args: argparse.Namespace) -> List[pd.Timestamp]:
    if args.ref_dates:
        dates = []
        for s in str(args.ref_dates).split(","):
            s = s.strip()
            if s:
                dates.append(pd.Timestamp(s))
        return dates
    end = pd.Timestamp(args.end_date) if args.end_date else pd.Timestamp(datetime.now().date())
    # 최근 weeks개 금요일 기준. 휴장일이면 직전 거래일로 잘라서 사용한다.
    dates = []
    d = end
    while len(dates) < args.weeks:
        if d.weekday() == 4:
            dates.append(pd.Timestamp(d.date()))
        d -= timedelta(days=1)
    return sorted(dates)


# ─────────────────────────────────────────────────────────────────────────────
# 리포트
# ─────────────────────────────────────────────────────────────────────────────

def summarize_group(df: pd.DataFrame, by: str, title: str, min_n: int = 1) -> List[str]:
    lines = [f"\n[{title}]"]
    if df.empty or by not in df.columns:
        lines.append("- 결과 없음")
        return lines
    for key, g in df.groupby(by, dropna=False):
        if len(g) < min_n:
            continue
        lines.append(
            f"- {key}: {len(g)}개 | "
            f"3일 {fmt_pct(g['ret3'].mean())} / 5일 {fmt_pct(g['ret5'].mean())} / 10일 {fmt_pct(g['ret10'].mean())} | "
            f"10일최고 {fmt_pct(g['max_up'].mean())} | "
            f"+3먼저 {g['plus3_first'].mean()*100:.1f}% | "
            f"+5먼저 {g['plus5_first'].mean()*100:.1f}% | "
            f"손절먼저 {g['stop_first'].mean()*100:.1f}%"
        )
    return lines


def build_report(res: pd.DataFrame, ref_dates: List[pd.Timestamp], universe_n: int, out_dir: Path) -> str:
    lines: List[str] = []
    lines.append(f"🧪 [{VERSION}]")
    lines.append("- 방식: 주봉→일봉→돌파 프록시 + 지수/섹터/주도주 눌림 원인 분리")
    lines.append(f"- 기준일 수: {len(ref_dates)}개 | 유니버스: {universe_n}개 | 검증신호: {len(res)}개")
    lines.append("- 기준일: " + ", ".join(d.strftime("%Y-%m-%d") for d in ref_dates))
    lines.append(SEP)

    if res.empty:
        lines.append("결과 없음: 데이터 수집 실패 또는 조건 충족 후보 없음")
        return "\n".join(lines)

    lines.append("\n[전체]")
    lines.append(
        f"- 검증 {len(res)}개 | 1일 {fmt_pct(res['ret1'].mean())} | "
        f"3일 {fmt_pct(res['ret3'].mean())} | 5일 {fmt_pct(res['ret5'].mean())} | 10일 {fmt_pct(res['ret10'].mean())}"
    )
    lines.append(
        f"- +3먼저 {res['plus3_first'].mean()*100:.1f}% | +5먼저 {res['plus5_first'].mean()*100:.1f}% | "
        f"손절먼저 {res['stop_first'].mean()*100:.1f}% | 평균최대상승 {fmt_pct(res['max_up'].mean())} | 평균최대하락 {fmt_pct(res['max_down'].mean())}"
    )

    lines += summarize_group(res, "topdown_label", "V1107 TOPDOWN 라벨별 성과")
    lines += summarize_group(res, "pullback_cause", "눌림 원인별 성과")
    lines += summarize_group(res, "weekly_pass", "주봉 PASS 여부별 성과")
    lines += summarize_group(res, "daily_ready", "일봉 READY 여부별 성과")
    lines += summarize_group(res, "d_breakout_proxy", "60분봉 프록시 여부별 성과")
    lines += summarize_group(res, "sector", "섹터별 성과", min_n=2)

    lines.append("\n[V1107 조건별 진단]")
    conds = [
        ("w_above20", "주가 20주선 위"),
        ("w_ma20_up", "20주선 우상향"),
        ("w_macd_hist_up", "주봉 MACD 히스토그램 상승"),
        ("d_pullback20", "일봉 20일선 눌림"),
        ("d_stoch_recover", "스토캐스틱 30 이하 탈출"),
        ("d_macd_div", "일봉 MACD 강세 다이버전스"),
    ]
    for col, label in conds:
        if col in res.columns:
            g = res[res[col] == True]
            b = res[res[col] != True]
            if len(g) > 0:
                lines.append(f"- {label}: {len(g)}개 | 5일 {fmt_pct(g['ret5'].mean())} | 손절먼저 {g['stop_first'].mean()*100:.1f}%")
            if len(b) > 0:
                lines.append(f"  ↳ 미충족: {len(b)}개 | 5일 {fmt_pct(b['ret5'].mean())} | 손절먼저 {b['stop_first'].mean()*100:.1f}%")

    lines.append("\n[V1107 T-CORE 예시]")
    sample = res.sort_values(["topdown_label", "ret10", "max_up"], ascending=[True, False, False])
    sample = sample[sample["topdown_label"].isin(["🟢 T-CORE", "🟡 T-WATCH", "🟠 T-RECOVER"])].head(12)
    if sample.empty:
        lines.append("- T-CORE/T-WATCH/T-RECOVER 예시 없음")
    else:
        for _, r in sample.iterrows():
            lines.append(
                f"- {r['date']} {r['name']}({r['code']}) | {r['topdown_label']} | {r['pullback_cause']} | "
                f"주봉:{'PASS' if r['weekly_pass'] else 'FAIL'} / 일봉:{'READY' if r['daily_ready'] else 'WAIT'} / 프록시:{'YES' if r['d_breakout_proxy'] else 'NO'} | "
                f"5일 {fmt_pct(r['ret5'])} / 10일 {fmt_pct(r['ret10'])} | 손절 {fmt_pct(r['stop_dist_pct'])}"
            )

    lines.append("\n[V1107 적용 가이드]")
    lines.append("- 🟢 T-CORE: 주봉 PASS + 일봉 READY + 돌파 프록시 + 시장/섹터 붕괴 아님. 실전 후보 상향 검증 대상입니다.")
    lines.append("- 🟡 T-WATCH: 주봉/일봉은 맞지만 돌파 프록시가 없습니다. 눌림 재지지·거래량 재유입 확인용입니다.")
    lines.append("- 🟠 T-RECOVER: 주봉은 살아 있으나 일봉 타점이 덜 익었습니다. 20일선·스토캐스틱·MACD 회복 후 상향합니다.")
    lines.append("- 🔴 T-FAIL: 주봉 탈락, 섹터 동반 붕괴, 종목 단독 고거래량 급락이면 세력 눌림목보다 실패/관찰로 봅니다.")
    lines.append("- HEALTHY_PULLBACK과 INDEX_PULLBACK은 눌림 후보로 남기고, SECTOR_PULLBACK/STOCK_BREAKDOWN은 실전 TOP에서 분리하는지 확인합니다.")
    lines.append(f"\n📁 상세 CSV/HTML 저장 위치: {out_dir}")
    return "\n".join(lines)


def save_html(df: pd.DataFrame, report: str, out_path: Path) -> None:
    table_html = df.head(500).to_html(index=False, escape=True) if not df.empty else "<p>결과 없음</p>"
    doc = f"""<!doctype html><html lang="ko"><head><meta charset="utf-8">
<title>{html.escape(VERSION)}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e5e7eb;margin:0;padding:24px;}}
pre{{white-space:pre-wrap;background:#111827;border:1px solid #334155;border-radius:14px;padding:18px;line-height:1.55;}}
table{{border-collapse:collapse;width:100%;font-size:13px;background:#111827;}}
th,td{{border:1px solid #334155;padding:6px 8px;text-align:left;}}
th{{background:#1f2937;color:#facc15;position:sticky;top:0;}}
.good{{color:#86efac}} .bad{{color:#fca5a5}}
</style></head><body>
<h1>{html.escape(VERSION)}</h1>
<pre>{html.escape(report)}</pre>
<h2>상세 결과</h2>
{table_html}
</body></html>"""
    out_path.write_text(doc, encoding="utf-8")


def send_telegram(text: str) -> None:
    if requests is None:
        log("⚠️ requests가 없어 텔레그램 전송을 생략합니다.")
        return
    token = os.getenv("TELEGRAM_BACKTEST_TOKEN") or os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_BACKTEST_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log("⚠️ TELEGRAM token/chat_id가 없어 텔레그램 전송을 생략합니다.")
        return
    chunks = []
    cur = ""
    for line in text.splitlines():
        if len(cur) + len(line) + 1 > 3500:
            chunks.append(cur)
            cur = line
        else:
            cur = cur + "\n" + line if cur else line
    if cur:
        chunks.append(cur)
    for i, ch in enumerate(chunks, 1):
        prefix = f"({i}/{len(chunks)})\n" if len(chunks) > 1 else ""
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            requests.post(url, data={"chat_id": chat_id, "text": prefix + ch}, timeout=15)
            time.sleep(0.4)
        except Exception as e:
            log(f"⚠️ 텔레그램 전송 실패: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 메인 백테스트
# ─────────────────────────────────────────────────────────────────────────────

def build_context_for_date(
    ref_dt: pd.Timestamp,
    universe: pd.DataFrame,
    data_map: Dict[str, pd.DataFrame],
    index_map: Dict[str, pd.DataFrame],
    leader_count: int = 5,
) -> pd.DataFrame:
    rows = []
    for _, u in universe.iterrows():
        code = u["code"]
        df = data_map.get(code, pd.DataFrame())
        d = df.loc[:ref_dt] if not df.empty else pd.DataFrame()
        if len(d) < 30:
            continue
        market = str(u.get("market", ""))
        idx_key = "KQ11" if "KOSDAQ" in market.upper() else "KS11"
        idx_df = index_map.get(idx_key, pd.DataFrame())
        idx_ret5 = ret_n(idx_df, ref_dt, 5) if idx_df is not None and not idx_df.empty else 0.0
        rows.append({
            "code": code,
            "name": u["name"],
            "sector": u.get("sector", "미분류"),
            "market": market,
            "stock_ret5": ret_n(d, ref_dt, 5),
            "stock_ret10": ret_n(d, ref_dt, 10),
            "index_ret5": idx_ret5,
            "amount20": safe_float((d["Amount"].tail(20).mean() / 100000000.0) if "Amount" in d.columns else 0.0),
        })
    ctx = pd.DataFrame(rows)
    if ctx.empty:
        return ctx
    sec = ctx.groupby("sector")["stock_ret5"].median().to_dict()
    ctx["sector_ret5"] = ctx["sector"].map(sec).fillna(ctx["stock_ret5"].median())
    ctx["sector_breadth5"] = ctx.groupby("sector")["stock_ret5"].transform(lambda s: float((s > 0).mean()))
    ctx["leader_ret5"] = 0.0
    for sector, g in ctx.groupby("sector"):
        leaders = g.sort_values("amount20", ascending=False).head(leader_count)
        leader_ret = safe_float(leaders["stock_ret5"].median()) if not leaders.empty else safe_float(g["stock_ret5"].median())
        ctx.loc[ctx["sector"] == sector, "leader_ret5"] = leader_ret
    return ctx


def analyze_one_signal(code: str, name: str, sector: str, market: str, df: pd.DataFrame, ref_dt: pd.Timestamp, ctx_row: Dict[str, Any], hold_days: int) -> Optional[Dict[str, Any]]:
    if df.empty:
        return None
    cut = df.loc[:ref_dt].copy()
    if len(cut) < 160:
        return None
    # 실제 휴장일이면 ref_dt 이전 마지막 거래일로 보정
    signal_dt = cut.index[-1]
    weekly = calc_weekly_signal(cut)
    daily = calc_daily_signal(cut)
    base: Dict[str, Any] = {
        "date": signal_dt.strftime("%Y-%m-%d"),
        "code": code,
        "name": name,
        "sector": sector,
        "market": market,
        "close": safe_float(cut["Close"].iloc[-1]),
    }
    base.update(weekly)
    base.update(daily)
    base.update({k: ctx_row.get(k) for k in ["stock_ret5", "stock_ret10", "index_ret5", "sector_ret5", "sector_breadth5", "leader_ret5", "amount20"]})
    base["pullback_cause"] = classify_pullback_cause(base)
    base["topdown_label"] = classify_topdown(base)
    ev = evaluate_future(df, signal_dt, hold_days=hold_days)
    if not ev.get("eval_ok"):
        return None
    base.update(ev)
    return base


def run_backtest(args: argparse.Namespace) -> Tuple[pd.DataFrame, str, Path]:
    if fdr is None:
        raise RuntimeError("FinanceDataReader가 설치되어 있지 않습니다. requirements.txt에 finance-datareader를 추가해 주세요.")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ref_dates = make_ref_dates(args)
    if not ref_dates:
        raise RuntimeError("기준일이 없습니다.")

    data_start = (min(ref_dates) - timedelta(days=args.lookback_days)).strftime("%Y-%m-%d")
    data_end = (max(ref_dates) + timedelta(days=args.hold_days * 3 + 20)).strftime("%Y-%m-%d")

    log(f"✅ {VERSION} 시작 | {now_kst_str()}")
    log(f"📅 데이터 범위: {data_start} ~ {data_end}")

    universe = load_universe(args)
    log(f"📌 유니버스 {len(universe)}개 로드")

    index_map = {
        "KS11": fetch_index("KS11", data_start, data_end),
        "KQ11": fetch_index("KQ11", data_start, data_end),
    }

    data_map: Dict[str, pd.DataFrame] = {}
    errors: List[str] = []

    def _fetch(row: pd.Series) -> Tuple[str, pd.DataFrame, str]:
        code = row["code"]
        try:
            return code, fetch_ohlcv(code, data_start, data_end), ""
        except Exception as e:
            return code, pd.DataFrame(), str(e)[:120]

    with cf.ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as ex:
        futs = [ex.submit(_fetch, row) for _, row in universe.iterrows()]
        for i, fut in enumerate(cf.as_completed(futs), 1):
            code, df, err = fut.result()
            data_map[code] = df
            if err:
                errors.append(f"{code}:{err}")
            if i % 25 == 0 or i == len(futs):
                log(f"  - 데이터 수집 {i}/{len(futs)}")

    rows: List[Dict[str, Any]] = []
    for ref_dt in ref_dates:
        log(f"🔎 기준일 분석: {ref_dt.strftime('%Y-%m-%d')}")
        ctx = build_context_for_date(ref_dt, universe, data_map, index_map, leader_count=args.leader_count)
        if ctx.empty:
            continue
        ctx_map = {r["code"]: r for _, r in ctx.iterrows()}
        for _, u in universe.iterrows():
            code = u["code"]
            if code not in ctx_map:
                continue
            sig = analyze_one_signal(
                code=code,
                name=str(u["name"]),
                sector=str(u.get("sector", "미분류")),
                market=str(u.get("market", "")),
                df=data_map.get(code, pd.DataFrame()),
                ref_dt=ref_dt,
                ctx_row=dict(ctx_map[code]),
                hold_days=args.hold_days,
            )
            if sig is not None:
                # 너무 완전한 쓰레기 후보를 줄이기 위해 최소 가격/거래대금 필터만 적용
                if safe_float(sig.get("close")) >= args.min_price and safe_float(sig.get("amount20")) >= args.min_amount20_b:
                    rows.append(sig)

    res = pd.DataFrame(rows)
    if not res.empty:
        bool_cols = [c for c in res.columns if c.startswith("w_") or c.startswith("d_") or c in ["weekly_pass", "daily_ready"]]
        for c in bool_cols:
            if res[c].dtype == object:
                # bool과 float가 섞인 컬럼은 건드리지 않는다.
                pass
        sort_cols = ["date", "topdown_label", "sector", "name"]
        res = res.sort_values([c for c in sort_cols if c in res.columns]).reset_index(drop=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"topdown_v1107_detail_{stamp}.csv"
    html_path = out_dir / f"topdown_v1107_report_{stamp}.html"
    txt_path = out_dir / f"topdown_v1107_report_{stamp}.txt"
    latest_txt = out_dir / "topdown_v1107_latest.txt"
    latest_csv = out_dir / "topdown_v1107_latest.csv"
    latest_html = out_dir / "topdown_v1107_latest.html"

    if not res.empty:
        res.to_csv(csv_path, index=False, encoding="utf-8-sig")
        res.to_csv(latest_csv, index=False, encoding="utf-8-sig")

    report = build_report(res, ref_dates, len(universe), out_dir)
    if errors:
        report += f"\n\n[데이터 수집 경고]\n- 실패 {len(errors)}개: " + " | ".join(errors[:10])
    txt_path.write_text(report, encoding="utf-8")
    latest_txt.write_text(report, encoding="utf-8")
    save_html(res, report, html_path)
    save_html(res, report, latest_html)

    log(report)
    log(f"\n📁 저장 완료: {csv_path}\n📁 {txt_path}\n📁 {html_path}")
    if args.send_telegram or env_bool("SEND_TELEGRAM", False):
        send_telegram(report)
    return res, report, out_dir


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="V1107 TOPDOWN MARKET PULLBACK BACKTEST")
    p.add_argument("--weeks", type=int, default=int(os.getenv("TOPDOWN_WEEKS", "8")))
    p.add_argument("--ref-dates", default=os.getenv("TOPDOWN_REF_DATES", ""), help="쉼표 구분 기준일 목록")
    p.add_argument("--end-date", default=os.getenv("TOPDOWN_END_DATE", ""))
    p.add_argument("--universe-top", type=int, default=int(os.getenv("TOPDOWN_UNIVERSE_TOP", "150")))
    p.add_argument("--universe-csv", default=os.getenv("TOPDOWN_UNIVERSE_CSV", ""))
    p.add_argument("--out-dir", default=os.getenv("TOPDOWN_OUT_DIR", "./topdown_v1107_logs"))
    p.add_argument("--lookback-days", type=int, default=int(os.getenv("TOPDOWN_LOOKBACK_DAYS", "520")))
    p.add_argument("--hold-days", type=int, default=int(os.getenv("TOPDOWN_HOLD_DAYS", "10")))
    p.add_argument("--leader-count", type=int, default=int(os.getenv("TOPDOWN_LEADER_COUNT", "5")))
    p.add_argument("--min-price", type=float, default=float(os.getenv("TOPDOWN_MIN_PRICE", "3000")))
    p.add_argument("--min-amount20-b", type=float, default=float(os.getenv("TOPDOWN_MIN_AMOUNT20_B", "20")), help="20일 평균 거래대금 최소값, 억원")
    p.add_argument("--max-workers", type=int, default=int(os.getenv("TOPDOWN_MAX_WORKERS", "10")))
    p.add_argument("--send-telegram", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        run_backtest(args)
        return 0
    except Exception as e:
        log(f"🚨 {VERSION} 실패: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
