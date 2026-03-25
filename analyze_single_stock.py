# -*- coding: utf-8 -*-
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
import pandas as pd
import pytz

KST = pytz.timezone("Asia/Seoul")

MIN_PRICE = 5_000
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

MODE_CHOICES = {
    "all",
    "closing_bet",
    "envelope_bet",
    "dolbanji",
    "watermelon",
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


def load_price_history(code: str) -> pd.DataFrame:
    end = datetime.now(KST).strftime("%Y-%m-%d")
    start = (datetime.now(KST) - pd.Timedelta(days=450)).strftime("%Y-%m-%d")
    df = fdr.DataReader(code, start, end)

    if df is None or df.empty:
        raise RuntimeError(f"가격 데이터를 불러오지 못했습니다: {code}")

    df = df.rename(columns={c: c.capitalize() for c in df.columns})
    needed = ["Open", "High", "Low", "Close", "Volume"]
    for col in needed:
        if col not in df.columns:
            raise RuntimeError(f"필수 컬럼 누락: {col}")

    df = df.dropna(subset=needed).copy()
    if len(df) < 40:
        raise RuntimeError("최소 40봉 이상 데이터가 필요합니다.")
    return df


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_gain = up.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = down.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
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
    neg_sum = neg.rolling(period).sum().replace(0, pd.NA)
    ratio = pos_sum / neg_sum
    out = 100 - (100 / (1 + ratio))
    return out.fillna(50)


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
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

    direction = out["Close"].diff().fillna(0)
    obv_delta = pd.Series(0, index=out.index, dtype="float64")
    obv_delta[direction > 0] = out.loc[direction > 0, "Volume"]
    obv_delta[direction < 0] = -out.loc[direction < 0, "Volume"]
    out["OBV"] = obv_delta.cumsum()

    vol5 = out["Volume"].rolling(5).sum().replace(0, pd.NA)
    vol10 = out["Volume"].rolling(10).sum().replace(0, pd.NA)
    out["OBV_SLOPE_5"] = ((out["OBV"].diff(5) / vol5) * 100).round(2)
    out["OBV_SLOPE_10"] = ((out["OBV"].diff(10) / vol10) * 100).round(2)

    out["RSI14"] = rsi(out["Close"], 14).round(1)
    out["MFI14"] = mfi(out, 14).round(1)

    out["Green"] = (out["Close"] >= out["Open"]).astype(int)
    out["Green_Days_10"] = out["Green"].rolling(10).sum()

    out["MA5_SLOPE"] = (((out["MA5"] - out["MA5"].shift(3)) / out["MA5"].shift(3)) * 100).round(2)
    out["MA20_SLOPE"] = (((out["MA20"] - out["MA20"].shift(3)) / out["MA20"].shift(3)) * 100).round(2)
    out["MA60_SLOPE"] = (((out["MA60"] - out["MA60"].shift(5)) / out["MA60"].shift(5)) * 100).round(2)

    out["MA200_GAP_PCT"] = ((out["Close"] - out["MA200"]) / out["MA200"] * 100).round(1)
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


def build_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
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

    return {
        "date": str(pd.to_datetime(df.index[-1]).date()),
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
        "obv_slope_5": round(safe_float(row.get("OBV_SLOPE_5")), 2),
        "obv_slope_10": round(safe_float(row.get("OBV_SLOPE_10")), 2),
        "green_days_10": safe_int(row.get("Green_Days_10")),
        "ma5": round(safe_float(row.get("MA5")), 1),
        "ma20": round(safe_float(row.get("MA20")), 1),
        "ma40": round(safe_float(row.get("MA40")), 1),
        "ma60": round(safe_float(row.get("MA60")), 1),
        "ma112": round(safe_float(row.get("MA112")), 1),
        "ma200": round(safe_float(row.get("MA200")), 1),
        "ma5_slope": round(safe_float(row.get("MA5_SLOPE")), 2),
        "ma20_slope": round(safe_float(row.get("MA20_SLOPE")), 2),
        "ma60_slope": round(safe_float(row.get("MA60_SLOPE")), 2),
        "ma200_gap_pct": round(safe_float(row.get("MA200_GAP_PCT")), 1),
        "env20_pct": env["env20_pct"],
        "env40_pct": env["env40_pct"],
        "env20_near": env["env20_near"],
        "env40_near": env["env40_near"],
        "env20_lower": env["lower20"],
        "env40_lower": env["lower40"],
        "double_bottom": double_bottom,
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


def build_watermelon(price: Dict[str, Any], df: pd.DataFrame) -> PatternResult:
    rows: List[CheckRow] = []
    s = "수박"
    close = safe_float(df["Close"].iloc[-1])
    bb40_mid = safe_float(df["BB40_MID"].iloc[-1])
    bb40_up = safe_float(df["BB40_UP"].iloc[-1])

    c1 = price["bb40_width"] <= 18.0
    add_check(rows, s, "BB40 폭", f"{price['bb40_width']}", "<= 18", c1, "응축 구간" if c1 else "밴드 폭 넓음")

    c2 = price["green_days_10"] >= 6
    add_check(rows, s, "최근 10봉 양봉 수", f"{price['green_days_10']}", ">= 6", c2, "양봉 우위" if c2 else "양봉 비중 부족")

    c3 = price["obv_slope_10"] > 0
    add_check(rows, s, "OBV 기울기", f"{price['obv_slope_10']}%", "> 0%", c3, "매집 우위" if c3 else "매집 신호 약함")

    c4 = price["mfi14"] >= 50
    add_check(rows, s, "MFI", f"{price['mfi14']}", ">= 50", c4, "자금 유입 우위" if c4 else "자금 유입 부족")

    c5 = close >= bb40_mid and close <= bb40_up * 1.03
    add_check(rows, s, "BB40 중단~상단 위치", f"종가 {fmt_int(close)} / 중단 {fmt_float(bb40_mid)} / 상단 {fmt_float(bb40_up)}", "중단 이상, 상단 과열 전", c5, "수박형 위치" if c5 else "위치가 다름")

    c6 = price["amount_b"] >= MIN_AMOUNT_B
    add_check(rows, s, "거래대금", f"{price['amount_b']}억", f">= {MIN_AMOUNT_B}억", c6, "유동성 충분" if c6 else "유동성 부족")

    score = sum(1 for r in rows if r.ok)
    status = decide_status(score, len(rows))
    if status == "해당":
        comment = "수박형 응축+매집 패턴으로 꽤 닮아 있습니다."
    elif status == "유사":
        comment = "수박형 느낌은 있으나 매집 강도나 위치가 완전하진 않습니다."
    else:
        comment = "현재는 수박 패턴으로 보기엔 응축 또는 수급이 부족합니다."
    return PatternResult("watermelon", s, status, score, len(rows), "응축 + 매집 + 밴드 위치형", comment, rows)


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

    return " ".join(dict.fromkeys(comments))


def sparkline_svg(df: pd.DataFrame, width: int = 960, height: int = 360) -> str:
    sub = df.tail(120).copy()
    env20 = calc_envelope(df, 20, ENV20_PCT)
    env40 = calc_envelope(df, 40, ENV40_PCT)

    sub["ENV20_LOWER"] = env20["lower"].tail(120).values
    sub["ENV40_LOWER"] = env40["lower"].tail(120).values

    price_cols = ["Close", "MA20", "MA60", "MA200", "BB40_UP", "BB40_DN", "ENV20_LOWER", "ENV40_LOWER"]
    values = []
    for col in price_cols:
        if col in sub.columns:
            values.extend(pd.to_numeric(sub[col], errors="coerce").dropna().tolist())
    if not values:
        return ""

    min_v = min(values)
    max_v = max(values)
    rng = max(max_v - min_v, 1e-9)
    left, right, top, bottom = 22, width - 22, 18, height - 34

    def x_pos(i: int, count: int) -> float:
        if count <= 1:
            return left
        return left + (right - left) * i / (count - 1)

    def y_pos(v: float) -> float:
        return top + (bottom - top) * (1 - (v - min_v) / rng)

    def series_points(series: pd.Series) -> List[str]:
        pts = []
        arr = pd.to_numeric(series, errors="coerce").tolist()
        for i, v in enumerate(arr):
            if pd.isna(v):
                pts.append("")
            else:
                pts.append(f"{x_pos(i, len(arr)):.1f},{y_pos(float(v)):.1f}")
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

    bb_up = pd.to_numeric(sub["BB40_UP"], errors="coerce").tolist()
    bb_dn = pd.to_numeric(sub["BB40_DN"], errors="coerce").tolist()
    band_up, band_dn = [], []
    for i, v in enumerate(bb_up):
        if pd.notna(v):
            band_up.append(f"{x_pos(i, len(bb_up)):.1f},{y_pos(float(v)):.1f}")
    for i in range(len(bb_dn) - 1, -1, -1):
        v = bb_dn[i]
        if pd.notna(v):
            band_dn.append(f"{x_pos(i, len(bb_dn)):.1f},{y_pos(float(v)):.1f}")

    band_polygon = ""
    if len(band_up) >= 2 and len(band_dn) >= 2:
        band_polygon = f'<polygon points="{" ".join(band_up + band_dn)}" fill="rgba(99,102,241,0.10)" stroke="none"/>'

    close_points = series_points(sub["Close"])
    ma20_points = series_points(sub["MA20"])
    ma60_points = series_points(sub["MA60"])
    ma200_points = series_points(sub["MA200"])
    bb40_up_points = series_points(sub["BB40_UP"])
    bb40_dn_points = series_points(sub["BB40_DN"])
    env20_points = series_points(sub["ENV20_LOWER"])
    env40_points = series_points(sub["ENV40_LOWER"])

    last_close = safe_float(sub["Close"].iloc[-1])
    first_close = safe_float(sub["Close"].iloc[0])
    close_color = "#22c55e" if last_close >= first_close else "#ef4444"

    def legend_item(x: int, y: int, color: str, label: str, dash: str = "") -> str:
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        return (
            f'<line x1="{x}" y1="{y}" x2="{x+18}" y2="{y}" stroke="{color}" stroke-width="3"{dash_attr}/>'
            f'<text x="{x+24}" y="{y+4}" fill="#d9e6f7" font-size="12">{escape(label)}</text>'
        )

    legends = [
        legend_item(26, 24, close_color, "종가"),
        legend_item(120, 24, "#f59e0b", "MA20"),
        legend_item(210, 24, "#38bdf8", "MA60"),
        legend_item(300, 24, "#a78bfa", "MA200"),
        legend_item(400, 24, "#818cf8", "BB40 상단"),
        legend_item(510, 24, "#6366f1", "BB40 하단"),
        legend_item(620, 24, "#10b981", "Env20 하단", "6 4"),
        legend_item(760, 24, "#f97316", "Env40 하단", "6 4"),
    ]

    return f"""
    <svg class="chart-svg" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none">
      <rect x="0" y="0" width="{width}" height="{height}" fill="transparent"/>
      <line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="#24385d" stroke-width="1"/>
      <line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" stroke="#24385d" stroke-width="1"/>
      {band_polygon}
      {polyline(bb40_up_points, "#818cf8", 1)}
      {polyline(bb40_dn_points, "#6366f1", 1)}
      {polyline(env20_points, "#10b981", 2, "6 4", 0.95)}
      {polyline(env40_points, "#f97316", 2, "6 4", 0.95)}
      {polyline(ma200_points, "#a78bfa", 2)}
      {polyline(ma60_points, "#38bdf8", 2)}
      {polyline(ma20_points, "#f59e0b", 2)}
      {polyline(close_points, close_color, 3)}
      <text x="{left}" y="{height-10}" fill="#9db2d2" font-size="12">최근 120봉 · 종가 / MA / BB40 / Envelope 하단</text>
      <text x="{width-24}" y="24" fill="#f8fbff" font-size="12" text-anchor="end">종가 {fmt_int(last_close)}</text>
      <text x="6" y="{bottom:.1f}" fill="#8aa0bf" font-size="11">{fmt_int(min_v)}</text>
      <text x="6" y="{top+4:.1f}" fill="#8aa0bf" font-size="11">{fmt_int(max_v)}</text>
      {''.join(legends)}
    </svg>
    """


def render_html(result: Dict[str, Any]) -> str:
    price = result["price"]
    patterns: List[PatternResult] = result["patterns"]
    df = result["df"]

    nav = "".join(f'<button class="nav-chip" onclick="scrollToId(\'sec-{escape(p.key)}\')">{escape(p.name)}</button>' for p in patterns)

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
        ("MA5 기울기", f'{price["ma5_slope"]}%'),
        ("MA60 기울기", f'{price["ma60_slope"]}%'),
        ("BB40 폭", f'{price["bb40_width"]}'),
        ("MA200 이격", f'{price["ma200_gap_pct"]}%'),
        ("Env20 하단 괴리", f'{price["env20_pct"]}%'),
        ("Env40 하단 괴리", f'{price["env40_pct"]}%'),
    ]
    metric_cards = "".join(
        f'<div class="metric"><div class="metric-label">{escape(k)}</div><div class="metric-value">{escape(v)}</div></div>'
        for k, v in metric_items
    )

    pattern_blocks = []
    all_rows = []

    for p in patterns:
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
    .sub {{ color:var(--muted); font-size:13px; margin-top:4px; }}
    .pill-row, .nav-row {{ display:flex; gap:8px; overflow:auto; white-space:nowrap; padding:10px 0 2px; scrollbar-width:none; }}
    .pill-row::-webkit-scrollbar, .nav-row::-webkit-scrollbar {{ display:none; }}
    .pill, .nav-chip {{ border:1px solid var(--line); background:#12203a; color:var(--text); padding:8px 12px; border-radius:999px; font-size:13px; }}
    .nav-chip {{ cursor:pointer; }}
    .card {{ background:linear-gradient(180deg,rgba(17,31,57,.98),rgba(12,24,44,.98)); border:1px solid var(--line); border-radius:22px; padding:16px; margin-bottom:14px; }}
    .section-title {{ font-size:18px; font-weight:800; margin:0 0 12px; }}
    .metric-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }}
    .metric {{ background:#0b1730; border:1px solid var(--line); border-radius:18px; padding:12px; min-height:84px; }}
    .metric-label {{ color:var(--muted); font-size:12px; }}
    .metric-value {{ font-size:24px; font-weight:900; margin-top:6px; }}
    .chart-svg {{ width:100%; height:260px; display:block; }}
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
    table {{ width:100%; border-collapse:collapse; min-width:680px; background:#0b1730; }}
    th, td {{ padding:11px 10px; border-bottom:1px solid #1f3150; text-align:left; vertical-align:top; font-size:13px; }}
    th {{ color:#c7d5ea; background:#0d1a33; position:sticky; top:0; }}
    .mono {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }}
    .muted {{ color:var(--muted); }}
    @media (min-width:768px) {{ .metric-grid {{ grid-template-columns:repeat(4,minmax(0,1fr)); }} .chart-svg {{ height:360px; }} }}
  </style>
  <script>
    function scrollToId(id) {{
      const el = document.getElementById(id);
      if (el) el.scrollIntoView({{ behavior:'smooth', block:'start' }});
    }}
  </script>
</head>
<body>
  <div class="hero"><div class="hero-inner">
      <h1 class="title">{escape(result['name'])} <span style="color:var(--muted)">({escape(result['code'])})</span></h1>
      <div class="sub">생성시각 {escape(result['generated_at'])} · 기준봉 {escape(price['date'])}</div>
      <div class="pill-row">
        <div class="pill">분석모드 {escape(result['mode'])}</div>
        <div class="pill">현재가 {fmt_int(price['close'])}</div>
        <div class="pill">데이터 {price['bars']}봉</div>
        <div class="pill">거래대금 {price['amount_b']}억</div>
      </div>
      <div class="nav-row">{nav}</div>
  </div></div>
  <div class="app">
    <section class="card"><div class="section-title">핵심 요약</div><p>{escape(result['summary'])}</p><p><strong>종합 코멘트</strong><br>{escape(result['smart_comment'])}</p></section>
    <section class="card"><div class="section-title">가격 구조 차트</div>{sparkline_svg(df)}</section>
    <section class="card"><div class="section-title">기본 수치</div><div class="metric-grid">{metric_cards}</div></section>
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
    ap.add_argument("--output-json", default="reports/latest_result.json")
    ap.add_argument("--output-html", default="site/index.html")
    args = ap.parse_args()

    if args.mode not in MODE_CHOICES:
        raise SystemExit(f"지원하지 않는 mode 입니다: {args.mode}")

    code = normalize_code(args.code)
    name = detect_name(code, args.name)

    df = load_price_history(code)
    df = add_indicators(df)

    price = build_snapshot(df)
    patterns = build_patterns(price, df)
    selected = patterns_for_mode(patterns, args.mode)

    summary = build_summary(name, selected)
    smart_comment = build_smart_comment(price, selected, name)

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
    }

    out_json.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_html.write_text(render_html(result), encoding="utf-8")

    print(f"saved: {out_json}")
    print(f"saved: {out_html}")


if __name__ == "__main__":
    main()
