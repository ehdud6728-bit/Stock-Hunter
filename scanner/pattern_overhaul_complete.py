# -*- coding: utf-8 -*-
"""
pattern_overhaul_complete.py
========================================================
주식 스캐너용 패턴 재정의 모듈 (완성형 보조 파일)

목표
----
1) 쌍바닥 / 유사쌍바닥 / 재안착형을 분리
2) 파란점선(저항대) 개념을 공통 저항선/돌파 트리거로 통합
3) 상승삼각형/박스상단/동적저항 돌파를 주패턴으로 승격
4) 패턴 과대중복 점수화를 막고 "주패턴 1개 + 보조패턴 2~3개" 체계로 재정렬
5) 상태를 "매수 대기 / 돌파 확인 / 실행 가능" 으로 분리
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.replace(",", "").replace("%", "").strip()
            if x == "":
                return default
        return float(x)
    except Exception:
        return default

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(round(safe_float(x, default)))
    except Exception:
        return default

def pct_diff(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a / b - 1.0) * 100.0

def ensure_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c not in out.columns:
            out[c] = 0.0
    if "MA5" not in out.columns:
        out["MA5"] = out["Close"].rolling(5).mean()
    if "MA20" not in out.columns:
        out["MA20"] = out["Close"].rolling(20).mean()
    if "MA60" not in out.columns:
        out["MA60"] = out["Close"].rolling(60).mean()
    if "MA112" not in out.columns:
        out["MA112"] = out["Close"].rolling(112).mean()
    if "OBV" not in out.columns:
        close_diff = out["Close"].diff().fillna(0)
        obv_step = np.where(close_diff > 0, out["Volume"], np.where(close_diff < 0, -out["Volume"], 0))
        out["OBV"] = pd.Series(obv_step, index=out.index).cumsum()
    if "RSI" not in out.columns:
        delta = out["Close"].diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        roll_up = up.rolling(14).mean()
        roll_down = down.rolling(14).mean()
        rs = roll_up / roll_down.replace(0, np.nan)
        out["RSI"] = 100 - (100 / (1 + rs))
        out["RSI"] = out["RSI"].fillna(50)
    if "BB_MID_40" not in out.columns:
        out["BB_MID_40"] = out["Close"].rolling(40).mean()
    if "BB_STD_40" not in out.columns:
        out["BB_STD_40"] = out["Close"].rolling(40).std(ddof=0)
    if "BB_UPPER_40" not in out.columns:
        out["BB_UPPER_40"] = out["BB_MID_40"] + 2 * out["BB_STD_40"]
    if "BB_LOWER_40" not in out.columns:
        out["BB_LOWER_40"] = out["BB_MID_40"] - 2 * out["BB_STD_40"]
    return out

def upper_wick_ratio(row: pd.Series) -> float:
    try:
        o = safe_float(row.get("Open", 0))
        h = safe_float(row.get("High", 0))
        l = safe_float(row.get("Low", 0))
        c = safe_float(row.get("Close", 0))
        rng = max(h - l, 1e-9)
        upper = max(h - max(o, c), 0)
        return upper / rng * 100.0
    except Exception:
        return 999.0

def volume_ratio(df: pd.DataFrame, i: int, window: int = 20) -> float:
    try:
        if i <= 0:
            return 0.0
        vol = safe_float(df.iloc[i]["Volume"], 0)
        ma = safe_float(df["Volume"].iloc[max(0, i - window + 1): i + 1].mean(), 0)
        return vol / ma if ma > 0 else 0.0
    except Exception:
        return 0.0

def obv_slope(df: pd.DataFrame, i: int, lookback: int = 5) -> float:
    try:
        if i - lookback < 0:
            return 0.0
        x = np.arange(lookback + 1)
        y = df["OBV"].iloc[i - lookback:i + 1].values.astype(float)
        slope = np.polyfit(x, y, 1)[0]
        return float(slope)
    except Exception:
        return 0.0

def find_pivot_lows(df: pd.DataFrame, left: int = 3, right: int = 3) -> List[int]:
    lows: List[int] = []
    if df is None or len(df) < left + right + 1:
        return lows
    low_arr = df["Low"].values
    for i in range(left, len(df) - right):
        v = low_arr[i]
        if np.nanmin(low_arr[i-left:i+right+1]) == v:
            lows.append(i)
    return lows

def find_pivot_highs(df: pd.DataFrame, left: int = 3, right: int = 3) -> List[int]:
    highs: List[int] = []
    if df is None or len(df) < left + right + 1:
        return highs
    high_arr = df["High"].values
    for i in range(left, len(df) - right):
        v = high_arr[i]
        if np.nanmax(high_arr[i-left:i+right+1]) == v:
            highs.append(i)
    return highs

@dataclass
class PatternSignal:
    ok: bool = False
    pattern_code: str = ""
    pattern_name: str = ""
    state_code: str = ""
    state_name: str = ""
    primary_score: int = 0
    confirm_score: int = 0
    risk_penalty: int = 0
    final_score: int = 0
    blue_line_type: str = ""
    blue_line_price: float = 0.0
    trigger_price: float = 0.0
    support_tags: List[str] = None
    risk_tags: List[str] = None
    notes: List[str] = None
    meta: Dict[str, Any] = None
    def __post_init__(self):
        if self.support_tags is None:
            self.support_tags = []
        if self.risk_tags is None:
            self.risk_tags = []
        if self.notes is None:
            self.notes = []
        if self.meta is None:
            self.meta = {}
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

def compute_support_features(df: pd.DataFrame, i: Optional[int] = None) -> Dict[str, Any]:
    if df is None or df.empty:
        return {}
    i = len(df) - 1 if i is None else i
    row = df.iloc[i]
    close = safe_float(row["Close"], 0)
    ma5 = safe_float(row.get("MA5", 0), 0)
    ma20 = safe_float(row.get("MA20", 0), 0)
    ma60 = safe_float(row.get("MA60", 0), 0)
    ma112 = safe_float(row.get("MA112", 0), 0)
    bb_u40 = safe_float(row.get("BB_UPPER_40", 0), 0)
    bb_l40 = safe_float(row.get("BB_LOWER_40", 0), 0)
    rsi = safe_float(row.get("RSI", 50), 50)
    vol_r = volume_ratio(df, i, 20)
    wick = upper_wick_ratio(row)
    obv_up = obv_slope(df, i, 5) > 0
    struct_conv = 999.0
    if close > 0:
        vals = [x for x in [ma20, ma60, ma112] if x > 0]
        if len(vals) >= 2:
            struct_conv = (max(vals) - min(vals)) / close * 100.0
    bb_width = 999.0
    if close > 0 and bb_u40 > 0 and bb_l40 > 0:
        bb_width = (bb_u40 - bb_l40) / close * 100.0
    return {
        "close": close,
        "vol_ratio": vol_r,
        "wick": wick,
        "rsi": rsi,
        "obv_up": obv_up,
        "ma5_reclaim": bool(ma5 > 0 and close >= ma5),
        "ma20_support": bool(ma20 > 0 and close >= ma20 * 0.985),
        "ma112_support": bool(ma112 > 0 and close >= ma112 * 0.985),
        "bb40_tight": bool(bb_width <= 22.0),
        "bb40_width": round(bb_width, 2) if bb_width < 999 else 999,
        "struct_conv": round(struct_conv, 2) if struct_conv < 999 else 999,
        "struct_conv_ok": bool(struct_conv <= 8.0),
        "vol_ok": bool(vol_r >= 1.15),
        "vol_strong": bool(vol_r >= 1.40),
        "wick_ok": bool(wick <= 38.0),
        "rsi_hot": bool(rsi >= 72.0),
        "disparity20": round(pct_diff(close, ma20), 2) if ma20 > 0 else 0.0,
    }

def support_tags_from_features(f: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    supports, risks = [], []
    if f.get("bb40_tight"): supports.append("BB40압축")
    if f.get("obv_up"): supports.append("OBV매집")
    if f.get("ma5_reclaim"): supports.append("5일재안착")
    if f.get("ma112_support"): supports.append("장기이평지지")
    if f.get("struct_conv_ok"): supports.append("구조수렴")
    if f.get("vol_ok"): supports.append("거래량보강")
    if not f.get("wick_ok", True): risks.append("윗꼬리과다")
    if f.get("rsi_hot"): risks.append("RSI과열")
    if f.get("disparity20", 0) >= 12.0: risks.append("이격과열")
    if not f.get("vol_ok", False): risks.append("거래량약함")
    return supports[:3], risks[:3]

def classify_breakout_state(close: float, high: float, low: float, line_price: float, vol_ratio_now: float, vol_ratio_prev: float = 0.0, close_prev: float = 0.0) -> str:
    if line_price <= 0 or close <= 0:
        return ""
    if close < line_price * 0.995:
        return "매수 대기"
    crossed = (high >= line_price * 1.002) or (close >= line_price * 0.998)
    if crossed and (close < line_price * 1.005):
        return "돌파 확인"
    if close >= line_price * 1.005 and vol_ratio_now >= 1.15:
        if low >= line_price * 0.985:
            return "실행 가능"
        return "돌파 확인"
    if close_prev > 0 and close_prev >= line_price * 1.005 and close >= line_price * 1.000:
        if vol_ratio_prev >= 1.15 or vol_ratio_now >= 1.0:
            return "실행 가능"
    return "돌파 확인"

def detect_box_breakout(df: pd.DataFrame, i: Optional[int] = None, lookback: int = 20) -> PatternSignal:
    if df is None or len(df) < lookback + 5:
        return PatternSignal()
    i = len(df) - 1 if i is None else i
    row, prev = df.iloc[i], df.iloc[max(0, i - 1)]
    box = df.iloc[max(0, i - lookback):i]
    line = safe_float(box["High"].max(), 0)
    close, high, low = safe_float(row["Close"], 0), safe_float(row["High"], 0), safe_float(row["Low"], 0)
    if line <= 0:
        return PatternSignal()
    f = compute_support_features(df, i)
    state = classify_breakout_state(close, high, low, line, f.get("vol_ratio", 0), volume_ratio(df, i - 1, 20) if i >= 1 else 0.0, safe_float(prev["Close"], 0))
    box_low = safe_float(box["Low"].min(), 0)
    box_range = pct_diff(line, box_low) if box_low > 0 else 999.0
    if box_range > 18.0:
        return PatternSignal()
    supports, risks = support_tags_from_features(f)
    primary, confirm, penalty = 46, 0, 0
    if state == "돌파 확인": confirm += 10
    elif state == "실행 가능": confirm += 16
    if f.get("vol_ok"): confirm += 4
    if f.get("obv_up"): confirm += 3
    if f.get("bb40_tight"): confirm += 3
    if "윗꼬리과다" in risks: penalty += 5
    if "RSI과열" in risks: penalty += 6
    return PatternSignal(True, "BOX_BREAKOUT", "박스상단 돌파형", state.replace(" ", "_"), state, primary, min(confirm, 20), penalty, primary + min(confirm, 20) - penalty, "수평저항", round(line, 2), round(line, 2), supports, risks, [f"박스폭 {box_range:.1f}%"], {"lookback": lookback, "box_range_pct": round(box_range, 2)})

def detect_double_bottom_breakout(df: pd.DataFrame, i: Optional[int] = None) -> PatternSignal:
    if df is None or len(df) < 60:
        return PatternSignal()
    i = len(df) - 1 if i is None else i
    work = df.iloc[:i + 1]
    lows = [x for x in find_pivot_lows(work, 3, 3) if x >= len(work) - 55]
    highs = find_pivot_highs(work, 3, 3)
    if len(lows) < 2:
        return PatternSignal()
    best = None
    for a in range(len(lows) - 1):
        for b in range(a + 1, len(lows)):
            l1, l2 = lows[a], lows[b]
            gap = l2 - l1
            if gap < 8 or gap > 35:
                continue
            p1, p2 = safe_float(work.iloc[l1]["Low"], 0), safe_float(work.iloc[l2]["Low"], 0)
            if p1 <= 0 or p2 <= 0:
                continue
            diff = abs(p1 - p2) / max(p1, p2) * 100.0
            if diff > 6.0:
                continue
            mid_high_candidates = [h for h in highs if l1 < h < l2]
            if not mid_high_candidates:
                continue
            neck_idx = max(mid_high_candidates, key=lambda x: safe_float(work.iloc[x]["High"], 0))
            neck = safe_float(work.iloc[neck_idx]["High"], 0)
            if neck <= max(p1, p2) * 1.06:
                continue
            if p2 < p1 * 0.94:
                continue
            strength = (neck / max(p1, p2) - 1.0) * 100.0
            cand = {"l1": l1, "l2": l2, "neck_idx": neck_idx, "p1": p1, "p2": p2, "neck": neck, "strength": strength, "low_diff": diff, "gap": gap}
            if best is None or cand["strength"] > best["strength"]:
                best = cand
    if not best:
        return PatternSignal()
    row, prev = work.iloc[-1], work.iloc[-2] if len(work) >= 2 else work.iloc[-1]
    close, high, low, neck = safe_float(row["Close"], 0), safe_float(row["High"], 0), safe_float(row["Low"], 0), safe_float(best["neck"], 0)
    f = compute_support_features(work)
    state = classify_breakout_state(close, high, low, neck, f.get("vol_ratio", 0), volume_ratio(work, len(work) - 2, 20) if len(work) >= 2 else 0.0, safe_float(prev["Close"], 0))
    supports, risks = support_tags_from_features(f)
    primary, confirm, penalty = 54, 0, 0
    if state == "돌파 확인": confirm += 10
    elif state == "실행 가능": confirm += 18
    if f.get("vol_ok"): confirm += 5
    if f.get("obv_up"): confirm += 4
    if f.get("ma5_reclaim"): confirm += 2
    if "윗꼬리과다" in risks: penalty += 5
    if "RSI과열" in risks: penalty += 5
    return PatternSignal(True, "DOUBLE_BOTTOM_BREAKOUT", "진짜쌍바닥 넥라인형", state.replace(" ", "_"), state, primary, min(confirm, 20), penalty, primary + min(confirm, 20) - penalty, "넥라인", round(neck, 2), round(neck, 2), supports, risks, [f"저점차이 {best['low_diff']:.1f}%", f"저점간격 {best['gap']}봉"], best)

def detect_dynamic_resistance_breakout(df: pd.DataFrame, i: Optional[int] = None) -> PatternSignal:
    if df is None or len(df) < 45:
        return PatternSignal()
    i = len(df) - 1 if i is None else i
    work = df.iloc[:i + 1]
    highs = [x for x in find_pivot_highs(work, 3, 3) if x >= len(work) - 40]
    if len(highs) < 2:
        return PatternSignal()
    line, best_r2 = None, -999.0
    for a in range(len(highs) - 1):
        for b in range(a + 1, len(highs)):
            x1, x2 = highs[a], highs[b]
            if x2 - x1 < 5:
                continue
            y1, y2 = safe_float(work.iloc[x1]["High"], 0), safe_float(work.iloc[x2]["High"], 0)
            if y1 <= 0 or y2 <= 0:
                continue
            slope = (y2 - y1) / (x2 - x1)
            if slope > 0.15:
                continue
            xs, ys = np.array([x1, x2], dtype=float), np.array([y1, y2], dtype=float)
            coef = np.polyfit(xs, ys, 1)
            pred = coef[0] * xs + coef[1]
            ss_res = ((ys - pred) ** 2).sum()
            ss_tot = ((ys - ys.mean()) ** 2).sum()
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
            if r2 > best_r2:
                best_r2, line = r2, (coef[0], coef[1], x1, x2)
    if line is None:
        return PatternSignal()
    slope, intercept, x1, x2 = line
    line_price = slope * i + intercept
    if line_price <= 0:
        return PatternSignal()
    row, prev = work.iloc[-1], work.iloc[-2] if len(work) >= 2 else work.iloc[-1]
    f = compute_support_features(work)
    state = classify_breakout_state(safe_float(row["Close"], 0), safe_float(row["High"], 0), safe_float(row["Low"], 0), line_price, f.get("vol_ratio", 0), volume_ratio(work, len(work) - 2, 20) if len(work) >= 2 else 0.0, safe_float(prev["Close"], 0))
    supports, risks = support_tags_from_features(f)
    primary, confirm, penalty = 50, 0, 0
    if state == "돌파 확인": confirm += 10
    elif state == "실행 가능": confirm += 17
    if f.get("vol_ok"): confirm += 4
    if f.get("obv_up"): confirm += 3
    if f.get("struct_conv_ok"): confirm += 3
    if "윗꼬리과다" in risks: penalty += 4
    if "RSI과열" in risks: penalty += 5
    return PatternSignal(True, "DYN_RES_BREAKOUT", "동적저항 돌파형", state.replace(" ", "_"), state, primary, min(confirm, 20), penalty, primary + min(confirm, 20) - penalty, "동적저항", round(line_price, 2), round(line_price, 2), supports, risks, [f"기울기 {slope:.3f}"], {"slope": slope, "intercept": intercept, "r2_proxy": round(best_r2, 4), "pivot1": x1, "pivot2": x2})

def detect_ascending_triangle_breakout(df: pd.DataFrame, i: Optional[int] = None) -> PatternSignal:
    if df is None or len(df) < 50:
        return PatternSignal()
    i = len(df) - 1 if i is None else i
    work = df.iloc[:i + 1]
    highs = [x for x in find_pivot_highs(work, 3, 3) if x >= len(work) - 40]
    lows = [x for x in find_pivot_lows(work, 3, 3) if x >= len(work) - 40]
    if len(highs) < 2 or len(lows) < 2:
        return PatternSignal()
    high_prices = [(idx, safe_float(work.iloc[idx]["High"], 0)) for idx in highs]
    best = None
    for a in range(len(high_prices) - 1):
        for b in range(a + 1, len(high_prices)):
            idx1, p1 = high_prices[a]
            idx2, p2 = high_prices[b]
            if idx2 - idx1 < 6:
                continue
            diff = abs(p1 - p2) / max(p1, p2) * 100.0
            if diff > 3.0:
                continue
            resistance = (p1 + p2) / 2.0
            sub_lows = [(li, safe_float(work.iloc[li]["Low"], 0)) for li in lows if idx1 <= li <= i]
            if len(sub_lows) < 2:
                continue
            xs = np.array([x for x, _ in sub_lows], dtype=float)
            ys = np.array([y for _, y in sub_lows], dtype=float)
            coef = np.polyfit(xs, ys, 1)
            slope = coef[0]
            if slope <= 0:
                continue
            cand = {"resistance": resistance, "high_diff": diff, "low_slope": slope, "idx1": idx1, "idx2": idx2}
            if best is None or slope > best["low_slope"]:
                best = cand
    if not best:
        return PatternSignal()
    row, prev = work.iloc[-1], work.iloc[-2] if len(work) >= 2 else work.iloc[-1]
    f = compute_support_features(work)
    line_price = safe_float(best["resistance"], 0)
    state = classify_breakout_state(safe_float(row["Close"], 0), safe_float(row["High"], 0), safe_float(row["Low"], 0), line_price, f.get("vol_ratio", 0), volume_ratio(work, len(work) - 2, 20) if len(work) >= 2 else 0.0, safe_float(prev["Close"], 0))
    supports, risks = support_tags_from_features(f)
    primary, confirm, penalty = 56, 0, 0
    if state == "돌파 확인": confirm += 10
    elif state == "실행 가능": confirm += 18
    if f.get("vol_ok"): confirm += 4
    if f.get("obv_up"): confirm += 3
    if f.get("bb40_tight"): confirm += 3
    if "윗꼬리과다" in risks: penalty += 5
    if "RSI과열" in risks: penalty += 5
    return PatternSignal(True, "ASC_TRI_BREAKOUT", "상승삼각형 돌파형", state.replace(" ", "_"), state, primary, min(confirm, 20), penalty, primary + min(confirm, 20) - penalty, "상승삼각 수평저항", round(line_price, 2), round(line_price, 2), supports, risks, [f"상단차이 {best['high_diff']:.1f}%", f"저점기울기 {best['low_slope']:.3f}"], best)

def detect_reclaim_breakout(df: pd.DataFrame, i: Optional[int] = None) -> PatternSignal:
    if df is None or len(df) < 30:
        return PatternSignal()
    i = len(df) - 1 if i is None else i
    work = df.iloc[:i + 1]
    row, prev = work.iloc[-1], work.iloc[-2] if len(work) >= 2 else work.iloc[-1]
    f = compute_support_features(work)
    ma5, ma20, close = safe_float(row.get("MA5", 0), 0), safe_float(row.get("MA20", 0), 0), safe_float(row["Close"], 0)
    reclaim = bool((ma5 > 0 and close >= ma5) and (ma20 > 0 and close >= ma20 * 0.99) and (len(work) >= 4 and work["Close"].iloc[-4:-1].min() <= ma20 * 1.01))
    if not reclaim:
        return PatternSignal()
    line_price = safe_float(work["High"].iloc[max(0, len(work)-10):-1].max(), 0)
    if line_price <= 0:
        return PatternSignal()
    state = classify_breakout_state(close, safe_float(row["High"], 0), safe_float(row["Low"], 0), line_price, f.get("vol_ratio", 0), volume_ratio(work, len(work) - 2, 20) if len(work) >= 2 else 0.0, safe_float(prev["Close"], 0))
    supports, risks = support_tags_from_features(f)
    primary, confirm, penalty = 43, 0, 0
    if state == "돌파 확인": confirm += 9
    elif state == "실행 가능": confirm += 14
    if f.get("obv_up"): confirm += 3
    if f.get("vol_ok"): confirm += 4
    if "윗꼬리과다" in risks: penalty += 4
    return PatternSignal(True, "RECLAIM_BREAKOUT", "재안착 돌파형", state.replace(" ", "_"), state, primary, min(confirm, 20), penalty, primary + min(confirm, 20) - penalty, "직전고점", round(line_price, 2), round(line_price, 2), supports, risks, ["5/20 재안착"], {"reclaim": True})

PRIMARY_PRIORITY = ["ASC_TRI_BREAKOUT", "DOUBLE_BOTTOM_BREAKOUT", "DYN_RES_BREAKOUT", "BOX_BREAKOUT", "RECLAIM_BREAKOUT"]

def choose_primary_pattern(signals: List[PatternSignal]) -> PatternSignal:
    valid = [s for s in signals if s.ok]
    if not valid:
        return PatternSignal()
    order = {code: idx for idx, code in enumerate(PRIMARY_PRIORITY)}
    valid.sort(key=lambda s: (s.final_score, -order.get(s.pattern_code, 999), 1 if s.state_name == "실행 가능" else 0, 1 if s.state_name == "돌파 확인" else 0), reverse=True)
    return valid[0]

def detect_all_primary_patterns(df: pd.DataFrame, i: Optional[int] = None) -> List[PatternSignal]:
    df = ensure_indicators(df)
    return [detect_ascending_triangle_breakout(df, i), detect_double_bottom_breakout(df, i), detect_dynamic_resistance_breakout(df, i), detect_box_breakout(df, i), detect_reclaim_breakout(df, i)]

def enrich_row_with_pattern_overhaul(row: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
    out = dict(row or {})
    df = ensure_indicators(df)
    signals = detect_all_primary_patterns(df)
    primary = choose_primary_pattern(signals)
    all_pattern_names = [s.pattern_name for s in signals if s.ok]
    all_pattern_codes = [s.pattern_code for s in signals if s.ok]
    out["주패턴코드"] = primary.pattern_code
    out["주패턴명"] = primary.pattern_name
    out["패턴상태"] = primary.state_name
    out["파란점선유형"] = primary.blue_line_type
    out["파란점선가격"] = round(primary.blue_line_price, 2) if primary.blue_line_price else 0
    out["트리거가격"] = round(primary.trigger_price, 2) if primary.trigger_price else 0
    out["패턴주점수"] = primary.primary_score
    out["패턴확인점수"] = primary.confirm_score
    out["패턴리스크감점"] = primary.risk_penalty
    out["패턴최종점수"] = primary.final_score
    out["패턴보조태그"] = " / ".join(primary.support_tags[:3])
    out["패턴리스크태그"] = " / ".join(primary.risk_tags[:3])
    out["감지패턴목록"] = " / ".join(all_pattern_names)
    out["감지패턴코드목록"] = " / ".join(all_pattern_codes)
    state = primary.state_name
    out["패턴액션"] = state if state in ("매수 대기", "돌파 확인", "실행 가능") else ""
    if primary.pattern_code == "DOUBLE_BOTTOM_BREAKOUT":
        out["쌍바닥분류"] = "진짜쌍바닥"
    else:
        out["쌍바닥분류"] = ""
    if not out["쌍바닥분류"]:
        lows = find_pivot_lows(df, 3, 3)
        if len(lows) >= 2:
            l1, l2 = lows[-2], lows[-1]
            p1, p2 = safe_float(df.iloc[l1]["Low"], 0), safe_float(df.iloc[l2]["Low"], 0)
            if p1 > 0 and p2 > 0:
                diff = abs(p1 - p2) / max(p1, p2) * 100.0
                if 6.0 < diff <= 9.0:
                    out["쌍바닥분류"] = "유사쌍바닥"
    if primary.pattern_code in ("RECLAIM_BREAKOUT", "DOUBLE_BOTTOM_BREAKOUT") and primary.final_score >= 48:
        out["돌반지재정의"] = "돌반지 후보"
    else:
        out["돌반지재정의"] = ""
    return out

def build_pattern_summary_text(row: Dict[str, Any]) -> str:
    pattern = str(row.get("주패턴명", "") or "")
    state = str(row.get("패턴상태", "") or "")
    blue_type = str(row.get("파란점선유형", "") or "")
    blue_price = safe_float(row.get("파란점선가격", 0), 0)
    trigger = safe_float(row.get("트리거가격", 0), 0)
    supports = str(row.get("패턴보조태그", "") or "")
    risks = str(row.get("패턴리스크태그", "") or "")
    score = safe_int(row.get("패턴최종점수", 0), 0)
    if not pattern:
        return ""
    lines = [f"🧩 주패턴: {pattern}"]
    if state: lines.append(f"📍 상태: {state}")
    if blue_type and blue_price > 0: lines.append(f"🔵 파란점선: {blue_type} {blue_price:,.0f}")
    if trigger > 0: lines.append(f"🎯 트리거: 종가 {trigger:,.0f} 돌파 확인")
    if supports: lines.append(f"➕ 보조: {supports}")
    if risks: lines.append(f"⚠️ 리스크: {risks}")
    lines.append(f"🧮 패턴점수: {score}")
    return "\n".join(lines)

def build_pattern_one_line(row: Dict[str, Any]) -> str:
    pattern = str(row.get("주패턴명", "") or "")
    state = str(row.get("패턴상태", "") or "")
    blue_price = safe_float(row.get("파란점선가격", 0), 0)
    if not pattern:
        return ""
    parts = [pattern]
    if state: parts.append(state)
    if blue_price > 0: parts.append(f"기준선 {blue_price:,.0f}")
    return " | ".join(parts)

INTEGRATION_GUIDE = """
[연동 예시]
from pattern_overhaul_complete import enrich_row_with_pattern_overhaul, build_pattern_summary_text
row = enrich_row_with_pattern_overhaul(row, df)
pattern_block = build_pattern_summary_text(row)
if pattern_block:
    block += "\\n" + pattern_block
"""

if __name__ == "__main__":
    print("✅ pattern_overhaul_complete loaded")
    print(INTEGRATION_GUIDE)
