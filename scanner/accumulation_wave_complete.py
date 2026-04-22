# -*- coding: utf-8 -*-
"""
accumulation_wave_complete.py

Stock-Hunter 보조 모듈
- 매집봉 강도 판정
- 최근 매집봉 탐색
- 매집봉 고가/저가를 소파동 기준으로 연결
- 현재 위치(소파동 하단/상단 접근/전고점 돌파) 판정
- 출력용 라인 생성
- 자리평가/파란타점 보조 점수까지 포함
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd


def _safe_num(x: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def _col(df: pd.DataFrame, *names: str) -> Optional[str]:
    for name in names:
        if name in df.columns:
            return name
    return None


def _get_price_cols(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    return {
        "open": _col(df, "Open", "시가"),
        "high": _col(df, "High", "고가"),
        "low": _col(df, "Low", "저가"),
        "close": _col(df, "Close", "종가"),
        "volume": _col(df, "Volume", "거래량"),
    }


def calc_accum_candle_score(df: pd.DataFrame, idx: int) -> Dict[str, Any]:
    if df is None or len(df) < 5 or idx <= 0 or idx >= len(df):
        return {"score": 0, "grade": "없음", "label": "매집봉 없음", "is_accum": False, "high": None, "low": None, "close": None, "desc": "데이터 부족"}

    cols = _get_price_cols(df)
    if not cols["open"] or not cols["high"] or not cols["low"] or not cols["close"]:
        return {"score": 0, "grade": "없음", "label": "매집봉 없음", "is_accum": False, "high": None, "low": None, "close": None, "desc": "OHLC 컬럼 부족"}

    row = df.iloc[idx]
    prev = df.iloc[max(0, idx - 20):idx]

    o = _safe_num(row[cols["open"]])
    h = _safe_num(row[cols["high"]])
    l = _safe_num(row[cols["low"]])
    c = _safe_num(row[cols["close"]])
    v = _safe_num(row[cols["volume"]]) if cols["volume"] else 0.0

    body = abs(c - o)
    rng = max(h - l, 1e-6)
    lower_wick = min(o, c) - l
    upper_wick = h - max(o, c)

    close_pos = (c - l) / rng
    body_ratio = body / rng
    lower_wick_ratio = lower_wick / rng
    upper_wick_ratio = upper_wick / rng

    vma20 = _safe_num(prev[cols["volume"]].mean(), 0.0) if cols["volume"] and len(prev) > 0 else 0.0
    vol_ratio = (v / vma20) if vma20 > 0 else 0.0

    score = 0
    reasons: List[str] = []

    if lower_wick_ratio >= 0.35:
        score += 25
        reasons.append("아래꼬리흡수강")
    elif lower_wick_ratio >= 0.20:
        score += 15
        reasons.append("아래꼬리흡수보통")
    elif lower_wick_ratio >= 0.10:
        score += 5
        reasons.append("아래꼬리약")

    if close_pos >= 0.75:
        score += 20
        reasons.append("고가권마감")
    elif close_pos >= 0.60:
        score += 12
        reasons.append("회복양호")
    elif close_pos >= 0.50:
        score += 5
        reasons.append("회복보통")

    if body_ratio >= 0.35:
        score += 12
        reasons.append("몸통확인")
    elif body_ratio >= 0.20:
        score += 6
        reasons.append("몸통보통")

    if upper_wick_ratio >= 0.35:
        score -= 10
        reasons.append("윗꼬리과다")
    elif upper_wick_ratio >= 0.25:
        score -= 5
        reasons.append("윗꼬리주의")

    if vol_ratio >= 2.0:
        score += 22
        reasons.append("거래량강")
    elif vol_ratio >= 1.4:
        score += 14
        reasons.append("거래량증가")
    elif vol_ratio >= 1.1:
        score += 6
        reasons.append("거래량보통")

    future_df = df.iloc[idx + 1:min(len(df), idx + 4)]
    if len(future_df) > 0:
        acc_low = l
        acc_mid = (h + l) / 2.0
        closes = future_df[cols["close"]]
        lows = future_df[cols["low"]]
        if _safe_num(closes.min(), -1e9) >= acc_mid:
            score += 18
            reasons.append("후속유지강")
        elif _safe_num(lows.min(), -1e9) >= acc_low:
            score += 10
            reasons.append("후속유지보통")
        else:
            score -= 5
            reasons.append("후속확인약")

    score = max(0, min(100, int(round(score))))

    if score >= 70:
        grade, label, is_accum = "강함", "매집봉 강함", True
    elif score >= 40:
        grade, label, is_accum = "보통", "매집봉 보통", True
    elif score >= 20:
        grade, label, is_accum = "약함", "매집봉 약함", False
    else:
        grade, label, is_accum = "없음", "매집봉 없음", False

    return {
        "score": score,
        "grade": grade,
        "label": label,
        "is_accum": is_accum,
        "high": h,
        "low": l,
        "close": c,
        "open": o,
        "vol_ratio": round(vol_ratio, 2),
        "close_pos": round(close_pos, 2),
        "lower_wick_ratio": round(lower_wick_ratio, 2),
        "upper_wick_ratio": round(upper_wick_ratio, 2),
        "desc": " / ".join(reasons),
    }


def find_recent_accum_candle(df: pd.DataFrame, lookback: int = 20) -> Dict[str, Any]:
    if df is None or len(df) < 10:
        return {"found": False, "idx": None, "score": 0, "grade": "없음", "label": "매집봉 없음"}

    start = max(1, len(df) - lookback)
    best = None
    for idx in range(start, len(df) - 1):
        info = calc_accum_candle_score(df, idx)
        if best is None or info["score"] > best["score"]:
            best = {"idx": idx, **info}

    if best is None or best["score"] < 20:
        return {"found": False, "idx": None, "score": 0, "grade": "없음", "label": "매집봉 없음"}
    return {"found": True, **best}


def build_small_wave_from_accum(df: pd.DataFrame, accum_info: Dict[str, Any]) -> Dict[str, Any]:
    if not accum_info or not accum_info.get("found"):
        return {"valid": False, "wave_high": None, "wave_low": None, "wave_mid": None, "desc": "매집봉 없음"}

    wave_high = _safe_num(accum_info.get("high"))
    wave_low = _safe_num(accum_info.get("low"))
    wave_mid = round((wave_high + wave_low) / 2.0, 2)
    return {"valid": True, "wave_high": wave_high, "wave_low": wave_low, "wave_mid": wave_mid, "desc": f"소파동 기준: 저점 {wave_low:.0f} / 전고점 {wave_high:.0f}"}


def build_mid_wave_from_small_wave(df: pd.DataFrame, small_wave: Dict[str, Any], lookback: int = 60) -> Dict[str, Any]:
    if df is None or len(df) < 20 or not small_wave.get("valid"):
        return {"valid": False, "mid_high": None, "mid_low": None, "mid_mid": None, "desc": "중파동 기준 없음"}

    cols = _get_price_cols(df)
    if not cols["high"] or not cols["low"]:
        return {"valid": False, "mid_high": None, "mid_low": None, "mid_mid": None, "desc": "고가/저가 컬럼 없음"}

    recent = df.iloc[max(0, len(df) - lookback):]
    mid_high = _safe_num(recent[cols["high"]].max())
    mid_low = _safe_num(recent[cols["low"]].min())
    mid_mid = round((mid_high + mid_low) / 2.0, 2)
    return {"valid": True, "mid_high": mid_high, "mid_low": mid_low, "mid_mid": mid_mid, "desc": f"중파동 기준: 저점 {mid_low:.0f} / 전고점 {mid_high:.0f}"}


def evaluate_small_wave_position(df: pd.DataFrame, wave_info: Dict[str, Any]) -> Dict[str, Any]:
    if df is None or len(df) == 0 or not wave_info.get("valid"):
        return {"valid": False, "state": "미정", "desc": "소파동 기준 없음"}

    cols = _get_price_cols(df)
    if not cols["close"]:
        return {"valid": False, "state": "미정", "desc": "종가 컬럼 없음"}

    c = _safe_num(df.iloc[-1][cols["close"]])
    h = wave_info["wave_high"]
    mid = wave_info["wave_mid"]

    if c < mid:
        state, desc = "소파동 하단", "아직 소파동 중간 아래. 확인 전 선취/관찰 구간"
    elif mid <= c < h:
        state, desc = "소파동 상단 접근", "전고점 재도전 구간. 파란타점 후보로 해석 가능"
    elif c >= h:
        state, desc = "소파동 전고점 돌파", "소파동 돌파 확인 구간. 이후 눌림 지지 여부가 중요"
    else:
        state, desc = "중립", "중립 구조"

    return {"valid": True, "state": state, "desc": desc, "current_close": c}


def evaluate_mid_wave_position(df: pd.DataFrame, mid_wave: Dict[str, Any]) -> Dict[str, Any]:
    if df is None or len(df) == 0 or not mid_wave.get("valid"):
        return {"valid": False, "state": "미정", "desc": "중파동 기준 없음"}

    cols = _get_price_cols(df)
    if not cols["close"]:
        return {"valid": False, "state": "미정", "desc": "종가 컬럼 없음"}

    c = _safe_num(df.iloc[-1][cols["close"]])
    h = mid_wave["mid_high"]
    mid = mid_wave["mid_mid"]

    if c < mid:
        state, desc = "중파동 하단", "중기 구조상 아직 하단. 선취 또는 관찰 구간"
    elif mid <= c < h:
        state, desc = "중파동 상단 접근", "중기 전고점 재도전 구간"
    elif c >= h:
        state, desc = "중파동 전고점 돌파", "중파동 돌파 확인 구간"
    else:
        state, desc = "중립", "중기 중립 구조"

    return {"valid": True, "state": state, "desc": desc, "current_close": c}


def calc_ma_pullback_score(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or len(df) < 210:
        return {"score": 0, "label": "장기눌림 정보 부족", "desc": "데이터 부족"}

    cols = _get_price_cols(df)
    if not cols["close"]:
        return {"score": 0, "label": "장기눌림 정보 부족", "desc": "종가 없음"}

    close_s = df[cols["close"]].astype(float)
    ma150 = close_s.rolling(150).mean()
    ma200 = close_s.rolling(200).mean()
    c = _safe_num(close_s.iloc[-1])
    m150 = _safe_num(ma150.iloc[-1], c)
    m200 = _safe_num(ma200.iloc[-1], c)
    d150 = (c / m150 - 1.0) * 100 if m150 > 0 else 0.0
    d200 = (c / m200 - 1.0) * 100 if m200 > 0 else 0.0

    score = 0
    reasons = []
    if -3.0 <= d150 <= 8.0:
        score += 18; reasons.append("MA150근접")
    elif -6.0 <= d150 <= 12.0:
        score += 10; reasons.append("MA150보통")
    if -3.0 <= d200 <= 10.0:
        score += 18; reasons.append("MA200근접")
    elif -6.0 <= d200 <= 15.0:
        score += 10; reasons.append("MA200보통")

    if score >= 30:
        label = "장기눌림 우수"
    elif score >= 15:
        label = "장기눌림 보통"
    else:
        label = "장기눌림 약함"

    return {"score": score, "label": label, "d150": round(d150, 1), "d200": round(d200, 1), "desc": " / ".join(reasons) if reasons else "장기이평 거리 애매"}


def calc_drawdown_score(df: pd.DataFrame, lookback: int = 120) -> Dict[str, Any]:
    if df is None or len(df) < 20:
        return {"score": 0, "label": "조정률 정보 부족", "desc": "데이터 부족"}

    cols = _get_price_cols(df)
    if not cols["high"] or not cols["close"]:
        return {"score": 0, "label": "조정률 정보 부족", "desc": "컬럼 부족"}

    recent = df.iloc[max(0, len(df) - lookback):]
    recent_high = _safe_num(recent[cols["high"]].max())
    c = _safe_num(df.iloc[-1][cols["close"]])
    if recent_high <= 0:
        return {"score": 0, "label": "조정률 정보 부족", "desc": "고점 계산 실패"}

    dd = (c / recent_high - 1.0) * 100
    if -25 <= dd <= -8:
        score, label = 28, "조정률 우수"
    elif -35 <= dd < -25 or -8 < dd <= -3:
        score, label = 18, "조정률 보통"
    elif dd > -3:
        score, label = 5, "고점근접"
    else:
        score, label = 10, "과조정"

    return {"score": score, "label": label, "drawdown_pct": round(dd, 1), "desc": f"최근고점대비 {dd:.1f}%"}


def calc_overheat_penalty(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or len(df) < 25:
        return {"penalty": 0, "label": "과열정보 부족", "desc": "데이터 부족"}

    cols = _get_price_cols(df)
    if not cols["close"]:
        return {"penalty": 0, "label": "과열정보 부족", "desc": "종가 없음"}

    close_s = df[cols["close"]].astype(float)
    ma20 = close_s.rolling(20).mean()
    c = _safe_num(close_s.iloc[-1])
    m20 = _safe_num(ma20.iloc[-1], c)

    disparity = (c / m20) * 100 if m20 > 0 else 100
    penalty = 0
    reasons = []
    if disparity >= 130:
        penalty -= 32; reasons.append("과열-32")
    elif disparity >= 122:
        penalty -= 22; reasons.append("과열-22")
    elif disparity >= 116:
        penalty -= 12; reasons.append("과열-12")
    elif disparity >= 112:
        penalty -= 6; reasons.append("과열-6")

    return {"penalty": penalty, "label": "과열주의" if penalty < 0 else "과열양호", "disparity": round(disparity, 1), "desc": " / ".join(reasons) if reasons else "과열부담 낮음"}


def evaluate_position_score(df: pd.DataFrame) -> Dict[str, Any]:
    pullback = calc_ma_pullback_score(df)
    drawdown = calc_drawdown_score(df)
    overheat = calc_overheat_penalty(df)
    raw_score = pullback["score"] + drawdown["score"] + overheat["penalty"]
    score = max(0, min(100, raw_score + 40))

    if score >= 75:
        label = "자리 매우 좋음"
    elif score >= 55:
        label = "자리 양호"
    elif score >= 35:
        label = "자리 보통"
    else:
        label = "자리 불리"

    return {
        "score": int(round(score)),
        "label": label,
        "desc": " / ".join([pullback["label"], drawdown["label"], overheat["label"]]),
        "pullback": pullback,
        "drawdown": drawdown,
        "overheat": overheat,
    }


def calc_blue_zone_hint(df: pd.DataFrame, small_wave: Dict[str, Any]) -> Dict[str, Any]:
    if not small_wave.get("valid"):
        return {"valid": False, "zone_low": None, "zone_high": None, "label": "파란영역 없음", "desc": "소파동 기준 없음"}

    low = _safe_num(small_wave["wave_low"])
    high = _safe_num(small_wave["wave_high"])
    zone_low = round(low + (high - low) * 0.55, 2)
    zone_high = round(low + (high - low) * 0.92, 2)
    return {"valid": True, "zone_low": zone_low, "zone_high": zone_high, "label": "파란타점 영역", "desc": f"{zone_low:.0f} ~ {zone_high:.0f}"}


def grade_accum_text(score: int) -> str:
    if score >= 70:
        return "매집봉 강함 | 세력 흡수 흔적이 비교적 뚜렷"
    if score >= 40:
        return "매집봉 보통 | 매집 흔적은 있으나 확인이 더 필요"
    if score >= 20:
        return "매집봉 약함 | 단순 반등봉과 구분이 애매"
    return "매집봉 없음 | 의미 있는 매집 신호 부족"


def render_accum_wave_lines(df: pd.DataFrame) -> List[str]:
    acc = find_recent_accum_candle(df, lookback=20)
    small_wave = build_small_wave_from_accum(df, acc)
    mid_wave = build_mid_wave_from_small_wave(df, small_wave, lookback=60)
    small_pos = evaluate_small_wave_position(df, small_wave)
    mid_pos = evaluate_mid_wave_position(df, mid_wave)
    pos_eval = evaluate_position_score(df)
    blue_zone = calc_blue_zone_hint(df, small_wave)

    lines: List[str] = []
    if not acc.get("found"):
        lines.append("🪵 매집봉: 없음")
        lines.append(f"🪑 자리평가: {pos_eval['label']} ({pos_eval['score']}) | {pos_eval['desc']}")
        return lines

    lines.append(f"🪵 매집봉: {acc['label']} ({acc['score']}) | 고가:{acc['high']:.0f} / 저가:{acc['low']:.0f}")
    lines.append(f"🧲 매집봉 해설: {acc['desc']}")
    lines.append(f"📝 매집봉 판정: {grade_accum_text(acc['score'])}")
    if small_wave.get("valid"):
        lines.append(f"〰️ 소파동: 저점 {small_wave['wave_low']:.0f} → 전고점 {small_wave['wave_high']:.0f}")
    if small_pos.get("valid"):
        lines.append(f"📍 소파동 위치: {small_pos['state']} | {small_pos['desc']}")
    if mid_wave.get("valid"):
        lines.append(f"📶 중파동: 저점 {mid_wave['mid_low']:.0f} → 전고점 {mid_wave['mid_high']:.0f}")
    if mid_pos.get("valid"):
        lines.append(f"🧭 중파동 위치: {mid_pos['state']} | {mid_pos['desc']}")
    if blue_zone.get("valid"):
        lines.append(f"🔵 파란타점 영역: {blue_zone['desc']} | 점이 아니라 지지 확인 구간으로 해석")
    lines.append(f"🪑 자리평가: {pos_eval['label']} ({pos_eval['score']}) | {pos_eval['desc']}")
    lines.append(f"   - 장기눌림: {pos_eval['pullback']['label']} ({pos_eval['pullback']['score']})")
    lines.append(f"   - 조정률: {pos_eval['drawdown']['label']} ({pos_eval['drawdown']['score']}) | {pos_eval['drawdown']['desc']}")
    lines.append(f"   - 과열패널티: {pos_eval['overheat']['label']} ({pos_eval['overheat']['penalty']}) | {pos_eval['overheat']['desc']}")
    return lines


def analyze_accum_wave_package(df: pd.DataFrame) -> Dict[str, Any]:
    acc = find_recent_accum_candle(df, lookback=20)
    small_wave = build_small_wave_from_accum(df, acc)
    mid_wave = build_mid_wave_from_small_wave(df, small_wave, lookback=60)
    small_pos = evaluate_small_wave_position(df, small_wave)
    mid_pos = evaluate_mid_wave_position(df, mid_wave)
    pos_eval = evaluate_position_score(df)
    blue_zone = calc_blue_zone_hint(df, small_wave)
    return {
        "accum": acc,
        "small_wave": small_wave,
        "mid_wave": mid_wave,
        "small_pos": small_pos,
        "mid_pos": mid_pos,
        "position_eval": pos_eval,
        "blue_zone": blue_zone,
        "lines": render_accum_wave_lines(df),
    }


INTEGRATION_EXAMPLE = r"""
from accumulation_wave_complete import analyze_accum_wave_package

pkg = analyze_accum_wave_package(df)

for line in pkg["lines"]:
    lines.append(line)

accum_score = pkg["accum"].get("score", 0)
accum_grade = pkg["accum"].get("grade", "없음")
small_wave_high = pkg["small_wave"].get("wave_high")
small_wave_low = pkg["small_wave"].get("wave_low")
blue_zone_low = pkg["blue_zone"].get("zone_low")
blue_zone_high = pkg["blue_zone"].get("zone_high")
position_score = pkg["position_eval"].get("score", 0)
"""


if __name__ == "__main__":
    print("accumulation_wave_complete.py loaded")
    print("이 파일은 보조 모듈입니다. 기존 스캐너에서 import 해서 사용하세요.")
