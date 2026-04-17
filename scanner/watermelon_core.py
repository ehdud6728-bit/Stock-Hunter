from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .utils import safe_float
from .breakout_logic import compute_breakout_mode_fields

"""실제 로직 이관형 watermelon_core 모듈.
수박 상태 판정/정제/Blue 계열 계산 로직을 분리했다.
"""


def compute_dante_mode_fields(df: pd.DataFrame) -> pd.DataFrame:
    """legacy에 남아있는 단테 로직을 지연 참조한다."""
    from scanner import legacy_main_patched as legacy
    return legacy.compute_dante_mode_fields(df)

def _clamp_grade_down(current_grade: str, target_grade: str) -> str:
    rank = _grade_rank_map()
    cur = str(current_grade or "C").strip()
    tgt = str(target_grade or "C").strip()
    if rank.get(cur, 0) > rank.get(tgt, 0):
        return tgt
    return cur

def _is_largecap_blue_guard_target(cur: dict, row: dict) -> bool:
    idx_label = str(row.get("index_label", "") or row.get("유니버스태그", "") or row.get("universe_tag", "") or "")
    marcap = 0.0
    try:
        marcap = float(row.get("marcap") or row.get("market_cap") or row.get("시가총액") or 0)
    except Exception:
        marcap = 0.0
    return ("코스피200" in idx_label) or ("KOSPI200" in idx_label) or (marcap >= 5_000_000_000_000)

def _largecap_blue_confirm_score(cur: dict) -> int:
    score = 0
    close = float(cur.get("close", 0) or 0)
    prev_box_high10 = float(cur.get("prev_box_high10", 0) or 0)
    volume = float(cur.get("volume", 0) or 0)
    vol_ma20 = float(cur.get("vol_ma20", 0) or 0)
    rsi = float(cur.get("rsi", 0) or 0)
    adx = float(cur.get("adx", 0) or 0)
    adx_prev = float(cur.get("adx_prev", 0) or 0)
    obv_slope5 = float(cur.get("obv_slope5", 0) or 0)
    obv_rising = bool(cur.get("obv_rising", False))

    if prev_box_high10 > 0 and close >= prev_box_high10 * 0.99:
        score += 1
    if vol_ma20 > 0 and volume >= vol_ma20 * 1.05:
        score += 1
    if rsi >= 54.0:
        score += 1
    if obv_slope5 > 0 or obv_rising:
        score += 1
    if adx >= 16.0 or adx > adx_prev:
        score += 1
    return score

def build_watermelon_state_bundle(df: pd.DataFrame) -> dict:
    """
    red onset(붉은 보조지표 시작점) 기반 수박/파란점 엔진.
    핵심:
      - 초입수박: 실제 박스권이 있고, 아직 과열되지 않았으며, 아직 빨강이 시작되기 전 준비 상태
      - Blue-1: 초입 박스에서 '첫 red onset'이 발생하는 순간(단기)
      - 눌림수박: Blue-1 이후 건강한 눌림 박스가 다시 만들어진 상태
      - Blue-2: 눌림 박스 이후 '두 번째 red onset'이 발생하는 순간(스윙)
      - 후행수박: 이미 많이 진행된 뒤의 늦은 상태(신규매수보다 경계/관리)
    """
    empty_bundle = {
        "wm_base_score": 0,
        "wm_pocket_score": 0,
        "wm_attack_score": 0,
        "wm_blue_score": 0,
        "wm_blue1_score": 0,
        "wm_blue2_score": 0,
        "wm_pocket_raw": False,
        "wm_pocket_hold": False,
        "wm_attack_raw": False,
        "wm_attack_hold": False,
        "wm_blue_raw": False,
        "wm_blue_hold": False,
        "wm_intro": False,
        "wm_pullback": False,
        "wm_late": False,
        "wm_blue1_raw": False,
        "wm_blue1_hold": False,
        "wm_blue2_raw": False,
        "wm_blue2_hold": False,
        "wm_state_green": False,
        "wm_state_red": False,
        "wm_state_blue": False,
        "wm_final_state": "",
        "wm_state_name": "",
        "wm_state_grade": "없음",
        "wm_state_tags": [],
        "wm_state_detail": {"ok": False, "error": "봉수 부족"},
    }
    if df is None or df.empty or len(df) < 80:
        return empty_bundle

    def _upper_wick_ratio(row) -> float:
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

    def _metrics(sub: pd.DataFrame) -> dict:
        r = sub.iloc[-1]

        close = safe_float(r.get("Close", 0))
        open_ = safe_float(r.get("Open", 0))
        high = safe_float(r.get("High", 0))
        low = safe_float(r.get("Low", 0))
        volume = safe_float(r.get("Volume", 0))
        ma5 = safe_float(r.get("MA5", 0))
        ma20 = safe_float(r.get("MA20", 0))
        ma60 = safe_float(r.get("MA60", 0))
        ma112 = safe_float(r.get("MA112", 0))
        obv = safe_float(r.get("OBV", 0))
        obv_ma10 = safe_float(r.get("OBV_MA10", 0))
        rsi = safe_float(r.get("RSI", 50), 50)

        recent120 = sub.tail(120)
        recent30 = sub.tail(30)
        recent25 = sub.tail(25)
        recent20 = sub.tail(20)
        recent15 = sub.tail(15)
        recent10 = sub.tail(10)
        recent5 = sub.tail(5)

        box_high25 = safe_float(recent25["High"].max(), 0) if len(recent25) else 0
        box_low25 = safe_float(recent25["Low"].min(), 0) if len(recent25) else 0
        prev_box_high25 = safe_float(recent25["High"].iloc[:-1].max(), 0) if len(recent25) >= 2 else box_high25
        prev_box_high20 = safe_float(recent20["High"].iloc[:-1].max(), 0) if len(recent20) >= 2 else safe_float(recent20["High"].max(), 0)
        prev_box_high10 = safe_float(recent10["High"].iloc[:-1].max(), 0) if len(recent10) >= 2 else safe_float(recent10["High"].max(), 0)
        prev_box_high5 = safe_float(recent5["High"].iloc[:-1].max(), 0) if len(recent5) >= 2 else safe_float(recent5["High"].max(), 0)

        low5 = safe_float(recent5["Low"].min(), 0) if len(recent5) else 0
        low10 = safe_float(recent10["Low"].min(), 0) if len(recent10) else 0
        low15 = safe_float(recent15["Low"].min(), 0) if len(recent15) else 0
        low20 = safe_float(recent20["Low"].min(), 0) if len(recent20) else 0

        vol_ma20 = safe_float(recent20["Volume"].mean(), 0) if len(recent20) else 0
        vol_ma5 = safe_float(recent5["Volume"].mean(), 0) if len(recent5) else 0
        amount = close * volume
        amount_ma20 = safe_float((recent20["Close"] * recent20["Volume"]).mean(), 0) if len(recent20) else 0

        box_range_pct25 = ((box_high25 - box_low25) / close * 100.0) if close > 0 and box_high25 > 0 else 999.0
        pullback_pct25 = ((box_high25 - close) / box_high25 * 100.0) if box_high25 > 0 else 0.0

        ret7 = ((close / safe_float(sub["Close"].iloc[-8], 0)) - 1.0) * 100.0 if len(sub) >= 8 and safe_float(sub["Close"].iloc[-8], 0) > 0 else 0.0
        ret15 = ((close / safe_float(sub["Close"].iloc[-16], 0)) - 1.0) * 100.0 if len(sub) >= 16 and safe_float(sub["Close"].iloc[-16], 0) > 0 else 0.0
        ret20 = ((close / safe_float(sub["Close"].iloc[-21], 0)) - 1.0) * 100.0 if len(sub) >= 21 and safe_float(sub["Close"].iloc[-21], 0) > 0 else 0.0

        daychg10 = (((recent10["Close"] / recent10["Close"].shift(1)) - 1.0) * 100.0).replace([np.inf, -np.inf], np.nan).fillna(0) if len(recent10) else pd.Series(dtype=float)
        max_day_up10 = safe_float(daychg10.max(), 0) if len(daychg10) else 0

        cond_base_1 = bool((((recent120["High"] / recent120["Close"].shift(1)) - 1.0) * 100 >= 8).fillna(False).any()) if {"High", "Close"} <= set(recent120.columns) else False
        cond_base_2 = bool((recent120["Volume"] > recent120["Volume"].rolling(20, min_periods=5).mean() * 1.8).fillna(False).any()) if "Volume" in recent120.columns else False
        cond_base_3 = (close >= ma60 * 0.98) if ma60 > 0 else False
        cond_base_4 = (ma20 >= ma60 * 0.96) if ma20 > 0 and ma60 > 0 else False
        cond_base_5 = (obv >= obv_ma10) if obv_ma10 != 0 else False
        base_score = int(cond_base_1) + int(cond_base_2) + int(cond_base_3) + int(cond_base_4) + int(cond_base_5)

        cond_pocket_1 = 6.0 <= box_range_pct25 <= 18.0
        cond_pocket_2 = (close >= box_low25 * 1.03 and close <= prev_box_high25 * 1.01) if box_low25 > 0 and prev_box_high25 > 0 else False
        cond_pocket_3 = (low5 >= low10 * 0.985) if low10 > 0 else False
        cond_pocket_4 = (vol_ma5 <= vol_ma20 * 1.05) if vol_ma20 > 0 else False
        cond_pocket_5 = ((close >= ma20 * 0.97) if ma20 > 0 else False) or ((close >= ma60 * 0.98) if ma60 > 0 else False)
        pocket_score = int(cond_pocket_1) + int(cond_pocket_2) + int(cond_pocket_3) + int(cond_pocket_4) + int(cond_pocket_5)

        cond_attack_1 = (obv >= obv_ma10) if obv_ma10 != 0 else False
        cond_attack_2 = (close >= ma20 * 0.99) if ma20 > 0 else False
        cond_attack_3 = (ma5 >= ma20 * 0.995) if ma5 > 0 and ma20 > 0 else False
        cond_attack_4 = (volume >= vol_ma20 * 1.05) if vol_ma20 > 0 else False
        cond_attack_5 = (amount >= amount_ma20 * 1.05) if amount_ma20 > 0 else False
        attack_score = int(cond_attack_1) + int(cond_attack_2) + int(cond_attack_3) + int(cond_attack_4) + int(cond_attack_5)

        change_ready = (
            (attack_score >= 3)
            and ((volume >= vol_ma20 * 1.05) if vol_ma20 > 0 else False)
            and ((amount >= amount_ma20 * 1.05) if amount_ma20 > 0 else False)
            and (_upper_wick_ratio(r) <= 40.0)
            and (rsi < 72.0)
        )

        box_ready = (base_score >= 2) and (pocket_score >= 4)

        return {
            "close": close, "open": open_, "high": high, "low": low, "volume": volume,
            "ma5": ma5, "ma20": ma20, "ma60": ma60, "ma112": ma112,
            "obv": obv, "obv_ma10": obv_ma10, "rsi": rsi,
            "vol_ma20": vol_ma20, "vol_ma5": vol_ma5,
            "amount": amount, "amount_ma20": amount_ma20,
            "box_high25": box_high25, "box_low25": box_low25,
            "prev_box_high25": prev_box_high25, "prev_box_high20": prev_box_high20,
            "prev_box_high10": prev_box_high10, "prev_box_high5": prev_box_high5,
            "low5": low5, "low10": low10, "low15": low15, "low20": low20,
            "box_range_pct25": box_range_pct25, "pullback_pct25": pullback_pct25,
            "ret7": ret7, "ret15": ret15, "ret20": ret20, "max_day_up10": max_day_up10,
            "base_score": base_score, "pocket_score": pocket_score, "attack_score": attack_score,
            "box_ready": box_ready, "change_ready": change_ready,
            "cond_base": [cond_base_1, cond_base_2, cond_base_3, cond_base_4, cond_base_5],
            "cond_pocket": [cond_pocket_1, cond_pocket_2, cond_pocket_3, cond_pocket_4, cond_pocket_5],
            "cond_attack": [cond_attack_1, cond_attack_2, cond_attack_3, cond_attack_4, cond_attack_5],
        }

    start_idx = max(80, len(df) - 60)
    hist = []
    red_state_hist = []
    red_onset_hist = []
    blue1_onset_hist = []
    pullback_hist = []
    blue2_onset_hist = []

    for i in range(start_idx, len(df)):
        sub = df.iloc[:i+1]
        m = _metrics(sub)

        had_red_onset_recent = sum(1 for x in red_onset_hist[-20:] if x) >= 1
        had_blue1_recent = sum(1 for x in blue1_onset_hist[-20:] if x) >= 1
        had_pullback_recent = sum(1 for x in pullback_hist[-12:] if x) >= 1
        had_blue2_recent = sum(1 for x in blue2_onset_hist[-25:] if x) >= 1

        # ✅ late 완화: 단순히 박스 고점 근처라는 이유만으로 후행수박으로 밀어내지 않음
        late = (
            (m["ret20"] >= 26.0)
            or (m["max_day_up10"] >= 14.0)
            or (
                ((m["close"] >= m["prev_box_high25"] * 1.015) if m["prev_box_high25"] > 0 else False)
                and (m["ret15"] >= 12.0)
            )
            or had_blue2_recent
        )

        intro_box_range_ok = bool(5.0 <= m["box_range_pct25"] <= 32.0)
        intro_attack_band_ok = bool(2 <= m["attack_score"] <= 5)
        intro_ret7_ok = bool(m["ret7"] <= 14.0)
        intro_ret15_ok = bool(m["ret15"] <= 18.0)
        intro_ret20_ok = bool(m["ret20"] <= 17.0)
        intro_dayup_ok = bool(m["max_day_up10"] <= 10.0)
        intro_top_near_ok = bool((m["close"] <= m["prev_box_high25"] * 1.00) if m["prev_box_high25"] > 0 else True)
        intro_vol_calm_ok = bool((m["volume"] <= m["vol_ma20"] * 1.80) if m["vol_ma20"] > 0 else True)
        intro_no_prior_blue1_ok = bool(not had_blue1_recent)
        intro_no_prior_blue2_ok = bool(not had_blue2_recent)
        intro_not_late_ok = bool(not late)

        # ✅ intro_box를 전부 AND가 아니라 점수형으로 완화
        intro_optional_score = sum([
            intro_ret7_ok,
            intro_ret15_ok,
            intro_ret20_ok,
            intro_dayup_ok,
            intro_top_near_ok,
            intro_vol_calm_ok,
            intro_no_prior_blue1_ok,
            intro_no_prior_blue2_ok,
            intro_not_late_ok,
        ])

        intro_box_base_ok = bool(
            m["box_ready"]
            or (
                intro_box_range_ok
                and intro_top_near_ok
                and intro_vol_calm_ok
                and intro_optional_score >= 8
            )
        )

        intro_box_ready = (
            intro_box_base_ok
            and intro_box_range_ok
            and intro_attack_band_ok
            and intro_optional_score >= 6
        )

        change_relaxed_1 = bool(
            m["change_ready"]
            or (
                intro_box_ready
                and ((m["close"] >= m["ma20"] * 0.992) if m["ma20"] > 0 else False)
                and ((m["ma5"] >= m["ma20"] * 0.995) if m["ma5"] > 0 and m["ma20"] > 0 else False)
            )
        )

        red_state_raw = (
            intro_box_ready
            and change_relaxed_1
            and ((m["close"] >= m["ma20"] * 0.985) if m["ma20"] > 0 else False)
            and ((m["ma5"] >= m["ma20"] * 0.985) if m["ma5"] > 0 and m["ma20"] > 0 else False)
            and ((m["close"] >= m["prev_box_high10"] * 0.970) if m["prev_box_high10"] > 0 else False)
            and not late
        )
        prev_red_state = red_state_hist[-1] if red_state_hist else False
        red_onset = bool(red_state_raw and (not prev_red_state))

        # ✅ Blue-1 완화: 초기 신호가 아예 안 뜨는 문제를 줄임
        blue1_onset = (
            red_onset
            and intro_box_ready
            and (m["ret7"] <= 11.0)
            and (m["ret15"] <= 15.0)
            and (m["ret20"] <= 17.0)
            and ((m["close"] <= m["prev_box_high25"] * 1.03) if m["prev_box_high25"] > 0 else True)
            and ((m["volume"] >= m["vol_ma20"] * 0.90) if m["vol_ma20"] > 0 else True)
            and not late
        )

        pullback_box = (
            had_blue1_recent
            and not late
            and (2.0 <= m["pullback_pct25"] <= 15.0)
            and (((m["close"] >= m["ma20"] * 0.97) if m["ma20"] > 0 else False) or ((m["close"] >= m["ma60"] * 0.98) if m["ma60"] > 0 else False))
            and ((m["vol_ma5"] <= m["vol_ma20"] * 1.10) if m["vol_ma20"] > 0 else False)
            and ((m["close"] < m["box_high25"] * 0.995) if m["box_high25"] > 0 else False)
            and (m["low5"] > m["low20"] * 1.00 if m["low20"] > 0 else False)
            and not red_state_raw
        )

        # Blue-1 이전이라도, 한 번 움직인 뒤 건강하게 눌리며 재정비되는 박스는 눌림수박 후보로 인정
        pre_pullback_box = (
            (not had_blue1_recent)
            and not late
            and (2.0 <= m["pullback_pct25"] <= 15.0)
            and (m["box_range_pct25"] <= 26.0)
            and (m["ret20"] <= 17.0)
            and (m["max_day_up10"] <= 10.0)
            and (((m["close"] >= m["ma20"] * 0.98) if m["ma20"] > 0 else False) or ((m["close"] >= m["ma60"] * 0.99) if m["ma60"] > 0 else False))
            and ((m["vol_ma5"] <= m["vol_ma20"] * 1.15) if m["vol_ma20"] > 0 else False)
            and ((m["close"] < m["box_high25"] * 0.996) if m["box_high25"] > 0 else False)
            and (m["low5"] > m["low20"] * 0.995 if m["low20"] > 0 else False)
            and not red_state_raw
        )

        change_relaxed_2 = bool(
            m["change_ready"]
            or (
                ((m["close"] >= m["ma20"] * 0.992) if m["ma20"] > 0 else False)
                and ((m["ma5"] >= m["ma20"] * 0.995) if m["ma20"] > 0 and m["ma5"] > 0 else False)
            )
        )

        red2_pullback_ok = bool(pullback_box or pre_pullback_box)
        red2_change_ok = bool(change_relaxed_2)
        red2_close_ma20_ok = bool((m["close"] >= m["ma20"] * 0.975) if m["ma20"] > 0 else False)
        red2_ma5_ma20_ok = bool((m["ma5"] >= m["ma20"] * 0.988) if m["ma20"] > 0 and m["ma5"] > 0 else False)
        red2_prevbox_ok = bool((m["close"] >= m["prev_box_high10"] * 0.965) if m["prev_box_high10"] > 0 else False)
        red2_vol_ok = bool((m["volume"] >= m["vol_ma20"] * 0.78) if m["vol_ma20"] > 0 else True)
        red2_candle_ok = bool(
            ((m["close"] >= m["open"] * 0.995) if m.get("open", 0) > 0 else True)
            or (m["close"] >= m.get("prev_close", 0))
        )
        red2_not_late_ok = bool(not late)

        # 핵심 구조: m520 또는 pbox 중 하나만 살아도 재점화 예비로 인정
        red2_structure_ok = bool(
            red2_close_ma20_ok
            and red2_vol_ok
            and red2_not_late_ok
            and (red2_ma5_ma20_ok or red2_prevbox_ok)
        )

        # 완화 구조: 5일선 회복 + 준양봉이면 pbox 부족도 예비 인정
        red2_soft_ok = bool(
            red2_close_ma20_ok
            and red2_vol_ok
            and red2_not_late_ok
            and red2_candle_ok
            and ((m["close"] >= m["ma5"]) if m["ma5"] > 0 else False)
        )

        red_state_raw_2 = (
            red2_pullback_ok
            and red2_change_ok
            and (red2_structure_ok or red2_soft_ok)
        )

        blue2_prev_clear_ok = bool((not prev_red_state) or red2_soft_ok)
        blue2_context_ok = bool(
            (had_blue1_recent and had_pullback_recent)
            or pre_pullback_box
            or (pullback_box and (3.0 <= m["pullback_pct25"] <= 15.0))
            or (pullback_box and (m["close"] >= m["ma5"] if m["ma5"] > 0 else False))
        )
        blue2_vol2_ok = bool((m["volume"] >= m["vol_ma20"] * 0.78) if m["vol_ma20"] > 0 else True)

        # ✅ Blue-2 정밀 보정: m520/pbox/candle 병목을 완화해 예비 재점화까지는 통과 허용
        blue2_onset = (
            red_state_raw_2
            and blue2_prev_clear_ok
            and blue2_context_ok
            and blue2_vol2_ok
            and not late
        )

        blue2_strong = bool(
            blue2_onset
            and red2_structure_ok
            and (
                red2_prevbox_ok
                or red2_ma5_ma20_ok
            )
        )
        blue2_preview = bool(
            blue2_onset
            and (not blue2_strong)
            and red2_soft_ok
        )

        # final state: onset를 우선, 그 다음 박스 준비 상태
        final_state = ""
        if late:
            final_state = "후행수박"
        elif blue2_strong:
            final_state = "Blue-2스윙"
        elif blue2_preview:
            final_state = "Blue-2예비"
        elif (pullback_box or pre_pullback_box):
            final_state = "눌림수박"
        elif blue1_onset:
            final_state = "Blue-1단기"
        elif intro_box_ready:
            final_state = "초입수박"

        pocket_raw = final_state in ("초입수박", "눌림수박")
        attack_raw = final_state in ("Blue-1단기", "Blue-2스윙", "Blue-2예비")

        hist.append({
            **m,
            "intro_box_ready": intro_box_ready,
            "red_state_raw": red_state_raw,
            "red_onset": red_onset,
            "blue1_onset": blue1_onset,
            "pullback_box": (pullback_box or pre_pullback_box),
            "red_state_raw_2": red_state_raw_2,
            "blue2_onset": blue2_onset,
            "blue2_strong": blue2_strong,
            "blue2_preview": blue2_preview,
            "late": late,
            "red2_pullback_ok": red2_pullback_ok,
            "red2_change_ok": red2_change_ok,
            "red2_close_ma20_ok": red2_close_ma20_ok,
            "red2_ma5_ma20_ok": red2_ma5_ma20_ok,
            "red2_prevbox_ok": red2_prevbox_ok,
            "red2_vol_ok": red2_vol_ok,
            "red2_candle_ok": red2_candle_ok,
            "red2_not_late_ok": red2_not_late_ok,
            "red2_structure_ok": red2_structure_ok,
            "red2_soft_ok": red2_soft_ok,
            "blue2_prev_clear_ok": blue2_prev_clear_ok,
            "blue2_context_ok": blue2_context_ok,
            "blue2_vol2_ok": blue2_vol2_ok,
            "intro_box_range_ok": intro_box_range_ok,
            "intro_attack_band_ok": intro_attack_band_ok,
            "intro_ret7_ok": intro_ret7_ok,
            "intro_ret15_ok": intro_ret15_ok,
            "intro_ret20_ok": intro_ret20_ok,
            "intro_dayup_ok": intro_dayup_ok,
            "intro_top_near_ok": intro_top_near_ok,
            "intro_vol_calm_ok": intro_vol_calm_ok,
            "intro_no_prior_blue1_ok": intro_no_prior_blue1_ok,
            "intro_no_prior_blue2_ok": intro_no_prior_blue2_ok,
            "intro_not_late_ok": intro_not_late_ok,
            "pocket_raw": pocket_raw,
            "pocket_hold": pocket_raw,
            "attack_raw": attack_raw,
            "attack_hold": attack_raw,
            "intro": final_state == "초입수박",
            "pullback": final_state == "눌림수박",
            "blue1_raw": blue1_onset,
            "blue1_hold": final_state == "Blue-1단기",
            "blue2_raw": blue2_onset,
            "blue2_hold": final_state == "Blue-2스윙",
            "final_state": final_state,
        })
        red_state_hist.append(bool(red_state_raw or red_state_raw_2))
        red_onset_hist.append(bool(red_onset or blue2_onset))
        blue1_onset_hist.append(bool(blue1_onset))
        pullback_hist.append(bool(pullback_box))
        blue2_onset_hist.append(bool(blue2_onset))

    cur = hist[-1]
    final_state = cur.get("final_state", "")

    wm_state_green = final_state in ("초입수박", "눌림수박")
    wm_state_red = final_state in ("Blue-1단기", "Blue-2스윙")
    wm_state_blue = wm_state_red

    if final_state == "초입수박":
        tags = ["🟢초입수박"]
        state_name = "초입수박"
    elif final_state == "눌림수박":
        tags = ["🟩눌림수박"]
        state_name = "눌림수박"
    elif final_state == "후행수박":
        tags = ["🟠후행수박"]
        state_name = "후행수박"
    elif final_state == "Blue-1단기":
        tags = ["🔵Blue-1단기", "🍉재점화공격"]
        state_name = "Blue-1단기"
    elif final_state == "Blue-2스윙":
        tags = ["🔷Blue-2스윙", "🍉재점화공격"]
        state_name = "Blue-2스윙"
    elif final_state == "Blue-2예비":
        tags = ["🔹Blue-2예비", "🍉재점화예비"]
        state_name = "Blue-2예비"
    else:
        tags = []
        state_name = "없음"

    if final_state == "Blue-1단기" and _is_largecap_blue_guard_target(cur, row):
        if blue_confirm >= 4:
            if "🧢대형주확정" not in tags:
                tags.append("🧢대형주확정")
        elif blue_confirm >= 2:
            tags = [t for t in tags if "🍉재점화공격" not in t]
            if "🔵Blue-1확인중" not in tags:
                tags.append("🔵Blue-1확인중")
        else:
            tags = [t for t in tags if "🍉재점화공격" not in t]
            if "🔵Blue-1예비" not in tags:
                tags.append("🔵Blue-1예비")

    wm_blue1_score = int(cur["intro_box_ready"]) + int(cur["change_ready"]) + int(cur["red_onset"]) + int(cur["blue1_onset"]) + int(cur["ret7"] <= 7.0) + int(cur["ret15"] <= 12.0)
    wm_blue2_score = int(cur["pullback_box"]) + int(cur["change_ready"]) + int(cur["blue2_onset"]) + int(3.0 <= cur["pullback_pct25"] <= 14.0) + int(cur["rsi"] < 71.0) + int(cur["volume"] >= cur["vol_ma20"] * 1.08 if cur["vol_ma20"] > 0 else False)
    wm_blue_score = max(wm_blue1_score, wm_blue2_score)

    score_sum = int(cur["base_score"] + cur["pocket_score"] + cur["attack_score"])
    if final_state == "Blue-1단기" and score_sum + wm_blue1_score >= 13:
        grade = "A+"
    elif final_state == "Blue-2스윙" and score_sum + wm_blue2_score >= 11:
        grade = "A"
    elif final_state == "Blue-2예비":
        grade = "B+"
    elif final_state == "후행수박":
        grade = "B"
    elif final_state in ("초입수박", "눌림수박"):
        grade = "C"
    else:
        grade = "없음"

    # ✅ 대형주 Blue-1 과열 보정
    blue_confirm = -1
    if final_state == "Blue-1단기" and _is_largecap_blue_guard_target(cur, row):
        blue_confirm = _largecap_blue_confirm_score(cur)
        if blue_confirm >= 4:
            pass
        elif blue_confirm >= 2:
            grade = _clamp_grade_down(grade, "A")
        else:
            grade = _clamp_grade_down(grade, "B+")

    detail = {
        "ok": True,
        "score_sum": score_sum,
        "ret7": round(cur["ret7"], 2),
        "ret15": round(cur["ret15"], 2),
        "ret20": round(cur["ret20"], 2),
        "pullback_pct25": round(cur["pullback_pct25"], 2),
        "box_range_pct25": round(cur["box_range_pct25"], 2),
        "max_day_up10": round(cur["max_day_up10"], 2),
        "intro_box_ready": bool(cur["intro_box_ready"]),
        "change_ready": bool(cur["change_ready"]),
        "red_state_raw": bool(cur["red_state_raw"]),
        "red_onset": bool(cur["red_onset"]),
        "blue1_onset": bool(cur["blue1_onset"]),
        "pullback_box": bool(cur["pullback_box"]),
        "red_state_raw_2": bool(cur["red_state_raw_2"]),
        "blue2_onset": bool(cur["blue2_onset"]),
        "blue2_strong": bool(cur.get("blue2_strong", False)),
        "blue2_preview": bool(cur.get("blue2_preview", False)),
        "blue_confirm": int(blue_confirm),
        "late": bool(cur["late"]),
        "intro_box_range_ok": bool(cur.get("intro_box_range_ok", False)),
        "intro_attack_band_ok": bool(cur.get("intro_attack_band_ok", False)),
        "intro_ret7_ok": bool(cur.get("intro_ret7_ok", False)),
        "intro_ret15_ok": bool(cur.get("intro_ret15_ok", False)),
        "intro_ret20_ok": bool(cur.get("intro_ret20_ok", False)),
        "intro_dayup_ok": bool(cur.get("intro_dayup_ok", False)),
        "intro_top_near_ok": bool(cur.get("intro_top_near_ok", False)),
        "intro_vol_calm_ok": bool(cur.get("intro_vol_calm_ok", False)),
        "intro_no_prior_blue1_ok": bool(cur.get("intro_no_prior_blue1_ok", False)),
        "intro_no_prior_blue2_ok": bool(cur.get("intro_no_prior_blue2_ok", False)),
        "intro_not_late_ok": bool(cur.get("intro_not_late_ok", False)),
    }

    return {
        "wm_base_score": int(cur["base_score"]),
        "wm_pocket_score": int(cur["pocket_score"]),
        "wm_attack_score": int(cur["attack_score"]),
        "wm_blue_score": int(wm_blue_score),
        "wm_blue1_score": int(wm_blue1_score),
        "wm_blue2_score": int(wm_blue2_score),
        "wm_pocket_raw": bool(cur["pocket_raw"]),
        "wm_pocket_hold": bool(cur["pocket_hold"]),
        "wm_attack_raw": bool(cur["attack_raw"]),
        "wm_attack_hold": bool(cur["attack_hold"]),
        "wm_blue_raw": bool(cur["blue1_raw"] or cur["blue2_raw"]),
        "wm_blue_hold": bool(cur["blue1_hold"] or cur["blue2_hold"]),
        "wm_final_state": final_state,
        "wm_intro": bool(final_state == "초입수박"),
        "wm_pullback": bool(final_state == "눌림수박"),
        "wm_late": bool(final_state == "후행수박"),
        "wm_blue1_raw": bool(cur["blue1_raw"]),
        "wm_blue1_hold": bool(final_state == "Blue-1단기"),
        "wm_blue2_raw": bool(cur["blue2_raw"]),
        "wm_blue2_hold": bool(final_state == "Blue-2스윙"),
        "wm_blue2_preview_hold": bool(final_state == "Blue-2예비"),
        "wm_state_green": bool(wm_state_green),
        "wm_state_red": bool(wm_state_red),
        "wm_state_blue": bool(wm_state_blue),
        "wm_state_name": state_name,
        "wm_state_grade": grade,
        "wm_debug_intro_box_ready": bool(cur["intro_box_ready"]),
        "wm_debug_change_ready": bool(cur["change_ready"]),
        "wm_debug_red_state_raw": bool(cur["red_state_raw"]),
        "wm_debug_red_onset": bool(cur["red_onset"]),
        "wm_debug_blue1_onset": bool(cur["blue1_onset"]),
        "wm_debug_pullback_box": bool(cur["pullback_box"]),
        "wm_debug_red_state_raw_2": bool(cur["red_state_raw_2"]),
        "wm_debug_blue2_onset": bool(cur["blue2_onset"]),
        "wm_debug_blue2_strong": bool(cur.get("blue2_strong", False)),
        "wm_debug_blue2_preview": bool(cur.get("blue2_preview", False)),
        "wm_debug_late": bool(cur["late"]),
        "wm_debug_red2_pullback_ok": bool(cur.get("red2_pullback_ok", False)),
        "wm_debug_red2_change_ok": bool(cur.get("red2_change_ok", False)),
        "wm_debug_red2_close_ma20_ok": bool(cur.get("red2_close_ma20_ok", False)),
        "wm_debug_red2_ma5_ma20_ok": bool(cur.get("red2_ma5_ma20_ok", False)),
        "wm_debug_red2_prevbox_ok": bool(cur.get("red2_prevbox_ok", False)),
        "wm_debug_red2_vol_ok": bool(cur.get("red2_vol_ok", False)),
        "wm_debug_red2_candle_ok": bool(cur.get("red2_candle_ok", False)),
        "wm_debug_red2_not_late_ok": bool(cur.get("red2_not_late_ok", False)),
        "wm_debug_red2_structure_ok": bool(cur.get("red2_structure_ok", False)),
        "wm_debug_red2_soft_ok": bool(cur.get("red2_soft_ok", False)),
        "wm_debug_blue2_prev_clear_ok": bool(cur.get("blue2_prev_clear_ok", False)),
        "wm_debug_blue2_context_ok": bool(cur.get("blue2_context_ok", False)),
        "wm_debug_blue2_vol2_ok": bool(cur.get("blue2_vol2_ok", False)),
        "wm_debug_intro_box_range_ok": bool(cur.get("intro_box_range_ok", False)),
        "wm_debug_intro_attack_band_ok": bool(cur.get("intro_attack_band_ok", False)),
        "wm_debug_intro_ret7_ok": bool(cur.get("intro_ret7_ok", False)),
        "wm_debug_intro_ret15_ok": bool(cur.get("intro_ret15_ok", False)),
        "wm_debug_intro_ret20_ok": bool(cur.get("intro_ret20_ok", False)),
        "wm_debug_intro_dayup_ok": bool(cur.get("intro_dayup_ok", False)),
        "wm_debug_intro_top_near_ok": bool(cur.get("intro_top_near_ok", False)),
        "wm_debug_intro_vol_calm_ok": bool(cur.get("intro_vol_calm_ok", False)),
        "wm_debug_intro_no_prior_blue1_ok": bool(cur.get("intro_no_prior_blue1_ok", False)),
        "wm_debug_intro_no_prior_blue2_ok": bool(cur.get("intro_no_prior_blue2_ok", False)),
        "wm_debug_intro_not_late_ok": bool(cur.get("intro_not_late_ok", False)),
        "wm_state_tags": tags,
        "wm_state_detail": detail,
    }

def build_watermelon_state_top5(df: pd.DataFrame):
    """
    전종목 스캔 결과에서 수박상태 / 단계 분류 / Blue-1 / Blue-2 / 단테 / 돌파 TOP5 추출
    반환:
      - green_df, red_df, blue_df
      - intro_df, pullback_df, late_df, blue1_df, blue2_df
      - dante_pick_df, dante_watch_df, dante_ex_df
      - breakout_pick_df, breakout_watch_df
    """
    if df is None or df.empty:
        empty = pd.DataFrame()
        return (
            empty, empty, empty,
            empty, empty, empty, empty, empty,
            empty, empty, empty,
            empty, empty,
        )

    work = df.copy()
    if "수박최종상태" not in work.columns:
        work["수박최종상태"] = work.get("수박상태명", "")

    def _sort(df_in: pd.DataFrame, cols: list, ascending=None) -> pd.DataFrame:
        if df_in is None or df_in.empty:
            return pd.DataFrame()
        sort_cols = [c for c in cols if c in df_in.columns]
        if not sort_cols:
            return df_in.head(5).reset_index(drop=True)
        if ascending is None:
            ascending = [False] * len(sort_cols)
        else:
            try:
                ascending = list(ascending)
            except Exception:
                ascending = [False] * len(sort_cols)
            if len(ascending) < len(sort_cols):
                ascending = ascending + [False] * (len(sort_cols) - len(ascending))
            elif len(ascending) > len(sort_cols):
                ascending = ascending[:len(sort_cols)]
        return df_in.sort_values(by=sort_cols, ascending=ascending).head(5).reset_index(drop=True)

    intro_df = _sort(work[work["수박최종상태"] == "초입수박"].copy(), ["수박포켓점수", "수박기반점수", "안전점수", "N점수"])
    pullback_df = _sort(work[work["수박최종상태"] == "눌림수박"].copy(), ["수박포켓점수", "수박기반점수", "안전점수", "N점수"])
    late_df = _sort(work[work["수박최종상태"] == "후행수박"].copy(), ["수박공격점수", "수박기반점수", "N점수"])
    blue1_df = _sort(work[work["수박최종상태"] == "Blue-1단기"].copy(), ["파란점선1단기점수", "수박공격점수", "안전점수", "N점수"])
    blue2_df = _sort(work[work["수박최종상태"] == "Blue-2스윙"].copy(), ["파란점선2스윙점수", "수박포켓점수", "안전점수", "N점수"])

    dante_work = compute_dante_mode_fields(work)
    breakout_work = compute_breakout_mode_fields(dante_work)
    preempt_state_col = "선취상태" if "선취상태" in dante_work.columns else "단테상태"
    preempt_score_col = "선취점수" if "선취점수" in dante_work.columns else "단테점수"
    dante_pick_df = _sort(dante_work[dante_work[preempt_state_col] == "선취형"].copy(), [preempt_score_col, "수박포켓점수", "안전점수", "N점수"])
    dante_watch_df = _sort(dante_work[dante_work[preempt_state_col] == "선취관찰형"].copy(), [preempt_score_col, "수박포켓점수", "안전점수", "N점수"])
    dante_ex_df = _sort(dante_work[dante_work[preempt_state_col] == "선취제외형"].copy(), [preempt_score_col, "수박포켓점수", "안전점수", "N점수"], ascending=[True, False, False, False])
    breakout_pick_df = _sort(breakout_work[breakout_work["돌파상태"] == "흰구름돌파형"].copy(), ["돌파점수", "수박공격점수", "안전점수", "N점수"])
    breakout_watch_df = _sort(breakout_work[breakout_work["돌파상태"] == "돌파관찰형"].copy(), ["돌파점수", "수박공격점수", "안전점수", "N점수"])

    green_df = _sort(work[work["수박최종상태"].isin(["초입수박", "눌림수박"])].copy(), ["수박포켓점수", "수박기반점수", "안전점수", "N점수"])
    red_df = _sort(work[work["수박최종상태"].isin(["Blue-1단기", "Blue-2스윙", "Blue-2예비"])].copy(), ["수박파란점선점수", "수박공격점수", "안전점수", "N점수"])
    blue_df = _sort(work[work["수박최종상태"].isin(["Blue-1단기", "Blue-2스윙"])].copy(), ["수박파란점선점수", "수박공격점수", "안전점수", "N점수"])

    return (
        green_df, red_df, blue_df,
        intro_df, pullback_df, late_df, blue1_df, blue2_df,
        dante_pick_df, dante_watch_df, dante_ex_df,
        breakout_pick_df, breakout_watch_df,
    )

def _ma5r_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def _ma5r_upper_wick_pct(open_p, high_p, low_p, close_p):
    try:
        body_high = max(open_p, close_p)
        body = max(abs(close_p - open_p), 1e-6)
        upper = max(high_p - body_high, 0.0)
        return (upper / body) * 100.0
    except Exception:
        return 999.0

def detect_refined_watermelon_filter(
    df: pd.DataFrame,
    i: int,
    white_cloud_bundle: Optional[dict] = None,
    ma5_reclaim_bundle: Optional[dict] = None,
) -> dict:
    """
    좋은 수박 / 가짜 수박을 가르는 보조 필터
    핵심:
    - 장기이평에서 너무 멀리 이탈하지 않았는가
    - 거래량/재안착/캔들 질이 받쳐주는가
    - 흰구름(장기 저항대) 위치가 과열 구간은 아닌가
    """
    empty = {
        "ok": False,
        "weak": False,
        "caution": False,
        "score": 0,
        "tag": "",
        "comment": "",
        "vol_ok": False,
        "reclaim_ok": False,
        "candle_ok": False,
        "wick_ok": False,
        "long_ok": False,
        "cloud_ok": False,
        "obv_ok": False,
        "hard_fail": False,
    }
    try:
        if df is None or df.empty or i < 0 or i >= len(df):
            return empty.copy()

        row = df.iloc[i]
        prev = df.iloc[i - 1] if i >= 1 else row

        close_p = _ma5r_float(row.get("Close", row.get("close", 0)))
        open_p = _ma5r_float(row.get("Open", row.get("open", 0)))
        high_p = _ma5r_float(row.get("High", row.get("high", 0)))
        vol = _ma5r_float(row.get("Volume", row.get("volume", 0)))
        prev_close = _ma5r_float(prev.get("Close", prev.get("close", 0)))
        prev_vol = _ma5r_float(prev.get("Volume", prev.get("volume", 0)))

        ma5 = _ma5r_float(row.get("MA5", row.get("ma5", 0)))
        ma20 = _ma5r_float(row.get("MA20", row.get("ma20", 0)))
        ma112 = _ma5r_float(row.get("MA112", row.get("ma112", 0)))
        ma224 = _ma5r_float(row.get("MA224", row.get("ma224", 0)))
        vma20 = _ma5r_float(row.get("VMA20", row.get("vol_ma20", 0)))

        wick_pct = _ma5r_upper_wick_pct(open_p, high_p, _ma5r_float(row.get("Low", row.get("low", 0))), close_p)

        obv = _ma5r_float(row.get("OBV", row.get("obv", 0)))
        prev_obv = _ma5r_float(prev.get("OBV", prev.get("obv", 0)))
        obv_slope = _ma5r_float(
            row.get(
                "OBV_Slope",
                row.get(
                    "OBV_SLOPE_5",
                    row.get("obv_slope", row.get("OBV_SLOPE_10", 0)),
                ),
            ),
            0.0,
        )

        long_refs = [x for x in [ma112, ma224] if x > 0]
        long_floor = min(long_refs) if long_refs else 0.0
        long_ceiling = max(long_refs) if long_refs else 0.0

        near_long_ok = True if long_floor <= 0 else (close_p >= long_floor * 0.84)
        not_too_high = True if long_ceiling <= 0 else (close_p <= long_ceiling * 1.10)
        long_ok = bool(near_long_ok and not_too_high)

        vol_ok = bool(
            (vma20 > 0 and vol >= vma20 * 0.85)
            or (prev_vol > 0 and vol >= prev_vol * 0.90)
        )

        reclaim_ok = bool(
            (ma5_reclaim_bundle or {}).get("ok", False)
            or (ma5_reclaim_bundle or {}).get("reclaim", False)
            or ((ma5 > 0) and (close_p >= ma5 * 0.995))
            or ((ma20 > 0) and (close_p >= ma20 * 0.985))
        )

        candle_ok = bool((close_p >= open_p * 0.992) or (close_p >= prev_close))
        wick_ok = bool(wick_pct <= 65.0)
        obv_ok = bool((obv_slope >= -3.0) or (prev_obv <= 0) or (obv >= prev_obv * 0.995))

        wc = white_cloud_bundle or {}
        wc_state = str(wc.get("state", "") or "").strip()
        wc_near_n = int(wc.get("near_n", 0) or 0)
        cloud_ok = bool(
            (wc_state == "below" and wc_near_n >= 1)
            or (wc_state == "inside")
            or (wc_state == "mixed" and int(wc.get("below_n", 0) or 0) >= 1 and int(wc.get("inside_n", 0) or 0) >= 1)
        )

        hard_fail = bool(
            (long_floor > 0 and close_p < long_floor * 0.75)
            or (vma20 > 0 and vol < vma20 * 0.45)
            or (wick_pct >= 140.0)
            or (ma20 > 0 and close_p < ma20 * 0.88)
        )

        score = sum([
            1 if vol_ok else 0,
            1 if reclaim_ok else 0,
            1 if candle_ok else 0,
            1 if wick_ok else 0,
            1 if long_ok else 0,
            1 if cloud_ok else 0,
            1 if obv_ok else 0,
        ])

        ok = bool((not hard_fail) and score >= 5 and vol_ok and long_ok)
        weak = bool((not ok) and (not hard_fail) and score >= 3)
        caution = bool(
            hard_fail
            or score <= 2
            or ((not vol_ok) and (not reclaim_ok))
            or (wc_state == "above" and score < 6)
        )

        tag = ""
        comment = ""
        if ok:
            tag = "✅정제수박"
            comment = "거래량·재안착·장기저항 위치가 양호"
        elif weak:
            tag = "🟨관찰수박"
            comment = "수박 구조는 있으나 한두 가지 확인이 필요"
        elif caution:
            tag = "⚠️가짜수박주의"
            comment = "거래량·윗꼬리·장기이평 위치 중 약한 요소 존재"

        return {
            "ok": ok,
            "weak": weak,
            "caution": caution,
            "score": int(score),
            "tag": tag,
            "comment": comment,
            "vol_ok": bool(vol_ok),
            "reclaim_ok": bool(reclaim_ok),
            "candle_ok": bool(candle_ok),
            "wick_ok": bool(wick_ok),
            "long_ok": bool(long_ok),
            "cloud_ok": bool(cloud_ok),
            "obv_ok": bool(obv_ok),
            "hard_fail": bool(hard_fail),
        }
    except Exception:
        return empty.copy()

def _wm_safe_series(df: pd.DataFrame, col: str, default=0.0) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors='coerce')
    return pd.Series(default, index=df.index, dtype='float64')

def integrate_watermelon_v2_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    ✅ Watermelon V3
    준비형 / 1차 발사형 / 재발사형 분리
    기존 컬럼과의 호환 유지:
      - Watermelon_Prepare
      - Watermelon_Launch
      - Watermelon_Signal / Watermelon_Signal_Refined
      - Supply_Turn_Signal / Supply_Turn_Prepare
      - Watermelon_Color / Watermelon_Score / Watermelon_Fire
      - WATERMELON_GREEN_SCORE / WATERMELON_RED_SCORE / WATERMELON_QUALITY
    """
    out = df.copy()

    close = _wm_safe_series(out, 'Close')
    open_ = _wm_safe_series(out, 'Open')
    high = _wm_safe_series(out, 'High')
    low = _wm_safe_series(out, 'Low')
    volume = _wm_safe_series(out, 'Volume')

    ma5 = _wm_safe_series(out, 'MA5')
    ma20 = _wm_safe_series(out, 'MA20')
    ma40 = _wm_safe_series(out, 'MA40')
    bb40_width = _wm_safe_series(out, 'BB40_Width')
    disparity = _wm_safe_series(out, 'Disparity', 100.0)
    vma20 = _wm_safe_series(out, 'VMA20')
    mfi = _wm_safe_series(out, 'MFI')
    obv = _wm_safe_series(out, 'OBV')

    # OBV 기울기 보강
    if 'OBV_SLOPE_10' not in out.columns:
        vol10 = volume.rolling(10).sum().replace(0, np.nan)
        out['OBV_SLOPE_10'] = ((obv.diff(10) / vol10) * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round(2)
    obv_slope_10 = _wm_safe_series(out, 'OBV_SLOPE_10')

    turnover_eok = (close * volume) / 100_000_000

    # 종가 품질
    body_top = pd.concat([open_, close], axis=1).max(axis=1)
    total_range = (high - low).replace(0, np.nan)
    upper_wick_total = (high - body_top).clip(lower=0)
    upper_wick_total_pct = (upper_wick_total / total_range * 100).replace([np.inf, -np.inf], np.nan).fillna(100)
    out['UPPER_WICK_TOTAL_PCT'] = upper_wick_total_pct.round(1)
    out['Upper_Wick_Total_Pct'] = out['UPPER_WICK_TOTAL_PCT']

    # 최근 고점 / MA5 기울기
    out['RecentHigh3'] = high.rolling(3).max()
    out['RecentHigh5'] = high.rolling(5).max()
    out['MA5_Slope_Pos'] = (ma5 >= ma5.shift(1))

    # 준비형 점수
    wm_green_score = (
        (bb40_width <= 18).astype(int) * 20 +
        (close >= ma20 * 0.98).astype(int) * 15 +
        (close >= ma40 * 0.96).astype(int) * 10 +
        ((disparity >= 97) & (disparity <= 108)).astype(int) * 15 +
        (obv_slope_10 > 0).astype(int) * 20 +
        (mfi >= 45).astype(int) * 10 +
        (turnover_eok >= 30).astype(int) * 10
    ).fillna(0)

    # 발사형 점수
    recent3_break = ((close >= out['RecentHigh3'].shift(1)) | (out['MA5_Slope_Pos'] & (close >= ma20))).fillna(False)
    wm_red_score = (
        (close >= ma20).astype(int) * 15 +
        (close >= ma40).astype(int) * 15 +
        (close >= open_).astype(int) * 10 +
        (volume >= vma20 * 1.0).astype(int) * 15 +
        (out['UPPER_WICK_TOTAL_PCT'] <= 35).astype(int) * 10 +
        ((disparity >= 98) & (disparity <= 110)).astype(int) * 10 +
        recent3_break.astype(int) * 15 +
        (obv_slope_10 > 0).astype(int) * 10
    ).fillna(0)

    out['WATERMELON_GREEN_SCORE'] = wm_green_score.astype(int)
    out['WATERMELON_RED_SCORE'] = wm_red_score.astype(int)
    out['WATERMELON_QUALITY'] = (wm_green_score * 0.45 + wm_red_score * 0.55).round(1)

    # 준비형
    out['Watermelon_Prepare'] = (
        (out['WATERMELON_GREEN_SCORE'] >= 65) &
        (bb40_width <= 18) &
        (obv_slope_10 > 0)
    ).astype(bool)

    # 최근 10봉 내 준비형 존재
    out['Watermelon_Prepare_Recent'] = (
        out['Watermelon_Prepare'].rolling(10, min_periods=1).max().shift(1).fillna(0) >= 1
    ).astype(bool)

    # 1차 발사형
    out['Watermelon_First_Launch'] = (
        out['Watermelon_Prepare_Recent'] &
        (out['WATERMELON_RED_SCORE'] >= 70) &
        (out['UPPER_WICK_TOTAL_PCT'] <= 35) &
        (bb40_width <= 20)
    ).astype(bool)

    # 과거 발사 이력
    out['Watermelon_Old_Signal'] = (
        out['Watermelon_First_Launch'].rolling(15, min_periods=1).max().shift(1).fillna(0) >= 1
    ).astype(bool)

    # 눌림 발생
    pullback_happened = (
        (close < ma20) |
        (volume < vma20) |
        (disparity < 100)
    ).fillna(False)
    out['Watermelon_Pullback_Recent'] = (
        pullback_happened.rolling(10, min_periods=1).max().shift(1).fillna(0) >= 1
    ).astype(bool)

    # 재발사형
    out['Watermelon_Relaunch'] = (
        out['Watermelon_Old_Signal'] &
        out['Watermelon_Pullback_Recent'] &
        (out['WATERMELON_RED_SCORE'] >= 62) &
        (close >= ma20) &
        (close >= open_) &
        (obv_slope_10 > 0)
    ).astype(bool)

    # 최종 발사형
    out['Watermelon_Launch'] = (out['Watermelon_First_Launch'] | out['Watermelon_Relaunch']).astype(bool)

    # 레거시 호환
    out['Watermelon_Signal_Refined'] = out['Watermelon_Launch']
    out['Watermelon_Signal'] = out['Watermelon_Launch']

    # 예전 컬럼 호환
    out['WATERMELON_GREEN'] = out['Watermelon_Prepare'].astype(int)
    out['WATERMELON_RED'] = out['Watermelon_Launch'].astype(int)
    out['WATERMELON_GREEN_NEW'] = (
        out['Watermelon_Prepare'] &
        (~out['Watermelon_Prepare'].shift(1).fillna(False))
    ).astype(int)
    out['WATERMELON_RED_NEW'] = (
        out['Watermelon_Launch'] &
        (~out['Watermelon_Launch'].shift(1).fillna(False))
    ).astype(int)

    green_days = []
    red_days = []
    g_cnt = 0
    r_cnt = 0
    for g, r in zip(out['WATERMELON_GREEN'].tolist(), out['WATERMELON_RED'].tolist()):
        g_cnt = g_cnt + 1 if int(g) == 1 else 0
        r_cnt = r_cnt + 1 if int(r) == 1 else 0
        green_days.append(g_cnt)
        red_days.append(r_cnt)
    out['WATERMELON_GREEN_DAYS'] = green_days
    out['WATERMELON_RED_DAYS'] = red_days

    out['Supply_Turn_Prepare'] = out['Watermelon_Prepare_Recent']
    out['Supply_Turn_Signal'] = out['Watermelon_Launch']

    out['Watermelon_Color'] = np.where(
        out['Watermelon_Launch'], 'red',
        np.where(out['Watermelon_Prepare'], 'green', 'none')
    )

    out['Watermelon_Score'] = np.where(
        out['Watermelon_Launch'], 3,
        np.where(out['Watermelon_Prepare'], 2, 0)
    )

    out['Green_Days_10'] = (out['Watermelon_Color'].shift(1) == 'green').rolling(10).sum().fillna(0).astype(int)

    out['Watermelon_Green'] = out['Watermelon_Prepare']
    out['Watermelon_Red'] = out['Watermelon_Launch']
    out['Watermelon_Red2'] = (close >= open_) & out['Watermelon_Launch']

    out['VWMA40'] = (close * volume).rolling(40).mean() / volume.rolling(40).mean()
    out['Vol_Accel'] = volume / volume.rolling(5).mean()
    out['Watermelon_Fire'] = out['WATERMELON_QUALITY'].round(2)

    return out

def watermelon_signal_snapshot(row: pd.Series) -> dict:
    green = bool(row.get('Watermelon_Prepare', False))
    red = bool(row.get('Watermelon_Launch', False))
    red_new = bool(int(row.get('WATERMELON_RED_NEW', 0)))
    green_new = bool(int(row.get('WATERMELON_GREEN_NEW', 0)))

    green_score = int(row.get('WATERMELON_GREEN_SCORE', 0) or 0)
    red_score = int(row.get('WATERMELON_RED_SCORE', 0) or 0)
    quality = float(row.get('WATERMELON_QUALITY', 0) or 0)
    green_days = int(row.get('WATERMELON_GREEN_DAYS', 0) or 0)
    red_days = int(row.get('WATERMELON_RED_DAYS', 0) or 0)

    first_launch = bool(row.get('Watermelon_First_Launch', False))
    relaunch = bool(row.get('Watermelon_Relaunch', False))
    prepare_recent = bool(row.get('Watermelon_Prepare_Recent', False))

    tags = []
    score_bonus = 0
    phase = '없음'

    if green and not red:
        phase = '준비형'
        tags.append(f'🍈수박준비형(G{green_score})')
        score_bonus += 20 + min(max(green_score - 60, 0), 20)
        if green_new:
            tags.append('🍈초록신규')
            score_bonus += 8
        if green_days >= 3:
            tags.append(f'🍈초록{green_days}일')
            score_bonus += min(green_days * 2, 12)

    if first_launch:
        phase = '1차발사형'
        tags.append(f'🍉1차발사형(R{red_score})')
        score_bonus += 90 + min(max(red_score - 70, 0), 20)
        if red_new:
            tags.append('🍉빨강신규점등')
            score_bonus += 25

    elif relaunch:
        phase = '재발사형'
        tags.append(f'🍉재발사형(R{red_score})')
        score_bonus += 70 + min(max(red_score - 62, 0), 18)

    elif red:
        phase = '발사형'
        tags.append(f'🍉수박발사형(R{red_score})')
        score_bonus += 50 + min(red_score * 3, 18)

    if quality >= 80:
        tags.append(f'🏆수박품질{quality:.1f}')
        score_bonus += 20
    elif quality >= 65:
        tags.append(f'✅수박품질{quality:.1f}')
        score_bonus += 10

    if prepare_recent and first_launch:
        tags.append('📦준비후점화')
        score_bonus += 12

    return {
        'green': green,
        'red': red,
        'red_new': red_new,
        'green_new': green_new,
        'phase': phase,
        'green_score': green_score,
        'red_score': red_score,
        'quality': quality,
        'green_days': green_days,
        'red_days': red_days,
        'first_launch': first_launch,
        'relaunch': relaunch,
        'prepare_recent': prepare_recent,
        'score_bonus': int(score_bonus),
        'tags': tags,
    }

def check_watermelon_relaunch(curr: pd.Series, past: pd.DataFrame):
    """
    수박 눌림 재폭발:
    - 최근 수박 시그널 이력 존재
    - 중간 눌림 구간 존재
    - 현재 다시 빨강/거래량/상승 압력 재개
    """
    if past.empty or len(past) < 15:
        return False, "데이터 부족"

    had_watermelon = past['Watermelon_Signal'].tail(15).any()

    pullback_happened = (
        (past['Close'] < past['MA20']).tail(10).any() or
        (past['Volume'] < past['Vol_Avg']).tail(10).any()
    )

    relaunch = (
        (curr['Watermelon_Color'] == 'red') and
        (curr['Volume'] > curr['Vol_Avg'] * 1.2) and
        (curr['Close'] >= curr['Open'])
    )

    obv_hold = curr['OBV_Rising']
    passed = had_watermelon and pullback_happened and relaunch and obv_hold
    return passed, f"기존수박:{had_watermelon}, 눌림:{pullback_happened}, 재시동:{relaunch}, OBV유지:{obv_hold}"

def build_watermelon_debug_block(title: str, df: pd.DataFrame) -> str:
    """
    수박/파란점 상태 분류가 왜 통과/실패했는지 디버그용으로 보여준다.
    누락 컬럼이 있어도 죽지 않도록 안전하게 0/빈값으로 처리한다.
    """
    if df is None or df.empty:
        return f"🔬 [{title}]\n- 해당 종목 없음\n"

    def _b(row, col: str, default: int = 0) -> int:
        try:
            v = row.get(col, default)
            if pd.isna(v):
                return default
            if isinstance(v, (bool, np.bool_)):
                return int(bool(v))
            return int(v)
        except Exception:
            try:
                return int(bool(row.get(col, default)))
            except Exception:
                return default

    lines = [f"🔬 [{title}]\n"]
    for rank, (_, row) in enumerate(df.iterrows(), 1):
        name = str(row.get('종목명', ''))
        code = str(row.get('code', ''))
        state = str(row.get('수박최종상태', row.get('수박상태명', '')))

        gate_parts = [
            f"intro_box={_b(row, '수박디버그_intro_box')}",
            f"change={_b(row, '수박디버그_change')}",
            f"red_raw={_b(row, '수박디버그_red_raw')}",
            f"red_onset={_b(row, '수박디버그_red_onset')}",
            f"blue1_onset={_b(row, '수박디버그_blue1_onset')}",
            f"pullback_box={_b(row, '수박디버그_pullback_box')}",
            f"red2_raw={_b(row, '수박디버그_red2_raw')}",
            f"blue2_onset={_b(row, '수박디버그_blue2_onset')}",
            f"late={_b(row, '수박디버그_late')}",
            f"blue_confirm={_b(row, 'blue_confirm', -1)}",
        ]

        intro_sub_parts = [
            f"range={_b(row, '수박디버그_box_range_ok')}",
            f"attack_band={_b(row, '수박디버그_attack_band_ok')}",
            f"ret7={_b(row, '수박디버그_ret7_ok')}",
            f"ret15={_b(row, '수박디버그_ret15_ok')}",
            f"ret20={_b(row, '수박디버그_ret20_ok')}",
            f"dayup={_b(row, '수박디버그_dayup_ok')}",
            f"top_near={_b(row, '수박디버그_top_near_ok')}",
            f"vol_calm={_b(row, '수박디버그_vol_calm_ok')}",
            f"no_blue1={_b(row, '수박디버그_no_blue1_ok')}",
            f"no_blue2={_b(row, '수박디버그_no_blue2_ok')}",
            f"not_late={_b(row, '수박디버그_not_late_ok')}",
        ]

        red2_sub_parts = [
            f"pb={_b(row, '수박디버그_red2_pullback_ok')}",
            f"chg={_b(row, '수박디버그_red2_change_ok')}",
            f"c20={_b(row, '수박디버그_red2_close_ma20_ok')}",
            f"m520={_b(row, '수박디버그_red2_ma5_ma20_ok')}",
            f"pbox={_b(row, '수박디버그_red2_prevbox_ok')}",
            f"vol={_b(row, '수박디버그_red2_vol_ok')}",
            f"candle={_b(row, '수박디버그_red2_candle_ok')}",
            f"not_late={_b(row, '수박디버그_red2_not_late_ok')}",
            f"struct={_b(row, '수박디버그_red2_structure_ok')}",
            f"soft={_b(row, '수박디버그_red2_soft_ok')}",
            f"strong={_b(row, '수박디버그_blue2_strong')}",
            f"preview={_b(row, '수박디버그_blue2_preview')}",
            f"prev_clear={_b(row, '수박디버그_blue2_prev_clear_ok')}",
            f"ctx={_b(row, '수박디버그_blue2_context_ok')}",
            f"vol2={_b(row, '수박디버그_blue2_vol2_ok')}",
        ]

        lines.append(
            f"{rank}) {name}({code})\n"
            f"- 최종상태: {state}\n"
            f"- gate: {' / '.join(gate_parts)}\n"
            f"- intro_sub: {' / '.join(intro_sub_parts)}\n"
            f"- red2_sub: {' / '.join(red2_sub_parts)}\n"
        )

    return "\n".join(lines)
