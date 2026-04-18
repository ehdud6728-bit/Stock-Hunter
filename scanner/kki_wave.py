from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .indicators import (
    enrich_indicators,
    recent_box,
    regression_angle,
    build_band_snapshot,
)
from .watermelon_engine import normalize_meta, decide_action, build_easy_context


PATTERN_WEIGHTS = {
    "장대양봉→눌림→재발사형": 1.35,
    "장대양봉→횡보→재발사형": 1.25,
    "연속장대양봉형": 1.05,
    "상단터치→눌림→2차상승형": 1.15,
    "하단터치반등형": 0.95,
    "수축후확장형": 1.10,
}


@dataclass
class KkiProfile:
    kki_score: int
    absorb_score: int
    pattern_name: str
    best_band: str
    support_band: str
    recurrence_score: int
    recurrence_count: int
    rebound_count: int
    range_relaunch_count: int
    current_position: str
    current_state_comment: str
    strengths: List[str]
    cautions: List[str]
    supply_axis: str
    absorb_comment: str
    action_axis: str
    show_block: bool
    habit_comment: str = ""
    band_comment: str = ""
    position_comment: str = ""
    integrated_comment: str = ""


@dataclass
class WaveProfile:
    small_zone: str
    medium_zone: str
    small_box_low: float
    small_box_mid: float
    small_box_high: float
    medium_box_low: float
    medium_box_mid: float
    medium_box_high: float
    small_angle: float
    medium_angle: float
    small_angle_label: str
    medium_angle_label: str
    wave_comment: str
    small_zone_comment: str = ""
    medium_zone_comment: str = ""
    angle_comment: str = ""
    combo_comment: str = ""


@dataclass
class CombinedAnalysis:
    kki: KkiProfile
    wave: WaveProfile
    final_action: str
    easy_commentary: str


def _safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _event_columns(band_name: str) -> Tuple[str, str, str]:
    b = band_name.lower()
    if b == "bb20":
        return "bb20_lower_touch", "bb20_upper_touch", "bb20_squeeze"
    if b == "bb40":
        return "bb40_lower_touch", "bb40_upper_touch", "bb40_squeeze"
    if b == "env20":
        return "env20_lower_touch", "env20_upper_touch", "env20_squeeze"
    if b == "env40":
        return "env40_lower_touch", "env40_upper_touch", "env40_squeeze"
    raise KeyError(band_name)


def _find_pattern_occurrences(df: pd.DataFrame, band_name: str) -> Dict[str, int]:
    lower_touch_col, upper_touch_col, squeeze_col = _event_columns(band_name)
    close = df["Close"].reset_index(drop=True)
    high = df["High"].reset_index(drop=True)

    out = {
        "장대양봉→눌림→재발사형": 0,
        "장대양봉→횡보→재발사형": 0,
        "연속장대양봉형": 0,
        "상단터치→눌림→2차상승형": 0,
        "하단터치반등형": 0,
        "수축후확장형": 0,
    }

    if len(df) < 80:
        return out

    impulse_idx = list(df.index[df["impulse_day"].fillna(False)])
    impulse_pos = {idx: pos for pos, idx in enumerate(df.index)}

    # 장대양봉 기반 패턴
    for idx in impulse_idx:
        i = impulse_pos[idx]
        if i >= len(df) - 10:
            continue
        base_close = float(close.iloc[i])
        if base_close <= 0:
            continue
        post = close.iloc[i + 1 : i + 25]
        if post.empty:
            continue
        drawdown = (post.min() / base_close - 1.0) * 100.0
        rehit = high.iloc[i + 3 : i + 35].max()
        post_range = close.iloc[i + 1 : i + 12]
        compress = (post_range.max() / post_range.min() - 1.0) * 100.0 if len(post_range) >= 3 else np.nan

        if -18.0 <= drawdown <= -3.5 and rehit >= base_close * 1.04:
            out["장대양봉→눌림→재발사형"] += 1
        if -8.0 <= drawdown <= -1.0 and compress <= 7.5 and rehit >= base_close * 1.03:
            out["장대양봉→횡보→재발사형"] += 1

    # 연속 장대양봉형
    impulse_positions = [impulse_pos[idx] for idx in impulse_idx]
    for p1, p2 in zip(impulse_positions, impulse_positions[1:]):
        if 1 <= p2 - p1 <= 6:
            out["연속장대양봉형"] += 1

    # 밴드 기반 패턴
    lower_touch = df[lower_touch_col].fillna(False).to_numpy()
    upper_touch = df[upper_touch_col].fillna(False).to_numpy()
    squeeze = df[squeeze_col].fillna(False).to_numpy()

    for i in range(20, len(df) - 10):
        if lower_touch[i]:
            rebound = close.iloc[i + 1 : i + 12].max()
            if rebound >= close.iloc[i] * 1.06:
                out["하단터치반등형"] += 1
        if upper_touch[i]:
            pull = close.iloc[i + 1 : i + 10].min()
            reup = high.iloc[i + 4 : i + 20].max()
            if pull <= close.iloc[i] * 0.96 and reup >= high.iloc[i] * 0.99:
                out["상단터치→눌림→2차상승형"] += 1
        if squeeze[i]:
            future = high.iloc[i + 1 : i + 15].max()
            if future >= close.iloc[i] * 1.07:
                out["수축후확장형"] += 1
    return out


def _current_position(df: pd.DataFrame, best_band: str) -> Tuple[str, str]:
    lower_touch_col, upper_touch_col, squeeze_col = _event_columns(best_band)
    row = df.iloc[-1]
    close = float(row["Close"])
    recent_high = float(df["High"].tail(15).max())
    recent_low = float(df["Low"].tail(15).min())
    last_impulse_gap = np.where(df["impulse_day"].fillna(False).to_numpy())[0]
    last_impulse_idx = int(last_impulse_gap[-1]) if len(last_impulse_gap) else -999
    dist_from_high = (close / recent_high - 1.0) * 100.0 if recent_high > 0 else 0.0

    if len(df) - 1 - last_impulse_idx <= 2 and row["impulse_day"]:
        return "장대직후", "직전 장대양봉 직후라 추가 확인 없는 추격은 부담스럽습니다."
    if bool(row[upper_touch_col]) and float(row.get("vol_ratio20", 0) or 0) >= 1.5:
        return "재발사 확인", "상단 재확장과 거래량 동반이 보여 재발사 확인 구간으로 볼 수 있습니다."
    if -9.0 <= dist_from_high <= -2.0 and close >= float(row.get("ma20") or 0):
        return "눌림 구간", "직전 발사 뒤 눌림을 소화하는 단계라 2차 발사 대기 구간에 가깝습니다."
    if bool(row[squeeze_col]):
        return "횡보 압축", "발사 직전 에너지를 모으는 압축 구간으로 볼 수 있습니다."
    if close >= float(row.get("ma5") or 0) and close >= float(df["Close"].tail(5).min()):
        return "재안착", "5일선/단기 구조를 다시 회복하는 재안착 구간으로 해석할 수 있습니다."
    if recent_low > 0 and close <= recent_low * 1.03:
        return "눌림 심화", "눌림 폭이 커져 2차 발사보다는 구조 복원이 우선입니다."
    return "중립", "과거 끼 패턴은 있으나 현재 위치는 중립에 가깝습니다."


def _score_band(df: pd.DataFrame, band_name: str) -> Tuple[float, Dict[str, int], str, str]:
    pattern_counts = _find_pattern_occurrences(df, band_name)
    weighted = sum(pattern_counts[name] * PATTERN_WEIGHTS[name] for name in pattern_counts)
    recurrence_count = sum(pattern_counts.values())
    current_position, pos_comment = _current_position(df, band_name)

    bonus = 0.0
    if current_position == "눌림 구간":
        bonus += 8.0
    elif current_position == "재안착":
        bonus += 6.0
    elif current_position == "횡보 압축":
        bonus += 5.0
    elif current_position == "재발사 확인":
        bonus += 9.0
    elif current_position == "장대직후":
        bonus += 2.0
    elif current_position == "눌림 심화":
        bonus -= 5.0

    score = weighted * 8.0 + min(recurrence_count * 2.0, 20.0) + bonus
    return score, pattern_counts, current_position, pos_comment


def _pick_best_band(df: pd.DataFrame) -> Tuple[str, str, Dict[str, int], int, str, str]:
    candidates = ["BB20", "BB40", "Env20", "Env40"]
    scored = []
    for band in candidates:
        score, counts, pos, pos_comment = _score_band(df, band)
        scored.append((band, score, counts, pos, pos_comment))
    scored.sort(key=lambda x: x[1], reverse=True)
    best = scored[0]
    support = scored[1][0] if len(scored) > 1 else best[0]
    pattern_name = max(best[2].items(), key=lambda kv: (kv[1] * PATTERN_WEIGHTS[kv[0]], kv[1]))[0]
    recurrence_score = int(round(min(best[1], 100)))
    return best[0], support, best[2], recurrence_score, best[3], best[4]


def _absorption(meta: Dict) -> Tuple[int, str, str]:
    personal_5 = _safe_float(meta.get("개인5일") or meta.get("personal_5d") or meta.get("개인_5일"))
    foreign_5 = _safe_float(meta.get("외인5일") or meta.get("foreign_5d") or meta.get("외인_5일"))
    inst_5 = _safe_float(meta.get("기관5일") or meta.get("inst_5d") or meta.get("기관_5일"))
    supply_label = str(meta.get("수급") or meta.get("수급라벨") or meta.get("supply_label") or "").strip()

    score = 0.0
    comment = "뚜렷한 투매 흡수 흔적은 아직 약합니다"
    supply_axis = "혼조수급"

    if personal_5 < 0 and (foreign_5 > 0 or inst_5 > 0):
        score = min(abs(personal_5) + max(foreign_5, 0) + max(inst_5, 0), 80.0)
        if foreign_5 >= inst_5 and foreign_5 > 0:
            supply_axis = f"외인주도(5일 외인 {foreign_5:+.1f}억)"
        elif inst_5 > 0:
            supply_axis = f"기관주도(5일 기관 {inst_5:+.1f}억)"
        else:
            supply_axis = f"쌍끌흡수(외인 {foreign_5:+.1f}억 / 기관 {inst_5:+.1f}억)"
        comment = "최근 5일 개인 물량을 부분적으로 흡수하는 구간입니다"
    elif personal_5 > 0 and foreign_5 < 0 and inst_5 <= 0:
        score = 8.0
        supply_axis = "개인추격우세"
        comment = "개인 추격 성격이 더 강해 흡수 시그널로 보기엔 약합니다"
    elif supply_label:
        supply_axis = supply_label
        score = 18.0

    return int(round(max(0.0, min(score, 100.0)))), supply_axis, comment


def _strengths_and_cautions(df: pd.DataFrame, meta: Dict, band_name: str, recurrence_score: int, current_position: str) -> Tuple[List[str], List[str]]:
    row = df.iloc[-1]
    strengths: List[str] = []
    cautions: List[str] = []

    if recurrence_score >= 70:
        strengths.append("과거재현강")
    if current_position in ("눌림 구간", "재안착"):
        strengths.append("현재눌림" if current_position == "눌림 구간" else "재안착")
    if current_position == "횡보 압축":
        strengths.append("압축구간")
    if float(row.get("obv_slope5") or 0) > 0:
        strengths.append("OBV매집")
    if float(row.get("vol_ratio20") or 0) >= 1.8:
        strengths.append("발사력")
    if float(row.get("disparity20") or 0) <= 106:
        strengths.append("포켓")
    if band_name in ("BB20", "BB40") and bool(row.get(f"{band_name.lower()}_squeeze") if f"{band_name.lower()}_squeeze" in row.index else False):
        strengths.append("밴드수축")
    if not strengths:
        strengths.append("기반")

    refine = str(meta.get("정제") or meta.get("refine_label") or "")
    if "가짜" in refine:
        cautions.append("정제주의")
    if float(row.get("upper_wick_pct") or 0) >= 35:
        cautions.append("윗꼬리")
    if float(row.get("vol_ratio20") or 0) < 1.1:
        cautions.append("거래량약함")
    if float(row.get("disparity20") or 0) >= 121:
        cautions.append("이격과열")
    if float(row.get("Close") or 0) < float(row.get("ma112") or 0):
        cautions.append("장기이평/중기저항")
    if float(row.get("body_pct") or 0) < 0:
        cautions.append("캔들약함")
    return strengths, cautions


def _current_state_comment(pattern_name: str, current_position: str, recurrence_count: int) -> str:
    if current_position == "눌림 구간":
        return f"과거 {pattern_name} 재현 이력이 {recurrence_count}회 있어 현재는 눌림 뒤 2차 발사 대기 구간으로 볼 수 있습니다."
    if current_position == "재안착":
        return f"과거 {pattern_name}이 자주 나오던 종목으로, 지금은 재안착 이후 재가속 후보 구간에 가깝습니다."
    if current_position == "횡보 압축":
        return f"과거 {pattern_name} 전개 전 압축이 반복된 종목이라 현재도 에너지 응축 구간으로 해석할 수 있습니다."
    if current_position == "재발사 확인":
        return f"과거 {pattern_name} 패턴이 재현되며 이미 재발사 확인 구간으로 진입한 모습입니다."
    if current_position == "장대직후":
        return f"과거 {pattern_name} 습성은 있으나 지금은 장대 직후라 눌림 확인이 먼저입니다."
    if current_position == "눌림 심화":
        return f"과거 {pattern_name} 습성은 있으나 현재 눌림이 깊어 재현 확률보다 구조 복원 여부가 더 중요합니다."
    return f"과거 {pattern_name} 습성은 확인되지만 현재는 중립 구간이라 추가 확인이 필요합니다."


def analyze_kki_profile(price_df: pd.DataFrame, meta: Optional[Dict] = None, show_threshold: int = 55) -> KkiProfile:
    df = enrich_indicators(price_df)
    if df.empty or len(df) < 60:
        return KkiProfile(
            kki_score=0,
            absorb_score=0,
            pattern_name="데이터부족",
            best_band="BB20",
            support_band="BB40",
            recurrence_score=0,
            recurrence_count=0,
            rebound_count=0,
            range_relaunch_count=0,
            current_position="데이터부족",
            current_state_comment="끼 분석에 필요한 가격 데이터가 부족합니다.",
            strengths=[],
            cautions=["데이터부족"],
            supply_axis="혼조수급",
            absorb_comment="흡수 분석 불가",
            action_axis="분석보류",
            show_block=False,
            habit_comment="",
            band_comment="",
            position_comment="",
            integrated_comment="",
        )

    meta = dict(meta or {})
    best_band, support_band, counts, recurrence_score, current_position, pos_comment = _pick_best_band(df)
    pattern_name = max(counts.items(), key=lambda kv: (kv[1] * PATTERN_WEIGHTS[kv[0]], kv[1]))[0]
    recurrence_count = int(sum(counts.values()))
    rebound_count = int(counts.get("장대양봉→눌림→재발사형", 0) + counts.get("상단터치→눌림→2차상승형", 0))
    range_relaunch_count = int(counts.get("장대양봉→횡보→재발사형", 0) + counts.get("수축후확장형", 0))

    current_bonus = {
        "장대직후": 4,
        "눌림 구간": 18,
        "횡보 압축": 14,
        "재안착": 16,
        "재발사 확인": 22,
        "눌림 심화": -8,
        "중립": 0,
    }.get(current_position, 0)
    kki_score = int(round(max(0, min(100, recurrence_score * 0.65 + current_bonus))))

    absorb_score, supply_axis, absorb_comment = _absorption(meta)
    strengths, cautions = _strengths_and_cautions(df, meta, best_band, recurrence_score, current_position)
    current_state_comment = _current_state_comment(pattern_name, current_position, recurrence_count)

    if kki_score >= 65:
        action_axis = "과거 재발사형 재현성이 높아 눌림 확인 뒤 2차 발사 대응이 어울리는 구조"
    elif kki_score >= 50:
        action_axis = "지지 확인 뒤 반등 대응이 더 어울리는 구조"
    else:
        action_axis = "탄력 확인보다 품질과 지지 확인이 먼저인 구조"

    return KkiProfile(
        kki_score=kki_score,
        absorb_score=absorb_score,
        pattern_name=pattern_name,
        best_band=best_band,
        support_band=support_band,
        recurrence_score=recurrence_score,
        recurrence_count=recurrence_count,
        rebound_count=rebound_count,
        range_relaunch_count=range_relaunch_count,
        current_position=current_position,
        current_state_comment=current_state_comment,
        strengths=strengths,
        cautions=cautions,
        supply_axis=supply_axis,
        absorb_comment=absorb_comment,
        action_axis=action_axis,
        show_block=kki_score >= show_threshold,
        habit_comment=_habit_comment(pattern_name, recurrence_count, rebound_count, range_relaunch_count),
        band_comment=_band_comment(best_band, support_band),
        position_comment=pos_comment,
        integrated_comment="",  # wave 결합 후 analyze_kki_and_wave에서 보강
    )



def _zone_comment(zone: str, wave_name: str) -> str:
    zone = str(zone or "").strip()
    if zone.endswith("하단"):
        return f"{wave_name}이 박스 하단이라 가격 부담이 상대적으로 덜하고, 지지 확인 시 선취 접근이 가능한 자리입니다."
    if zone.endswith("중단"):
        return f"{wave_name}이 박스 중단이라 상하 어느 방향으로도 열려 있는 중립 구간입니다."
    if zone.endswith("상단"):
        return f"{wave_name}이 박스 상단이라 추격 부담이 있으며, 강하면 더 가지만 일반적으로는 눌림 재확인이 유리합니다."
    return f"{wave_name} 위치 해석에 필요한 데이터가 충분하지 않습니다."


def _wave_combo_comment(small_zone: str, medium_zone: str) -> str:
    s = str(small_zone or "").replace("소파동 ", "").strip()
    m = str(medium_zone or "").replace("중파동 ", "").strip()
    combo = (s, m)
    table = {
        ("하단", "하단"): "소파동과 중파동이 모두 하단권이라 전체적으로 바닥권 탐색 성격이 강합니다. 반등 초입을 노리는 선취형 관점이 유리합니다.",
        ("하단", "중단"): "소파동은 눌림 저점권이고 중파동은 중립이라, 단기 선취와 눌림 대응이 가장 무난한 구조입니다.",
        ("하단", "상단"): "단기 눌림은 깊지만 중파동은 높은 자리라 기술적 반등은 가능하나 상위 파동 부담을 함께 고려해야 합니다.",
        ("중단", "하단"): "소파동은 중립이나 중파동은 아직 낮아 상위 과열 부담은 크지 않습니다. 방향만 잡히면 위 공간이 남아 있습니다.",
        ("중단", "중단"): "소파동과 중파동 모두 중립이라 명확한 우위 구간은 아닙니다. 돌파 또는 눌림 확인이 먼저입니다.",
        ("중단", "상단"): "중파동이 높은 자리라 상단 부담이 존재합니다. 추격보다 눌림 재확인이 유리합니다.",
        ("상단", "하단"): "단기적으로는 빠르게 올라온 자리지만 중파동은 아직 낮아, 강한 종목이면 한 단계 더 뻗을 여지도 남아 있습니다.",
        ("상단", "중단"): "소파동은 상단 부담이 있으나 중파동은 과열이 아니라 강한 종목이라면 눌림 후 재상승 연결이 가능합니다.",
        ("상단", "상단"): "소파동과 중파동이 모두 상단권이라 추격 부담이 큽니다. 신규 접근보다 눌림 재확인 대응이 더 적절합니다.",
    }
    return table.get(combo, "파동 조합 해석을 위해 추가 확인이 필요합니다.")


def _angle_comment(small_label: str, medium_label: str) -> str:
    s = str(small_label or "").strip()
    m = str(medium_label or "").strip()

    if "상승" in s and "상승" in m:
        return "소파동과 중파동이 함께 우상향이라 추세 진행력은 양호합니다. 다만 위치가 상단이면 추격보다 눌림 대응이 좋습니다."
    if "상승" in s and ("횡보" in m or "전환" in m):
        return "단기 각도는 먼저 살아나고 있으나 중파동은 아직 방향 확정 전입니다. 첫 반응 뒤 유지력이 중요합니다."
    if ("횡보" in s or "전환" in s) and "상승" in m:
        return "중파동은 나쁘지 않은데 소파동이 방향을 잡는 중입니다. 단기 눌림 후 재상승 연결 여부를 보면 됩니다."
    if "하락" in s and "상승" in m:
        return "상위 흐름은 아직 살아 있지만 단기 조정이 남아 있습니다. 성급한 추격보다 눌림 확인이 유리합니다."
    if "상승" in s and "하락" in m:
        return "단기 반등은 나오고 있으나 중파동이 아직 꺾여 있어 되밀림 가능성을 함께 봐야 합니다."
    if "하락" in s and "하락" in m:
        return "소파동과 중파동이 모두 아래를 향해 있어 아직은 방어보다 확인이 우선입니다."
    return "각도는 방향 전환 여부를 확인하는 구간으로 보면 됩니다."


def _band_comment(best_band: str, support_band: str) -> str:
    desc = {
        "BB20": "단기 반응과 재점화 해석에 유리한 밴드입니다.",
        "BB40": "조금 더 넓은 호흡의 구조와 2차 파동 해석에 유리한 밴드입니다.",
        "Env20": "짧은 복원과 빠른 반등 체크에 잘 맞는 밴드입니다.",
        "Env40": "중간 호흡의 눌림과 지지 확인에 유리한 밴드입니다.",
    }
    main = desc.get(best_band, "주 반응 밴드입니다.")
    if support_band and support_band != best_band:
        sub = desc.get(support_band, "보조 확인용 밴드입니다.")
        return f"주 반응은 {best_band}로 보는 편이 맞고, 보조로는 {support_band}도 함께 보면 좋습니다. {main} 보조로는 {sub}"
    return f"주 반응은 {best_band}입니다. {main}"


def _habit_comment(pattern_name: str, recurrence_count: int, rebound_count: int, range_relaunch_count: int) -> str:
    p = str(pattern_name or "").strip()
    if "장대양봉→눌림→재발사형" in p:
        return f"과거에 장대양봉 뒤 눌림을 거쳐 다시 시세를 붙이는 흐름이 자주 나왔습니다. 재발사 이력 {rebound_count}회를 감안하면 한 번에 끝나는 타입보다는 2차 시세 가능성을 함께 보는 종목입니다."
    if "상단터치→눌림→2차상승형" in p:
        return f"상단 첫 반응보다 한 번 밀린 뒤 재차 상단을 공략하는 성향이 보입니다. 비슷한 재현 흔적은 {recurrence_count}회 수준입니다."
    if "하단터치반등형" in p:
        return "하단을 건드린 뒤 복원력이 나오는 습성이 비교적 뚜렷합니다. 밀리면 받치는 유형에 가깝습니다."
    if "수축후확장형" in p or "횡보" in p:
        return f"바로 치고 가기보다 시간을 먹고 박스를 만든 뒤 다시 확장되는 편입니다. 횡보 후 재발사 이력은 {range_relaunch_count}회입니다."
    return f"과거 재현 패턴은 {p or '혼합형'}에 가깝습니다. 비슷한 재현 카운트는 {recurrence_count}회 수준입니다."


def _integrated_comment(state_comment: str, kki_score: int, absorb_score: int, small_zone: str, medium_zone: str, best_band: str) -> str:
    parts = [str(state_comment or "").strip()]

    if kki_score >= 75 and absorb_score >= 65:
        parts.append("끼와 흡수가 함께 강한 편이라 눌림 뒤 재시세 연결 가능성을 적극적으로 열어둘 수 있습니다.")
    elif kki_score >= 60 and absorb_score >= 50:
        parts.append("끼는 살아 있고 흡수도 무너지지 않아, 눌림 확인 후 재상승 연결 가능성을 볼 수 있습니다.")
    elif kki_score >= 60 and absorb_score < 50:
        parts.append("끼는 살아 있지만 흡수는 강하지 않아, 한 번에 치고 가기보다 흔들림 후 재도전 흐름으로 보는 편이 좋습니다.")
    elif kki_score < 45 and absorb_score >= 60:
        parts.append("시세 성향 자체는 강하지 않아도 매물 소화는 양호해 급등형보다 안정형 반등 패턴에 가깝습니다.")
    else:
        parts.append("끼와 흡수가 모두 압도적이지는 않아 추격보다 자리 선별이 더 중요합니다.")

    parts.append(_wave_combo_comment(small_zone, medium_zone))
    if best_band in ("BB20", "BB40"):
        parts.append("볼린저 밴드 해석이 잘 맞는 구조라 밴드 수축·확장과 중심선 회복을 함께 보면 좋습니다.")
    elif best_band in ("Env20", "Env40"):
        parts.append("엔벨로프 해석이 잘 맞는 구조라 하단 복원과 하단 근접 반등 여부가 중요합니다.")
    return " ".join(p for p in parts if p)

def _zone_from_ratio(ratio: float) -> str:
    if ratio <= 0.33:
        return "하단"
    if ratio <= 0.66:
        return "중단"
    return "상단"


def _angle_label(angle: float) -> str:
    if angle >= 1.8:
        return "가파른 상승"
    if angle >= 0.6:
        return "완만 상승"
    if angle > -0.6:
        return "횡보"
    return "하락"


def analyze_wave_profile(price_df: pd.DataFrame) -> WaveProfile:
    df = enrich_indicators(price_df)
    if df.empty or len(df) < 40:
        return WaveProfile(
            small_zone="데이터부족",
            medium_zone="데이터부족",
            small_box_low=0.0,
            small_box_mid=0.0,
            small_box_high=0.0,
            medium_box_low=0.0,
            medium_box_mid=0.0,
            medium_box_high=0.0,
            small_angle=0.0,
            medium_angle=0.0,
            small_angle_label="중립",
            medium_angle_label="중립",
            wave_comment="파동 분석에 필요한 최소 데이터가 부족합니다.",
            small_zone_comment="",
            medium_zone_comment="",
            angle_comment="",
            combo_comment="",
        )

    close = float(df.iloc[-1]["Close"])
    s_lo, s_mid, s_hi = recent_box(df["High"], df["Low"], 15)
    m_lo, m_mid, m_hi = recent_box(df["High"], df["Low"], 45)

    s_ratio = 0.5 if s_hi <= s_lo else (close - s_lo) / max(s_hi - s_lo, 1e-9)
    m_ratio = 0.5 if m_hi <= m_lo else (close - m_lo) / max(m_hi - m_lo, 1e-9)
    small_zone = f"소파동 {_zone_from_ratio(float(s_ratio))}"
    medium_zone = f"중파동 {_zone_from_ratio(float(m_ratio))}"

    small_angle = regression_angle(df["Close"], 8)
    medium_angle = regression_angle(df["Close"], 20)
    small_angle_label = _angle_label(small_angle)
    medium_angle_label = _angle_label(medium_angle)

    if s_ratio <= 0.33 and m_ratio <= 0.50:
        comment = "소파동 하단, 중파동 하단~중단이라 눌림 확인형 대응이 어울립니다."
    elif s_ratio >= 0.67 and m_ratio >= 0.67:
        comment = "소파동과 중파동 모두 상단이라 추격보다 눌림 확인이 우선입니다."
    elif 0.33 < s_ratio < 0.67:
        comment = "소파동 중단에서 방향성을 만드는 구간이라 상단 재도전 여부를 확인해야 합니다."
    else:
        comment = "소파동과 중파동 위치가 엇갈려 확인 신호를 더 보는 편이 좋습니다."

    return WaveProfile(
        small_zone=small_zone,
        medium_zone=medium_zone,
        small_box_low=round(s_lo, 3),
        small_box_mid=round(s_mid, 3),
        small_box_high=round(s_hi, 3),
        medium_box_low=round(m_lo, 3),
        medium_box_mid=round(m_mid, 3),
        medium_box_high=round(m_hi, 3),
        small_angle=round(small_angle, 2),
        medium_angle=round(medium_angle, 2),
        small_angle_label=small_angle_label,
        medium_angle_label=medium_angle_label,
        wave_comment=comment,
        small_zone_comment=_zone_comment(small_zone, "소파동"),
        medium_zone_comment=_zone_comment(medium_zone, "중파동"),
        angle_comment=_angle_comment(small_angle_label, medium_angle_label),
        combo_comment=_wave_combo_comment(small_zone, medium_zone),
    )


def analyze_kki_and_wave(price_df: pd.DataFrame, meta: Optional[Dict] = None, show_threshold: int = 55) -> CombinedAnalysis:
    meta = dict(meta or {})
    norm = normalize_meta(meta)
    kki = analyze_kki_profile(price_df, meta=meta, show_threshold=show_threshold)
    wave = analyze_wave_profile(price_df)
    final_action = decide_action(norm, kki.kki_score, wave.small_zone)
    easy = build_easy_context(norm)

    kki.integrated_comment = _integrated_comment(
        kki.current_state_comment,
        kki.kki_score,
        kki.absorb_score,
        wave.small_zone,
        wave.medium_zone,
        kki.best_band,
    )

    parts = [easy["phase"], easy["location"], easy["quality"]]
    if kki.current_position and kki.current_position != "중립":
        parts.append(f"끼 관점으로는 현재 {kki.current_position}에 가깝습니다")
    parts.append(wave.wave_comment)
    if kki.integrated_comment:
        parts.append(kki.integrated_comment)
    easy_commentary = ". ".join(part.strip().rstrip(".") for part in parts if part).strip() + "."

    return CombinedAnalysis(
        kki=kki,
        wave=wave,
        final_action=final_action,
        easy_commentary=easy_commentary,
    )
