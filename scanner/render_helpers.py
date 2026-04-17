from __future__ import annotations

from typing import Optional

from .kki_wave import KkiProfile, WaveProfile, CombinedAnalysis


def _kki_grade(score: int) -> str:
    if score >= 75:
        return "🔥끼강"
    if score >= 55:
        return "⚡끼중"
    return "😴끼약"


def _absorb_grade(score: int) -> str:
    if score >= 65:
        return "🧲투매흡수강"
    if score >= 35:
        return "🧲투매흡수"
    return "🧲흡수약"


def build_kki_block(kki: KkiProfile, show_threshold: int = 55) -> str:
    if not kki.show_block or kki.kki_score < show_threshold:
        return ""

    strengths = " / ".join(kki.strengths) if kki.strengths else "기반"
    cautions = " / ".join(kki.cautions) if kki.cautions else "특이사항 없음"
    return (
        f"🎭 끼점수: {_kki_grade(kki.kki_score)} {kki.kki_score} | {_absorb_grade(kki.absorb_score)} {kki.absorb_score}\n"
        f"🧬 끼 패턴: 재현형:{kki.pattern_name} / 적합밴드:{kki.best_band} / 보조밴드:{kki.support_band} / "
        f"재현점수:{kki.recurrence_score} / 재발사이력:{kki.rebound_count}회 / 횡보후재발사:{kki.range_relaunch_count}회 / 현재위치:{kki.current_position}\n"
        f"🧲 끼/흡수 해설: 패턴축:재현형:{kki.pattern_name} / 적합밴드:{kki.best_band} / 재현점수:{kki.recurrence_score} / "
        f"재발사이력:{kki.rebound_count}회 / 현재위치:{kki.current_position} | 강점:{strengths} | 수급축:{kki.supply_axis} | "
        f"흡수판정:{kki.absorb_comment} | 주의:{cautions} | 대응축:{kki.action_axis}"
    )


def build_wave_block(wave: WaveProfile) -> str:
    if wave.small_zone == "데이터부족":
        return ""
    return (
        "🌊 파동분석:\n"
        f"- 소파동: {wave.small_zone}\n"
        f"- 소파동 박스: 하단 {wave.small_box_low:,.0f} / 중단 {wave.small_box_mid:,.0f} / 상단 {wave.small_box_high:,.0f}\n"
        f"- 중파동: {wave.medium_zone}\n"
        f"- 중파동 박스: 하단 {wave.medium_box_low:,.0f} / 중단 {wave.medium_box_mid:,.0f} / 상단 {wave.medium_box_high:,.0f}\n"
        f"- 상승각도: 소파동 {wave.small_angle_label}({wave.small_angle:+.2f}) / 중파동 {wave.medium_angle_label}({wave.medium_angle:+.2f})\n"
        f"- 현재 해석: {wave.wave_comment}"
    )


def build_easy_commentary_block(analysis: CombinedAnalysis) -> str:
    return f"🧠 쉬운 해석: {analysis.easy_commentary}\n✅ 최종 판정: {analysis.final_action}"


def build_kki_wave_bundle(analysis: CombinedAnalysis, show_threshold: int = 55) -> str:
    parts = []
    kki_block = build_kki_block(analysis.kki, show_threshold=show_threshold)
    if kki_block:
        parts.append(kki_block)
    wave_block = build_wave_block(analysis.wave)
    if wave_block:
        parts.append(wave_block)
    parts.append(build_easy_commentary_block(analysis))
    return "\n".join(p for p in parts if p).strip()
