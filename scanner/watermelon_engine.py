from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


STATE_ALIASES = {
    "초입수박": "intro",
    "눌림수박": "pullback",
    "후행수박": "late",
    "Blue-1단기": "blue1",
    "Blue-2예비": "blue2_preview",
    "Blue-2스윙": "blue2",
    "구름테스트관찰": "cloud_test",
    "돌파관찰형": "breakout_watch",
    "돌파우선형": "breakout_priority",
    "중립관찰": "neutral_watch",
}

REFINE_ALIASES = {
    "✅정제수박": "good",
    "🟨관찰수박": "watch",
    "⚠️가짜수박주의": "fake",
}

CLOUD_ALIASES = {
    "☁ 저항전": "before_cloud",
    "☁ 저항테스트": "cloud_test",
    "☁ 저항돌파": "cloud_break",
    "☁ 저항혼합": "cloud_mixed",
}


@dataclass
class NormalizedMeta:
    state_label: str
    state_key: str
    refine_label: str
    refine_key: str
    cloud_label: str
    cloud_key: str
    pass_label: str
    score_safety: float
    score_n: float
    score_lead: float
    score_breakout: float


def normalize_meta(item: Optional[Dict]) -> NormalizedMeta:
    item = dict(item or {})
    state_label = str(item.get("상태") or item.get("state_label") or "").strip()
    refine_label = str(item.get("정제") or item.get("refine_label") or "").strip()
    cloud_label = str(item.get("저항구름") or item.get("cloud_label") or "").strip()
    pass_label = str(item.get("PASS") or item.get("pass_label") or item.get("pass_text") or "").strip()
    score_safety = float(item.get("안전점수") or item.get("safety_score") or 0)
    score_n = float(item.get("N점수") or item.get("n_score") or 0)
    score_lead = float(item.get("선취점수") or item.get("lead_score") or 0)
    score_breakout = float(item.get("돌파점수") or item.get("breakout_score") or 0)

    state_key = STATE_ALIASES.get(state_label, state_label or "unknown")
    refine_key = REFINE_ALIASES.get(refine_label, refine_label or "unknown")
    cloud_key = CLOUD_ALIASES.get(cloud_label, cloud_label or "unknown")

    return NormalizedMeta(
        state_label=state_label,
        state_key=state_key,
        refine_label=refine_label,
        refine_key=refine_key,
        cloud_label=cloud_label,
        cloud_key=cloud_key,
        pass_label=pass_label,
        score_safety=score_safety,
        score_n=score_n,
        score_lead=score_lead,
        score_breakout=score_breakout,
    )


def build_easy_context(meta: NormalizedMeta) -> Dict[str, str]:
    state_key = meta.state_key
    refine_key = meta.refine_key
    cloud_key = meta.cloud_key

    if state_key == "intro":
        phase = "바닥을 정리한 뒤 처음 살아나는 초입 구간"
    elif state_key == "pullback":
        phase = "한 번 살아난 뒤 눌림을 주는 재진입 구간"
    elif state_key in ("blue1", "blue2_preview", "blue2"):
        phase = "눌림 뒤 다시 시동을 거는 재점화 구간"
    elif state_key == "late":
        phase = "이미 한 박자 진행된 뒤라 신규 접근이 불리한 구간"
    else:
        phase = "구조는 일부 보이지만 해석은 아직 조심해야 하는 구간"

    if cloud_key == "before_cloud":
        location = "아직 저항구름 아래라 미리 볼 수 있는 여지"
    elif cloud_key == "cloud_test":
        location = "지금은 저항구름을 시험하는 중이라 확인이 먼저"
    elif cloud_key == "cloud_break":
        location = "이미 저항 위로 올라온 자리라 추격보다 눌림 확인이 더 중요"
    else:
        location = "저항 위치는 중립적으로 보되 상태와 정제를 더 우선해 해석"

    if refine_key == "good":
        quality = "정제수박이라 구조 품질은 비교적 양호"
    elif refine_key == "watch":
        quality = "관찰수박 단계라 한두 가지 확인 요소가 더 필요"
    elif refine_key == "fake":
        quality = "가짜수박주의가 있어 신호를 보수적으로 읽는 편이 낫"
    else:
        quality = "상태·저항구름·정제 중 하나라도 약하면 해석을 보수적으로 해야 함"

    return {
        "phase": phase,
        "location": location,
        "quality": quality,
    }


def decide_action(meta: NormalizedMeta, kki_score: float, wave_zone: str = "") -> str:
    state_key = meta.state_key
    refine_key = meta.refine_key
    cloud_key = meta.cloud_key

    if state_key == "late":
        if refine_key == "fake":
            return "추격 금지"
        return "보유자 대응"

    if state_key in ("blue1", "blue2_preview", "blue2"):
        if refine_key == "good" and kki_score >= 62:
            return "즉시 관찰"
        if refine_key == "fake":
            return "관망"
        return "즉시 관찰"

    if state_key == "intro":
        if cloud_key == "before_cloud" and refine_key == "good":
            return "선취 대기"
        if cloud_key == "cloud_test" and kki_score >= 55:
            return "돌파 확인"
        if refine_key == "fake":
            return "눌림 대기"
        return "관망" if kki_score < 35 else "눌림 대기"

    if state_key == "pullback":
        if refine_key == "good" and kki_score >= 55:
            return "눌림 대기"
        if refine_key == "fake":
            return "눌림 대기"
        return "눌림 대기"

    if cloud_key == "cloud_test":
        return "돌파 확인"

    if wave_zone in ("소파동 하단", "소파동 중단") and kki_score >= 55:
        return "즉시 관찰"
    return "관망"
