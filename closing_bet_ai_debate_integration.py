# -*- coding: utf-8 -*-
"""
closing_bet_ai_debate_integration.py

closing_bet_ai_prompts_complete.py 를 실제 종가배팅 스캐너에 연결하는 통합 레이어.
이 모듈은 LLM 호출 자체를 직접 구현하지 않고, scanner 쪽의 llm_runner 콜백을 받아 사용한다.

핵심 기능:
- 토론 대상 후보 선별
- 역할별 JSON 토론 실행
- 최종 심판 JSON 실행
- 역할/심판 결과를 후보별로 병합
- 텔레그램 카드형 메시지 렌더링
- 구글시트 저장용 AI 판정 행 생성

필수 요구:
- 같은 폴더에 closing_bet_ai_prompts_complete.py 존재
- scanner 쪽에서 아래 형태의 llm_runner 콜백 제공

예시:
    def llm_runner(system_prompt: str, user_prompt: str, preferred_models=None, role_name=""):
        # return (raw_text, provider_name)
        ...

사용 예:
    from closing_bet_ai_debate_integration import run_closing_bet_debate_pipeline

    result = run_closing_bet_debate_pipeline(
        hits=hits,
        llm_runner=llm_runner,
        now_dt=now,
        mins_left=mins_left,
        top_n=7,
        extra_market_context=market_context_text,
        role_model_prefs={
            "tech": ["anthropic", "openai"],
            "flow": ["openai", "groq"],
            "theme": ["anthropic", "gemini"],
            "risk": ["openai", "groq"],
            "judge": ["anthropic", "openai"],
        },
    )

    debate_rows = result["rows"]
    telegram_text = result["telegram_text"]
    judgment_rows = result["judgment_rows"]
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from closing_bet_ai_prompts_complete import (
    TECH_SYSTEM_PROMPT,
    FLOW_SYSTEM_PROMPT,
    THEME_SYSTEM_PROMPT,
    RISK_SYSTEM_PROMPT,
    JUDGE_SYSTEM_PROMPT,
    build_candidate_payload_json,
    build_role_user_prompt,
    build_judge_user_prompt,
    build_role_payload_block,
    find_item_by_idx,
)


LlmRunner = Callable[[str, str, Optional[Sequence[str]], str], Tuple[str, str]]


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return float(v)
    except Exception:
        return default


def _safe_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    try:
        s = str(v).strip()
        return s if s else default
    except Exception:
        return default


def _provider_tag(name: str) -> str:
    s = _safe_str(name, "").lower()
    if "anthropic" in s or "claude" in s:
        return "[Claude]"
    if "openai" in s or "gpt" in s:
        return "[GPT]"
    if "gemini" in s or "google" in s:
        return "[Gemini]"
    if "groq" in s:
        return "[Groq]"
    return "[Unknown]"


def _grade_rank(grade: str) -> int:
    g = _safe_str(grade)
    mapping = {
        "완전체": 5,
        "✅A급": 4,
        "A급": 4,
        "B급": 3,
        "C급": 2,
        "D급": 1,
    }
    return mapping.get(g, 0)


def _mode_rank(mode: str) -> int:
    m = _safe_str(mode).upper()
    return {"B1": 3, "B2": 2, "A": 1}.get(m, 0)


def pick_debate_candidates(hits: Iterable[Mapping[str, Any]], top_n: int = 7) -> List[Dict[str, Any]]:
    """토론 대상 후보 선별. 점수/등급/전략 우선순위 기반."""
    rows = [dict(x) for x in hits]
    rows.sort(
        key=lambda h: (
            _safe_float(h.get("score"), 0.0),
            _grade_rank(h.get("grade")),
            _mode_rank(h.get("mode")),
            _safe_float(h.get("amount_b"), 0.0),
            -_safe_float(h.get("wick_pct"), 999.0),
        ),
        reverse=True,
    )
    return rows[: max(1, top_n)]


def build_base_context(now_dt: datetime, mins_left: int, extra_market_context: str = "") -> str:
    base = [
        f"현재 시각: {now_dt.strftime('%Y-%m-%d %H:%M')}",
        f"마감까지 남은 시간: {mins_left}분",
        "전략: 종가배팅",
        "판정 기준: 기술/수급/시황/리스크를 종합해 최종판정",
    ]
    if extra_market_context:
        base.append(f"추가 시장 맥락: {extra_market_context}")
    return "\n".join(base)


def _run_role_json(
    role_name: str,
    system_prompt: str,
    base_context: str,
    candidates: Sequence[Mapping[str, Any]],
    llm_runner: LlmRunner,
    preferred_models: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    candidate_payload_json = build_candidate_payload_json(candidates)
    user_prompt = build_role_user_prompt(role_name, base_context, candidate_payload_json)
    raw_text, provider = llm_runner(system_prompt, user_prompt, preferred_models, role_name)
    return build_role_payload_block(provider_name=provider, raw_text=raw_text, kind="role")


def _run_judge_json(
    base_context: str,
    candidates: Sequence[Mapping[str, Any]],
    role_payloads: Mapping[str, Any],
    llm_runner: LlmRunner,
    preferred_models: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    candidate_payload_json = build_candidate_payload_json(candidates)
    user_prompt = build_judge_user_prompt(base_context, candidate_payload_json, role_payloads)
    raw_text, provider = llm_runner(JUDGE_SYSTEM_PROMPT, user_prompt, preferred_models, "최종심판")
    return build_role_payload_block(provider_name=provider, raw_text=raw_text, kind="judge")


def run_all_roles(
    base_context: str,
    candidates: Sequence[Mapping[str, Any]],
    llm_runner: LlmRunner,
    role_model_prefs: Optional[Mapping[str, Sequence[str]]] = None,
) -> Dict[str, Any]:
    role_model_prefs = dict(role_model_prefs or {})

    tech = _run_role_json(
        role_name="기술분석가",
        system_prompt=TECH_SYSTEM_PROMPT,
        base_context=base_context,
        candidates=candidates,
        llm_runner=llm_runner,
        preferred_models=role_model_prefs.get("tech"),
    )
    flow = _run_role_json(
        role_name="수급분석가",
        system_prompt=FLOW_SYSTEM_PROMPT,
        base_context=base_context,
        candidates=candidates,
        llm_runner=llm_runner,
        preferred_models=role_model_prefs.get("flow"),
    )
    theme = _run_role_json(
        role_name="시황테마분석가",
        system_prompt=THEME_SYSTEM_PROMPT,
        base_context=base_context,
        candidates=candidates,
        llm_runner=llm_runner,
        preferred_models=role_model_prefs.get("theme"),
    )
    risk = _run_role_json(
        role_name="리스크관리자",
        system_prompt=RISK_SYSTEM_PROMPT,
        base_context=base_context,
        candidates=candidates,
        llm_runner=llm_runner,
        preferred_models=role_model_prefs.get("risk"),
    )

    role_payloads = {
        "tech": tech,
        "flow": flow,
        "theme": theme,
        "risk": risk,
    }

    judge = _run_judge_json(
        base_context=base_context,
        candidates=candidates,
        role_payloads=role_payloads,
        llm_runner=llm_runner,
        preferred_models=role_model_prefs.get("judge"),
    )

    role_payloads["judge"] = judge
    return role_payloads


def _stance_to_vote(stance: str) -> int:
    s = _safe_str(stance)
    if s == "추천":
        return 1
    if s == "조건부추천":
        return 1
    if s == "비추천":
        return -1
    return 0


def merge_debate_results(
    candidates: Sequence[Mapping[str, Any]],
    role_payloads: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    tech_items = role_payloads.get("tech", {}).get("items", []) or []
    flow_items = role_payloads.get("flow", {}).get("items", []) or []
    theme_items = role_payloads.get("theme", {}).get("items", []) or []
    risk_items = role_payloads.get("risk", {}).get("items", []) or []
    judge_items = role_payloads.get("judge", {}).get("items", []) or []

    tech_provider = role_payloads.get("tech", {}).get("provider", "[Unknown]")
    flow_provider = role_payloads.get("flow", {}).get("provider", "[Unknown]")
    theme_provider = role_payloads.get("theme", {}).get("provider", "[Unknown]")
    risk_provider = role_payloads.get("risk", {}).get("provider", "[Unknown]")
    judge_provider = role_payloads.get("judge", {}).get("provider", "[Unknown]")

    rows: List[Dict[str, Any]] = []

    for idx, cand in enumerate(candidates, start=1):
        tech = find_item_by_idx(tech_items, idx)
        flow = find_item_by_idx(flow_items, idx)
        theme = find_item_by_idx(theme_items, idx)
        risk = find_item_by_idx(risk_items, idx)
        judge = find_item_by_idx(judge_items, idx)

        positive_votes = sum(
            1 for s in [
                tech.get("stance"),
                flow.get("stance"),
                theme.get("stance"),
                risk.get("stance"),
            ]
            if _stance_to_vote(s) > 0
        )
        negative_votes = sum(
            1 for s in [
                tech.get("stance"),
                flow.get("stance"),
                theme.get("stance"),
                risk.get("stance"),
            ]
            if _stance_to_vote(s) < 0
        )

        row = dict(cand)
        row.update({
            "idx": idx,
            "AI최종판정": _safe_str(judge.get("verdict"), "관찰"),
            "AI확신도": _safe_int(judge.get("confidence"), 0),
            "AI재료분류": _safe_str(judge.get("material_type"), "단순 눌림"),
            "AI상승이유": _safe_str(judge.get("rise_reason"), "정보 부족"),
            "AI내일유효성": _safe_str(judge.get("next_day_validity"), "애매"),
            "AI섹터동행": _safe_str(judge.get("sector_sync"), "개별주"),
            "AI종가강도": _safe_str(judge.get("close_strength"), "보통"),
            "AI강한근거": _safe_str(judge.get("strong_point"), "근거 부족"),
            "AI위험요인": _safe_str(judge.get("risk_point"), "리스크 정보 부족"),
            "AI실행계획": _safe_str(judge.get("execution_plan"), "손절 기준 우선"),
            "AI심판요약": _safe_str(judge.get("summary"), "요약 없음"),
            "AI기술모델": tech_provider,
            "AI수급모델": flow_provider,
            "AI시황모델": theme_provider,
            "AI리스크모델": risk_provider,
            "AI심판모델": judge_provider,
            "AI기술의견": _safe_str(tech.get("core_reason"), "근거 부족"),
            "AI수급의견": _safe_str(flow.get("core_reason"), "수급 근거 부족"),
            "AI시황의견": _safe_str(theme.get("core_reason"), "시황 근거 부족"),
            "AI리스크의견": _safe_str(risk.get("core_reason"), "리스크 정보 부족"),
            "AI기술리스크": _safe_str(tech.get("risk"), ""),
            "AI수급리스크": _safe_str(flow.get("risk"), ""),
            "AI시황리스크": _safe_str(theme.get("risk"), ""),
            "AI리스크상세": _safe_str(risk.get("risk"), ""),
            "AI기술플랜": _safe_str(tech.get("plan"), ""),
            "AI수급플랜": _safe_str(flow.get("plan"), ""),
            "AI시황플랜": _safe_str(theme.get("plan"), ""),
            "AI리스크플랜": _safe_str(risk.get("plan"), ""),
            "positive_votes": positive_votes,
            "negative_votes": negative_votes,
        })
        rows.append(row)

    return rows


def format_telegram_cards(rows: Sequence[Mapping[str, Any]], mins_left: int) -> str:
    if not rows:
        return "🧠 종가배팅 AI 토론 결과 없음"

    first = rows[0]
    header = [
        "🧠 종가배팅 AI 토론 TOP{}".format(len(rows)),
        f"⏰ 마감까지 {mins_left}분",
        "모델사용: 기술{} | 수급{} | 시황{} | 리스크{} | 심판{}".format(
            _safe_str(first.get("AI기술모델"), "[Unknown]"),
            _safe_str(first.get("AI수급모델"), "[Unknown]"),
            _safe_str(first.get("AI시황모델"), "[Unknown]"),
            _safe_str(first.get("AI리스크모델"), "[Unknown]"),
            _safe_str(first.get("AI심판모델"), "[Unknown]"),
        ),
        "",
    ]

    lines: List[str] = header

    for i, r in enumerate(rows, start=1):
        name = _safe_str(r.get("name"), "")
        code = _safe_str(r.get("code"), "")
        mode = _safe_str(r.get("mode"), "")
        grade = _safe_str(r.get("grade"), "")
        verdict = _safe_str(r.get("AI최종판정"), "관찰")
        conf = _safe_int(r.get("AI확신도"), 0)

        lines.extend([
            f"{i}. {name}({code}) [{mode}/{grade}] → {verdict} {conf}점",
            f"   심판:{_safe_str(r.get('AI심판모델'))} {_safe_str(r.get('AI심판요약'))}",
            f"   강한근거: {_safe_str(r.get('AI강한근거'))}",
            f"   위험요인: {_safe_str(r.get('AI위험요인'))}",
            f"   실행계획: {_safe_str(r.get('AI실행계획'))}",
            f"   기술:{_safe_str(r.get('AI기술모델'))} {_safe_str(r.get('AI기술의견'))}",
            f"   수급:{_safe_str(r.get('AI수급모델'))} {_safe_str(r.get('AI수급의견'))}",
            f"   시황:{_safe_str(r.get('AI시황모델'))} {_safe_str(r.get('AI시황의견'))}",
            f"   리스크:{_safe_str(r.get('AI리스크모델'))} {_safe_str(r.get('AI리스크의견'))}",
            f"   손절:{_safe_str(r.get('stoploss'))} | 목표:{_safe_str(r.get('target1'))}",
            "",
        ])

    return "\n".join(lines).strip()


def build_ai_judgment_rows(rows: Sequence[Mapping[str, Any]], now_dt: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    scan_date = now_dt.strftime("%Y-%m-%d")
    scan_time = now_dt.strftime("%H:%M")

    for r in rows:
        out.append({
            "scan_date": scan_date,
            "scan_time": scan_time,
            "code": _safe_str(r.get("code")),
            "name": _safe_str(r.get("name")),
            "mode": _safe_str(r.get("mode")),
            "grade": _safe_str(r.get("grade")),
            "recommended_band": _safe_str(r.get("recommended_band")),
            "universe_tag": _safe_str(r.get("universe_tag")),
            "final_verdict": _safe_str(r.get("AI최종판정")),
            "final_confidence": _safe_int(r.get("AI확신도"), 0),
            "judge_summary": _safe_str(r.get("AI심판요약")),
            "material_type": _safe_str(r.get("AI재료분류")),
            "rise_reason": _safe_str(r.get("AI상승이유")),
            "next_day_validity": _safe_str(r.get("AI내일유효성")),
            "sector_sync": _safe_str(r.get("AI섹터동행")),
            "close_strength": _safe_str(r.get("AI종가강도")),
            "strong_point": _safe_str(r.get("AI강한근거")),
            "risk_point": _safe_str(r.get("AI위험요인")),
            "execution_plan": _safe_str(r.get("AI실행계획")),
            "tech_provider": _safe_str(r.get("AI기술모델")),
            "flow_provider": _safe_str(r.get("AI수급모델")),
            "theme_provider": _safe_str(r.get("AI시황모델")),
            "risk_provider": _safe_str(r.get("AI리스크모델")),
            "judge_provider": _safe_str(r.get("AI심판모델")),
            "tech_view": _safe_str(r.get("AI기술의견")),
            "flow_view": _safe_str(r.get("AI수급의견")),
            "theme_view": _safe_str(r.get("AI시황의견")),
            "risk_view": _safe_str(r.get("AI리스크의견")),
            "positive_votes": _safe_int(r.get("positive_votes"), 0),
            "negative_votes": _safe_int(r.get("negative_votes"), 0),
            "stoploss": _safe_str(r.get("stoploss")),
            "target1": _safe_str(r.get("target1")),
        })
    return out


def run_closing_bet_debate_pipeline(
    hits: Sequence[Mapping[str, Any]],
    llm_runner: LlmRunner,
    now_dt: datetime,
    mins_left: int,
    top_n: int = 7,
    extra_market_context: str = "",
    role_model_prefs: Optional[Mapping[str, Sequence[str]]] = None,
) -> Dict[str, Any]:
    candidates = pick_debate_candidates(hits, top_n=top_n)
    base_context = build_base_context(now_dt=now_dt, mins_left=mins_left, extra_market_context=extra_market_context)
    role_payloads = run_all_roles(
        base_context=base_context,
        candidates=candidates,
        llm_runner=llm_runner,
        role_model_prefs=role_model_prefs,
    )
    rows = merge_debate_results(candidates, role_payloads)
    telegram_text = format_telegram_cards(rows, mins_left=mins_left)
    judgment_rows = build_ai_judgment_rows(rows, now_dt=now_dt)

    return {
        "candidates": candidates,
        "base_context": base_context,
        "role_payloads": role_payloads,
        "rows": rows,
        "telegram_text": telegram_text,
        "judgment_rows": judgment_rows,
    }


__all__ = [
    "pick_debate_candidates",
    "build_base_context",
    "run_all_roles",
    "merge_debate_results",
    "format_telegram_cards",
    "build_ai_judgment_rows",
    "run_closing_bet_debate_pipeline",
]
