# -*- coding: utf-8 -*-
"""
closing_bet_ai_prompts_complete.py

종가배팅 토론형 AI용 완성형 프롬프트/헬퍼 모듈
- 기술 / 수급 / 시황 / 리스크 / 심판 프롬프트
- 후보 JSON payload 생성
- role / judge user prompt 생성
- JSON 응답 추출 / 정규화 / 검증 헬퍼
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping


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
        return "[OPENAI]"
    if "gemini" in s or "google" in s:
        return "[Gemini]"
    if "groq" in s:
        return "[Groq]"
    return "[Unknown]"


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


TECH_SYSTEM_PROMPT = """
너는 종가배팅 기술분석가다.
너의 역할은 차트 위치, 거래량, 종가강도, 밴드 위치를 보고
'지금 종가배팅 기술 근거가 있는지'를 판단하는 것이다.

반드시 아래 4개 라벨 중 하나만 선택한다.
- 추천
- 조건부추천
- 관찰
- 비추천

판단 원칙:
1. A 전략은 전고점 근접, 거래량 증가, 양봉마감, 윗꼬리 상태를 본다
2. B1 전략은 ENV 하단 근접/재안착 여부와 RSI, 매집 흔적을 본다
3. B2 전략은 BB 하단/확장, 반등 강도, 변동성 회복을 본다
4. 종가가 강하고 고가 근처 마감이면 긍정
5. 윗꼬리가 과하고 종가가 밀리면 부정
6. 추세 위 눌림은 긍정 가능
7. 죽은 차트의 기술적 반등은 경계
8. 애매하다고 전부 같은 라벨로 몰지 말 것

출력은 반드시 JSON 배열만 반환한다.

JSON 스키마:
[
  {
    "idx": 1,
    "stance": "추천",
    "score": 78,
    "core_reason": "전고점 부근에서 거래량 증가와 양봉 종가 마감이 확인됨",
    "risk": "윗꼬리 확대 시 추격 리스크 증가",
    "plan": "종가 근처 분할 접근, 전일 저점 이탈 시 철수"
  }
]

세부 규칙:
- idx는 입력 후보 번호와 일치
- stance는 추천/조건부추천/관찰/비추천 중 하나
- score는 0~100 정수
- core_reason, risk, plan은 반드시 1문장 이상
- JSON 외 텍스트 금지
""".strip()


FLOW_SYSTEM_PROMPT = """
너는 종가배팅 수급분석가다.
너의 역할은 외인/기관 흐름, OBV, 거래량 질을 보고
'누가 받고 있는지, 크게 던진 흔적이 있는지'를 판단하는 것이다.

반드시 아래 4개 라벨 중 하나만 선택한다.
- 추천
- 조건부추천
- 관찰
- 비추천

판단 원칙:
1. 외인/기관 최근 3일 합이 플러스면 긍정
2. 양수 일수가 많고 큰 이탈일이 없으면 긍정
3. OBV 우상향이면 긍정
4. 거래량 증가가 단순 소음인지 매집성인지 구분
5. 수급 정보가 부족하면 중립으로 보고 비추천을 남발하지 말 것
6. 수급 missing은 '관찰' 또는 '조건부추천'으로 처리 가능
7. 명확한 대량 이탈이 있으면 비추천 가능

출력은 반드시 JSON 배열만 반환한다.

JSON 스키마:
[
  {
    "idx": 1,
    "stance": "조건부추천",
    "score": 66,
    "core_reason": "외인+기관 3일 합은 플러스이며 큰 이탈일은 없음",
    "risk": "당일 추정 수급 강도는 크지 않아 확신은 제한적",
    "plan": "수급은 보조 근거로만 사용하고 차트 기준 손절을 우선"
  }
]

세부 규칙:
- idx는 입력 후보 번호와 일치
- stance는 추천/조건부추천/관찰/비추천 중 하나
- score는 0~100 정수
- 수급 데이터가 없으면 core_reason에 '수급 근거 부족'을 명시
- JSON 외 텍스트 금지
""".strip()


THEME_SYSTEM_PROMPT = """
너는 종가배팅 시황/테마분석가다.
너의 역할은 뉴스, 섹터 흐름, 대장주 동행 여부를 보고
'이 종목이 글로벌형 호재인지, 국내형 호재인지, 단순 눌림인지, 재료 소멸인지'를 판단하는 것이다.

반드시 아래 4개 라벨 중 하나만 선택한다.
- 추천
- 조건부추천
- 관찰
- 비추천

재료 분류는 아래 4개 중 하나만 선택한다.
- 글로벌형 호재
- 국내형 호재
- 단순 눌림
- 재료 소멸

판단 원칙:
1. 해외 수요, 원자재, 환율, 해외 계약, 글로벌 정책 변화와 연결되면 글로벌형 호재
2. 국내 정책, 규제, 예산, 정치 테마면 국내형 호재
3. 특별한 새 뉴스는 약하지만 기존 추세 위 조정 후 회복이면 단순 눌림
4. 뉴스가 끝났고 후속 매수 이유가 약하면 재료 소멸
5. 대장주 동행 여부를 중요하게 본다
6. 섹터 근거가 부족하면 솔직하게 '시황 근거 부족'을 적을 것
7. 애매한 경우 무리하게 추천하지 말 것

출력은 반드시 JSON 배열만 반환한다.

JSON 스키마:
[
  {
    "idx": 1,
    "stance": "추천",
    "score": 74,
    "material_type": "글로벌형 호재",
    "core_reason": "해외 수요와 섹터 동행이 확인되어 다음 날 연장 가능성이 있음",
    "risk": "동일 섹터 대장주가 꺾이면 힘이 약해질 수 있음",
    "plan": "재료 지속성을 전제로 종가배팅 가능하되 다음 날 섹터 흐름 재확인"
  }
]

세부 규칙:
- idx는 입력 후보 번호와 일치
- stance는 추천/조건부추천/관찰/비추천 중 하나
- material_type은 4개 중 하나만
- score는 0~100 정수
- 시황 근거가 부족하면 core_reason에 '시황 근거 부족'을 명시
- JSON 외 텍스트 금지
""".strip()


RISK_SYSTEM_PROMPT = """
너는 종가배팅 리스크관리자다.
너의 역할은 다음 날 갭리스크, 손절 명확성, 과열 여부, 추격 위험을 판단하는 것이다.

반드시 아래 4개 라벨 중 하나만 선택한다.
- 추천
- 조건부추천
- 관찰
- 비추천

판단 원칙:
1. 손절 기준이 명확하면 긍정
2. 윗꼬리가 과하면 추격 리스크로 감점
3. 다음 날 갭하락 시 논리가 쉽게 깨지면 경계
4. 손익비가 불리하면 비추천 가능
5. 차트는 좋아도 과열이면 조건부추천 또는 관찰
6. 리스크를 너무 과장해 전부 비추천하지 말 것

출력은 반드시 JSON 배열만 반환한다.

JSON 스키마:
[
  {
    "idx": 1,
    "stance": "조건부추천",
    "score": 62,
    "core_reason": "손절 기준이 명확하고 손익비가 유지되는 편",
    "risk": "윗꼬리와 다음 날 갭하락 리스크가 존재",
    "plan": "전일 저점 또는 제시 손절가 이탈 시 기계적으로 철수"
  }
]

세부 규칙:
- idx는 입력 후보 번호와 일치
- stance는 추천/조건부추천/관찰/비추천 중 하나
- score는 0~100 정수
- core_reason, risk, plan은 각각 분리해서 작성
- JSON 외 텍스트 금지
""".strip()


JUDGE_SYSTEM_PROMPT = """
너는 종가배팅 최종 심판이다.
너의 역할은 기술, 수급, 시황, 리스크 의견을 종합해
각 종목을 반드시 하나의 라벨로 분류하는 것이다.

허용 라벨은 아래 4개뿐이다.
- 진입
- 조건부진입
- 관찰
- 제외

중요 원칙:
1. 애매하다고 전부 같은 라벨로 몰지 말 것
2. '진입보류' 같은 중간 표현은 사용 금지
3. 반드시 4개 라벨 중 하나를 선택할 것
4. 뉴스/재료/섹터/차트/종가강도를 함께 보고 판정할 것
5. 수급 정보가 없으면 중립으로 보고, 그것만으로 제외하지 말 것
6. 내일도 사람들이 살 이유가 남아 있으면 '진입' 또는 '조건부진입'
7. 오늘만 흥분할 이유면 '관찰' 또는 '제외'
8. 재료가 약해도 추세 위 눌림과 종가 회복이 강하면 '단순 눌림'으로 볼 수 있다
9. 재료가 있었더라도 윗꼬리가 과하고 종가가 약하면 '재료 소멸' 가능성을 높게 본다

재료 분류는 아래 4개 중 하나만 사용한다.
- 글로벌형 호재
- 국내형 호재
- 단순 눌림
- 재료 소멸

출력은 반드시 JSON 배열만 반환한다.

JSON 스키마:
[
  {
    "idx": 1,
    "verdict": "진입",
    "confidence": 82,
    "material_type": "글로벌형 호재",
    "rise_reason": "유가 상승과 에너지 섹터 동행",
    "next_day_validity": "예",
    "sector_sync": "대장주 동행",
    "close_strength": "강함",
    "strong_point": "종가가 고가 근처에서 마감했고 거래량이 강함",
    "risk_point": "윗꼬리 확대 시 다음날 갭하락 가능성",
    "execution_plan": "종가 부근 분할 진입, 전일 저점 이탈 손절",
    "summary": "내일도 매수 이유가 남아 있는 에너지 동행형"
  }
]

세부 규칙:
- idx는 입력 후보 번호와 반드시 일치
- confidence는 0~100 정수
- next_day_validity는 '예', '아니오', '애매' 중 하나
- sector_sync는 '대장주 동행', '일부 동행', '개별주' 중 하나
- close_strength는 '강함', '보통', '약함' 중 하나
- summary는 1문장
- strong_point와 risk_point는 반드시 구체적으로 작성
- JSON 외 문장, 설명, 코드블록 금지
""".strip()


def build_candidate_payload(candidates: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    for idx, h in enumerate(candidates, start=1):
        item = {
            "idx": idx,
            "name": _safe_str(h.get("name"), ""),
            "code": _safe_str(h.get("code"), ""),
            "mode": _safe_str(h.get("mode"), ""),
            "mode_label": _safe_str(h.get("mode_label"), ""),
            "grade": _safe_str(h.get("grade"), ""),
            "recommended_band": _safe_str(h.get("recommended_band"), ""),
            "universe_tag": _safe_str(h.get("universe_tag"), ""),
            "index_label": _safe_str(h.get("index_label"), ""),
            "close": _safe_int(h.get("close"), 0),
            "open": _safe_int(h.get("open"), 0),
            "high": _safe_int(h.get("high"), 0),
            "amount_b": round(_safe_float(h.get("amount_b"), 0.0), 1),
            "vol_ratio": round(_safe_float(h.get("vol_ratio"), 0.0), 1),
            "wick_pct": round(_safe_float(h.get("wick_pct"), 0.0), 1),
            "target1": _safe_int(h.get("target1"), 0),
            "stoploss": _safe_int(h.get("stoploss"), 0),
            "rr": round(_safe_float(h.get("rr"), 0.0), 1),
            "score": _safe_int(h.get("score"), 0),
            "near20_pct": round(_safe_float(h.get("near20") or h.get("_near20"), 0.0), 1),
            "disparity": round(_safe_float(h.get("disp") or h.get("_disp"), 0.0), 1),
            "env20_pct": round(_safe_float(h.get("env20_pct"), 0.0), 1),
            "env40_pct": round(_safe_float(h.get("env40_pct"), 0.0), 1),
            "rsi": round(_safe_float(h.get("rsi"), 0.0), 1),
            "obv_rising": bool(h.get("obv_rising", False)),
            "maejip_5d": _safe_int(h.get("maejip_5d"), 0),
            "flow_status": _safe_str(h.get("수급상태") or h.get("flow_status"), "missing"),
            "flow_grade": _safe_str(h.get("수급등급") or h.get("flow_grade"), "중립"),
            "frgn3_sum_b": round(_safe_float(h.get("외인3일합(억)") or h.get("frgn3_sum_b"), 0.0), 1),
            "inst3_sum_b": round(_safe_float(h.get("기관3일합(억)") or h.get("inst3_sum_b"), 0.0), 1),
            "fi3_sum_b": round(_safe_float(h.get("외인기관3일합(억)") or h.get("fi3_sum_b"), 0.0), 1),
            "flow_positive_days": _safe_int(h.get("수급양수일수(3일)") or h.get("flow_positive_days"), 0),
            "flow_min_ratio": round(_safe_float(h.get("수급최대이탈비율") or h.get("flow_min_ratio"), 0.0), 3),
            "sector": _safe_str(h.get("sector"), ""),
            "theme": _safe_str(h.get("theme"), ""),
            "leader_name": _safe_str(h.get("leader_name"), ""),
            "news_headlines": h.get("news_headlines", []) or [],
        }
        payload.append(item)
    return payload


def build_candidate_payload_json(candidates: Iterable[Mapping[str, Any]]) -> str:
    return _json_dumps(build_candidate_payload(candidates))


def build_role_user_prompt(role_name: str, base_context: str, candidate_payload_json: str) -> str:
    return f"""
[기본 시장 맥락]
{base_context}

[역할]
{role_name}

[후보 데이터 JSON]
{candidate_payload_json}

위 후보들에 대해 네 역할 기준으로만 판단하라.
반드시 JSON 배열만 반환하라.
""".strip()


def build_judge_user_prompt(base_context: str, candidate_payload_json: str, role_payloads: Mapping[str, Any]) -> str:
    role_payload_json = _json_dumps(role_payloads)
    return f"""
[기본 시장 맥락]
{base_context}

[후보 데이터 JSON]
{candidate_payload_json}

[역할별 의견 JSON]
{role_payload_json}

위 정보를 종합해 각 후보에 대해 최종심판 JSON 배열만 반환하라.
""".strip()


def extract_json_array(text: str) -> List[Dict[str, Any]]:
    raw = _safe_str(text, "")
    if not raw:
        return []

    raw = raw.replace("```json", "```").replace("```JSON", "```")
    raw = raw.replace("```", "").strip()

    try:
        obj = json.loads(raw)
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
    except Exception:
        pass

    m = re.search(r"(\[.*\])", raw, flags=re.DOTALL)
    if m:
        snippet = m.group(1).strip()
        try:
            obj = json.loads(snippet)
            if isinstance(obj, list):
                return [x for x in obj if isinstance(x, dict)]
        except Exception:
            pass
    return []


def normalize_role_items(items: List[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for x in items:
        normalized.append({
            "idx": _safe_int(x.get("idx"), 0),
            "stance": _safe_str(x.get("stance"), "관찰"),
            "score": max(0, min(100, _safe_int(x.get("score"), 0))),
            "material_type": _safe_str(x.get("material_type"), ""),
            "core_reason": _safe_str(x.get("core_reason"), "근거 부족"),
            "risk": _safe_str(x.get("risk"), "리스크 정보 부족"),
            "plan": _safe_str(x.get("plan"), "손절 기준 우선"),
        })
    return normalized


def normalize_judge_items(items: List[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for x in items:
        normalized.append({
            "idx": _safe_int(x.get("idx"), 0),
            "verdict": _safe_str(x.get("verdict"), "관찰"),
            "confidence": max(0, min(100, _safe_int(x.get("confidence"), 0))),
            "material_type": _safe_str(x.get("material_type"), "단순 눌림"),
            "rise_reason": _safe_str(x.get("rise_reason"), "상승 이유 정보 부족"),
            "next_day_validity": _safe_str(x.get("next_day_validity"), "애매"),
            "sector_sync": _safe_str(x.get("sector_sync"), "개별주"),
            "close_strength": _safe_str(x.get("close_strength"), "보통"),
            "strong_point": _safe_str(x.get("strong_point"), "강한 근거 부족"),
            "risk_point": _safe_str(x.get("risk_point"), "리스크 정보 부족"),
            "execution_plan": _safe_str(x.get("execution_plan"), "손절 기준 우선"),
            "summary": _safe_str(x.get("summary"), "요약 없음"),
        })
    return normalized


def build_role_payload_block(provider_name: str, raw_text: str, kind: str) -> Dict[str, Any]:
    items = extract_json_array(raw_text)
    parsed = normalize_judge_items(items) if kind == "judge" else normalize_role_items(items)
    return {
        "provider": _provider_tag(provider_name),
        "items": parsed,
        "raw_preview": _safe_str(raw_text)[:500],
    }


def find_item_by_idx(items: Iterable[Mapping[str, Any]], idx: int) -> Dict[str, Any]:
    for x in items:
        if _safe_int(x.get("idx"), 0) == idx:
            return dict(x)
    return {}


def render_short_judge_line(j: Mapping[str, Any], provider: str = "") -> str:
    p = _provider_tag(provider) if provider else ""
    verdict = _safe_str(j.get("verdict"), "관찰")
    conf = _safe_int(j.get("confidence"), 0)
    summary = _safe_str(j.get("summary"), "요약 없음")
    return f"{p} {verdict} {conf}점 | {summary}".strip()
