# -*- coding: utf-8 -*-
"""
trading_view_compressed_ready.py

Stock-Hunter / my_stock_search_bot 전용
[오늘의 매매관점 압축] 블록 생성 모듈

사용 위치:
    final_report 문자열이 완성된 뒤,
    send_telegram_chunks(final_report) 호출 직전에 아래처럼 붙입니다.

사용 예:
    from trading_view_compressed_ready import append_trading_view_compressed

    final_report = append_trading_view_compressed(final_report)
    send_telegram_chunks(final_report)

특징:
    - 기존 DataFrame 변수명을 몰라도 사용 가능
    - 이미 만들어진 리포트 문자열을 파싱해서 자동 요약
    - 실행 / 선취 / 눌림 / 보유자 대응 / 추격 금지로 압축
    - BB30 시프트, PASS_A, PASS_B, 후행형, 가짜수박주의, Blue-2, 정제수박 등을 반영
    - 같은 종목이 여러 섹션에 반복되어도 우선순위 기준으로 1회만 분류
"""

import re
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


COMPRESSED_MARKER = "🎯 [오늘의 매매관점 압축]"


@dataclass
class ParsedStockBlock:
    name: str
    code: str
    text: str
    start: int
    section: str = ""


@dataclass
class ClassifiedStock:
    name: str
    code: str
    category: str
    priority: int
    reason: str
    judge: str = ""
    status: str = ""
    structure: str = ""
    section: str = ""


SECTION_NAMES = [
    "오늘의 실시간 TOP 15",
    "오늘의 관찰형 참고",
    "오늘의 후행형 참고",
    "즉시진입 후보 TOP",
    "즉시진입 관찰 TOP",
    "수박상태 빨강 TOP",
    "파란점선 타점 TOP",
    "수박/파란점 디버그 TOP",
    "선취 후보 TOP",
    "선취 관찰 후보 TOP",
    "흰구름 돌파 후보 TOP",
    "흰구름 돌파 관찰 TOP",
    "선취 제외 후보 TOP",
    "초입수박 TOP",
    "눌림수박 TOP",
    "후행수박 TOP",
    "Blue-1 단기 TOP",
    "Blue-2 예비 TOP",
    "Blue-2 스윙 TOP",
]

CATEGORY_ORDER = ["execute", "preemptive", "pullback", "holder", "avoid"]

CATEGORY_META = {
    "execute": {"title": "1순위 실행 후보", "icon": "✅", "limit": 3},
    "preemptive": {"title": "2순위 선취 후보", "icon": "🧭", "limit": 3},
    "pullback": {"title": "3순위 눌림 관찰", "icon": "👀", "limit": 3},
    "holder": {"title": "보유자 대응", "icon": "🛡️", "limit": 4},
    "avoid": {"title": "추격 금지", "icon": "⛔", "limit": 4},
}


def _clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _first_match(pattern: str, text: str, default: str = "") -> str:
    m = re.search(pattern, text, re.S)
    return _clean(m.group(1)) if m else default


def _normalize_name(name: str) -> str:
    name = _clean(name)
    name = re.sub(r"^\d+\)\s*", "", name)
    name = re.sub(r"^[\-\*\s]+", "", name)
    return name.strip()


def _extract_code_from_text(text: str) -> str:
    return _first_match(r"\(([0-9]{6})\)", text)


def _near_section_name(report_text: str, start: int) -> str:
    """블록 시작 위치 기준으로 가장 가까운 앞쪽 섹션명을 찾습니다."""
    prev = report_text[max(0, start - 3000):start]
    best_name = ""

    for sec in SECTION_NAMES:
        pos = prev.rfind(sec)
        if pos >= 0:
            best_name = sec

    if not best_name:
        m_all = list(re.finditer(r"\[[^\]\n]{2,40}\]", prev))
        if m_all:
            best_name = m_all[-1].group(0).strip("[]")

    return best_name


def _extract_star_blocks(report_text: str) -> List[ParsedStockBlock]:
    """⭐ 🚀SS [유진로봇] 25,850원 형태의 블록 추출."""
    blocks: List[ParsedStockBlock] = []
    pattern = re.compile(
        r"(?ms)^⭐[^\n]*?\[[^\]]+\][^\n]*\n.*?(?=^────────────────|^━━━━━━━━|^⭐|\n\[TEST\]|\Z)"
    )

    for m in pattern.finditer(report_text):
        raw = m.group(0).strip()
        header = raw.splitlines()[0] if raw else ""
        name = _first_match(r"\[([^\]]+)\]", header)
        if not name:
            continue
        code = _extract_code_from_text(raw)
        start = m.start()
        blocks.append(
            ParsedStockBlock(
                name=_normalize_name(name),
                code=code,
                text=raw,
                start=start,
                section=_near_section_name(report_text, start),
            )
        )
    return blocks


def _extract_numbered_blocks(report_text: str) -> List[ParsedStockBlock]:
    """1) 레인보우로보틱스(277810) 형태의 블록 추출."""
    blocks: List[ParsedStockBlock] = []
    pattern = re.compile(
        r"(?ms)^\d+\)\s*[^\n]+(?:\n(?!\d+\)\s)[\s\S]*?)(?=^\d+\)\s|\n━━━━━━━━|\n\[TEST\]|\Z)"
    )

    for m in pattern.finditer(report_text):
        raw = m.group(0).strip()
        if not raw:
            continue
        header = raw.splitlines()[0]
        name = _first_match(r"^\d+\)\s*([^\(\n]+?)(?:\([0-9]{6}\)|$)", header)
        code = _extract_code_from_text(header) or _extract_code_from_text(raw)
        if not name or len(name) > 35:
            continue
        start = m.start()
        blocks.append(
            ParsedStockBlock(
                name=_normalize_name(name),
                code=code,
                text=raw,
                start=start,
                section=_near_section_name(report_text, start),
            )
        )
    return blocks


def extract_stock_blocks(report_text: str) -> List[ParsedStockBlock]:
    """리포트 문자열에서 종목 단위 블록을 추출합니다."""
    if not report_text:
        return []
    blocks: List[ParsedStockBlock] = []
    blocks.extend(_extract_star_blocks(report_text))
    blocks.extend(_extract_numbered_blocks(report_text))
    blocks.sort(key=lambda x: x.start)
    return blocks


def _judge_text(block_text: str) -> str:
    return _first_match(r"최종 판정:\s*([^\n]+)", block_text) or _first_match(r"- 최종 판정:\s*([^\n]+)", block_text)


def _status_text(block_text: str) -> str:
    return _first_match(r"- 상태:\s*([^\n]+)", block_text)


def _structure_text(block_text: str) -> str:
    return (
        _first_match(r"구조 해석:\s*([^\n]+)", block_text)
        or _first_match(r"PASS 해석:\s*([^\n]+)", block_text)
        or _first_match(r"📶\s*([^\n]+)", block_text)
    )


def _contains_any(text: str, keywords: Iterable[str]) -> bool:
    return any(k in text for k in keywords)


def classify_stock_block(block: ParsedStockBlock) -> ClassifiedStock:
    """종목 블록 1개를 매매관점 카테고리로 분류합니다."""
    t = block.text
    sec = block.section or ""
    judge = _judge_text(t)
    status = _status_text(t)
    structure = _structure_text(t)

    is_fake = "가짜수박주의" in t
    is_late = _contains_any(t, ["후행형", "후행수박", "후행유형", "보유관리후행형", "과열후행형"]) or "후행형" in sec or "후행수박" in sec
    is_overheated_late = "과열후행형" in t
    is_pass_b = "PASS_B" in t
    is_pass_a = "PASS_A" in t
    is_shift = "시프트" in t or "BB30시프트" in t
    is_blue2 = "Blue-2" in t or "Blue-2" in sec
    is_blue1 = "Blue-1" in t or "Blue-1" in sec
    is_clean = "정제수박" in t and not is_fake
    is_watch_watermelon = "관찰수박" in t
    is_breakout = "저항돌파" in t or "흰구름 돌파 후보" in sec
    is_resistance_test = "저항테스트" in t or "흰구름 돌파 관찰" in sec
    is_preemptive_section = "선취 후보" in sec or "선취 관찰" in sec
    is_pullback = "눌림수박" in t or "눌림 대기" in t or "눌림수박" in sec
    is_immediate_entry = "즉시진입 후보" in sec
    is_immediate_watch = "즉시진입 관찰" in sec
    is_execute_section = (
        is_immediate_entry
        or "파란점선" in sec
        or "수박상태 빨강" in sec
        or "Blue-2 스윙" in sec
        or "흰구름 돌파 후보" in sec
    )
    is_avoid_section = "선취 제외" in sec
    no_chase = _contains_any(t, ["추격 금지", "신규 추격 금지"]) or is_avoid_section

    if no_chase or is_overheated_late or (is_late and is_fake):
        reason = "후행/과열 또는 가짜수박주의가 겹쳐 신규 접근보다 회피 우선"
        if is_overheated_late:
            reason = "과열후행형으로 신규 추격 금지, 보유자는 분할 익절·5일선 이탈 관리"
        elif is_fake:
            reason = "가짜수박주의가 있어 신호를 보수적으로 읽고 신규 추격 회피"
        return ClassifiedStock(block.name, block.code, "avoid", 100, reason, judge, status, structure, sec)

    if is_late:
        if is_pass_b:
            reason = "PASS_B 강화 확인 구조지만 후행형이라 신규 추격보다 보유 관리/눌림 재확인"
            priority = 82
        elif is_pass_a:
            reason = "PASS_A 기본 구조 통과지만 후행형이라 신규보다 눌림 재확인"
            priority = 78
        elif is_shift:
            reason = "시프트 발사형이지만 후행형이라 보유자는 관리, 신규는 눌림 대기"
            priority = 74
        else:
            reason = "후행 구간으로 신규 추격보다 보유자 이탈선 관리 우선"
            priority = 70
        return ClassifiedStock(block.name, block.code, "holder", priority, reason, judge, status, structure, sec)

    if is_execute_section or is_blue2 or is_blue1:
        if is_immediate_entry:
            reason = "즉시진입 후보 블록에서 선별된 종목으로, 5일재안착·정제·수급 조건을 우선 확인"
            priority = 98
        elif is_blue2 and is_clean:
            reason = "Blue-2 재점화 + 정제수박 조합으로 실행 우선 후보"
            priority = 95
        elif is_breakout and is_clean:
            reason = "저항돌파 + 정제수박 조합으로 돌파 확인 후보"
            priority = 90
        elif is_blue2 and is_watch_watermelon:
            reason = "Blue-2 재점화 후보지만 관찰수박이라 눌림/안착 확인 후 실행"
            priority = 86
        elif is_fake:
            reason = "재점화 후보이나 가짜수박주의로 즉시 실행보다 관찰"
            priority = 65
        else:
            reason = "재점화/돌파 후보로 거래량 유지와 안착 확인 시 실행"
            priority = 84
        category = "execute" if not is_fake else "pullback"
        if is_immediate_watch and not is_immediate_entry:
            category = "pullback"
            priority = max(64, priority - 8)
        return ClassifiedStock(block.name, block.code, category, priority, reason, judge, status, structure, sec)

    if is_preemptive_section or ("초입수박" in t and is_resistance_test):
        if is_clean and "5일재안착" in t:
            reason = "초입/눌림 구조 + 정제통과 + 5일재안착으로 선취 관찰 우수"
            priority = 72
        elif is_clean:
            reason = "초입/눌림 구조 + 정제통과로 선취 관찰 가능"
            priority = 68
        elif is_watch_watermelon:
            reason = "선취 구조는 있으나 관찰수박이라 거래량·양봉 확인 필요"
            priority = 62
        else:
            reason = "선취 예비 후보로 위치 확인 후 분할 관찰"
            priority = 58
        return ClassifiedStock(block.name, block.code, "preemptive", priority, reason, judge, status, structure, sec)

    if is_pullback or is_resistance_test or is_breakout:
        if is_breakout and is_clean:
            reason = "저항돌파 구조는 있으나 추격보다 구름 상단/5일선 눌림 확인"
            priority = 60
        elif is_pullback:
            reason = "눌림 구조로 5일선 재안착·양봉·거래량 보강 확인 필요"
            priority = 56
        else:
            reason = "구조는 있으나 핵심 타점 미충족, 눌림 확인 후 대응"
            priority = 52
        return ClassifiedStock(block.name, block.code, "pullback", priority, reason, judge, status, structure, sec)

    return ClassifiedStock(
        block.name,
        block.code,
        "pullback",
        40,
        "핵심 타점은 아니지만 구조 확인용 관찰 후보",
        judge,
        status,
        structure,
        sec,
    )


def _candidate_key(item: ClassifiedStock) -> str:
    return item.code if item.code else item.name


def classify_report(report_text: str) -> Dict[str, OrderedDict]:
    """전체 리포트를 파싱해서 카테고리별 후보를 반환합니다."""
    blocks = extract_stock_blocks(report_text)
    best_by_stock: Dict[str, ClassifiedStock] = {}

    for block in blocks:
        item = classify_stock_block(block)
        key = _candidate_key(item)
        old = best_by_stock.get(key)
        if old is None or item.priority > old.priority:
            best_by_stock[key] = item

    grouped: Dict[str, OrderedDict] = {cat: OrderedDict() for cat in CATEGORY_ORDER}
    sorted_items = sorted(best_by_stock.values(), key=lambda x: (-x.priority, x.name))

    for item in sorted_items:
        grouped[item.category][_candidate_key(item)] = item

    return grouped


def _format_stock_item(idx: int, item: ClassifiedStock) -> str:
    code = f"({item.code})" if item.code else ""
    judge = f" / 판정:{item.judge}" if item.judge else ""
    status = f" / 상태:{item.status}" if item.status else ""
    return f"{idx}) {item.name}{code}{judge}{status}\n   → {item.reason}"


def _format_category(category: str, bucket: OrderedDict) -> str:
    meta = CATEGORY_META[category]
    icon = meta["icon"]
    title = meta["title"]
    limit = int(meta["limit"])

    if not bucket:
        return f"{icon} {title}\n- 해당 후보 없음"

    lines = [f"{icon} {title}"]
    for idx, item in enumerate(list(bucket.values())[:limit], 1):
        lines.append(_format_stock_item(idx, item))
    return "\n".join(lines)


def _extract_top_sector(report_text: str) -> str:
    m = re.search(r"1위\s*\[([^\]]+)\].*?점수\s*([0-9\.\-]+)", report_text, re.S)
    if not m:
        return ""
    return f"{_clean(m.group(1))} 중심"


def _first_item(grouped: Dict[str, OrderedDict], category: str) -> Optional[ClassifiedStock]:
    bucket = grouped.get(category) or OrderedDict()
    if not bucket:
        return None
    return next(iter(bucket.values()))


def _build_conclusion(report_text: str, grouped: Dict[str, OrderedDict]) -> str:
    lines: List[str] = []
    top_sector = _extract_top_sector(report_text)
    if top_sector:
        lines.append(f"- 주도 섹터: {top_sector}")

    execute = _first_item(grouped, "execute")
    preemptive = _first_item(grouped, "preemptive")
    pullback = _first_item(grouped, "pullback")

    if execute:
        lines.append(f"- 실행 우선: {execute.name}")
    if preemptive:
        lines.append(f"- 선취 관찰: {preemptive.name}")
    if not execute and pullback:
        lines.append(f"- 관찰 우선: {pullback.name}")
    if grouped.get("holder"):
        lines.append("- 후행형은 신규보다 보유 관리/눌림 재확인")
    if grouped.get("avoid"):
        lines.append("- 가짜수박주의·과열후행형은 추격 금지")
    if not lines:
        lines.append("- 오늘은 확정 실행보다 후보 관찰 중심")
    return "\n".join(lines)


def build_trading_view_compressed(report_text: str) -> str:
    """최종 리포트 텍스트를 기반으로 [오늘의 매매관점 압축] 블록을 생성합니다."""
    if not report_text:
        return ""

    grouped = classify_report(report_text)

    body_parts = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        COMPRESSED_MARKER,
        "",
        _format_category("execute", grouped["execute"]),
        "",
        _format_category("preemptive", grouped["preemptive"]),
        "",
        _format_category("pullback", grouped["pullback"]),
        "",
        _format_category("holder", grouped["holder"]),
        "",
        _format_category("avoid", grouped["avoid"]),
        "",
        "📌 오늘 결론",
        _build_conclusion(report_text, grouped),
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(body_parts).strip()


def append_trading_view_compressed(final_report: str) -> str:
    """
    final_report 맨 뒤에 [오늘의 매매관점 압축] 블록을 붙입니다.
    이미 붙어 있으면 중복 방지를 위해 그대로 반환합니다.
    """
    if not final_report:
        return final_report
    if COMPRESSED_MARKER in final_report:
        return final_report
    compressed = build_trading_view_compressed(final_report)
    if not compressed:
        return final_report
    return final_report.rstrip() + "\n\n" + compressed + "\n"


# 기존 코드 스타일에 맞춰 alias도 제공

def build_trade_summary(report_text: str) -> str:
    return build_trading_view_compressed(report_text)


def append_trade_summary(final_report: str) -> str:
    return append_trading_view_compressed(final_report)
