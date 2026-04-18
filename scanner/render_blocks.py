from __future__ import annotations

import pandas as pd

from .config import KKI_SHOW_MIN, SHOW_KKI_ONLY_WHEN_CONFIDENT
from .narrative import enrich_row_with_human_commentary
from .watermelon_refine import build_refine_validation_text


def build_watermelon_guide_block() -> str:
    lines = [
        '🍉 [수박 상태 가이드]',
        '- 초입수박: 구조가 막 살아나는 초동 회복형 | 대응: 소액 탐색, 거래량·5일재안착 확인',
        '- 눌림수박: 가장 실전적인 눌림 재진입형 | 대응: 주력 관찰, 재안착/양봉 우대',
        '- 빨강수박: 재점화 직전 경계 구간 | 대응: 확인매수 대기',
        '- 파란점선: 실행 타점 | 대응: 분할 진입, 손절 명확화',
        '- 후행수박: 한 박자 늦은 자리 | 대응: 원칙적 관망',
        '- 참고: 끼 점수는 확신 구간만 표시하며, 재현 패턴·현재 위치·흡수 흔적을 함께 봅니다.',
    ]
    return '\n'.join(lines) + '\n'


def build_watermelon_summary_block(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return '🍉 [수박 상태 요약]\n- 집계 대상 없음\n'
    work = df.copy()
    if '수박최종상태' not in work.columns:
        work['수박최종상태'] = work.get('수박상태명', '')
    counts = work['수박최종상태'].fillna('').astype(str).value_counts().to_dict()
    intro_n = int(counts.get('초입수박', 0))
    pull_n = int(counts.get('눌림수박', 0))
    blue1_n = int(counts.get('Blue-1단기', 0))
    blue2_n = int(counts.get('Blue-2스윙', 0))
    late_n = int(counts.get('후행수박', 0))
    lines = [
        '🍉 [수박 상태 요약]',
        f'- 관찰군(초록): {intro_n + pull_n}개 = 초입 {intro_n} / 눌림 {pull_n}',
        f'- 재점화군(빨강): {blue1_n + blue2_n}개 = Blue-1 {blue1_n} / Blue-2 {blue2_n}',
        f'- 후행수박: {late_n}개',
    ]
    return '\n'.join(lines) + '\n'


def _render_kki_lines(item: dict) -> str:
    score = int(item.get('kki_score', 0) or 0)
    if SHOW_KKI_ONLY_WHEN_CONFIDENT and score < KKI_SHOW_MIN:
        return ''

    parts = []
    tag = str(item.get('kki_tag', '') or '').strip()
    absorb_tag = str(item.get('absorb_tag', '') or '').strip()
    absorb_score = int(item.get('absorb_score', 0) or 0)

    habit = str(item.get('kki_habit_comment', item.get('kki_habit', '')) or '').strip()
    current_state = str(item.get('kki_current_state', '') or '').strip()
    integrated = str(item.get('kki_reason', item.get('kki_comment', '')) or '').strip()
    best_band = str(item.get('kki_best_band', item.get('recommended_band', '')) or '').strip()
    support_band = str(item.get('kki_support_band', item.get('support_band', '')) or '').strip()

    line = f'- 🎭 끼점수: {tag} {score}'.rstrip()
    if absorb_tag:
        line += f' | {absorb_tag} {absorb_score}'
    parts.append(line)

    if habit:
        parts.append(f'- 🧬 과거 습성: {habit}')
    if current_state:
        parts.append(f'- 📍 현재 상태: {current_state}')
    if best_band:
        if support_band and support_band != best_band:
            parts.append(f'- 📐 밴드 적합성: 주밴드 {best_band} / 보조밴드 {support_band}')
        else:
            parts.append(f'- 📐 밴드 적합성: 주밴드 {best_band}')
    if integrated:
        parts.append(f'- 🧲 끼/흡수 해설: {integrated}')

    return '\n'.join(parts) + ('\n' if parts else '')


def build_watermelon_state_block(title: str, df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return f'🍉 [{title}]\n- 해당 종목 없음\n'
    lines = [f'🍉 [{title}]\n']
    for rank, (_, row) in enumerate(df.head(5).iterrows(), 1):
        item = enrich_row_with_human_commentary(row)
        name = str(item.get('종목명', item.get('name', '')) or '').strip()
        code = str(item.get('code', item.get('종목코드', '')) or '').strip()
        state = str(item.get('수박최종상태', item.get('수박상태명', '')) or '').strip()
        grade = str(item.get('수박상태등급', item.get('wm_state_grade', '')) or '').strip()
        cloud_tag = str(item.get('저항구름태그', '') or '').strip()
        refine_tag = str(item.get('수박정제태그', '') or '').strip()
        refine_check = build_refine_validation_text(item)
        easy = str(item.get('easy_interpretation', '') or '').strip()
        need_check = str(item.get('need_check', '') or '').strip()
        action = str(item.get('action_summary', '') or '').strip()
        caution = str(item.get('caution', '') or '').strip()
        final_label = str(item.get('final_label', '') or '').strip()
        score_summary = str(item.get('score_summary', '') or '').strip()
        card = (
            f'{rank}) {name}({code})\n'
            f'- 상태: {state} | 등급:{grade}\n'
            + (f'- 저항구름: {cloud_tag}\n' if cloud_tag else '')
            + (f'- 정제: {refine_tag}\n' if refine_tag else '')
            + (f'- 정제검증: {refine_check}\n' if refine_check else '')
            + _render_kki_lines(item)
            + (f'- 쉬운 해설: {easy}\n' if easy else '')
            + (f'- 확인 필요: {need_check}\n' if need_check else '')
            + (f'- 대응 요약: {action}\n' if action else '')
            + (f'- 최종 판정: {final_label}\n' if final_label else '')
            + (f'- 주의 포인트: {caution}\n' if caution else '')
            + (f'- 점수 해석: {score_summary}\n' if score_summary else '')
        )
        lines.append(card)
    return ''.join(lines).rstrip() + '\n'
