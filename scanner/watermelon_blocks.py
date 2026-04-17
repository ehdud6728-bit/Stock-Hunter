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
    ]
    return '
'.join(lines) + '
'


def build_watermelon_summary_block(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return '🍉 [수박 상태 요약]
- 집계 대상 없음
'
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
    return '
'.join(lines) + '
'


def _render_kki_lines(item: dict) -> str:
    score = item.get('kki_score', item.get('끼점수수치', 0) or 0)
    tag = item.get('kki_tag', item.get('끼점수태그', '') or '')
    explain = item.get('kki_commentary', item.get('끼해설', '') or '')
    recurrence = item.get('kki_recurrence', item.get('끼재현요약', '') or '')
    current_state = item.get('kki_state', item.get('끼현재상태', '') or '')
    if SHOW_KKI_ONLY_WHEN_CONFIDENT and int(score or 0) < KKI_SHOW_MIN:
        return ''
    parts = []
    if tag or score:
        parts.append(f'- 🎭 끼점수: {tag} {score}'.rstrip())
    if recurrence:
        parts.append(f'- 🧲 끼 재현이력: {recurrence}')
    if current_state:
        parts.append(f'- 📍 현재 끼 위치: {current_state}')
    if explain:
        parts.append(f'- 💬 끼 해설: {explain}')
    return '
'.join(parts) + ('
' if parts else '')


def build_watermelon_state_block(title: str, df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return f'🍉 [{title}]
- 해당 종목 없음
'
    lines = [f'🍉 [{title}]
']
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
            f'{rank}) {name}({code})
'
            f'- 상태: {state} | 등급:{grade}
'
            + (f'- 저항구름: {cloud_tag}
' if cloud_tag else '')
            + (f'- 정제: {refine_tag}
' if refine_tag else '')
            + (f'- 정제검증: {refine_check}
' if refine_check else '')
            + _render_kki_lines(item)
            + (f'- 쉬운 해설: {easy}
' if easy else '')
            + (f'- 확인 필요: {need_check}
' if need_check else '')
            + (f'- 대응 요약: {action}
' if action else '')
            + (f'- 주의 포인트: {caution}
' if caution else '')
            + (f'- 최종 판정: {final_label}
' if final_label else '')
            + (f'- 점수 해석: {score_summary}
' if score_summary else '')
        )
        lines.append(card)
    return ''.join(lines).rstrip() + '
'
