from __future__ import annotations

import numpy as np
import pandas as pd

from .config import KKI_SHOW_MIN, SHOW_KKI_ONLY_WHEN_CONFIDENT
from .narrative import enrich_row_with_human_commentary
from .watermelon_refine import build_refine_validation_text


DIVIDER = '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'


def _text_block_header(icon: str, title: str) -> str:
    return f"{icon} [{title}]\n\n"


def _wrap_stock_card(card_text: str, rank: int = 1) -> str:
    body = str(card_text or '').strip()
    if not body:
        return ''
    return f"{DIVIDER}\n{body}\n{DIVIDER}\n\n"


def build_watermelon_guide_block() -> str:
    lines = [
        '🍉 [수박 상태 가이드]',
        '- 초입수박: 구조가 막 살아나는 초동 회복형 | 대응: 소액 탐색, 거래량·5일재안착 확인',
        '- 눌림수박: 가장 실전적인 눌림 재진입형 | 대응: 주력 관찰, 재안착/양봉 우대',
        '- 빨강수박: 재점화 직전 경계 구간 | 대응: 확인매수 대기',
        '- 파란점선: 실행 타점 | 대응: 분할 진입, 손절 명확화',
        '- 후행수박: 한 박자 늦은 자리 | 대응: 원칙적 관망',
        '- 참고: 초록은 독립 상태가 아니라 초입/눌림 관찰군 요약입니다',
        '- 저항구름: 미래 저항 구름의 저항전/저항테스트/저항돌파 해석 참고',
        '- 정제수박: 좋은 수박/관찰수박/가짜수박주의를 구분하는 보조 필터',
        '- 참고: STRICT_FAKE_FILTER=1 이면 액션 블록에서 가짜수박주의 종목을 자동 제외',
    ]
    return _wrap_stock_card('\n'.join(lines), rank=0).rstrip() + '\n'


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
    blue2_pre_n = int(counts.get('Blue-2예비', 0))
    late_n = int(counts.get('후행수박', 0))

    total_green = intro_n + pull_n
    total_red = blue1_n + blue2_n + blue2_pre_n

    lines = [
        '🍉 [수박 상태 요약]',
        f'- 관찰군(초록): {total_green}개 = 초입 {intro_n} / 눌림 {pull_n}',
        f'- 재점화군(빨강): {total_red}개 = Blue-1 {blue1_n} / Blue-2 {blue2_n} / 예비 {blue2_pre_n}',
        f'- 후행수박: {late_n}개',
    ]
    if total_green > 0 and total_red == 0:
        lines.append('- 해석: 관찰군은 있으나 재점화 확정은 아직 적음')
    elif total_red > 0:
        lines.append('- 해석: 재점화 후보가 살아 있어 실행형 후보를 함께 점검할 구간')
    return _wrap_stock_card('\n'.join(lines), rank=0).rstrip() + '\n'


def _render_kki_lines(item: dict) -> str:
    score = int(item.get('kki_score', 0) or 0)
    if SHOW_KKI_ONLY_WHEN_CONFIDENT and score < KKI_SHOW_MIN:
        return ''
    parts = []
    tag = str(item.get('kki_tag', '') or '').strip()
    absorb_tag = str(item.get('absorb_tag', '') or '').strip()
    absorb_score = int(item.get('absorb_score', 0) or 0)
    recurrence = str(item.get('kki_recurrence', '') or '').strip()
    current_state = str(item.get('kki_current_state', '') or '').strip()
    explain = str(item.get('kki_reason', '') or '').strip()
    if tag or score:
        line = f'- 🎭 끼점수: {tag} {score}'.rstrip()
        if absorb_tag:
            line += f' | {absorb_tag} {absorb_score}'
        parts.append(line)
    if recurrence:
        parts.append(f'- 🧬 끼 재현이력: {recurrence}')
    if current_state:
        parts.append(f'- 📍 현재 끼 위치: {current_state}')
    if explain:
        parts.append(f'- 🧲 끼/흡수 해설: {explain}')
    return '\n'.join(parts) + ('\n' if parts else '')


def build_watermelon_state_block(title: str, df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return f'🍉 [{title}]\n- 해당 종목 없음\n'

    def _safe_int(v, default=0):
        try:
            return int(round(float(v)))
        except Exception:
            return default

    lines = [_text_block_header('🍉', title)]
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

        small_low = _safe_int(item.get('소파동저점', 0), 0)
        small_high = _safe_int(item.get('소파동전고점', 0), 0)
        small_state = str(item.get('소파동위치', '') or '').strip()
        small_comment = str(item.get('소파동코멘트', '') or '').strip()

        mid_low = _safe_int(item.get('중파동저점', 0), 0)
        mid_high = _safe_int(item.get('중파동전고점', 0), 0)
        mid_state = str(item.get('중파동위치', '') or '').strip()
        mid_comment = str(item.get('중파동코멘트', '') or '').strip()

        bz_low = _safe_int(item.get('파란타점영역하단', 0), 0)
        bz_high = _safe_int(item.get('파란타점영역상단', 0), 0)

        pos_label = str(item.get('자리평가라벨', '') or '').strip()
        pos_score = _safe_int(item.get('자리평가점수', 0), 0)
        pos_comment = str(item.get('자리평가코멘트', '') or '').strip()

        wave_small = ''
        if small_low > 0 and small_high > 0:
            wave_small = f'저점 {small_low:,} → 전고점 {small_high:,}'
        elif small_state or small_comment:
            wave_small = '계산됨 [BOOTCHECK]'
        if wave_small:
            if small_state:
                wave_small += f' | {small_state}'
            if small_comment:
                wave_small += f' | {small_comment}'

        wave_mid = ''
        if mid_low > 0 and mid_high > 0:
            wave_mid = f'저점 {mid_low:,} → 전고점 {mid_high:,}'
        elif mid_state or mid_comment:
            wave_mid = '계산됨 [BOOTCHECK]'
        if wave_mid:
            if mid_state:
                wave_mid += f' | {mid_state}'
            if mid_comment:
                wave_mid += f' | {mid_comment}'

        wave_blue = f'{bz_low:,} ~ {bz_high:,} | 점이 아니라 지지 확인 구간' if (bz_low > 0 and bz_high > 0) else ''
        wave_pos = ''
        if pos_label:
            wave_pos = f'{pos_label} ({pos_score})'
            if pos_comment:
                wave_pos += f' | {pos_comment}'

        wave_debug = ''
        debug_note = str(item.get('파동디버그노트', item.get('wave_debug_note', '')) or '').strip()
        debug_cols = str(item.get('파동디버그컬럼', item.get('wave_debug_cols', '')) or '').strip()
        debug_len = _safe_int(item.get('파동디버그길이', item.get('wave_debug_len', 0)), 0)
        accum_found = _safe_int(item.get('매집봉발견', item.get('accum_found', 0)), 0)
        if (not wave_small) or (not wave_mid):
            wave_debug = (
                f'accum_found={accum_found} | '
                f'small={small_low}/{small_high}/{small_state or "-"} | '
                f'mid={mid_low}/{mid_high}/{mid_state or "-"} | '
                f'blue={bz_low}/{bz_high} | '
                f'note={debug_note or "-"} | '
                f'cols={debug_cols or "-"} | '
                f'len={debug_len}'
            )

        card = (
            f'{rank}) {name}({code})\n'
            f'- 상태: {state} | 등급:{grade}\n'
            + (f'- 저항구름: {cloud_tag}\n' if cloud_tag else '')
            + (f'- 정제: {refine_tag}\n' if refine_tag else '')
            + (f'- 정제검증: {refine_check}\n' if refine_check else '')
            + (f'- 〰️ 소파동: {wave_small}\n' if wave_small else '')
            + (f'- 📶 중파동: {wave_mid}\n' if wave_mid else '')
            + (f'- 🔵 파란타점: {wave_blue}\n' if wave_blue else '')
            + (f'- 🧮 자리평가: {wave_pos}\n' if wave_pos else '')
            + (f'- 🧪 파동디버그: {wave_debug}\n' if wave_debug else '')
            + _render_kki_lines(item)
            + (f'- 쉬운 해설: {easy}\n' if easy else '')
            + (f'- 확인 필요: {need_check}\n' if need_check else '')
            + (f'- 대응 요약: {action}\n' if action else '')
            + (f'- 최종 판정: {final_label}\n' if final_label else '')
            + (f'- 주의 포인트: {caution}\n' if caution else '')
            + (f'- 점수 해석: {score_summary}\n' if score_summary else '')
        )
        lines.append(_wrap_stock_card(card, rank=rank))
    return ''.join(lines).rstrip() + '\n'


def build_watermelon_debug_block(title: str, df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return f'🔬 [{title}]\n- 해당 종목 없음\n'

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

    lines = [_text_block_header('🔬', title)]
    for rank, (_, row) in enumerate(df.head(5).iterrows(), 1):
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
        card = (
            f"{rank}) {name}({code})\n"
            f"- 최종상태: {state}\n"
            f"- gate: {' / '.join(gate_parts)}\n"
            f"- intro_sub: {' / '.join(intro_sub_parts)}\n"
            f"- red2_sub: {' / '.join(red2_sub_parts)}\n"
        )
        lines.append(_wrap_stock_card(card, rank=rank))
    return ''.join(lines).rstrip() + '\n'
