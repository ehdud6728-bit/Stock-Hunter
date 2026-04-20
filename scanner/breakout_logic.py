from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from .narrative import enrich_row_with_human_commentary, append_interpretation_to_block
from .watermelon_refine import build_refine_validation_text

"""실제 로직 이관형 breakout_logic 모듈.
흰구름/선취/돌파 관찰 블록 및 상태 계산 로직을 분리했다.
"""

def _wc_safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def _wc_ema(series, span=5):
    try:
        return series.ewm(span=span, adjust=False).mean()
    except Exception:
        return series

def add_white_cloud_candidates(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    work = df.copy()
    if 'MA112' not in work.columns and 'Close' in work.columns:
        work['MA112'] = work['Close'].rolling(112).mean()
    if 'MA224' not in work.columns and 'Close' in work.columns:
        work['MA224'] = work['Close'].rolling(224).mean()
    if 'MA112' not in work.columns or 'MA224' not in work.columns:
        return work
    ma112 = work['MA112']
    ma224 = work['MA224']
    wc_a_mid = (ma112 + ma224) / 2.0
    work['WC_A_TOP'] = _wc_ema(wc_a_mid, span=5)
    work['WC_A_BOT'] = ma224
    work['WC_B_TOP'] = ma112 * 1.02
    work['WC_B_BOT'] = ma224
    work['WC_C_TOP'] = _wc_ema(ma112, span=5) * 1.01
    work['WC_C_BOT'] = ma224
    return work

def detect_white_cloud_state(df: pd.DataFrame, i: int, mode: str = "A") -> dict:
    empty = {'below': False, 'inside': False, 'above': False, 'near_lower': False, 'top': 0.0, 'bottom': 0.0, 'tag': '', 'comment': ''}
    try:
        if df is None or i < 0 or i >= len(df):
            return empty.copy()
        row = df.iloc[i]
        close_p = _wc_safe_float(row.get('Close', 0))
        mode = str(mode or 'A').upper().strip()
        if mode == 'A':
            top = _wc_safe_float(row.get('WC_A_TOP', 0))
            bottom = _wc_safe_float(row.get('WC_A_BOT', 0))
        elif mode == 'B':
            top = _wc_safe_float(row.get('WC_B_TOP', 0))
            bottom = _wc_safe_float(row.get('WC_B_BOT', 0))
        else:
            top = _wc_safe_float(row.get('WC_C_TOP', 0))
            bottom = _wc_safe_float(row.get('WC_C_BOT', 0))
        if top <= 0 or bottom <= 0:
            return empty.copy()
        upper = max(top, bottom)
        lower = min(top, bottom)
        below = close_p < lower
        inside = lower <= close_p <= upper
        above = close_p > upper
        near_lower = below and (close_p >= lower * 0.90)
        tag = ''
        comment = ''
        if near_lower:
            tag = f'☁{mode}-아래근접'
            comment = '흰배경 아래 선취 가능 구간'
        elif below:
            tag = f'☁{mode}-아래'
            comment = '장기 저항 아래 구간'
        elif inside:
            tag = f'☁{mode}-안'
            comment = '구름 내부 저항 확인 구간'
        elif above:
            tag = f'☁{mode}-위'
            comment = '이미 장기 저항 위, 추격 주의'
        return {'below': bool(below), 'inside': bool(inside), 'above': bool(above), 'near_lower': bool(near_lower), 'top': round(upper, 2), 'bottom': round(lower, 2), 'tag': tag, 'comment': comment}
    except Exception:
        return empty.copy()

def detect_white_cloud_vote(df: pd.DataFrame, i: int) -> dict:
    a = detect_white_cloud_state(df, i, 'A')
    b = detect_white_cloud_state(df, i, 'B')
    c = detect_white_cloud_state(df, i, 'C')
    below_n = int(a['below']) + int(b['below']) + int(c['below'])
    inside_n = int(a['inside']) + int(b['inside']) + int(c['inside'])
    above_n = int(a['above']) + int(b['above']) + int(c['above'])
    near_n = int(a['near_lower']) + int(b['near_lower']) + int(c['near_lower'])
    state = 'mixed'
    if below_n >= 2:
        state = 'below'
    elif inside_n >= 2:
        state = 'inside'
    elif above_n >= 2:
        state = 'above'
    tag = ''
    comment = ''
    if near_n >= 2:
        tag = '☁아래근접'
        comment = '흰배경 아래 선취 후보'
    elif state == 'below':
        tag = '☁아래'
        comment = '장기 저항 아래 구간'
    elif state == 'inside':
        tag = '☁안'
        comment = '구름 내부 저항 확인 구간'
    elif state == 'above':
        tag = '☁위'
        comment = '이미 장기 저항 위, 추격 주의'
    else:
        tag = '☁혼합'
        comment = '후보별 판정이 엇갈림'
    return {'A': a, 'B': b, 'C': c, 'state': state, 'tag': tag, 'comment': comment, 'below_n': below_n, 'inside_n': inside_n, 'above_n': above_n, 'near_n': near_n}

def classify_resistance_cloud_phase_from_bundle(white_cloud_bundle: dict, wm_refine_bundle: Optional[dict] = None, wm_bundle: Optional[dict] = None) -> dict:
    wc = dict(white_cloud_bundle or {})
    refine = dict(wm_refine_bundle or {})
    wm = dict(wm_bundle or {})
    state = str(wc.get('state', '') or '').strip()
    near_n = int(wc.get('near_n', 0) or 0)
    late = bool(wm.get('wm_late', False))
    strong_energy = bool(refine.get('ok', False) and refine.get('vol_ok', False) and refine.get('candle_ok', False) and not late)
    phase = '저항혼합'
    tag = '☁ 저항혼합'
    comment = '미래 저항 구름 해석이 엇갈림'
    if state == 'below':
        phase = '저항전'
        tag = '☁ 저항전'
        comment = '미래 저항 구름 아래 선취 가능 구간' if near_n >= 2 else '저항 구름 아래지만 아직 거리가 있음'
    elif state == 'inside':
        phase = '저항테스트'
        tag = '☁ 저항테스트'
        comment = '미래 저항 구름을 시험하는 구간, 힘 확인 필요'
    elif state == 'above':
        phase = '저항돌파'
        tag = '☁ 저항돌파'
        comment = '미래 저항 구름 상향 돌파, 강한 진행형' if strong_energy else '저항 구름 위 진행형, 추격보다 눌림 대응'
    elif state == 'mixed':
        phase = '저항혼합'
        tag = '☁ 저항혼합'
        comment = '저항 위치 판단이 혼합, 보조 참고'
    return {'phase': phase, 'tag': tag, 'comment': comment, 'strong_energy': bool(strong_energy)}

def _is_breakout_priority_type(row) -> bool:
    def _b(x):
        try: return bool(x)
        except Exception: return False
    def _f(x, default=0.0):
        try: return float(x)
        except Exception: return default
    wc_state = str(row.get('저항구름상태', '') or '').strip()
    wm_state = str(row.get('수박최종상태', row.get('최종상태', row.get('상태', ''))) or '').strip()
    if not wm_state:
        wm_state = str(row.get('돌파상태', row.get('선취상태', '')) or '').strip()
    strong_energy = _b(row.get('저항구름강에너지', False))
    late_flag = _b(row.get('수박디버그_late', False))
    ma224 = _f(row.get('MA224', row.get('ma224', 0)))
    close_p = _f(row.get('현재가', row.get('Close', row.get('close', 0))))
    disparity = _f(row.get('이격', row.get('Disparity', 0)))
    above_ma224 = bool(ma224 > 0 and close_p >= ma224 * 1.02)
    progressing_state = wm_state in ('Blue-1단기', 'Blue-2스윙', '후행수박')
    overheated = disparity >= 108
    if wc_state == '저항돌파' and (strong_energy or above_ma224 or progressing_state): return True
    if wc_state == '저항테스트' and strong_energy and above_ma224 and progressing_state and not late_flag: return True
    if strong_energy and above_ma224 and overheated: return True
    return False

def compute_breakout_mode_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    work = df.copy()
    def _b(x):
        try: return bool(x)
        except Exception: return False
    def _f(x, default=0.0):
        try: return float(x)
        except Exception: return default
    scores, states, comments, reasons = [], [], [], []
    for _, row in work.iterrows():
        rc_state = str(row.get('저항구름상태', '') or '').strip()
        wm_state = _resolve_track_display_state(row, track='breakout')
        late = _b(row.get('수박디버그_late', False))
        breakout_priority = _is_breakout_priority_type(row)
        score = 0
        rs = []
        if rc_state == '저항돌파': score += 30; rs.append('저항돌파')
        elif rc_state == '저항테스트': score += 12; rs.append('저항테스트')
        elif rc_state == '저항전': score -= 8; rs.append('저항전')
        else: score -= 6
        if _b(row.get('저항구름강에너지', False)): score += 20; rs.append('강에너지')
        if breakout_priority: score += 14; rs.append('돌파우선형')
        if _b(row.get('수박정제통과', False)): score += 12; rs.append('정제통과')
        elif _b(row.get('수박정제관찰', False)): score += 5; rs.append('정제관찰')
        elif _b(row.get('수박정제주의', False)): score -= 10; rs.append('정제주의')
        if _b(row.get('수박정제_vol_ok', False)): score += 8
        if _b(row.get('수박정제_candle_ok', False)): score += 8
        if _b(row.get('수박정제_obv_ok', False)): score += 5
        if _b(row.get('5일재안착', False)): score += 8; rs.append('재안착')
        elif _b(row.get('5일재안착예비', False)): score += 3
        if wm_state == 'Blue-2스윙': score += 10; rs.append('Blue-2스윙')
        elif wm_state == 'Blue-1단기': score += 8; rs.append('Blue-1')
        elif wm_state in ('초입수박', '눌림수박', 'Blue-2예비'): score += 4
        disparity = _f(row.get('이격', row.get('Disparity', 0)))
        if disparity >= 125: score -= 18; rs.append('과열')
        elif disparity >= 118: score -= 10; rs.append('이격과열')
        if late: score -= 15; rs.append('late')
        if rc_state == '저항돌파' and (_b(row.get('저항구름강에너지', False)) or breakout_priority) and not late and score >= 45:
            b_state = '흰구름돌파형'
            b_comment = '미래 저항 구름을 강하게 돌파한 진행형 후보' if not breakout_priority else '돌파우선형: 선취 대응보다 저항구름 돌파 대응 우선'
        elif rc_state in ('저항돌파', '저항테스트') and score >= 28 and not late:
            b_state = '돌파관찰형'
            b_comment = '구름 돌파 또는 테스트 구간, 힘 확인 후 대응'
        else:
            b_state = '돌파제외형'
            b_comment = '돌파 에너지 부족 또는 과열/후행 가능성'
        scores.append(int(score)); states.append(b_state); comments.append(b_comment); reasons.append(' | '.join(rs[:6]))
    work['돌파점수'] = scores
    work['돌파상태'] = states
    work['돌파코멘트'] = comments
    work['돌파이유'] = reasons
    return work

def _resolve_track_display_state(row: pd.Series, track: str = "preempt") -> str:
    wm_state = str(row.get('수박최종상태', row.get('최종상태', row.get('상태', ''))) or '').strip()
    if wm_state:
        return wm_state
    rc_state = str(row.get('저항구름상태', '') or '').strip()
    breakout_priority = _is_breakout_priority_type(row)
    if track == 'preempt':
        pre_state = str(row.get('선취상태', row.get('단테상태', '')) or '').strip()
        if pre_state == '선취형': return '선취형'
        if pre_state == '선취관찰형': return '구조관찰'
        if pre_state == '선취제외형': return '선취제외'
        if rc_state == '저항전': return '저항전관찰'
        if rc_state == '저항테스트': return '저항테스트관찰'
        return '중립관찰'
    breakout_state = str(row.get('돌파상태', '') or '').strip()
    if breakout_state == '흰구름돌파형': return '돌파우선형' if breakout_priority else '구름돌파형'
    if breakout_state == '돌파관찰형': return '구름테스트관찰' if rc_state == '저항테스트' else '돌파관찰형'
    if rc_state == '저항돌파': return '돌파관찰형'
    if rc_state == '저항테스트': return '구름테스트관찰'
    return '중립관찰'

def _text_block_header(icon: str, title: str) -> str:
    return f"{icon} [{title}]\n\n"

def _join_stock_cards(cards: list[str]) -> str:
    cleaned = [str(c).rstrip() for c in cards if str(c).strip()]
    if not cleaned:
        return ''
    return '\n\n'.join(cleaned).rstrip() + '\n'

def _strict_fake_filter_enabled() -> bool:
    return str(os.getenv('STRICT_FAKE_FILTER', '0')).strip().lower() in ('1', 'true', 'y', 'yes', 'on')

def _is_fake_caution_row(row) -> bool:
    try:
        if bool(row.get('수박정제주의', False)):
            return True
    except Exception:
        pass
    tag = str(row.get('수박정제태그', '') or '').strip()
    comment = str(row.get('수박정제코멘트', '') or '').strip()
    return ('가짜수박주의' in tag) or ('가짜수박' in comment)

def _filter_fake_rows_for_actionable_block(df: pd.DataFrame, title: str) -> pd.DataFrame:
    if df is None or df.empty or not _strict_fake_filter_enabled():
        return df
    title = str(title or '')
    filter_keywords = ('선취 후보', '선취 관찰', '흰구름 돌파 후보', '흰구름 돌파 관찰', '초입수박', '눌림수박', '파란점선', 'Blue-2')
    if not any(k in title for k in filter_keywords):
        return df
    try:
        return df[~df.apply(_is_fake_caution_row, axis=1)].copy()
    except Exception:
        return df

def _build_candidate_explain_lines(row: pd.Series, track: str = "preempt") -> tuple[str, str, str]:
    def _b(x):
        try: return bool(x)
        except Exception: return False
    def _f(x, default=0.0):
        try: return float(x)
        except Exception: return default
    wm_state = _resolve_track_display_state(row, track='breakout')
    rc_state = str(row.get('저항구름상태', '') or '').strip()
    refine = '정제통과' if _b(row.get('수박정제통과', False)) else ('정제관찰' if _b(row.get('수박정제관찰', False)) else ('정제주의' if _b(row.get('수박정제주의', False)) else ''))
    has_reanchor = _b(row.get('5일재안착', False))
    has_reanchor_preview = _b(row.get('5일재안착예비', False))
    breakout_priority = _is_breakout_priority_type(row)
    late_flag = _b(row.get('수박디버그_late', False)) or (wm_state == '후행수박')
    disparity = _f(row.get('이격', row.get('Disparity', 0)))
    strong_energy = _b(row.get('저항구름강에너지', False))
    reasons, lacks = [], []
    if track == 'preempt':
        if wm_state in ('초입수박', '눌림수박', 'Blue-2예비'): reasons.append(wm_state)
        elif wm_state: reasons.append(wm_state)
        if rc_state == '저항전': reasons.append('저항전')
        elif rc_state == '저항테스트': reasons.append('저항테스트')
        elif rc_state == '저항혼합': lacks.append('저항 위치 추가 확인')
        elif rc_state == '저항돌파': lacks.append('선취보다 눌림 확인')
        if has_reanchor: reasons.append('5일재안착')
        else:
            lacks.append('5일재안착 미확인')
            if has_reanchor_preview: lacks.append('5일재안착 예비 확인')
        if refine == '정제통과': reasons.append('정제통과')
        elif refine == '정제관찰': reasons.append('정제관찰'); lacks.append('정제 추가 확인')
        elif refine == '정제주의': lacks.append('정제 보완 필요')
        if breakout_priority: lacks.append('돌파우선형 여부 확인')
        if late_flag: lacks.append('후행 구간 주의')
        if disparity >= 118: lacks.append('이격 과열')
        elif disparity >= 112: lacks.append('이격 부담')
        if rc_state == '저항전' and not late_flag:
            action = '소액 선진입보다 분할관찰이 유리하며, 5일선 재안착과 양봉 유지 시 대응합니다.' if not has_reanchor else '소액 분할 접근 가능하나, 5일선 이탈 시 보수적으로 대응합니다.'
        else:
            action = '바로 추격보다 한 번 더 확인이 유리하며, 재안착이나 거래량 보강 후 대응합니다.'
    else:
        if rc_state in ('저항돌파', '저항테스트'): reasons.append(rc_state)
        if strong_energy: reasons.append('강에너지')
        if breakout_priority: reasons.append('돌파우선형')
        if has_reanchor: reasons.append('재안착')
        elif has_reanchor_preview: lacks.append('재안착 예비 확인')
        if refine == '정제통과': reasons.append('정제통과')
        elif refine == '정제관찰': reasons.append('정제관찰'); lacks.append('정제 추가 확인')
        elif refine == '정제주의': lacks.append('정제 보완 필요')
        if wm_state in ('초입수박', '눌림수박', 'Blue-2예비'): reasons.append(wm_state)
        elif wm_state == '후행수박': lacks.append('후행 구간 주의')
        if rc_state == '저항테스트': lacks.append('구름 상단 안착 확인')
        if not strong_energy and rc_state == '저항돌파': lacks.append('거래량/힘 유지 확인')
        if disparity >= 118: lacks.append('이격 과열')
        elif disparity >= 112: lacks.append('추격 부담')
        if late_flag: lacks.append('후행 구간 주의')
        if rc_state == '저항돌파':
            action = '돌파 추격보다 구름 상단 또는 5일선 눌림 확인 후 대응하는 편이 유리합니다.'
        else:
            action = '돌파 테스트 구간으로, 거래량 유지와 구름 상단 안착을 확인한 뒤 대응합니다.'
    def _uniq(items):
        out, seen = [], set()
        for x in items:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out
    reasons = _uniq(reasons); lacks = _uniq(lacks)
    selected = ' + '.join(reasons[:4]) if reasons else '핵심 근거 집계 중'
    lacking = ' / '.join(lacks[:3]) if lacks else ('구조는 양호하나 추격 여부는 별도 확인' if track == 'breakout' else '큰 부족은 없지만 위치 확인 후 대응')
    return selected, lacking, action

def build_breakout_state_block(title: str, df: pd.DataFrame) -> str:
    df = _filter_fake_rows_for_actionable_block(df, title)
    if df is None or df.empty:
        return f"🚀 [{title}]\n- 해당 종목 없음\n"
    header = f"🚀 [{title}]"
    cards = []
    for rank, (_, row) in enumerate(df.head(5).iterrows(), start=1):
        item2 = enrich_row_with_human_commentary(row)
        name = str(item2.get('종목명', item2.get('name', '')) or '').strip()
        code = str(item2.get('code', item2.get('종목코드', '')) or '').strip()
        wm_state = _resolve_track_display_state(item2, track='breakout')
        score = int(float(item2.get('돌파점수', 0) or 0))
        comment = str(item2.get('돌파코멘트', '') or '').strip()
        reason = str(item2.get('돌파이유', '') or '').strip()
        rc_tag = str(item2.get('저항구름태그', '') or '').strip()
        refine_tag = str(item2.get('수박정제태그', '') or '').strip()
        refine_check = build_refine_validation_text(item2)
        selected, lacking, action = _build_candidate_explain_lines(item2, track='breakout')
        card = (
            f"{rank}) {name}({code})\n"
            f"- 상태: {wm_state} | 돌파점수:{score}\n"
            + (f"- 저항구름: {rc_tag}\n" if rc_tag else '')
            + (f"- 정제: {refine_tag}\n" if refine_tag else '')
            + (f"- 정제검증: {refine_check}\n" if refine_check else '')
            + (f"- 해석: {comment}\n" if comment else '')
            + (f"- 이유: {reason}\n" if reason else '')
            + (f"- 선정 이유: {selected}\n" if selected else '')
            + (f"- 부족한 점: {lacking}\n" if lacking else '')
            + (f"- 추천 대응: {action}\n" if action else '')
        )
        cards.append(append_interpretation_to_block(card, item2).rstrip())
    return header + '\n\n' + _join_stock_cards(cards)
