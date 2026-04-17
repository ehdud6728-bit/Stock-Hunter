from __future__ import annotations

from .utils import safe_int, row_text, row_get, score_band_breakout, score_band_n, score_band_preempt, score_band_safe
from .kki_profile import build_kki_profile


def get_pass_stage_bundle(row) -> dict:
    stage = row_text(row, '단계상태', default='').strip()
    if stage == 'PASS_B':
        return {
            'pass_stage': stage,
            'pass_title': '🟣 PASS_B | 강화 확인 구조 통과',
            'pass_desc': '기본 3단계 통과 후 재안착·거래량·저항 위치까지 추가 확인된 실전 우선 구조입니다.',
        }
    if stage == 'PASS_A':
        return {
            'pass_stage': stage,
            'pass_title': '✅ PASS_A | 매집→응축→돌파준비 3단계 통과',
            'pass_desc': '매집 + 응축 + 돌파준비 흐름이 순서대로 맞는 기본 구조 통과 상태입니다.',
        }
    return {
        'pass_stage': 'DROP',
        'pass_title': '⛔ DROP | 단계 시퀀스 미성립',
        'pass_desc': '단계 시퀀스 자체는 부족하지만, 상태·저항구름·정제 조합에 따라 실전 해석은 별도로 판단합니다.',
    }


def build_score_summary(row) -> str:
    preempt = safe_int(row_get(row, '선취점수', '단테점수', default=0))
    breakout = safe_int(row_get(row, '돌파점수', default=0))
    safe = safe_int(row_get(row, '안전점수', default=0))
    n_score = safe_int(row_get(row, 'N점수', default=0))
    parts = [f'선취:{score_band_preempt(preempt)}']
    if breakout:
        parts.append(f'돌파:{score_band_breakout(breakout)}')
    parts.append(f'안전:{score_band_safe(safe)}')
    parts.append(f'N:{score_band_n(n_score)}')
    return ' | '.join(parts)


def decide_final_label(row) -> str:
    wm_state = row_text(row, '수박최종상태', '수박상태명', default='')
    cloud = row_text(row, '저항구름상태', '흰구름상태', default='')
    fake = '가짜수박' in row_text(row, '수박정제태그', default='')
    good_refine = '정제수박' in row_text(row, '수박정제태그', default='')
    breakout = safe_int(row_get(row, '돌파점수', default=0))
    preempt = safe_int(row_get(row, '선취점수', '단테점수', default=0))

    if wm_state in ('Blue-1단기', 'Blue-2스윙'):
        return '즉시 대응' if good_refine else '즉시 관찰'
    if wm_state == '후행수박':
        return '추격 금지' if fake else '보유자 대응'
    if wm_state == '눌림수박':
        return '눌림 대기'
    if wm_state == '초입수박' and cloud == '저항전':
        return '선취 가능' if good_refine else '선취 대기'
    if cloud == '저항테스트':
        return '돌파 확인'
    if cloud == '저항돌파':
        if fake:
            return '눌림 대기'
        return '돌파 확인' if breakout >= 60 else '눌림 대기'
    if preempt >= 50:
        return '선취 대기'
    return '관망'


def build_check_needed_text(row) -> str:
    checks = []
    if not bool(row_get(row, '수박정제_vol_ok', default=True)):
        checks.append('거래량 보강: 반등일 거래량이 20일 평균 대비 1.3배 이상 붙는지 확인')
    if not bool(row_get(row, '수박정제_reclaim_ok', default=True)):
        checks.append('5일선 재안착: 5일선 위 종가 재안착과 다음 봉 저점 보존 확인')
    if not bool(row_get(row, '수박정제_candle_ok', default=True)):
        checks.append('양봉/캔들 유지: 장중 강세보다 종가 양봉 마감 확인')
    if not bool(row_get(row, '수박정제_wick_ok', default=True)):
        checks.append('윗꼬리 안정: 긴 윗꼬리 연속 발생 여부 확인')
    if not bool(row_get(row, '수박정제_long_ok', default=True)):
        checks.append('장기이평/중기저항 위치: MA112·중기저항 위 종가 유지 확인')
    if not bool(row_get(row, '수박정제_cloud_ok', default=True)):
        checks.append('돌파 후 지지 전환: 구름 상단 눌림 뒤 재양봉 확인')
    return ' / '.join(checks[:3])


def build_easy_interpretation(row) -> dict:
    wm_state = row_text(row, '수박최종상태', '수박상태명', default='')
    cloud = row_text(row, '저항구름상태', '흰구름상태', default='')
    fake = '가짜수박' in row_text(row, '수박정제태그', default='')
    good_refine = '정제수박' in row_text(row, '수박정제태그', default='')
    final_label = decide_final_label(row)
    need_check = build_check_needed_text(row)

    state_desc = {
        '초입수박': '바닥을 정리한 뒤 처음 살아나는 초입 구간입니다.',
        '눌림수박': '한 번 살아난 뒤 눌림을 주는 재진입 구간입니다.',
        '후행수박': '이미 한 박자 진행된 뒤라 신규 접근이 불리한 구간입니다.',
        'Blue-1단기': '재점화 첫 신호가 살아 있는 실행 초동 구간입니다.',
        'Blue-2스윙': '눌림 뒤 두 번째 재점화가 나온 실행형 스윙 구간입니다.',
    }.get(wm_state, '구조는 일부 보이지만 해석은 아직 조심해야 하는 구간입니다.')

    cloud_desc = {
        '저항전': '아직 저항구름 아래라 선취 관점이 가능합니다.',
        '저항테스트': '지금은 저항구름을 시험하는 중이라 확인이 먼저입니다.',
        '저항돌파': '이미 저항 위로 올라온 자리라 추격보다 눌림 확인이 더 중요합니다.',
    }.get(cloud, '저항 위치는 중립적으로 보고 상태와 정제를 더 우선해 해석합니다.')

    refine_desc = '정제수박이라 구조 품질은 비교적 양호합니다.' if good_refine else (
        '가짜수박주의가 있어 신호를 보수적으로 읽는 편이 낫습니다.' if fake else '관찰수박 단계라 한두 가지 확인 요소가 더 필요합니다.'
    )

    kki = build_kki_profile(row)
    kki_desc = ''
    if kki.get('kki_show'):
        if kki.get('kki_recurrence'):
            kki_desc = f"과거 재현축은 '{kki.get('kki_recurrence')}' 쪽에 가깝고, 현재는 {kki.get('kki_current_state', '중립 구조')}입니다."
        else:
            kki_desc = f"현재는 {kki.get('kki_current_state', '중립 구조')}입니다."

    if final_label in ('즉시 대응', '즉시 관찰'):
        action = '거래량 유지와 구름 상단 안착, 재돌파 양봉 중 하나가 확인되면 실행 대응으로 올립니다.'
        caution = '돌파처럼 보여도 안착이 없으면 하루 반짝 후 다시 밀릴 수 있습니다.'
    elif final_label in ('선취 가능', '선취 대기'):
        action = '소액 분할 또는 관찰이 유리하며, 5일선 재안착과 거래량 보강을 먼저 확인합니다.'
        caution = '확인 전 진입은 실패 확률이 높을 수 있습니다.'
    elif final_label in ('보유자 대응', '추격 금지'):
        action = '신규 추격보다 눌림 재확인, 기존 보유분은 이탈 기준과 분할 대응 기준을 먼저 정합니다.'
        caution = '후행 구간은 좋은 종목이어도 진입 타점이 늦어질 수 있습니다.'
    else:
        action = '구름 상단 안착, 거래량 재유입, 재안착 신호가 나오는지 더 지켜봅니다.'
        caution = '상태·저항구름·정제 중 하나라도 약하면 해석을 보수적으로 해야 합니다.'

    easy = ' '.join([x for x in [state_desc, cloud_desc, refine_desc, kki_desc] if x])
    return {
        'easy_interpretation': easy,
        'need_check': need_check,
        'action_summary': action,
        'caution': caution,
        'final_label': final_label,
    }


def enrich_row_with_human_commentary(row):
    base = dict(row) if isinstance(row, dict) else row.to_dict()
    pass_bundle = get_pass_stage_bundle(base)
    easy = build_easy_interpretation(base)
    score_summary = build_score_summary(base)
    kki = build_kki_profile(base)

    base['pass_stage'] = pass_bundle['pass_stage']
    base['pass_title'] = pass_bundle['pass_title']
    base['pass_desc'] = pass_bundle['pass_desc']
    base['easy_interpretation'] = easy['easy_interpretation']
    base['action_summary'] = easy['action_summary']
    base['caution'] = easy['caution']
    base['final_label'] = easy['final_label']
    base['need_check'] = easy.get('need_check', '')
    base['score_summary'] = score_summary
    base['kki_score'] = kki['kki_score']
    base['kki_tag'] = kki['kki_tag']
    base['absorb_score'] = kki['absorb_score']
    base['absorb_tag'] = kki['absorb_tag']
    base['kki_reason'] = kki['kki_reason']
    base['kki_recurrence'] = kki.get('kki_recurrence', '')
    base['kki_current_state'] = kki.get('kki_current_state', '')
    base['kki_show'] = kki.get('kki_show', False)

    block_lines = []
    if base['pass_title']:
        block_lines.append(f"🧩 PASS 해석: {base['pass_title']}")
    if base['pass_desc']:
        block_lines.append(f"→ {base['pass_desc']}")
    if base.get('kki_show') and base.get('kki_tag'):
        kline = f"🎭 끼점수: {base['kki_tag']} {base['kki_score']}"
        if base.get('absorb_tag'):
            kline += f" | {base['absorb_tag']} {base['absorb_score']}"
        block_lines.append(kline)
    if base.get('kki_show') and base.get('kki_recurrence'):
        block_lines.append(f"🧬 끼 재현이력: {base['kki_recurrence']}")
    if base.get('kki_show') and base.get('kki_current_state'):
        block_lines.append(f"📍 현재 끼 위치: {base['kki_current_state']}")
    if base.get('kki_show') and base.get('kki_reason'):
        block_lines.append(f"🧲 끼/흡수 해설: {base['kki_reason']}")
    if base['easy_interpretation']:
        block_lines.append(f"🧠 쉬운 해석: {base['easy_interpretation']}")
    if base.get('need_check'):
        block_lines.append(f"🔍 확인 필요: {base['need_check']}")
    if base['action_summary']:
        block_lines.append(f"🎯 대응 요약: {base['action_summary']}")
    if base['caution']:
        block_lines.append(f"⚠️ 주의 포인트: {base['caution']}")
    if base['final_label']:
        block_lines.append(f"✅ 최종 판정: {base['final_label']}")
    if base['score_summary']:
        block_lines.append(f"🧮 점수 해석: {base['score_summary']}")
    base['interpretation_block'] = "\n".join(block_lines)
    return base
