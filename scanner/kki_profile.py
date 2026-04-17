from __future__ import annotations

from .config import SHOW_KKI_ONLY_WHEN_CONFIDENT, KKI_SHOW_MIN, KKI_HIGH, KKI_MEDIUM, KKI_VOL_GOOD, KKI_VOL_STRONG, BB_SQUEEZE_MID, BB_SQUEEZE_STRONG
from .utils import row_num, row_text, boolish, text_join


def _best_band_label(row) -> tuple[str, list[str]]:
    notes = []
    candidates = []

    bb20w = row_num(row, 'BB20_Width', 'BB20_WIDTH', default=99)
    bb40w = row_num(row, 'BB40_Width', 'BB40_WIDTH', default=99)
    env20p = row_num(row, 'Env20_Pct', 'env20_pct', default=999)
    env40p = row_num(row, 'Env40_Pct', 'env40_pct', default=999)
    pct_b20 = row_num(row, 'BB20_PercentB', 'BB20_%B', default=0.5)
    pct_b40 = row_num(row, 'BB40_PercentB', 'BB40_%B', default=0.5)

    if bb20w <= BB_SQUEEZE_STRONG:
        candidates.append(('BB20', 4, 'BB20 강한 수축 뒤 방향 선택'))
    elif bb20w <= BB_SQUEEZE_MID:
        candidates.append(('BB20', 2, 'BB20 완만한 수축 구간'))

    if bb40w <= BB_SQUEEZE_STRONG:
        candidates.append(('BB40', 5, 'BB40 중기 수축 뒤 2차파동 후보'))
    elif bb40w <= BB_SQUEEZE_MID:
        candidates.append(('BB40', 3, 'BB40 구조 수렴 구간'))

    if abs(env20p) <= 2.0:
        candidates.append(('ENV20', 3, 'Env20 하단/중심선 근처 회복 구간'))
    if abs(env40p) <= 10.0:
        candidates.append(('ENV40', 4, 'Env40 하단권 회복 또는 지지 시험 구간'))

    if pct_b20 >= 0.9:
        notes.append('BB20 상단 돌파 뒤 눌림 여부 확인')
    elif pct_b20 <= 0.15:
        notes.append('BB20 하단 터치 뒤 반등 성격')

    if pct_b40 >= 0.9:
        notes.append('BB40 상단권 재점화 후보')
    elif pct_b40 <= 0.15:
        notes.append('BB40 하단권 반등 후보')

    if not candidates:
        return ('혼합', notes)
    candidates.sort(key=lambda x: x[1], reverse=True)
    return (candidates[0][0], [c[2] for c in candidates[:2]] + notes)


def _detect_recurrence_signals(row) -> tuple[int, list[str], str]:
    attack = row_num(row, '수박공격점수', 'wm_attack_score', default=0)
    blue = row_num(row, '수박파란점선점수', 'wm_blue_score', default=0)
    breakout = row_num(row, '돌파점수', default=0)
    preempt = row_num(row, '선취점수', '단테점수', default=0)
    reanchor = boolish(row_text(row, '5일재안착태그', default='')) or boolish(row_get(row, '5일재안착', default=False))
    n_combo = row_text(row, 'N조합', default='')
    tags = row_text(row, 'N태그', default='') + ' ' + row_text(row, '조합태그', default='') + ' ' + n_combo
    state = row_text(row, '수박최종상태', '수박상태명', default='')

    score = 0
    patterns = []
    current_state = '중립 구조'

    if '재폭발' in tags or blue >= 4:
        score += 26
        patterns.append('과거 재점화형 성격 강함')
        current_state = '눌림 후 재발사 시험 구간'
    if '2차파동' in tags or 'Blue-2' in state:
        score += 22
        patterns.append('2차파동 재현 성격')
        current_state = '2차 상승 재현 후보 구간'
    if '거래량3배' in tags or breakout >= 80:
        score += 18
        patterns.append('장대양봉 재현 가능성')
    if '돌반지' in tags or '종가배팅' in tags:
        score += 12
        patterns.append('눌림 후 재발사형 패턴 적합')
    if reanchor:
        score += 10
        patterns.append('재안착 후 재상승형 적합')
    if attack >= 4:
        score += 8
    elif attack >= 2:
        score += 4
    if preempt >= 50:
        score += 6

    return score, patterns, current_state


def row_get(row, *keys, default=None):
    for key in keys:
        try:
            if isinstance(row, dict) and key in row:
                v = row.get(key)
                if v is not None:
                    return v
            elif hasattr(row, 'get'):
                v = row.get(key, None)
                if v is not None:
                    return v
        except Exception:
            pass
        try:
            v = row[key]
            if v is not None:
                return v
        except Exception:
            pass
    return default


def build_kki_profile(row) -> dict:
    vol = row_num(row, 'Volume', '거래량', default=0.0)
    vma20 = max(row_num(row, 'VMA20', default=0.0), 1.0)
    preempt = row_num(row, '선취점수', '단테점수', default=0.0)
    breakout = row_num(row, '돌파점수', default=0.0)
    n_score = row_num(row, 'N점수', default=0.0)
    safe_score = row_num(row, '안전점수', default=0.0)
    obv = row_num(row, 'OBV_Slope', 'OBV기울기', 'OBV', default=0.0)
    reanchor = boolish(row_get(row, '5일재안착', default=False)) or boolish(row_text(row, '5일재안착태그', default=''))
    wick_ok = boolish(row_get(row, '수박정제_wick_ok', default=True))
    candle_ok = boolish(row_get(row, '수박정제_candle_ok', default=True))
    cloud_ok = boolish(row_get(row, '수박정제_cloud_ok', default=True))
    long_ok = boolish(row_get(row, '수박정제_long_ok', default=True))
    vol_ok = boolish(row_get(row, '수박정제_vol_ok', default=True))

    vol_ratio20 = vol / vma20 if vma20 > 0 else 0.0
    recurrence_score, recurrence_patterns, current_state = _detect_recurrence_signals(row)
    best_band, band_notes = _best_band_label(row)

    burst = 0
    if vol_ratio20 >= KKI_VOL_STRONG:
        burst += 16
    elif vol_ratio20 >= KKI_VOL_GOOD:
        burst += 10
    if breakout >= 90:
        burst += 14
    elif breakout >= 70:
        burst += 8
    if obv > 0:
        burst += 6
    if reanchor:
        burst += 8
    if preempt >= 50:
        burst += 6
    burst += min(int(max(n_score, safe_score) / 150), 8)

    trap = 0
    bad = []
    if not wick_ok:
        trap += 8; bad.append('윗꼬리')
    if not candle_ok:
        trap += 6; bad.append('캔들약함')
    if not cloud_ok:
        trap += 6; bad.append('저항안착미흡')
    if not long_ok:
        trap += 6; bad.append('장기이평/중기저항')
    if not vol_ok or vol_ratio20 < 0.9:
        trap += 5; bad.append('거래량약함')

    kki_score = max(0, min(100, recurrence_score + burst - trap))
    if kki_score >= KKI_HIGH:
        kki_tag = '🔥끼강'
    elif kki_score >= KKI_MEDIUM:
        kki_tag = '🟡끼보통'
    elif kki_score > 0:
        kki_tag = '😴끼약'
    else:
        kki_tag = ''

    # 흡수 점수
    change = row_num(row, '등락률', 'Change%', 'Change', default=0.0)
    today_p = row_num(row, 'today_p', default=0.0)
    today_f = row_num(row, 'today_f', default=0.0)
    today_i = row_num(row, 'today_i', default=0.0)
    cum5_f = row_num(row, 'cum5_f', default=0.0)
    cum5_i = row_num(row, 'cum5_i', default=0.0)
    close = row_num(row, '종가', 'Close', '현재가', default=0.0)
    low = row_num(row, '저가', 'Low', default=0.0)
    high = row_num(row, '고가', 'High', default=0.0)
    recov = (close - low) / max(high - low, 1e-6) if high > low else 0.0

    absorb_score = 0
    absorb_reasons = []
    if today_p < 0 and (today_f > 0 or today_i > 0):
        absorb_score += 35; absorb_reasons.append('개인매도흡수')
    if change <= -1.0 and (today_f + today_i) > 0:
        absorb_score += 20; absorb_reasons.append('하락중받아먹기')
    if vol_ratio20 >= 1.3:
        absorb_score += 15; absorb_reasons.append('거래량동반')
    if recov >= 0.55:
        absorb_score += 15; absorb_reasons.append('종가회복')
    if (cum5_f + cum5_i) > 0:
        absorb_score += 10; absorb_reasons.append('5일누적유입')
    absorb_score = max(0, min(100, absorb_score))
    if absorb_score >= 70:
        absorb_tag = '🧲투매흡수강'
    elif absorb_score >= 45:
        absorb_tag = '🧲받아먹기'
    elif absorb_score >= 25:
        absorb_tag = '🪙약흡수'
    else:
        absorb_tag = ''

    good = []
    if recurrence_patterns:
        good.extend(recurrence_patterns[:2])
    if reanchor:
        good.append('재안착')
    if obv > 0:
        good.append('OBV매집')
    if cloud_ok:
        good.append('구름위치')
    if best_band and best_band != '혼합':
        good.append(best_band + ' 적합')

    band_text = text_join(band_notes[:3], sep=' / ')
    supply_axis = '혼조수급'
    if (today_f + today_i) > 0 and today_p < 0:
        supply_axis = '개인매도 흡수형'
    elif (cum5_f + cum5_i) > 0:
        supply_axis = '외인/기관 누적 유입형'

    if absorb_score >= 70:
        absorb_text = '투매 물량을 받아내는 흔적이 비교적 선명합니다'
    elif absorb_score >= 45:
        absorb_text = '눌림 구간에서 받아먹는 흔적이 일부 보입니다'
    else:
        absorb_text = '뚜렷한 투매 흡수 흔적은 아직 약합니다'

    if kki_score >= KKI_HIGH:
        response_axis = '재현 확률이 높아 실행 대응 후보로 격상 가능'
    elif kki_score >= KKI_MEDIUM:
        response_axis = '지지 확인 뒤 반등 대응이 더 어울리는 구조'
    else:
        response_axis = '탄력 확인보다 품질과 지지 확인이 먼저인 구조'

    reason_parts = []
    if good:
        reason_parts.append('강점:' + ' / '.join(good[:4]))
    if band_text:
        reason_parts.append('밴드축:' + band_text)
    reason_parts.append(f'수급축:{supply_axis}(외인 {today_f:+.1f}억 / 기관 {today_i:+.1f}억)')
    reason_parts.append(f'흡수판정:{absorb_text}')
    if bad:
        reason_parts.append('주의:' + ' / '.join(bad[:4]))
    reason_parts.append('대응축:' + response_axis)

    recurrence_summary = text_join(recurrence_patterns + ([best_band + ' 적합'] if best_band else []), sep=' | ') or '재현 패턴 약함'
    show = (not SHOW_KKI_ONLY_WHEN_CONFIDENT) or (kki_score >= KKI_SHOW_MIN)
    return {
        'kki_score': int(kki_score),
        'kki_tag': kki_tag,
        'absorb_score': int(absorb_score),
        'absorb_tag': absorb_tag,
        'kki_reason': ' | '.join(reason_parts),
        'kki_recurrence': recurrence_summary,
        'kki_current_state': current_state,
        'kki_show': bool(show),
    }
