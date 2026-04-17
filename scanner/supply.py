from __future__ import annotations

import os
import re
from datetime import datetime, timedelta

import pandas as pd
import requests
try:
    from pykrx import stock
except Exception:
    stock = None

from .utils import safe_float, safe_int

"""실제 로직 이관형 supply 모듈.
KRX/pykrx 기반 수급 스냅샷, 수급 프로파일, 텍스트 요약 로직을 분리했다.
"""

REAL_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Referer': 'https://finance.naver.com/',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
}

_supply_cache = {}

_KRX_RUNTIME = {
    'warned_keys': set(),
    'cred_checked': False,
    'cred_ok': False,
    'cred_user': '',
    'cred_pw': '',
    'session': None,
    'last_error': '',
    'supply_probe_done': False,
    'supply_probe_ok': False,
    'supply_probe_message': '',
}


def log_info(msg: str):
    try:
        print(msg)
    except Exception:
        pass


def _legacy_profile_has_meaningful_amount(legacy) -> bool:
    if not isinstance(legacy, dict):
        return False
    keys = ('today_f','today_i','cum3_f','cum3_i','cum5_f','cum5_i','total_m')
    try:
        return any(abs(safe_float(legacy.get(k), 0.0)) > 0 for k in keys)
    except Exception:
        return False


_legacy_get_supply_profile = None

def _warn_once(key: str, msg: str):
    try:
        if key in _KRX_RUNTIME['warned_keys']:
            return
        _KRX_RUNTIME['warned_keys'].add(key)
        try:
            log_info(msg)
        except Exception:
            print(msg)
    except Exception:
        pass

def _read_krx_credentials():
    if _KRX_RUNTIME['cred_checked']:
        return _KRX_RUNTIME['cred_user'], _KRX_RUNTIME['cred_pw']

    user = (os.getenv('KRX_DATA_ID') or os.getenv('KRX_ID') or '').strip()
    pw = (os.getenv('KRX_DATA_PW') or os.getenv('KRX_PW') or '').strip()

    _KRX_RUNTIME['cred_checked'] = True
    _KRX_RUNTIME['cred_ok'] = bool(user and pw)
    _KRX_RUNTIME['cred_user'] = user
    _KRX_RUNTIME['cred_pw'] = pw

    if _KRX_RUNTIME['cred_ok']:
        _warn_once('krx_cred_detected', '✅ KRX 계정 시크릿 감지: KRX_DATA_ID/KRX_DATA_PW 별칭까지 포함해 로드합니다.')
    else:
        _warn_once('krx_cred_missing', '⚠️ KRX 계정 시크릿이 없습니다. KRX_DATA_ID/KRX_DATA_PW 또는 KRX_ID/KRX_PW를 확인하세요. 수급은 미확인 처리로 계속 진행합니다.')

    return user, pw

def _safe_bday_list(end_dt: datetime, max_days: int = 5):
    out = []
    cur = end_dt
    while len(out) < max_days:
        if cur.weekday() < 5:
            out.append(cur)
        cur -= timedelta(days=1)
    return out

def _safe_get_market_trading_value_by_date(start_ymd: str, end_ymd: str, code: str, on_mode: str):
    if stock is None:
        return pd.DataFrame()
    try:
        return stock.get_market_trading_value_by_date(start_ymd, end_ymd, code, on=on_mode)
    except Exception as e:
        msg = str(e)
        _KRX_RUNTIME['last_error'] = msg
        if 'Expecting value' in msg:
            _warn_once('krx_json_decode_supply', f"⚠️ KRX/pykrx 수급 응답 파싱 실패 감지 → 반복 경고 없이 우회합니다. ({msg})")
        else:
            _warn_once('krx_supply_generic_error', f"⚠️ KRX/pykrx 수급 조회 실패 감지 → 반복 경고 없이 우회합니다. ({msg})")
        return pd.DataFrame()

def _probe_supply_api_once(sample_code: str = '005930'):
    if _KRX_RUNTIME['supply_probe_done']:
        return _KRX_RUNTIME['supply_probe_ok']

    _KRX_RUNTIME['supply_probe_done'] = True
    _read_krx_credentials()

    today = datetime.now()
    for dt in _safe_bday_list(today, max_days=5):
        s = (dt - timedelta(days=7)).strftime('%Y%m%d')
        e = dt.strftime('%Y%m%d')
        try:
            if stock is None:
                continue
            df = stock.get_market_trading_value_by_date(s, e, sample_code, on='순매수')
            if isinstance(df, pd.DataFrame) and not df.empty:
                _KRX_RUNTIME['supply_probe_ok'] = True
                _KRX_RUNTIME['supply_probe_message'] = 'ok'
                return True
        except Exception as ex:
            _KRX_RUNTIME['last_error'] = str(ex)

    _KRX_RUNTIME['supply_probe_ok'] = False
    _KRX_RUNTIME['supply_probe_message'] = _KRX_RUNTIME['last_error'] or 'probe_failed'
    _warn_once('krx_supply_probe_failed', '⚠️ 수급 API 사전 점검 실패. 오늘 수급은 미확인 처리하고 분석은 계속 진행합니다.')
    return False

def _fmt_eok(v: float) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    sign = '+' if v >= 0 else '-'
    return f"{sign}{abs(v):.1f}억"

def _parse_supply_tag_counts(tag: str) -> tuple[int, int]:
    s = str(tag or '').strip()
    m = re.search(r'\((\d+)\s*/\s*(\d+)\)', s)
    if not m:
        return 0, 0
    try:
        return int(m.group(1)), int(m.group(2))
    except Exception:
        return 0, 0

def _pick_supply_strength_label(today_val: float, cum3_val: float, cum5_val: float, streak: int) -> str:
    today_val = safe_float(today_val, 0.0)
    cum3_val = safe_float(cum3_val, 0.0)
    cum5_val = safe_float(cum5_val, 0.0)
    streak = safe_int(streak, 0)

    if today_val >= 15.0 or cum3_val >= 30.0 or cum5_val >= 45.0 or streak >= 7:
        return 'strong'
    if today_val >= 5.0 or cum3_val >= 10.0 or cum5_val >= 18.0 or streak >= 3:
        return 'medium'
    if today_val > 0 or cum3_val > 0 or cum5_val > 0 or streak >= 1:
        return 'weak'
    return 'none'

def _pick_supply_flow_state(today_val: float, cum3_val: float, cum5_val: float, streak: int) -> str:
    today_val = safe_float(today_val, 0.0)
    cum3_val = safe_float(cum3_val, 0.0)
    cum5_val = safe_float(cum5_val, 0.0)
    streak = safe_int(streak, 0)

    if cum5_val >= 45.0 or (cum5_val >= 25.0 and streak >= 3) or (today_val >= 12.0 and cum3_val >= 20.0):
        return 'strong_buy'
    if cum5_val >= 10.0 or cum3_val >= 5.0 or today_val >= 3.0 or streak >= 2:
        return 'buy'
    if cum5_val <= -45.0 or (cum5_val <= -25.0 and today_val <= -5.0) or (today_val <= -12.0 and cum3_val <= -20.0):
        return 'strong_sell'
    if cum5_val <= -10.0 or cum3_val <= -5.0 or today_val <= -3.0:
        return 'sell'
    return 'neutral'

def _build_supply_flow_text(today_p: float, today_f: float, today_i: float,
                            cum3_p: float, cum3_f: float, cum3_i: float,
                            cum5_p: float, cum5_f: float, cum5_i: float,
                            p_s: int = 0, f_s: int = 0, i_s: int = 0) -> str:
    f_state = _pick_supply_flow_state(today_f, cum3_f, cum5_f, f_s)
    i_state = _pick_supply_flow_state(today_i, cum3_i, cum5_i, i_s)

    if f_state in ('strong_buy', 'buy') and i_state in ('strong_buy', 'buy'):
        if f_state == 'strong_buy' or i_state == 'strong_buy' or (cum5_f + cum5_i) >= 40.0:
            base = '외인·기관 동반 강매집중'
        else:
            base = '외인·기관 동반 매집중'
    elif i_state == 'strong_buy':
        base = '기관 강매집중'
    elif f_state == 'strong_buy':
        base = '외인 강매집중'
    elif i_state == 'buy':
        base = '기관 매집중'
    elif f_state == 'buy':
        base = '외인 매집중'
    elif f_state in ('strong_sell', 'sell') and i_state in ('strong_sell', 'sell'):
        if f_state == 'strong_sell' or i_state == 'strong_sell':
            base = '외인·기관 동반 강매도중'
        else:
            base = '외인·기관 동반 매도우세'
    elif i_state == 'strong_sell':
        base = '기관 강매도중'
    elif f_state == 'strong_sell':
        base = '외인 강매도중'
    elif i_state == 'sell':
        base = '기관 매도우세'
    elif f_state == 'sell':
        base = '외인 매도우세'
    else:
        base = '수급 혼조'

    if today_p <= -5.0 and (today_f > 0 or today_i > 0):
        base += ' | 개인투매흡수'
    elif cum5_p <= -15.0 and (cum5_f > 0 or cum5_i > 0):
        base += ' | 개인매도흡수'
    elif today_p >= 5.0 and today_f <= 0 and today_i <= 0:
        base += ' | 개인추격주의'
    elif cum5_p >= 15.0 and cum5_f <= 0 and cum5_i <= 0:
        base += ' | 개인과열주의'

    return base

def _summarize_supply_strength(today_p: float, today_f: float, today_i: float,
                               cum3_p: float, cum3_f: float, cum3_i: float,
                               cum5_p: float, cum5_f: float, cum5_i: float,
                               p_s: int = 0, f_s: int = 0, i_s: int = 0) -> tuple[str, str]:
    summary_parts = []
    personal_judgement = ''

    f_lv = _pick_supply_strength_label(today_f, cum3_f, cum5_f, f_s)
    i_lv = _pick_supply_strength_label(today_i, cum3_i, cum5_i, i_s)

    if f_lv == 'none' and i_lv == 'none':
        summary_parts.append('수급 중립')
    elif f_lv in ('medium', 'strong') and i_lv in ('medium', 'strong'):
        summary_parts.append('외인·기관 동반매수')
    elif f_lv != 'none' and i_lv != 'none':
        summary_parts.append('외인·기관 동반매수(약)')
    elif i_lv == 'strong':
        summary_parts.append('기관매수 강함')
    elif f_lv == 'strong':
        summary_parts.append('외인매수 강함')
    elif i_lv == 'medium':
        summary_parts.append('기관매수 우세')
    elif f_lv == 'medium':
        summary_parts.append('외인매수 우세')
    elif i_lv == 'weak':
        summary_parts.append('기관매수 유입')
    elif f_lv == 'weak':
        summary_parts.append('외인매수 유입')
    elif today_f < -3.0 or cum3_f < -5.0 or cum5_f < -10.0:
        summary_parts.append('외인매도 우세')
    elif today_i < -3.0 or cum3_i < -5.0 or cum5_i < -10.0:
        summary_parts.append('기관매도 우세')

    if today_p <= -3.0 and (today_f > 0 or today_i > 0):
        personal_judgement = '개인투매흡수'
    elif cum3_p <= -5.0 and (cum3_f > 0 or cum3_i > 0):
        personal_judgement = '개인매도흡수'
    elif today_p >= 3.0 and today_f < 0 and today_i <= 0:
        personal_judgement = '개인추격주의'
    elif cum5_p >= 10.0 and cum5_f <= 0 and cum5_i <= 0:
        personal_judgement = '개인과열주의'

    if not summary_parts:
        summary_parts.append('수급 혼조')
    if personal_judgement:
        summary_parts.append(personal_judgement)

    return ' / '.join(summary_parts), personal_judgement

def _select_supply_tag(today_f: float, today_i: float, cum3_f: float, cum3_i: float,
                       cum5_f: float, cum5_i: float, i_s: int, f_s: int) -> str:
    f_lv = _pick_supply_strength_label(today_f, cum3_f, cum5_f, f_s)
    i_lv = _pick_supply_strength_label(today_i, cum3_i, cum5_i, i_s)

    has_f = f_lv != 'none'
    has_i = i_lv != 'none'

    if not has_f and not has_i:
        leader = '➖중립'
    elif f_lv in ('medium', 'strong') and i_lv in ('medium', 'strong'):
        leader = '🤝쌍끌'
    elif has_f and has_i:
        leader = '🤝쌍끌'
    elif has_i:
        leader = '🔴기관'
    elif has_f:
        leader = '🔵외인'
    else:
        leader = '➖중립'
    return f"{leader}({i_s}/{f_s})"

def _extract_actor_streak(streak_text: str, actor: str) -> int:
    s = str(streak_text or '')
    m = re.search(rf'{actor}\s*(\d+)일', s)
    if not m:
        return 0
    try:
        return int(m.group(1))
    except Exception:
        return 0

def _supply_actor_state(today_val: float, cum3_val: float, cum5_val: float, streak: int) -> str:
    today_val = safe_float(today_val, 0.0)
    cum3_val = safe_float(cum3_val, 0.0)
    cum5_val = safe_float(cum5_val, 0.0)
    streak = safe_int(streak, 0)

    if cum5_val >= 120 or (cum5_val >= 80 and streak >= 5) or (today_val >= 30 and cum3_val >= 50):
        return 'very_strong_buy'
    if cum5_val >= 50 or (cum5_val >= 30 and streak >= 3) or (today_val >= 12 and cum3_val >= 20):
        return 'strong_buy'
    if cum5_val >= 15 or cum3_val >= 8 or today_val >= 3 or streak >= 2:
        return 'buy'
    if cum5_val > 0 or cum3_val > 0 or today_val > 0 or streak >= 1:
        return 'weak_buy'

    if cum5_val <= -120 or (cum5_val <= -80 and streak >= 5) or (today_val <= -30 and cum3_val <= -50):
        return 'very_strong_sell'
    if cum5_val <= -50 or (cum5_val <= -30 and streak >= 3) or (today_val <= -12 and cum3_val <= -20):
        return 'strong_sell'
    if cum5_val <= -15 or cum3_val <= -8 or today_val <= -3:
        return 'sell'
    if cum5_val < 0 or cum3_val < 0 or today_val < 0:
        return 'weak_sell'

    return 'neutral'

def _is_buy_state(state: str) -> bool:
    return state in ('very_strong_buy', 'strong_buy', 'buy', 'weak_buy')

def _is_sell_state(state: str) -> bool:
    return state in ('very_strong_sell', 'strong_sell', 'sell', 'weak_sell')

def _build_supply_badge_from_profile(prof: dict) -> str:
    i_s = safe_int(prof.get('inst_streak', 0), 0)
    f_s = safe_int(prof.get('frgn_streak', 0), 0)

    fi = _supply_actor_state(prof.get('today_i', 0), prof.get('cum3_i', 0), prof.get('cum5_i', 0), i_s)
    ff = _supply_actor_state(prof.get('today_f', 0), prof.get('cum3_f', 0), prof.get('cum5_f', 0), f_s)
    sum5 = safe_float(prof.get('cum5_i', 0), 0.0) + safe_float(prof.get('cum5_f', 0), 0.0)

    if _is_buy_state(fi) and _is_buy_state(ff):
        if fi in ('very_strong_buy', 'strong_buy') or ff in ('very_strong_buy', 'strong_buy') or sum5 >= 60:
            leader = '🤝쌍끌강'
        else:
            leader = '🤝쌍끌'
    elif fi in ('very_strong_buy', 'strong_buy'):
        leader = '🔴기관강'
    elif ff in ('very_strong_buy', 'strong_buy'):
        leader = '🔵외인강'
    elif _is_buy_state(fi):
        leader = '🔴기관'
    elif _is_buy_state(ff):
        leader = '🔵외인'
    elif _is_sell_state(fi) and _is_sell_state(ff):
        leader = '🔻동반매도'
    elif (_is_buy_state(fi) and _is_sell_state(ff)) or (_is_buy_state(ff) and _is_sell_state(fi)):
        leader = '⚠️혼조'
    else:
        leader = '➖중립'

    return f"{leader}({i_s}/{f_s})"

def _build_supply_summary_from_profile(prof: dict) -> str:
    i_s = safe_int(prof.get('inst_streak', 0), 0)
    f_s = safe_int(prof.get('frgn_streak', 0), 0)

    fi = _supply_actor_state(prof.get('today_i', 0), prof.get('cum3_i', 0), prof.get('cum5_i', 0), i_s)
    ff = _supply_actor_state(prof.get('today_f', 0), prof.get('cum3_f', 0), prof.get('cum5_f', 0), f_s)

    c5i = safe_float(prof.get('cum5_i', 0), 0.0)
    c5f = safe_float(prof.get('cum5_f', 0), 0.0)
    c5p = safe_float(prof.get('cum5_p', 0), 0.0)

    pieces = []

    if _is_buy_state(fi) and _is_buy_state(ff):
        if fi in ('very_strong_buy', 'strong_buy') or ff in ('very_strong_buy', 'strong_buy'):
            pieces.append('외인·기관 동반매수 강함')
        else:
            pieces.append('외인·기관 동반매수')
    elif fi in ('very_strong_buy', 'strong_buy'):
        pieces.append('기관매수 강함')
    elif ff in ('very_strong_buy', 'strong_buy'):
        pieces.append('외인매수 강함')
    elif fi in ('buy', 'weak_buy'):
        pieces.append('기관매수 우세')
    elif ff in ('buy', 'weak_buy'):
        pieces.append('외인매수 우세')
    elif _is_sell_state(fi) and _is_sell_state(ff):
        pieces.append('외인·기관 동반매도')
    elif _is_sell_state(fi):
        pieces.append('기관매도 우세')
    elif _is_sell_state(ff):
        pieces.append('외인매도 우세')
    else:
        pieces.append('수급 중립')

    if c5p <= -15 and (c5i > 0 or c5f > 0):
        pieces.append('개인매도흡수')
    elif c5p >= 15 and c5i <= 0 and c5f <= 0:
        pieces.append('개인추격주의')

    return ' / '.join(pieces)

def _build_supply_flow_from_profile(prof: dict) -> str:
    i_s = safe_int(prof.get('inst_streak', 0), 0)
    f_s = safe_int(prof.get('frgn_streak', 0), 0)

    fi = _supply_actor_state(prof.get('today_i', 0), prof.get('cum3_i', 0), prof.get('cum5_i', 0), i_s)
    ff = _supply_actor_state(prof.get('today_f', 0), prof.get('cum3_f', 0), prof.get('cum5_f', 0), f_s)

    c5i = safe_float(prof.get('cum5_i', 0), 0.0)
    c5f = safe_float(prof.get('cum5_f', 0), 0.0)
    total5 = c5i + c5f

    if _is_buy_state(fi) and _is_buy_state(ff):
        if fi in ('very_strong_buy', 'strong_buy') or ff in ('very_strong_buy', 'strong_buy') or total5 >= 80:
            return '최근 5일 기준 외인·기관 동반 강매집 진행형'
        return '최근 5일 기준 외인·기관 동반 매집 진행형'
    if fi in ('very_strong_buy', 'strong_buy'):
        return '최근 5일 기준 기관 중심 강매집 진행형'
    if ff in ('very_strong_buy', 'strong_buy'):
        return '최근 5일 기준 외인 중심 강매집 진행형'
    if fi in ('buy', 'weak_buy'):
        return '최근 5일 기준 기관 중심 매집 진행형'
    if ff in ('buy', 'weak_buy'):
        return '최근 5일 기준 외인 중심 매집 진행형'
    if _is_sell_state(fi) and _is_sell_state(ff):
        return '최근 5일 기준 외인·기관 동반 매도 진행형'
    if _is_sell_state(fi):
        return '최근 5일 기준 기관 매도 우세'
    if _is_sell_state(ff):
        return '최근 5일 기준 외인 매도 우세'
    return '최근 5일 기준 뚜렷한 매집 방향성 없음'

def _upgrade_whale_score(prof: dict) -> int:
    total_m = safe_float(prof.get('total_m', 0), 0.0)
    c5i = safe_float(prof.get('cum5_i', 0), 0.0)
    c5f = safe_float(prof.get('cum5_f', 0), 0.0)
    i_s = safe_int(prof.get('inst_streak', 0), 0)
    f_s = safe_int(prof.get('frgn_streak', 0), 0)

    buy_sum = max(0.0, c5i) + max(0.0, c5f)
    streak_bonus = 0
    if max(i_s, f_s) >= 7:
        streak_bonus = 10
    elif max(i_s, f_s) >= 5:
        streak_bonus = 7
    elif max(i_s, f_s) >= 3:
        streak_bonus = 4

    base = int(total_m * 0.25)
    acc_bonus = int(min(35, buy_sum * 0.18))
    return int(max(0, min(70, base + acc_bonus + streak_bonus)))

def _finalize_supply_profile_dict(prof: dict) -> dict:
    if not isinstance(prof, dict):
        return prof

    streak_text = str(prof.get('streak_text', '') or '')
    prof['inst_streak'] = safe_int(prof.get('inst_streak', _extract_actor_streak(streak_text, '기관')), 0)
    prof['frgn_streak'] = safe_int(prof.get('frgn_streak', _extract_actor_streak(streak_text, '외인')), 0)
    prof['personal_streak'] = safe_int(prof.get('personal_streak', _extract_actor_streak(streak_text, '개인')), 0)

    prof['tag'] = _build_supply_badge_from_profile(prof)
    prof['summary'] = _build_supply_summary_from_profile(prof)
    prof['flow_text'] = _build_supply_flow_from_profile(prof)

    prof['today_text'] = (
        f"개인 {_fmt_eok(prof.get('today_p', 0))} | "
        f"외인 {_fmt_eok(prof.get('today_f', 0))} | "
        f"기관 {_fmt_eok(prof.get('today_i', 0))}"
    )
    prof['cum3_text'] = (
        f"개인 {_fmt_eok(prof.get('cum3_p', 0))} | "
        f"외인 {_fmt_eok(prof.get('cum3_f', 0))} | "
        f"기관 {_fmt_eok(prof.get('cum3_i', 0))}"
    )
    prof['cum5_text'] = (
        f"개인 {_fmt_eok(prof.get('cum5_p', 0))} | "
        f"외인 {_fmt_eok(prof.get('cum5_f', 0))} | "
        f"기관 {_fmt_eok(prof.get('cum5_i', 0))}"
    )
    prof['streak_text'] = (
        f"기관 {prof['inst_streak']}일 / "
        f"외인 {prof['frgn_streak']}일 / "
        f"개인 {prof['personal_streak']}일"
    )

    prof['twin_b'] = '🤝쌍끌' in str(prof.get('tag', ''))
    prof['total_m'] = round(abs(safe_float(prof.get('today_i', 0), 0.0)) + abs(safe_float(prof.get('today_f', 0), 0.0)))
    prof['whale_score'] = _upgrade_whale_score(prof)
    return prof

def _normalize_investor_value_df_v2(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or len(raw) == 0:
        return pd.DataFrame()
    df = raw.copy()
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    def _pick(cols):
        for c in cols:
            if c in df.columns:
                return c
        return None
    personal_col = _pick(['개인', '개인합계'])
    foreign_col = _pick(['외국인합계', '외국인', '외국인투자자'])
    inst_col = _pick(['기관합계', '기관', '기관투자자'])
    out = pd.DataFrame(index=df.index)
    out['개인'] = df[personal_col] if personal_col in df.columns else 0.0
    out['외인'] = df[foreign_col] if foreign_col in df.columns else 0.0
    out['기관'] = df[inst_col] if inst_col in df.columns else 0.0
    if (inst_col not in df.columns) or out['기관'].fillna(0).abs().sum() == 0:
        inst_children = [
            c for c in df.columns
            if c not in {personal_col, foreign_col}
            and any(k in str(c) for k in ['기관', '금융투자', '보험', '투신', '사모', '은행', '기타금융', '연기금', '국가', '기타법인'])
        ]
        if inst_children:
            out['기관'] = df[inst_children].sum(axis=1)
    return out.fillna(0.0)

def _positive_streak_v2(series: pd.Series) -> int:
    cnt = 0
    for v in reversed(list(series.fillna(0.0))):
        try:
            if float(v) > 0:
                cnt += 1
            else:
                break
        except Exception:
            break
    return cnt

def _negative_streak_v2(series: pd.Series) -> int:
    cnt = 0
    for v in reversed(list(series.fillna(0.0))):
        try:
            if float(v) < 0:
                cnt += 1
            else:
                break
        except Exception:
            break
    return cnt

def _sum_actor_window(df: pd.DataFrame, col: str, n: int) -> float:
    try:
        if df is None or df.empty or col not in df.columns:
            return 0.0
        return float(df.tail(min(n, len(df)))[col].sum())
    except Exception:
        return 0.0

def _build_gross_line(prefix: str, buy_df: pd.DataFrame, sell_df: pd.DataFrame, net_df: pd.DataFrame, n: int) -> str:
    try:
        fb = _sum_actor_window(buy_df, '외인', n)
        fs = _sum_actor_window(sell_df, '외인', n)
        fn = _sum_actor_window(net_df, '외인', n)
        ib = _sum_actor_window(buy_df, '기관', n)
        is_ = _sum_actor_window(sell_df, '기관', n)
        inn = _sum_actor_window(net_df, '기관', n)
        return (
            f"{prefix} 외인 매수 {_fmt_eok_or_unknown_won(fb)} / 매도 {_fmt_eok_or_unknown_won(fs)} / 순 {_fmt_eok_or_unknown_won(fn)} | "
            f"기관 매수 {_fmt_eok_or_unknown_won(ib)} / 매도 {_fmt_eok_or_unknown_won(is_)} / 순 {_fmt_eok_or_unknown_won(inn)}"
        )
    except Exception:
        return ''

def _fetch_ohlcv_for_supply_v2(code: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    try:
        raw = stock.get_market_ohlcv_by_date(start_dt.strftime('%Y%m%d'), end_dt.strftime('%Y%m%d'), code)
        if raw is None or raw.empty:
            return pd.DataFrame()
        df = raw.copy()
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[~df.index.isna()].sort_index()
        for c in ['시가', '고가', '저가', '종가', '거래량', '거래대금']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
            else:
                df[c] = 0.0
        return df
    except Exception:
        return pd.DataFrame()

def _fmt_eok_or_unknown_won(value_won, unavailable_text='미확인'):
    try:
        if value_won is None:
            return unavailable_text
        return f"{float(value_won) / 100000000:.1f}억"
    except Exception:
        return unavailable_text

def _drop_trailing_zero_supply_rows_v2(net_df: pd.DataFrame, buy_df: pd.DataFrame, sell_df: pd.DataFrame, ohlcv: pd.DataFrame):
    try:
        net_df = net_df.copy()
        buy_df = buy_df.copy()
        sell_df = sell_df.copy()
        ohlcv = ohlcv.copy() if ohlcv is not None else pd.DataFrame()
        while len(net_df) > 1:
            idx = net_df.index[-1]
            net_abs = float(net_df.loc[idx, ['개인', '외인', '기관']].abs().sum()) if idx in net_df.index else 0.0
            buy_abs = float(buy_df.loc[idx, ['개인', '외인', '기관']].abs().sum()) if idx in buy_df.index else 0.0
            sell_abs = float(sell_df.loc[idx, ['개인', '외인', '기관']].abs().sum()) if idx in sell_df.index else 0.0
            if (net_abs + buy_abs + sell_abs) > 0:
                break
            net_df = net_df.iloc[:-1]
            if not buy_df.empty:
                buy_df = buy_df.loc[buy_df.index.intersection(net_df.index)]
            if not sell_df.empty:
                sell_df = sell_df.loc[sell_df.index.intersection(net_df.index)]
            if not ohlcv.empty:
                ohlcv = ohlcv.loc[ohlcv.index.intersection(net_df.index)]
        return net_df, buy_df, sell_df, ohlcv
    except Exception:
        return net_df, buy_df, sell_df, ohlcv

def _maybe_scale_investor_value_frames_by_amount_v2(net_df: pd.DataFrame, buy_df: pd.DataFrame, sell_df: pd.DataFrame, ohlcv: pd.DataFrame):
    scale = 1.0
    scale_reason = ''
    try:
        if net_df is None or net_df.empty or buy_df is None or buy_df.empty or sell_df is None or sell_df.empty:
            return net_df, buy_df, sell_df, scale, scale_reason
        if ohlcv is None or ohlcv.empty or '거래대금' not in ohlcv.columns:
            return net_df, buy_df, sell_df, scale, scale_reason

        common_idx = net_df.index.intersection(buy_df.index).intersection(sell_df.index).intersection(ohlcv.index)
        if len(common_idx) == 0:
            return net_df, buy_df, sell_df, scale, scale_reason

        amount = pd.to_numeric(ohlcv.loc[common_idx, '거래대금'], errors='coerce').fillna(0.0)
        gross = (
            buy_df.loc[common_idx, ['외인', '기관']].abs().sum(axis=1) +
            sell_df.loc[common_idx, ['외인', '기관']].abs().sum(axis=1)
        )
        ratio = (gross / amount.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).dropna()
        max_abs = float(max(
            net_df.loc[common_idx, ['외인', '기관']].abs().max().max(),
            buy_df.loc[common_idx, ['외인', '기관']].abs().max().max(),
            sell_df.loc[common_idx, ['외인', '기관']].abs().max().max(),
        )) if len(common_idx) else 0.0
        amt_med = float(amount[amount > 0].median()) if (amount > 0).any() else 0.0
        ratio_med = float(ratio.median()) if not ratio.empty else 0.0

        # pykrx 투자자별 거래대금이 천원 단위로 내려오는 환경 방어
        if amt_med >= 1e8 and max_abs > 0 and (ratio_med == 0.0 or ratio_med < 0.02):
            scale = 1000.0
            scale_reason = 'pykrx_investor_value_thousand_won_scaled'
            net_df = net_df * scale
            buy_df = buy_df * scale
            sell_df = sell_df * scale
    except Exception:
        pass
    return net_df, buy_df, sell_df, scale, scale_reason

def _fetch_supply_snapshot_v2(code: str, end_ymd: str = None, calendar_back: int = 35) -> dict:
    base = {
        'ok': False,
        'reason': 'unknown',
        'rows': 0,
        'last_date': None,
        'p1': None, 'f1': None, 'i1': None,
        'p3': None, 'f3': None, 'i3': None,
        'p5': None, 'f5': None, 'i5': None,
        'fs': 0, 'is': 0, 'ps': 0, 'pds': 0,
        'fb1': None, 'fs1': None, 'ib1': None, 'is1': None,
        'fb3': None, 'fs3': None, 'ib3': None, 'is3': None,
        'fb5': None, 'fs5': None, 'ib5': None, 'is5': None,
        'amount1': None, 'amount3': None, 'amount5': None,
        'close3_pct': 0.0, 'close5_pct': 0.0,
        'gross_today_text': '',
        'gross_3d_text': '',
        'gross_5d_text': '',
        'distribution_trace': '',
        'total5': None,
        'scale': 1.0,
        'scale_reason': '',
    }

    if not _probe_supply_api_once():
        base['reason'] = _KRX_RUNTIME.get('supply_probe_message', 'supply_probe_failed')
        return base

    try:
        base_end = datetime.strptime(end_ymd, '%Y%m%d') if end_ymd else datetime.now()
    except Exception:
        base_end = datetime.now()

    for end_dt in _safe_bday_list(base_end, max_days=5):
        start_dt = end_dt - timedelta(days=calendar_back)
        s_ymd = start_dt.strftime('%Y%m%d')
        e_ymd = end_dt.strftime('%Y%m%d')

        net_raw = _safe_get_market_trading_value_by_date(s_ymd, e_ymd, code, '순매수')
        buy_raw = _safe_get_market_trading_value_by_date(s_ymd, e_ymd, code, '매수')
        sell_raw = _safe_get_market_trading_value_by_date(s_ymd, e_ymd, code, '매도')

        net_df = _normalize_investor_value_df_v2(net_raw)
        buy_df = _normalize_investor_value_df_v2(buy_raw)
        sell_df = _normalize_investor_value_df_v2(sell_raw)

        if net_df.empty:
            continue

        common_idx = net_df.index
        if not buy_df.empty:
            common_idx = common_idx.intersection(buy_df.index)
        if not sell_df.empty:
            common_idx = common_idx.intersection(sell_df.index)
        if len(common_idx) == 0:
            continue

        net_df = net_df.loc[common_idx].sort_index().tail(7)
        buy_df = buy_df.loc[buy_df.index.intersection(net_df.index)].sort_index().tail(7)
        sell_df = sell_df.loc[sell_df.index.intersection(net_df.index)].sort_index().tail(7)

        ohlcv = _fetch_ohlcv_for_supply_v2(code, start_dt, end_dt)
        if not ohlcv.empty:
            ohlcv = ohlcv.loc[ohlcv.index.intersection(net_df.index)].sort_index().tail(7)

        net_df, buy_df, sell_df, ohlcv = _drop_trailing_zero_supply_rows_v2(net_df, buy_df, sell_df, ohlcv)
        if net_df.empty:
            continue

        net_df, buy_df, sell_df, scale, scale_reason = _maybe_scale_investor_value_frames_by_amount_v2(net_df, buy_df, sell_df, ohlcv)

        if (
            net_df[['개인', '외인', '기관']].fillna(0).abs().sum().sum() == 0 and
            buy_df[['개인', '외인', '기관']].fillna(0).abs().sum().sum() == 0 and
            sell_df[['개인', '외인', '기관']].fillna(0).abs().sum().sum() == 0
        ):
            continue

        net_df = net_df.sort_index().tail(5)
        buy_df = buy_df.loc[buy_df.index.intersection(net_df.index)].sort_index().tail(5)
        sell_df = sell_df.loc[sell_df.index.intersection(net_df.index)].sort_index().tail(5)
        if not ohlcv.empty:
            ohlcv = ohlcv.loc[ohlcv.index.intersection(net_df.index)].sort_index().tail(5)

        last1 = net_df.tail(1)
        last3 = net_df.tail(min(3, len(net_df)))
        last5 = net_df.tail(min(5, len(net_df)))

        try:
            last_date = str(last5.index[-1].date())
        except Exception:
            last_date = str(last5.index[-1]) if len(last5.index) else None

        fs = _positive_streak_v2(last5['외인'])
        is_ = _positive_streak_v2(last5['기관'])
        ps = _positive_streak_v2(last5['개인'])
        pds = _negative_streak_v2(last5['개인'])

        p1 = float(last1['개인'].sum())
        f1 = float(last1['외인'].sum())
        i1 = float(last1['기관'].sum())
        p3 = float(last3['개인'].sum())
        f3 = float(last3['외인'].sum())
        i3 = float(last3['기관'].sum())
        p5 = float(last5['개인'].sum())
        f5 = float(last5['외인'].sum())
        i5 = float(last5['기관'].sum())

        fb1 = _sum_actor_window(buy_df, '외인', 1)
        fs1 = _sum_actor_window(sell_df, '외인', 1)
        ib1 = _sum_actor_window(buy_df, '기관', 1)
        is1 = _sum_actor_window(sell_df, '기관', 1)
        fb3 = _sum_actor_window(buy_df, '외인', 3)
        fs3 = _sum_actor_window(sell_df, '외인', 3)
        ib3 = _sum_actor_window(buy_df, '기관', 3)
        is3 = _sum_actor_window(sell_df, '기관', 3)
        fb5 = _sum_actor_window(buy_df, '외인', 5)
        fs5 = _sum_actor_window(sell_df, '외인', 5)
        ib5 = _sum_actor_window(buy_df, '기관', 5)
        is5 = _sum_actor_window(sell_df, '기관', 5)

        if not ohlcv.empty:
            close = pd.to_numeric(ohlcv['종가'], errors='coerce').fillna(0.0)
            amount = pd.to_numeric(ohlcv['거래대금'], errors='coerce').fillna(0.0)
            amount1 = float(amount.tail(1).sum())
            amount3 = float(amount.tail(min(3, len(amount))).sum())
            amount5 = float(amount.tail(min(5, len(amount))).sum())
            if len(close) >= 4 and float(close.iloc[-4]) > 0:
                close3_pct = round((float(close.iloc[-1]) / float(close.iloc[-4]) - 1.0) * 100.0, 2)
            else:
                close3_pct = 0.0
            if len(close) >= 5 and float(close.iloc[-5]) > 0:
                close5_pct = round((float(close.iloc[-1]) / float(close.iloc[-5]) - 1.0) * 100.0, 2)
            else:
                close5_pct = 0.0
        else:
            amount1 = amount3 = amount5 = 0.0
            close3_pct = close5_pct = 0.0

        gross_today_text = _build_gross_line('최근1일', buy_df, sell_df, net_df, 1)
        gross_3d_text = _build_gross_line('최근3일', buy_df, sell_df, net_df, 3)
        gross_5d_text = _build_gross_line('최근5일', buy_df, sell_df, net_df, 5)

        distribution_trace = ''
        combined_net_5 = f5 + i5
        combined_sell_5 = fs5 + is5
        combined_buy_5 = fb5 + ib5
        combined_net_ratio_5 = (combined_net_5 / amount5 * 100.0) if amount5 > 0 else 0.0
        combined_sell_ratio_5 = (combined_sell_5 / amount5 * 100.0) if amount5 > 0 else 0.0

        if combined_net_5 < 0 and p5 > 0 and close3_pct >= 0.5 and combined_sell_ratio_5 >= 0.7:
            distribution_trace = '세력 이탈 흔적: 상승 구간에서 외인·기관 순매도 / 개인 순매수'
        elif combined_sell_5 > combined_buy_5 * 1.10 and close3_pct >= 1.0:
            distribution_trace = '단기 차익실현 흔적: 외인·기관 매도 우위 확대'
        elif (f5 > 0 or i5 > 0) and p5 < 0 and (fs >= 2 or is_ >= 2):
            distribution_trace = '재매집 흔적: 개인 매도 물량을 외인·기관이 흡수하는 흐름'
        elif p5 > 0 and (f5 + i5) <= 0:
            distribution_trace = '개인 추격 흔적: 외인·기관 동행 없는 개인 매수 우세'

        base.update({
            'ok': True,
            'reason': 'ok',
            'rows': len(net_df),
            'last_date': last_date,
            'p1': p1, 'f1': f1, 'i1': i1,
            'p3': p3, 'f3': f3, 'i3': i3,
            'p5': p5, 'f5': f5, 'i5': i5,
            'fs': fs, 'is': is_, 'ps': ps, 'pds': pds,
            'fb1': fb1, 'fs1': fs1, 'ib1': ib1, 'is1': is1,
            'fb3': fb3, 'fs3': fs3, 'ib3': ib3, 'is3': is3,
            'fb5': fb5, 'fs5': fs5, 'ib5': ib5, 'is5': is5,
            'amount1': amount1, 'amount3': amount3, 'amount5': amount5,
            'close3_pct': close3_pct, 'close5_pct': close5_pct,
            'gross_today_text': gross_today_text,
            'gross_3d_text': gross_3d_text,
            'gross_5d_text': gross_5d_text,
            'distribution_trace': distribution_trace,
            'total5': combined_net_5,
            'combined_net_ratio_5': combined_net_ratio_5,
            'combined_sell_ratio_5': combined_sell_ratio_5,
            'scale': scale,
            'scale_reason': scale_reason,
        })
        return base

    base['reason'] = _KRX_RUNTIME.get('last_error') or '최근 영업일 수급 미확인'
    return base

def _resolve_supply_grade_v2(snap: dict) -> dict:
    if not snap.get('ok'):
        return {
            'tag': '➖중립',
            'summary': '실금액 미확인',
            'flow_text': '최근 5일 실금액 기준 수급 미확인',
            'confidence': '추정',
            'twin_b': False,
            'whale_score': 0,
            'warning_text': '',
        }

    f1 = safe_float(snap.get('f1'), 0.0)
    i1 = safe_float(snap.get('i1'), 0.0)
    p1 = safe_float(snap.get('p1'), 0.0)
    f3 = safe_float(snap.get('f3'), 0.0)
    i3 = safe_float(snap.get('i3'), 0.0)
    p3 = safe_float(snap.get('p3'), 0.0)
    f5 = safe_float(snap.get('f5'), 0.0)
    i5 = safe_float(snap.get('i5'), 0.0)
    p5 = safe_float(snap.get('p5'), 0.0)

    fs = safe_int(snap.get('fs'), 0)
    is_ = safe_int(snap.get('is'), 0)

    amount1 = safe_float(snap.get('amount1'), 0.0)
    amount3 = safe_float(snap.get('amount3'), 0.0)
    amount5 = safe_float(snap.get('amount5'), 0.0)

    distribution_trace = str(snap.get('distribution_trace', '') or '').strip()
    close3_pct = safe_float(snap.get('close3_pct'), 0.0)
    close5_pct = safe_float(snap.get('close5_pct'), 0.0)

    def _pct(part, whole):
        return (float(part) / float(whole) * 100.0) if whole and abs(float(whole)) > 0 else 0.0

    def _meaning_threshold(amount):
        # 너무 작은 금액은 “양수/음수”만으로 수급 판정하지 않도록 하한선 부여
        return max(2.0 * 100000000.0, amount * 0.003) if amount > 0 else 2.0 * 100000000.0

    def _is_meaningful(v, amount):
        return abs(float(v)) >= _meaning_threshold(amount)

    f_mean = _is_meaningful(f5, amount5)
    i_mean = _is_meaningful(i5, amount5)
    p_mean = _is_meaningful(p5, amount5)

    f_ratio = _pct(f5, amount5)
    i_ratio = _pct(i5, amount5)
    joint_ratio = _pct(f5 + i5, amount5)

    f_pos = f5 > 0 and f_mean
    i_pos = i5 > 0 and i_mean
    f_neg = f5 < 0 and f_mean
    i_neg = i5 < 0 and i_mean

    warning_text = distribution_trace

    # 0) 개인 추격 / 세력 이탈 경고 우선
    if distribution_trace.startswith('세력 이탈 흔적'):
        return {
            'tag': f'⚠️분산({is_}/{fs})',
            'summary': '외인·기관 차익실현',
            'flow_text': '최근 5일 기준 외인·기관 분산 진행형',
            'confidence': '확정',
            'twin_b': False,
            'whale_score': -8,
            'warning_text': warning_text,
        }

    if p_mean and p5 > 0 and (f5 + i5) <= 0:
        warning_text = warning_text or '개인 추격 흔적: 외인·기관 동행 없는 개인 매수 우세'
        return {
            'tag': f'🟠개인추격({is_}/{fs})',
            'summary': '개인 매수 우세 / 세력 동행 약함',
            'flow_text': '최근 5일 기준 개인 중심 수급으로 추격 주의',
            'confidence': '확정',
            'twin_b': False,
            'whale_score': -4,
            'warning_text': warning_text,
        }

    # 1) 동반 강매집: 둘 다 의미 있는 양수 + 비중 충분 + 연속성
    if f_pos and i_pos and joint_ratio >= 1.0 and (fs >= 2 or is_ >= 2):
        return {
            'tag': f'🤝쌍끌강({is_}/{fs})',
            'summary': '외인·기관 동반매수 강함 / 개인매도흡수' if p5 < 0 else '외인·기관 동반매수 강함',
            'flow_text': '최근 5일 기준 외인·기관 동반 강매집 진행형',
            'confidence': '확정',
            'twin_b': True,
            'whale_score': 32,
            'warning_text': warning_text,
        }

    # 2) 동반 매집: 둘 다 의미 있는 양수지만 강매집까지는 아님
    if f_pos and i_pos:
        return {
            'tag': f'🤝쌍끌({is_}/{fs})',
            'summary': '외인·기관 동반매수',
            'flow_text': '최근 5일 기준 외인·기관 동반 매집 진행형',
            'confidence': '확정',
            'twin_b': True,
            'whale_score': 22,
            'warning_text': warning_text,
        }

    # 3) 외인 중심 / 기관 중심
    if f_pos and not i_pos:
        summary = '외인매수 강함' if (f_ratio >= 0.8 or fs >= 3) else '외인매수 우세'
        flow = '최근 5일 기준 외인 중심 강매집 진행형' if (f_ratio >= 0.8 or fs >= 3) else '최근 5일 기준 외인 중심 매집 진행형'
        if i5 < 0 and i_mean:
            summary += ' / 기관 차익실현'
        elif p5 < 0 and p_mean:
            summary += ' / 개인매도흡수'
        return {
            'tag': f'🔵외인강({is_}/{fs})' if (f_ratio >= 0.8 or fs >= 3) else f'🔵외인({is_}/{fs})',
            'summary': summary,
            'flow_text': flow,
            'confidence': '확정',
            'twin_b': False,
            'whale_score': 18 if (f_ratio >= 0.8 or fs >= 3) else 12,
            'warning_text': warning_text,
        }

    if i_pos and not f_pos:
        summary = '기관매수 강함' if (i_ratio >= 0.8 or is_ >= 3) else '기관매수 우세'
        flow = '최근 5일 기준 기관 중심 강매집 진행형' if (i_ratio >= 0.8 or is_ >= 3) else '최근 5일 기준 기관 중심 매집 진행형'
        if f5 < 0 and f_mean:
            summary += ' / 외인 차익실현'
        elif p5 < 0 and p_mean:
            summary += ' / 개인매도흡수'
        return {
            'tag': f'🔴기관강({is_}/{fs})' if (i_ratio >= 0.8 or is_ >= 3) else f'🔴기관({is_}/{fs})',
            'summary': summary,
            'flow_text': flow,
            'confidence': '확정',
            'twin_b': False,
            'whale_score': 18 if (i_ratio >= 0.8 or is_ >= 3) else 12,
            'warning_text': warning_text,
        }

    # 4) 혼조: 한쪽 의미 있는 매수, 다른 한쪽 의미 있는 매도
    if (f_pos and i_neg) or (i_pos and f_neg):
        main_actor = '외인' if abs(f5) >= abs(i5) else '기관'
        return {
            'tag': f'🟡혼조({is_}/{fs})',
            'summary': f'{main_actor} 주도 / 반대축 차익실현',
            'flow_text': '최근 5일 기준 외인·기관 혼조 수급',
            'confidence': '확정',
            'twin_b': False,
            'whale_score': 6,
            'warning_text': warning_text or '한 축은 매수, 다른 축은 매도로 수급 해석이 엇갈립니다.',
        }

    # 5) 동반 매도
    if f_neg and i_neg:
        return {
            'tag': f'🔻동반매도({is_}/{fs})',
            'summary': '외인·기관 동반매도',
            'flow_text': '최근 5일 기준 외인·기관 동반 매도 진행형',
            'confidence': '확정',
            'twin_b': False,
            'whale_score': -12,
            'warning_text': warning_text or ('개인 추격 주의' if p5 > 0 else ''),
        }

    # 6) 약한 유입 / 중립
    if (f5 + i5) > 0 and (fs >= 1 or is_ >= 1):
        return {
            'tag': f'➖중립({is_}/{fs})',
            'summary': '완만한 순매수 유입',
            'flow_text': '최근 5일 기준 완만한 매집 유입',
            'confidence': '확정',
            'twin_b': False,
            'whale_score': 4,
            'warning_text': warning_text,
        }

    return {
        'tag': f'➖중립({is_}/{fs})',
        'summary': '수급 중립',
        'flow_text': '최근 5일 기준 뚜렷한 매집 방향성 없음',
        'confidence': '확정',
        'twin_b': False,
        'whale_score': 0,
        'warning_text': warning_text,
    }

def get_supply_profile(code, price):
    cache_key = f'profile::realmoney::{code}'
    cached = _supply_cache.get(cache_key)
    if isinstance(cached, dict):
        return cached

    snap = _fetch_supply_snapshot_v2(str(code).zfill(6))
    grade = _resolve_supply_grade_v2(snap)

    if snap.get('ok'):
        prof = {
            'tag': grade['tag'],
            'total_m': round((abs(safe_float(snap.get('f1'), 0.0)) + abs(safe_float(snap.get('i1'), 0.0))) / 100000000, 1),
            'whale_streak': max(safe_int(snap.get('fs'), 0), safe_int(snap.get('is'), 0)),
            'whale_score': grade['whale_score'],
            'twin_b': grade['twin_b'],
            'today_text': f"개인 {_fmt_eok_or_unknown_won(snap.get('p1'))} | 외인 {_fmt_eok_or_unknown_won(snap.get('f1'))} | 기관 {_fmt_eok_or_unknown_won(snap.get('i1'))}",
            'cum3_text': f"개인 {_fmt_eok_or_unknown_won(snap.get('p3'))} | 외인 {_fmt_eok_or_unknown_won(snap.get('f3'))} | 기관 {_fmt_eok_or_unknown_won(snap.get('i3'))}",
            'cum5_text': f"개인 {_fmt_eok_or_unknown_won(snap.get('p5'))} | 외인 {_fmt_eok_or_unknown_won(snap.get('f5'))} | 기관 {_fmt_eok_or_unknown_won(snap.get('i5'))}",
            'streak_text': f"기관 {safe_int(snap.get('is'), 0)}일 / 외인 {safe_int(snap.get('fs'), 0)}일 / 개인 {safe_int(snap.get('ps'), 0)}일",
            'summary': grade['summary'],
            'flow_text': grade['flow_text'],
            'warning_text': grade.get('warning_text', ''),
            'gross_today_text': snap.get('gross_today_text', ''),
            'gross_3d_text': snap.get('gross_3d_text', ''),
            'gross_5d_text': snap.get('gross_5d_text', ''),
            'personal_judgement': '개인 추격성 매수' if safe_float(snap.get('p5'), 0.0) > 0 and safe_float(snap.get('f5'), 0.0) + safe_float(snap.get('i5'), 0.0) <= 0 else ('개인 차익실현성 매도' if safe_float(snap.get('p5'), 0.0) < 0 else '개인 방향성 중립'),
            'foreign_hold_text': '미확인',
            'today_f': round(safe_float(snap.get('f1'), 0.0) / 100000000, 1),
            'today_i': round(safe_float(snap.get('i1'), 0.0) / 100000000, 1),
            'today_p': round(safe_float(snap.get('p1'), 0.0) / 100000000, 1),
            'cum3_f': round(safe_float(snap.get('f3'), 0.0) / 100000000, 1),
            'cum3_i': round(safe_float(snap.get('i3'), 0.0) / 100000000, 1),
            'cum3_p': round(safe_float(snap.get('p3'), 0.0) / 100000000, 1),
            'cum5_f': round(safe_float(snap.get('f5'), 0.0) / 100000000, 1),
            'cum5_i': round(safe_float(snap.get('i5'), 0.0) / 100000000, 1),
            'cum5_p': round(safe_float(snap.get('p5'), 0.0) / 100000000, 1),
            'inst_streak': safe_int(snap.get('is'), 0),
            'frgn_streak': safe_int(snap.get('fs'), 0),
            'personal_streak': safe_int(snap.get('ps'), 0),
            'confidence': grade['confidence'],
            'last_date': snap.get('last_date') or '미확인',
            'raw_ok': True,
            'raw_reason': snap.get('scale_reason') or snap.get('reason', 'ok'),
        }
        prof = _finalize_supply_profile_dict(prof)
        _supply_cache[cache_key] = prof
        return prof

    try:
        legacy = _legacy_get_supply_profile(code, price)
    except Exception:
        legacy = None

    if _legacy_profile_has_meaningful_amount(legacy):
        legacy = dict(legacy)
        legacy['confidence'] = '추정'
        legacy['last_date'] = '미확인'
        legacy['raw_ok'] = False
        legacy['raw_reason'] = snap.get('reason', 'legacy_fallback')
        legacy['summary'] = (str(legacy.get('summary', '')).strip() + ' (추정)').strip() if str(legacy.get('summary', '')).strip() else '실금액 미확인 / 보조추정'
        legacy['flow_text'] = (str(legacy.get('flow_text', '')).strip() + ' [추정]').strip() if str(legacy.get('flow_text', '')).strip() else '최근 5일 기준 수급 보조추정'
        legacy.setdefault('warning_text', '')
        legacy.setdefault('gross_today_text', '')
        legacy.setdefault('gross_3d_text', '')
        legacy.setdefault('gross_5d_text', '')
        legacy = _finalize_supply_profile_dict(legacy)
        _supply_cache[cache_key] = legacy
        return legacy

    prof = {
        'tag': '➖중립', 'total_m': 0, 'whale_streak': 0, 'whale_score': 0, 'twin_b': False,
        'today_text': '개인 미확인 | 외인 미확인 | 기관 미확인',
        'cum3_text': '개인 미확인 | 외인 미확인 | 기관 미확인',
        'cum5_text': '개인 미확인 | 외인 미확인 | 기관 미확인',
        'streak_text': '', 'summary': '실금액 미확인', 'flow_text': '최근 5일 실금액 기준 수급 미확인',
        'warning_text': '', 'gross_today_text': '', 'gross_3d_text': '', 'gross_5d_text': '',
        'personal_judgement': '미확인', 'foreign_hold_text': '미확인',
        'today_f': 0.0, 'today_i': 0.0, 'today_p': 0.0, 'cum3_f': 0.0, 'cum3_i': 0.0, 'cum3_p': 0.0,
        'cum5_f': 0.0, 'cum5_i': 0.0, 'cum5_p': 0.0, 'inst_streak': 0, 'frgn_streak': 0, 'personal_streak': 0,
        'confidence': '미확인', 'last_date': '미확인', 'raw_ok': False, 'raw_reason': snap.get('reason', 'unknown'),
    }
    prof = _finalize_supply_profile_dict(prof)
    _supply_cache[cache_key] = prof
    return prof

def get_supply_and_money(code, price):
    if code in _supply_cache:
        return _supply_cache[code]
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        res = requests.get(url, headers=REAL_HEADERS, timeout=5)
        res.encoding = 'euc-kr'
        df = pd.read_html(res.text, match='날짜')[0].dropna().head(10)
        
        new_cols = ['_'.join(col) if isinstance(col, tuple) else col for col in df.columns]
        df.columns = new_cols
        
        inst_col = next((c for c in df.columns if '기관' in c and '순매매' in c), None)
        frgn_col = next((c for c in df.columns if '외국인' in c and '순매매' in c), None)
        
        inst_qty = [int(float(str(v).replace(',', ''))) for v in df[inst_col].values]
        frgn_qty = [int(float(str(v).replace(',', ''))) for v in df[frgn_col].values]
        
        def get_streak(data):
            c = 0
            for v in data:
                if v > 0: c += 1
                else: break
            return c
            
        i_s, f_s = get_streak(inst_qty), get_streak(frgn_qty)
        inst_m = round((inst_qty[0] * price) / 100000000)
        frgn_m = round((frgn_qty[0] * price) / 100000000)
        total_m = abs(inst_m) + abs(frgn_m)
        
        twin_b = (inst_qty[0] > 0 and frgn_qty[0] > 0)
        leader = "🤝쌍끌" if twin_b else ("🔴기관" if inst_m > frgn_m else "🔵외인")
        
        whale_streak = 0
        for k in range(len(df)):
            if (abs(inst_qty[k]) + abs(frgn_qty[k])) * price / 100000000 >= 10:
                whale_streak += 1
            else: break
        
        w_score = (total_m // 2) + (3 if whale_streak >= 3 else 0)
        result = f"{leader}({i_s}/{f_s})", total_m, whale_streak, w_score, twin_b
        _supply_cache[code] = result
        return result
    except: 
        result = "⚠️오류", 0, 0, 0, False
        _supply_cache[code] = result
        return result
