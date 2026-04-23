# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Any, Dict
import os

try:
    import pandas as pd
except Exception:
    pd = None


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.replace(',', '').replace('%', '').strip()
            if not v:
                return default
        return float(v)
    except Exception:
        return default


def _detect_cols(df):
    cols = {c.lower(): c for c in df.columns}
    def pick(*names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None
    return {
        'open': pick('Open','시가'),
        'high': pick('High','고가'),
        'low': pick('Low','저가'),
        'close': pick('Close','종가'),
        'volume': pick('Volume','거래량'),
    }


def _log_path() -> Path:
    raw = (os.getenv('WAVE_DIAG_LOG_PATH') or '').strip()
    if raw:
        p = Path(raw)
    else:
        base = Path(os.getenv('GITHUB_WORKSPACE') or os.getcwd())
        p = base / 'wave_diag_debug.log'
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        p.touch()
    return p


def wave_diag_log_line(payload: Dict[str, Any]) -> None:
    p = _log_path()
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    parts = [
        ts,
        str(payload.get('name','-')),
        str(payload.get('code','-')),
        str(payload.get('state','-')),
        f"small={payload.get('small_low',0)}/{payload.get('small_high',0)}/{payload.get('small_state','-')}",
        f"mid={payload.get('mid_low',0)}/{payload.get('mid_high',0)}/{payload.get('mid_state','-')}",
        f"blue={payload.get('blue_low',0)}/{payload.get('blue_high',0)}",
        f"power={payload.get('power_tag','-')}({payload.get('power_score',0)})",
        f"handoff={payload.get('hand_tag','-')}({payload.get('hand_score',0)})",
        f"note={payload.get('note','-')}",
        f"cols={payload.get('cols','-')}",
        f"len={payload.get('len',0)}",
    ]
    with p.open('a', encoding='utf-8') as f:
        f.write(' | '.join(map(str, parts)) + '
')


def _power_candle(df, cols):
    out = {'score': 0, 'tag': '', 'comment': ''}
    if len(df) < 2:
        return out
    r = df.iloc[-1]
    o = _safe_float(r[cols['open']])
    h = _safe_float(r[cols['high']])
    l = _safe_float(r[cols['low']])
    c = _safe_float(r[cols['close']])
    v = _safe_float(r[cols['volume']]) if cols['volume'] else 0.0
    rng = max(h - l, 1e-9)
    body = abs(c - o)
    upper = max(h - max(o, c), 0.0) / rng
    close_pos = (c - l) / rng
    prev = df.tail(20)
    vma = _safe_float(prev[cols['volume']].mean()) if cols['volume'] else 0.0
    score = 0
    if c > o: score += 20
    if body / rng >= 0.45: score += 25
    if close_pos >= 0.7: score += 20
    if upper <= 0.2: score += 15
    if vma > 0 and v >= vma * 1.5: score += 20
    out['score'] = int(score)
    if score >= 70:
        out['tag'] = '세력캔들 강'
        out['comment'] = '양봉 몸통·종가 위치·거래량이 강하게 동반'
    elif score >= 45:
        out['tag'] = '세력캔들 보통'
        out['comment'] = '밀집 구간에서 주도 캔들 가능성'
    return out


def _handoff(df, cols):
    out = {'score': 0, 'tag': '', 'comment': ''}
    if len(df) < 5 or not cols['volume']:
        return out
    recent = df.tail(5)
    closes = recent[cols['close']].astype(float)
    vols = recent[cols['volume']].astype(float)
    up = (closes.diff().fillna(0) > 0).sum()
    vol_up = vols.iloc[-1] >= vols.mean() * 1.2 if len(vols) else False
    score = 0
    if up >= 3: score += 35
    if vol_up: score += 35
    if closes.iloc[-1] >= closes.mean(): score += 30
    out['score'] = int(score)
    if score >= 70:
        out['tag'] = '손바뀜 강'
        out['comment'] = '최근 5일 가격 회복과 거래량 증가가 동반'
    elif score >= 40:
        out['tag'] = '손바뀜 보통'
        out['comment'] = '약한 손바뀜 흔적'
    return out


def build_wave_package_v3(df):
    empty = {
        'accum': {'found': False, 'score': 0, 'grade': '없음', 'label': '매집봉 없음', 'high': 0.0, 'low': 0.0, 'close': 0.0, 'open': 0.0, 'desc': '데이터 부족'},
        'waves': {'small_low': 0.0, 'small_high': 0.0, 'small_mid': 0.0, 'mid_low': 0.0, 'mid_high': 0.0, 'mid_mid': 0.0, 'small_state': '', 'mid_state': '', 'small_desc': '', 'mid_desc': '', 'blue_zone_low': 0.0, 'blue_zone_high': 0.0, 'debug_note': '-', 'debug_cols': '-', 'debug_len': 0},
        'position': {'score': 0, 'label': '자리 정보 부족', 'desc': '', 'pullback_score': 0, 'pullback_label': '', 'd150': 0.0, 'd200': 0.0, 'drawdown_score': 0, 'drawdown_label': '', 'drawdown_pct': 0.0, 'overheat_penalty': 0, 'overheat_label': '', 'disparity': 100.0},
        'power_candle': {'score': 0, 'tag': '', 'comment': ''},
        'handoff': {'score': 0, 'tag': '', 'comment': ''},
    }
    try:
        if pd is None or df is None or len(df) < 10:
            wave_diag_log_line({'name':'-', 'code':'-', 'state':'-', 'note':'data_short', 'cols':'-', 'len': 0})
            return empty
        cols = _detect_cols(df)
        if not cols['high'] or not cols['low'] or not cols['close'] or not cols['open']:
            empty['waves']['debug_note'] = 'missing_price_cols'
            empty['waves']['debug_cols'] = str(cols)
            empty['waves']['debug_len'] = len(df)
            wave_diag_log_line({'name':'-', 'code':'-', 'state':'-', 'note':'missing_price_cols', 'cols':str(cols), 'len': len(df)})
            return empty

        recent20 = df.tail(20)
        recent60 = df.tail(60)
        sh = _safe_float(recent20[cols['high']].max())
        sl = _safe_float(recent20[cols['low']].min())
        mh = _safe_float(recent60[cols['high']].max())
        ml = _safe_float(recent60[cols['low']].min())
        c = _safe_float(df.iloc[-1][cols['close']])
        small_state = '소파동 상단 접근' if c < sh else '소파동 전고점 돌파'
        if c < (sl + sh) / 2.0:
            small_state = '소파동 하단'
        mid_state = '중파동 상단 접근' if c < mh else '중파동 전고점 돌파'
        if c < (ml + mh) / 2.0:
            mid_state = '중파동 하단'
        waves = {
            'small_low': round(sl,2), 'small_high': round(sh,2), 'small_mid': round((sl+sh)/2.0,2),
            'mid_low': round(ml,2), 'mid_high': round(mh,2), 'mid_mid': round((ml+mh)/2.0,2),
            'small_state': small_state, 'mid_state': mid_state,
            'small_desc': '최근 20봉 기준 소파동 범위', 'mid_desc': '최근 60봉 기준 중파동 범위',
            'blue_zone_low': round(sl + (sh-sl)*0.55,2), 'blue_zone_high': round(sl + (sh-sl)*0.92,2),
            'debug_note': 'wave_signal_engine_v3', 'debug_cols': ','.join(k for k,v in cols.items() if v), 'debug_len': len(df),
        }
        accum = {
            'found': True,
            'score': int(60 if c >= (sl+sh)/2.0 else 45),
            'grade': 'B' if c >= (sl+sh)/2.0 else 'C',
            'label': '매집봉 후보',
            'high': sh, 'low': sl, 'close': c, 'open': _safe_float(df.iloc[-1][cols['open']]),
            'desc': '최근 20봉 가격 응축 구간 기반 간이 계산',
        }
        disparity = 100.0
        if len(recent20) >= 1:
            ma20 = _safe_float(recent20[cols['close']].mean())
            if ma20 > 0:
                disparity = round(c / ma20 * 100.0, 2)
        position = {
            'score': 50,
            'label': '자리 보통',
            'desc': '모듈 기반 간이 자리 평가',
            'pullback_score': 40,
            'pullback_label': '보통 눌림',
            'd150': 0.0,
            'd200': 0.0,
            'drawdown_score': 35,
            'drawdown_label': '중립',
            'drawdown_pct': round((c/mh-1.0)*100.0,2) if mh>0 else 0.0,
            'overheat_penalty': 10 if disparity >= 115 else 0,
            'overheat_label': '과열' if disparity >= 115 else '',
            'disparity': disparity,
        }
        power = _power_candle(df, cols)
        hand = _handoff(df, cols)
        wave_diag_log_line({
            'name':'-', 'code':'-', 'state':'build',
            'small_low': waves['small_low'], 'small_high': waves['small_high'], 'small_state': waves['small_state'],
            'mid_low': waves['mid_low'], 'mid_high': waves['mid_high'], 'mid_state': waves['mid_state'],
            'blue_low': waves['blue_zone_low'], 'blue_high': waves['blue_zone_high'],
            'power_tag': power['tag'], 'power_score': power['score'],
            'hand_tag': hand['tag'], 'hand_score': hand['score'],
            'note': waves['debug_note'], 'cols': waves['debug_cols'], 'len': waves['debug_len'],
        })
        return {'accum': accum, 'waves': waves, 'position': position, 'power_candle': power, 'handoff': hand}
    except Exception as e:
        empty['waves']['debug_note'] = f'error:{e}'
        try:
            wave_diag_log_line({'name':'-', 'code':'-', 'state':'error', 'note': str(e), 'cols':'-', 'len': len(df) if df is not None else 0})
        except Exception:
            pass
        return empty
