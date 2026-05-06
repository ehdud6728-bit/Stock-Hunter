# -*- coding: utf-8 -*-
"""
triangle_combo_analyzer.py
Stock-Hunter V1066

목적
- 기존 legacy_main_patched.py의 `jongbe_triangle_combo_v3(df)` 호출 호환 유지
- 고점-고점 하락 추세선 + 저점-저점 상승 추세선 기반 삼각수렴 탐지
- 삼각수렴 → 엘리어트 1파 초동 → 첫 눌림/2파 눌림 → 3파 초입 후보 분류
- 스캔 결과 DataFrame에서 별도 TOP5 텔레그램 블록 생성

설계 원칙
- scipy/ta 없이 pandas/numpy만 사용해서 GitHub Actions 의존성 리스크를 낮춤
- df 컬럼명은 Open/High/Low/Close/Volume 기준, 한글 컬럼 일부 자동 매핑
- 기존 반환 키(pass, score, triangle_pattern, apex_remain 등)를 유지해 기존 메인 코드가 깨지지 않게 함
"""
from __future__ import annotations

import math
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# =============================================================
# 공통 유틸
# =============================================================

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, str):
            s = x.replace(',', '').replace('%', '').strip()
            if s.lower() in ('', '-', 'nan', 'none', 'null', 'inf', '-inf'):
                return default
            x = s
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(round(_safe_float(x, default)))
    except Exception:
        return default


def _txt(x: Any, default: str = '') -> str:
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s.lower() in ('nan', 'none', 'null', 'inf', '-inf'):
            return default
        return re.sub(r'\s+', ' ', s)
    except Exception:
        return default


def _fmt_price(v: Any) -> str:
    n = _safe_int(v, 0)
    return f"{n:,}" if n > 0 else "-"


def _row_get(row: Any, *keys: str, default: Any = None) -> Any:
    try:
        if hasattr(row, 'to_dict'):
            d = row.to_dict()
        elif isinstance(row, dict):
            d = row
        else:
            d = {}
    except Exception:
        d = {}
    for k in keys:
        try:
            v = d.get(k, None)
        except Exception:
            v = None
        if _txt(v, ''):
            return v
    return default


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """영문/한글 OHLCV 컬럼을 표준 Open/High/Low/Close/Volume으로 보정."""
    if df is None or len(df) == 0:
        return pd.DataFrame()
    out = df.copy()
    aliases = {
        'Open': ['Open', 'open', '시가'],
        'High': ['High', 'high', '고가'],
        'Low': ['Low', 'low', '저가'],
        'Close': ['Close', 'close', '종가', '현재가', 'Price'],
        'Volume': ['Volume', 'volume', '거래량'],
    }
    for std, cand in aliases.items():
        if std not in out.columns:
            for c in cand:
                if c in out.columns:
                    out[std] = out[c]
                    break
    for c in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors='coerce')
    need = ['High', 'Low', 'Close']
    if not all(c in out.columns for c in need):
        return pd.DataFrame()
    if 'Open' not in out.columns:
        out['Open'] = out['Close']
    if 'Volume' not in out.columns:
        out['Volume'] = 0
    return out.dropna(subset=['High', 'Low', 'Close']).copy()


def _linreg(x: Iterable[float], y: Iterable[float]) -> Tuple[float, float, float]:
    """slope, intercept, r2. scipy 없이 계산."""
    x = np.asarray(list(x), dtype=float)
    y = np.asarray(list(y), dtype=float)
    if len(x) < 2 or len(y) < 2 or len(x) != len(y):
        return 0.0, 0.0, 0.0
    try:
        slope, intercept = np.polyfit(x, y, 1)
        yhat = slope * x + intercept
        ss_res = float(np.sum((y - yhat) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
        return float(slope), float(intercept), max(0.0, min(1.0, r2))
    except Exception:
        return 0.0, 0.0, 0.0


# =============================================================
# 1) 삼각수렴 탐지: 고점 추세선 + 저점 추세선
# =============================================================

def _pivot_points(df: pd.DataFrame, pivot_n: int = 2) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    highs: List[Tuple[int, float]] = []
    lows: List[Tuple[int, float]] = []
    if df is None or len(df) < pivot_n * 2 + 5:
        return highs, lows
    high_s = df['High'].reset_index(drop=True)
    low_s = df['Low'].reset_index(drop=True)
    for i in range(pivot_n, len(df) - pivot_n):
        h_win = high_s.iloc[i - pivot_n:i + pivot_n + 1]
        l_win = low_s.iloc[i - pivot_n:i + pivot_n + 1]
        h = high_s.iloc[i]
        l = low_s.iloc[i]
        if pd.notna(h) and h == h_win.max() and list(h_win).count(h) == 1:
            highs.append((i, float(h)))
        if pd.notna(l) and l == l_win.min() and list(l_win).count(l) == 1:
            lows.append((i, float(l)))
    return highs, lows


def analyze_triangle_convergence_pivot_v2(
    df: pd.DataFrame,
    window: int = 40,
    pivot_n: int = 2,
    r2_threshold: float = 0.50,
    convergence_threshold: float = 18.0,
) -> Optional[Dict[str, Any]]:
    """
    고점-고점, 저점-저점 추세선이 서로 가까워지는지 확인한다.
    반환 None이면 유효 삼각수렴이 아니거나 데이터 부족.
    """
    src = _normalize_ohlcv(df)
    if src.empty or len(src) < max(45, window + 5):
        return None

    d = src.iloc[-window:].copy().reset_index(drop=True)
    piv_high, piv_low = _pivot_points(d, pivot_n=pivot_n)
    if len(piv_high) < 2 or len(piv_low) < 2:
        return None

    xh = [p[0] for p in piv_high]
    yh = [p[1] for p in piv_high]
    xl = [p[0] for p in piv_low]
    yl = [p[1] for p in piv_low]

    slope_h, int_h, r2_h = _linreg(xh, yh)
    slope_l, int_l, r2_l = _linreg(xl, yl)

    # 너무 지저분한 피벗은 제외하되, 완전 엄격하게 하지는 않는다.
    if r2_h < r2_threshold or r2_l < r2_threshold:
        return None

    price_mean = float(d['Close'].mean() or 0)
    if price_mean <= 0:
        return None

    slope_h_pct = slope_h / price_mean * 100.0
    slope_l_pct = slope_l / price_mean * 100.0

    x_start = min(min(xh), min(xl))
    start_upper = int_h + slope_h * x_start
    start_lower = int_l + slope_l * x_start
    start_width = start_upper - start_lower

    x_end = len(d) - 1
    end_upper = int_h + slope_h * x_end
    end_lower = int_l + slope_l * x_end
    end_width = end_upper - end_lower

    if start_width <= 0:
        return None

    convergence_pct = max(min((1.0 - end_width / start_width) * 100.0, 100.0), 0.0)
    width_pct_now = end_width / max(price_mean, 1e-9) * 100.0
    lines_crossed = end_width < 0

    if slope_h_pct < -0.035 and slope_l_pct > 0.035:
        pattern = 'Symmetrical'
        pattern_kr = '대칭삼각수렴'
    elif abs(slope_h_pct) <= 0.045 and slope_l_pct > 0.035:
        pattern = 'Ascending'
        pattern_kr = '상승삼각수렴'
    elif slope_h_pct < -0.035 and abs(slope_l_pct) <= 0.045:
        pattern = 'Descending'
        pattern_kr = '하락삼각수렴'
    else:
        # 완벽한 삼각은 아니어도 박스가 좁아지는 경우는 구조수렴 후보로 남긴다.
        pattern = 'Squeeze'
        pattern_kr = '구조수렴'

    denom = slope_h - slope_l
    bars_to_apex: Optional[int]
    if abs(denom) > 1e-9:
        bars_to_apex = int(round((int_l - int_h) / denom - x_end))
    else:
        bars_to_apex = None

    vol = d['Volume'].fillna(0)
    vol_ma20 = float(vol.rolling(20).mean().iloc[-1] or 0) if len(d) >= 20 else 0.0
    close_now = float(d['Close'].iloc[-1])
    close_prev = float(d['Close'].iloc[-2]) if len(d) >= 2 else close_now
    upper_prev = int_h + slope_h * (x_end - 1)
    lower_now = end_lower

    breakout_up = bool(
        close_now > end_upper * 1.003
        and close_prev >= upper_prev * 0.995
        and (vol_ma20 <= 0 or float(vol.iloc[-1]) >= vol_ma20 * 1.15)
    )
    breakout_down = bool(
        close_now < lower_now * 0.997
        and (vol_ma20 <= 0 or float(vol.iloc[-1]) >= vol_ma20 * 1.15)
    )

    is_triangle = bool(
        not lines_crossed
        and convergence_pct >= convergence_threshold
        and width_pct_now <= 16.0
        and pattern in ('Symmetrical', 'Ascending', 'Descending', 'Squeeze')
    )

    confidence = 'HIGH' if (len(piv_high) >= 3 and len(piv_low) >= 3 and convergence_pct >= 35 and width_pct_now <= 10) else 'LOW'

    return {
        'pattern': pattern,
        'pattern_kr': pattern_kr,
        'confidence': confidence,
        'convergence_pct': round(convergence_pct, 2),
        'width_pct_now': round(width_pct_now, 2),
        'lines_crossed': lines_crossed,
        'bars_to_apex': bars_to_apex,
        'apex_remain': bars_to_apex,
        'is_triangle': is_triangle,
        'breakout_up': breakout_up,
        'breakout_down': breakout_down,
        'upper_line_now': round(end_upper, 2),
        'lower_line_now': round(end_lower, 2),
        'r2_upper': round(r2_h, 3),
        'r2_lower': round(r2_l, 3),
        'pivot_high_count': len(piv_high),
        'pivot_low_count': len(piv_low),
        'slope_upper_pct': round(slope_h_pct, 3),
        'slope_lower_pct': round(slope_l_pct, 3),
    }


# =============================================================
# 2) 지지 DNA / 종베 전환 호환 로직
# =============================================================

def analyze_support_dna(df: pd.DataFrame, target_ma: str = 'MA20', window: int = 120) -> float:
    src = _normalize_ohlcv(df)
    if src.empty:
        return 0.0
    d = src.copy()
    if target_ma not in d.columns:
        if target_ma == 'MA20':
            d['MA20'] = d['Close'].rolling(20).mean()
        elif target_ma == 'MA40':
            d['MA40'] = d['Close'].rolling(40).mean()
    if target_ma not in d.columns:
        return 0.0
    sub = d.iloc[-min(window, len(d)):].copy()
    touch = sub[(sub[target_ma] > 0) & (abs(sub['Low'] - sub[target_ma]) / (sub[target_ma] + 1e-9) < 0.018)]
    if touch.empty:
        return 0.0
    success = 0
    for idx in touch.index:
        try:
            loc = d.index.get_loc(idx)
            future = d.iloc[loc:loc + 7]
            if len(future) > 1 and float(future['High'].max()) > float(d.loc[idx, target_ma]) * 1.045:
                success += 1
        except Exception:
            continue
    return round(success / max(len(touch), 1), 3)


def _simple_dmi_state(df: pd.DataFrame) -> Dict[str, Any]:
    """ta 패키지 없이 +DI/-DI 비슷한 방향성만 간단 판정."""
    d = _normalize_ohlcv(df)
    if len(d) < 20:
        return {'dmi_ok': False, 'adx_proxy': 0.0}
    up_move = d['High'].diff()
    down_move = -d['Low'].diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = pd.concat([
        d['High'] - d['Low'],
        (d['High'] - d['Close'].shift()).abs(),
        (d['Low'] - d['Close'].shift()).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(14).mean().replace(0, np.nan)
    plus_di = 100 * pd.Series(plus_dm, index=d.index).rolling(14).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=d.index).rolling(14).mean() / atr
    spread = plus_di - minus_di
    adx_proxy = spread.abs().rolling(14).mean()
    try:
        ok = bool(plus_di.iloc[-1] > minus_di.iloc[-1] and spread.iloc[-1] > spread.iloc[-2] and adx_proxy.iloc[-1] >= 6)
        return {'dmi_ok': ok, 'adx_proxy': round(float(adx_proxy.iloc[-1] or 0), 2)}
    except Exception:
        return {'dmi_ok': False, 'adx_proxy': 0.0}


def _classify_triangle_wave_phase(df: pd.DataFrame, tri: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """삼각수렴 이후 1파/첫눌림/3파 초입을 단순 가격구조로 분류."""
    d = _normalize_ohlcv(df)
    if d.empty or len(d) < 45:
        return {'phase': '파동미확인', 'tag': '🌊파동확인필요', 'comment': '소파동·중파동 데이터가 부족합니다.', 'score': 0}

    close = d['Close']
    high = d['High']
    low = d['Low']
    vol = d['Volume'].fillna(0)
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()

    recent_low = float(low.iloc[-30:].min())
    recent_high = float(high.iloc[-30:].max())
    c = float(close.iloc[-1])
    if recent_high <= recent_low:
        pos = 0.0
    else:
        pos = (c - recent_low) / (recent_high - recent_low) * 100.0

    breakout_up = bool((tri or {}).get('breakout_up', False))
    above_ma = bool(c >= float(ma5.iloc[-1] or c) and c >= float(ma20.iloc[-1] or c))
    ma_turn = bool(float(ma5.iloc[-1] or 0) >= float(ma20.iloc[-1] or 0) * 0.995)
    vol_calm = bool(len(vol) >= 6 and float(vol.iloc[-1]) <= float(vol.iloc[-5:].mean() or 0) * 1.25)

    # 1차 상승폭 대비 되돌림률 추정
    pullback_pct = 0.0
    try:
        impulse_high = float(high.iloc[-15:].max())
        impulse_low = float(low.iloc[-40:-10].min()) if len(low) >= 45 else recent_low
        if impulse_high > impulse_low:
            pullback_pct = (impulse_high - c) / (impulse_high - impulse_low) * 100.0
    except Exception:
        pullback_pct = 0.0

    if breakout_up and above_ma and pos >= 55:
        return {'phase': '1파초동', 'tag': '🌊1파초동', 'comment': '수렴 상단을 거래량과 함께 넘기기 시작한 자리입니다. 첫 돌파 뒤 눌림 깊이를 확인합니다.', 'score': 18, 'pullback_pct': round(pullback_pct, 1)}
    if ma_turn and 22 <= pullback_pct <= 62 and c >= float(ma20.iloc[-1] or 0) * 0.98:
        return {'phase': '1차눌림', 'tag': '🌊1차눌림', 'comment': '1차 상승 뒤 38.2~61.8 부근에서 쉬는 자리입니다. 5일선·파란점선 재지지가 핵심입니다.', 'score': 25, 'pullback_pct': round(pullback_pct, 1)}
    if ma_turn and 10 <= pullback_pct <= 45 and c >= float(ma5.iloc[-1] or 0) * 0.995 and not vol_calm:
        return {'phase': '3파초입후보', 'tag': '🌊3파초입후보', 'comment': '첫 눌림 이후 다시 힘이 붙는 후보입니다. 재양봉과 거래량 재유입이 필요합니다.', 'score': 20, 'pullback_pct': round(pullback_pct, 1)}
    if pos <= 45:
        return {'phase': '수렴하단관찰', 'tag': '🌊수렴하단관찰', 'comment': '수렴 하단권이라 손절을 짧게 두고 재지지 여부를 보는 구간입니다.', 'score': 10, 'pullback_pct': round(pullback_pct, 1)}
    return {'phase': '수렴상단대기', 'tag': '🌊수렴상단대기', 'comment': '수렴 상단 접근 구간입니다. 상단 돌파 후 안착 전 추격은 줄입니다.', 'score': 12, 'pullback_pct': round(pullback_pct, 1)}


def jongbe_triangle_combo_v3(df: pd.DataFrame) -> Dict[str, Any]:
    """기존 main과 호환되는 삼각수렴+종베 통합 결과."""
    fail = {
        'date': 'N/A', 'pass': False, 'grade': 'C (관찰)', 'score': 0,
        'jongbe': False, 'has_triangle': False, 'ma20_dna': '0%',
        'triangle_pattern': 'None', 'convergence_pct': 0, 'apex_remain': None,
        'is_breakout': False, 'lines_crossed': False, 'triangle': {}, 'jongbe_detail': {},
        'wave_phase': '파동미확인', 'wave_tag': '🌊파동확인필요', 'wave_comment': '데이터 부족',
    }
    try:
        d = _normalize_ohlcv(df)
        if d.empty or len(d) < 60:
            return fail
        d = d.copy()
        d['MA5'] = d['Close'].rolling(5).mean()
        d['MA20'] = d['Close'].rolling(20).mean()
        d['MA40'] = d['Close'].rolling(40).mean()
        d['MA60'] = d['Close'].rolling(60).mean()
        d['MA20_slope'] = (d['MA20'] - d['MA20'].shift(5)) / (d['MA20'].shift(5) + 1e-9) * 100.0
        d['MA40_slope'] = (d['MA40'] - d['MA40'].shift(5)) / (d['MA40'].shift(5) + 1e-9) * 100.0

        cur = d.iloc[-1]
        prev = d.iloc[-2]
        cross_recent = bool(((d['MA20'] > d['MA40']) & (d['MA20'].shift(1) <= d['MA40'].shift(1))).iloc[-6:].any())
        gap_ratio = abs(float(cur['MA20']) - float(cur['MA40'])) / (float(cur['MA40']) + 1e-9)
        cross_near = bool(float(cur['MA20']) > float(cur['MA40']) and gap_ratio < 0.035 and float(cur['MA20']) > float(d['MA20'].iloc[-3]))
        ma20_rising = bool(float(cur['MA20_slope'] or 0) > -0.02)
        ma40_rising = bool(float(cur['MA40_slope'] or 0) > -0.08)
        ma20_accel = bool(float(cur['MA20_slope'] or 0) >= float(d['MA20_slope'].shift(5).iloc[-1] or -999))
        jongbe_ok = bool((cross_recent or cross_near) and ma20_rising and ma40_rising and ma20_accel and float(cur['Close']) >= float(cur['MA20']) * 0.995)

        tri = analyze_triangle_convergence_pivot_v2(d)
        has_triangle = bool(tri and tri.get('is_triangle'))
        dmi = _simple_dmi_state(d)
        dna = analyze_support_dna(d, 'MA20')
        wave = _classify_triangle_wave_phase(d, tri)

        score = 0
        if jongbe_ok:
            score += 28
        if dmi.get('dmi_ok'):
            score += 8
        if has_triangle:
            score += 30
            if tri.get('confidence') == 'HIGH':
                score += 7
            if tri.get('breakout_up'):
                score += 12
            if tri.get('breakout_down'):
                score -= 25
            apex = tri.get('bars_to_apex')
            if isinstance(apex, (int, float)):
                if -3 <= apex <= 7:
                    score += 8
                elif apex < -8:
                    score -= 8
        if dna >= 0.65:
            score += 8
        score += int(wave.get('score', 0))
        score = max(0, min(100, int(score)))

        if score >= 82:
            grade = 'A+ (강한관찰)'
        elif score >= 68:
            grade = 'A (관찰)'
        elif score >= 52:
            grade = 'B (보조관찰)'
        else:
            grade = 'C (대기)'

        return {
            'date': str(d.index[-1])[:10],
            'pass': bool(score >= 60 and (has_triangle or jongbe_ok)),
            'grade': grade,
            'score': score,
            'jongbe': jongbe_ok,
            'has_triangle': has_triangle,
            'ma20_dna': f"{round(dna * 100)}%",
            'triangle_pattern': (tri or {}).get('pattern', 'None'),
            'triangle_pattern_kr': (tri or {}).get('pattern_kr', 'None'),
            'convergence_pct': (tri or {}).get('convergence_pct', 0),
            'apex_remain': (tri or {}).get('bars_to_apex', None),
            'is_breakout': bool((tri or {}).get('breakout_up', False)),
            'lines_crossed': bool((tri or {}).get('lines_crossed', False)),
            'triangle': tri or {},
            'jongbe_detail': {
                'cross_recent': cross_recent,
                'cross_near': cross_near,
                'ma20_rising': ma20_rising,
                'ma40_rising': ma40_rising,
                'ma20_accel': ma20_accel,
                'dmi_ok': bool(dmi.get('dmi_ok')),
                'adx_proxy': dmi.get('adx_proxy', 0),
            },
            'wave_phase': wave.get('phase', ''),
            'wave_tag': wave.get('tag', ''),
            'wave_comment': wave.get('comment', ''),
            'pullback_pct': wave.get('pullback_pct', 0),
        }
    except Exception as e:
        out = dict(fail)
        out['error'] = str(e)
        return out


# =============================================================
# 3) 기존 스캔 결과 DataFrame용 삼각수렴 TOP5 블록
# =============================================================

def _has_triangle_text(row: Any) -> bool:
    blob = ' '.join(_txt(_row_get(row, k, default=''), '') for k in [
        'N구분', 'N조합', '검색패턴', 'triangle_pattern', '삼각수렴태그', '구조', '패턴', '신호'
    ])
    blob_l = blob.lower()
    return any(k in blob for k in ['삼각', '수렴', '구조수렴', '🔺']) or 'triangle' in blob_l


def _infer_wave_phase_from_row(row: Any) -> Tuple[str, str, int]:
    phase = _txt(_row_get(row, '파동타점상태', '엘리어트단계', 'wave_phase', default=''), '')
    pull_score = _safe_int(_row_get(row, '파동눌림점수', 'pullback_score', default=0), 0)
    restart_score = _safe_int(_row_get(row, '파동재출발점수', 'restart_score', default=0), 0)
    first_score = _safe_int(_row_get(row, '파동1파점수', 'first_wave_score', default=0), 0)
    rsi = _safe_float(_row_get(row, 'RSI', default=0), 0)
    disp = _safe_float(_row_get(row, '이격', default=100), 100)
    small_pos = _txt(_row_get(row, '소파동위치권', '소파동위치', default=''), '')
    mid_pos = _txt(_row_get(row, '중파동위치권', '중파동위치', default=''), '')
    txt = f"{phase} {small_pos} {mid_pos}"

    if '3파' in txt or restart_score >= 18:
        return '🌊3파초입후보', '1차 눌림 뒤 다시 힘이 붙는 후보입니다. 재양봉과 거래량 재유입이 확인될 때만 실행합니다.', 25
    if '눌림' in txt or pull_score >= 16 or (95 <= disp <= 110 and 38 <= rsi <= 65 and ('중단' in txt or '하단' in txt)):
        return '🌊1차눌림', '삼각수렴 또는 1차 상승 뒤 쉬는 자리입니다. 눌림이 얕고 기준선 위에서 버티면 다음 파동 후보입니다.', 30
    if '1파' in txt or first_score >= 15:
        return '🌊1파초동', '수렴 이후 첫 상승파동이 시작된 후보입니다. 바로 추격보다 첫 눌림을 확인합니다.', 22
    if '상단' in txt:
        return '🌊상단대기', '수렴 상단권입니다. 돌파 안착 전에는 추격보다 눌림 확인이 유리합니다.', 10
    return '🌊수렴관찰', '수렴 에너지가 모이는 후보입니다. 상단 돌파 또는 1차 눌림 재지지를 기다립니다.', 14


def _supply_score_and_label(row: Any) -> Tuple[int, str]:
    label = _txt(_row_get(row, '수급판정', '수급요약', '수급', default=''), '')
    blob = ' '.join([
        label,
        _txt(_row_get(row, '수급당일', '최근1일수급', '수급1일', default=''), ''),
        _txt(_row_get(row, '수급3일누적', '최근3일수급', '수급3일', default=''), ''),
        _txt(_row_get(row, '수급5일누적', '최근5일수급', '수급5일', default=''), ''),
    ])
    score = 0
    if any(k in blob for k in ['쌍끌강', '동반매수 강함', '외인·기관 동반매수 강함']):
        score += 18
    elif any(k in blob for k in ['쌍끌', '동반매수', '외인·기관']):
        score += 14
    elif any(k in blob for k in ['외인강', '외인우세', '외인매수']):
        score += 10
    elif any(k in blob for k in ['기관강', '기관우세', '기관매수']):
        score += 9
    if any(k in blob for k in ['개인수급/외인기관매도', '수급부담', '외인기관매도']):
        score -= 10
    if not label:
        label = '수급 확인 필요'
    return max(-15, min(20, score)), label


def _format_supply_day_lines(row: Any) -> List[str]:
    label = _txt(_row_get(row, '수급판정', '수급요약', '수급', default=''), '')
    d1 = _txt(_row_get(row, '수급당일', '최근1일수급', '수급1일', default=''), '')
    d3 = _txt(_row_get(row, '수급3일누적', '최근3일수급', '수급3일', default=''), '')
    d5 = _txt(_row_get(row, '수급5일누적', '최근5일수급', '수급5일', default=''), '')
    lines = []
    if label:
        lines.append(f"   🏦 수급: {label}")
    if d1:
        lines.append(f"      ├ 1일: {d1}")
    if d3:
        lines.append(f"      └ 3일: {d3}")
    elif d5:
        lines.append(f"      └ 5일: {d5}")
    return lines


def _wave_line_from_row(row: Any) -> str:
    s_low = _safe_int(_row_get(row, '소파동저점', default=0), 0)
    s_high = _safe_int(_row_get(row, '소파동전고점', default=0), 0)
    s_pos = _txt(_row_get(row, '소파동위치권', '소파동위치', default=''), '')
    s_dir = _txt(_row_get(row, '소파동박스방향', default=''), '')
    s_ang = _safe_float(_row_get(row, '소파동각도', '파동각도', default=0), 0)
    m_low = _safe_int(_row_get(row, '중파동저점', default=0), 0)
    m_high = _safe_int(_row_get(row, '중파동전고점', default=0), 0)
    m_pos = _txt(_row_get(row, '중파동위치권', '중파동위치', default=''), '')
    m_dir = _txt(_row_get(row, '중파동박스방향', default=''), '')
    m_ang = _safe_float(_row_get(row, '중파동각도', default=0), 0)
    if s_low > 0 and s_high > 0:
        small = f"소 {s_low:,}~{s_high:,} {s_pos or '위치확인'}"
        if s_dir:
            small += f"/{s_dir}"
        small += f" 각{s_ang:.1f}"
    else:
        small = '소파동 확인 필요'
    if m_low > 0 and m_high > 0:
        mid = f"중 {m_low:,}~{m_high:,} {m_pos or '위치확인'}"
        if m_dir:
            mid += f"/{m_dir}"
        mid += f" 각{m_ang:.1f}"
    else:
        mid = '중파동 확인 필요'
    return f"   〰️/📶 파동: {small} | {mid}"


def _fib_line_from_row(row: Any) -> str:
    f382 = _safe_int(_row_get(row, 'Fib382', 'fib382', '피보382', default=0), 0)
    f500 = _safe_int(_row_get(row, 'Fib500', 'fib500', '피보500', default=0), 0)
    f618 = _safe_int(_row_get(row, 'Fib618', 'fib618', '피보618', default=0), 0)
    comment = _txt(_row_get(row, '피보해설', '피보회귀해설', 'fib_comment', default=''), '')
    if f382 > 0 or f500 > 0 or f618 > 0:
        base = f"   📐 피보/회귀: 38.2 {_fmt_price(f382)} / 50 {_fmt_price(f500)} / 61.8 {_fmt_price(f618)}"
        if comment:
            base += f" | {comment}"
        return base
    return "   📐 피보/회귀: 38.2/50/61.8 기준 추가 확인"


def score_triangle_candidate_from_row(row: Any) -> Dict[str, Any]:
    name = _txt(_row_get(row, '종목명', 'Name', 'name', default='종목명확인'), '종목명확인')
    code = _txt(_row_get(row, 'code', 'Code', '종목코드', default=''), '')
    current = _safe_int(_row_get(row, '현재가', 'Close', 'Price', default=0), 0)
    blob = ' '.join(_txt(_row_get(row, k, default=''), '') for k in ['N구분', 'N조합', '검색패턴', '구조', '수박정제태그'])

    tri_flag = bool(_row_get(row, 'triangle_signal', '삼각수렴', default=False)) or _has_triangle_text(row)
    structure_squeeze = any(k in blob for k in ['구조수렴', '수렴', '삼각', '🔺'])
    phase_tag, phase_comment, phase_score = _infer_wave_phase_from_row(row)
    supply_score, supply_label = _supply_score_and_label(row)
    safe_score = _safe_int(_row_get(row, '안전점수', default=0), 0)
    n_score = _safe_int(_row_get(row, 'N점수', default=0), 0)
    rsi = _safe_float(_row_get(row, 'RSI', default=0), 0)
    disp = _safe_float(_row_get(row, '이격', default=100), 100)
    bb40 = _safe_float(_row_get(row, 'BB40', 'BB40폭', default=0), 0)
    ma_ultra = _safe_float(_row_get(row, '초단기MA수렴도', 'MA수렴', default=0), 0)
    ma_short = _safe_float(_row_get(row, '단기MA수렴도', default=0), 0)
    kki_angle = _safe_float(_row_get(row, '파동각도', '소파동각도', default=0), 0)
    absorb = _safe_float(_row_get(row, '흡수점수', '흡수', default=0), 0)

    score = 0
    if tri_flag:
        score += 34
    if structure_squeeze:
        score += 16
    if 0 < bb40 <= 12:
        score += 10
    elif 0 < bb40 <= 22:
        score += 6
    if 0 < ma_ultra <= 5:
        score += 5
    if 0 < ma_short <= 7:
        score += 4
    score += phase_score
    score += supply_score
    if 35 <= rsi <= 66:
        score += 8
    elif rsi >= 70:
        score -= 12
    if 96 <= disp <= 112:
        score += 7
    elif disp >= 116:
        score -= 10
    if safe_score >= 500:
        score += 7
    elif safe_score >= 350:
        score += 4
    if n_score >= 650:
        score += 6
    elif n_score >= 450:
        score += 3
    if abs(kki_angle) >= 25:
        score += 4
    if absorb >= 10:
        score += 3

    # 삼각/구조수렴 흔적이 전혀 없으면 별도 TOP5 대상에서 제외되도록 점수 대폭 제한
    if not tri_flag and not structure_squeeze:
        score = min(score, 39)

    score = max(0, min(100, int(score)))
    if '1차눌림' in phase_tag:
        role = '수렴돌파 후 첫눌림'
    elif '3파' in phase_tag:
        role = '첫눌림 후 재출발 후보'
    elif '1파' in phase_tag:
        role = '수렴돌파 1파 초동'
    else:
        role = '삼각수렴 관찰'

    return {
        'name': name, 'code': code, 'current': current, 'score': score,
        'role': role, 'phase_tag': phase_tag, 'phase_comment': phase_comment,
        'supply_label': supply_label, 'tri_flag': tri_flag, 'structure_squeeze': structure_squeeze,
        'rsi': rsi, 'disp': disp, 'bb40': bb40, 'safe_score': safe_score, 'n_score': n_score,
    }


def build_triangle_squeeze_top5_df(df: pd.DataFrame, limit: int = 5) -> pd.DataFrame:
    if df is None or not hasattr(df, 'empty') or df.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        try:
            scored = score_triangle_candidate_from_row(row)
            if scored['score'] >= 45:
                item = row.to_dict()
                item['_triangle_score'] = scored['score']
                item['_triangle_role'] = scored['role']
                item['_triangle_phase_tag'] = scored['phase_tag']
                item['_triangle_phase_comment'] = scored['phase_comment']
                rows.append(item)
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    sort_cols = [c for c in ['_triangle_score', '안전점수', 'N점수', '거래대금', 'Amount'] if c in out.columns]
    if sort_cols:
        out = out.sort_values(by=sort_cols, ascending=[False] * len(sort_cols), na_position='last')
    code_col = 'code' if 'code' in out.columns else ('Code' if 'Code' in out.columns else None)
    if code_col:
        out = out.drop_duplicates(subset=[code_col], keep='first')
    return out.head(limit).reset_index(drop=True)


def build_triangle_squeeze_top5_block(df: pd.DataFrame, limit: int = 5, title: str = '삼각수렴→1파→첫눌림 TOP 5') -> str:
    top = build_triangle_squeeze_top5_df(df, limit=limit)
    if top is None or top.empty:
        return f"🏆 [{title}]\n\n- 해당 종목 없음"

    lines: List[str] = []
    lines.append(f"🏆 [{title}]")
    lines.append('')
    lines.append('기준: 고점 추세선은 낮아지고 저점 추세선은 올라가며 에너지가 모인 뒤, 1파 초동 또는 첫눌림 재지지 가능성을 따로 추립니다.')
    lines.append('')
    for i, row in top.iterrows():
        scored = score_triangle_candidate_from_row(row)
        name = scored['name']
        code = scored['code']
        cp = f"({code})" if code else ''
        price = scored['current']
        role = scored['role']
        phase_tag = scored['phase_tag']
        phase_comment = scored['phase_comment']
        score = scored['score']
        signal = _txt(_row_get(row, 'N조합', '신호', default='삼각수렴/구조수렴'), '삼각수렴/구조수렴')
        base = _safe_int(_row_get(row, '파란점선기준가', '기준가', 'blue_line_price', default=0), 0)
        target = _safe_int(_row_get(row, '🎯목표타점', '목표1', default=0), 0)
        stop = _safe_int(_row_get(row, '🚨손절가', '손절', default=0), 0)
        ai = _txt(_row_get(row, 'ai_tip', 'AI코멘트', default=''), '')
        if not ai:
            if '부담' in scored['supply_label'] or scored['rsi'] >= 70 or scored['disp'] >= 116:
                ai = '수렴 구조는 보이지만 과열·수급 부담이 있어 회복선 위 안착과 거래량 재유입을 확인합니다.'
            else:
                ai = '수렴 에너지는 확인됩니다. 급하게 쫓기보다 5일선·파란점선 재지지 후 접근하는 편이 좋습니다.'

        lines.append(f"{i + 1}) {name}{cp} | 현재 {_fmt_price(price)} | 점수 {score} | {role}")
        lines.append(f"   🏷️ {phase_tag} | {signal}")
        lines.append(f"   🌊 파동: {phase_comment}")
        lines.append(_wave_line_from_row(row))
        lines.append(_fib_line_from_row(row))
        if base > 0:
            lines.append(f"   🔵 기준선: {_fmt_price(base)}원 재지지/종가 안착 확인")
        lines.append(f"   🧭 해설: 삼각수렴은 매수 신호라기보다 에너지가 모인 자리입니다. {phase_comment}")
        lines.append(f"   🤖 AI코멘트: {ai[:120]}")
        if target > 0 or stop > 0:
            lines.append(f"   🎯 가격: 목표 {_fmt_price(target)} / 손절 {_fmt_price(stop)}")
        for sline in _format_supply_day_lines(row):
            lines.append(sline)
        lines.append('')
    return '\n'.join(lines).rstrip()


def enrich_triangle_squeeze_columns(df: pd.DataFrame) -> pd.DataFrame:
    """원하면 외부에서 호출해 triangle score/role 컬럼을 붙일 수 있다."""
    if df is None or not hasattr(df, 'empty') or df.empty:
        return df
    out = df.copy()
    scores, roles, phases, comments = [], [], [], []
    for _, row in out.iterrows():
        s = score_triangle_candidate_from_row(row)
        scores.append(s['score'])
        roles.append(s['role'])
        phases.append(s['phase_tag'])
        comments.append(s['phase_comment'])
    out['삼각수렴점수'] = scores
    out['삼각수렴역할'] = roles
    out['삼각수렴파동'] = phases
    out['삼각수렴해설'] = comments
    return out

# =============================================================
# V1067: 예비 삼각수렴 · BB응축 TOP3
# =============================================================

def _contains_any(text: str, words: Iterable[str]) -> bool:
    try:
        s = _txt(text, '')
        return any(w in s for w in words)
    except Exception:
        return False


def score_triangle_pre_squeeze_from_row(row: Any) -> Dict[str, Any]:
    """아직 터지기 전의 삼각수렴 + 볼린저밴드 응축 후보를 점수화한다.

    목적은 이미 돌파한 종목이 아니라, 고점/저점 추세선이 좁아지고 BB 에너지가
    모이는 후보를 예비 관찰용으로 따로 뽑는 것이다. 실제 진입은 5일선·파란점선
    재지지와 거래량 재유입 확인 후로 제한한다.
    """
    name = _txt(_row_get(row, '종목명', 'Name', 'name', default='종목명확인'), '종목명확인')
    code = _txt(_row_get(row, 'code', 'Code', '종목코드', default=''), '')
    current = _safe_int(_row_get(row, '현재가', 'Close', 'Price', default=0), 0)
    blob = ' '.join(_txt(_row_get(row, k, default=''), '') for k in [
        'N구분', 'N조합', '검색패턴', '구조', '수박정제태그', '수박상태', '유형', '신호'
    ])

    tri_flag = bool(_row_get(row, 'triangle_signal', '삼각수렴', default=False)) or _has_triangle_text(row)
    structure_squeeze = any(k in blob for k in ['구조수렴', '삼각수렴', '삼각', '🔺', '수렴'])

    bb20 = _safe_float(_row_get(row, 'BB20폭', 'BB20', 'bb20_width', 'BB20_WIDTH', default=0), 0)
    bb40 = _safe_float(_row_get(row, 'BB40폭', 'BB40', 'bb40_width', 'BB40_WIDTH', default=0), 0)
    ma_ultra = _safe_float(_row_get(row, '초단기MA수렴도', '초', 'MA초', 'MA수렴', default=0), 0)
    ma_short = _safe_float(_row_get(row, '단기MA수렴도', '단', 'MA단', default=0), 0)
    ma_struct = _safe_float(_row_get(row, '구조MA수렴도', '구', 'MA구', default=0), 0)
    vol_ratio = _safe_float(_row_get(row, '거래량비', 'VolumeRatio', 'vol_ratio', 'volume_ratio', default=0), 0)
    rsi = _safe_float(_row_get(row, 'RSI', 'RSI14', default=0), 0)
    disp = _safe_float(_row_get(row, '이격', 'Disparity', default=100), 100)
    safe_score = _safe_int(_row_get(row, '안전점수', default=0), 0)
    n_score = _safe_int(_row_get(row, 'N점수', default=0), 0)
    supply_score, supply_label = _supply_score_and_label(row)

    score = 0
    if tri_flag:
        score += 28
    if structure_squeeze:
        score += 18

    # BB 응축: 낮을수록 에너지 저장으로 해석. 단, 지나치게 넓으면 예비 응축에서 감점.
    if 0 < bb20 <= 8:
        score += 12
    elif 0 < bb20 <= 12:
        score += 8
    elif 0 < bb20 <= 18:
        score += 4
    if 0 < bb40 <= 10:
        score += 22
    elif 0 < bb40 <= 16:
        score += 16
    elif 0 < bb40 <= 22:
        score += 9
    elif bb40 >= 32:
        score -= 8

    # 이평선/구조 응축: 파동 에너지가 모이는지 확인.
    if 0 < ma_ultra <= 3.8:
        score += 10
    elif 0 < ma_ultra <= 5.5:
        score += 6
    if 0 < ma_short <= 5.5:
        score += 8
    elif 0 < ma_short <= 7.5:
        score += 4
    if 0 < ma_struct <= 7.5:
        score += 6
    elif 0 < ma_struct <= 10.0:
        score += 3

    # 거래량이 과하게 터진 것보다 줄어든 상태를 예비 응축으로 우대.
    if 0 < vol_ratio <= 0.85:
        score += 7
    elif 0 < vol_ratio <= 1.15:
        score += 4

    # 아직 과열 전이어야 예비 가치가 높다.
    if 36 <= rsi <= 62:
        score += 8
    elif 30 <= rsi < 36 or 62 < rsi <= 67:
        score += 4
    elif rsi >= 70:
        score -= 12
    if 94 <= disp <= 106:
        score += 8
    elif 106 < disp <= 112:
        score += 3
    elif disp >= 115:
        score -= 12

    score += min(16, max(0, supply_score))
    if safe_score >= 500:
        score += 6
    elif safe_score >= 350:
        score += 3
    if n_score >= 650:
        score += 5
    elif n_score >= 450:
        score += 2

    # 이미 강하게 터진 문구는 예비 TOP3에서는 살짝 감점하되 완전 배제하지 않는다.
    breakout_blob = _contains_any(blob, ['거래량폭발초동돌파', '진짜매집발사', '수박재폭발', '독사발사', '발사형'])
    late_blob = _contains_any(blob, ['후행형', '후행수박', '추격금지'])
    if breakout_blob:
        score -= 7
    if late_blob:
        score -= 10

    has_energy = bool(tri_flag or structure_squeeze or (0 < bb40 <= 16) or (0 < ma_ultra <= 5.5 and 0 < ma_short <= 7.5))
    if not has_energy:
        score = min(score, 35)

    score = max(0, min(100, int(score)))
    if score >= 80:
        readiness = '응축 우수'
        comment = '삼각수렴과 BB응축이 같이 보이는 예비 구간입니다. 돌파 전이라 추격보다 기준선 재지지와 거래량 증가를 확인합니다.'
    elif score >= 65:
        readiness = '응축 관찰'
        comment = '수렴 에너지는 보이지만 아직 확정 돌파 전입니다. 파란점선 위 종가와 거래량 재유입이 붙을 때 관심도가 올라갑니다.'
    else:
        readiness = '예비 관찰'
        comment = '수렴 단서는 있으나 신뢰도는 더 확인이 필요합니다. 5일선 이탈 시 관찰 우선순위를 낮춥니다.'

    return {
        'name': name, 'code': code, 'current': current, 'score': score,
        'readiness': readiness, 'comment': comment,
        'tri_flag': bool(tri_flag), 'structure_squeeze': bool(structure_squeeze),
        'bb20': bb20, 'bb40': bb40, 'ma_ultra': ma_ultra, 'ma_short': ma_short,
        'ma_struct': ma_struct, 'vol_ratio': vol_ratio, 'rsi': rsi, 'disp': disp,
        'supply_label': supply_label, 'supply_score': supply_score,
    }


def build_triangle_pre_squeeze_top3_df(df: pd.DataFrame, limit: int = 3) -> pd.DataFrame:
    """예비 삼각수렴·BB응축 후보 TOP3 DataFrame."""
    if df is None or not hasattr(df, 'empty') or df.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        try:
            scored = score_triangle_pre_squeeze_from_row(row)
            if scored['score'] >= 55:
                item = row.to_dict()
                item['_triangle_pre_score'] = scored['score']
                item['_triangle_pre_readiness'] = scored['readiness']
                item['_triangle_pre_comment'] = scored['comment']
                rows.append(item)
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    sort_cols = [c for c in ['_triangle_pre_score', '안전점수', 'N점수', '거래대금', 'Amount'] if c in out.columns]
    if sort_cols:
        out = out.sort_values(by=sort_cols, ascending=[False] * len(sort_cols), na_position='last')
    code_col = 'code' if 'code' in out.columns else ('Code' if 'Code' in out.columns else ('종목코드' if '종목코드' in out.columns else None))
    if code_col:
        out = out.drop_duplicates(subset=[code_col], keep='first')
    return out.head(limit).reset_index(drop=True)


def _pre_energy_line(row: Any, scored: Dict[str, Any]) -> str:
    bb20 = scored.get('bb20', 0)
    bb40 = scored.get('bb40', 0)
    ma_u = scored.get('ma_ultra', 0)
    ma_s = scored.get('ma_short', 0)
    ma_g = scored.get('ma_struct', 0)
    vr = scored.get('vol_ratio', 0)
    parts = []
    if bb20 > 0:
        parts.append(f"BB20 {bb20:.1f}")
    if bb40 > 0:
        parts.append(f"BB40 {bb40:.1f}")
    ma_parts = []
    if ma_u > 0:
        ma_parts.append(f"초{ma_u:.1f}")
    if ma_s > 0:
        ma_parts.append(f"단{ma_s:.1f}")
    if ma_g > 0:
        ma_parts.append(f"구{ma_g:.1f}")
    if ma_parts:
        parts.append('MA수렴 ' + '/'.join(ma_parts))
    if vr > 0:
        parts.append(f"거래량비 {vr:.2f}")
    return "   🔋 응축: " + (' | '.join(parts) if parts else 'BB폭·MA수렴·거래량 감소 추가 확인')


def build_triangle_pre_squeeze_top3_block(df: pd.DataFrame, limit: int = 3, title: str = '예비 삼각수렴·BB응축 TOP 3') -> str:
    top = build_triangle_pre_squeeze_top3_df(df, limit=limit)
    if top is None or top.empty:
        return f"🧩 [{title}]\n\n- 해당 종목 없음"

    lines: List[str] = []
    lines.append(f"🧩 [{title}]")
    lines.append('')
    lines.append('기준: 아직 돌파 전인 삼각수렴 후보 중 BB20/BB40 폭 축소, 이평 수렴, 거래량 응축, 수급 우세를 함께 봅니다.')
    lines.append('')
    for i, row in top.iterrows():
        scored = score_triangle_pre_squeeze_from_row(row)
        name = scored['name']
        code = scored['code']
        cp = f"({code})" if code else ''
        signal = _txt(_row_get(row, 'N조합', '신호', '검색패턴', default='삼각수렴/BB응축'), '삼각수렴/BB응축')
        base = _safe_int(_row_get(row, '파란점선기준가', '기준가', 'blue_line_price', default=0), 0)
        target = _safe_int(_row_get(row, '🎯목표타점', '목표1', default=0), 0)
        stop = _safe_int(_row_get(row, '🚨손절가', '손절', default=0), 0)
        ai = _txt(_row_get(row, 'ai_tip', 'AI코멘트', default=''), '')
        if not ai:
            ai = '수렴과 응축은 예비 신호입니다. 돌파 전 매수보다 5일선·파란점선 위 재지지와 거래량 재유입을 확인합니다.'
        lines.append(f"{i + 1}) {name}{cp} | 현재 {_fmt_price(scored['current'])} | 점수 {scored['score']} | {scored['readiness']}")
        lines.append(f"   🏷️ {signal}")
        lines.append(_pre_energy_line(row, scored))
        lines.append(_wave_line_from_row(row))
        lines.append(_fib_line_from_row(row))
        if base > 0:
            lines.append(f"   🔵 기준선: {_fmt_price(base)}원 위 종가 안착 또는 재지지 확인")
        lines.append(f"   🧭 해설: {scored['comment']}")
        lines.append(f"   🤖 AI코멘트: {ai[:120]}")
        if target > 0 or stop > 0:
            lines.append(f"   🎯 가격: 목표 {_fmt_price(target)} / 손절 {_fmt_price(stop)}")
        for sline in _format_supply_day_lines(row):
            lines.append(sline)
        lines.append('')
    return '\n'.join(lines).rstrip()

