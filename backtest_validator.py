# HTS 정확복제형 + main7 통일 엔진 반영 백테스트 최종본
# =============================================================
# 📊 backtest_validator_latest_complete.py
# main7_bugfix_2 기준 최신 통합 백테스트 교체본
# - live 로직과 최대한 동일하게 신호 주입
# - 급등초동(track B), 수급전환/수박발사형, 종가배팅, 피보/피봇 반영
# - Stage / combo / forward return / 패턴별 승률 집계
# =============================================================

import argparse
import os
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ─────────────────────────────────────────────────────────────
# 로거
# ─────────────────────────────────────────────────────────────
try:
    from scan_logger import set_log_level, log_error, log_info, log_debug, log_scan_date
    set_log_level('NORMAL')
except ImportError:
    def log_info(msg):  print(msg)
    def log_error(msg): print(msg)
    def log_debug(msg): pass
    def log_scan_date(date, n): print(f"🗓️  {date} → 히트: {n}개")

# ─────────────────────────────────────────────────────────────
# 최신 본진 모듈: HTS 정확복제 통합본 우선, 없으면 기존 V4/기본 main7 fallback
# ─────────────────────────────────────────────────────────────
try:
    import main7_bugfix_2 as _main7
except Exception:
    try:
        import main7_bugfix_2_engine_consistent_final as _main7
    except Exception:
        try:
            import main7_bugfix_2_v4_integrated_synced as _main7
        except Exception:
            try:
                import main7_bugfix_2_v4_integrated as _main7
            except Exception:
                import main7_bugfix_2 as _main7

get_indicators              = _main7.get_indicators
calculate_combination_score = _main7.calculate_combination_score
build_default_signals       = _main7.build_default_signals
inject_tri_result           = _main7.inject_tri_result
classify_style              = _main7.classify_style
STYLE_WEIGHTS               = _main7.STYLE_WEIGHTS
stage_rank_value            = _main7.stage_rank_value

judge_trade_with_sequence   = getattr(_main7, 'judge_trade_with_sequence', None)
calc_support_resistance     = getattr(_main7, 'calc_support_resistance', None)
load_krx_listing_safe       = getattr(_main7, 'load_krx_listing_safe', None)
evaluate_stage_sequence_v2  = getattr(_main7, 'evaluate_stage_sequence_v2',
                                      getattr(_main7, 'evaluate_stage_sequence', None))
detect_fear_absorption      = getattr(_main7, 'detect_fear_absorption', None)
apply_dante_v4              = getattr(_main7, 'apply_dante_v4', None)
build_v4_signal_map         = getattr(_main7, 'build_v4_signal_map', None)
apply_fear_and_quality_bonus = getattr(_main7, 'apply_fear_and_quality_bonus', None)

if apply_dante_v4 is None or build_v4_signal_map is None or apply_fear_and_quality_bonus is None:
    try:
        from dante_3phase_v4_module_hts_exact_integrated_final import apply_dante_v4 as _apply_dante_v4, build_v4_signal_map as _build_v4_signal_map, apply_fear_and_quality_bonus as _apply_fear_and_quality_bonus
        apply_dante_v4 = apply_dante_v4 or _apply_dante_v4
        build_v4_signal_map = build_v4_signal_map or _build_v4_signal_map
        apply_fear_and_quality_bonus = apply_fear_and_quality_bonus or _apply_fear_and_quality_bonus
    except Exception:
        try:
            from dante_3phase_v4_module_engine_consistent import apply_dante_v4 as _apply_dante_v4, build_v4_signal_map as _build_v4_signal_map, apply_fear_and_quality_bonus as _apply_fear_and_quality_bonus
            apply_dante_v4 = apply_dante_v4 or _apply_dante_v4
            build_v4_signal_map = build_v4_signal_map or _build_v4_signal_map
            apply_fear_and_quality_bonus = apply_fear_and_quality_bonus or _apply_fear_and_quality_bonus
        except Exception:
            try:
                from dante_3phase_v4_module import apply_dante_v4 as _apply_dante_v4, build_v4_signal_map as _build_v4_signal_map, apply_fear_and_quality_bonus as _apply_fear_and_quality_bonus
                apply_dante_v4 = apply_dante_v4 or _apply_dante_v4
                build_v4_signal_map = build_v4_signal_map or _build_v4_signal_map
                apply_fear_and_quality_bonus = apply_fear_and_quality_bonus or _apply_fear_and_quality_bonus
            except Exception:
                pass

# 선택 함수들
classify_momentum       = getattr(_main7, 'classify_momentum', None)
classify_volume_pattern = getattr(_main7, 'classify_volume_pattern', None)
classify_position       = getattr(_main7, 'classify_position', None)
classify_rsi_state      = getattr(_main7, 'classify_rsi_state', None)
classify_bb_state       = getattr(_main7, 'classify_bb_state', None)
classify_candle         = getattr(_main7, 'classify_candle', None)
classify_obv_trend      = getattr(_main7, 'classify_obv_trend', None)
classify_pattern_type   = getattr(_main7, 'classify_pattern_type', None)

# 외부 엔진
try:
    from triangle_combo_analyzer import jongbe_triangle_combo_v3
except Exception:
    jongbe_triangle_combo_v3 = None

# HTS 정확복제형 예비돌반지
try:
    from pre_dolbanji_hts_exact_clone import build_pre_dolbanji_hts_exact_bundle
except Exception:
    def build_pre_dolbanji_hts_exact_bundle(df, fund=None, require_fundamentals=False):
        return {
            "pre_dolbanji_hts_exact": False,
            "pre_dolbanji_hts_exact_score": 0,
            "pre_dolbanji_hts_exact_tags": [],
            "pre_dolbanji_hts_exact_detail": {},
        }

# =============================================================
# 설정
# =============================================================
HOLD_DAYS_LIST        = [3, 5, 10, 15, 20]
MIN_SCORE             = 150
MIN_AMOUNT_MAIN       = 50     # 일반형 최소 최근 평균 거래대금(억)
MIN_AMOUNT_TRACK_B    = 5      # 급등초동형 최소 최근 평균 거래대금(억)
MAX_WORKERS           = 12
PROFIT_TARGET         = 5.0
STOP_LOSS             = -5.0
PATTERN_COL           = 'N조합'
MIN_MARCAP_DEFAULT    = 1_000_000_000

# =============================================================
# 유틸
# =============================================================
def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def safe_int(v, default=0):
    try:
        return int(float(v))
    except Exception:
        return default

def to_bool(v):
    try:
        return bool(v)
    except Exception:
        return False

def get_trading_dates(start_str: str, end_str: str, freq: str = 'weekly') -> list:
    """
    거래일 목록 생성.
    우선은 평일 기반으로 생성하고, 휴일 오차는 실데이터 유무로 자연 제거.
    """
    s = datetime.strptime(start_str, '%Y-%m-%d')
    e = datetime.strptime(end_str, '%Y-%m-%d')
    all_days = pd.bdate_range(s, e).strftime('%Y-%m-%d').tolist()

    if freq == 'weekly':
        weeks = {}
        for d in all_days:
            dt = datetime.strptime(d, '%Y-%m-%d')
            key = dt.strftime('%Y-W%U')
            if key not in weeks:
                weeks[key] = d
        return sorted(weeks.values())
    return all_days

def _get_tickers_dataframe(min_marcap: int = MIN_MARCAP_DEFAULT) -> pd.DataFrame:
    if load_krx_listing_safe is not None:
        try:
            df = load_krx_listing_safe()
        except Exception as e:
            log_error(f"⚠️ load_krx_listing_safe 실패: {e}")
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()

    if df is None or df.empty:
        try:
            codes = stock.get_market_ticker_list(market='KOSPI') + stock.get_market_ticker_list(market='KOSDAQ')
            df = pd.DataFrame({'Code': codes})
            df['Name'] = df['Code'].apply(lambda c: stock.get_market_ticker_name(c))
            df['Market'] = 'KOSPI'
        except Exception as e:
            log_error(f"⚠️ pykrx fallback 실패: {e}")
            return pd.DataFrame(columns=['Code', 'Name', 'Market'])

    if 'Code' not in df.columns:
        if '티커' in df.columns:
            df = df.rename(columns={'티커': 'Code'})
        elif 'Ticker' in df.columns:
            df = df.rename(columns={'Ticker': 'Code'})

    if 'Name' not in df.columns:
        if '종목명' in df.columns:
            df = df.rename(columns={'종목명': 'Name'})

    if 'Market' not in df.columns:
        df['Market'] = ''

    df['Code'] = df['Code'].fillna('').astype(str).str.replace('.0', '', regex=False).str.zfill(6)
    df['Name'] = df['Name'].fillna('').astype(str)

    if 'Market' in df.columns:
        df = df[df['Market'].isin(['KOSPI', 'KOSDAQ', '코스닥', '유가', ''])]

    df = df[~df['Name'].str.contains('ETF|ETN|스팩|제[0-9]+호|우$|우A|우B|우C', regex=True, na=False)]

    if 'Marcap' in df.columns:
        df = df[df['Marcap'] >= min_marcap]
    elif 'MarCap' in df.columns:
        df = df[df['MarCap'] >= min_marcap]

    if 'Amount' in df.columns:
        df = df.sort_values('Amount', ascending=False)
    return df.reset_index(drop=True)

def load_krx_tickers(top_n: int = 500, min_marcap: int = MIN_MARCAP_DEFAULT) -> list:
    df = _get_tickers_dataframe(min_marcap=min_marcap)
    if df.empty:
        return []
    return list(zip(df['Code'].tolist()[:top_n], df['Name'].tolist()[:top_n]))

def build_live_like_signals(hist_df: pd.DataFrame, row: pd.Series, prev: pd.Series, ticker: str = ''):
    """
    live analyze_final() 에서 build_default_signals 이후 추가로 주입하는 신호를
    백테스트에서도 최대한 동일하게 반영.
    """
    close_p = float(row['Close'])
    signals = build_default_signals(row, close_p, prev)
    new_tags = []

    # style 주입 (style bonus 사용하는 조합 대응)
    try:
        signals['style'] = classify_style(row)
    except Exception:
        signals['style'] = 'NONE'

    # V4 3박자 신호 주입
    if build_v4_signal_map is not None:
        try:
            signals.update(build_v4_signal_map(row))
        except Exception:
            pass

    # 공포매물흡수 주입
    fear_info = {
        'fear_absorb': False,
        'personal_sell': 0,
        'foreign_buy': 0,
        'inst_buy': 0,
        'score': 0,
        'tag': '',
    }
    if detect_fear_absorption is not None and ticker:
        try:
            close_chg_pct = ((safe_float(row.get('Close', 0)) - safe_float(prev.get('Close', 0))) / safe_float(prev.get('Close', 1))) * 100 if safe_float(prev.get('Close', 0)) > 0 else 0.0
            fear_info = detect_fear_absorption(ticker, safe_float(row.get('Close', 0)), close_chg_pct) or fear_info
            signals['fear_absorb'] = to_bool(fear_info.get('fear_absorb', False))
        except Exception:
            pass

    # triangle / jongbe 주입
    tri_result = {}
    if jongbe_triangle_combo_v3 is not None:
        try:
            tri_result = jongbe_triangle_combo_v3(hist_df) or {}
        except Exception:
            tri_result = {}
    try:
        signals, new_tags = inject_tri_result(signals, tri_result, new_tags)
    except Exception:
        pass

    # 급등초동 surge_breakout
    _vma20_val = safe_float(row.get('VMA20', 0))
    _vol_ratio = safe_float(row.get('Volume', 0)) / _vma20_val if _vma20_val > 0 else 0
    _is_bigbull = safe_float(row.get('Close', 0)) > safe_float(row.get('Open', 0)) * 1.05
    _obv_up = to_bool(row.get('OBV_Rising', False))
    _near_ma20 = safe_float(row.get('Close', 0)) >= safe_float(row.get('MA20', 0)) * 0.97 if safe_float(row.get('MA20', 0)) > 0 else False
    _bb_touch = safe_float(row.get('Close', 0)) >= safe_float(row.get('BB40_Upper', row.get('Close', 0))) * 0.95 if safe_float(row.get('BB40_Upper', 0)) > 0 else False

    signals['surge_breakout'] = (
        _vol_ratio >= 3.0 and
        _is_bigbull and
        _obv_up and
        (_near_ma20 or _bb_touch)
    )

    # 피보 / 피봇 / ATR
    _sr = {'fib': {}, 'pivot': {}, 'atr_targets': {}}
    if calc_support_resistance is not None:
        try:
            _sr = calc_support_resistance(hist_df, row, close_p) or _sr
        except Exception:
            _sr = {'fib': {}, 'pivot': {}, 'atr_targets': {}}

    _fib = _sr.get('fib', {}) or {}
    _pivot = _sr.get('pivot', {}) or {}
    _tol = 0.02

    _fib382_val = safe_float(_fib.get('fib_382', 0))
    _fib618_val = safe_float(_fib.get('fib_618', 0))
    _pivot_s1   = safe_float(_pivot.get('S1', 0))
    _pivot_r1   = safe_float(_pivot.get('R1', 0))

    if _fib382_val > 0:
        signals['fib_support_382'] = abs(close_p - _fib382_val) / _fib382_val <= _tol
    if _fib618_val > 0:
        signals['fib_support_618'] = abs(close_p - _fib618_val) / _fib618_val <= _tol
    if _pivot_s1 > 0:
        signals['pivot_support'] = abs(close_p - _pivot_s1) / _pivot_s1 <= _tol
    if _pivot_r1 > 0:
        signals['pivot_resist'] = abs(close_p - _pivot_r1) / _pivot_r1 <= _tol

    # 종가배팅
    _high_p = safe_float(row.get('High', close_p))
    _low_p  = safe_float(row.get('Low', close_p))
    _open_p = safe_float(row.get('Open', close_p))
    _vol    = safe_float(row.get('Volume', 0))
    _vma20  = safe_float(row.get('VMA20', 0))
    _ma20   = safe_float(row.get('MA20', 0))
    _ma60   = safe_float(row.get('MA60', 0))
    _rsi_v  = safe_float(row.get('RSI', 50))
    _disp   = safe_float(row.get('Disparity', 100))

    _high20 = float(hist_df['High'].tail(20).max()) if len(hist_df) >= 20 else _high_p
    _high60 = float(hist_df['High'].tail(60).max()) if len(hist_df) >= 60 else _high20

    _total_range = _high_p - _low_p
    _upper_wick = max(0.0, _high_p - max(_open_p, close_p))
    _wick_ratio = _upper_wick / _total_range if _total_range > 0 else 1.0

    _near_high20 = (_high20 > 0) and (0.85 <= close_p / _high20 <= 1.02)
    _near_high60 = (_high60 > 0) and (0.80 <= close_p / _high60 <= 1.02)
    _no_upwick   = _wick_ratio <= 0.20
    _vol_x2      = (_vma20 > 0) and (_vol >= _vma20 * 2.0)
    _vol_x15     = (_vma20 > 0) and (_vol >= _vma20 * 1.5)
    _disp_ok     = _disp <= 108
    _ma_align    = (_ma20 > 0 and _ma60 > 0) and (_ma20 >= _ma60)
    _rsi_ok      = 40 <= _rsi_v <= 70
    _bull_close  = close_p >= _open_p

    _cb_a = (_near_high20 and _no_upwick and _vol_x2 and _disp_ok and _ma_align and _rsi_ok and _bull_close and signals.get('watermelon_signal', False))
    _cb_b = (_near_high20 and _no_upwick and _vol_x2 and _disp_ok and _obv_up)
    _cb_c = (_near_high60 and _no_upwick and _vol_x15 and _bull_close)

    if _cb_a:
        signals['closing_bet'] = True
        signals['closing_bet_grade'] = 'A'
    elif _cb_b:
        signals['closing_bet'] = True
        signals['closing_bet_grade'] = 'B'
    elif _cb_c:
        signals['closing_bet'] = True
        signals['closing_bet_grade'] = 'C'

    return signals, new_tags, _sr, fear_info

# =============================================================
# 단일 종목/단일 날짜 분석
# =============================================================
def analyze_on_date(ticker: str, name: str, scan_date: str) -> dict | None:
    try:
        scan_dt = datetime.strptime(scan_date, '%Y-%m-%d')
        start_dt = scan_dt - timedelta(days=400)
        end_dt = scan_dt + timedelta(days=60)

        full_df = fdr.DataReader(
            ticker,
            start=start_dt.strftime('%Y-%m-%d'),
            end=end_dt.strftime('%Y-%m-%d')
        )

        if full_df is None or full_df.empty or len(full_df) < 60:
            return None

        hist_df = full_df[full_df.index <= scan_date].copy()
        if hist_df.empty or len(hist_df) < 60:
            return None

        hist_df = get_indicators(hist_df)
        if hist_df is None or hist_df.empty or len(hist_df) < 20:
            return None

        if apply_dante_v4 is not None and 'DANTE_3PHASE_SCORE' not in hist_df.columns:
            try:
                hist_df = apply_dante_v4(hist_df)
            except Exception:
                pass

        row = hist_df.iloc[-1]
        prev = hist_df.iloc[-2]

        # 거래대금 필터: live와 동일하게 일반형 / track B 분리
        recent_avg_amount = (hist_df['Close'] * hist_df['Volume']).tail(5).mean() / 1e8
        is_track_b = bool(hist_df['_is_track_b'].iloc[-1]) if '_is_track_b' in hist_df.columns else False
        min_amount = MIN_AMOUNT_TRACK_B if is_track_b else MIN_AMOUNT_MAIN
        if recent_avg_amount < min_amount:
            return None

        # live와 동일한 신호 주입
        signals, new_tags, sr_data, fear_info = build_live_like_signals(hist_df, row, prev, ticker=ticker)

        # live와 동일하게 시퀀스 반영
        if judge_trade_with_sequence is not None:
            try:
                result = judge_trade_with_sequence(hist_df, signals)
            except Exception:
                result = calculate_combination_score(signals)
        else:
            result = calculate_combination_score(signals)

        if apply_fear_and_quality_bonus is not None:
            try:
                result['score'] = round(apply_fear_and_quality_bonus(safe_float(result.get('score', 0)), row), 1)
            except Exception:
                pass

        if safe_int(result.get('score', 0)) < MIN_SCORE:
            return None

        # Stage 판정
        stage_eval = {}
        if evaluate_stage_sequence_v2 is not None:
            try:
                stage_eval = evaluate_stage_sequence_v2(hist_df) or {}
            except Exception:
                stage_eval = {}
        entry_price = float(row['Close'])

        # 미래 수익률
        future_df = full_df[full_df.index > scan_date].head(max(HOLD_DAYS_LIST) + 5)
        forward_returns = {}
        max_high_pct = 0.0
        max_low_pct = 0.0
        stop_triggered_day = None

        if not future_df.empty and entry_price > 0:
            highs = future_df['High'].values
            lows = future_df['Low'].values
            closes = future_df['Close'].values
            dates = [d.strftime('%Y-%m-%d') for d in future_df.index]

            for hold in HOLD_DAYS_LIST:
                if hold <= len(closes):
                    exit_price = closes[hold - 1]
                    ret = (exit_price - entry_price) / entry_price * 100
                    forward_returns[f'수익률_{hold}일'] = round(ret, 2)
                else:
                    forward_returns[f'수익률_{hold}일'] = None

            n = min(max(HOLD_DAYS_LIST), len(highs))
            if n > 0:
                max_high_pct = round((highs[:n].max() - entry_price) / entry_price * 100, 2)
                max_low_pct = round((lows[:n].min() - entry_price) / entry_price * 100, 2)

            for i, (low_p, date_s) in enumerate(zip(lows, dates)):
                if i >= max(HOLD_DAYS_LIST):
                    break
                if (low_p - entry_price) / entry_price * 100 <= STOP_LOSS:
                    stop_triggered_day = i + 1
                    break

        # 보조 분류
        sub = {}
        try:
            if classify_momentum:       sub['추세강도'] = classify_momentum(row)
            if classify_volume_pattern: sub['거래량패턴'] = classify_volume_pattern(row)
            if classify_position:       sub['가격위치'] = classify_position(row)
            if classify_rsi_state:      sub['RSI상태'] = classify_rsi_state(safe_float(row.get('RSI', 50)))
            if classify_bb_state:       sub['BB상태'] = classify_bb_state(row)
            if classify_candle:         sub['캔들패턴'] = classify_candle(row)
            if classify_obv_trend:      sub['OBV추세'] = classify_obv_trend(row)
            if classify_pattern_type:   sub['단테패턴'] = classify_pattern_type(row)
        except Exception:
            sub = {}

        fib = sr_data.get('fib', {}) if isinstance(sr_data, dict) else {}
        pivot = sr_data.get('pivot', {}) if isinstance(sr_data, dict) else {}
        atr_targets = sr_data.get('atr_targets', {}) if isinstance(sr_data, dict) else {}

        # HTS 정확복제형 예비돌반지
        try:
            exact_bundle = build_pre_dolbanji_hts_exact_bundle(hist_df, fund=None, require_fundamentals=False) or {}
        except Exception:
            exact_bundle = {
                "pre_dolbanji_hts_exact": False,
                "pre_dolbanji_hts_exact_score": 0,
                "pre_dolbanji_hts_exact_tags": [],
                "pre_dolbanji_hts_exact_detail": {},
            }
        exact_detail = exact_bundle.get("pre_dolbanji_hts_exact_detail", {}) or {}

        record = {
            '스캔일':       scan_date,
            '종목명':       name,
            'code':         ticker,
            '진입가':       safe_int(entry_price),

            'N등급':        f"{result.get('type')}{result.get('grade')}",
            PATTERN_COL:    result.get('combination', ''),
            'N점수':        safe_int(result.get('score', 0)),
            'N구분':        " ".join(new_tags),
            '복합조합수':   safe_int(result.get('combo_count', 0)),

            '단계상태':     stage_eval.get('stage_status', 'DROP'),
            '단계랭크':     stage_rank_value(stage_eval.get('stage_status', 'DROP')),
            'S1날짜':       stage_eval.get('s1_date'),
            'S2날짜':       stage_eval.get('s2_date'),
            'S3날짜':       stage_eval.get('s3_date'),

            'RSI':          round(safe_float(row.get('RSI', 0)), 1),
            'BB40폭':       round(safe_float(row.get('BB40_Width', 0)), 1),
            'MA수렴':       round(safe_float(row.get('MA_Convergence', 0)), 1),
            'OBV기울기':    safe_int(row.get('OBV_Slope', 0)),
            '이격':         safe_int(row.get('Disparity', 0)),
            'ADX':          round(safe_float(row.get('ADX', 0)), 1),
            'MACD히스토':   round(safe_float(row.get('MACD_Hist', 0)), 2),
            'Stoch_K':      round(safe_float(row.get('Sto_K', 0)), 1),
            'BB_PercentB':  round(safe_float(row.get('BB40_PercentB', 0)), 2),
            'MFI':          round(safe_float(row.get('MFI', 0)), 1),
            'ATR':          round(safe_float(row.get('ATR', 0)), 1),

            # V4 3박자 컬럼
            '기간대칭점수':   round(safe_float(row.get('SYM_SCORE', row.get('sym_score_v4', 0))), 1),
            '기간대칭등급':   str(row.get('SYM_GRADE', '')),
            '파동에너지점수': round(safe_float(row.get('ENERGY_TOTAL', row.get('energy_total_v4', 0))), 1),
            '파동에너지등급': str(row.get('ENERGY_GRADE', '')),
            '3박자종합점수':  round(safe_float(row.get('DANTE_3PHASE_SCORE', row.get('dante_v4_score', 0))), 1),
            '3박자등급':      str(row.get('DANTE_3PHASE_GRADE', row.get('dante_v4_grade', ''))),
            '수박상태V4':     str(row.get('WM_STATE_NAME', row.get('watermelon_phase_v4', ''))),
            '3박자준비형':    to_bool(row.get('DANTE_FINAL_PREP', False)),
            '3박자발사형':    to_bool(row.get('DANTE_FINAL_FIRE', False)),
            '3박자유지형':    to_bool(row.get('DANTE_FINAL_HOLD', False)),
            '수박1차발사형V4': to_bool(row.get('Watermelon_First_Launch', False)),
            '수박재발사형V4': to_bool(row.get('Watermelon_Relaunch', False)),
            '공포매물흡수':   to_bool(fear_info.get('fear_absorb', False)) or to_bool(row.get('공포매물흡수', False)),
            '공포흡수점수':   safe_int(fear_info.get('score', row.get('공포흡수점수', 0))),
            '공포흡수태그':   str(fear_info.get('tag', row.get('공포흡수태그', ''))),

            # 기존 + 최신 컬럼 같이 저장
            '수급전환':      to_bool(row.get('Watermelon_Signal', False)),
            '수박준비형':    to_bool(row.get('Watermelon_Prepare', False)),
            '수박발사형':    to_bool(row.get('Watermelon_Signal_Refined', False)),
            '수박색':        str(row.get('Watermelon_Color', '')),
            '돌반지':        to_bool(row.get('Dolbanzi', False)),
            '독사훅':        to_bool(row.get('Real_Viper_Hook', False)),
            '골파기':        to_bool(row.get('Golpagi_Trap', False)),
            '종베':          to_bool(row.get('Jongbe_Break', False)),
            'BB40로스':      to_bool(row.get('BB40_Ross', False)),
            'RSI다이버':     to_bool(row.get('RSI_DIV', False)),
            'BB40RSI':       to_bool(row.get('BB40_Reclaim_RSI_DIV', False)),
            '세력눌림':      to_bool(row.get('Force_Pullback', False)),
            'OBV매집돌파':   to_bool(row.get('OBV_Acc_Breakout', False)),
            '좋은수렴':      to_bool(row.get('Good_MA_Convergence', False)),
            '폭발직전':      to_bool(row.get('MA_Convergence_Break_Ready', False)),
            '급등초동':      is_track_b,

            # HTS 정확복제형 예비돌반지
            '예비돌반지HTS정확복제': to_bool(exact_bundle.get('pre_dolbanji_hts_exact', False)),
            '예비돌반지HTS정확점수': safe_int(exact_bundle.get('pre_dolbanji_hts_exact_score', 0)),
            '예비돌반지HTS정확최대': safe_int(exact_detail.get('tech_max', 10)),
            '예비돌반지HTS정확태그': " ".join(exact_bundle.get('pre_dolbanji_hts_exact_tags', [])),

            '초단기MA수렴도':   round(safe_float(row.get('MAConv_5_10_20', 0)), 1),
            '단기MA수렴도':     round(safe_float(row.get('MAConv_5_20_60', 0)), 1),
            '구조MA수렴도':     round(safe_float(row.get('MAConv_20_60_112', 0)), 1),
            '브릿지MA수렴도':   round(safe_float(row.get('MAConv_5_20_112', 0)), 1),
            '구조접속MA수렴도': round(safe_float(row.get('MAConv_5_60_112', 0)), 1),

            '초단기MA수렴':   to_bool(row.get('Is_UltraShort_MA_Conv', False)),
            '단기MA수렴':     to_bool(row.get('Is_Short_MA_Conv', False)),
            '구조MA수렴':     to_bool(row.get('Is_Structure_MA_Conv', False)),
            '브릿지MA수렴':   to_bool(row.get('Is_Bridge_MA_Conv', False)),
            '구조접속MA수렴': to_bool(row.get('Is_Structure_Link_MA_Conv', False)),
            '초강력MA수렴':   to_bool(row.get('Is_Super_MA_Conv', False)),

            '매집품질':      str(row.get('Maejip_Quality', '')),
            '매집강도':      str(row.get('Maejip_Power_Grade', '')),
            '매집일수':      safe_int(row.get('Maejip_Days_10', 0)),
            '수박직전매집':  safe_int(row.get('Pre_Signal_Maejip', 0)),

            'PP':           pivot.get('PP', 0),
            'R1':           pivot.get('R1', 0),
            'S1':           pivot.get('S1', 0),
            'Fib382':       fib.get('fib_382', 0),
            'Fib618':       fib.get('fib_618', 0),
            'ATR목표1':     atr_targets.get('target_1', 0),
            'ATR목표2':     atr_targets.get('target_2', 0),
            'RR비율':       atr_targets.get('risk_reward', 0),

            **sub,

            '최고점%':      max_high_pct,
            '최저점%':      max_low_pct,
            '손절발동일':   stop_triggered_day,
            **forward_returns,
        }

        for hold in HOLD_DAYS_LIST:
            key = f'수익률_{hold}일'
            ret = record.get(key)
            if ret is not None:
                record[f'승패_{hold}일'] = '승' if ret >= PROFIT_TARGET else ('손절' if ret <= STOP_LOSS else '패')
            else:
                record[f'승패_{hold}일'] = 'N/A'

        return record

    except Exception as e:
        log_debug(f"analyze_on_date 실패 {ticker} {scan_date}: {e}")
        return None

# =============================================================
# 백테스트 실행
# =============================================================
def run_backtest(start_date: str, end_date: str,
                 freq: str = 'daily',
                 top_n: int = 500,
                 output_csv: str = 'backtest_result.csv') -> pd.DataFrame:
    log_info(f"\n{'='*60}")
    log_info(f"📊 백테스트 시작: {start_date} ~ {end_date}")
    log_info(f"   스캔 주기: {freq} | 보유일: {HOLD_DAYS_LIST} | 종목수: {top_n}")
    log_info(f"{'='*60}")

    scan_dates = get_trading_dates(start_date, end_date, freq=freq)
    log_info(f"📅 총 스캔일: {len(scan_dates)}개")

    tickers_list = load_krx_tickers(top_n=top_n)
    log_info(f"🔭 대상 종목: {len(tickers_list)}개")

    all_records = []

    for scan_date in scan_dates:
        date_records = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {
                executor.submit(analyze_on_date, code, name, scan_date): (code, name)
                for code, name in tickers_list
            }

            for future in as_completed(future_map, timeout=max(30, 20 * len(tickers_list))):
                try:
                    rec = future.result(timeout=20)
                    if rec:
                        date_records.append(rec)
                except Exception:
                    pass

        log_scan_date(scan_date, len(date_records))
        all_records.extend(date_records)

    if not all_records:
        log_error("⚠️ 결과 없음")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    log_info(f"✅ 원본 결과 저장: {output_csv} ({len(df)}건)")
    return df

# =============================================================
# 집계
# =============================================================
def analyze_pattern_winrate(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for pattern, grp in df.groupby(PATTERN_COL):
        n_total = len(grp)
        if n_total < 3:
            continue

        row_data = {
            '패턴':       pattern,
            '총건수':     n_total,
            '평균N점수':  round(grp['N점수'].mean(), 1),
            'MFE평균%':   round(grp['최고점%'].mean(), 2),
            'MAE평균%':   round(grp['최저점%'].mean(), 2),
            '손절발동률': round(grp['손절발동일'].notna().sum() / n_total * 100, 1),
            '평균RSI':    round(grp['RSI'].mean(), 1) if 'RSI' in grp.columns else 0,
            '평균BB40폭': round(grp['BB40폭'].mean(), 1) if 'BB40폭' in grp.columns else 0,
            '평균MA수렴': round(grp['MA수렴'].mean(), 1) if 'MA수렴' in grp.columns else 0,
            '평균이격':   round(grp['이격'].mean(), 1) if '이격' in grp.columns else 0,
            '평균ADX':    round(grp['ADX'].mean(), 1) if 'ADX' in grp.columns else 0,
            '수급전환비율%': round(grp['수급전환'].mean()*100, 1) if '수급전환' in grp.columns else 0,
            '수박발사형비율%': round(grp['수박발사형'].mean()*100, 1) if '수박발사형' in grp.columns else 0,
            '돌반지비율%': round(grp['돌반지'].mean()*100, 1) if '돌반지' in grp.columns else 0,
            '독사비율%':   round(grp['독사훅'].mean()*100, 1) if '독사훅' in grp.columns else 0,
            '골파기비율%': round(grp['골파기'].mean()*100, 1) if '골파기' in grp.columns else 0,
            '평균RR비율':  round(grp['RR비율'].mean(), 2) if 'RR비율' in grp.columns else 0,
        }

        for hold in HOLD_DAYS_LIST:
            ret_col = f'수익률_{hold}일'
            win_col = f'승패_{hold}일'
            valid = grp[grp[win_col] != 'N/A']
            if valid.empty:
                row_data[f'승률_{hold}일%'] = None
                row_data[f'평균수익_{hold}일%'] = None
                continue

            row_data[f'승률_{hold}일%'] = round((valid[win_col] == '승').sum() / len(valid) * 100, 1)
            row_data[f'평균수익_{hold}일%'] = round(valid[ret_col].mean(), 2)

        rows.append(row_data)

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary

    sort_col = '승률_10일%' if '승률_10일%' in summary.columns else '승률_5일%'
    return summary.sort_values(sort_col, ascending=False).reset_index(drop=True)

def analyze_stage_winrate(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for stage, grp in df.groupby('단계상태'):
        n_total = len(grp)
        if n_total < 2:
            continue
        row_data = {'단계': stage, '건수': n_total}
        for hold in HOLD_DAYS_LIST:
            win_col = f'승패_{hold}일'
            ret_col = f'수익률_{hold}일'
            valid = grp[grp[win_col] != 'N/A']
            if valid.empty:
                row_data[f'승률_{hold}일%'] = None
                row_data[f'평균수익_{hold}일%'] = None
                continue
            row_data[f'승률_{hold}일%'] = round((valid[win_col] == '승').sum() / len(valid) * 100, 1)
            row_data[f'평균수익_{hold}일%'] = round(valid[ret_col].mean(), 2)
        rows.append(row_data)
    return pd.DataFrame(rows)

def analyze_v4_signal_winrate(df: pd.DataFrame) -> pd.DataFrame:
    signal_cols = [
        ('3박자발사형', 'DANTE_FINAL_FIRE'),
        ('3박자준비형', 'DANTE_FINAL_PREP'),
        ('3박자유지형', 'DANTE_FINAL_HOLD'),
        ('수박1차발사형V4', 'Watermelon_First_Launch'),
        ('수박재발사형V4', 'Watermelon_Relaunch'),
        ('공포매물흡수', 'FearAbsorb'),
    ]

    rows = []
    for col_name, label in signal_cols:
        if col_name not in df.columns:
            continue
        grp = df[df[col_name] == True].copy()
        n_total = len(grp)
        if n_total < 2:
            continue

        row_data = {
            '신호': label,
            '컬럼': col_name,
            '건수': n_total,
            '평균N점수': round(grp['N점수'].mean(), 1) if 'N점수' in grp.columns else 0,
            '평균3박자점수': round(grp['3박자종합점수'].mean(), 1) if '3박자종합점수' in grp.columns else 0,
            'MFE평균%': round(grp['최고점%'].mean(), 2),
            'MAE평균%': round(grp['최저점%'].mean(), 2),
        }

        for hold in HOLD_DAYS_LIST:
            win_col = f'승패_{hold}일'
            ret_col = f'수익률_{hold}일'
            valid = grp[grp[win_col] != 'N/A']
            if valid.empty:
                row_data[f'승률_{hold}일%'] = None
                row_data[f'평균수익_{hold}일%'] = None
                continue
            row_data[f'승률_{hold}일%'] = round((valid[win_col] == '승').sum() / len(valid) * 100, 1)
            row_data[f'평균수익_{hold}일%'] = round(valid[ret_col].mean(), 2)

        rows.append(row_data)

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary
    sort_col = '승률_10일%' if '승률_10일%' in summary.columns else '승률_5일%'
    return summary.sort_values(sort_col, ascending=False).reset_index(drop=True)

def analyze_monthly_winrate(df: pd.DataFrame, hold_col: str = '수익률_10일') -> pd.DataFrame:
    if '스캔일' not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df['월'] = pd.to_datetime(df['스캔일']).dt.to_period('M').astype(str)

    hold_win_col = hold_col.replace('수익률_', '승패_')
    rows = []
    for month, grp in df.groupby('월'):
        valid = grp[grp[hold_win_col] != 'N/A']
        if valid.empty:
            continue
        rows.append({
            '월': month,
            '건수': len(grp),
            '승률%': round((valid[hold_win_col] == '승').sum() / len(valid) * 100, 1),
            '평균수익%': round(valid[hold_col].mean(), 2),
        })
    return pd.DataFrame(rows)

# =============================================================
# 리포트
# =============================================================
def print_report(df: pd.DataFrame):
    log_info("\n" + "="*60)
    log_info("📊 [백테스트 결과 요약]")
    log_info("="*60)
    log_info(f"총 신호 건수: {len(df)}")
    log_info(f"기간: {df['스캔일'].min()} ~ {df['스캔일'].max()}")
    log_info(f"평균 최고점(MFE): {df['최고점%'].mean():.2f}%")
    log_info(f"평균 최저점(MAE): {df['최저점%'].mean():.2f}%")
    if '예비돌반지HTS정확복제' in df.columns:
        hit_cnt = int(df['예비돌반지HTS정확복제'].fillna(False).sum())
        log_info(f"HTS정확복제 예비돌반지 발생 건수: {hit_cnt}")


    for hold in HOLD_DAYS_LIST:
        ret_col = f'수익률_{hold}일'
        win_col = f'승패_{hold}일'
        valid = df[df[win_col] != 'N/A']
        if valid.empty:
            continue
        wr = (valid[win_col] == '승').sum() / len(valid) * 100
        avg = valid[ret_col].mean()
        log_info(f"  {hold}일 보유: 승률 {wr:.1f}% | 평균수익 {avg:.2f}%")

    log_info("\n📌 [패턴별 Top10 승률 — 10일 기준]")
    summary = analyze_pattern_winrate(df)
    if not summary.empty:
        cols = ['패턴', '총건수', '승률_10일%', '평균수익_10일%', 'MFE평균%', 'MAE평균%']
        cols = [c for c in cols if c in summary.columns]
        log_info(summary[cols].head(10).to_string(index=False))

    log_info("\n📌 [Stage별 승률]")
    stage_summary = analyze_stage_winrate(df)
    if not stage_summary.empty:
        log_info(stage_summary.to_string(index=False))

# =============================================================
# CLI
# =============================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='backtest_validator_hts_exact_integrated_final')
    parser.add_argument('--start', default='2024-09-01', help='시작일 YYYY-MM-DD')
    parser.add_argument('--end', default='2025-01-31', help='종료일 YYYY-MM-DD')
    parser.add_argument('--freq', default='daily', help='weekly or daily')
    parser.add_argument('--top_n', default=500, type=int, help='스캔 종목 수')
    parser.add_argument('--workers', default=12, type=int, help='병렬 분석 스레드 수')
    parser.add_argument('--prefetch_workers', default=8, type=int, help='호환용 옵션(현재는 예약값으로만 사용)')
    parser.add_argument('--no_sheet', action='store_true', help='구글시트 업로드 생략')
    args = parser.parse_args()

    # 워크플로 호환: --workers 실제 반영, --prefetch_workers는 현재 호환용으로만 수용
    globals()['MAX_WORKERS'] = max(1, int(args.workers))

    print(f"[Backtest] workers={args.workers}, prefetch_workers={args.prefetch_workers}, top_n={args.top_n}, freq={args.freq}")

    result_df = run_backtest(
        start_date=args.start,
        end_date=args.end,
        freq=args.freq,
        top_n=args.top_n,
        output_csv='backtest_result.csv',
    )

    if result_df.empty:
        log_error("결과 없음 종료")
        raise SystemExit(0)

    pattern_df = analyze_pattern_winrate(result_df)
    stage_df = analyze_stage_winrate(result_df)
    monthly_df = analyze_monthly_winrate(result_df)
    v4_df = analyze_v4_signal_winrate(result_df)

    pattern_df.to_csv('backtest_pattern_winrate.csv', index=False, encoding='utf-8-sig')
    stage_df.to_csv('backtest_stage_winrate.csv', index=False, encoding='utf-8-sig')
    monthly_df.to_csv('backtest_monthly.csv', index=False, encoding='utf-8-sig')
    v4_df.to_csv('backtest_v4_signal_winrate.csv', index=False, encoding='utf-8-sig')

    print_report(result_df)

    if not args.no_sheet:
        try:
            from backtest_sheet_uploader import upload_backtest_to_sheet
            upload_backtest_to_sheet(
                result_df, pattern_df, stage_df, monthly_df,
                start=args.start, end=args.end
            )
        except Exception as e:
            log_error(f"⚠️ 시트 업로드 실패: {e}")
