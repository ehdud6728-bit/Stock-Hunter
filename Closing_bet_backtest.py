# =============================================================
# Closing_bet_backtest_A_B1_B2.py
# =============================================================
# 종가배팅 백테스트 (A / B1 / B2)
#
# A  : 돌파형 종가배팅
# B1 : ENV 엄격형 바닥 반등
#      - Env20 하단 2% 이내
#      - Env40 하단 10% 이내
#      - 동시 만족(AND)
# B2 : BB 확장형 하단 재안착
#      - BB40 하단 근접
#
# 방법
# - 과거 날짜의 일봉 데이터에 조건을 적용
# - 다음 거래일 시가 진입
# - 1/3/5/10일 후 수익률 및 MFE/MAE 측정
#
# 실행 예시
# python Closing_bet_backtest_A_B1_B2.py --start 2024-01-01 --end 2024-12-31
# python Closing_bet_backtest_A_B1_B2.py --start 2024-01-01 --end 2024-12-31 --top 200
# python Closing_bet_backtest_A_B1_B2.py --start 2024-01-01 --end 2024-12-31 --universe kospi200+kosdaq150
# =============================================================

import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import FinanceDataReader as fdr

try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

try:
    from scan_logger import set_log_level, log_info, log_error, log_debug
    set_log_level('NORMAL')
except ImportError:
    def log_info(m):
        print(m)

    def log_error(m):
        print(m)

    def log_debug(m):
        pass

# -------------------------------------------------------------
# scanner / indicator import
# -------------------------------------------------------------
try:
    from main7_bugfix_2 import get_indicators
except ImportError:
    from main7_bugfix import get_indicators

# 우선순위:
# 1) repo 안에 최종 통합 scanner 가 있으면 그걸 사용
# 2) 기존 Closing_bet_scanner 가 있으면 거기서 일부 사용
try:
    from Closing_bet_scanner_v2 import (
        _calc_envelope,
        _check_envelope_bottom,
        _calc_bollinger,
        _check_bb_bottom,
        _calc_upper_wick_ratio,
        _get_kospi200,
        _get_kosdaq150,
        MIN_PRICE,
        MIN_AMOUNT,
        NEAR_HIGH20_MIN,
        NEAR_HIGH20_MAX,
        UPPER_WICK_MAX,
        VOL_MULT,
        DISPARITY_MIN,
        DISPARITY_MAX,
    )
except ImportError:
    from Closing_bet_scanner import (  # type: ignore
        _calc_envelope,
        _check_envelope_bottom,
        _calc_upper_wick_ratio,
        MIN_PRICE,
        MIN_AMOUNT,
        NEAR_HIGH20_MIN,
        NEAR_HIGH20_MAX,
        UPPER_WICK_MAX,
        VOL_MULT,
        DISPARITY_MIN,
        DISPARITY_MAX,
    )

    # 기존 스캐너에 BB 함수가 없을 수 있으므로 로컬 구현
    def _calc_bollinger(df: pd.DataFrame, period: int = 40, std_mult: float = 2.0) -> dict:
        mid = df['Close'].rolling(period).mean()
        std = df['Close'].rolling(period).std()
        upper = mid + std * std_mult
        lower = mid - std * std_mult
        width = pd.Series(
            np.where(mid > 0, (upper - lower) / mid * 100, np.nan),
            index=df.index,
        )
        return {'mid': mid, 'upper': upper, 'lower': lower, 'width': width}

    def _check_bb_bottom(row: pd.Series, df: pd.DataFrame) -> dict:
        close = float(row.get('Close', 0))
        if close <= 0:
            return {
                'bb40_near': False,
                'bb40_pct': 0.0,
                'bb40_width': 0.0,
                'lower40': 0,
                'mid40': 0,
            }
        bb40 = _calc_bollinger(df, 40, 2.0)
        lower40 = float(bb40['lower'].iloc[-1]) if not pd.isna(bb40['lower'].iloc[-1]) else 0.0
        mid40 = float(bb40['mid'].iloc[-1]) if not pd.isna(bb40['mid'].iloc[-1]) else 0.0
        width40 = float(bb40['width'].iloc[-1]) if not pd.isna(bb40['width'].iloc[-1]) else 0.0
        if lower40 <= 0:
            return {
                'bb40_near': False,
                'bb40_pct': 999.0,
                'bb40_width': round(width40, 1),
                'lower40': 0,
                'mid40': round(mid40) if mid40 > 0 else 0,
            }
        bb40_pct = (close - lower40) / lower40 * 100
        return {
            'bb40_near': -2.5 <= bb40_pct <= 2.5,
            'bb40_pct': round(bb40_pct, 1),
            'bb40_width': round(width40, 1),
            'lower40': round(lower40),
            'mid40': round(mid40) if mid40 > 0 else 0,
        }

    def _get_kospi200() -> list:
        return []

    def _get_kosdaq150() -> list:
        return []


# =============================================================
# 설정
# =============================================================
HOLD_DAYS_LIST = [1, 3, 5, 10]
PROFIT_TARGET = 3.0
STOP_LOSS = -3.0
MAX_WORKERS = 15
TOP_N = 300

JSON_KEY_PATH = 'stock-key.json'
SHEET_NAME = '종가배팅'
SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]

DEFAULT_UNIVERSE = 'top_marketcap'
INDEX_MAP: dict[str, str] = {}


# =============================================================
# 유니버스
# =============================================================
def _get_ticker_list(top_n: int, universe: str = DEFAULT_UNIVERSE) -> list:
    """
    universe:
      - top_marketcap
      - kospi200+kosdaq150
      - kospi200
      - kosdaq150
    """
    global INDEX_MAP
    INDEX_MAP = {}

    if universe in ('kospi200+kosdaq150', 'kospi200', 'kosdaq150'):
        codes: list[str] = []
        if universe in ('kospi200+kosdaq150', 'kospi200'):
            kospi = _get_kospi200()
            for c in kospi:
                INDEX_MAP[c] = '코스피200'
            codes.extend(kospi)
        if universe in ('kospi200+kosdaq150', 'kosdaq150'):
            kosdaq = _get_kosdaq150()
            for c in kosdaq:
                if c not in INDEX_MAP:
                    INDEX_MAP[c] = '코스닥150'
            codes.extend(kosdaq)
        codes = list(dict.fromkeys(codes))
        if codes:
            log_info(f"지수 유니버스: {len(codes)}개")
            return codes
        log_info("지수 유니버스 로드 실패 → 시총 상위 폴백")

    log_info("종목 리스트 수집...")
    try:
        df_k = fdr.StockListing('KOSPI')
        df_q = fdr.StockListing('KOSDAQ')
        df = pd.concat([df_k, df_q], ignore_index=True)
        if df is not None and not df.empty:
            mcap_col = next((c for c in df.columns if 'cap' in c.lower()), None)
            sym_col = next((c for c in df.columns if c in ('Code', 'Symbol', '종목코드')), None)
            if mcap_col and sym_col:
                df = df.nlargest(top_n, mcap_col)
                codes = [str(c).zfill(6) for c in df[sym_col].tolist()]
                log_info(f"FDR 시총상위: {len(codes)}개")
                return codes
    except Exception as e:
        log_error(f"FDR 실패: {e}")

    try:
        from pykrx import stock as _pk
        codes = _pk.get_market_ticker_list(market='ALL')
        log_info(f"pykrx: {len(codes[:top_n])}개")
        return list(codes[:top_n])
    except Exception as e:
        log_error(f"pykrx 실패: {e}")

    return []


# =============================================================
# 판정 유틸
# =============================================================
def _safe_float(v, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _compute_common_state(sub_df: pd.DataFrame) -> dict | None:
    if sub_df is None or len(sub_df) < 60:
        return None

    row = sub_df.iloc[-1]
    close = _safe_float(row['Close'])
    open_p = _safe_float(row['Open'])
    high = _safe_float(row['High'])
    low = _safe_float(row['Low'])
    vol = _safe_float(row['Volume'])

    if close < MIN_PRICE:
        return None

    amount = close * vol
    if amount < MIN_AMOUNT:
        return None

    vma20 = _safe_float(row.get('VMA20', sub_df['Volume'].rolling(20).mean().iloc[-1]))
    ma20 = _safe_float(row.get('MA20', sub_df['Close'].rolling(20).mean().iloc[-1]))
    disp = (close / ma20 * 100) if ma20 > 0 else 100.0
    high20 = _safe_float(sub_df['High'].rolling(20).max().iloc[-1])
    near20 = (close / high20 * 100) if high20 > 0 else 0.0
    rsi = _safe_float(row.get('RSI', 50), 50.0)
    upper_wick = _safe_float(_calc_upper_wick_ratio(row), 0.0)

    total = max(high - low, 1e-9)
    body_top = max(open_p, close)
    body_bot = min(open_p, close)
    body_size = max(abs(close - open_p), 1e-9)
    upper_wick_len = max(0.0, high - body_top)
    lower_wick_len = max(0.0, body_bot - low)
    close_to_high = (close / high * 100) if high > 0 else 0.0

    env = _check_envelope_bottom(row, sub_df)
    bb = _check_bb_bottom(row, sub_df)

    obv = (
        sub_df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        * sub_df['Volume']
    ).cumsum()
    obv_ma5 = obv.rolling(5).mean()
    obv_ma10 = obv.rolling(10).mean()
    obv_rising = _safe_float(obv_ma5.iloc[-1]) > _safe_float(obv_ma10.iloc[-1])

    recent5 = sub_df.tail(5)
    vma10_val = _safe_float(sub_df['Volume'].rolling(10).mean().iloc[-1])
    maejip_5d = int(((recent5['Volume'] > vma10_val) & (recent5['Close'] > recent5['Open'])).sum()) if vma10_val > 0 else 0

    atr = _safe_float(row.get('ATR', 0), 0.0)
    atr_pct = (atr / close * 100) if close > 0 else 0.0

    return {
        'row': row,
        'close': close,
        'open': open_p,
        'high': high,
        'low': low,
        'vol': vol,
        'amount': amount,
        'amount_b': round(amount / 1e8, 1),
        'vma20': vma20,
        'ma20': ma20,
        'disp': disp,
        'near20': near20,
        'rsi': rsi,
        'upper_wick': upper_wick,
        'upper_wick_pct': round(upper_wick * 100, 1),
        'upper_wick_body_pct': round(upper_wick_len / body_size * 100, 1),
        'lower_wick_body_pct': round(lower_wick_len / body_size * 100, 1),
        'close_to_high': close_to_high,
        'env': env,
        'bb': bb,
        'obv_rising': obv_rising,
        'maejip_5d': maejip_5d,
        'atr_pct': round(atr_pct, 1),
        'vol_ratio': round(vol / vma20, 1) if vma20 > 0 else 0.0,
        'index_label': '',
    }


def _pick_index_label(code: str) -> str:
    return INDEX_MAP.get(str(code).zfill(6), '')


# =============================================================
# 단일 날짜/종목 종가배팅 조건 체크
# =============================================================
def _check_conditions_on_date(df: pd.DataFrame, date_idx: int, code: str = '') -> dict | None:
    """특정 날짜(date_idx)의 일봉 데이터로 A/B1/B2 체크"""
    if date_idx < 60:
        return None

    sub_df = df.iloc[:date_idx + 1].copy()
    state = _compute_common_state(sub_df)
    if state is None:
        return None

    state['index_label'] = _pick_index_label(code)
    close = state['close']
    open_p = state['open']
    rsi = state['rsi']
    env = state['env']
    bb = state['bb']

    # A: 돌파형
    a_cond = {
        '①전고점85~100%': NEAR_HIGH20_MIN <= state['near20'] <= NEAR_HIGH20_MAX,
        '②윗꼬리20%이하': state['upper_wick'] <= UPPER_WICK_MAX,
        '③거래량2배폭발': state['vma20'] > 0 and state['vol'] >= state['vma20'] * VOL_MULT,
        '④양봉마감': close >= open_p,
        '⑤이격도98~112': DISPARITY_MIN <= state['disp'] <= DISPARITY_MAX,
        '⑥MA20위마감': state['ma20'] > 0 and close >= state['ma20'],
    }
    a_passed = [k for k, v in a_cond.items() if v]
    a_score = len(a_passed)
    a_hit = a_score >= 4

    # B1: ENV 엄격형
    b1_cond = {
        '①Env20하단2%': env['env20_near'],
        '②Env40하단10%': env['env40_near'],
        '③RSI40이하': rsi <= 40,
        '④OBV매수세유입': state['obv_rising'],
        '⑤5일내매집봉1회↑': state['maejip_5d'] >= 1,
        '⑥종가강도양호': (close >= open_p) or (state['close_to_high'] >= 95),
        '⑦윗꼬리25%이하': state['upper_wick'] <= 0.25,
    }
    b1_passed = [k for k, v in b1_cond.items() if v]
    b1_score = len(b1_passed)
    b1_hit = env['env20_near'] and env['env40_near'] and b1_score >= 4

    # B2: BB 확장형
    b2_cond = {
        '①BB40하단근접': bb['bb40_near'],
        '②RSI45이하': rsi <= 45,
        '③OBV매수세유입': state['obv_rising'],
        '④5일내매집봉1회↑': state['maejip_5d'] >= 1,
        '⑤종가강도양호': (close >= open_p) or (state['close_to_high'] >= 95),
        '⑥윗꼬리25%이하': state['upper_wick'] <= 0.25,
        '⑦BB폭확대/변동성': (bb['bb40_width'] >= 14) or (state['atr_pct'] >= 3.0),
    }
    b2_passed = [k for k, v in b2_cond.items() if v]
    b2_score = len(b2_passed)
    b2_hit = bb['bb40_near'] and b2_score >= 4

    candidates = []
    if a_hit:
        candidates.append({
            'mode': 'A',
            'mode_label': '돌파형(A)',
            'score': a_score,
            'passed': a_passed,
            'band_type': '',
            'band_pct_text': '',
        })
    if b1_hit:
        candidates.append({
            'mode': 'B1',
            'mode_label': 'ENV엄격형(B1)',
            'score': b1_score,
            'passed': b1_passed,
            'band_type': 'ENV',
            'band_pct_text': f"Env20:{env['env20_pct']:+.1f}% | Env40:{env['env40_pct']:+.1f}%",
        })
    if b2_hit:
        candidates.append({
            'mode': 'B2',
            'mode_label': 'BB확장형(B2)',
            'score': b2_score,
            'passed': b2_passed,
            'band_type': 'BB',
            'band_pct_text': f"BB40:{bb['bb40_pct']:+.1f}% | BB폭:{bb['bb40_width']:.1f}%",
        })

    if not candidates:
        return None

    # 점수 우선, 동점이면 A > B1 > B2
    rank_order = {'A': 0, 'B1': 1, 'B2': 2}
    candidates.sort(key=lambda x: (-x['score'], rank_order.get(x['mode'], 9)))
    pick = candidates[0]

    return {
        'mode': pick['mode'],
        'mode_label': pick['mode_label'],
        'score': pick['score'],
        'passed': pick['passed'],
        'band_type': pick['band_type'],
        'band_pct_text': pick['band_pct_text'],
        'index_label': state['index_label'],
        'a_score': a_score,
        'b1_score': b1_score,
        'b2_score': b2_score,
        'close': close,
        'near20': round(state['near20'], 1),
        'disp': round(state['disp'], 1),
        'vol_ratio': state['vol_ratio'],
        'upper_wick_pct': state['upper_wick_pct'],
        'rsi': round(rsi, 1),
        'env20_pct': env.get('env20_pct', 0),
        'env40_pct': env.get('env40_pct', 0),
        'bb40_pct': bb.get('bb40_pct', 0),
        'bb40_width': bb.get('bb40_width', 0),
        'amount_b': state['amount_b'],
        'atr_pct': state['atr_pct'],
        'maejip_5d': state['maejip_5d'],
        'obv_rising': state['obv_rising'],
    }


# =============================================================
# 단일 종목 백테스트
# =============================================================
def backtest_ticker(code: str, start: str, end: str) -> list:
    """종목 코드의 기간 내 종가배팅 신호 발생일 + 결과 계산"""
    records = []
    try:
        load_start = (datetime.strptime(start, '%Y-%m-%d') - timedelta(days=120)).strftime('%Y-%m-%d')
        df_raw = fdr.DataReader(code, start=load_start, end=end)
        if df_raw is None or len(df_raw) < 80:
            return []

        df = get_indicators(df_raw.copy())
        if df is None or df.empty:
            return []

        df = df.reset_index()
        date_col = 'Date' if 'Date' in df.columns else df.columns[0]

        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end_dt = datetime.strptime(end, '%Y-%m-%d')

        for i in range(60, len(df) - max(HOLD_DAYS_LIST) - 1):
            row_date = pd.to_datetime(df[date_col].iloc[i])
            row_dt = row_date.to_pydatetime().replace(tzinfo=None)
            if not (start_dt <= row_dt <= end_dt):
                continue

            cond = _check_conditions_on_date(df, i, code=code)
            if cond is None:
                continue

            entry_idx = i + 1
            entry_price = _safe_float(df['Open'].iloc[entry_idx])
            if entry_price <= 0:
                continue

            forward_returns = {}
            stop_day = None
            max_high_pct = 0.0
            max_low_pct = 0.0

            for hold in HOLD_DAYS_LIST:
                future_idx = entry_idx + hold
                if future_idx >= len(df):
                    forward_returns[f'수익률_{hold}일'] = None
                    forward_returns[f'승패_{hold}일'] = 'N/A'
                    continue

                future_close = _safe_float(df['Close'].iloc[future_idx])
                ret = (future_close - entry_price) / entry_price * 100
                forward_returns[f'수익률_{hold}일'] = round(ret, 2)
                forward_returns[f'승패_{hold}일'] = (
                    '승' if ret >= PROFIT_TARGET else ('손절' if ret <= STOP_LOSS else '보합')
                )

            future_range = df.iloc[entry_idx:entry_idx + 11]
            if not future_range.empty:
                max_high = _safe_float(future_range['High'].max())
                min_low = _safe_float(future_range['Low'].min())
                max_high_pct = round((max_high - entry_price) / entry_price * 100, 2)
                max_low_pct = round((min_low - entry_price) / entry_price * 100, 2)

            for d in range(1, min(11, len(df) - entry_idx)):
                daily_low = _safe_float(df['Low'].iloc[entry_idx + d])
                if (daily_low - entry_price) / entry_price * 100 <= STOP_LOSS:
                    stop_day = d
                    break

            records.append({
                '스캔일': row_dt.strftime('%Y-%m-%d'),
                'code': str(code).zfill(6),
                '전략': cond['mode'],
                '전략명': cond['mode_label'],
                '지수구분': cond['index_label'],
                '밴드구분': cond['band_type'],
                '밴드상태': cond['band_pct_text'],
                '충족조건': ' '.join(cond['passed']),
                '총점수': cond['score'],
                'A점수': cond['a_score'],
                'B1점수': cond['b1_score'],
                'B2점수': cond['b2_score'],
                '종가': int(cond['close']),
                '진입가': int(entry_price),
                '거래량배율': cond['vol_ratio'],
                '전고점%': cond['near20'],
                '이격도': cond['disp'],
                '윗꼬리%': cond['upper_wick_pct'],
                'RSI': cond['rsi'],
                'Env20%': cond['env20_pct'],
                'Env40%': cond['env40_pct'],
                'BB40%': cond['bb40_pct'],
                'BB폭40%': cond['bb40_width'],
                'ATR%': cond['atr_pct'],
                '5일매집수': cond['maejip_5d'],
                'OBV상승': 'Y' if cond['obv_rising'] else 'N',
                '거래대금억': cond['amount_b'],
                '최고점%': max_high_pct,
                '최저점%': max_low_pct,
                '손절발동일': stop_day,
                **forward_returns,
            })

    except Exception as e:
        log_debug(f"[{code}] 오류: {e}")

    return records


# =============================================================
# 통계 집계
# =============================================================
def summarize(df: pd.DataFrame) -> dict:
    results = {}

    rows = []
    for strategy in ['A', 'B1', 'B2']:
        grp = df[df['전략'] == strategy]
        if grp.empty:
            continue

        n = len(grp)
        name_map = {
            'A': '돌파형(A)',
            'B1': 'ENV엄격형(B1)',
            'B2': 'BB확장형(B2)',
        }
        row = {
            '전략': name_map.get(strategy, strategy),
            '총건수': n,
            '평균총점': round(grp['총점수'].mean(), 1),
            '평균A점수': round(grp['A점수'].mean(), 1),
            '평균B1점수': round(grp['B1점수'].mean(), 1),
            '평균B2점수': round(grp['B2점수'].mean(), 1),
            'MFE평균%': round(grp['최고점%'].mean(), 2),
            'MAE평균%': round(grp['최저점%'].mean(), 2),
            '손절발동률%': round(grp['손절발동일'].notna().sum() / n * 100, 1),
        }
        for hold in HOLD_DAYS_LIST:
            key = f'승패_{hold}일'
            if key in grp.columns:
                valid = grp[grp[key] != 'N/A']
                if not valid.empty:
                    win_rate = (valid[key] == '승').mean() * 100
                    avg_ret = valid[f'수익률_{hold}일'].dropna().mean()
                    row[f'{hold}일_승률%'] = round(win_rate, 1)
                    row[f'{hold}일_평균수익%'] = round(avg_ret, 2)
        rows.append(row)
    results['전략별'] = pd.DataFrame(rows)

    df2 = df.copy()
    df2['년월'] = df2['스캔일'].str[:7]
    monthly = []
    for ym, grp in df2.groupby('년월'):
        n = len(grp)
        row = {
            '년월': ym,
            '총건수': n,
            'A건수': (grp['전략'] == 'A').sum(),
            'B1건수': (grp['전략'] == 'B1').sum(),
            'B2건수': (grp['전략'] == 'B2').sum(),
        }
        for hold in HOLD_DAYS_LIST:
            key = f'승패_{hold}일'
            if key in grp.columns:
                valid = grp[grp[key] != 'N/A']
                if not valid.empty:
                    row[f'{hold}일승률%'] = round((valid[key] == '승').mean() * 100, 1)
        monthly.append(row)
    results['월별'] = pd.DataFrame(monthly)

    band_rows = []
    if '밴드구분' in df.columns:
        for (strategy, band), grp in df.groupby(['전략', '밴드구분']):
            if grp.empty:
                continue
            band_rows.append({
                '전략': strategy,
                '밴드': band,
                '건수': len(grp),
                '1일평균수익%': round(grp['수익률_1일'].dropna().mean(), 2) if '수익률_1일' in grp.columns else None,
                '3일평균수익%': round(grp['수익률_3일'].dropna().mean(), 2) if '수익률_3일' in grp.columns else None,
                '5일평균수익%': round(grp['수익률_5일'].dropna().mean(), 2) if '수익률_5일' in grp.columns else None,
                '10일평균수익%': round(grp['수익률_10일'].dropna().mean(), 2) if '수익률_10일' in grp.columns else None,
            })
    results['밴드별'] = pd.DataFrame(band_rows)

    return results


# =============================================================
# 구글시트 저장
# =============================================================
def _get_gspread_client():
    if not HAS_GSPREAD:
        return None, None
    try:
        import json as _json
        if os.path.exists(JSON_KEY_PATH):
            creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_PATH, SCOPE)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            creds = ServiceAccountCredentials.from_json_keyfile_dict(
                _json.loads(os.environ['GOOGLE_JSON_KEY']), SCOPE
            )
        else:
            log_info("⚠️ 구글시트 인증 없음")
            return None, None
        gc = gspread.authorize(creds)
        doc = gc.open(SHEET_NAME)
        return gc, doc
    except Exception as e:
        log_error(f"구글시트 연결 실패: {e}")
        return None, None


def _upsert_tab(doc, tab_name: str, df: pd.DataFrame):
    try:
        try:
            ws = doc.worksheet(tab_name)
            ws.clear()
        except Exception:
            ws = doc.add_worksheet(title=tab_name, rows=max(1000, len(df) + 50), cols=max(20, df.shape[1] + 5))
        if df is None or df.empty:
            ws.update('A1', [[f'{tab_name}: 데이터 없음']])
            return
        data = [df.columns.tolist()] + df.fillna('').astype(str).values.tolist()
        chunk = 500
        for i in range(0, len(data), chunk):
            ws.append_rows(data[i:i + chunk], value_input_option='RAW')
            if i + chunk < len(data):
                time.sleep(1)
        log_info(f"✅ [{tab_name}] {len(df)}행 저장")
    except Exception as e:
        log_error(f"❌ [{tab_name}] 저장 실패: {e}")


def save_to_gsheet(raw_df: pd.DataFrame, summary: dict, start: str, end: str):
    gc, doc = _get_gspread_client()
    if doc is None:
        log_info("⚠️ 구글시트 저장 생략")
        return

    _upsert_tab(doc, 'CB_원본', raw_df)
    for tab_name, df in summary.items():
        _upsert_tab(doc, f'CB_{tab_name}', df)


# =============================================================
# 메인
# =============================================================
def main():
    parser = argparse.ArgumentParser(description='종가배팅 백테스트 (A/B1/B2)')
    parser.add_argument('--start', required=True, help='시작일 YYYY-MM-DD')
    parser.add_argument('--end', required=True, help='종료일 YYYY-MM-DD')
    parser.add_argument('--top', type=int, default=TOP_N, help='분석 종목 수')
    parser.add_argument(
        '--universe',
        default=DEFAULT_UNIVERSE,
        choices=['top_marketcap', 'kospi200+kosdaq150', 'kospi200', 'kosdaq150'],
        help='백테스트 유니버스',
    )
    parser.add_argument('--save-csv', action='store_true', help='원본/요약 CSV 저장')
    parser.add_argument('--gsheet', action='store_true', help='구글시트 저장')
    args = parser.parse_args()

    codes = _get_ticker_list(args.top, universe=args.universe)
    if not codes:
        log_error('분석할 종목이 없습니다.')
        sys.exit(1)

    log_info(f"백테스트 시작: {args.start} ~ {args.end} | {len(codes)}개")

    all_records = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(backtest_ticker, code, args.start, args.end): code for code in codes}
        done = 0
        for future in as_completed(futures):
            done += 1
            code = futures[future]
            try:
                rows = future.result()
                if rows:
                    all_records.extend(rows)
            except Exception as e:
                log_error(f"[{code}] 실패: {e}")

            if done % 20 == 0:
                log_info(f"진행: {done}/{len(codes)} | 레코드: {len(all_records)}")

    if not all_records:
        log_info('백테스트 결과 없음')
        sys.exit(0)

    raw_df = pd.DataFrame(all_records)
    raw_df = raw_df.sort_values(['스캔일', '전략', 'code']).reset_index(drop=True)
    summary = summarize(raw_df)

    log_info(f"총 레코드: {len(raw_df)}")
    for name, df in summary.items():
        log_info(f"[{name}] {len(df)}행")
        if not df.empty:
            print(df.head(20).to_string(index=False))
            print()

    stamp = f"{args.start}_{args.end}".replace('-', '')
    if args.save_csv:
        raw_path = f"Closing_bet_backtest_raw_{stamp}.csv"
        raw_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
        log_info(f"원본 CSV 저장: {raw_path}")
        for name, df in summary.items():
            out = f"Closing_bet_backtest_{name}_{stamp}.csv"
            df.to_csv(out, index=False, encoding='utf-8-sig')
            log_info(f"요약 CSV 저장: {out}")

    if args.gsheet:
        save_to_gsheet(raw_df, summary, args.start, args.end)


if __name__ == '__main__':
    main()
