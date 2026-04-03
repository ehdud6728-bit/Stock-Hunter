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
# - 신호 당일 종가 진입
# - 다음 거래일부터 1/3/5/10/15일 성과 측정
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
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import FinanceDataReader as fdr
import requests

try:
    from pykrx import stock as pk_stock
    HAS_PYKRX = True
except Exception:
    HAS_PYKRX = False
    pk_stock = None

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


# 검색기와 백테스트 종목 불일치를 막기 위해,
# 신호 판정에 필요한 함수/상수는 이 파일 안에 직접 고정한다.
# (외부 Closing_bet_scanner_v2 / Closing_bet_scanner import에 의존하지 않음)

MIN_PRICE = 5_000
MIN_AMOUNT = 3_000_000_000
NEAR_HIGH20_MIN = 85.0
NEAR_HIGH20_MAX = 100.0
UPPER_WICK_MAX = 0.20
VOL_MULT = 2.0
DISPARITY_MIN = 98.0
DISPARITY_MAX = 112.0

BB40_NEAR_PCT = 2.5


def _calc_upper_wick_ratio(row) -> float:
    high_p = float(row.get('High', 0))
    open_p = float(row.get('Open', 0))
    close_p = float(row.get('Close', 0))
    body_top = max(open_p, close_p)
    body_size = max(abs(close_p - open_p), 1e-9)
    upper_wick = max(0.0, high_p - body_top)
    return upper_wick / body_size


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
        'bb40_near': -BB40_NEAR_PCT <= bb40_pct <= BB40_NEAR_PCT,
        'bb40_pct': round(bb40_pct, 1),
        'bb40_width': round(width40, 1),
        'lower40': round(lower40),
        'mid40': round(mid40) if mid40 > 0 else 0,
    }


def _get_index_tickers_naver(index_code: str) -> list:
    try:
        from bs4 import BeautifulSoup

        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://finance.naver.com/',
        }
        url_map = {
            'KOSPI200': 'https://finance.naver.com/sise/entryJongmok.naver?kospiCode=KOSPI200',
            'KQ150': 'https://finance.naver.com/sise/entryJongmok.naver?kospiCode=KQ150',
        }
        url = url_map.get(index_code, '')
        if not url:
            return []

        tickers = []
        for page in range(1, 30):
            res = requests.get(f"{url}&page={page}", headers=headers, timeout=10)
            res.encoding = 'euc-kr'
            soup = BeautifulSoup(res.text, 'html.parser')
            links = soup.select('td.ctg a[href*="code="]')
            if not links:
                break
            for a in links:
                href = a.get('href', '')
                code = href.split('code=')[-1].strip()
                if code and len(code) == 6 and code.isdigit():
                    tickers.append(code)

        tickers = list(dict.fromkeys(tickers))
        if tickers:
            log_info(f"네이버 {index_code}: {len(tickers)}개 ✅")
        return tickers
    except Exception as e:
        log_error(f"⚠️ 네이버 {index_code} 실패: {e}")
        return []


def _get_index_tickers_krx(market: str, top_n: int) -> list:
    try:
        from pykrx import stock as _pk

        target_name = '코스피 200' if market == 'KOSPI' else '코스닥 150'
        idx_codes = _pk.get_index_ticker_list(market=market)

        target_code = None
        for idx_code in idx_codes:
            try:
                idx_name = _pk.get_index_ticker_name(idx_code)
                if idx_name == target_name:
                    target_code = idx_code
                    break
            except Exception:
                continue

        if target_code:
            tickers = _pk.get_index_portfolio_deposit_file(target_code)
            if tickers and len(tickers) > 50:
                log_info(f"pykrx {target_name}: {len(tickers)}개 ✅")
                return list(tickers)[:top_n]
    except Exception as e:
        log_error(f"⚠️ pykrx {market} 실패: {e}")

    try:
        df = fdr.StockListing(market)
        if df is not None and not df.empty:
            mcap_col = next((c for c in df.columns if 'cap' in c.lower()), None)
            sym_col = next((c for c in df.columns if c in ('Code', 'Symbol', '코드', '종목코드')), None)
            if mcap_col and sym_col:
                df = df.nlargest(top_n, mcap_col)
                tickers = [str(c).zfill(6) for c in df[sym_col].tolist()]
                log_info(f"FDR {market} 시총상위{top_n}: {len(tickers)}개 ✅")
                return tickers
    except Exception as e:
        log_error(f"⚠️ FDR {market} 실패: {e}")

    return []


def _get_kospi200() -> list:
    tickers = _get_index_tickers_naver('KOSPI200')
    if len(tickers) >= 150:
        return tickers
    log_info("코스피200 네이버 실패 → pykrx/FDR 폴백")
    return _get_index_tickers_krx('KOSPI', 200)


def _get_kosdaq150() -> list:
    tickers = _get_index_tickers_naver('KQ150')
    if len(tickers) >= 100:
        return tickers
    log_info("코스닥150 네이버 실패 → pykrx/FDR 폴백")
    return _get_index_tickers_krx('KOSDAQ', 150)


# HTS 설정 기준 고정: 엔벨로프(20,10), 엔벨로프(40,10)
def _calc_envelope(df: pd.DataFrame, period: int, pct: float) -> dict:
    ma = df['Close'].rolling(period).mean()
    upper = ma * (1 + pct / 100)
    lower = ma * (1 - pct / 100)
    return {'ma': ma, 'upper': upper, 'lower': lower}


def _check_envelope_bottom(row: pd.Series, df: pd.DataFrame) -> dict:
    close = float(row.get('Close', 0))
    if close <= 0:
        return {
            'env20_near': False,
            'env40_near': False,
            'env20_pct': 0.0,
            'env40_pct': 0.0,
            'lower20': 0,
            'lower40': 0,
        }

    env20 = _calc_envelope(df, 20, 10)
    lower20 = float(env20['lower'].iloc[-1]) if not pd.isna(env20['lower'].iloc[-1]) else 0.0
    env20_pct = ((close - lower20) / lower20 * 100) if lower20 > 0 else 999.0

    env40 = _calc_envelope(df, 40, 10)
    lower40 = float(env40['lower'].iloc[-1]) if not pd.isna(env40['lower'].iloc[-1]) else 0.0
    env40_pct = ((close - lower40) / lower40 * 100) if lower40 > 0 else 999.0

    return {
        'env20_near': -2.0 <= env20_pct <= 2.0,
        'env40_near': -10.0 <= env40_pct <= 10.0,
        'env20_pct': round(env20_pct, 1),
        'env40_pct': round(env40_pct, 1),
        'lower20': round(lower20) if lower20 > 0 else 0,
        'lower40': round(lower40) if lower40 > 0 else 0,
    }


# =============================================================
# 설정
# =============================================================
HOLD_DAYS_LIST = [1, 3, 5, 10, 15]
PROFIT_TARGET = 3.0
STOP_LOSS = -3.0
MAX_HOLD_DAYS = 15
TARGET_HIT_PCT = 2.0
SAME_DAY_EXIT_POLICY = 'stop_first'  # stop_first | target_first
MAX_WORKERS = 15
TOP_N = 300

JSON_KEY_PATH = str(Path(__file__).resolve().with_name('stock-key.json'))
SHEET_NAME = '주식자동매매일지'
MAIN_TAB_NAME = '종가배팅'
SUMMARY_TAB_PREFIX = '종가배팅_'
FLOW_SNAPSHOT_CSV = Path(os.environ.get('CLOSING_BET_FLOW_SNAPSHOT_CSV', './closing_bet_logs/closing_bet_flow_snapshots.csv'))
FLOW_SNAPSHOT_LOOKUP: dict[str, dict] = {}


PRETTY_RAW_COLUMNS = {
    '스캔일': '신호일',
    'code': '종목코드',
    'name': '종목명',
    '전략': '전략코드',
    '전략명': '전략명',
    '지수구분': '지수구분',
    '유니버스태그': '유니버스태그',
    '추천밴드': '추천밴드',
    '변동성성격': '변동성성격',
    '밴드코멘트': '밴드코멘트',
    '밴드추천사유': '밴드추천사유',
    '시총상위여부': '시총상위포함여부',
    '밴드구분': '적용밴드',
    '밴드상태': '밴드상태',
    '충족조건': '충족조건',
    '총점수': '종합점수',
    'A점수': '돌파형점수',
    'B1점수': 'ENV엄격형점수',
    'B2점수': 'BB확장형점수',
    '종가': '신호일종가',
    '진입가': '당일종가진입가',
    '거래량배율': '거래량배수',
    '전고점%': '20일전고점대비%',
    '이격도': '20일이격도',
    '윗꼬리%': '윗꼬리비율%',
    'RSI': 'RSI',
    'Env20%': 'Env20하단거리%',
    'Env40%': 'Env40하단거리%',
    'BB40%': 'BB40하단거리%',
    'BB폭40%': 'BB40폭%',
    'ATR%': 'ATR%',
    '5일매집수': '최근5일매집봉수',
    'OBV상승': 'OBV상승여부',
    '거래대금억': '신호일거래대금(억)',
    '15일판정': '15일최종판정',
    '15일내2%도달': '15일내2%도달(고가기준)',
    '2%도달일': '2%도달일(고가기준)',
    '15일내2%도달_고가기준': '15일내2%도달(고가기준_상세)',
    '2%도달일_고가기준': '2%도달일(고가기준_상세)',
    '15일내2%도달_종가기준': '15일내2%도달(종가기준)',
    '2%도달일_종가기준': '2%도달일(종가기준)',
    '15일내손절터치': '15일내손절터치여부',
    '손절터치일': '손절터치일',
    '15일종가수익률%': '15일종가수익률%',
    '15일최고수익률%': '15일최대수익률%(MFE)',
    '15일최저수익률%': '15일최대낙폭%(MAE)',
    '실전청산일': '실전청산일',
    '실전청산사유': '실전청산사유',
    '평가완료일수': '평가완료일수',
    '진행상태': '진행상태',
    '추정수급스냅샷': '추정수급스냅샷',
    '추정수급시각': '추정수급시각',
    '추정기관수량': '추정기관수량',
    '추정외인수량': '추정외인수량',
    '추정기관금액(억)': '추정기관금액(억)',
    '추정외인금액(억)': '추정외인금액(억)',
    '추정외인기관합(억)': '추정외인기관합(억)',
    '추정수급코멘트': '추정수급코멘트',
}

SUMMARY_TAB_NAME_MAP = {
    '전략별': '전략성과',
    '월별': '월별성과',
    '밴드별': '적용밴드별',
    '추천밴드별': '추천밴드별',
    '유니버스태그별': '유니버스별',
    '보유기간별': '보유기간별',
    '추정수급스냅샷별': '추정수급스냅샷별',
}

SUMMARY_COLUMN_RENAME_MAP = {
    '총건수': '신호건수',
    '건수': '신호건수',
    '평균총점': '평균종합점수',
    '평균A점수': '평균돌파형점수',
    '평균B1점수': '평균ENV엄격형점수',
    '평균B2점수': '평균BB확장형점수',
    '15일판정승률%': '15일최종승률%',
    '15일내2%도달률%(고가기준)': '15일내2%도달률%(고가)',
    '15일내2%종가도달률%': '15일내2%도달률%(종가)',
    '15일내손절터치율%': '15일내손절터치율%',
    '15일평균종가수익%': '15일평균종가수익률%',
    '15일MFE평균%': '15일평균최대수익률%(MFE)',
    '15일MAE평균%': '15일평균최대낙폭%(MAE)',
    '승률%': '승률%',
    '평균수익%': '평균수익률%',
    '년월': '스캔년월',
    'A건수': '돌파형건수',
    'B1건수': 'ENV엄격형건수',
    'B2건수': 'BB확장형건수',
    '밴드': '적용밴드',
    '완료건수': '완료건수',
    '부분평가건수': '부분평가건수',
}

def _prettify_raw_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rename_map = {c: PRETTY_RAW_COLUMNS[c] for c in out.columns if c in PRETTY_RAW_COLUMNS}
    out = out.rename(columns=rename_map)
    return out


def _prettify_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rename_map = {c: SUMMARY_COLUMN_RENAME_MAP[c] for c in out.columns if c in SUMMARY_COLUMN_RENAME_MAP}
    out = out.rename(columns=rename_map)
    return out
SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]

DEFAULT_UNIVERSE = 'hybrid_union'
INDEX_MAP: dict[str, str] = {}
TOP_MCAP_SET: set[str] = set()
NAME_MAP: dict[str, str] = {}


def _load_flow_snapshot_lookup():
    global FLOW_SNAPSHOT_LOOKUP
    FLOW_SNAPSHOT_LOOKUP = {}

    if not FLOW_SNAPSHOT_CSV.exists():
        log_info(f"추정 수급 스냅샷 파일 없음: {FLOW_SNAPSHOT_CSV}")
        return

    try:
        snap = pd.read_csv(FLOW_SNAPSHOT_CSV, dtype={'code': str}, encoding='utf-8-sig')
        if snap is None or snap.empty:
            return
        snap['code'] = snap['code'].astype(str).str.zfill(6)
        if 'scan_time' not in snap.columns:
            snap['scan_time'] = ''
        if 'mode' not in snap.columns:
            snap['mode'] = ''
        snap = snap.sort_values(['scan_date', 'scan_time', 'code']).copy()
        # scan_date + code + mode 우선, 없으면 scan_date + code 보조로 조회 가능하게 둘 다 생성
        for _, r in snap.iterrows():
            row = r.to_dict()
            key_mode = f"{row.get('scan_date','')}|{str(row.get('code','')).zfill(6)}|{row.get('mode','')}"
            key_fallback = f"{row.get('scan_date','')}|{str(row.get('code','')).zfill(6)}"
            FLOW_SNAPSHOT_LOOKUP[key_mode] = row
            FLOW_SNAPSHOT_LOOKUP[key_fallback] = row
        log_info(f"✅ 추정 수급 스냅샷 로드: {len(snap)}행")
    except Exception as e:
        log_error(f"⚠️ 추정 수급 스냅샷 로드 실패: {e}")
        FLOW_SNAPSHOT_LOOKUP = {}


def _get_flow_snapshot_info(scan_date: str, code: str, mode: str) -> dict:
    code = str(code).zfill(6)
    key_mode = f"{scan_date}|{code}|{mode}"
    key_fallback = f"{scan_date}|{code}"
    row = FLOW_SNAPSHOT_LOOKUP.get(key_mode) or FLOW_SNAPSHOT_LOOKUP.get(key_fallback) or {}
    if not row:
        return {
            'snapshot_ok': 'N',
            'snapshot_time': '',
            'inst_qty_est': np.nan,
            'frgn_qty_est': np.nan,
            'inst_amt_est_b': np.nan,
            'frgn_amt_est_b': np.nan,
            'fi_amt_est_b': np.nan,
            'flow_comment_est': '',
        }
    return {
        'snapshot_ok': 'Y' if int(row.get('flow_snapshot_ok', 0) or 0) == 1 else 'N',
        'snapshot_time': row.get('scan_time', ''),
        'inst_qty_est': _safe_float(row.get('inst_qty_est', np.nan), np.nan),
        'frgn_qty_est': _safe_float(row.get('frgn_qty_est', np.nan), np.nan),
        'inst_amt_est_b': _safe_float(row.get('inst_amt_est_b', np.nan), np.nan),
        'frgn_amt_est_b': _safe_float(row.get('frgn_amt_est_b', np.nan), np.nan),
        'fi_amt_est_b': _safe_float(row.get('fi_amt_est_b', np.nan), np.nan),
        'flow_comment_est': row.get('flow_comment', ''),
    }


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
      - hybrid_union
      - hybrid_intersection
    """
    global INDEX_MAP, TOP_MCAP_SET, NAME_MAP
    INDEX_MAP = {}
    TOP_MCAP_SET = set()
    NAME_MAP = {}

    kospi = _get_kospi200()
    kosdaq = _get_kosdaq150()
    for c in kospi:
        INDEX_MAP[c] = '코스피200'
    for c in kosdaq:
        if c not in INDEX_MAP:
            INDEX_MAP[c] = '코스닥150'

    top_codes = []
    try:
        log_info("종목 리스트 수집...")
        df_k = fdr.StockListing('KOSPI')
        df_q = fdr.StockListing('KOSDAQ')
        df = pd.concat([df_k, df_q], ignore_index=True)
        if df is not None and not df.empty:
            name_col = next((c for c in df.columns if c in ('Name', 'name', '종목명')), None)
            sym_name_col = next((c for c in df.columns if c in ('Code', 'Symbol', '종목코드')), None)
            if name_col and sym_name_col:
                NAME_MAP = {str(c).zfill(6): str(n) for c, n in zip(df[sym_name_col], df[name_col])}
            mcap_col = next((c for c in df.columns if 'cap' in c.lower()), None)
            sym_col = next((c for c in df.columns if c in ('Code', 'Symbol', '종목코드')), None)
            if mcap_col and sym_col:
                df = df.nlargest(top_n, mcap_col)
                top_codes = [str(c).zfill(6) for c in df[sym_col].tolist()]
                TOP_MCAP_SET = set(top_codes)
                log_info(f"FDR 시총상위: {len(top_codes)}개")
    except Exception as e:
        log_error(f"FDR 실패: {e}")

    idx_codes = list(dict.fromkeys(kospi + kosdaq))

    if universe == 'kospi200':
        codes = kospi
    elif universe == 'kosdaq150':
        codes = kosdaq
    elif universe == 'kospi200+kosdaq150':
        codes = idx_codes
    elif universe == 'top_marketcap':
        codes = top_codes
    elif universe == 'hybrid_union':
        codes = list(dict.fromkeys(idx_codes + top_codes))
    elif universe == 'hybrid_intersection':
        top_set = set(top_codes)
        codes = [c for c in idx_codes if c in top_set]
    else:
        codes = top_codes or idx_codes

    if codes:
        log_info(f"유니버스 {universe}: {len(codes)}개")
        return codes

    try:
        from pykrx import stock as _pk
        codes = _pk.get_market_ticker_list(market='ALL')
        log_info(f"pykrx 폴백: {len(codes[:top_n])}개")
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

def _get_ticker_name(code: str) -> str:
    code = str(code).zfill(6)
    if code in NAME_MAP:
        return NAME_MAP[code]
    try:
        from pykrx import stock as _pk
        name = _pk.get_market_ticker_name(code)
        return name or code
    except Exception:
        return code

def _build_universe_tag(index_label: str = '', is_top_mcap: bool = False) -> str:
    tags = []
    if index_label == '코스피200':
        tags.append('코스피200')
    elif index_label == '코스닥150':
        tags.append('코스닥150')
    if is_top_mcap:
        tags.append('시총상위')
    return '+'.join(tags) if tags else '기타'


def _get_band_recommendation(
    code: str,
    sub_df: pd.DataFrame,
    row: pd.Series,
    index_label: str = '',
    is_top_mcap: bool = False,
) -> dict:
    close = _safe_float(row.get('Close', 0), 0.0)
    atr = _safe_float(row.get('ATR', 0), 0.0)
    atr_pct = (atr / close * 100) if close > 0 else 0.0

    bb = _check_bb_bottom(row, sub_df)
    bb_width = float(bb.get('bb40_width', 0) or 0)
    amount_b_series = (sub_df['Close'] * sub_df['Volume']) / 1e8
    amount20_b = float(amount_b_series.rolling(20).mean().iloc[-1]) if len(amount_b_series) >= 20 else 0.0

    if index_label == '코스피200':
        recommended_band = 'ENV'
        base_reason = '코스피200 기본값'
    elif index_label == '코스닥150':
        recommended_band = 'BB'
        base_reason = '코스닥150 기본값'
    else:
        recommended_band = 'BB'
        base_reason = '비지수/확장형 기본값'

    if bb_width >= 18 or atr_pct >= 4.0 or amount20_b >= 500:
        volatility_type = '변동형'
    elif bb_width <= 10 and atr_pct <= 2.2 and amount20_b <= 150:
        volatility_type = '안정형'
    else:
        volatility_type = '중간형'

    reason_parts = [base_reason]
    if volatility_type == '변동형':
        recommended_band = 'BB'
        if bb_width >= 18:
            reason_parts.append(f'BB폭 큼({bb_width:.1f})')
        if atr_pct >= 4.0:
            reason_parts.append(f'ATR 큼({atr_pct:.1f}%)')
        if amount20_b >= 500:
            reason_parts.append(f'거래대금 큼({amount20_b:.1f}억)')
    elif volatility_type == '안정형':
        recommended_band = 'ENV'
        reason_parts.append(f'안정형(BB폭 {bb_width:.1f}, ATR {atr_pct:.1f}%)')
    else:
        reason_parts.append('중간형')

    universe_tag = _build_universe_tag(index_label=index_label, is_top_mcap=is_top_mcap)
    comment = f"추천밴드 {recommended_band} | {volatility_type} | {universe_tag} | " + ", ".join(reason_parts)

    return {
        'recommended_band': recommended_band,
        'volatility_type': volatility_type,
        'universe_tag': universe_tag,
        'band_comment': comment,
        'band_recommend_reason': ', '.join(reason_parts),
        'is_top_mcap': int(is_top_mcap),
        'bb40_width': round(bb_width, 1),
        'atr_pct': round(atr_pct, 1),
        'amount20_b': round(amount20_b, 1),
    }


# =============================================================
# 최근 3일 외인/기관 수급 유틸
# =============================================================
def _get_investor_flow_df(code: str, start: str, end: str) -> pd.DataFrame:
    """
    pykrx 일자별 투자자 거래대금(순매수) 조회 캐시.
    columns 예시: 기관합계, 외국인합계, 개인, 전체
    """
    code = str(code).zfill(6)
    cache_key = f"{code}|{start}|{end}"
    if cache_key in INVESTOR_FLOW_CACHE:
        return INVESTOR_FLOW_CACHE[cache_key]

    if not HAS_PYKRX or pk_stock is None:
        INVESTOR_FLOW_CACHE[cache_key] = pd.DataFrame()
        return INVESTOR_FLOW_CACHE[cache_key]

    try:
        df = pk_stock.get_market_trading_value_by_date(start.replace('-', ''), end.replace('-', ''), code)
        if df is None or df.empty:
            INVESTOR_FLOW_CACHE[cache_key] = pd.DataFrame()
            return INVESTOR_FLOW_CACHE[cache_key]
        df = df.copy()
        df.index = pd.to_datetime(df.index)
        INVESTOR_FLOW_CACHE[cache_key] = df
        return df
    except Exception as e:
        log_debug(f"수급조회 실패 {code}: {e}")
        INVESTOR_FLOW_CACHE[cache_key] = pd.DataFrame()
        return INVESTOR_FLOW_CACHE[cache_key]


def _pick_flow_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _calc_investor_flow_features(code: str, signal_date, lookback_days: int = 3, avg_amount20_b: float = 0.0) -> dict:
    """
    최근 3거래일 외인/기관 순매수 흔적을 점수화.

    - flow_3d_sum_b: 외인+기관 최근 3일 합(억원)
    - flow_pos_days: 외인+기관 합 기준 양수 일수
    - flow_min_ratio: 최근 3일 중 최저 일별순매수 / 최근20일평균거래대금
    - flow_score: 0~4
    """
    neutral = {
        'flow_inst_3d_b': 0.0,
        'flow_foreign_3d_b': 0.0,
        'flow_3d_sum_b': 0.0,
        'flow_pos_days': 0,
        'flow_neg_days': 0,
        'flow_min_ratio': np.nan,
        'flow_score': 0,
        'flow_grade': '중립',
        'flow_support': 'N',
        'flow_comment': '수급데이터없음',
    }

    signal_ts = pd.to_datetime(signal_date)
    start = (signal_ts - pd.Timedelta(days=20)).strftime('%Y-%m-%d')
    end = signal_ts.strftime('%Y-%m-%d')
    flow_df = _get_investor_flow_df(code, start, end)
    if flow_df is None or flow_df.empty:
        return neutral

    inst_col = _pick_flow_col(flow_df, ['기관합계', '기관'])
    foreign_col = _pick_flow_col(flow_df, ['외국인합계', '외국인'])
    if not inst_col or not foreign_col:
        return neutral

    window = flow_df.loc[flow_df.index <= signal_ts, [inst_col, foreign_col]].tail(lookback_days).copy()
    if window.empty:
        return neutral

    window[inst_col] = pd.to_numeric(window[inst_col], errors='coerce').fillna(0)
    window[foreign_col] = pd.to_numeric(window[foreign_col], errors='coerce').fillna(0)
    fi_daily = window[inst_col] + window[foreign_col]

    inst_3d_b = float(window[inst_col].sum() / 1e8)
    foreign_3d_b = float(window[foreign_col].sum() / 1e8)
    flow_3d_sum_b = float(fi_daily.sum() / 1e8)
    pos_days = int((fi_daily > 0).sum())
    neg_days = int((fi_daily < 0).sum())

    avg_amount20_krw = float(avg_amount20_b) * 1e8
    if avg_amount20_krw > 0:
        min_ratio = float((fi_daily / avg_amount20_krw).min())
    else:
        min_ratio = np.nan

    score = 0
    if flow_3d_sum_b > 0:
        score += 1
    if pos_days >= 2:
        score += 1
    if pd.notna(min_ratio) and min_ratio > -0.03:
        score += 1
    if inst_3d_b > 0 or foreign_3d_b > 0:
        score += 1

    if score >= 4:
        grade = '강함'
    elif score >= 2:
        grade = '양호'
    elif score >= 1:
        grade = '보통'
    else:
        grade = '약함'

    support = 'Y' if score >= 2 else 'N'
    comment = (
        f"수급3일합 {flow_3d_sum_b:+.1f}억 | 외인 {foreign_3d_b:+.1f}억 | 기관 {inst_3d_b:+.1f}억 | "
        f"양수일수 {pos_days}/{len(window)} | 최대이탈비율 {min_ratio:.3f}" if pd.notna(min_ratio) else
        f"수급3일합 {flow_3d_sum_b:+.1f}억 | 외인 {foreign_3d_b:+.1f}억 | 기관 {inst_3d_b:+.1f}억 | 양수일수 {pos_days}/{len(window)}"
    )

    return {
        'flow_inst_3d_b': round(inst_3d_b, 1),
        'flow_foreign_3d_b': round(foreign_3d_b, 1),
        'flow_3d_sum_b': round(flow_3d_sum_b, 1),
        'flow_pos_days': pos_days,
        'flow_neg_days': neg_days,
        'flow_min_ratio': round(min_ratio, 4) if pd.notna(min_ratio) else np.nan,
        'flow_score': score,
        'flow_grade': grade,
        'flow_support': support,
        'flow_comment': comment,
    }


def _pass_flow_filter(flow_info: dict, mode: str = 'off') -> bool:
    mode = (mode or 'off').lower()
    if mode == 'off':
        return True
    score = int(flow_info.get('flow_score', 0) or 0)
    if mode == 'soft':
        return score >= 2
    if mode == 'strict':
        return score >= 3
    return True


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
    is_top_mcap = str(code).zfill(6) in TOP_MCAP_SET
    band_rec = _get_band_recommendation(code, sub_df, state['row'], state['index_label'], is_top_mcap)
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
        'recommended_band': band_rec['recommended_band'],
        'volatility_type': band_rec['volatility_type'],
        'universe_tag': band_rec['universe_tag'],
        'band_comment': band_rec['band_comment'],
        'band_recommend_reason': band_rec['band_recommend_reason'],
        'is_top_mcap': band_rec['is_top_mcap'],
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
        'amount20_b': band_rec['amount20_b'],
        'atr_pct': state['atr_pct'],
        'maejip_5d': state['maejip_5d'],
        'obv_rising': state['obv_rising'],
    }


# =============================================================
# 실전형 판정 유틸 (최대 15일 / +2% 선도달 / -3% 손절)
# =============================================================

def _evaluate_trade_window(df: pd.DataFrame, signal_idx: int, entry_price: float) -> dict:
    """
    signal_idx 당일 종가에 진입하고, 다음 거래일부터 최대 15거래일을 추적.

    최근 구간도 제외하지 않고, 가능한 구간까지만 부분 평가한다.
    - 15거래일이 모두 있으면 진행상태='완료'
    - 부족하면 진행상태='부분평가'
    - 0거래일이면 진행상태='미평가'

    판정 우선순위
    1) +2% 먼저 도달(고가 기준) -> 승
    2) -3% 먼저 도달 -> 손절
    3) 둘 다 없으면 현재 확보 가능한 마지막 종가가 진입가보다 높으면 승(종가), 낮으면 패(종가)
    """
    result = {
        '평가완료일수': 0,
        '진행상태': '미평가',
        '15일판정': 'N/A',
        '15일내2%도달': 'N',                 # 하위 호환용 = 고가 기준
        '2%도달일': None,                    # 하위 호환용 = 고가 기준
        '15일내2%도달_고가기준': 'N',
        '2%도달일_고가기준': None,
        '15일내2%도달_종가기준': 'N',
        '2%도달일_종가기준': None,
        '15일내손절터치': 'N',
        '손절터치일': None,
        '15일종가수익률%': None,
        '15일최고수익률%': None,
        '15일최저수익률%': None,
        '실전청산일': None,
        '실전청산사유': '',
    }

    if entry_price <= 0 or signal_idx + 1 >= len(df):
        return result

    eval_start_idx = signal_idx + 1
    window = df.iloc[eval_start_idx:eval_start_idx + MAX_HOLD_DAYS].copy()
    days_available = len(window)
    result['평가완료일수'] = int(days_available)

    if days_available <= 0:
        return result

    result['진행상태'] = '완료' if days_available >= MAX_HOLD_DAYS else '부분평가'

    target_price = entry_price * (1 + TARGET_HIT_PCT / 100.0)
    stop_price = entry_price * (1 + STOP_LOSS / 100.0)

    max_high = _safe_float(window['High'].max())
    min_low = _safe_float(window['Low'].min())
    final_close = _safe_float(window['Close'].iloc[-1])

    result['15일최고수익률%'] = round((max_high - entry_price) / entry_price * 100, 2) if entry_price > 0 else None
    result['15일최저수익률%'] = round((min_low - entry_price) / entry_price * 100, 2) if entry_price > 0 else None
    result['15일종가수익률%'] = round((final_close - entry_price) / entry_price * 100, 2) if entry_price > 0 else None

    for day_no, (_, r) in enumerate(window.iterrows(), start=1):
        high = _safe_float(r.get('High', 0))
        low = _safe_float(r.get('Low', 0))
        close = _safe_float(r.get('Close', 0))

        hit_target_high = high >= target_price
        hit_stop = low <= stop_price

        # 실전 판정은 기존대로 고가 기준 익절 / 저가 기준 손절
        if hit_target_high and hit_stop:
            if SAME_DAY_EXIT_POLICY == 'target_first':
                result['15일판정'] = '승'
                result['실전청산일'] = day_no
                result['실전청산사유'] = '동일봉_익절우선'
            else:
                result['15일판정'] = '손절'
                result['실전청산일'] = day_no
                result['실전청산사유'] = '동일봉_손절우선'
            break
        elif hit_target_high:
            result['15일판정'] = '승'
            result['실전청산일'] = day_no
            result['실전청산사유'] = '2%도달(고가기준)'
            break
        elif hit_stop:
            result['15일판정'] = '손절'
            result['실전청산일'] = day_no
            result['실전청산사유'] = '손절터치'
            break

    # 무결성 재검증
    high_hit_mask = (window['High'] >= target_price).fillna(False)
    close_hit_mask = (window['Close'] >= target_price).fillna(False)
    stop_hit_mask = (window['Low'] <= stop_price).fillna(False)

    if bool(high_hit_mask.any()):
        first_high_day = int(high_hit_mask.to_numpy().argmax()) + 1
        result['15일내2%도달'] = 'Y'
        result['2%도달일'] = first_high_day
        result['15일내2%도달_고가기준'] = 'Y'
        result['2%도달일_고가기준'] = first_high_day
    else:
        result['15일내2%도달'] = 'N'
        result['2%도달일'] = None
        result['15일내2%도달_고가기준'] = 'N'
        result['2%도달일_고가기준'] = None

    if bool(close_hit_mask.any()):
        first_close_day = int(close_hit_mask.to_numpy().argmax()) + 1
        result['15일내2%도달_종가기준'] = 'Y'
        result['2%도달일_종가기준'] = first_close_day
    else:
        result['15일내2%도달_종가기준'] = 'N'
        result['2%도달일_종가기준'] = None

    if bool(stop_hit_mask.any()):
        first_stop_day = int(stop_hit_mask.to_numpy().argmax()) + 1
        result['15일내손절터치'] = 'Y'
        result['손절터치일'] = first_stop_day
    else:
        result['15일내손절터치'] = 'N'
        result['손절터치일'] = None

    # 아직 청산 판정이 안 났다면, 현재 확보 가능한 마지막 종가 기준으로 임시 판정
    if result['15일판정'] == 'N/A':
        if final_close > entry_price:
            result['15일판정'] = '승(종가)'
            result['실전청산일'] = days_available
            result['실전청산사유'] = '현재구간종가상승'
        elif final_close < entry_price:
            result['15일판정'] = '패(종가)'
            result['실전청산일'] = days_available
            result['실전청산사유'] = '현재구간종가하락'
        else:
            result['15일판정'] = '보합'
            result['실전청산일'] = days_available
            result['실전청산사유'] = '현재구간종가보합'

    return result


# =============================================================
# 단일 종목 백테스트
# =============================================================
def _build_signal_record(df: pd.DataFrame, i: int, code: str, cond: dict, entry_price=None, trade_eval=None, forward_returns=None, flow_info=None, snapshot_info=None) -> dict:
    row_dt = pd.to_datetime(df.iloc[i][df.columns[0] if df.columns[0] == "Date" else 'Date']) if 'Date' in df.columns else pd.to_datetime(df.iloc[i][df.columns[0]])
    record = {
        '스캔일': row_dt.strftime('%Y-%m-%d'),
        'code': str(code).zfill(6),
        'name': _get_ticker_name(code),
        '전략': cond['mode'],
        '전략명': cond['mode_label'],
        '지수구분': cond['index_label'],
        '유니버스태그': cond['universe_tag'],
        '추천밴드': cond['recommended_band'],
        '변동성성격': cond['volatility_type'],
        '밴드코멘트': cond['band_comment'],
        '밴드추천사유': cond['band_recommend_reason'],
        '시총상위여부': cond['is_top_mcap'],
        '밴드구분': cond['band_type'],
        '밴드상태': cond['band_pct_text'],
        '충족조건': ' '.join(cond['passed']),
        '총점수': cond['score'],
        'A점수': cond['a_score'],
        'B1점수': cond['b1_score'],
        'B2점수': cond['b2_score'],
        '종가': int(cond['close']),
        '진입가': int(entry_price) if entry_price is not None and _safe_float(entry_price) > 0 else None,
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
    }
    if flow_info:
        record.update({
            '수급점수': flow_info.get('flow_score', 0),
            '수급등급': flow_info.get('flow_grade', ''),
            '수급지지': flow_info.get('flow_support', 'N'),
            '수급코멘트': flow_info.get('flow_comment', ''),
            '외인3일합(억)': flow_info.get('flow_foreign_3d_b', 0.0),
            '기관3일합(억)': flow_info.get('flow_inst_3d_b', 0.0),
            '외인기관3일합(억)': flow_info.get('flow_3d_sum_b', 0.0),
            '수급양수일수(3일)': flow_info.get('flow_pos_days', 0),
            '수급음수일수(3일)': flow_info.get('flow_neg_days', 0),
            '수급최대이탈비율': flow_info.get('flow_min_ratio', np.nan),
        })
    if snapshot_info:
        record.update({
            '추정수급스냅샷': snapshot_info.get('snapshot_ok', 'N'),
            '추정수급시각': snapshot_info.get('snapshot_time', ''),
            '추정기관수량': snapshot_info.get('inst_qty_est', np.nan),
            '추정외인수량': snapshot_info.get('frgn_qty_est', np.nan),
            '추정기관금액(억)': snapshot_info.get('inst_amt_est_b', np.nan),
            '추정외인금액(억)': snapshot_info.get('frgn_amt_est_b', np.nan),
            '추정외인기관합(억)': snapshot_info.get('fi_amt_est_b', np.nan),
            '추정수급코멘트': snapshot_info.get('flow_comment_est', ''),
        })
    if trade_eval:
        record.update(trade_eval)
    if forward_returns:
        record.update(forward_returns)
    return record


def backtest_ticker(code: str, start: str, end: str) -> list:
    """성과 백테스트: 신호 당일 종가 진입 + 다음 거래일부터 최대 15일 평가"""
    records = []
    try:
        load_start = (datetime.strptime(start, '%Y-%m-%d') - timedelta(days=700)).strftime('%Y-%m-%d')
        df_raw = fdr.DataReader(code, start=load_start, end=end)
        if df_raw is None or len(df_raw) < 260:
            return []

        df = get_indicators(df_raw.copy())
        if df is None or df.empty:
            return []

        df = df.reset_index()
        date_col = 'Date' if 'Date' in df.columns else df.columns[0]

        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end_dt = datetime.strptime(end, '%Y-%m-%d')

        for i in range(60, len(df)):
            row_date = pd.to_datetime(df[date_col].iloc[i])
            row_dt = row_date.to_pydatetime().replace(tzinfo=None)
            if not (start_dt <= row_dt <= end_dt):
                continue

            cond = _check_conditions_on_date(df, i, code=code)
            if cond is None:
                continue

            flow_info = _calc_investor_flow_features(code, row_dt, lookback_days=3, avg_amount20_b=cond.get('amount20_b', 0))
            if not _pass_flow_filter(flow_info, FLOW_FILTER_MODE):
                continue

            entry_price = _safe_float(df['Close'].iloc[i])
            if entry_price <= 0:
                continue

            forward_returns = {}
            for hold in HOLD_DAYS_LIST:
                future_idx = i + hold
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

            trade_eval = _evaluate_trade_window(df, i, entry_price)
            records.append(_build_signal_record(df, i, code, cond, entry_price, trade_eval, forward_returns))

    except Exception as e:
        log_debug(f"[{code}] 오류: {e}")

    return records


def replay_ticker(code: str, start: str, end: str) -> list:
    """신호 재현 모드: 해당 날짜 종가 기준으로 어떤 종목이 떴는지만 재현"""
    records = []
    try:
        load_start = (datetime.strptime(start, '%Y-%m-%d') - timedelta(days=700)).strftime('%Y-%m-%d')
        # end 다음 영업일까지 보지 않고, 지정 end 종가 봉까지만 사용
        df_raw = fdr.DataReader(code, start=load_start, end=(datetime.strptime(end, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'))
        if df_raw is None or len(df_raw) < 260:
            return []

        df = get_indicators(df_raw.copy())
        if df is None or df.empty:
            return []

        df = df.reset_index()
        date_col = 'Date' if 'Date' in df.columns else df.columns[0]
        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end_dt = datetime.strptime(end, '%Y-%m-%d')

        for i in range(60, len(df)):
            row_date = pd.to_datetime(df[date_col].iloc[i])
            row_dt = row_date.to_pydatetime().replace(tzinfo=None)
            if not (start_dt <= row_dt <= end_dt):
                continue

            cond = _check_conditions_on_date(df, i, code=code)
            if cond is None:
                continue

            # 재현 모드에서는 신호 당일 종가 기준 진입가만 표기
            entry_price = _safe_float(df['Close'].iloc[i])
            if entry_price <= 0:
                entry_price = None

            record = _build_signal_record(df, i, code, cond, entry_price=entry_price)
            record.update({
                '모드': 'replay',
                '평가완료일수': '',
                '진행상태': '재현전용',
                '15일판정': '재현전용',
                '15일내2%도달': '',
                '2%도달일': '',
                '15일내2%도달_고가기준': '',
                '2%도달일_고가기준': '',
                '15일내2%도달_종가기준': '',
                '2%도달일_종가기준': '',
                '15일내손절터치': '',
                '손절터치일': '',
                '15일종가수익률%': None,
                '15일최고수익률%': None,
                '15일최저수익률%': None,
                '실전청산일': None,
                '실전청산사유': '신호재현',
            })
            for hold in HOLD_DAYS_LIST:
                record[f'수익률_{hold}일'] = None
                record[f'승패_{hold}일'] = 'N/A'
            records.append(record)

    except Exception as e:
        log_debug(f"[replay/{code}] 오류: {e}")

    return records


def summarize_replay(df: pd.DataFrame) -> dict:
    results = {}
    if df.empty:
        return {
            '전략별': pd.DataFrame(),
            '월별': pd.DataFrame(),
            '밴드별': pd.DataFrame(),
            '추천밴드별': pd.DataFrame(),
            '유니버스태그별': pd.DataFrame(),
            '보유기간별': pd.DataFrame(),
        }

    # 전략별 신호 건수/평균점수
    rows = []
    name_map = {'A': '돌파형(A)', 'B1': 'ENV엄격형(B1)', 'B2': 'BB확장형(B2)'}
    for strategy in ['A', 'B1', 'B2']:
        grp = df[df['전략'] == strategy]
        if grp.empty:
            continue
        rows.append({
            '전략': name_map.get(strategy, strategy),
            '총건수': len(grp),
            '평균총점': round(pd.to_numeric(grp['총점수'], errors='coerce').mean(), 1),
            '평균A점수': round(pd.to_numeric(grp['A점수'], errors='coerce').mean(), 1),
            '평균B1점수': round(pd.to_numeric(grp['B1점수'], errors='coerce').mean(), 1),
            '평균B2점수': round(pd.to_numeric(grp['B2점수'], errors='coerce').mean(), 1),
        })
    results['전략별'] = pd.DataFrame(rows)

    df2 = df.copy()
    df2['년월'] = df2['스캔일'].astype(str).str[:7]
    monthly = []
    for ym, grp in df2.groupby('년월'):
        monthly.append({
            '년월': ym,
            '총건수': len(grp),
            'A건수': int((grp['전략'] == 'A').sum()),
            'B1건수': int((grp['전략'] == 'B1').sum()),
            'B2건수': int((grp['전략'] == 'B2').sum()),
        })
    results['월별'] = pd.DataFrame(monthly)

    band_rows = []
    if '밴드구분' in df.columns:
        for (strategy, band), grp in df.groupby(['전략', '밴드구분']):
            band_rows.append({'전략': strategy, '밴드': band, '건수': len(grp)})
    results['밴드별'] = pd.DataFrame(band_rows)

    rec_band_rows = []
    if '추천밴드' in df.columns:
        for rec_band, grp in df.groupby('추천밴드'):
            rec_band_rows.append({'추천밴드': rec_band, '건수': len(grp)})
    results['추천밴드별'] = pd.DataFrame(rec_band_rows)

    univ_rows = []
    if '유니버스태그' in df.columns:
        for tag, grp in df.groupby('유니버스태그'):
            univ_rows.append({'유니버스태그': tag, '건수': len(grp)})
    results['유니버스태그별'] = pd.DataFrame(univ_rows)

    results['보유기간별'] = pd.DataFrame()
    return results


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
        판정승 = grp['15일판정'].isin(['승', '승(종가)']).mean() * 100 if '15일판정' in grp.columns else 0
        타겟도달 = (grp['15일내2%도달'] == 'Y').mean() * 100 if '15일내2%도달' in grp.columns else 0
        손절터치 = (grp['15일내손절터치'] == 'Y').mean() * 100 if '15일내손절터치' in grp.columns else 0

        row = {
            '전략': name_map.get(strategy, strategy),
            '총건수': n,
            '평균총점': round(grp['총점수'].mean(), 1),
            '평균A점수': round(grp['A점수'].mean(), 1),
            '평균B1점수': round(grp['B1점수'].mean(), 1),
            '평균B2점수': round(grp['B2점수'].mean(), 1),
            '15일판정승률%': round(판정승, 1),
            '15일내2%도달률%(고가기준)': round(타겟도달, 1),
            '15일내2%종가도달률%': round((grp['15일내2%도달_종가기준'] == 'Y').mean() * 100, 1) if '15일내2%도달_종가기준' in grp.columns else 0,
            '15일내손절터치율%': round(손절터치, 1),
            '15일평균종가수익%': round(pd.to_numeric(grp['15일종가수익률%'], errors='coerce').dropna().mean(), 2),
            '15일MFE평균%': round(pd.to_numeric(grp['15일최고수익률%'], errors='coerce').dropna().mean(), 2),
            '15일MAE평균%': round(pd.to_numeric(grp['15일최저수익률%'], errors='coerce').dropna().mean(), 2),
        }
        for hold in HOLD_DAYS_LIST:
            key = f'승패_{hold}일'
            if key in grp.columns:
                valid = grp[grp[key] != 'N/A']
                if not valid.empty:
                    win_rate = (valid[key] == '승').mean() * 100
                    avg_ret = pd.to_numeric(valid[f'수익률_{hold}일'], errors='coerce').dropna().mean()
                    row[f'{hold}일_승률%'] = round(win_rate, 1)
                    row[f'{hold}일_평균수익%'] = round(avg_ret, 2)
        rows.append(row)
    results['전략별'] = pd.DataFrame(rows)

    hold_rows = []
    for strategy in ['A', 'B1', 'B2']:
        grp = df[df['전략'] == strategy]
        if grp.empty:
            continue
        name_map = {
            'A': '돌파형(A)',
            'B1': 'ENV엄격형(B1)',
            'B2': 'BB확장형(B2)',
        }
        for hold in HOLD_DAYS_LIST:
            key = f'승패_{hold}일'
            ret_key = f'수익률_{hold}일'
            if key not in grp.columns:
                continue
            valid = grp[grp[key] != 'N/A']
            if valid.empty:
                continue
            hold_rows.append({
                '전략': name_map.get(strategy, strategy),
                '보유일': hold,
                '건수': len(valid),
                '승률%': round((valid[key] == '승').mean() * 100, 1),
                '평균수익%': round(pd.to_numeric(valid[ret_key], errors='coerce').dropna().mean(), 2),
            })
    results['보유기간별'] = pd.DataFrame(hold_rows)

    df2 = df.copy()
    df2['년월'] = df2['스캔일'].astype(str).str[:7]
    monthly = []
    for ym, grp in df2.groupby('년월'):
        n = len(grp)
        row = {
            '년월': ym,
            '총건수': n,
            'A건수': (grp['전략'] == 'A').sum(),
            'B1건수': (grp['전략'] == 'B1').sum(),
            'B2건수': (grp['전략'] == 'B2').sum(),
            '15일판정승률%': round(grp['15일판정'].isin(['승', '승(종가)']).mean() * 100, 1),
            '15일내2%도달률%(고가기준)': round((grp['15일내2%도달'] == 'Y').mean() * 100, 1),
            '15일내2%종가도달률%': round((grp['15일내2%도달_종가기준'] == 'Y').mean() * 100, 1) if '15일내2%도달_종가기준' in grp.columns else 0,
        }
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
                '15일판정승률%': round(grp['15일판정'].isin(['승', '승(종가)']).mean() * 100, 1),
                '15일내2%도달률%(고가기준)': round((grp['15일내2%도달'] == 'Y').mean() * 100, 1),
            '15일내2%종가도달률%': round((grp['15일내2%도달_종가기준'] == 'Y').mean() * 100, 1) if '15일내2%도달_종가기준' in grp.columns else 0,
                '15일평균종가수익%': round(pd.to_numeric(grp['15일종가수익률%'], errors='coerce').dropna().mean(), 2),
                '15일MFE평균%': round(pd.to_numeric(grp['15일최고수익률%'], errors='coerce').dropna().mean(), 2),
                '15일MAE평균%': round(pd.to_numeric(grp['15일최저수익률%'], errors='coerce').dropna().mean(), 2),
            })
    results['밴드별'] = pd.DataFrame(band_rows)

    rec_band_rows = []
    if '추천밴드' in df.columns:
        for rec_band, grp in df.groupby('추천밴드'):
            if grp.empty:
                continue
            rec_band_rows.append({
                '추천밴드': rec_band,
                '건수': len(grp),
                '15일판정승률%': round(grp['15일판정'].isin(['승', '승(종가)']).mean() * 100, 1),
                '15일내2%도달률%(고가기준)': round((grp['15일내2%도달'] == 'Y').mean() * 100, 1),
            '15일내2%종가도달률%': round((grp['15일내2%도달_종가기준'] == 'Y').mean() * 100, 1) if '15일내2%도달_종가기준' in grp.columns else 0,
                '15일평균종가수익%': round(pd.to_numeric(grp['15일종가수익률%'], errors='coerce').dropna().mean(), 2),
            })
    results['추천밴드별'] = pd.DataFrame(rec_band_rows)

    tag_rows = []
    if '유니버스태그' in df.columns:
        for tag, grp in df.groupby('유니버스태그'):
            if grp.empty:
                continue
            tag_rows.append({
                '유니버스태그': tag,
                '건수': len(grp),
                '15일판정승률%': round(grp['15일판정'].isin(['승', '승(종가)']).mean() * 100, 1),
                '15일내2%도달률%(고가기준)': round((grp['15일내2%도달'] == 'Y').mean() * 100, 1),
            '15일내2%종가도달률%': round((grp['15일내2%도달_종가기준'] == 'Y').mean() * 100, 1) if '15일내2%도달_종가기준' in grp.columns else 0,
                '15일평균종가수익%': round(pd.to_numeric(grp['15일종가수익률%'], errors='coerce').dropna().mean(), 2),
            })
    results['유니버스태그별'] = pd.DataFrame(tag_rows)

    flow_rows = []
    if '수급등급' in df.columns:
        for flow_grade, grp in df.groupby('수급등급'):
            if grp.empty:
                continue
            flow_rows.append({
                '수급등급': flow_grade,
                '건수': len(grp),
                '평균수급점수': round(pd.to_numeric(grp['수급점수'], errors='coerce').dropna().mean(), 2) if '수급점수' in grp.columns else np.nan,
                '15일판정승률%': round(grp['15일판정'].isin(['승', '승(종가)']).mean() * 100, 1),
                '15일내2%도달률%(고가기준)': round((grp['15일내2%도달'] == 'Y').mean() * 100, 1),
                '15일내2%종가도달률%': round((grp['15일내2%도달_종가기준'] == 'Y').mean() * 100, 1) if '15일내2%도달_종가기준' in grp.columns else 0,
                '15일평균종가수익%': round(pd.to_numeric(grp['15일종가수익률%'], errors='coerce').dropna().mean(), 2),
            })
    results['수급등급별'] = pd.DataFrame(flow_rows)

    return results


# =============================================================
# 구글시트 저장
# =============================================================
def _get_gspread_client():
    if not HAS_GSPREAD:
        log_info("⚠️ gspread 미설치")
        return None, None
    try:
        import json as _json
        log_info(f"구글시트 연결 확인 | JSON exists={os.path.exists(JSON_KEY_PATH)} | GOOGLE_JSON_KEY exists={'YES' if os.environ.get('GOOGLE_JSON_KEY') else 'NO'}")

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
        log_info(f"✅ 구글시트 문서 연결 성공: {SHEET_NAME}")
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

    log_info(f"구글시트 저장 시작... [{SHEET_NAME}]")
    saved_at = datetime.now().strftime('%Y-%m-%d %H:%M')

    raw_out = _prettify_raw_df(raw_df)
    raw_out.insert(0, '분석기간', f"{start}~{end}")
    raw_out.insert(1, '저장시각', saved_at)
    _upsert_tab(doc, MAIN_TAB_NAME, raw_out.head(5000))

    for key, df in summary.items():
        if df is None or df.empty:
            continue
        out = _prettify_summary_df(df)
        out.insert(0, '분석기간', f"{start}~{end}")
        out.insert(1, '저장시각', saved_at)
        pretty_tab = SUMMARY_TAB_NAME_MAP.get(key, key)
        _upsert_tab(doc, f'{SUMMARY_TAB_PREFIX}{pretty_tab}', out)

    log_info('✅ 구글시트 저장 완료')



# =============================================================
# 메인
# =============================================================
def main():
    parser = argparse.ArgumentParser(description='종가배팅 백테스트/신호재현 (A/B1/B2)')
    parser.add_argument('--start', required=True, help='시작일 YYYY-MM-DD')
    parser.add_argument('--end', required=True, help='종료일 YYYY-MM-DD')
    parser.add_argument('--top', type=int, default=TOP_N, help='분석 종목 수')
    parser.add_argument(
        '--universe',
        default=DEFAULT_UNIVERSE,
        choices=['top_marketcap', 'kospi200+kosdaq150', 'kospi200', 'kosdaq150', 'hybrid_union', 'hybrid_intersection'],
        help='백테스트 유니버스',
    )
    parser.add_argument('--save-csv', action='store_true', help='원본/요약 CSV 저장')
    parser.add_argument('--mode', default='performance', choices=['performance', 'replay'], help='performance=성과백테스트, replay=신호재현')
    parser.add_argument('--flow-filter', default='off', choices=['off', 'soft', 'strict'], help='최근3일 외인/기관 수급 필터: off=미적용, soft=점수2이상, strict=점수3이상')
    args = parser.parse_args()

    codes = _get_ticker_list(args.top, universe=args.universe)
    if not codes:
        log_error('분석할 종목이 없습니다.')
        sys.exit(1)

    log_info(f"실행 시작: {args.start} ~ {args.end} | {len(codes)}개 | mode={args.mode} | flow_filter={args.flow_filter}")

    all_records = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        worker = backtest_ticker if args.mode == 'performance' else replay_ticker
        futures = {ex.submit(worker, code, args.start, args.end): code for code in codes}
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
    summary = summarize(raw_df) if args.mode == 'performance' else summarize_replay(raw_df)

    log_info(f"총 레코드: {len(raw_df)}")
    for name, df in summary.items():
        log_info(f"[{name}] {len(df)}행")
        if not df.empty:
            print(df.head(20).to_string(index=False))
            print()

    stamp = f"{args.start}_{args.end}".replace('-', '')
    if args.save_csv:
        raw_path = f"Closing_bet_{args.mode}_raw_{stamp}.csv"
        raw_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
        log_info(f"원본 CSV 저장: {raw_path}")
        for name, df in summary.items():
            out = f"Closing_bet_{args.mode}_{name}_{stamp}.csv"
            df.to_csv(out, index=False, encoding='utf-8-sig')
            log_info(f"요약 CSV 저장: {out}")

    auto_gsheet = bool(os.environ.get('GOOGLE_JSON_KEY')) or os.path.exists(JSON_KEY_PATH)
    if auto_gsheet:
        log_info("구글시트 자동저장 조건 충족")
        save_to_gsheet(raw_df, summary, args.start, args.end)
    else:
        log_info("구글시트 자동저장 생략 (인증정보 없음)")


if __name__ == '__main__':
    main()
