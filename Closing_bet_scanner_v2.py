
# =============================================================
# closing_bet_scanner.py — 종가배팅 타점 스캐너 (최종 통합본)
# =============================================================
# 전략 구성
# A  : 돌파형 종가배팅
# B1 : ENV 엄격형 바닥 반등 (HTS 철학 유지)
# B2 : BB 확장형 하단 재안착
#
# 검증 기능
# - 오늘 후보를 CSV 로그로 저장
# - 다음 거래일 OHLC로 자동 성과 평가
# - 전략별/등급별/지수별 누적 통계 출력
#
# 실행 예시
# python closing_bet_scanner.py
# python closing_bet_scanner.py --force
# python closing_bet_scanner.py --eval-pending --summary --send-summary --force
# =============================================================

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
import FinanceDataReader as fdr

from main7_bugfix_2 import (
    get_indicators,
    _calc_upper_wick_ratio,
    load_krx_listing_safe,
    ANTHROPIC_API_KEY,
    OPENAI_API_KEY,
    GROQ_API_KEY,
    TODAY_STR,
    KST,
)

# ── 종가배팅 전용 텔레그램 설정
TELEGRAM_TOKEN = (
    os.environ.get('CLOSING_BET_TOKEN')
    or os.environ.get('TELEGRAM_TOKEN', '')
)
CHAT_ID_LIST = [
    c.strip()
    for c in (
        os.environ.get('CLOSING_BET_CHAT_ID')
        or os.environ.get('TELEGRAM_CHAT_ID', '')
    ).split(',')
    if c.strip()
]

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


# =============================================================
# 기본 설정
# =============================================================
MIN_PRICE = 5_000
MIN_AMOUNT = 3_000_000_000    # 거래대금 30억 이상
MIN_MARCAP = 50_000_000_000   # 시총 500억 이상
TOP_N = 400

# 유니버스
# 'kospi200+kosdaq150' : 코스피200 + 코스닥150
# 'amount_top400'      : 거래대금 상위 400개
# 'kospi200'           : 코스피200만
# 'hybrid_union'       : 코스피200 + 코스닥150 + 시총상위 합집합
# 'hybrid_intersection': 지수유니버스 ∩ 시총상위 교집합
SCAN_UNIVERSE = 'hybrid_union'

MAX_WORKERS = 20

# 실행 가능 시간대
SCAN_START_HOUR = 14
SCAN_START_MIN = 50
SCAN_END_HOUR = 15
SCAN_END_MIN = 25

# 전략 A 임계값
NEAR_HIGH20_MIN = 85.0
NEAR_HIGH20_MAX = 100.0
UPPER_WICK_MAX = 0.20
VOL_MULT = 2.0
DISPARITY_MIN = 98.0
DISPARITY_MAX = 112.0

# B2용 BB 기준
BB40_NEAR_PCT = 2.5
BB_SWITCH_WIDTH = 18.0
ENV_SWITCH_WIDTH = 10.0
BB_SWITCH_ATR = 4.0
ENV_SWITCH_ATR = 2.2
BB_SWITCH_AMOUNT20_B = 500.0
ENV_SWITCH_AMOUNT20_B = 150.0

# 알림/로그
ALERTED_FILE = '/tmp/closing_bet_alerted.json'
LOG_DIR = Path(os.environ.get("CLOSING_BET_LOG_DIR", "./closing_bet_logs"))
SIGNAL_LOG_CSV = LOG_DIR / "closing_bet_signals.csv"
SUMMARY_REPORT_TXT = LOG_DIR / "closing_bet_summary.txt"

# 다음날 성과 평가 가능 시간
EVAL_READY_HOUR = 16
EVAL_READY_MIN = 10

# 전역 지수 소속 맵 / 시총상위 맵
INDEX_MAP: dict = {}
TOP_MCAP_SET: set = set()


# =============================================================
# 유틸
# =============================================================
def _build_universe_tag(index_label: str = '', is_top_mcap: bool = False) -> str:
    tags = []
    if index_label == '코스피200':
        tags.append('K200')
    elif index_label == '코스닥150':
        tags.append('KQ150')
    if is_top_mcap:
        tags.append('MCAP')
    return '+'.join(tags) if tags else 'OTHER'


def _refresh_top_mcap_set(top_n: int = TOP_N):
    global TOP_MCAP_SET
    try:
        codes, _ = _load_amount_top_universe(top_n)
        TOP_MCAP_SET = set(codes)
    except Exception:
        TOP_MCAP_SET = set()


def _ensure_log_dir():
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def _calc_upper_wick_body_ratio(row) -> float:
    """윗꼬리 비율 — 몸통 기준"""
    high_p = float(row.get('High', 0))
    open_p = float(row.get('Open', 0))
    close_p = float(row.get('Close', 0))
    body_top = max(open_p, close_p)
    body_size = max(abs(close_p - open_p), 1e-9)
    upper_wick = max(0.0, high_p - body_top)
    return upper_wick / body_size


# =============================================================
# 텔레그램 전송
# =============================================================
def send_telegram_photo(message: str, image_paths: list = None):
    if image_paths is None:
        image_paths = []
    if not TELEGRAM_TOKEN or not message.strip():
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        if not chat_id:
            continue
        try:
            requests.post(
                url,
                data={
                    'chat_id': chat_id,
                    'text': message[:4000],
                },
                timeout=5,
            )
        except Exception as e:
            log_error(f"텔레그램 전송 실패: {e}")


def send_telegram_chunks(message: str, max_len: int = 3800):
    if not message.strip():
        return

    paragraphs = message.split('\n\n')
    chunks, current = [], ''
    for para in paragraphs:
        candidate = current + para + '\n\n'
        if len(candidate) > max_len and current.strip():
            chunks.append(current.strip())
            current = para + '\n\n'
        else:
            current = candidate

    if current.strip():
        chunks.append(current.strip())

    total = len(chunks)
    for idx, chunk in enumerate(chunks, 1):
        if total > 1:
            chunk = f"({idx}/{total})\n" + chunk
        send_telegram_photo(chunk)


# =============================================================
# Envelope / Bollinger 계산 유틸
# =============================================================
def _calc_envelope(df: pd.DataFrame, period: int, pct: float) -> dict:
    ma = df['Close'].rolling(period).mean()
    upper = ma * (1 + pct / 100)
    lower = ma * (1 - pct / 100)
    return {
        'ma': ma,
        'upper': upper,
        'lower': lower,
    }


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

    env20 = _calc_envelope(df, 20, 20)
    lower20 = float(env20['lower'].iloc[-1]) if not pd.isna(env20['lower'].iloc[-1]) else 0.0
    env20_pct = ((close - lower20) / lower20 * 100) if lower20 > 0 else 999.0

    env40 = _calc_envelope(df, 40, 40)
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


def _calc_bollinger(df: pd.DataFrame, period: int = 40, std_mult: float = 2.0) -> dict:
    mid = df['Close'].rolling(period).mean()
    std = df['Close'].rolling(period).std()
    upper = mid + std * std_mult
    lower = mid - std * std_mult
    width = pd.Series(
        np.where(mid > 0, (upper - lower) / mid * 100, np.nan),
        index=df.index,
    )
    return {
        'mid': mid,
        'upper': upper,
        'lower': lower,
        'width': width,
    }


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


def _choose_lower_band_type(code: str, df: pd.DataFrame, row: pd.Series) -> dict:
    """
    기본:
      - 코스피200  -> ENV
      - 코스닥150 -> BB

    예외:
      - 변동성 크고 거래대금 크면 BB
      - 너무 안정적이면 ENV
    """
    idx = INDEX_MAP.get(code, '')
    close = float(row.get('Close', 0) or 0)
    atr = float(row.get('ATR', 0) or 0)
    atr_pct = (atr / close * 100) if close > 0 else 0.0

    amount_b_series = (df['Close'] * df['Volume']) / 1e8
    amount20_b = float(amount_b_series.rolling(20).mean().iloc[-1]) if len(amount_b_series) >= 20 else 0.0

    bb = _check_bb_bottom(row, df)
    bb40_width = float(bb.get('bb40_width', 0) or 0)

    if idx == '코스피200':
        selected = 'ENV'
        reason = '기본=코스피200'
    elif idx == '코스닥150':
        selected = 'BB'
        reason = '기본=코스닥150'
    else:
        selected = 'BB'
        reason = '기본=비지수/변동성우선'

    if selected == 'ENV':
        if bb40_width >= BB_SWITCH_WIDTH:
            selected = 'BB'
            reason = f'예외전환=BB폭확대({bb40_width:.1f})'
        elif atr_pct >= BB_SWITCH_ATR:
            selected = 'BB'
            reason = f'예외전환=ATR확대({atr_pct:.1f}%)'
        elif amount20_b >= BB_SWITCH_AMOUNT20_B:
            selected = 'BB'
            reason = f'예외전환=거래대금활발({amount20_b:.1f}억)'
    elif selected == 'BB':
        if (
            bb40_width <= ENV_SWITCH_WIDTH
            and atr_pct <= ENV_SWITCH_ATR
            and amount20_b <= ENV_SWITCH_AMOUNT20_B
        ):
            selected = 'ENV'
            reason = f'예외전환=안정형(BB폭{bb40_width:.1f}/ATR{atr_pct:.1f}%)'

    return {
        'index_label': idx,
        'selected': selected,
        'reason': reason,
        'atr_pct': round(atr_pct, 1),
        'amount20_b': round(amount20_b, 1),
        'bb40_width': round(bb40_width, 1),
    }


def _get_band_recommendation(
    code: str,
    df: pd.DataFrame,
    row: pd.Series,
    index_label: str = '',
    is_top_mcap: bool = False,
) -> dict:
    close = float(row.get('Close', 0) or 0)
    atr = float(row.get('ATR', 0) or 0)
    atr_pct = (atr / close * 100) if close > 0 else 0.0

    bb = _check_bb_bottom(row, df)
    bb_width = float(bb.get('bb40_width', 0) or 0)
    amount_b_series = (df['Close'] * df['Volume']) / 1e8
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
        'reason': ', '.join(reason_parts),
        'comment': comment,
        'bb40_width': round(bb_width, 1),
        'atr_pct': round(atr_pct, 1),
        'amount20_b': round(amount20_b, 1),
    }


# =============================================================
# 유니버스 로딩
# =============================================================
def _get_index_tickers_naver(index_code: str) -> list:
    """
    네이버 금융에서 지수 구성 종목 코드 수집.
    index_code:
        'KOSPI200' -> 코스피200
        'KQ150'    -> 코스닥150
    """
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
    """
    pykrx에서 지수명으로 구성종목 가져오기.
    market:
        'KOSPI'  -> 코스피200
        'KOSDAQ' -> 코스닥150
    """
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


def _normalize_listing_df(df_krx: pd.DataFrame) -> pd.DataFrame:
    if df_krx is None or df_krx.empty:
        return pd.DataFrame()

    col_map = {}
    for c in df_krx.columns:
        cs = str(c).strip()
        if cs in ('Code', 'code', '티커', '종목코드'):
            col_map[c] = 'Code'
        elif cs in ('Name', 'name', '종목명'):
            col_map[c] = 'Name'
        elif cs in ('Amount', 'amount', '거래대금'):
            col_map[c] = 'Amount'
        elif cs in ('Market', 'market', '시장구분'):
            col_map[c] = 'Market'
        elif cs in ('Marcap', 'marcap', '시가총액', '시총'):
            col_map[c] = 'Marcap'

    df_krx = df_krx.rename(columns=col_map).copy()
    if 'Code' not in df_krx.columns:
        return pd.DataFrame()

    df_krx['Code'] = df_krx['Code'].astype(str).str.zfill(6)

    if 'Amount' in df_krx.columns:
        df_krx['Amount'] = pd.to_numeric(df_krx['Amount'], errors='coerce').fillna(0)
    else:
        df_krx['Amount'] = 0

    if 'Marcap' in df_krx.columns:
        df_krx['Marcap'] = pd.to_numeric(df_krx['Marcap'], errors='coerce').fillna(0)
    else:
        df_krx['Marcap'] = 0

    if 'Name' not in df_krx.columns:
        df_krx['Name'] = df_krx['Code']

    if 'Market' not in df_krx.columns:
        df_krx['Market'] = ''

    return df_krx


def _load_amount_top_universe(top_n: int = TOP_N) -> tuple[list, list]:
    df_krx = _normalize_listing_df(load_krx_listing_safe())
    if df_krx.empty:
        return [], []

    if 'Market' in df_krx.columns:
        df_krx = df_krx[
            df_krx['Market'].astype(str).isin(['KOSPI', 'KOSDAQ', '코스피', '코스닥', '유가'])
        ]

    if 'Name' in df_krx.columns:
        df_krx = df_krx[
            ~df_krx['Name'].astype(str).str.contains(
                r'ETF|ETN|스팩|제[0-9]+호|우$|우A|우B',
                na=False,
                regex=True,
            )
        ]

    df_krx = df_krx[
        (df_krx['Amount'] >= MIN_AMOUNT)
        & ((df_krx['Marcap'] >= MIN_MARCAP) | (df_krx['Marcap'] == 0))
    ].copy()

    if df_krx.empty:
        return [], []

    df_krx = df_krx.sort_values('Amount', ascending=False).head(top_n)
    codes = df_krx['Code'].astype(str).tolist()
    names = df_krx['Name'].astype(str).tolist()
    return codes, names


def _load_universe(mode: str = 'hybrid_union') -> list:
    global INDEX_MAP
    INDEX_MAP = {}

    log_info(f"유니버스 로딩: {mode}")

    kospi = _get_kospi200()
    kosdaq = _get_kosdaq150()
    top_codes, _ = _load_amount_top_universe(TOP_N)

    for c in kospi:
        INDEX_MAP[c] = '코스피200'
    for c in kosdaq:
        if c not in INDEX_MAP:
            INDEX_MAP[c] = '코스닥150'

    if mode == 'kospi200':
        codes = kospi
    elif mode == 'kospi200+kosdaq150':
        codes = list(dict.fromkeys(kospi + kosdaq))
    elif mode == 'amount_top400':
        codes = top_codes
    elif mode == 'hybrid_union':
        codes = list(dict.fromkeys(kospi + kosdaq + top_codes))
    elif mode == 'hybrid_intersection':
        idx_codes = list(dict.fromkeys(kospi + kosdaq))
        top_set = set(top_codes)
        codes = [c for c in idx_codes if c in top_set]
    else:
        log_error(f"⚠️ 알 수 없는 유니버스 모드: {mode}")
        return []

    log_info(f"✅ 유니버스: {len(codes)}개 종목")
    return codes


# =============================================================
# 시간 체크
# =============================================================
def _is_closing_time(force: bool = False) -> bool:
    if force:
        return True
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    start = SCAN_START_HOUR * 60 + SCAN_START_MIN
    end = SCAN_END_HOUR * 60 + SCAN_END_MIN
    return start <= t <= end


def _time_to_close() -> int:
    now = datetime.now(KST)
    close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return max(0, int((close - now).total_seconds() / 60))


# =============================================================
# 데이터 로딩 / 공통 정보
# =============================================================
def _load_df(code: str) -> pd.DataFrame | None:
    try:
        start = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')
        df = fdr.DataReader(code, start=start)
        if df is None or len(df) < 60:
            return None

        df = get_indicators(df)
        if df is None or df.empty:
            return None

        if 'NearHigh20_Pct' not in df.columns:
            df['High20'] = df['High'].rolling(20).max()
            df['NearHigh20_Pct'] = df['Close'] / df['High20'] * 100

        return df
    except Exception as e:
        log_debug(f"_load_df 실패 {code}: {e}")
        return None


def _base_info(row, df) -> dict:
    close = float(row['Close'])
    open_p = float(row['Open'])
    high = float(row['High'])
    low = float(row.get('Low', close))
    vol = float(row['Volume'])
    vma20 = float(row.get('VMA20', row.get('Vol_Avg', 0)) or 0)
    atr = float(row.get('ATR', 0) or 0)

    target1 = round(close + atr * 2) if atr > 0 else round(close * 1.05)
    stoploss = round(close - atr * 1.5) if atr > 0 else round(close * 0.97)
    rr = round((target1 - close) / (close - stoploss), 1) if close > stoploss else 0

    total = max(high - low, 1e-9)
    body_top = max(open_p, close)
    body_bot = min(open_p, close)
    body_size = max(abs(close - open_p), 1e-9)
    upper_wick_len = max(0.0, high - body_top)
    lower_wick_len = max(0.0, body_bot - low)

    upper_wick_body_pct = upper_wick_len / body_size * 100.0
    upper_wick_total_pct = upper_wick_len / total * 100.0
    lower_wick_body_pct = lower_wick_len / body_size * 100.0

    return {
        'close': int(close),
        'open': int(open_p),
        'high': int(high),
        'vol_ratio': round(vol / vma20, 1) if vma20 > 0 else 0,
        'wick_pct': round(upper_wick_body_pct, 1),
        'wick_pct_body': round(upper_wick_body_pct, 1),
        'wick_pct_total': round(upper_wick_total_pct, 1),
        'amount_b': round(close * vol / 1e8, 1),
        'atr': int(atr),
        'target1': target1,
        'stoploss': stoploss,
        'rr': rr,
        '_close': close,
        '_open': open_p,
        '_vol': vol,
        '_vma20': vma20,
        '_ma20': float(row.get('MA20', 0) or 0),
        '_disp': float(row.get('Disparity', 100) or 100),
        '_near20': float(row.get('NearHigh20_Pct', 0) or 0),
        '_upper_wick': _calc_upper_wick_ratio(row),
        '_upper_wick_body': upper_wick_len / body_size,
        '_upper_wick_total': upper_wick_len / total,
        '_lower_wick_body_pct': lower_wick_body_pct,
    }


def _build_maejip_chart(df: pd.DataFrame) -> str:
    if df is None or len(df) < 6:
        return ''

    recent = df.tail(6).copy()
    vma10 = float(df['Volume'].rolling(10).mean().iloc[-1]) or 1
    lines = ['최근 5일 매집 현황']

    rows = list(recent.iterrows())
    for idx, (_, row) in enumerate(rows[-5:], start=1):
        label = f"D-{5 - idx}" if idx < 5 else 'D-0(오늘)'
        close = float(row['Close'])
        open_p = float(row['Open'])
        vol = float(row['Volume'])
        pct = (close - open_p) / open_p * 100 if open_p > 0 else 0
        v_ratio = vol / vma10

        if pct > 0.3:
            candle = '양봉'
        elif pct < -0.3:
            candle = '음봉'
        else:
            candle = '도지'

        is_maejip = v_ratio > 1.0 and close > open_p
        maejip_mark = ' | 매집✅' if is_maejip else ''

        lines.append(
            f"{label:<9} {candle} {pct:+.1f}% | 거래량{v_ratio:.1f}배{maejip_mark}"
        )

    return '\n'.join(lines)


# =============================================================
# 전략 A / B1 / B2
# =============================================================
def _check_breakout_bet(code: str, name: str) -> dict | None:
    """
    전략 A — 전고점 돌파형 종가배팅
    """
    try:
        df = _load_df(code)
        if df is None:
            return None

        row = df.iloc[-1]
        info = _base_info(row, df)

        if info['_close'] < MIN_PRICE or info['amount_b'] < MIN_AMOUNT / 1e8:
            return None

        cond = {
            '①전고점85~100%': NEAR_HIGH20_MIN <= info['_near20'] <= NEAR_HIGH20_MAX,
            '②윗꼬리20%이하': info['_upper_wick_body'] <= UPPER_WICK_MAX,
            '③거래량2배폭발': info['_vma20'] > 0 and info['_vol'] >= info['_vma20'] * VOL_MULT,
            '④양봉마감': info['_close'] >= info['_open'],
            '⑤이격도98~112': DISPARITY_MIN <= info['_disp'] <= DISPARITY_MAX,
            '⑥MA20위마감': info['_ma20'] > 0 and info['_close'] >= info['_ma20'],
        }

        passed = [k for k, v in cond.items() if v]
        score = len(passed)
        if score < 4:
            return None

        band_rec = _get_band_recommendation(
            code=code,
            df=df,
            row=row,
            index_label=INDEX_MAP.get(code, ''),
            is_top_mcap=(code in TOP_MCAP_SET),
        )

        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'A',
            'mode_label': '돌파형',
            'index_label': INDEX_MAP.get(code, ''),
            'recommended_band': band_rec['recommended_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': band_rec['comment'],
            'band_recommend_reason': band_rec['reason'],
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'near20': round(info['_near20'], 1),
            'disp': round(info['_disp'], 1),
            'score': score,
            'grade': '완전체' if score == 6 else ('✅A급' if score == 5 else 'B급'),
            'passed': passed,
        }
    except Exception as e:
        log_debug(f"[A/{name}] {e}")
        return None


def _check_env_strict_bet(code: str, name: str) -> dict | None:
    """
    전략 B1 — ENV 엄격형 바닥 반등
    HTS 철학 그대로:
      - Env20 하단 2% 이내
      - Env40 하단 10% 이내
      - 동시 만족(AND)
    """
    try:
        df = _load_df(code)
        if df is None:
            return None

        row = df.iloc[-1]
        info = _base_info(row, df)

        idx = INDEX_MAP.get(code, '')
        if INDEX_MAP and not idx:
            return None

        if info['_close'] < MIN_PRICE:
            return None

        env = _check_envelope_bottom(row, df)
        rsi = float(row.get('RSI', 50) or 50)

        env_strict = env['env20_near'] and env['env40_near']
        if not env_strict:
            return None

        close = info['_close']
        open_p = info['_open']
        high = float(row.get('High', close))
        low = float(row.get('Low', close))
        vol = info['_vol']

        body_bot = min(close, open_p)
        body_top = max(close, open_p)
        body_size = max(body_top - body_bot, 1)
        lower_wick = body_bot - low
        upper_wick_len = high - body_top
        lower_wick_long = lower_wick > upper_wick_len
        close_to_high = (close / high * 100) if high > 0 else 0

        vma3 = float(df['Volume'].tail(3).mean())
        vma10 = float(df['Volume'].tail(10).mean())
        vol_drying = vma3 < vma10 * 0.85

        obv = (
            df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
            * df['Volume']
        ).cumsum()
        obv_ma5 = obv.rolling(5).mean()
        obv_ma10 = obv.rolling(10).mean()
        obv_rising = float(obv_ma5.iloc[-1]) > float(obv_ma10.iloc[-1])

        recent5 = df.tail(5)
        vma10_val = float(df['Volume'].rolling(10).mean().iloc[-1])
        maejip_5d = int(((recent5['Volume'] > vma10_val) & (recent5['Close'] > recent5['Open'])).sum())

        vma3_val = float(df['Volume'].tail(3).mean())
        vol_vs_3d = round(vol / vma3_val * 100, 1) if vma3_val > 0 else 0

        lower_wick_comment = '아랫꼬리↑' if lower_wick_long else '아랫꼬리↓'

        bonus = {
            '①Env20하단2%': env['env20_near'],
            '②Env40하단10%': env['env40_near'],
            '③RSI40이하': rsi <= 40,
            '④OBV매수세유입': obv_rising,
            '⑤5일내매집봉1회↑': maejip_5d >= 1,
            '⑥종가강도양호': (close >= open_p) or (close_to_high >= 95),
            '⑦윗꼬리25%이하': info['_upper_wick_body'] <= 0.25,
        }
        passed = [k for k, v in bonus.items() if v]
        score = len(passed)

        if score < 4:
            return None

        if score >= 6:
            grade = '완전체'
        elif score == 5:
            grade = '✅A급'
        else:
            grade = 'B급'

        env20_ma = float(_calc_envelope(df, 20, 20)['ma'].iloc[-1])
        target_env = round(env20_ma)
        maejip_chart = _build_maejip_chart(df)
        band_rec = _get_band_recommendation(
            code=code,
            df=df,
            row=row,
            index_label=INDEX_MAP.get(code, ''),
            is_top_mcap=(code in TOP_MCAP_SET),
        )

        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'B1',
            'mode_label': 'ENV엄격형',
            'index_label': INDEX_MAP.get(code, ''),
            'recommended_band': band_rec['recommended_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': band_rec['comment'],
            'band_recommend_reason': band_rec['reason'],
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'band_type': 'ENV',
            'band_reason': 'HTS엄격형(Env20&Env40 동시만족)',
            'band_pct_text': f"Env20:{env['env20_pct']:+.1f}% | Env40:{env['env40_pct']:+.1f}%",
            'env20_pct': env['env20_pct'],
            'env40_pct': env['env40_pct'],
            'lower20': env['lower20'],
            'lower40': env['lower40'],
            'rsi': round(rsi, 1),
            'obv_rising': obv_rising,
            'maejip_5d': maejip_5d,
            'vol_vs_3d': vol_vs_3d,
            'lower_wick_comment': lower_wick_comment,
            'lower_wick_pct': round(lower_wick / body_size * 100, 1),
            'upper_wick_pct': round(upper_wick_len / body_size * 100, 1),
            'target1': target_env,
            'score': score,
            'grade': grade,
            'passed': passed,
            'maejip_chart': maejip_chart,
            '_vol_drying': vol_drying,
        }
    except Exception as e:
        log_debug(f"[B1/{name}] {e}")
        return None


def _check_bb_expand_bet(code: str, name: str) -> dict | None:
    """
    전략 B2 — BB/확장형 하단 재안착
    """
    try:
        df = _load_df(code)
        if df is None:
            return None

        row = df.iloc[-1]
        info = _base_info(row, df)

        idx = INDEX_MAP.get(code, '')
        if INDEX_MAP and not idx:
            return None

        if info['_close'] < MIN_PRICE:
            return None

        bb = _check_bb_bottom(row, df)
        rsi = float(row.get('RSI', 50) or 50)

        if not bb['bb40_near']:
            return None

        close = info['_close']
        open_p = info['_open']
        high = float(row.get('High', close))
        low = float(row.get('Low', close))
        vol = info['_vol']

        body_bot = min(close, open_p)
        body_top = max(close, open_p)
        body_size = max(body_top - body_bot, 1)
        lower_wick = body_bot - low
        upper_wick_len = high - body_top
        lower_wick_long = lower_wick > upper_wick_len
        close_to_high = (close / high * 100) if high > 0 else 0

        vma3 = float(df['Volume'].tail(3).mean())
        vma10 = float(df['Volume'].tail(10).mean())
        vol_drying = vma3 < vma10 * 0.85

        obv = (
            df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
            * df['Volume']
        ).cumsum()
        obv_ma5 = obv.rolling(5).mean()
        obv_ma10 = obv.rolling(10).mean()
        obv_rising = float(obv_ma5.iloc[-1]) > float(obv_ma10.iloc[-1])

        recent5 = df.tail(5)
        vma10_val = float(df['Volume'].rolling(10).mean().iloc[-1])
        maejip_5d = int(((recent5['Volume'] > vma10_val) & (recent5['Close'] > recent5['Open'])).sum())

        vma3_val = float(df['Volume'].tail(3).mean())
        vol_vs_3d = round(vol / vma3_val * 100, 1) if vma3_val > 0 else 0

        band_meta = _choose_lower_band_type(code, df, row)
        lower_wick_comment = '아랫꼬리↑' if lower_wick_long else '아랫꼬리↓'

        bonus = {
            '①BB40하단근접': bb['bb40_near'],
            '②RSI45이하': rsi <= 45,
            '③OBV매수세유입': obv_rising,
            '④5일내매집봉1회↑': maejip_5d >= 1,
            '⑤종가강도양호': (close >= open_p) or (close_to_high >= 95),
            '⑥윗꼬리25%이하': info['_upper_wick_body'] <= 0.25,
            '⑦BB폭확대/변동성': (band_meta['bb40_width'] >= 14) or (band_meta['atr_pct'] >= 3.0),
        }
        passed = [k for k, v in bonus.items() if v]
        score = len(passed)

        if score < 4:
            return None

        if score >= 6:
            grade = '완전체'
        elif score == 5:
            grade = '✅A급'
        else:
            grade = 'B급'

        maejip_chart = _build_maejip_chart(df)
        band_rec = _get_band_recommendation(
            code=code,
            df=df,
            row=row,
            index_label=INDEX_MAP.get(code, ''),
            is_top_mcap=(code in TOP_MCAP_SET),
        )

        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'B2',
            'mode_label': 'BB확장형',
            'index_label': INDEX_MAP.get(code, ''),
            'recommended_band': band_rec['recommended_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': band_rec['comment'],
            'band_recommend_reason': band_rec['reason'],
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'band_type': 'BB',
            'band_reason': band_meta.get('reason', 'BB40하단재안착'),
            'band_pct_text': f"BB40:{bb['bb40_pct']:+.1f}% | BB폭:{bb['bb40_width']:.1f}%",
            'bb40_pct': bb['bb40_pct'],
            'bb40_width': bb['bb40_width'],
            'bb40_lower': bb['lower40'],
            'atr_pct': band_meta['atr_pct'],
            'amount20_b': band_meta['amount20_b'],
            'rsi': round(rsi, 1),
            'obv_rising': obv_rising,
            'maejip_5d': maejip_5d,
            'vol_vs_3d': vol_vs_3d,
            'lower_wick_comment': lower_wick_comment,
            'lower_wick_pct': round(lower_wick / body_size * 100, 1),
            'upper_wick_pct': round(upper_wick_len / body_size * 100, 1),
            'target1': bb['mid40'],
            'score': score,
            'grade': grade,
            'passed': passed,
            'maejip_chart': maejip_chart,
            '_vol_drying': vol_drying,
        }
    except Exception as e:
        log_debug(f"[B2/{name}] {e}")
        return None


def _check_closing_bet(code: str, name: str) -> dict | None:
    """
    A / B1 / B2 중 우선순위가 가장 높은 전략 1개 반환
    """
    a = _check_breakout_bet(code, name)
    b1 = _check_env_strict_bet(code, name)
    b2 = _check_bb_expand_bet(code, name)

    candidates = [x for x in [a, b1, b2] if x]
    if not candidates:
        return None

    def _priority(h):
        grade = h.get('grade', '')
        g_rank = 0 if '완전체' in grade else (1 if 'A급' in grade else 2)
        return (g_rank, -h.get('score', 0), -h.get('vol_ratio', 0), -h.get('amount_b', 0))

    candidates.sort(key=_priority)
    return candidates[0]


# =============================================================
# 검증 로그 / 다음날 성과 평가
# =============================================================
def _load_signal_log() -> pd.DataFrame:
    _ensure_log_dir()
    if SIGNAL_LOG_CSV.exists():
        try:
            df = pd.read_csv(
                SIGNAL_LOG_CSV,
                dtype={
                    'code': str,
                    'name': str,
                    'scan_date': str,
                    'scan_time': str,
                    'mode': str,
                    'mode_label': str,
                    'grade': str,
                    'index_label': str,
                    'band_type': str,
                    'band_reason': str,
                    'status': str,
                    'eval_date': str,
                },
                encoding='utf-8-sig',
            )
            if not df.empty and 'code' in df.columns:
                df['code'] = df['code'].astype(str).str.zfill(6)
            return df
        except Exception as e:
            log_error(f"⚠️ 검증 로그 로드 실패: {e}")
    return pd.DataFrame()


def _save_signal_log(df: pd.DataFrame):
    _ensure_log_dir()
    try:
        df.to_csv(SIGNAL_LOG_CSV, index=False, encoding='utf-8-sig')
    except Exception as e:
        log_error(f"⚠️ 검증 로그 저장 실패: {e}")


def _append_hits_to_validation_log(hits: list, scan_dt: datetime):
    if not hits:
        return

    rows = []
    scan_date = scan_dt.strftime('%Y-%m-%d')
    scan_time = scan_dt.strftime('%H:%M')

    for h in hits:
        rows.append({
            'key': f"{scan_date}|{h.get('code','')}|{h.get('mode','')}",
            'scan_date': scan_date,
            'scan_time': scan_time,
            'code': str(h.get('code', '')).zfill(6),
            'name': h.get('name', ''),
            'mode': h.get('mode', ''),
            'mode_label': h.get('mode_label', ''),
            'grade': h.get('grade', ''),
            'score': h.get('score', 0),
            'index_label': h.get('index_label', ''),
            'universe_tag': h.get('universe_tag', ''),
            'recommended_band': h.get('recommended_band', ''),
            'volatility_type': h.get('volatility_type', ''),
            'band_comment': h.get('band_comment', ''),
            'band_type': h.get('band_type', ''),
            'band_reason': h.get('band_reason', ''),
            'close_entry': _safe_float(h.get('close', 0)),
            'target1': _safe_float(h.get('target1', 0)),
            'stoploss': _safe_float(h.get('stoploss', 0)),
            'rr': _safe_float(h.get('rr', 0)),
            'vol_ratio': _safe_float(h.get('vol_ratio', 0)),
            'wick_pct': _safe_float(h.get('wick_pct', 0)),
            'amount_b': _safe_float(h.get('amount_b', 0)),
            'near20': _safe_float(h.get('near20', 0)),
            'disp': _safe_float(h.get('disp', 0)),
            'env20_pct': _safe_float(h.get('env20_pct', 0)),
            'env40_pct': _safe_float(h.get('env40_pct', 0)),
            'bb40_pct': _safe_float(h.get('bb40_pct', 0)),
            'bb40_width': _safe_float(h.get('bb40_width', 0)),
            'atr_pct': _safe_float(h.get('atr_pct', 0)),
            'amount20_b': _safe_float(h.get('amount20_b', 0)),
            'rsi': _safe_float(h.get('rsi', 0)),
            'maejip_5d': _safe_float(h.get('maejip_5d', 0)),
            'status': 'pending',
            'eval_date': '',
            'next_open': np.nan,
            'next_high': np.nan,
            'next_low': np.nan,
            'next_close': np.nan,
            'ret_open': np.nan,
            'ret_high': np.nan,
            'ret_low': np.nan,
            'ret_close': np.nan,
            'hit_plus2': np.nan,
            'hit_plus3': np.nan,
            'hit_plus5': np.nan,
            'close_win': np.nan,
            'stoploss_hit': np.nan,
            'target1_hit': np.nan,
        })

    new_df = pd.DataFrame(rows)
    old_df = _load_signal_log()

    if old_df.empty:
        merged = new_df
    else:
        if 'key' not in old_df.columns:
            old_df['key'] = (
                old_df['scan_date'].astype(str) + '|' +
                old_df['code'].astype(str).str.zfill(6) + '|' +
                old_df['mode'].astype(str)
            )
        merged = pd.concat([old_df, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=['key'], keep='last')

    merged = merged.sort_values(['scan_date', 'code', 'mode']).reset_index(drop=True)
    _save_signal_log(merged)
    log_info(f"✅ 검증로그 저장: {len(new_df)}건 추가")


def _get_next_trading_bar(code: str, scan_date: str) -> dict | None:
    """
    scan_date 다음 첫 거래일의 OHLC 반환.
    단, 그 다음 거래일이 오늘이고 아직 종가가 확정되지 않았으면 평가 보류.
    """
    try:
        now = datetime.now(KST)
        eval_ready = now.replace(hour=EVAL_READY_HOUR, minute=EVAL_READY_MIN, second=0, microsecond=0)

        base_date = pd.Timestamp(scan_date).date()
        start = (pd.Timestamp(scan_date) - pd.Timedelta(days=10)).strftime('%Y-%m-%d')
        end = (now + timedelta(days=5)).strftime('%Y-%m-%d')

        df = fdr.DataReader(code, start=start, end=end)
        if df is None or df.empty:
            return None

        df = df.sort_index()
        df = df[~df.index.duplicated(keep='last')]

        next_rows = df[df.index.date > base_date]
        if next_rows.empty:
            return None

        next_dt = next_rows.index[0]
        next_date = next_dt.date()

        if next_date == now.date() and now < eval_ready:
            return None

        row = next_rows.iloc[0]
        return {
            'eval_date': next_date.strftime('%Y-%m-%d'),
            'open': _safe_float(row.get('Open', 0)),
            'high': _safe_float(row.get('High', 0)),
            'low': _safe_float(row.get('Low', 0)),
            'close': _safe_float(row.get('Close', 0)),
        }
    except Exception as e:
        log_debug(f"_get_next_trading_bar 실패 {code} {scan_date}: {e}")
        return None


def _evaluate_pending_signals() -> int:
    df = _load_signal_log()
    if df.empty:
        log_info("검증 로그 없음")
        return 0

    if 'status' not in df.columns:
        df['status'] = 'pending'

    pending_idx = df.index[df['status'] != 'resolved'].tolist()
    if not pending_idx:
        log_info("미평가 후보 없음")
        return 0

    updated = 0
    for idx in pending_idx:
        row = df.loc[idx]
        code = str(row.get('code', '')).zfill(6)
        scan_date = str(row.get('scan_date', ''))
        entry = _safe_float(row.get('close_entry', 0))
        stoploss = _safe_float(row.get('stoploss', 0))
        target1 = _safe_float(row.get('target1', 0))

        if not code or not scan_date or entry <= 0:
            continue

        bar = _get_next_trading_bar(code, scan_date)
        if not bar:
            continue

        next_open = _safe_float(bar['open'])
        next_high = _safe_float(bar['high'])
        next_low = _safe_float(bar['low'])
        next_close = _safe_float(bar['close'])

        ret_open = (next_open / entry - 1) * 100 if entry > 0 else np.nan
        ret_high = (next_high / entry - 1) * 100 if entry > 0 else np.nan
        ret_low = (next_low / entry - 1) * 100 if entry > 0 else np.nan
        ret_close = (next_close / entry - 1) * 100 if entry > 0 else np.nan

        df.at[idx, 'eval_date'] = bar['eval_date']
        df.at[idx, 'next_open'] = round(next_open, 2)
        df.at[idx, 'next_high'] = round(next_high, 2)
        df.at[idx, 'next_low'] = round(next_low, 2)
        df.at[idx, 'next_close'] = round(next_close, 2)
        df.at[idx, 'ret_open'] = round(ret_open, 2)
        df.at[idx, 'ret_high'] = round(ret_high, 2)
        df.at[idx, 'ret_low'] = round(ret_low, 2)
        df.at[idx, 'ret_close'] = round(ret_close, 2)

        df.at[idx, 'hit_plus2'] = int(ret_high >= 2.0)
        df.at[idx, 'hit_plus3'] = int(ret_high >= 3.0)
        df.at[idx, 'hit_plus5'] = int(ret_high >= 5.0)
        df.at[idx, 'close_win'] = int(ret_close > 0)
        df.at[idx, 'stoploss_hit'] = int(next_low <= stoploss) if stoploss > 0 else np.nan
        df.at[idx, 'target1_hit'] = int(next_high >= target1) if target1 > 0 else np.nan
        df.at[idx, 'status'] = 'resolved'
        updated += 1

    if updated > 0:
        _save_signal_log(df)
        log_info(f"✅ 다음날 성과 평가 완료: {updated}건")
    else:
        log_info("평가 가능한 후보 없음")

    return updated


def _group_summary_lines(df: pd.DataFrame, group_cols: list[str], title: str) -> list[str]:
    if df.empty:
        return [f"[{title}] 데이터 없음"]

    lines = [f"[{title}]"]
    grouped = df.groupby(group_cols, dropna=False)

    for keys, sub in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)

        cnt = len(sub)
        avg_close = sub['ret_close'].mean()
        avg_high = sub['ret_high'].mean()
        avg_low = sub['ret_low'].mean()
        win_close = (sub['close_win'].fillna(0).mean() * 100) if 'close_win' in sub.columns else 0
        hit2 = (sub['hit_plus2'].fillna(0).mean() * 100) if 'hit_plus2' in sub.columns else 0
        hit3 = (sub['hit_plus3'].fillna(0).mean() * 100) if 'hit_plus3' in sub.columns else 0
        hit5 = (sub['hit_plus5'].fillna(0).mean() * 100) if 'hit_plus5' in sub.columns else 0
        stop_hit = (sub['stoploss_hit'].fillna(0).mean() * 100) if 'stoploss_hit' in sub.columns else 0

        key_str = " | ".join([str(k) if str(k) != '' else '-' for k in keys])
        lines.append(
            f"- {key_str}: {cnt}건 | 종가승률 {win_close:.1f}% | "
            f"+2%도달 {hit2:.1f}% | +3%도달 {hit3:.1f}% | +5%도달 {hit5:.1f}% | "
            f"평균 고가수익 {avg_high:.2f}% | 평균 종가수익 {avg_close:.2f}% | "
            f"평균 저가낙폭 {avg_low:.2f}% | 손절터치 {stop_hit:.1f}%"
        )

    return lines


def _build_validation_summary(last_n_days: int = 120) -> str:
    df = _load_signal_log()
    if df.empty:
        return "검증 로그가 없습니다."

    if 'status' not in df.columns:
        return "검증 로그 형식이 맞지 않습니다."

    df = df[df['status'] == 'resolved'].copy()
    if df.empty:
        return "아직 resolved 된 검증 데이터가 없습니다."

    if 'scan_date' in df.columns:
        df['scan_date_dt'] = pd.to_datetime(df['scan_date'], errors='coerce')
        cutoff = pd.Timestamp(datetime.now(KST).date()) - pd.Timedelta(days=last_n_days)
        df = df[df['scan_date_dt'] >= cutoff]

    if df.empty:
        return f"최근 {last_n_days}일 기준 resolved 데이터가 없습니다."

    total = len(df)
    avg_close = df['ret_close'].mean()
    avg_high = df['ret_high'].mean()
    avg_low = df['ret_low'].mean()
    win_close = df['close_win'].fillna(0).mean() * 100
    hit2 = df['hit_plus2'].fillna(0).mean() * 100
    hit3 = df['hit_plus3'].fillna(0).mean() * 100
    hit5 = df['hit_plus5'].fillna(0).mean() * 100
    stop_hit = df['stoploss_hit'].fillna(0).mean() * 100

    lines = []
    lines.append(f"종가배팅 검증 요약 (최근 {last_n_days}일)")
    lines.append(f"전체 {total}건")
    lines.append(
        f"전체 성과 | 종가승률 {win_close:.1f}% | +2%도달 {hit2:.1f}% | +3%도달 {hit3:.1f}% | "
        f"+5%도달 {hit5:.1f}% | 평균 고가수익 {avg_high:.2f}% | 평균 종가수익 {avg_close:.2f}% | "
        f"평균 저가낙폭 {avg_low:.2f}% | 손절터치 {stop_hit:.1f}%"
    )
    lines.append("")

    lines += _group_summary_lines(df, ['mode_label'], '전략별')
    lines.append("")
    lines += _group_summary_lines(df, ['mode_label', 'grade'], '전략+등급별')
    lines.append("")
    lines += _group_summary_lines(df, ['mode_label', 'index_label'], '전략+지수별')

    if 'band_type' in df.columns and df['band_type'].notna().any():
        lines.append("")
        lines += _group_summary_lines(df[df['band_type'] != ''], ['band_type'], '밴드별')
        lines.append("")
        lines += _group_summary_lines(df[df['band_type'] != ''], ['index_label', 'band_type'], '지수+밴드별')

    report = "\n".join(lines)

    _ensure_log_dir()
    try:
        SUMMARY_REPORT_TXT.write_text(report, encoding='utf-8')
    except Exception as e:
        log_error(f"⚠️ 요약 리포트 저장 실패: {e}")

    return report


# =============================================================
# 텔레그램 출력 포맷
# =============================================================
def _format_hit(hit: dict, rank: int, mins_left: int) -> str:
    passed_str = ' '.join(hit['passed'])
    mode_label = hit.get('mode_label', '')

    extra = ''
    if hit.get('mode') == 'A':
        extra = f"전고점:{hit.get('near20', 0)}% | 이격:{hit.get('disp', 0)}"
    else:
        vol3d = hit.get('vol_vs_3d', 0)
        vol3d_comment = (
            f"거래량소진({vol3d:.0f}%)" if vol3d < 85
            else f"거래량보통({vol3d:.0f}%)" if vol3d < 120
            else f"거래량증가({vol3d:.0f}%)"
        )
        extra = (
            f"밴드:{hit.get('band_type', '')} | "
            f"{hit.get('band_pct_text', '')} | "
            f"RSI:{hit.get('rsi', 0)} | "
            f"5일매집:{hit.get('maejip_5d', 0)}회 | "
            f"{'OBV↑' if hit.get('obv_rising') else 'OBV↓'} | "
            f"ATR:{hit.get('atr_pct', 0)}% | "
            f"20일평균거래대금:{hit.get('amount20_b', 0)}억 | "
            f"{vol3d_comment} | "
            f"{hit.get('lower_wick_comment', '')} "
            f"(아랫:{hit.get('lower_wick_pct', 0):.0f}% 윗:{hit.get('upper_wick_pct', 0):.0f}%) | "
            f"{hit.get('band_reason', '')}"
        )

    chart_str = ''
    if hit.get('mode') in ('B1', 'B2') and hit.get('maejip_chart'):
        chart_str = f"\n{hit['maejip_chart']}\n"

    idx_raw = hit.get('index_label', '')
    if idx_raw == '코스피200':
        idx_str = ' | 코스피200'
    elif idx_raw == '코스닥150':
        idx_str = ' | 코스닥150'
    else:
        idx_str = ' | 비지수'

    return (
        f"{'─' * 28}\n"
        f"{mode_label} {hit['grade']} [{hit['name']}({hit['code']})] {hit['close']:,}원{idx_str}\n"
        f"유형:{hit.get('universe_tag', '')} | 추천:{hit.get('recommended_band', '')} | {hit.get('volatility_type', '')}\n"
        f"코멘트:{hit.get('band_comment', '')}\n"
        f"✅ {passed_str}\n"
        f"거래량:{hit['vol_ratio']}배 | 윗꼬리(몸통):{hit['wick_pct']}% | {extra}\n"
        f"거래대금:{hit['amount_b']}억 | ATR:{hit['atr']:,}원\n"
        f"목표:{hit['target1']:,} | 손절:{hit['stoploss']:,} (RR {hit['rr']}){chart_str}\n"
        f"⏰ 마감까지 {mins_left}분\n"
    )


def _send_ai_comment(hits: list, mins_left: int, strategy: str = 'A'):
    try:
        if strategy == 'A':
            strategy_name = '돌파형(A)'
        elif strategy == 'B1':
            strategy_name = 'ENV엄격형(B1)'
        else:
            strategy_name = 'BB확장형(B2)'

        if strategy == 'A':
            data_lines = '\n'.join([
                f"- {h['name']}({h['code']}): 현재가={h['close']:,}원 | "
                f"거래량={h['vol_ratio']}배 | 전고점={h.get('near20', 0)}% | "
                f"이격={h.get('disp', 0)} | 윗꼬리={h['wick_pct']}% | "
                f"목표={h['target1']:,} 손절={h['stoploss']:,} | "
                f"지수={h.get('index_label', '')}"
                for h in hits
            ])
            strategy_context = (
                "전략 A는 전고점 돌파형 종가배팅이다. "
                "전고점 85~100% 구간에서 거래량이 터지고 종가가 강하게 잠기는 패턴이다."
            )
        elif strategy == 'B1':
            data_lines = '\n'.join([
                f"- {h['name']}({h['code']}): 현재가={h['close']:,}원 | "
                f"Env20={h.get('env20_pct', 0):+.1f}% | Env40={h.get('env40_pct', 0):+.1f}% | "
                f"RSI={h.get('rsi', 0)} | 5일매집={h.get('maejip_5d', 0)}회 | "
                f"OBV={'↑' if h.get('obv_rising') else '↓'} | "
                f"목표={h['target1']:,} 손절={h['stoploss']:,} | "
                f"지수={h.get('index_label', '')}"
                for h in hits
            ])
            strategy_context = (
                "전략 B1은 HTS와 같은 ENV 엄격형이다. "
                "Env20 하단 2% 이내와 Env40 하단 10% 이내를 동시에 만족하는 깊은 바닥 반등형이다."
            )
        else:
            data_lines = '\n'.join([
                f"- {h['name']}({h['code']}): 현재가={h['close']:,}원 | "
                f"BB40={h.get('bb40_pct', 0):+.1f}% | BB폭={h.get('bb40_width', 0):.1f}% | "
                f"RSI={h.get('rsi', 0)} | 5일매집={h.get('maejip_5d', 0)}회 | "
                f"OBV={'↑' if h.get('obv_rising') else '↓'} | "
                f"ATR={h.get('atr_pct', 0)}% | "
                f"목표={h['target1']:,} 손절={h['stoploss']:,} | "
                f"지수={h.get('index_label', '')}"
                for h in hits
            ])
            strategy_context = (
                "전략 B2는 BB40 확장형 하단 재안착 전략이다. "
                "변동성이 있는 종목이 볼린저밴드40 하단 근처에서 반등하는 종가베팅 전략이다."
            )

        system_msg = (
            "너는 단테 역매공파 매매법 전문가야. "
            "종가배팅 타점을 분석해줘. "
            "각 종목당 2문장으로 핵심만 간결하게. "
            "진입 추천/보류 판단을 반드시 포함해줘."
        )
        user_msg = (
            f"[{strategy_name} 종가배팅 후보 — 마감 {mins_left}분 전]\n\n"
            f"{strategy_context}\n\n"
            f"후보 종목:\n{data_lines}\n\n"
            f"각 종목별 진입 여부와 핵심 이유를 알려줘."
        )

        comment = ''

        if ANTHROPIC_API_KEY:
            try:
                res = requests.post(
                    'https://api.anthropic.com/v1/messages',
                    headers={
                        'Content-Type': 'application/json',
                        'x-api-key': ANTHROPIC_API_KEY,
                        'anthropic-version': '2023-06-01',
                    },
                    json={
                        'model': 'claude-sonnet-4-20250514',
                        'max_tokens': 800,
                        'system': system_msg,
                        'messages': [{'role': 'user', 'content': user_msg}],
                    },
                    timeout=30,
                )
                data = res.json()
                if 'content' in data and data['content']:
                    comment = data['content'][0].get('text', '').strip()
                    log_info(f"✅ Claude {strategy_name} 코멘트 완료")
            except Exception as e:
                log_error(f"⚠️ Claude 실패: {e}")

        if not comment and OPENAI_API_KEY:
            try:
                from openai import OpenAI as _OAI

                client = _OAI(api_key=OPENAI_API_KEY)
                res = client.chat.completions.create(
                    model='gpt-4o-mini',
                    messages=[
                        {'role': 'system', 'content': system_msg},
                        {'role': 'user', 'content': user_msg},
                    ],
                    max_tokens=800,
                )
                comment = res.choices[0].message.content.strip()
                log_info("✅ GPT 코멘트 완료")
            except Exception as e:
                log_error(f"⚠️ GPT 실패: {e}")

        if comment:
            emoji = '📈' if strategy == 'A' else '📉'
            send_telegram_chunks(
                f"{emoji} {strategy_name} AI 분석\n\n{comment}",
                max_len=3500,
            )
    except Exception as e:
        log_error(f"⚠️ AI 코멘트 실패: {e}")


def _send_results(hits: list, mins_left: int):
    log_info(f"_send_results 호출: {len(hits)}개 | TOKEN={'✅' if TELEGRAM_TOKEN else '❌'}")

    if not hits:
        log_info("→ 후보 없음 메시지 전송")
        send_telegram_photo(
            f"[{TODAY_STR}] 종가배팅 후보 없음\n"
            f"(대상: {SCAN_UNIVERSE} | 조건 미충족)",
            [],
        )
        return

    def _pick_top5(mode: str) -> list:
        pool = [h for h in hits if h.get('mode') == mode]

        def _priority(h):
            grade = h.get('grade', '')
            g_rank = 0 if '완전체' in grade else (1 if 'A급' in grade else 2)
            return (g_rank, -h.get('score', 0), -h.get('vol_ratio', 0), -h.get('amount_b', 0))

        pool.sort(key=_priority)
        return pool[:5]

    hits_a = _pick_top5('A')
    hits_b1 = _pick_top5('B1')
    hits_b2 = _pick_top5('B2')
    total = len(hits_a) + len(hits_b1) + len(hits_b2)

    log_info(f"돌파형 {len(hits_a)}개")
    log_info(f"ENV엄격형 {len(hits_b1)}개")
    log_info(f"BB확장형 {len(hits_b2)}개")

    header = (
        f"종가배팅 선별 TOP {total} ({TODAY_STR})\n"
        f"⏰ 마감까지 {mins_left}분\n"
        f"돌파형(A): {len(hits_a)}개 | ENV엄격형(B1): {len(hits_b1)}개 | BB확장형(B2): {len(hits_b2)}개\n"
    )
    send_telegram_photo(header, [])

    for title, pool in [
        ("── 돌파형(A) TOP5 ──", hits_a),
        ("── ENV엄격형(B1) TOP5 ──", hits_b1),
        ("── BB확장형(B2) TOP5 ──", hits_b2),
    ]:
        if not pool:
            continue

        send_telegram_photo(title, [])
        current_msg = ''
        max_char = 3800
        for hit in pool:
            entry = _format_hit(hit, 0, mins_left)
            if len(current_msg) + len(entry) > max_char:
                send_telegram_photo(current_msg, [])
                current_msg = entry
            else:
                current_msg += entry
        if current_msg.strip():
            send_telegram_photo(current_msg, [])

    if ANTHROPIC_API_KEY or OPENAI_API_KEY:
        if hits_a:
            _send_ai_comment(hits_a, mins_left, strategy='A')
        if hits_b1:
            _send_ai_comment(hits_b1, mins_left, strategy='B1')
        if hits_b2:
            _send_ai_comment(hits_b2, mins_left, strategy='B2')


# =============================================================
# 메인 스캔
# =============================================================
def run_closing_bet_scan(force: bool = False) -> list:
    now = datetime.now(KST)
    now_str = now.strftime('%H:%M')
    mins_left = _time_to_close()

    if not _is_closing_time(force):
        log_info(
            f"⏸️ 종가배팅 스캐너는 14:50~15:25에만 실행 (현재 {now_str})\n"
            f"테스트: python closing_bet_scanner.py --force"
        )
        return []

    log_info(f"\n{'=' * 55}")
    log_info(f"종가배팅 스캔 시작: {now_str} (마감 {mins_left}분 전)")
    log_info(f"{'=' * 55}")

    _refresh_top_mcap_set(TOP_N)

    if SCAN_UNIVERSE == 'amount_top400':
        codes, names = _load_amount_top_universe(TOP_N)
        if not codes:
            log_error("⚠️ amount_top400 유니버스 로드 실패")
            return []
    else:
        codes = _load_universe(SCAN_UNIVERSE)
        if not codes:
            log_error("⚠️ 유니버스 로드 실패")
            return []

        try:
            from pykrx import stock as _pk
            name_map = {}
            for c in codes[:500]:
                try:
                    name_map[c] = _pk.get_market_ticker_name(c)
                except Exception:
                    name_map[c] = c
            names = [name_map.get(c, c) for c in codes]
        except Exception:
            names = codes

    log_info(f"대상: {len(codes)}개 ({SCAN_UNIVERSE})")

    hits = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(_check_closing_bet, code, name): (code, name)
            for code, name in zip(codes, names)
        }
        done = 0
        for future in as_completed(futures, timeout=300):
            done += 1
            try:
                result = future.result(timeout=20)
                if result:
                    hits.append(result)
            except Exception:
                pass

            if done % 100 == 0:
                log_info(f"진행: {done}/{len(codes)} | 후보: {len(hits)}개")

    hits_a = [h for h in hits if h.get('mode') == 'A']
    hits_b1 = [h for h in hits if h.get('mode') == 'B1']
    hits_b2 = [h for h in hits if h.get('mode') == 'B2']

    hits_a.sort(key=lambda x: (x['score'], x['vol_ratio']), reverse=True)
    hits_b1.sort(key=lambda x: (x['score'], x['amount_b']), reverse=True)
    hits_b2.sort(key=lambda x: (x['score'], x['amount_b']), reverse=True)
    hits = hits_a + hits_b1 + hits_b2

    log_info(f"\n종가배팅 후보: {len(hits)}개")
    log_info(f"돌파형(A): {len(hits_a)}개 | ENV엄격형(B1): {len(hits_b1)}개 | BB확장형(B2): {len(hits_b2)}개")
    log_info(f"완전체: {sum(1 for h in hits if '완전체' in h.get('grade', ''))}개")
    log_info(f"✅A급: {sum(1 for h in hits if 'A급' in h.get('grade', ''))}개")
    log_info(f"B급: {sum(1 for h in hits if h.get('grade') == 'B급')}개")

    _send_results(hits, mins_left)
    _append_hits_to_validation_log(hits, now)
    return hits


# =============================================================
# 엔트리포인트
# =============================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='종가배팅 타점 스캐너')
    parser.add_argument('--force', action='store_true', help='시간 무관 강제 실행')
    parser.add_argument('--eval-pending', action='store_true', help='미평가 후보를 다음날 성과로 평가')
    parser.add_argument('--summary', action='store_true', help='검증 요약 출력')
    parser.add_argument('--send-summary', action='store_true', help='검증 요약을 텔레그램으로 전송')
    args = parser.parse_args()

    now = datetime.now(KST)
    log_info(f"종가배팅 스캐너 시작: {now.strftime('%H:%M')} (force={args.force})")
    log_info(f"TELEGRAM_TOKEN: {'✅' if TELEGRAM_TOKEN else '❌ 없음'}")
    log_info(f"CHAT_ID_LIST: {'✅ ' + str(CHAT_ID_LIST) if CHAT_ID_LIST else '❌ 없음'}")
    log_info(f"SCAN_UNIVERSE: {SCAN_UNIVERSE}")
    log_info(f"시간 체크: {'✅ 통과' if _is_closing_time(args.force) else '❌ 시간 외'}")

    if args.eval_pending:
        updated = _evaluate_pending_signals()
        log_info(f"평가 완료 건수: {updated}")

    if args.summary:
        summary_text = _build_validation_summary(last_n_days=120)
        log_info("\n" + summary_text)
        if args.send_summary and TELEGRAM_TOKEN:
            send_telegram_chunks(summary_text, max_len=3500)

    # 스캔 없이 평가/요약만 실행한 경우 빠져나갈 수 있게 처리
    run_scan = not (args.eval_pending or args.summary)

    if run_scan:
        if not _is_closing_time(args.force):
            log_info(f"⏸️ 종가배팅 유효 시간 아님 ({now.strftime('%H:%M')}) — 텔레그램 전송 안 함")
            log_info("유효 시간: 14:50~15:25 | 강제 실행: --force")
            sys.exit(0)

        hits = run_closing_bet_scan(force=args.force)
        if not hits:
            log_info("✅ 종가배팅 후보 없음")
            if TELEGRAM_TOKEN:
                send_telegram_photo(
                    f"[{TODAY_STR} {now.strftime('%H:%M')}] 종가배팅 후보 없음\n"
                    f"(대상: {SCAN_UNIVERSE} | 조건 미충족)",
                    [],
                )
                log_info("✅ '후보없음' 텔레그램 전송 완료")
        else:
            log_info(f"✅ 종가배팅 후보 {len(hits)}개 텔레그램 전송 완료")

    sys.exit(0)
