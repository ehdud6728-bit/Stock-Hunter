
# =============================================================
# closing_bet_scanner.py — 종가배팅 타점 스캐너 (G Morales v3.1 BACKTEST EXIT RULES 완성형)
# =============================================================
# 전략 구성
# A  : 돌파형 종가배팅
# B1 : ENV 엄격형 바닥 반등 (HTS 철학 유지)
# B2 : BB 확장형 하단 재안착
# G  : 길 모랄레스식 갭 돌파 종가매수형
# S  : 고점권 재응축 2차 슈팅 종가매수형
# C  : 역매공파 장기 저항 돌파형
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
import json
import argparse
import re
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
import threading
from functools import lru_cache

import numpy as np
import pandas as pd
import requests
import FinanceDataReader as fdr

CLOSING_BET_SCANNER_VERSION = 'G_MORALES_V3_3_CORE_TUNING_STRICT_C_20260510'


try:
    from pykrx import stock as pykrx_stock
except Exception:
    pykrx_stock = None

try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

try:
    from closing_bet_ai_debate_integration import run_closing_bet_debate_pipeline
except Exception as _debate_import_error:
    def run_closing_bet_debate_pipeline(*args, **kwargs):
        return {
            'judgment_rows': [],
            'telegram_text': '',
            'error': f'closing_bet_ai_debate_integration import failed: {_debate_import_error}',
        }

try:
    from main7_bugfix_2 import ANTHROPIC_API_KEY, OPENAI_API_KEY, GROQ_API_KEY, TODAY_STR, KST
except Exception:
    import pytz
    KST = pytz.timezone('Asia/Seoul')
    TODAY_STR = datetime.now(KST).strftime('%Y-%m-%d')
    ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
    GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')

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

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')
CLOSING_BET_DEBATE_TOP_N = int(os.environ.get('CLOSING_BET_DEBATE_TOP_N', '7'))

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
MCAP_OR_MIN = 200_000_000_000   # 시총 2000억 이상이면 지수 밖이어도 포함
TOP_N = 400

# 유니버스
# 'kospi200+kosdaq150' : 코스피200 + 코스닥150
# 'amount_top400'      : 거래대금 상위 400개
# 'kospi200'           : 코스피200만
# 'hybrid_union'       : 코스피200 + 코스닥150 + 시총상위 합집합
# 'hybrid_intersection': 지수유니버스 ∩ 시총상위 교집합
SCAN_UNIVERSE = 'hybrid_union'

MAX_WORKERS = int(os.environ.get('CLOSING_BET_MAX_WORKERS', '12'))
SCAN_FUTURES_TIMEOUT = int(os.environ.get('CLOSING_BET_FUTURES_TIMEOUT', '540'))

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

# 전략 G — 길 모랄레스식 갭 돌파 종가매수형
# 핵심 철학: 아무 갭이나 사지 않고, 거래량/갭지지/위치/과열제외를 모두 통과한 갭만 종가 후보로 본다.
GAP_MIN_PCT = float(os.environ.get('CLOSING_BET_GAP_MIN_PCT', '2.0'))
GAP_MAX_PCT = float(os.environ.get('CLOSING_BET_GAP_MAX_PCT', '12.0'))
GAP_VOL50_MULT = float(os.environ.get('CLOSING_BET_GAP_VOL50_MULT', '1.5'))
GAP_LOW_KEEP_PCT = float(os.environ.get('CLOSING_BET_GAP_LOW_KEEP_PCT', '0.5'))
GAP_CLOSE_OPEN_KEEP = float(os.environ.get('CLOSING_BET_GAP_CLOSE_OPEN_KEEP', '0.995'))
GAP_BOX_LOOKBACK = int(os.environ.get('CLOSING_BET_GAP_BOX_LOOKBACK', '60'))
GAP_HIGH_LOOKBACK = int(os.environ.get('CLOSING_BET_GAP_HIGH_LOOKBACK', '120'))
GAP_DISPARITY20_MAX = float(os.environ.get('CLOSING_BET_GAP_DISPARITY20_MAX', '118.0'))
GAP_RUNUP20_MAX = float(os.environ.get('CLOSING_BET_GAP_RUNUP20_MAX', '35.0'))
GAP_UPPER_WICK_MAX = float(os.environ.get('CLOSING_BET_GAP_UPPER_WICK_MAX', '0.25'))
GAP_LARGE_CAP_MARCAP = float(os.environ.get('CLOSING_BET_GAP_LARGE_CAP_MARCAP', '5000000000000'))  # 5조

# 전략 S — 고점권 재응축 2차 슈팅 종가매수형
# 핵심 철학: 이미 강하게 오른 주도주가 고점 부근에서 무너지지 않고, 종가가 고점권에서 잠길 때 2차 슈팅 후보로 본다.
HIGH_REACCUM_LOOKBACK = int(os.environ.get('CLOSING_BET_HIGH_REACCUM_LOOKBACK', '120'))
HIGH_REACCUM_RUNUP_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_RUNUP_MIN', '80.0'))
HIGH_REACCUM_NEAR_HIGH_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_NEAR_HIGH_MIN', '85.0'))
HIGH_REACCUM_NEAR_HIGH_MAX = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_NEAR_HIGH_MAX', '101.5'))
HIGH_REACCUM_MAX_PULLBACK = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_MAX_PULLBACK', '22.0'))
HIGH_REACCUM_RSI_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_RSI_MIN', '45.0'))
HIGH_REACCUM_RSI_MAX = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_RSI_MAX', '72.0'))
HIGH_REACCUM_CLOSE_LOC_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_CLOSE_LOC_MIN', '65.0'))
HIGH_REACCUM_UPPER_WICK_RANGE_MAX = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_UPPER_WICK_RANGE_MAX', '35.0'))
HIGH_REACCUM_CLOSE_OPEN_KEEP = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_CLOSE_OPEN_KEEP', '0.99'))
HIGH_REACCUM_VMA5_20_MAX = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_VMA5_20_MAX', '1.25'))
HIGH_REACCUM_TODAY_VOL_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_TODAY_VOL_MIN', '0.50'))
HIGH_REACCUM_DISPARITY20_MAX = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_DISPARITY20_MAX', '125.0'))
HIGH_REACCUM_RUNUP20_MAX = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_RUNUP20_MAX', '60.0'))
HIGH_REACCUM_SCORE_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_SCORE_MIN', '75.0'))
# v2.7: S전략 실전 필터 — 목표 공간/RR/유동성/거래량 상태 보정
HIGH_REACCUM_RR_EXCLUDE_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_RR_EXCLUDE_MIN', '0.30'))
HIGH_REACCUM_RR_GOOD_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_RR_GOOD_MIN', '0.70'))
HIGH_REACCUM_AMOUNT_GOOD_B = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_AMOUNT_GOOD_B', '100.0'))
HIGH_REACCUM_TODAY_VOL_GOOD = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_TODAY_VOL_GOOD', '1.50'))
HIGH_REACCUM_VOLUME_DRY_MAX = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_VOLUME_DRY_MAX', '0.85'))
HIGH_REACCUM_VOLUME_NORMAL_MAX = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_VOLUME_NORMAL_MAX', '1.20'))

# v2.8: S전략을 관찰형(S1)과 실행형(S2)으로 분리
HIGH_REACCUM_EXEC_VOL_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_EXEC_VOL_MIN', '1.20'))
HIGH_REACCUM_EXEC_AMOUNT_MIN_B = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_EXEC_AMOUNT_MIN_B', '100.0'))
HIGH_REACCUM_EXEC_CLOSE_LOC_MIN = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_EXEC_CLOSE_LOC_MIN', '70.0'))
HIGH_REACCUM_EXEC_WICK_MAX = float(os.environ.get('CLOSING_BET_HIGH_REACCUM_EXEC_WICK_MAX', '30.0'))

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
BACKTEST_SUMMARY_TXT = LOG_DIR / "closing_bet_backtest_summary.txt"
BACKTEST_DEBUG_TXT = LOG_DIR / "closing_bet_backtest_debug.txt"
FLOW_SNAPSHOT_CSV = LOG_DIR / "closing_bet_flow_snapshots.csv"
JSON_KEY_PATH = str(Path(__file__).resolve().parent / 'stock-key.json')
AI_GSHEET_NAME = '사령부_통합_상황판'
AI_JUDGMENT_TAB_NAME = '종가배팅_AI판정'

# 다음날 성과 평가 가능 시간
EVAL_READY_HOUR = 16
EVAL_READY_MIN = 10

# 전역 지수 소속 맵 / 시총상위 맵 / 시총 맵
INDEX_MAP: dict = {}
TOP_MCAP_SET: set = set()
MARCAP_MAP: dict = {}

STRATEGY_DIAG = {
    'A_try': 0, 'A_hit': 0,
    'B1_try': 0, 'B1_hit': 0,
    'B2_try': 0, 'B2_hit': 0,
    'G_try': 0, 'G_hit': 0,
    'S_try': 0, 'S_hit': 0,
    'C_try': 0, 'C_hit': 0,
}
STRATEGY_FAIL = {
    'A_no_df': 0, 'A_universe': 0, 'A_price_amount': 0, 'A_score': 0,
    'B1_no_df': 0, 'B1_universe': 0, 'B1_price': 0, 'B1_env_strict': 0, 'B1_score': 0,
    'B2_no_df': 0, 'B2_universe': 0, 'B2_price': 0, 'B2_bb40': 0, 'B2_score': 0,
    'G_no_df': 0, 'G_universe': 0, 'G_price_amount': 0, 'G_gap': 0, 'G_volume': 0,
    'G_support': 0, 'G_location': 0, 'G_climax': 0, 'G_score': 0,
    'S_no_df': 0, 'S_universe': 0, 'S_price_amount': 0, 'S_runup': 0, 'S_position': 0,
    'S_trend': 0, 'S_close_strength': 0, 'S_climax': 0, 'S_score': 0,
    'C_no_df': 0, 'C_universe': 0, 'C_price_amount': 0, 'C_pattern': 0,
}
DIAG_LOCK = threading.Lock()


# =============================================================
# 유틸
# =============================================================
def _build_universe_tag(index_label: str = '', is_top_mcap: bool = False, is_mcap_or: bool = False) -> str:
    tags = []
    if index_label == '코스피200':
        tags.append('K200')
    elif index_label == '코스닥150':
        tags.append('KQ150')
    if is_top_mcap:
        tags.append('MCAP')
    if is_mcap_or:
        tags.append('MCAP2000+')
    return '+'.join(tags) if tags else 'OTHER'


def _refresh_top_mcap_set(top_n: int = TOP_N):
    global TOP_MCAP_SET
    try:
        codes, _ = _load_amount_top_universe(top_n)
        TOP_MCAP_SET = set(codes)
    except Exception:
        TOP_MCAP_SET = set()


def _refresh_marcap_map():
    global MARCAP_MAP
    MARCAP_MAP = {}

    try:
        listing = fdr.StockListing("KRX")
        if listing is None or listing.empty:
            log_error("⚠️ KRX listing 비어 있음")
            return

        log_info(f"KRX listing rows: {len(listing)}")
        log_info(f"KRX listing cols: {list(listing.columns)}")

        code_col = "Code" if "Code" in listing.columns else ("Symbol" if "Symbol" in listing.columns else None)

        marcap_col = None
        for c in ["Marcap", "MarCap", "marcap", "MarketCap", "Market_Cap", "시가총액"]:
            if c in listing.columns:
                marcap_col = c
                break

        if code_col is None:
            log_error("⚠️ 시총 맵 로딩 실패: code 컬럼 없음")
            return
        if marcap_col is None:
            log_error("⚠️ 시총 맵 로딩 실패: marcap 컬럼 없음")
            return

        work = listing[[code_col, marcap_col]].copy()
        work[code_col] = work[code_col].astype(str).map(_normalize_code)
        work[marcap_col] = pd.to_numeric(work[marcap_col], errors="coerce").fillna(0)

        MARCAP_MAP = dict(zip(work[code_col], work[marcap_col]))

        log_info(f"시총 맵 로딩 완료: {len(MARCAP_MAP)}개")
        sample_items = list(MARCAP_MAP.items())[:5]
        log_info(f"시총 맵 샘플: {sample_items}")

    except Exception as e:
        log_error(f"⚠️ 시총 맵 로딩 실패: {e}")
        MARCAP_MAP = {}


def _is_universe_allowed(code: str) -> bool:
    code = _normalize_code(code)

    idx = str(INDEX_MAP.get(code, "") or "").strip()
    marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)

    is_index_member = idx in ("코스피200", "코스닥150")
    is_mcap_or = marcap >= MCAP_OR_MIN

    return bool(is_index_member or is_mcap_or)


def _normalize_code(code) -> str:
    try:
        s = str(code).strip()
        s = s.replace(".0", "")
        s = "".join(ch for ch in s if ch.isdigit())
        return s.zfill(6)
    except Exception:
        return str(code).strip()


def _get_krx_listing() -> pd.DataFrame:
    try:
        listing = fdr.StockListing('KRX')
        if listing is None or listing.empty:
            return pd.DataFrame(columns=['Code','Name'])
        out = listing.copy()
        if 'Code' not in out.columns and 'Symbol' in out.columns:
            out['Code'] = out['Symbol']
        if 'Name' not in out.columns and '종목명' in out.columns:
            out['Name'] = out['종목명']
        if 'Code' not in out.columns:
            return pd.DataFrame(columns=['Code','Name'])
        out['Code'] = out['Code'].astype(str).str.zfill(6)
        if 'Name' not in out.columns:
            out['Name'] = out['Code']
        return out
    except Exception as e:
        log_error(f"⚠️ KRX listing 로드 실패: {e}")
        return pd.DataFrame(columns=['Code','Name'])


def _load_index_members(index_name: str) -> tuple[list, list]:
    global INDEX_MAP
    code_map = {'kospi200': '1028', '코스피200': '1028', 'kosdaq150': '2203', '코스닥150': '2203'}
    idx_code = code_map.get(index_name, '')
    if not idx_code:
        return [], []

    label = '코스피200' if idx_code == '1028' else '코스닥150'
    codes = []

    if pykrx_stock is None:
        log_error(f"⚠️ {label} 구성종목 로드 실패: pykrx import 실패")
        return [], []

    try:
        raw = pykrx_stock.get_index_portfolio_deposit_file(idx_code)
        codes = [_normalize_code(c) for c in raw if _normalize_code(c)]
        log_info(f"{label} 구성종목 로드: {len(codes)}개")
        log_info(f"{label} 샘플: {codes[:10]}")
    except Exception as e:
        log_error(f"⚠️ {label} 구성종목 로드 실패: {e}")
        return [], []

    listing = _get_krx_listing()
    name_map = dict(zip(listing['Code'], listing['Name'])) if not listing.empty else {}
    names = [name_map.get(c, c) for c in codes]
    for c in codes:
        INDEX_MAP[c] = label
    return codes, names


def _load_amount_top_universe(top_n: int = TOP_N) -> tuple[list, list]:
    listing = _get_krx_listing()
    if listing.empty:
        return [], []
    amount_col = None
    # 최근 거래대금 컬럼은 listing에 없는 경우가 많아서 시총 대체 fallback
    for c in ['Marcap', 'MarCap', 'marcap', 'MarketCap', 'Market_Cap', '시가총액']:
        if c in listing.columns:
            amount_col = c
            break
    if amount_col is None:
        codes = listing['Code'].astype(str).head(top_n).tolist()
        names = listing['Name'].astype(str).head(top_n).tolist()
        return codes, names
    work = listing[['Code','Name', amount_col]].copy()
    work[amount_col] = pd.to_numeric(work[amount_col], errors='coerce').fillna(0)
    work = work.sort_values(amount_col, ascending=False).head(top_n)
    return work['Code'].astype(str).tolist(), work['Name'].astype(str).tolist()


def _refresh_index_map():
    global INDEX_MAP
    INDEX_MAP = {}

    try:
        k200, _ = _load_index_members('코스피200')
        for code in k200:
            INDEX_MAP[_normalize_code(code)] = '코스피200'
    except Exception as e:
        log_error(f"⚠️ 코스피200 INDEX_MAP 반영 실패: {e}")

    try:
        kq150, _ = _load_index_members('코스닥150')
        for code in kq150:
            INDEX_MAP[_normalize_code(code)] = '코스닥150'
    except Exception as e:
        log_error(f"⚠️ 코스닥150 INDEX_MAP 반영 실패: {e}")

    log_info(f"INDEX_MAP 로딩 완료: {len(INDEX_MAP)}개")
    log_info(f"INDEX_MAP 샘플: {list(INDEX_MAP.items())[:10]}")


def _load_universe(universe_name: str) -> list:
    global INDEX_MAP
    INDEX_MAP = {}
    universe_name = str(universe_name or '').strip()

    k200_codes, _ = _load_index_members('코스피200')
    kq150_codes, _ = _load_index_members('코스닥150')
    top_codes, _ = _load_amount_top_universe(TOP_N)

    if universe_name == 'kospi200':
        codes = k200_codes
    elif universe_name == 'kospi200+kosdaq150':
        codes = list(dict.fromkeys(k200_codes + kq150_codes))
    elif universe_name == 'hybrid_intersection':
        idx_set = set(k200_codes) | set(kq150_codes)
        codes = [c for c in top_codes if c in idx_set]
    else:
        listing = _get_krx_listing()
        mcap_codes = []
        if not listing.empty:
            mcap_col = None
            for c in ['Marcap', 'MarCap', 'marcap', 'MarketCap', 'Market_Cap', '시가총액']:
                if c in listing.columns:
                    mcap_col = c
                    break
            if mcap_col is not None:
                work = listing[['Code', mcap_col]].copy()
                work['Code'] = work['Code'].astype(str).map(_normalize_code)
                work[mcap_col] = pd.to_numeric(work[mcap_col], errors='coerce').fillna(0)
                mcap_codes = work.loc[work[mcap_col] >= MCAP_OR_MIN, 'Code'].astype(str).tolist()
        codes = list(dict.fromkeys(k200_codes + kq150_codes + mcap_codes))

    codes = [_normalize_code(c) for c in codes if str(c).strip()]
    codes = sorted(set(codes))

    log_info(f"INDEX_MAP 로딩 완료: {len(INDEX_MAP)}개")
    log_info(f"INDEX_MAP 샘플: {list(INDEX_MAP.items())[:10]}")
    log_info(f"_load_universe({universe_name}) -> {len(codes)}개")
    log_info(f"_load_universe 샘플: {codes[:10]}")

    return codes


def _ensure_log_dir():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

def _safe_float(v, default=0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v, default=0) -> int:
    try:
        if pd.isna(v):
            return default
        return int(float(v))
    except Exception:
        return default



def _calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    diff = series.diff()
    up = diff.clip(lower=0)
    down = -diff.clip(upper=0)
    avg_up = up.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_down = down.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_up / avg_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


@lru_cache(maxsize=4096)
def _load_df(code: str, lookback_days: int = 730) -> pd.DataFrame:
    """전략 공용 데이터 로더: FDR 일봉 + 핵심 지표 계산"""
    code = _normalize_code(code)
    try:
        end_dt = _now_kst().date() if '_now_kst' in globals() else datetime.now().date()
    except Exception:
        end_dt = datetime.now().date()
    start_dt = end_dt - timedelta(days=lookback_days)

    try:
        df = fdr.DataReader(code, start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))
    except Exception as e:
        log_error(f"_load_df 실패 [{code}]: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    try:
        df = df.copy()
        rename_map = {}
        for c in df.columns:
            cl = str(c).strip().lower()
            if cl == 'open':
                rename_map[c] = 'Open'
            elif cl == 'high':
                rename_map[c] = 'High'
            elif cl == 'low':
                rename_map[c] = 'Low'
            elif cl == 'close':
                rename_map[c] = 'Close'
            elif cl == 'volume':
                rename_map[c] = 'Volume'
        if rename_map:
            df = df.rename(columns=rename_map)

        required = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in required:
            if col not in df.columns:
                log_error(f"_load_df 컬럼부족 [{code}]: {list(df.columns)}")
                return pd.DataFrame()
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.dropna(subset=['Open', 'High', 'Low', 'Close']).copy()
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
        if 'Amount' not in df.columns:
            df['Amount'] = df['Close'] * df['Volume']
        else:
            df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(df['Close'] * df['Volume'])

        df['MA5'] = df['Close'].rolling(5).mean()
        df['MA10'] = df['Close'].rolling(10).mean()
        df['MA20'] = df['Close'].rolling(20).mean()
        df['MA50'] = df['Close'].rolling(50).mean()
        df['MA112'] = df['Close'].rolling(112).mean()
        df['MA224'] = df['Close'].rolling(224).mean()
        df['VMA20'] = df['Volume'].rolling(20).mean()
        df['VMA50'] = df['Volume'].rolling(50).mean()
        tr1 = df['High'] - df['Low']
        tr2 = (df['High'] - df['Close'].shift(1)).abs()
        tr3 = (df['Low'] - df['Close'].shift(1)).abs()
        df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['ATR'] = df['TR'].rolling(14).mean()
        df['RSI'] = _calc_rsi(df['Close'], 14)
        direction = np.sign(df['Close'].diff().fillna(0))
        df['OBV'] = (direction * df['Volume']).cumsum()

        return df.reset_index(drop=True)
    except Exception as e:
        log_error(f"_load_df 후처리 실패 [{code}]: {e}")
        return pd.DataFrame()


def _base_info(row: pd.Series, df: pd.DataFrame) -> dict:
    close = _safe_float(row.get('Close', 0), 0.0)
    open_p = _safe_float(row.get('Open', 0), 0.0)
    high = _safe_float(row.get('High', close), close)
    low = _safe_float(row.get('Low', close), close)
    vol = _safe_float(row.get('Volume', 0), 0.0)
    ma20 = _safe_float(row.get('MA20', 0), 0.0)
    vma20 = _safe_float(row.get('VMA20', 0), 0.0)
    amount_b = _safe_float(row.get('Amount', close * vol), close * vol) / 1e8

    recent20_high = _safe_float(df['High'].tail(20).max(), high) if df is not None and len(df) else high
    near20 = (close / recent20_high * 100.0) if recent20_high > 0 else 0.0
    disp = (close / ma20 * 100.0) if ma20 > 0 else 0.0

    upper_wick_body = _calc_upper_wick_body_ratio(row)
    vol_ratio = (vol / vma20) if vma20 > 0 else 0.0
    atr = _safe_float(row.get('ATR', 0), 0.0)

    stoploss = low
    default_target = recent20_high if recent20_high > close else close * 1.03
    risk = close - stoploss
    reward = default_target - close
    rr = (reward / risk) if risk > 0 and reward > 0 else 0.0

    return {
        '_close': close,
        '_open': open_p,
        '_high': high,
        '_low': low,
        '_vol': vol,
        '_ma20': ma20,
        '_vma20': vma20,
        '_near20': near20,
        '_disp': disp,
        '_upper_wick_body': upper_wick_body,
        'amount_b': amount_b,
        'vol_ratio': round(vol_ratio, 2),
        'wick_pct': round(upper_wick_body * 100, 1),
        'atr': round(atr, 2),
        'stoploss': round(stoploss) if stoploss > 0 else 0,
        'target1': round(default_target) if default_target > 0 else 0,
        'rr': round(rr, 2),
    }


def _build_maejip_chart(df: pd.DataFrame) -> str:
    """간단 매집 차트 텍스트 요약"""
    try:
        if df is None or df.empty or len(df) < 5:
            return ''
        recent5 = df.tail(5)
        vma10_val = float(df['Volume'].rolling(10).mean().iloc[-1]) if len(df) >= 10 else float(df['Volume'].mean())
        cnt = int(((recent5['Volume'] > vma10_val) & (recent5['Close'] > recent5['Open'])).sum())
        return f'최근5일 매집봉 {cnt}회'
    except Exception:
        return ''

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
# Google Sheets 저장 (AI 판정)
# =============================================================
def _get_gspread_client():
    if not HAS_GSPREAD:
        log_info("⚠️ gspread 미설치 → AI 판정 시트 저장 생략")
        return None, None

    log_info(f"JSON exists={os.path.exists(JSON_KEY_PATH)}")
    log_info(f"GOOGLE_JSON_KEY exists={'YES' if os.environ.get('GOOGLE_JSON_KEY') else 'NO'}")

    key_path = JSON_KEY_PATH
    if not os.path.exists(key_path):
        json_key = os.environ.get('GOOGLE_JSON_KEY', '')
        if json_key:
            try:
                Path(key_path).write_text(json_key, encoding='utf-8')
            except Exception as e:
                log_error(f"⚠️ GOOGLE_JSON_KEY 파일화 실패: {e}")

    if not os.path.exists(key_path):
        log_info("⚠️ 구글시트 인증 없음")
        return None, None

    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(key_path, scope)
        gc = gspread.authorize(creds)
        doc = gc.open(AI_GSHEET_NAME)
        log_info(f"✅ 구글시트 문서 연결 성공: {AI_GSHEET_NAME}")
        return gc, doc
    except Exception as e:
        log_error(f"⚠️ 구글시트 연결 실패: {e}")
        return None, None


def _upsert_tab(doc, tab_name: str, df: pd.DataFrame):
    if df is None or df.empty:
        log_info(f"⚠️ [{tab_name}] 저장할 데이터 없음")
        return

    values = [df.columns.tolist()] + df.astype(object).fillna('').values.tolist()
    rows = max(len(values), 2)
    cols = max(len(df.columns), 2)

    try:
        try:
            ws = doc.worksheet(tab_name)
            ws.clear()
        except Exception:
            ws = doc.add_worksheet(title=tab_name, rows=rows, cols=cols)

        ws.update(values, value_input_option='USER_ENTERED')
        log_info(f"✅ [{tab_name}] {len(df)}행 저장")
    except Exception as e:
        log_error(f"❌ [{tab_name}] 저장 실패: {e}")


def _save_ai_judgments_to_gsheet(judgment_rows: list):
    if not judgment_rows:
        log_info("⚠️ 저장할 AI 판정 없음")
        return

    gc, doc = _get_gspread_client()
    if doc is None:
        log_info("⚠️ AI 판정 구글시트 저장 생략")
        return

    try:
        df = pd.DataFrame(judgment_rows).copy()
        if df.empty:
            log_info("⚠️ 저장할 AI 판정 DataFrame 비어있음")
            return

        now_str = datetime.now(KST).strftime('%Y-%m-%d %H:%M')
        if 'saved_at' not in df.columns:
            df.insert(0, 'saved_at', now_str)

        preferred = [
            'saved_at', 'scan_date', 'scan_time', 'code', 'name', 'mode', 'mode_label', 'grade',
            'final_verdict', 'final_confidence', 'judge_provider', 'judge_summary',
            'tech_provider', 'tech_view', 'flow_provider', 'flow_view',
            'theme_provider', 'theme_view', 'risk_provider', 'risk_view',
            'positive_votes', 'negative_votes',
            'recommended_band', 'volatility_type', 'universe_tag',
            'index_label', 'band_comment', 'band_recommend_reason'
        ]
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        df = df[cols]
        _upsert_tab(doc, AI_JUDGMENT_TAB_NAME, df)
    except Exception as e:
        log_error(f"⚠️ AI 판정 시트 저장 실패: {e}")


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
    # HTS 설정 기준: 엔벨로프(20,10), 엔벨로프(40,10)
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
    is_mcap_or: bool = False,
) -> dict:
    close = float(row.get('Close', 0) or 0)
    atr = float(row.get('ATR', 0) or 0)
    atr_pct = (atr / close * 100) if close > 0 else 0.0

    bb = _check_bb_bottom(row, df)
    env = _check_envelope_bottom(row, df)

    bb40_width = float(bb.get('bb40_width', 0) or 0)
    bb40_pct = float(bb.get('bb40_pct', 999) or 999)
    env20_pct = float(env.get('env20_pct', 999) or 999)
    env40_pct = float(env.get('env40_pct', 999) or 999)

    amount_b_series = (df['Close'] * df['Volume']) / 1e8
    amount20_b = float(amount_b_series.rolling(20).mean().iloc[-1]) if len(amount_b_series) >= 20 else 0.0

    candidates = []

    try:
        bb20 = _calc_bollinger(df, 20, 2.0)
        lower20 = float(bb20['lower'].iloc[-1]) if not pd.isna(bb20['lower'].iloc[-1]) else 0.0
        width20 = float(bb20['width'].iloc[-1]) if not pd.isna(bb20['width'].iloc[-1]) else 999.0
        bb20_pct = ((close - lower20) / lower20 * 100) if lower20 > 0 else 999.0
    except Exception:
        width20 = 999.0
        bb20_pct = 999.0

    if abs(env20_pct) <= 2.0:
        score = 65
        if atr_pct <= 2.5:
            score += 8
        if bb40_width <= 12:
            score += 6
        candidates.append(('ENV20', score, f'Env20 하단 근접({env20_pct:.1f}%)'))

    if abs(env40_pct) <= 10.0:
        score = 60
        if atr_pct <= 2.8:
            score += 6
        if amount20_b <= 200:
            score += 4
        candidates.append(('ENV40', score, f'Env40 하단권({env40_pct:.1f}%)'))

    if bb40_pct <= 6.0:
        score = 62
        if bb40_width >= 14:
            score += 8
        if atr_pct >= 3.0:
            score += 6
        candidates.append(('BB40', score, f'BB40 하단 근접({bb40_pct:.1f}%)'))

    if bb20_pct <= 4.0:
        score = 58
        if width20 <= 12:
            score += 8
        candidates.append(('BB20', score, f'BB20 하단 근접({bb20_pct:.1f}%)'))

    if not candidates:
        if index_label == '코스피200':
            candidates.append(('ENV40', 50, '코스피200 기본값'))
            candidates.append(('BB40', 44, '보조 밴드'))
        elif index_label == '코스닥150':
            candidates.append(('BB40', 50, '코스닥150 기본값'))
            candidates.append(('ENV40', 44, '보조 밴드'))
        else:
            candidates.append(('BB40', 48, '비지수 기본값'))
            candidates.append(('ENV40', 42, '보조 밴드'))

    candidates.sort(key=lambda x: x[1], reverse=True)

    recommended_band = candidates[0][0]
    support_band = candidates[1][0] if len(candidates) >= 2 and candidates[1][0] != recommended_band else recommended_band

    volatility_type = '변동형' if (bb40_width >= 18 or atr_pct >= 4.0 or amount20_b >= 500) else (
        '안정형' if (bb40_width <= 10 and atr_pct <= 2.2 and amount20_b <= 150) else '중간형'
    )

    universe_tag = _build_universe_tag(index_label=index_label, is_top_mcap=is_top_mcap, is_mcap_or=is_mcap_or)
    reason_parts = [x[2] for x in candidates[:3]]
    comment = f"주밴드 {recommended_band} / 보조밴드 {support_band} | {volatility_type} | {universe_tag} | " + ", ".join(reason_parts)

    return {
        'recommended_band': recommended_band,
        'support_band': support_band,
        'volatility_type': volatility_type,
        'universe_tag': universe_tag,
        'reason': ', '.join(reason_parts),
        'comment': comment,
        'bb40_width': round(bb40_width, 1),
        'atr_pct': round(atr_pct, 1),
        'amount20_b': round(amount20_b, 1),
        'env20_pct': round(env20_pct, 1),
        'env40_pct': round(env40_pct, 1),
        'bb40_pct': round(bb40_pct, 1),
        'bb20_pct': round(bb20_pct, 1) if bb20_pct < 900 else 999.0,
    }



def _analyze_kki_pattern_for_closing(df: pd.DataFrame, row: pd.Series, info: dict, band_rec: dict) -> dict:
    if df is None or len(df) < 80:
        return {
            'kki_score': 0,
            'absorb_score': 0,
            'kki_pattern': '',
            'kki_comment': '',
            'kki_habit': '',
        }

    close = float(row.get('Close', 0) or 0)
    high = float(row.get('High', 0) or 0)
    low = float(row.get('Low', 0) or 0)
    ma20 = float(row.get('MA20', 0) or 0)
    vol = float(row.get('Volume', 0) or 0)
    vma20 = float(row.get('VMA20', row.get('Vol_Avg', 0)) or 0)
    vol_ratio = vol / vma20 if vma20 > 0 else 0.0

    try:
        obv = float(row.get('OBV', 0) or 0)
        obv_prev = float(df['OBV'].iloc[-2]) if 'OBV' in df.columns and len(df) >= 2 else obv
    except Exception:
        obv = 0.0
        obv_prev = 0.0

    recent20 = df.tail(20)
    recent_high20 = float(recent20['High'].max()) if not recent20.empty else high
    recent_low20 = float(recent20['Low'].min()) if not recent20.empty else low
    pullback_pct = ((recent_high20 - close) / recent_high20 * 100.0) if recent_high20 > 0 else 0.0
    near_low_pct = ((close - recent_low20) / recent_low20 * 100.0) if recent_low20 > 0 else 999.0

    impulse_days = 0
    start_i = max(1, len(df) - 60)
    vol_ma = df['Volume'].rolling(20).mean()
    for i in range(start_i, len(df)):
        try:
            prev_close = float(df['Close'].iloc[i - 1])
            c = float(df['Close'].iloc[i])
            v = float(df['Volume'].iloc[i])
            vv = float(vol_ma.iloc[i]) if i < len(vol_ma) else 0.0
            if prev_close > 0 and ((c / prev_close - 1.0) * 100 >= 8.0) and (vv > 0 and v >= vv * 1.8):
                impulse_days += 1
        except Exception:
            pass

    lower_break_recovery = False
    try:
        band = str(band_rec.get('recommended_band', '') or '')
        if band.startswith('ENV'):
            lower20 = _calc_envelope(df, 20, 2.0)['lower']
            recent_lower = lower20.tail(len(recent20)).reset_index(drop=True)
            recent_close = recent20['Close'].reset_index(drop=True)
            if len(recent_close) >= 3 and len(recent_lower) == len(recent_close):
                was_below = bool((recent_close.iloc[:-1] < recent_lower.iloc[:-1]).any())
                now_recovered = bool(recent_close.iloc[-1] >= recent_lower.iloc[-1])
                lower_break_recovery = was_below and now_recovered
        else:
            bb20 = _calc_bollinger(df, 20, 2.0)
            recent_close = recent20['Close'].reset_index(drop=True)
            recent_lower = bb20['lower'].tail(len(recent20)).reset_index(drop=True)
            if len(recent_close) >= 3 and len(recent_lower) == len(recent_close):
                was_below = bool((recent_close.iloc[:-1] < recent_lower.iloc[:-1]).any())
                now_recovered = bool(recent_close.iloc[-1] >= recent_lower.iloc[-1])
                lower_break_recovery = was_below and now_recovered
    except Exception:
        lower_break_recovery = False

    pattern = '혼합형'
    habit = '특정 재현 패턴이 강하게 우세하지는 않습니다.'

    if lower_break_recovery:
        pattern = '하단이탈복귀형'
        habit = '밴드 하단을 잠깐 이탈했다가 다시 밴드 안으로 복귀한 흔적이 있어, 단순 반등보다 복원력과 투매 흡수 성격이 더 강한 편입니다.'
    elif impulse_days >= 1 and 3.0 <= pullback_pct <= 15.0 and (ma20 <= 0 or close >= ma20 * 0.98):
        pattern = '장대양봉→눌림→재발사형'
        habit = '한 번 튄 뒤 눌림을 주고 다시 시세를 붙이는 성향이 있는 종목입니다.'
    elif high >= recent_high20 * 0.995 and 2.0 <= pullback_pct <= 10.0:
        pattern = '상단터치→눌림→2차상승형'
        habit = '상단 첫 반응 뒤 바로 끝나기보다, 한 번 밀렸다가 다시 상단을 재타진하는 타입에 가깝습니다.'
    elif near_low_pct <= 6.0:
        pattern = '하단터치반등형'
        habit = '하단을 건드린 뒤 복원력이 나오는 편이라, 밀리면 받치는 습성이 있습니다.'
    elif band_rec.get('recommended_band') in ('BB20', 'BB40') and band_rec.get('bb40_width', 0) <= 12:
        pattern = '횡보후재발사형'
        habit = '바로 쏘기보다 박스권에서 힘을 모은 뒤 다시 확장되는 흐름에 더 가깝습니다.'

    kki_score = 0
    if pattern == '하단이탈복귀형':
        kki_score += 32
    elif pattern == '장대양봉→눌림→재발사형':
        kki_score += 35
    elif pattern == '상단터치→눌림→2차상승형':
        kki_score += 30
    elif pattern == '하단터치반등형':
        kki_score += 24
    elif pattern == '횡보후재발사형':
        kki_score += 28

    if vol_ratio >= 2.0:
        kki_score += 18
    elif vol_ratio >= 1.3:
        kki_score += 10
    if ma20 > 0 and close >= ma20:
        kki_score += 10
    if obv >= obv_prev:
        kki_score += 8
    if info.get('_close', 0) >= info.get('_open', 0):
        kki_score += 6

    absorb_score = 0
    if near_low_pct <= 6.0:
        absorb_score += 18
    if 2.0 <= pullback_pct <= 12.0:
        absorb_score += 20
    if lower_break_recovery:
        absorb_score += 12
    if ma20 > 0 and close >= ma20 * 0.99:
        absorb_score += 14
    if obv >= obv_prev:
        absorb_score += 10
    if vol_ratio >= 1.0:
        absorb_score += 8

    if pattern == '하단이탈복귀형':
        comment = '밴드 하단을 이탈했다가 복귀한 흔적이 있어, 단순 저점 반등보다 되돌림 복원력과 흡수 성격을 함께 보는 편이 좋습니다.'
    elif kki_score >= 75 and absorb_score >= 50:
        comment = '끼와 흡수가 함께 살아 있어 종가배팅 후 다음 파동 연결 가능성을 열어둘 수 있습니다.'
    elif kki_score >= 60:
        comment = '끼는 살아 있으나 흡수는 보통 수준이라, 다음 날 시가 추격보다 눌림 확인이 더 좋습니다.'
    elif absorb_score >= 50:
        comment = '급등형보다는 매물 소화형 반등에 가까운 구조입니다.'
    else:
        comment = '끼와 흡수가 압도적이지 않아 종가배팅 이후 추격 대응은 보수적으로 보는 편이 좋습니다.'

    return {
        'kki_score': int(kki_score),
        'absorb_score': int(absorb_score),
        'kki_pattern': pattern,
        'kki_habit': habit,
        'kki_comment': comment,
    }


def _select_morales_trailing_ma(df: pd.DataFrame, marcap: float = 0.0, idx_label: str = '') -> dict:
    """
    길 모랄레스식 보유선 선택.
    - MA10: 중소형/탄력형 주도주, 최근 7주 동안 10일선 유지력이 강한 경우
    - MA50: 대형 우량주 또는 10일선을 자주 흔드는 변동성 섹터
    - MA20: 둘 사이의 중간형 관리선
    """
    try:
        if df is None or len(df) < 70:
            return {
                'trail_ma': 'MA20',
                'trail_reason': '데이터가 부족해 기본 MA20 관리',
                'sell_rule': '초기에는 갭 당일 저가 이탈을 실패 기준으로 보고, 이후 MA20 이탈을 확인합니다.',
                'ma10_hold_ratio_35': 0.0,
                'ma10_cross_count_60': 0,
            }

        work = df.copy()
        close = pd.to_numeric(work['Close'], errors='coerce')
        ma10 = close.rolling(10).mean() if 'MA10' not in work.columns else pd.to_numeric(work['MA10'], errors='coerce')
        ma50 = close.rolling(50).mean() if 'MA50' not in work.columns else pd.to_numeric(work['MA50'], errors='coerce')

        recent35_close = close.tail(35)
        recent35_ma10 = ma10.tail(35)
        valid35 = recent35_ma10.notna() & recent35_close.notna()
        if int(valid35.sum()) > 0:
            ma10_break_count_35 = int((recent35_close[valid35] < recent35_ma10[valid35]).sum())
            ma10_hold_ratio = 1 - (ma10_break_count_35 / int(valid35.sum()))
        else:
            ma10_break_count_35 = 35
            ma10_hold_ratio = 0.0

        recent60_close = close.tail(60)
        recent60_ma10 = ma10.tail(60)
        valid60 = recent60_ma10.notna() & recent60_close.notna()
        above_ma10 = recent60_close[valid60] > recent60_ma10[valid60]
        cross_count = int((above_ma10 != above_ma10.shift(1)).sum()) if len(above_ma10) > 1 else 0

        ma10_slope = 0.0
        try:
            if len(ma10.dropna()) >= 6 and float(ma10.iloc[-6]) > 0:
                ma10_slope = (float(ma10.iloc[-1]) / float(ma10.iloc[-6]) - 1) * 100
        except Exception:
            ma10_slope = 0.0

        large_cap_flag = bool(marcap >= GAP_LARGE_CAP_MARCAP or idx_label == '코스피200')

        if (not large_cap_flag) and ma10_hold_ratio >= 0.90 and ma10_slope > 0:
            return {
                'trail_ma': 'MA10',
                'trail_reason': f'최근 7주 10일선 유지율 {ma10_hold_ratio * 100:.0f}% / 10일선 기울기 {ma10_slope:+.1f}%로 탄력형 주도주 성격',
                'sell_rule': '종가가 10일선 아래에서 마감되면 1차 경고, 다음날 그 이탈일 저가를 깨면 매도 확정',
                'ma10_hold_ratio_35': round(ma10_hold_ratio * 100, 1),
                'ma10_cross_count_60': cross_count,
            }

        if large_cap_flag or cross_count >= 8:
            reason_prefix = '대형 우량주/코스피200 성격' if large_cap_flag else '10일선 등락이 잦은 변동성 종목'
            return {
                'trail_ma': 'MA50',
                'trail_reason': f'{reason_prefix}이며 최근 60일 10일선 등락 {cross_count}회 → 50일선 추세 관리가 적합',
                'sell_rule': '10일선 이탈은 흔들림으로 볼 수 있고, 종가가 50일선을 완전히 이탈하면 전량 매도',
                'ma10_hold_ratio_35': round(ma10_hold_ratio * 100, 1),
                'ma10_cross_count_60': cross_count,
            }

        return {
            'trail_ma': 'MA20',
            'trail_reason': f'10일선 유지율 {ma10_hold_ratio * 100:.0f}% / 등락 {cross_count}회로 중간형 관리가 적합',
            'sell_rule': '초기 손절은 갭 당일 저가, 수익 구간 이후에는 MA20 이탈 여부를 확인',
            'ma10_hold_ratio_35': round(ma10_hold_ratio * 100, 1),
            'ma10_cross_count_60': cross_count,
        }
    except Exception as e:
        return {
            'trail_ma': 'MA20',
            'trail_reason': f'보유선 판단 오류: {e}',
            'sell_rule': '갭 당일 저가 이탈 시 우선 방어',
            'ma10_hold_ratio_35': 0.0,
            'ma10_cross_count_60': 0,
        }


def _check_morales_gap_bet(code: str, name: str) -> dict | None:
    """
    전략 G — 길 모랄레스식 갭 돌파 종가매수형.

    매수 필터:
      1) 갭 상승 2~12%
      2) 거래량 50일 평균 대비 1.5배 이상
      3) 갭 부근/전일종가를 장중에 지지
      4) 종가가 시가 부근 이상 + 캔들 중간 이상 마감
      5) 60일 박스 또는 120일 신고가 돌파
      6) 클라이맥스 갭 제외

    보유/매도:
      - 초기 실패 기준은 갭 당일 저가 이탈
      - 이후 성격에 따라 MA10 / MA20 / MA50 자동 선택
    """
    try:
        code = _normalize_code(code)
        df = _load_df(code, lookback_days=730)
        if df is None or len(df) < max(130, GAP_HIGH_LOOKBACK + 5):
            with DIAG_LOCK:
                STRATEGY_FAIL['G_no_df'] += 1
            return None

        row = df.iloc[-1]
        prev = df.iloc[-2]
        past = df.iloc[:-1]
        info = _base_info(row, df)

        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        is_mcap_or = marcap >= MCAP_OR_MIN

        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            with DIAG_LOCK:
                STRATEGY_FAIL['G_universe'] += 1
            return None

        if info['_close'] < MIN_PRICE or info['amount_b'] < MIN_AMOUNT / 1e8:
            with DIAG_LOCK:
                STRATEGY_FAIL['G_price_amount'] += 1
            return None

        today_open = info['_open']
        today_high = info['_high']
        today_low = info['_low']
        today_close = info['_close']
        today_volume = info['_vol']
        prev_close = _safe_float(prev.get('Close', 0), 0.0)

        if today_open <= 0 or today_close <= 0 or prev_close <= 0:
            with DIAG_LOCK:
                STRATEGY_FAIL['G_gap'] += 1
            return None

        # 1) 갭 상승
        gap_pct = (today_open / prev_close - 1.0) * 100.0
        gap_ok = GAP_MIN_PCT <= gap_pct <= GAP_MAX_PCT
        if not gap_ok:
            with DIAG_LOCK:
                STRATEGY_FAIL['G_gap'] += 1
            return None

        # 2) 거래량 50일 평균 대비 1.5배 이상
        vol50 = _safe_float(past['Volume'].tail(50).mean(), 0.0)
        vol50_ratio = today_volume / vol50 if vol50 > 0 else 0.0
        volume_ok = vol50_ratio >= GAP_VOL50_MULT
        if not volume_ok:
            with DIAG_LOCK:
                STRATEGY_FAIL['G_volume'] += 1
            return None

        # 3) 갭 지지: 전일 종가를 의미 있게 지키고, 종가가 시가 근처 이상에서 마감
        gap_unfilled = today_low >= prev_close * (1.0 + GAP_LOW_KEEP_PCT / 100.0)
        close_support = today_close >= today_open * GAP_CLOSE_OPEN_KEEP
        close_strength = today_close >= (today_high + today_low) / 2.0 if today_high > today_low else today_close >= today_open
        support_ok = gap_unfilled and close_support and close_strength
        if not support_ok:
            with DIAG_LOCK:
                STRATEGY_FAIL['G_support'] += 1
            return None

        # 4) 위치: 박스권 돌파 또는 신고가 돌파
        box_high_60 = _safe_float(past['High'].tail(GAP_BOX_LOOKBACK).max(), 0.0)
        high_120 = _safe_float(past['High'].tail(GAP_HIGH_LOOKBACK).max(), 0.0)
        box_breakout = bool(box_high_60 > 0 and today_close >= box_high_60 * 1.002)
        new_high_breakout = bool(high_120 > 0 and today_close >= high_120 * 1.002)
        location_ok = box_breakout or new_high_breakout
        if not location_ok:
            with DIAG_LOCK:
                STRATEGY_FAIL['G_location'] += 1
            return None

        # 5) 클라이맥스 갭 제외
        ma20 = _safe_float(row.get('MA20', 0), 0.0)
        disparity20 = today_close / ma20 * 100.0 if ma20 > 0 else 999.0
        close_20ago = _safe_float(df['Close'].iloc[-21], 0.0) if len(df) >= 21 else 0.0
        runup20 = (today_close / close_20ago - 1.0) * 100.0 if close_20ago > 0 else 999.0
        candle_range = today_high - today_low
        upper_wick_ratio = ((today_high - max(today_open, today_close)) / candle_range) if candle_range > 0 else 0.0
        not_climax = (
            disparity20 <= GAP_DISPARITY20_MAX
            and runup20 <= GAP_RUNUP20_MAX
            and upper_wick_ratio <= GAP_UPPER_WICK_MAX
        )
        if not not_climax:
            with DIAG_LOCK:
                STRATEGY_FAIL['G_climax'] += 1
            return None

        passed = []
        score = 0
        if gap_ok:
            score += 15
            passed.append(f'①갭{gap_pct:+.1f}%')
        if volume_ok:
            score += 25
            passed.append(f'②Vol50 {vol50_ratio:.1f}배')
        if gap_unfilled:
            score += 15
            passed.append('③갭미메움')
        if close_support:
            score += 10
            passed.append('④시가지지')
        if close_strength:
            score += 10
            passed.append('⑤종가강도')
        if box_breakout:
            score += 15
            passed.append('⑥60일박스돌파')
        if new_high_breakout:
            score += 20
            passed.append('⑦120일신고가')
        if not_climax:
            score += 10
            passed.append('⑧클라이맥스제외')
        score = min(score, 100)

        if score < 85:
            with DIAG_LOCK:
                STRATEGY_FAIL['G_score'] += 1
            return None

        if score >= 95:
            grade = '완전체'
        elif score >= 88:
            grade = '✅A급'
        else:
            grade = 'B급'

        band_rec = _get_band_recommendation(
            code=code,
            df=df,
            row=row,
            index_label=idx_label,
            is_top_mcap=(code in TOP_MCAP_SET),
            is_mcap_or=is_mcap_or,
        )
        kki_rec = _analyze_kki_pattern_for_closing(df, row, info, band_rec)
        trail = _select_morales_trailing_ma(df, marcap=marcap, idx_label=idx_label)

        stoploss = round(today_low)
        risk = max(today_close - stoploss, 0.0)
        target1 = round(today_close + risk * 2.0) if risk > 0 else round(today_close * 1.05)
        rr = round(((target1 - today_close) / risk), 2) if risk > 0 else 0.0

        gap_quality = '주도주출발형' if (new_high_breakout and vol50_ratio >= 2.0) else '박스돌파형'
        band_comment = (
            f"{gap_quality} | 갭{gap_pct:+.1f}% / Vol50 {vol50_ratio:.1f}배 / "
            f"이격{disparity20:.1f} / 20일상승{runup20:+.1f}%"
        )

        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'G',
            'strategy': 'G',
            'mode_label': '모랄레스갭',
            'index_label': idx_label,
            'recommended_band': band_rec['recommended_band'],
            'support_band': band_rec['support_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': band_comment,
            'band_recommend_reason': band_rec['reason'],
            'kki_score': kki_rec['kki_score'],
            'absorb_score': kki_rec['absorb_score'],
            'kki_pattern': kki_rec['kki_pattern'],
            'kki_habit': kki_rec['kki_habit'],
            'kki_comment': kki_rec['kki_comment'],
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'marcap': marcap,
            'is_mcap_or': int(is_mcap_or),
            'close': today_close,
            'band_type': 'GAP',
            'band_reason': '길모랄레스 갭+거래량+갭지지+위치 필터 통과',
            'band_pct_text': f"갭:{gap_pct:+.1f}% | Vol50:{vol50_ratio:.1f}배 | 이격:{disparity20:.1f} | 20일상승:{runup20:+.1f}%",
            'gap_pct': round(gap_pct, 1),
            'vol50_ratio': round(vol50_ratio, 2),
            'vol_ratio': round(vol50_ratio, 2),
            'gap_low': round(today_low),
            'prev_close': round(prev_close),
            'box_high_60': round(box_high_60),
            'high_120': round(high_120),
            'disparity20': round(disparity20, 1),
            'runup20': round(runup20, 1),
            'upper_wick_pct': round(upper_wick_ratio * 100, 1),
            'wick_pct': round(upper_wick_ratio * 100, 1),
            'trail_ma': trail['trail_ma'],
            'trail_reason': trail['trail_reason'],
            'sell_rule': trail['sell_rule'],
            'ma10_hold_ratio_35': trail['ma10_hold_ratio_35'],
            'ma10_cross_count_60': trail['ma10_cross_count_60'],
            'initial_stop_rule': '갭 당일 저가 이탈 시 실패 처리',
            'stoploss': stoploss,
            'target1': target1,
            'rr': rr,
            'score': score,
            'grade': grade,
            'passed': passed,
        }
    except Exception as e:
        with DIAG_LOCK:
            STRATEGY_FAIL['G_no_df'] += 1
        log_error(f"_check_morales_gap_bet 오류 [{code}/{name}]: {e}")
        return None



def _evaluate_high_reaccum_signal(df: pd.DataFrame) -> dict:
    """
    전략 S 공용 판정: 고점권 재응축 2차 슈팅 후보.

    v2.8 핵심:
      - S1 관찰형: 구조는 좋지만 당일 거래량 재점화가 부족한 후보
      - S2 실행형: 거래량/거래대금/종가강도/RR이 같이 붙은 종가배팅 실행 후보
    """
    try:
        if df is None or len(df) < max(140, HIGH_REACCUM_LOOKBACK + 5):
            return {'pass': False, 'fail': 'no_df'}

        row = df.iloc[-1]
        info = _base_info(row, df)
        amount_b = _safe_float(info.get('amount_b', 0), 0.0)
        close = info['_close']
        open_p = info['_open']
        high = info['_high']
        low = info['_low']
        vol = info['_vol']
        ma20 = _safe_float(row.get('MA20', 0), 0.0)
        ma50 = _safe_float(row.get('MA50', 0), 0.0)
        rsi = _safe_float(row.get('RSI', 50), 50.0)

        if close <= 0 or high <= 0 or low <= 0:
            return {'pass': False, 'fail': 'price'}

        lookback = int(min(HIGH_REACCUM_LOOKBACK, len(df) - 1))
        past = df.iloc[:-1]
        high120 = _safe_float(df['High'].tail(lookback).max(), 0.0)
        past_high20 = _safe_float(past['High'].tail(20).max(), 0.0)
        close_ago = _safe_float(df['Close'].iloc[-lookback], 0.0) if len(df) > lookback else 0.0
        close_20ago = _safe_float(df['Close'].iloc[-21], 0.0) if len(df) >= 21 else 0.0

        runup120 = (close / close_ago - 1.0) * 100.0 if close_ago > 0 else 0.0
        near_high120 = close / high120 * 100.0 if high120 > 0 else 0.0
        pullback_from_high = (high120 - close) / high120 * 100.0 if high120 > 0 else 999.0
        runup20 = (close / close_20ago - 1.0) * 100.0 if close_20ago > 0 else 0.0
        disparity20 = close / ma20 * 100.0 if ma20 > 0 else 999.0

        ma20_prev5 = _safe_float(df['MA20'].iloc[-6], 0.0) if 'MA20' in df.columns and len(df) >= 6 else 0.0
        ma50_prev10 = _safe_float(df['MA50'].iloc[-11], 0.0) if 'MA50' in df.columns and len(df) >= 11 else 0.0
        ma20_slope5 = (ma20 / ma20_prev5 - 1.0) * 100.0 if ma20 > 0 and ma20_prev5 > 0 else 0.0
        ma50_slope10 = (ma50 / ma50_prev10 - 1.0) * 100.0 if ma50 > 0 and ma50_prev10 > 0 else 0.0

        vma5 = _safe_float(df['Volume'].tail(5).mean(), 0.0)
        vma20 = _safe_float(row.get('VMA20', df['Volume'].tail(20).mean()), 0.0)
        vma5_20_ratio = vma5 / vma20 if vma20 > 0 else 0.0
        today_vol_ratio = vol / vma20 if vma20 > 0 else 0.0

        try:
            obv_now = _safe_float(row.get('OBV', 0), 0.0)
            obv_20ago = _safe_float(df['OBV'].iloc[-21], 0.0) if 'OBV' in df.columns and len(df) >= 21 else obv_now
            obv_ma5 = _safe_float(df['OBV'].rolling(5).mean().iloc[-1], 0.0) if 'OBV' in df.columns else obv_now
            obv_ma20 = _safe_float(df['OBV'].rolling(20).mean().iloc[-1], 0.0) if 'OBV' in df.columns else obv_now
            obv_alive = bool((obv_now >= obv_20ago) or (obv_ma5 >= obv_ma20))
        except Exception:
            obv_alive = False

        candle_range = high - low
        if candle_range > 0:
            close_loc_pct = (close - low) / candle_range * 100.0
            upper_wick_range_pct = (high - max(open_p, close)) / candle_range * 100.0
        else:
            close_loc_pct = 100.0 if close >= open_p else 50.0
            upper_wick_range_pct = 0.0

        strong_runup = runup120 >= HIGH_REACCUM_RUNUP_MIN
        near_high = HIGH_REACCUM_NEAR_HIGH_MIN <= near_high120 <= HIGH_REACCUM_NEAR_HIGH_MAX
        not_broken = pullback_from_high <= HIGH_REACCUM_MAX_PULLBACK
        trend_alive = bool(
            ma20 > 0 and ma50 > 0
            and close >= ma20 * 0.98
            and ma20_slope5 >= -1.0
            and ma50_slope10 >= -1.5
        )
        cooling = HIGH_REACCUM_RSI_MIN <= rsi <= HIGH_REACCUM_RSI_MAX

        if vma5_20_ratio <= HIGH_REACCUM_VOLUME_DRY_MAX:
            volume_state = '응축'
        elif vma5_20_ratio <= HIGH_REACCUM_VOLUME_NORMAL_MAX:
            volume_state = '보통'
        else:
            volume_state = '재증가'

        volume_ok = bool(vma20 > 0 and today_vol_ratio >= HIGH_REACCUM_TODAY_VOL_MIN)
        high_close = bool(
            close_loc_pct >= HIGH_REACCUM_CLOSE_LOC_MIN
            and upper_wick_range_pct <= HIGH_REACCUM_UPPER_WICK_RANGE_MAX
            and close >= open_p * HIGH_REACCUM_CLOSE_OPEN_KEEP
        )
        near_recent_resist = bool(past_high20 > 0 and close >= past_high20 * 0.92)
        not_climax = bool(disparity20 <= HIGH_REACCUM_DISPARITY20_MAX and runup20 <= HIGH_REACCUM_RUNUP20_MAX)

        if not strong_runup:
            fail = 'runup'
        elif not (near_high and not_broken):
            fail = 'position'
        elif not trend_alive:
            fail = 'trend'
        elif not high_close:
            fail = 'close_strength'
        elif not not_climax:
            fail = 'climax'
        else:
            fail = ''

        passed = []
        score = 0
        if strong_runup:
            score += 15; passed.append(f'①120일상승{runup120:+.0f}%')
        if near_high:
            score += 15; passed.append(f'②고점근접{near_high120:.1f}%')
        if not_broken:
            score += 10; passed.append(f'③고점대비하락{pullback_from_high:.1f}%')
        if trend_alive:
            score += 15; passed.append('④추세생존')
        if cooling:
            score += 8; passed.append(f'⑤RSI식힘{rsi:.1f}')
        if volume_ok:
            score += 6; passed.append(f'⑥당일거래량{today_vol_ratio:.2f}배')
        if vma5_20_ratio <= HIGH_REACCUM_VMA5_20_MAX:
            score += 5; passed.append(f'⑦거래량상태:{volume_state}{vma5_20_ratio:.2f}')
        if obv_alive:
            score += 10; passed.append('⑧OBV유지')
        if high_close:
            score += 18; passed.append(f'⑨종가고점마감{close_loc_pct:.0f}%')
        if near_recent_resist:
            score += 8; passed.append('⑩전고점재도전권')
        if not_climax:
            score += 8; passed.append('⑪클라이맥스제외')
        score_raw = score

        recent15_low = _safe_float(df['Low'].tail(15).min(), 0.0)
        stop_candidates = [x for x in [recent15_low, ma20] if x > 0]
        if stop_candidates:
            structural_stop = min(stop_candidates)
            stoploss = max(structural_stop, close * 0.88)
        else:
            stoploss = close * 0.90
        if stoploss > close * 0.97:
            stoploss = close * 0.97
        target1 = high120 if high120 > close else close * 1.05
        target2 = target1 * 1.08
        risk = close - stoploss
        rr = (target1 - close) / risk if risk > 0 and target1 > close else 0.0

        # S2 실행형 조건: 종가배팅 당일 매수 후보로 볼 수 있는 최소 재점화 조건
        is_s2_exec = bool(
            today_vol_ratio >= HIGH_REACCUM_EXEC_VOL_MIN
            and amount_b >= HIGH_REACCUM_EXEC_AMOUNT_MIN_B
            and close_loc_pct >= HIGH_REACCUM_EXEC_CLOSE_LOC_MIN
            and upper_wick_range_pct <= HIGH_REACCUM_EXEC_WICK_MAX
            and rr >= HIGH_REACCUM_RR_GOOD_MIN
        )

        if is_s2_exec:
            s_type = 'S2'
            s_type_label = 'S2 실행형'
            execution_verdict = '종가배팅 실행 후보 — 거래량 재점화와 종가 상단 마감 동시 확인'
        elif today_vol_ratio < 1.0:
            s_type = 'S1'
            s_type_label = 'S1 관찰형'
            execution_verdict = '구조는 좋지만 당일 거래량 부족 — 다음날 거래량 1.2배 이상 재점화 확인'
        else:
            s_type = 'S1'
            s_type_label = 'S1 관찰형'
            execution_verdict = '실행 전 대기 — 거래량/전고점 재돌파 확인 시 S2로 승격 가능'

        score_adjust = 0
        rr_flag = '양호'
        if rr < HIGH_REACCUM_RR_EXCLUDE_MIN:
            rr_flag = '제외권'; score_adjust -= 35
        elif rr < 0.50:
            rr_flag = '낮음'; score_adjust -= 18
        elif rr < HIGH_REACCUM_RR_GOOD_MIN:
            rr_flag = '보통하단'; score_adjust -= 8
        elif rr >= 1.00:
            rr_flag = '우수'; score_adjust += 8
        else:
            score_adjust += 3

        if amount_b >= HIGH_REACCUM_AMOUNT_GOOD_B:
            score_adjust += 4
        elif amount_b < 50:
            score_adjust -= 8
        else:
            score_adjust -= 3

        if today_vol_ratio >= HIGH_REACCUM_TODAY_VOL_GOOD:
            score_adjust += 6
        elif today_vol_ratio >= HIGH_REACCUM_EXEC_VOL_MIN:
            score_adjust += 3
        elif today_vol_ratio < 1.0:
            score_adjust -= 12
        else:
            score_adjust -= 4

        if is_s2_exec:
            score_adjust += 8
        if close_loc_pct >= 85:
            score_adjust += 4
        if upper_wick_range_pct <= 15:
            score_adjust += 3
        if near_high120 >= 98.0 and rr < 0.50:
            score_adjust -= 12

        final_score = max(0, min(100, score_raw + score_adjust))

        # v2.8 점수 캡: 거래량 재점화가 없으면 완전체 금지
        if today_vol_ratio < 1.0:
            final_score = min(final_score, 84.0)
        elif today_vol_ratio < HIGH_REACCUM_EXEC_VOL_MIN:
            final_score = min(final_score, 89.0)
        elif not is_s2_exec:
            final_score = min(final_score, 89.0)

        if fail == '' and rr < HIGH_REACCUM_RR_EXCLUDE_MIN:
            fail = 'rr'

        passed_gate = (fail == '') and final_score >= HIGH_REACCUM_SCORE_MIN
        if not passed_gate and fail == '':
            fail = 'score'

        if s_type == 'S2' and final_score >= 90:
            grade = '완전체'
        elif final_score >= 80:
            grade = '✅A급'
        else:
            grade = 'B급'

        return {
            'pass': bool(passed_gate),
            'fail': fail,
            'score': round(final_score, 1),
            'score_raw': round(score_raw, 1),
            'score_adjust': round(score_adjust, 1),
            'grade': grade,
            's_type': s_type,
            's_type_label': s_type_label,
            'execution_verdict': execution_verdict,
            'passed': passed,
            'runup120': round(runup120, 1),
            'near_high120': round(near_high120, 1),
            'pullback_from_high': round(pullback_from_high, 1),
            'ma20_slope5': round(ma20_slope5, 2),
            'ma50_slope10': round(ma50_slope10, 2),
            'rsi': round(rsi, 1),
            'vma5_20_ratio': round(vma5_20_ratio, 2),
            'volume_state': volume_state,
            'today_vol_ratio': round(today_vol_ratio, 2),
            'obv_alive': int(bool(obv_alive)),
            'close_loc_pct': round(close_loc_pct, 1),
            'upper_wick_range_pct': round(upper_wick_range_pct, 1),
            'disparity20': round(disparity20, 1),
            'runup20': round(runup20, 1),
            'stoploss': round(stoploss),
            'target1': round(target1),
            'target2': round(target2),
            'rr': round(rr, 2),
            'rr_flag': rr_flag,
            'high120': round(high120),
            'past_high20': round(past_high20),
            'high_close_rule': f'종가위치 {close_loc_pct:.0f}% / 윗꼬리 {upper_wick_range_pct:.0f}%',
            'stop_logic': '최근 15일 눌림저점·20일선 중 낮은 지지선을 기본으로 하되, 현재가 대비 최대 -12% 안쪽으로 보정',
            'initial_stop_rule': '100% 예측매수가 아니라, 종가 고점 마감 후 진입하고 20일선/최근 눌림 저점 종가 이탈 시 실패 처리',
        }
    except Exception as e:
        return {'pass': False, 'fail': f'error:{e}'}

def _check_high_reaccum_shooting_bet(code: str, name: str) -> dict | None:
    """
    전략 S — 고점권 재응축 2차 슈팅 종가매수형.

    쉽게 말해 이미 크게 오른 주도주가 고점 부근에서 무너지지 않고,
    거래량이 식는 동안 OBV/추세가 유지되다가 그날 종가가 고점권에서 잠기는지를 본다.
    """
    try:
        code = _normalize_code(code)
        df = _load_df(code, lookback_days=730)
        if df is None or len(df) < max(140, HIGH_REACCUM_LOOKBACK + 5):
            with DIAG_LOCK:
                STRATEGY_FAIL['S_no_df'] += 1
            return None

        row = df.iloc[-1]
        info = _base_info(row, df)
        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        is_mcap_or = marcap >= MCAP_OR_MIN

        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            with DIAG_LOCK:
                STRATEGY_FAIL['S_universe'] += 1
            return None
        if info['_close'] < MIN_PRICE or info['amount_b'] < MIN_AMOUNT / 1e8:
            with DIAG_LOCK:
                STRATEGY_FAIL['S_price_amount'] += 1
            return None

        sig = _evaluate_high_reaccum_signal(df)
        if not sig.get('pass'):
            fail = str(sig.get('fail', 'score'))
            key = {
                'runup': 'S_runup',
                'position': 'S_position',
                'trend': 'S_trend',
                'close_strength': 'S_close_strength',
                'climax': 'S_climax',
                'score': 'S_score',
                'rr': 'S_score',
                'no_df': 'S_no_df',
            }.get(fail, 'S_score')
            with DIAG_LOCK:
                STRATEGY_FAIL[key] += 1
            return None

        band_rec = _get_band_recommendation(
            code=code,
            df=df,
            row=row,
            index_label=idx_label,
            is_top_mcap=(code in TOP_MCAP_SET),
            is_mcap_or=is_mcap_or,
        )
        kki_rec = _analyze_kki_pattern_for_closing(df, row, info, band_rec)

        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'S',
            'strategy': 'S',
            'mode_label': '고점재응축',
            's_type': sig.get('s_type', ''),
            's_type_label': sig.get('s_type_label', ''),
            'execution_verdict': sig.get('execution_verdict', ''),
            'index_label': idx_label,
            'recommended_band': band_rec['recommended_band'],
            'support_band': band_rec['support_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': f"{sig.get('s_type_label','S')} | 고점권 재응축 2차 슈팅 후보 | {sig.get('high_close_rule','')}",
            'band_recommend_reason': band_rec['reason'],
            'kki_score': kki_rec['kki_score'],
            'absorb_score': kki_rec['absorb_score'],
            'kki_pattern': kki_rec['kki_pattern'],
            'kki_habit': kki_rec['kki_habit'],
            'kki_comment': kki_rec['kki_comment'],
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'marcap': marcap,
            'is_mcap_or': int(is_mcap_or),
            'close': info['_close'],
            'band_type': 'HIGH_REACCUM',
            'band_reason': '고점권 재응축+종가고점마감',
            'band_pct_text': f"120일상승:{sig.get('runup120',0):+.1f}% | 고점근접:{sig.get('near_high120',0):.1f}% | 종가위치:{sig.get('close_loc_pct',0):.0f}%",
            'score': sig['score'],
            'grade': sig['grade'],
            'passed': sig['passed'],
            'runup120': sig.get('runup120', 0),
            'near_high120': sig.get('near_high120', 0),
            'pullback_from_high': sig.get('pullback_from_high', 0),
            'close_loc_pct': sig.get('close_loc_pct', 0),
            'upper_wick_range_pct': sig.get('upper_wick_range_pct', 0),
            'vma5_20_ratio': sig.get('vma5_20_ratio', 0),
            'volume_state': sig.get('volume_state', ''),
            'today_vol_ratio': sig.get('today_vol_ratio', 0),
            'obv_alive': sig.get('obv_alive', 0),
            'rsi': sig.get('rsi', 0),
            'disparity20': sig.get('disparity20', 0),
            'runup20': sig.get('runup20', 0),
            'stoploss': sig.get('stoploss', 0),
            'target1': sig.get('target1', 0),
            'target2': sig.get('target2', 0),
            'rr': sig.get('rr', 0),
            'rr_flag': sig.get('rr_flag', ''),
            'score_raw': sig.get('score_raw', 0),
            'score_adjust': sig.get('score_adjust', 0),
            'stop_logic': sig.get('stop_logic', ''),
            'initial_stop_rule': sig.get('initial_stop_rule', ''),
            'high_close_rule': sig.get('high_close_rule', ''),
            'ma20_slope5': sig.get('ma20_slope5', 0),
            'ma50_slope10': sig.get('ma50_slope10', 0),
        }
    except Exception as e:
        with DIAG_LOCK:
            STRATEGY_FAIL['S_no_df'] += 1
        log_error(f"_check_high_reaccum_shooting_bet 오류 [{code}/{name}]: {e}")
        return None

def _check_breakout_bet(code: str, name: str) -> dict | None:
    """
    전략 A — 전고점 돌파형 종가배팅
    """
    try:
        code = _normalize_code(code)
        df = _load_df(code)
        if df is None or len(df) < 80:
            with DIAG_LOCK:
                STRATEGY_FAIL['A_no_df'] += 1
            return None

        row = df.iloc[-1]
        info = _base_info(row, df)

        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        is_mcap_or = marcap >= MCAP_OR_MIN

        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            with DIAG_LOCK:
                STRATEGY_FAIL['A_universe'] += 1
            return None

        if info['_close'] < MIN_PRICE or info['amount_b'] < MIN_AMOUNT / 1e8:
            with DIAG_LOCK:
                STRATEGY_FAIL['A_price_amount'] += 1
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
            with DIAG_LOCK:
                STRATEGY_FAIL['A_score'] += 1
            return None

        band_rec = _get_band_recommendation(
            code=code,
            df=df,
            row=row,
            index_label=idx_label,
            is_top_mcap=(code in TOP_MCAP_SET),
            is_mcap_or=is_mcap_or,
        )
        kki_rec = _analyze_kki_pattern_for_closing(df, row, info, band_rec)

        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'A',
            'mode_label': '돌파형',
            'index_label': idx_label,
            'recommended_band': band_rec['recommended_band'],
            'support_band': band_rec['support_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': band_rec['comment'],
            'band_recommend_reason': band_rec['reason'],
            'kki_score': kki_rec['kki_score'],
            'absorb_score': kki_rec['absorb_score'],
            'kki_pattern': kki_rec['kki_pattern'],
            'kki_habit': kki_rec['kki_habit'],
            'kki_comment': kki_rec['kki_comment'],
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'marcap': marcap,
            'is_mcap_or': int(is_mcap_or),
            'close': info['_close'],
            'near20': round(info['_near20'], 1),
            'disp': round(info['_disp'], 1),
            'score': score,
            'grade': '완전체' if score == 6 else ('✅A급' if score == 5 else 'B급'),
            'passed': passed,
        }
    except Exception as e:
        with DIAG_LOCK:
            STRATEGY_FAIL['A_no_df'] += 1
        log_error(f"_check_breakout_bet 오류 [{code}/{name}]: {e}")
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
        code = _normalize_code(code)
        df = _load_df(code)
        if df is None or len(df) < 80:
            with DIAG_LOCK:
                STRATEGY_FAIL['B1_no_df'] += 1
            return None

        row = df.iloc[-1]
        info = _base_info(row, df)

        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        is_mcap_or = marcap >= MCAP_OR_MIN

        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            with DIAG_LOCK:
                STRATEGY_FAIL['B1_universe'] += 1
            return None

        if info['_close'] < MIN_PRICE or info['amount_b'] < MIN_AMOUNT / 1e8:
            with DIAG_LOCK:
                STRATEGY_FAIL['B1_price'] += 1
            return None

        env = _check_envelope_bottom(row, df)
        rsi = float(row.get('RSI', 50) or 50)

        env_strict = env['env20_near'] and env['env40_near']
        if not env_strict:
            with DIAG_LOCK:
                STRATEGY_FAIL['B1_env_strict'] += 1
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
            with DIAG_LOCK:
                STRATEGY_FAIL['B1_score'] += 1
            return None

        if score >= 6:
            grade = '완전체'
        elif score == 5:
            grade = '✅A급'
        else:
            grade = 'B급'

        env20_ma = float(_calc_envelope(df, 20, 10)['ma'].iloc[-1])
        target_env = round(env20_ma)
        maejip_chart = _build_maejip_chart(df)
        band_rec = _get_band_recommendation(
            code=code,
            df=df,
            row=row,
            index_label=idx_label,
            is_top_mcap=(code in TOP_MCAP_SET),
            is_mcap_or=is_mcap_or,
        )
        kki_rec = _analyze_kki_pattern_for_closing(df, row, info, band_rec)

        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'B1',
            'mode_label': 'ENV엄격형',
            'index_label': idx_label,
            'recommended_band': band_rec['recommended_band'],
            'support_band': band_rec['support_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': band_rec['comment'],
            'band_recommend_reason': band_rec['reason'],
            'kki_score': kki_rec['kki_score'],
            'absorb_score': kki_rec['absorb_score'],
            'kki_pattern': kki_rec['kki_pattern'],
            'kki_habit': kki_rec['kki_habit'],
            'kki_comment': kki_rec['kki_comment'],
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'marcap': marcap,
            'is_mcap_or': int(is_mcap_or),
            'close': info['_close'],
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
        with DIAG_LOCK:
            STRATEGY_FAIL['B1_no_df'] += 1
        log_error(f"_check_env_strict_bet 오류 [{code}/{name}]: {e}")
        return None


def _check_bb_expand_bet(code: str, name: str) -> dict | None:
    """
    전략 B2 — BB/확장형 하단 재안착
    """
    try:
        code = _normalize_code(code)
        df = _load_df(code)
        if df is None or len(df) < 80:
            with DIAG_LOCK:
                STRATEGY_FAIL['B2_no_df'] += 1
            return None

        row = df.iloc[-1]
        info = _base_info(row, df)

        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        is_mcap_or = marcap >= MCAP_OR_MIN

        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            with DIAG_LOCK:
                STRATEGY_FAIL['B2_universe'] += 1
            return None

        if info['_close'] < MIN_PRICE or info['amount_b'] < MIN_AMOUNT / 1e8:
            with DIAG_LOCK:
                STRATEGY_FAIL['B2_price'] += 1
            return None

        bb = _check_bb_bottom(row, df)
        rsi = float(row.get('RSI', 50) or 50)

        if not bb['bb40_near']:
            with DIAG_LOCK:
                STRATEGY_FAIL['B2_bb40'] += 1
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
            with DIAG_LOCK:
                STRATEGY_FAIL['B2_score'] += 1
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
            index_label=idx_label,
            is_top_mcap=(code in TOP_MCAP_SET),
            is_mcap_or=is_mcap_or,
        )
        kki_rec = _analyze_kki_pattern_for_closing(df, row, info, band_rec)

        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'B2',
            'mode_label': 'BB확장형',
            'index_label': idx_label,
            'recommended_band': band_rec['recommended_band'],
            'support_band': band_rec['support_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': band_rec['comment'],
            'band_recommend_reason': band_rec['reason'],
            'kki_score': kki_rec['kki_score'],
            'absorb_score': kki_rec['absorb_score'],
            'kki_pattern': kki_rec['kki_pattern'],
            'kki_habit': kki_rec['kki_habit'],
            'kki_comment': kki_rec['kki_comment'],
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'marcap': marcap,
            'is_mcap_or': int(is_mcap_or),
            'close': info['_close'],
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
        with DIAG_LOCK:
            STRATEGY_FAIL['B2_no_df'] += 1
        log_error(f"_check_bb_expand_bet 오류 [{code}/{name}]: {e}")
        return None


def _check_closing_bet(code: str, name: str) -> dict | None:
    """
    G / S / A / B1 / B2 / C 중 우선순위가 가장 높은 전략 1개 반환.
    S는 고점권 2차 슈팅 특화 전략이므로 같은 완전체라면 일반 A보다 우선한다.
    """
    with DIAG_LOCK:
        STRATEGY_DIAG['G_try'] += 1
    g = _check_morales_gap_bet(code, name)
    if g is not None:
        with DIAG_LOCK:
            STRATEGY_DIAG['G_hit'] += 1

    with DIAG_LOCK:
        STRATEGY_DIAG['S_try'] += 1
    s = _check_high_reaccum_shooting_bet(code, name)
    if s is not None:
        with DIAG_LOCK:
            STRATEGY_DIAG['S_hit'] += 1

    with DIAG_LOCK:
        STRATEGY_DIAG['A_try'] += 1
    a = _check_breakout_bet(code, name)
    if a is not None:
        with DIAG_LOCK:
            STRATEGY_DIAG['A_hit'] += 1

    with DIAG_LOCK:
        STRATEGY_DIAG['B1_try'] += 1
    b1 = _check_env_strict_bet(code, name)
    if b1 is not None:
        with DIAG_LOCK:
            STRATEGY_DIAG['B1_hit'] += 1

    with DIAG_LOCK:
        STRATEGY_DIAG['B2_try'] += 1
    b2 = _check_bb_expand_bet(code, name)
    if b2 is not None:
        with DIAG_LOCK:
            STRATEGY_DIAG['B2_hit'] += 1

    with DIAG_LOCK:
        STRATEGY_DIAG['C_try'] += 1
    c = _check_ymgp_bet(code, name)
    if c is not None:
        with DIAG_LOCK:
            STRATEGY_DIAG['C_hit'] += 1

    candidates = [x for x in [g, s, a, b1, b2, c] if x]
    if not candidates:
        return None

    def _priority(h):
        grade = str(h.get('grade', ''))
        mode = str(h.get('mode', ''))
        g_rank = 0 if '완전체' in grade else (1 if 'A급' in grade else 2)
        mode_rank = {'G': 0, 'S': 1, 'A': 2, 'B1': 3, 'B2': 4, 'C': 5}.get(mode, 9)
        return (g_rank, mode_rank, -h.get('score', 0), -h.get('vol_ratio', 0), -h.get('amount_b', 0))

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


def _read_html_first_table(text: str) -> pd.DataFrame:
    try:
        tables = pd.read_html(text, match='날짜')
        if tables:
            return tables[0]
    except Exception:
        pass
    return pd.DataFrame()


def _get_intraday_flow_estimate(code: str, price: float = 0.0) -> dict:
    """
    네이버 종목별 외국인/기관 매매 페이지에서
    당일 시점 기준 추정 외인/기관 수급 스냅샷을 가져온다.
    주의: 공식 '최종치'가 아니라 장중/장마감 직후 기준 추정치일 수 있음.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://finance.naver.com/',
        }
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        res = requests.get(url, headers=headers, timeout=8)
        res.encoding = 'euc-kr'
        raw = _read_html_first_table(res.text)
        if raw is None or raw.empty:
            return {
                'flow_snapshot_ok': False,
                'flow_date': '',
                'inst_qty_est': 0,
                'frgn_qty_est': 0,
                'inst_amt_est_b': 0.0,
                'frgn_amt_est_b': 0.0,
                'fi_amt_est_b': 0.0,
                'flow_comment': '추정수급조회실패',
            }

        df = raw.dropna(how='all').copy()
        new_cols = ['_'.join(col) if isinstance(col, tuple) else str(col) for col in df.columns]
        df.columns = new_cols

        date_col = next((c for c in df.columns if '날짜' in c), None)
        inst_col = next((c for c in df.columns if '기관' in c and '순매매' in c), None)
        frgn_col = next((c for c in df.columns if '외국인' in c and '순매매' in c), None)

        if not inst_col or not frgn_col:
            return {
                'flow_snapshot_ok': False,
                'flow_date': '',
                'inst_qty_est': 0,
                'frgn_qty_est': 0,
                'inst_amt_est_b': 0.0,
                'frgn_amt_est_b': 0.0,
                'fi_amt_est_b': 0.0,
                'flow_comment': '추정수급컬럼없음',
            }

        latest = df.iloc[0]

        def _to_int(v):
            try:
                s = str(v).replace(',', '').replace('+', '').strip()
                if s in ('', 'nan', 'None'):
                    return 0
                return int(float(s))
            except Exception:
                return 0

        inst_qty = _to_int(latest.get(inst_col, 0))
        frgn_qty = _to_int(latest.get(frgn_col, 0))
        flow_date = str(latest.get(date_col, '')) if date_col else ''

        price_f = float(price or 0)
        inst_amt_b = round(inst_qty * price_f / 1e8, 1) if price_f > 0 else 0.0
        frgn_amt_b = round(frgn_qty * price_f / 1e8, 1) if price_f > 0 else 0.0
        fi_amt_b = round(inst_amt_b + frgn_amt_b, 1)

        parts = []
        if inst_qty > 0:
            parts.append('기관유입')
        elif inst_qty < 0:
            parts.append('기관유출')

        if frgn_qty > 0:
            parts.append('외인유입')
        elif frgn_qty < 0:
            parts.append('외인유출')

        if inst_qty > 0 and frgn_qty > 0:
            parts.append('쌍끌')
        elif inst_qty < 0 and frgn_qty < 0:
            parts.append('동반이탈')

        flow_comment = '/'.join(parts) if parts else '중립'

        return {
            'flow_snapshot_ok': True,
            'flow_date': flow_date,
            'inst_qty_est': inst_qty,
            'frgn_qty_est': frgn_qty,
            'inst_amt_est_b': inst_amt_b,
            'frgn_amt_est_b': frgn_amt_b,
            'fi_amt_est_b': fi_amt_b,
            'flow_comment': flow_comment,
        }

    except Exception as e:
        return {
            'flow_snapshot_ok': False,
            'flow_date': '',
            'inst_qty_est': 0,
            'frgn_qty_est': 0,
            'inst_amt_est_b': 0.0,
            'frgn_amt_est_b': 0.0,
            'fi_amt_est_b': 0.0,
            'flow_comment': f'추정수급오류:{e}',
        }


def _save_estimated_flow_snapshots(hits: list, scan_dt: datetime):
    """
    종가배팅 후보(hits)에 대해 15:20 전후 추정 외인/기관 수급 스냅샷을 CSV로 누적 저장.
    나중에 실전형 수급 백테스트용 원본으로 활용 가능.
    """
    if not hits:
        return

    _ensure_log_dir()
    rows = []
    scan_date = scan_dt.strftime('%Y-%m-%d')
    scan_time = scan_dt.strftime('%H:%M:%S')

    for h in hits:
        code = str(h.get('code', '')).zfill(6)
        close_price = float(h.get('close', 0) or 0)
        flow = _get_intraday_flow_estimate(code, close_price)
        rows.append({
            'key': f"{scan_date}|{code}|{h.get('mode','')}",
            'scan_date': scan_date,
            'scan_time': scan_time,
            'code': code,
            'name': h.get('name', ''),
            'mode': h.get('mode', ''),
            'mode_label': h.get('mode_label', ''),
            'grade': h.get('grade', ''),
            'score': h.get('score', 0),
            'close': close_price,
            'index_label': h.get('index_label', ''),
            'universe_tag': h.get('universe_tag', ''),
            'recommended_band': h.get('recommended_band', ''),
            'volatility_type': h.get('volatility_type', ''),
            'band_comment': h.get('band_comment', ''),
            'flow_snapshot_ok': int(bool(flow.get('flow_snapshot_ok', False))),
            'flow_date': flow.get('flow_date', ''),
            'inst_qty_est': flow.get('inst_qty_est', 0),
            'frgn_qty_est': flow.get('frgn_qty_est', 0),
            'inst_amt_est_b': flow.get('inst_amt_est_b', 0.0),
            'frgn_amt_est_b': flow.get('frgn_amt_est_b', 0.0),
            'fi_amt_est_b': flow.get('fi_amt_est_b', 0.0),
            'flow_comment': flow.get('flow_comment', ''),
        })

    new_df = pd.DataFrame(rows)
    if FLOW_SNAPSHOT_CSV.exists():
        try:
            old_df = pd.read_csv(FLOW_SNAPSHOT_CSV, dtype={'code': str, 'key': str}, encoding='utf-8-sig')
        except Exception:
            old_df = pd.DataFrame()
    else:
        old_df = pd.DataFrame()

    if old_df.empty:
        merged = new_df
    else:
        merged = pd.concat([old_df, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=['key'], keep='last')

    merged = merged.sort_values(['scan_date', 'scan_time', 'code']).reset_index(drop=True)
    try:
        merged.to_csv(FLOW_SNAPSHOT_CSV, index=False, encoding='utf-8-sig')
        log_info(f"✅ 추정 수급 스냅샷 저장: {len(new_df)}건 -> {FLOW_SNAPSHOT_CSV.name}")
    except Exception as e:
        log_error(f"⚠️ 추정 수급 스냅샷 저장 실패: {e}")


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
            'gap_pct': _safe_float(h.get('gap_pct', 0)),
            'vol50_ratio': _safe_float(h.get('vol50_ratio', 0)),
            'disparity20': _safe_float(h.get('disparity20', 0)),
            'runup20': _safe_float(h.get('runup20', 0)),
            'runup120': _safe_float(h.get('runup120', 0)),
            'near_high120': _safe_float(h.get('near_high120', 0)),
            'pullback_from_high': _safe_float(h.get('pullback_from_high', 0)),
            'close_loc_pct': _safe_float(h.get('close_loc_pct', 0)),
            'upper_wick_range_pct': _safe_float(h.get('upper_wick_range_pct', 0)),
            'vma5_20_ratio': _safe_float(h.get('vma5_20_ratio', 0)),
            'today_vol_ratio': _safe_float(h.get('today_vol_ratio', 0)),
            'target2': _safe_float(h.get('target2', 0)),
            'trail_ma': h.get('trail_ma', ''),
            'ma10_hold_ratio_35': _safe_float(h.get('ma10_hold_ratio_35', 0)),
            'ma10_cross_count_60': _safe_float(h.get('ma10_cross_count_60', 0)),
            'sell_rule': h.get('sell_rule', ''),
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
        now = _now_kst()
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
def _check_ymgp_bet(code: str, name: str) -> dict | None:
    """
    전략 C — 역매공파 (역배열 -> 매집 -> 공구리 -> 장기선 돌파)
    """
    try:
        code = _normalize_code(code)
        df = _load_df(code, lookback_days=730)
        if df is None or len(df) < 250:
            return None

        row = df.iloc[-1]
        info = _base_info(row, df)

        # 유니버스 필터링 (기존 전략과 동일)
        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        is_mcap_or = marcap >= MCAP_OR_MIN

        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            return None
        if info['_close'] < MIN_PRICE or info['amount_b'] < MIN_AMOUNT / 1e8:
            return None

        # --- 역매공파 조건 로직 ---
        recent_60 = df.iloc[-60:]
        past_idx = -60
        
        # 1. 역 (역배열 확인)
        ma112_past = df['MA112'].iloc[past_idx]
        ma224_past = df['MA224'].iloc[past_idx]
        close_past = df['Close'].iloc[past_idx]
        is_reverse = (ma112_past < ma224_past) and (close_past < ma112_past)

        # 2. 매 (매집봉 확인: 거래량 2배 이상 & 윗꼬리 3% 이상)
        spike_vol = recent_60['Volume'] > (recent_60['VMA20'] * 2.0)
        upper_tail = (recent_60['High'] - recent_60[['Open', 'Close']].max(axis=1)) / recent_60['Close'] > 0.03
        is_accumulation = (spike_vol & upper_tail).any()

        # 3. 공 (공구리: 최근 20일 저가가 60일 최저가의 95% 이상)
        min_60 = recent_60['Low'].min()
        min_20 = df.iloc[-20:]['Low'].min()
        is_concrete = min_20 >= (min_60 * 0.95)

        # 4. 파 (돌파: 오늘 종가가 112일선 또는 224일선 돌파)
        ma112_now = float(row.get('MA112', 0))
        ma224_now = float(row.get('MA224', 0))
        is_breakout = (info['_close'] >= ma112_now) or (info['_close'] >= ma224_now)

        if not (is_reverse and is_accumulation and is_concrete and is_breakout):
            return None

        band_rec = _get_band_recommendation(code, df, row, idx_label, code in TOP_MCAP_SET, is_mcap_or)
        
        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'C',
            'mode_label': '역매공파',
            'index_label': idx_label,
            'recommended_band': band_rec['recommended_band'],
            'support_band': band_rec['support_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': '장기 매물대 돌파 초입(YMGP)',
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'marcap': marcap,
            'score': 7,
            'close': info['_close'],
            'grade': '완전체',
            'kki_pattern': '바닥탈출대시세형',
            'kki_habit': '매집 완료 후 장기 저항 돌파',
            'kki_comment': '역매공파 타점 포착. 스윙 관점 유효.'
        }
    except Exception as e:
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


def _clean_band_comment_for_display(band_comment: str, recommended_band: str = '', support_band: str = '') -> str:
    """텔레그램 밴드 문구 중복 제거.

    _format_hit()가 이미 "주밴드 X / 보조밴드 Y"를 앞에 붙이므로,
    band_comment 안에 같은 문구가 다시 들어 있으면 뒤쪽 설명만 남긴다.
    """
    try:
        comment = str(band_comment or '').strip()
        if not comment:
            return ''

        rec = str(recommended_band or '').strip()
        sup = str(support_band or '').strip()
        prefixes = []
        if rec:
            p = f"주밴드 {rec}"
            if sup and sup != rec:
                p += f" / 보조밴드 {sup}"
            prefixes.append(p)
        prefixes.append('주밴드')

        for p in prefixes:
            if p and comment.startswith(p):
                # 예: "주밴드 BB40 / 보조밴드 ENV40 | 변동형 | ..." -> "변동형 | ..."
                if '|' in comment:
                    parts = [x.strip() for x in comment.split('|') if x.strip()]
                    if parts and parts[0].startswith('주밴드'):
                        comment = ' | '.join(parts[1:]).strip()
                break

        return comment
    except Exception:
        return str(band_comment or '').strip()


def _format_hit(hit: dict, rank: int = 0, mins_left: int = 0) -> str:
    def _g(*keys, default=""):
        for k in keys:
            if k in hit and hit.get(k) is not None:
                return hit.get(k)
        return default

    code = str(_g("code", "Code", default="")).strip()
    name = str(_g("name", "Name", "종목명", default=code)).strip() or code

    close = _safe_float(_g("close", "Close", "현재가", "price", "종가", "_close", default=0), 0.0)
    score = _safe_float(_g("score", "점수", default=0), 0.0)
    vol_ratio = _safe_float(_g("vol_ratio", "volume_ratio", default=0), 0.0)
    amount_b = _safe_float(_g("amount_b", "거래대금억", default=0), 0.0)

    grade = str(_g("grade_label", "grade", "등급", default="B급")).strip()
    strategy = str(_g("strategy", "mode", "전략", default="")).strip()
    mode_label = str(_g("mode_label", default=(strategy if strategy else "종가배팅"))).strip() or (strategy if strategy else "종가배팅")

    recommended_band = str(_g("recommended_band", default="")).strip()
    support_band = str(_g("support_band", default="")).strip()
    band_comment = str(_g("band_comment", "band_reason", default="")).strip()
    band_comment = _clean_band_comment_for_display(band_comment, recommended_band, support_band)
    gap_pct = _safe_float(_g("gap_pct", default=0), 0.0)
    vol50_ratio = _safe_float(_g("vol50_ratio", default=0), 0.0)
    runup120 = _safe_float(_g("runup120", default=0), 0.0)
    near_high120 = _safe_float(_g("near_high120", default=0), 0.0)
    pullback_from_high = _safe_float(_g("pullback_from_high", default=0), 0.0)
    close_loc_pct = _safe_float(_g("close_loc_pct", default=0), 0.0)
    upper_wick_range_pct = _safe_float(_g("upper_wick_range_pct", default=0), 0.0)
    vma5_20_ratio = _safe_float(_g("vma5_20_ratio", default=0), 0.0)
    volume_state = str(_g("volume_state", default="")).strip()
    if not volume_state:
        if vma5_20_ratio <= HIGH_REACCUM_VOLUME_DRY_MAX:
            volume_state = '응축'
        elif vma5_20_ratio <= HIGH_REACCUM_VOLUME_NORMAL_MAX:
            volume_state = '보통'
        else:
            volume_state = '재증가'
    today_vol_ratio = _safe_float(_g("today_vol_ratio", default=0), 0.0)
    rr_flag = str(_g("rr_flag", default="")).strip()
    target2 = _safe_float(_g("target2", default=0), 0.0)
    stop_logic = str(_g("stop_logic", default="")).strip()
    high_close_rule = str(_g("high_close_rule", default="")).strip()
    trail_ma = str(_g("trail_ma", default="")).strip()
    trail_reason = str(_g("trail_reason", default="")).strip()
    sell_rule = str(_g("sell_rule", default="")).strip()
    initial_stop_rule = str(_g("initial_stop_rule", default="")).strip()
    stoploss = _safe_float(_g("stoploss", default=0), 0.0)
    target1 = _safe_float(_g("target1", default=0), 0.0)
    rr = _safe_float(_g("rr", default=0), 0.0)

    kki_pattern = str(_g("kki_pattern", default="")).strip()
    kki_habit = str(_g("kki_habit", default="")).strip()
    kki_comment = str(_g("kki_comment", default="")).strip()
    kki_score = _safe_int(_g("kki_score", default=0), 0)
    absorb_score = _safe_int(_g("absorb_score", default=0), 0)

    idx_label = str(_g("index_label", default="")).strip()
    universe_tag = str(_g("universe_tag", default="")).strip()
    location = idx_label if idx_label else universe_tag

    passed = _g("passed", default=[])
    if isinstance(passed, (list, tuple)):
        passed_str = " · ".join(str(x) for x in passed if str(x).strip())
    else:
        passed_str = str(passed).strip()

    vol_ratio_text = f"{vol_ratio:.2f}" if vol_ratio > 0 else "확인필요"
    amount_text = f"{amount_b:.1f}억" if amount_b > 0 else "확인필요"

    mode_easy = {
        "돌파형": "저항 부근에서 종가가 강하게 살아 있는 타입",
        "ENV엄격형": "엔벨로프 하단권에서 보수적으로 받치는 타입",
        "BB확장형": "볼린저 하단권에서 변동성 확장을 노리는 타입",
        "모랄레스갭": "갭+거래량+갭지지를 동시에 통과한 주도주 출발 후보",
        "고점재응축": "강한 상승 뒤 고점권에서 무너지지 않고 종가가 위에서 잠기는 2차 슈팅 후보",
        "역매공파": "장기 저항을 돌파하며 바닥 구조에서 추세 전환을 시도하는 타입",
    }.get(mode_label, "종가 기준으로 선별된 후보")

    interpretation_parts = [mode_easy]
    if mode_label == "고점재응축":
        interpretation_parts.append("이미 크게 오른 주도주가 고점 부근에서 다시 힘을 모으는 구조")
        interpretation_parts.append("종가가 캔들 상단에서 잠겨야 다음날 2차 슈팅 가능성이 커짐")
    elif mode_label == "역매공파":
        interpretation_parts.append("역배열 바닥→매집→장기선 돌파 구조 확인")
        interpretation_parts.append("장기 저항 돌파 후에는 추격보다 눌림/재지지 확인이 핵심")
    else:
        if kki_pattern:
            interpretation_parts.append(f"끼 패턴은 '{kki_pattern}' 쪽")
        if kki_pattern == "하단이탈복귀형":
            interpretation_parts.append("하단 이탈 뒤 복귀라 단순 터치보다 복원력 확인이 핵심")
        elif kki_score >= 60:
            interpretation_parts.append("재상승 탄력 기대 가능")
        elif kki_score >= 35:
            interpretation_parts.append("약한 반등보다 눌림 확인이 유리")
        else:
            interpretation_parts.append("무리한 추격보다는 보수적 접근이 적절")

    lines = []
    display_name = name
    if display_name == code or (display_name.isdigit() and len(display_name) == 6):
        display_name = "종목명확인필요"
    s_type_label = str(_g('s_type_label', 's_type', default='')).strip()
    c_type_label = str(_g('c_type_label', default='')).strip()
    if mode_label == '고점재응축' and s_type_label:
        head = f"{rank}) {s_type_label} {grade} | {display_name}({code})"
    elif mode_label == '역매공파' and c_type_label:
        head = f"{rank}) {c_type_label} {grade} | {display_name}({code})"
    else:
        head = f"{rank}) {mode_label} {grade} | {display_name}({code})"
    if location:
        head += f" | {location}"
    lines.append(head)
    lines.append(f"   현재가 {int(close):,}원 | 점수 {score:.1f} | 거래량비 {vol_ratio_text} | 거래대금 {amount_text}")

    # v2.8: 고점재응축(S)은 S1 관찰형 / S2 실행형을 명확히 분리해서 출력
    if mode_label == "고점재응축":
        ref_bits = []
        if recommended_band:
            ref_bits.append(f"참고 {recommended_band}")
        if support_band and support_band != recommended_band:
            ref_bits.append(f"보조 {support_band}")
        ref_text = " · ".join(ref_bits)
        basis = "고점권 재응축+종가고점마감" + (f" / {ref_text}" if ref_text else "")
        lines.append(f"   기준: {basis}")
        lines.append(f"   핵심: 120일 {runup120:+.1f}% 상승 후 고점대비 -{pullback_from_high:.1f}% | 고점근접 {near_high120:.1f}%")
        lines.append(f"   종가: 캔들상단 {close_loc_pct:.0f}% 마감 / 윗꼬리 {upper_wick_range_pct:.0f}% | 거래량상태 {volume_state}(VMA5/VMA20 {vma5_20_ratio:.2f})")
        execution_verdict = str(_g('execution_verdict', default='')).strip()
        if execution_verdict:
            lines.append(f"   판정: {execution_verdict}")
        if stoploss > 0:
            rr_txt = f"RR {rr:.2f}" + (f"({rr_flag})" if rr_flag else "")
            lines.append(f"   진입/손절: 종가상단 유지·전고점 재돌파 확인 | 손절 {int(stoploss):,}원 종가이탈 | {rr_txt}")
        if s_type_label.startswith('S2'):
            caution = "실행형 — 단, 고점권이므로 실패 시 손절선 엄수"
        elif rr < HIGH_REACCUM_RR_GOOD_MIN:
            caution = "관찰형+RR 낮음 — 전고점 돌파 확인 전 신규진입 보수"
        else:
            caution = "관찰형 — 다음날 거래량 1.2배↑ 또는 전고점 재돌파 확인"
        lines.append(f"   주의: {caution}")
        return "\n".join(lines)

    if recommended_band:
        if mode_label == "ENV엄격형":
            band_line = "   밴드: 전략밴드 ENV20&ENV40"
            refs = []
            if recommended_band:
                refs.append(f"추천 {recommended_band}")
            if support_band and support_band != recommended_band:
                refs.append(f"보조 {support_band}")
            if refs:
                band_line += " / " + " · ".join(refs)
        elif mode_label == "BB확장형":
            band_line = "   밴드: 전략밴드 BB40"
            refs = []
            if recommended_band and recommended_band != "BB40":
                refs.append(f"추천 {recommended_band}")
            if support_band and support_band not in ("", "BB40", recommended_band):
                refs.append(f"보조 {support_band}")
            if refs:
                band_line += " / " + " · ".join(refs)
        elif mode_label == "고점재응축":
            band_line = "   기준: 고점권 재응축+종가고점마감"
            if recommended_band:
                band_line += f" / 참고 {recommended_band}"
            if support_band and support_band != recommended_band:
                band_line += f" · 보조 {support_band}"
        elif mode_label == "모랄레스갭":
            band_line = "   밴드: 갭돌파 기준"
            if recommended_band:
                band_line += f" / 참고 {recommended_band}"
            if support_band and support_band != recommended_band:
                band_line += f" · 보조 {support_band}"
        else:
            band_line = f"   밴드: 주밴드 {recommended_band}"
            if support_band and support_band != recommended_band:
                band_line += f" / 보조밴드 {support_band}"
        if band_comment:
            band_line += f" | {band_comment}"
        lines.append(band_line)

    if passed_str:
        lines.append(f"   통과근거: {passed_str}")

    if mode_label == "돌파형":
        lines.append("   쉬운설명: 아래에서 받치는 자리라기보다, 이미 위로 붙을 힘이 남아 있는 종목입니다.")
    elif mode_label == "ENV엄격형":
        lines.append("   쉬운설명: 크게 뜬 자리보다 아래에서 다시 받치기 시작하는 보수형 반등 후보입니다.")
        lines.append("   판정: 관찰형 — 백테스트상 손절선행이 높아 오늘 종가 즉시매수보다 다음날 양봉/전일고가 돌파 확인이 우선입니다.")
    elif mode_label == "BB확장형":
        lines.append("   쉬운설명: 볼린저 하단권에서 움직임이 다시 커질 수 있는 종목입니다.")
        lines.append("   판정: 관찰형 — 하단 재안착 후보지만 다음날 반등 확인 전에는 신규진입을 보수적으로 봅니다.")
    elif mode_label == "고점재응축":
        lines.append("   쉬운설명: 이미 크게 오른 뒤에도 고점 부근에서 무너지지 않고, 종가가 캔들 위쪽에서 잠긴 2차 슈팅 후보입니다.")
        lines.append(f"   2차슈팅조건: 120일상승 {runup120:+.1f}% | 고점근접 {near_high120:.1f}% | 고점대비하락 {pullback_from_high:.1f}%")
        lines.append(f"   종가확인: 종가위치 {close_loc_pct:.0f}% | 윗꼬리 {upper_wick_range_pct:.0f}% | 거래량상태 {volume_state}(VMA5/VMA20 {vma5_20_ratio:.2f}) | 당일거래량 {today_vol_ratio:.2f}배")
        if stoploss > 0:
            lines.append(f"   손절/목표: 손절 {int(stoploss):,}원 | 1차목표 {int(target1):,}원 | 2차목표 {int(target2):,}원 | RR {rr:.2f}")
        if stop_logic:
            lines.append(f"   손절산식: {stop_logic}")
        if initial_stop_rule:
            lines.append(f"   초기실패: {initial_stop_rule}")
    elif mode_label == "모랄레스갭":
        lines.append("   쉬운설명: 단순 갭상승이 아니라 거래량·갭지지·신고가/박스돌파를 함께 통과한 갭 후보입니다.")
        if gap_pct or vol50_ratio:
            lines.append(f"   갭품질: 갭 {gap_pct:+.1f}% | Vol50 {vol50_ratio:.2f}배 | 초기손절 {int(stoploss):,}원 | 목표 {int(target1):,}원 | RR {rr:.2f}")
        if trail_ma:
            lines.append(f"   보유선: {trail_ma} | {trail_reason}")
        if sell_rule:
            lines.append(f"   매도기준: {sell_rule}")
        if initial_stop_rule:
            lines.append(f"   초기실패: {initial_stop_rule}")
    elif mode_label == "역매공파":
        lines.append("   쉬운설명: 역배열 바닥에서 매집 흔적을 만든 뒤 장기 저항을 돌파하려는 스윙형 후보입니다.")

    if mode_label == "역매공파":
        lines.append("   구조분석: 역배열 바닥 → 매집흔적 → 공구리/장기선 돌파")
    elif kki_pattern or kki_score > 0 or absorb_score > 0:
        lines.append(f"   끼 분석: {kki_pattern or '혼합형'} | 끼 {kki_score} / 흡수 {absorb_score}")

    def _dedupe_text_parts(parts):
        out = []
        for part in parts:
            txt = str(part or '').strip()
            if not txt:
                continue
            duplicate = False
            for prev in out:
                if txt == prev or txt in prev or prev in txt or txt[:24] == prev[:24]:
                    duplicate = True
                    break
            if not duplicate:
                out.append(txt)
        return " ".join(out).strip()

    natural_kki = _dedupe_text_parts([kki_habit, kki_comment])
    if natural_kki:
        lines.append(f"   해석: {natural_kki}")

    lines.append(f"   한줄해석: {' / '.join(interpretation_parts)}")

    if hit.get("mode") in ("B1", "B2") and hit.get("maejip_chart"):
        lines.append(f"   매집흔적: {hit.get('maejip_chart')}")

    return "\n".join(lines)

def _call_anthropic_text(system_msg: str, user_msg: str, max_tokens: int = 900) -> str:
    if not ANTHROPIC_API_KEY:
        return ''
    res = requests.post(
        'https://api.anthropic.com/v1/messages',
        headers={
            'Content-Type': 'application/json',
            'x-api-key': ANTHROPIC_API_KEY,
            'anthropic-version': '2023-06-01',
        },
        json={
            'model': os.environ.get('CLOSING_BET_ANTHROPIC_MODEL', 'claude-sonnet-4-20250514'),
            'max_tokens': max_tokens,
            'system': system_msg,
            'messages': [{'role': 'user', 'content': user_msg}],
        },
        timeout=40,
    )
    data = res.json()
    if 'content' in data and data['content']:
        return data['content'][0].get('text', '').strip()
    return ''


def _call_openai_text(system_msg: str, user_msg: str, max_tokens: int = 900) -> str:
    if not OPENAI_API_KEY:
        return ''
    from openai import OpenAI as _OAI
    client = _OAI(api_key=OPENAI_API_KEY)
    res = client.chat.completions.create(
        model=os.environ.get('CLOSING_BET_OPENAI_MODEL', 'gpt-4o-mini'),
        messages=[
            {'role': 'system', 'content': system_msg},
            {'role': 'user', 'content': user_msg},
        ],
        max_tokens=max_tokens,
    )
    return (res.choices[0].message.content or '').strip()


def _call_groq_text(system_msg: str, user_msg: str, max_tokens: int = 900) -> str:
    if not GROQ_API_KEY:
        return ''
    res = requests.post(
        'https://api.groq.com/openai/v1/chat/completions',
        headers={
            'Authorization': f'Bearer {GROQ_API_KEY}',
            'Content-Type': 'application/json',
        },
        json={
            'model': os.environ.get('CLOSING_BET_GROQ_MODEL', 'llama-3.3-70b-versatile'),
            'messages': [
                {'role': 'system', 'content': system_msg},
                {'role': 'user', 'content': user_msg},
            ],
            'max_tokens': max_tokens,
            'temperature': 0.2,
        },
        timeout=40,
    )
    data = res.json()
    return (((data.get('choices') or [{}])[0].get('message') or {}).get('content') or '').strip()


def _call_gemini_text(system_msg: str, user_msg: str, max_tokens: int = 900) -> str:
    if not GEMINI_API_KEY:
        return ''
    model = os.environ.get('CLOSING_BET_GEMINI_MODEL', 'gemini-2.0-flash')
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}'
    res = requests.post(
        url,
        headers={'Content-Type': 'application/json'},
        json={
            'systemInstruction': {'parts': [{'text': system_msg}]},
            'contents': [{'parts': [{'text': user_msg}]}],
            'generationConfig': {'temperature': 0.2, 'maxOutputTokens': max_tokens},
        },
        timeout=40,
    )
    data = res.json()
    candidates = data.get('candidates') or []
    if candidates:
        parts = ((candidates[0].get('content') or {}).get('parts') or [])
        texts = [p.get('text', '') for p in parts if p.get('text')]
        return '\n'.join(texts).strip()
    return ''


def _call_llm_with_fallback(system_msg: str, user_msg: str, role_label: str = '', max_tokens: int = 900, provider_order=None):
    if provider_order is None:
        provider_order = ['anthropic', 'openai', 'gemini', 'groq']

    providers = {
        'anthropic': _call_anthropic_text,
        'openai': _call_openai_text,
        'gemini': _call_gemini_text,
        'groq': _call_groq_text,
    }

    errors = []
    for provider in provider_order:
        fn = providers.get(provider)
        if not fn:
            continue
        try:
            text = fn(system_msg, user_msg, max_tokens=max_tokens)
            if text:
                log_info(f"✅ {role_label} LLM 성공: {provider}")
                return text, provider
        except Exception as e:
            errors.append(f'{provider}:{e}')
            log_error(f"⚠️ {role_label} {provider} 실패: {e}")

    if errors:
        log_error(f"⚠️ {role_label} 전체 실패: {' | '.join(errors)}")
    return '', 'none'




def _provider_tag(provider: str) -> str:
    mapping = {
        'openai': '[GPT]',
        'anthropic': '[Claude]',
        'gemini': '[Gemini]',
        'groq': '[Groq]',
        'none': '[None]',
    }
    return mapping.get(str(provider).lower(), f"[{provider}]")



def _normalize_debate_label(text: str, kind: str = 'role') -> str:
    t = str(text or '').strip().replace(' ', '')
    if kind == 'role':
        if '조건부' in t and '추천' in t:
            return '조건부추천'
        if '추천' in t:
            return '추천'
        if '보류' in t:
            return '보류'
        if '제외' in t:
            return '제외'
        return t or '미출력'
    if '조건부' in t and '진입' in t:
        return '조건부진입'
    if '진입' in t:
        return '진입'
    if '보류' in t:
        return '보류'
    if '제외' in t:
        return '제외'
    return t or '미출력'


def _extract_json_payload(text: str):
    if not text:
        return None
    raw = str(text).strip()

    fenced = re.findall(r"```(?:json)?\s*(.*?)```", raw, flags=re.DOTALL | re.IGNORECASE)
    candidates = []
    if fenced:
        candidates.extend([c.strip() for c in fenced if c.strip()])

    candidates.append(raw)

    # first array/object slice
    first_arr = raw.find('[')
    last_arr = raw.rfind(']')
    if first_arr != -1 and last_arr != -1 and last_arr > first_arr:
        candidates.append(raw[first_arr:last_arr + 1])

    first_obj = raw.find('{')
    last_obj = raw.rfind('}')
    if first_obj != -1 and last_obj != -1 and last_obj > first_obj:
        candidates.append(raw[first_obj:last_obj + 1])

    seen = set()
    for cand in candidates:
        cand = cand.strip()
        if not cand or cand in seen:
            continue
        seen.add(cand)
        try:
            return json.loads(cand)
        except Exception:
            continue
    return None


def _coerce_role_json(parsed, count: int) -> dict:
    rows = {}
    if isinstance(parsed, dict):
        if isinstance(parsed.get('items'), list):
            parsed = parsed.get('items')
        else:
            parsed = [parsed]
    if not isinstance(parsed, list):
        return rows

    for item in parsed:
        if not isinstance(item, dict):
            continue
        idx = _safe_int(item.get('candidate') or item.get('idx') or item.get('후보') or item.get('번호'), 0)
        if idx <= 0 or idx > count:
            continue
        rows[idx] = {
            'verdict': _normalize_debate_label(item.get('stance') or item.get('판정') or item.get('verdict') or '', kind='role'),
            'score': _safe_int(item.get('score') or item.get('점수') or 0, 0),
            'summary': str(item.get('core_reason') or item.get('요약') or item.get('summary') or item.get('근거') or '').strip(),
            'risk': str(item.get('risk') or item.get('리스크') or '').strip(),
            'plan': str(item.get('plan') or item.get('실행계획') or item.get('action_plan') or '').strip(),
        }
    return rows


def _coerce_judge_json(parsed, count: int) -> dict:
    rows = {}
    if isinstance(parsed, dict):
        if isinstance(parsed.get('items'), list):
            parsed = parsed.get('items')
        else:
            parsed = [parsed]
    if not isinstance(parsed, list):
        return rows

    for item in parsed:
        if not isinstance(item, dict):
            continue
        idx = _safe_int(item.get('candidate') or item.get('idx') or item.get('후보') or item.get('번호'), 0)
        if idx <= 0 or idx > count:
            continue
        rows[idx] = {
            'final_verdict': _normalize_debate_label(item.get('final_verdict') or item.get('최종판정') or item.get('verdict') or '', kind='judge'),
            'confidence': _safe_int(item.get('confidence') or item.get('확신도') or item.get('score') or 0, 0),
            'summary': str(item.get('summary') or item.get('한줄') or item.get('strong_point') or '').strip(),
            'strong_point': str(item.get('strong_point') or item.get('핵심근거') or '').strip(),
            'risk_point': str(item.get('risk_point') or item.get('위험요인') or '').strip(),
            'action_plan': str(item.get('action_plan') or item.get('실행계획') or item.get('plan') or '').strip(),
            'stop_note': str(item.get('stop') or item.get('손절') or '').strip(),
            'target_note': str(item.get('target') or item.get('목표') or '').strip(),
        }
    return rows


def _build_role_json_prompt(base_context: str, role_name: str) -> str:
    role_guides = {
        '기술분석가': (
            '차트/캔들/종가강도/ENV/BB/전고점 이격만 중심으로 본다. '
            '핵심 수치(거래량배수, 윗꼬리%, 전고점%, Env/BB 거리)를 최소 1개 이상 직접 언급하라. '
            '지금 자리가 재안착인지 추격인지 명확히 구분하라.'
        ),
        '수급분석가': (
            '외인/기관/OBV/매집흔적 중심으로 본다. '
            '수급추정 수치(기관추정, 외인추정, 외인기관합)와 흐름 해석을 연결하라. '
            '들어온 흔적은 있는데 크게 나간 흔적은 없는지 판단하라.'
        ),
        '시황테마분석가': (
            '업종/섹터/대장주/뉴스 맥락을 본다. '
            '후보 정보만으로 확실한 시황 근거가 부족하면 반드시 "시황 근거 부족"을 명시하라. '
            '억지 확신을 만들지 말고, 단순 눌림인지 관심 이탈인지 구분하라.'
        ),
        '리스크관리자': (
            '가장 보수적으로 본다. '
            '다음날 갭리스크, 손절 명확성, 늦은 자리 여부를 판단하라. '
            '실행계획에는 반드시 손절/축소/관망 중 하나가 드러나야 한다.'
        ),
    }
    role_rule = role_guides.get(role_name, '후보를 평가하라.')
    return (
        base_context + "\n\n"
        + f"너의 역할은 {role_name}이다. {role_rule} "
        + "모든 후보를 빠짐없이 평가하라. 반드시 JSON 배열만 출력하라. 다른 설명, 마크다운, 코드블록 금지. "
        + "각 원소 형식: "
        + '[{"candidate":1,"stance":"추천|조건부추천|보류|제외","score":0,"core_reason":"핵심 근거 1문장(가능하면 수치 포함)","risk":"핵심 리스크 1문장","plan":"실행 포인트 1문장"}]'
    )

def _build_judge_json_prompt(base_context: str, role_payloads: dict, count: int) -> str:
    role_json = json.dumps(role_payloads, ensure_ascii=False)
    return (
        base_context
        + "\n\n아래는 역할별 구조화 의견(JSON)이다.\n"
        + role_json
        + "\n\n모든 후보를 빠짐없이 최종 판정하라. "
        + "기술/수급/시황/리스크 의견을 함께 읽고, 가장 강한 근거와 가장 큰 위험을 분리해서 써라. "
        + "실행계획은 종가배팅 관점에서 진입/관망/손절 기준이 드러나야 한다. "
        + "시황 근거가 약하면 그 사실을 요약에 명시하라. 반드시 JSON 배열만 출력하라. 다른 설명, 마크다운, 코드블록 금지. "
        + "각 원소 형식: "
        + '[{"candidate":1,"final_verdict":"진입|조건부진입|보류|제외","confidence":0,"strong_point":"가장 강한 근거 1문장","risk_point":"가장 큰 위험 1문장","action_plan":"진입/관망/손절 계획 1문장","stop":"손절가/조건","target":"목표가/조건","summary":"최종 한줄 요약"}]'
    )

def _run_role_json(role_name: str, role_system: str, base_context: str, candidates: list, provider_order=None) -> tuple[dict, str]:
    if provider_order is None:
        provider_order = ['anthropic', 'openai', 'gemini', 'groq']
    user_msg = _build_role_json_prompt(base_context, role_name)
    text, provider = _call_llm_with_fallback(role_system, user_msg, role_label=role_name, max_tokens=2200, provider_order=provider_order)
    parsed = _coerce_role_json(_extract_json_payload(text), len(candidates))

    missing = [idx for idx in range(1, len(candidates) + 1) if idx not in parsed]
    for idx in missing:
        h = candidates[idx - 1]
        single_context = (
            f"[종가배팅 단일 후보 검토]\n"
            f"후보 {idx}: {h['name']}({h['code']}) | 전략:{h.get('mode_label','')} | 등급:{h.get('grade','')} | 점수:{h.get('score',0)} | "
            f"가격:{h['close']:,}원 | 거래대금:{h.get('amount_b',0)}억 | 거래량:{h.get('vol_ratio',0)}배 | 주밴드:{h.get('recommended_band','')} | 보조:{h.get('support_band','')} | 유형:{h.get('volatility_type','')} | 유니버스:{h.get('universe_tag','')}\n"
            f"종가강도/캔들: 윗꼬리(몸통){h.get('wick_pct',0)}% | 목표:{h.get('target1',0):,} | 손절:{h.get('stoploss',0):,} | RR:{h.get('rr',0)}\n"
            f"밴드/전략세부: {h.get('band_pct_text','')} | {h.get('band_comment','')}\n끼/습성: {h.get('kki_pattern','')} | 끼점수:{h.get('kki_score',0)} | 흡수점수:{h.get('absorb_score',0)} | {h.get('kki_comment','')}\n"
            f"수급추정: {h.get('flow_comment','수급추정정보없음')}"
        )
        single_user = (
            single_context
            + "\n\n반드시 JSON 배열만 출력하라. 다른 설명 금지. "
            + '[{"candidate":1,"stance":"추천|조건부추천|보류|제외","score":0,"core_reason":"핵심 근거 1문장","risk":"핵심 리스크 1문장","plan":"실행 포인트 1문장"}]'
        )
        text2, provider2 = _call_llm_with_fallback(role_system, single_user, role_label=f'{role_name}-단일재시도', max_tokens=600, provider_order=provider_order)
        parsed2 = _coerce_role_json(_extract_json_payload(text2), 1)
        if 1 in parsed2:
            item = parsed2[1]
            parsed[idx] = item
            provider = provider2 or provider

    for idx in range(1, len(candidates) + 1):
        parsed.setdefault(idx, {
            'verdict': '보류',
            'score': 0,
            'summary': '데이터 부족 또는 응답 불완전',
            'risk': '판단 유보',
            'plan': '추가 확인 필요',
        })
    return parsed, provider


def _run_judge_json(judge_system: str, base_context: str, role_payloads: dict, candidates: list) -> tuple[dict, str]:
    judge_user = _build_judge_json_prompt(base_context, role_payloads, len(candidates))
    text, provider = _call_llm_with_fallback(judge_system, judge_user, role_label='최종심판', max_tokens=2600, provider_order=['anthropic', 'openai', 'gemini', 'groq'])
    parsed = _coerce_judge_json(_extract_json_payload(text), len(candidates))

    missing = [idx for idx in range(1, len(candidates) + 1) if idx not in parsed]
    for idx in missing:
        role_subset = {k: v.get(idx, {}) for k, v in role_payloads.items()}
        single_user = (
            f"[종가배팅 최종심판 단일 후보]\n후보:{idx}\n역할의견(JSON):\n" + json.dumps(role_subset, ensure_ascii=False) +
            "\n\n반드시 JSON 배열만 출력하라. 다른 설명 금지. "
            + '[{"candidate":1,"final_verdict":"진입|조건부진입|보류|제외","confidence":0,"strong_point":"가장 강한 근거","risk_point":"가장 큰 위험","action_plan":"실행계획 1문장","stop":"손절가/조건","target":"목표가/조건","summary":"최종 한줄 요약"}]'
        )
        text2, provider2 = _call_llm_with_fallback(judge_system, single_user, role_label='최종심판-단일재시도', max_tokens=800, provider_order=['anthropic', 'openai', 'gemini', 'groq'])
        parsed2 = _coerce_judge_json(_extract_json_payload(text2), 1)
        if 1 in parsed2:
            parsed[idx] = parsed2[1]
            provider = provider2 or provider

    for idx in range(1, len(candidates) + 1):
        parsed.setdefault(idx, {
            'final_verdict': '보류',
            'confidence': 0,
            'summary': '데이터 부족 또는 응답 불완전',
            'strong_point': '',
            'risk_point': '판단 유보',
            'action_plan': '추가 확인 필요',
            'stop_note': '',
            'target_note': '',
        })
    return parsed, provider


def _debate_sort_key(hit: dict):
    grade = str(hit.get('grade', ''))
    g_rank = 0 if '완전체' in grade else (1 if 'A급' in grade else 2)
    mode = str(hit.get('mode', ''))
    mode_rank = {'G': 0, 'S': 1, 'A': 2, 'B1': 3, 'B2': 4, 'C': 5}.get(mode, 9)
    return (g_rank, mode_rank, -hit.get('score', 0), -hit.get('amount_b', 0), -hit.get('vol_ratio', 0))


def _select_debate_candidates(hits: list, top_n: int = None) -> list:
    if top_n is None:
        top_n = CLOSING_BET_DEBATE_TOP_N
    pool = sorted(hits, key=_debate_sort_key)
    return pool[:top_n]


def _build_debate_candidate_lines(candidates: list) -> str:
    lines = []
    for idx, h in enumerate(candidates, 1):
        flow_bits = []
        if h.get('inst_amt_est_b', 0) or h.get('frgn_amt_est_b', 0) or h.get('fi_amt_est_b', 0):
            flow_bits.append(
                f"기관추정:{h.get('inst_amt_est_b', 0):+.1f}억 | 외인추정:{h.get('frgn_amt_est_b', 0):+.1f}억 | 합산:{h.get('fi_amt_est_b', 0):+.1f}억"
            )
        if h.get('flow_comment'):
            flow_bits.append(h.get('flow_comment'))
        flow_text = ' | '.join(flow_bits) if flow_bits else '수급추정정보없음'

        if h.get('mode') == 'A':
            setup_text = (
                f"돌파세부: 전고점근접 {h.get('near20', 0):.1f}% | 이격 {h.get('disp', 0):.1f} | 거래량 {h.get('vol_ratio', 0)}배 | 윗꼬리(몸통) {h.get('wick_pct', 0)}%"
            )
        elif h.get('mode') == 'G':
            setup_text = (
                f"모랄레스갭: 갭 {h.get('gap_pct', 0):+.1f}% | Vol50 {h.get('vol50_ratio', 0)}배 | 이격 {h.get('disparity20', 0)} | 20일상승 {h.get('runup20', 0):+.1f}% | 보유선 {h.get('trail_ma', '')}"
            )
        elif h.get('mode') == 'C':
            setup_text = (
                f"역매공파세부: 장기저항 돌파 | 현재가 {h.get('close', 0):,} | 거래대금 {h.get('amount_b', 0)}억 | {h.get('band_comment', '')}"
            )
        else:
            setup_text = (
                f"하단세부: {h.get('band_pct_text', '')} | RSI {h.get('rsi', 0)} | 5일매집 {h.get('maejip_5d', 0)}회 | OBV {'상승' if h.get('obv_rising') else '중립/약세'}"
            )

        lines.append(
            f"후보 {idx}: {h['name']}({h['code']}) | 전략:{h.get('mode_label','')} | 등급:{h.get('grade','')} | 점수:{h.get('score',0)} | 가격:{h['close']:,}원 | 거래대금:{h.get('amount_b',0)}억 | 주밴드:{h.get('recommended_band','')} | 보조:{h.get('support_band','')} | 유형:{h.get('volatility_type','')} | 유니버스:{h.get('universe_tag','')}\n"
            f"기술세부: {setup_text}\n"
            f"공통세부: 목표:{h.get('target1',0):,} | 손절:{h.get('stoploss',0):,} | RR:{h.get('rr',0)} | ATR:{h.get('atr',0):,}\n"
            f"밴드/전략코멘트: {h.get('band_comment','')}\n끼/습성: {h.get('kki_pattern','')} | 끼점수:{h.get('kki_score',0)} | 흡수점수:{h.get('absorb_score',0)} | {h.get('kki_comment','')}\n"
            f"수급추정: {flow_text}\n"
            f"통과조건: {' '.join(h.get('passed', [])) if h.get('passed') else '없음'}"
        )
    return '\n\n'.join(lines)

def _run_role_brief(role_name: str, system_msg: str, user_msg: str, provider_order=None) -> tuple[str, str]:
    return _call_llm_with_fallback(system_msg, user_msg, role_label=role_name, max_tokens=1200, provider_order=provider_order)



def _debate_llm_runner(system_prompt: str, user_prompt: str, preferred_models=None, role_name: str = ''):
    max_tokens = 2600 if '심판' in str(role_name) else 2200
    text, provider = _call_llm_with_fallback(
        system_prompt,
        user_prompt,
        role_label=role_name,
        max_tokens=max_tokens,
        provider_order=list(preferred_models) if preferred_models else None,
    )
    return text, provider


def _send_closing_bet_debate(hits: list, mins_left: int, top_n: int = None):
    if top_n is None:
        top_n = CLOSING_BET_DEBATE_TOP_N
    if not hits:
        return

    try:
        now = _now_kst()
        market_context = os.environ.get('CLOSING_BET_MARKET_CONTEXT', '').strip()
        result = run_closing_bet_debate_pipeline(
            hits=hits,
            llm_runner=_debate_llm_runner,
            now_dt=now,
            mins_left=mins_left,
            top_n=top_n,
            extra_market_context=market_context,
            role_model_prefs={
                'tech': ['anthropic', 'openai', 'gemini', 'groq'],
                'flow': ['openai', 'anthropic', 'groq', 'gemini'],
                'theme': ['anthropic', 'gemini', 'openai', 'groq'],
                'risk': ['openai', 'groq', 'anthropic', 'gemini'],
                'judge': ['anthropic', 'openai', 'gemini', 'groq'],
            },
        )

        judgment_rows = result.get('judgment_rows', []) or []
        telegram_text = result.get('telegram_text', '') or ''

        if judgment_rows:
            _save_ai_judgments_to_gsheet(judgment_rows)
        if telegram_text.strip():
            send_telegram_chunks(telegram_text, max_len=3500)
    except Exception as e:
        log_error(f"⚠️ 종가배팅 AI 토론 실패: {e}")


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
                f"- {h['name']}({h['code']}): 현재가={h.get('close', h.get('_close', 0)):,}원 | "
                f"거래량={h.get('vol_ratio', round(h.get('_vol', 0) / h.get('_vma20', 1), 2) if h.get('_vma20', 0) > 0 else 0)}배 | 전고점={h.get('near20', 0)}% | "
                f"이격={h.get('disp', 0)} | 윗꼬리={h.get('wick_pct', round(h.get('_upper_wick_body', 0) * 100, 1))}% | "
                f"목표={h.get('target1', 0):,} 손절={h.get('stoploss', 0):,} | "
                f"지수={h.get('index_label', '')}"
                for h in hits
            ])
            strategy_context = (
                "전략 A는 전고점 돌파형 종가배팅이다. "
                "전고점 85~100% 구간에서 거래량이 터지고 종가가 강하게 잠기는 패턴이다."
            )
        elif strategy == 'B1':
            data_lines = '\n'.join([
                f"- {h['name']}({h['code']}): 현재가={h.get('close', h.get('_close', 0)):,}원 | "
                f"Env20={h.get('env20_pct', 0):+.1f}% | Env40={h.get('env40_pct', 0):+.1f}% | "
                f"RSI={h.get('rsi', 0)} | 5일매집={h.get('maejip_5d', 0)}회 | "
                f"OBV={'↑' if h.get('obv_rising') else '↓'} | "
                f"목표={h.get('target1', 0):,} 손절={h.get('stoploss', 0):,} | "
                f"지수={h.get('index_label', '')}"
                for h in hits
            ])
            strategy_context = (
                "전략 B1은 HTS와 같은 ENV 엄격형이다. "
                "Env20 하단 2% 이내와 Env40 하단 10% 이내를 동시에 만족하는 깊은 바닥 반등형이다."
            )
        else:
            data_lines = '\n'.join([
                f"- {h['name']}({h['code']}): 현재가={h.get('close', h.get('_close', 0)):,}원 | "
                f"BB40={h.get('bb40_pct', 0):+.1f}% | BB폭={h.get('bb40_width', 0):.1f}% | "
                f"RSI={h.get('rsi', 0)} | 5일매집={h.get('maejip_5d', 0)}회 | "
                f"OBV={'↑' if h.get('obv_rising') else '↓'} | "
                f"ATR={h.get('atr_pct', 0)}% | "
                f"목표={h.get('target1', 0):,} 손절={h.get('stoploss', 0):,} | "
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
        msg = (
            f"[{TODAY_STR}] 종가배팅 후보 없음\n"
            f"(대상: {SCAN_UNIVERSE} | 조건 미충족)"
        )
        log_info("→ 후보 없음 메시지 전송")
        send_telegram_photo(msg, [])
        return

    def _pick_strategy(hit):
        return str(hit.get("strategy") or hit.get("mode") or hit.get("전략") or "").strip()

    def _grade_core(hit):
        g_raw = str(hit.get("grade", "")).strip()
        g = g_raw.upper()
        if g == "COMPLETE" or "완전체" in g_raw:
            return "COMPLETE"
        if g == "A" or "A급" in g_raw:
            return "A"
        return "B"

    def _safe_score(hit):
        return _safe_float(hit.get("score", hit.get("점수", 0)), 0.0)

    def _priority(h):
        gc = _grade_core(h)
        g_rank = 0 if gc == "COMPLETE" else (1 if gc == "A" else 2)
        return (
            g_rank,
            -_safe_score(h),
            -_safe_float(h.get("vol_ratio", h.get("volume_ratio", 0)), 0.0),
            -_safe_float(h.get("amount_b", 0), 0.0),
        )

    def _priority_s(h):
        gc = _grade_core(h)
        g_rank = 0 if gc == "COMPLETE" else (1 if gc == "A" else 2)
        s_type = str(h.get('s_type', '') or '')
        s_rank = 0 if s_type == 'S2' else 1
        return (
            s_rank,
            g_rank,
            -_safe_score(h),
            -_safe_float(h.get("today_vol_ratio", h.get("vol_ratio", 0)), 0.0),
            -_safe_float(h.get("rr", 0), 0.0),
            -_safe_float(h.get("amount_b", 0), 0.0),
        )

    hits_g = [x for x in hits if _pick_strategy(x) == "G"]
    hits_s = [x for x in hits if _pick_strategy(x) == "S"]
    hits_s2 = [x for x in hits_s if str(x.get('s_type','')) == 'S2']
    hits_s1 = [x for x in hits_s if str(x.get('s_type','')) != 'S2']
    hits_a = [x for x in hits if _pick_strategy(x) == "A"]
    hits_b1 = [x for x in hits if _pick_strategy(x) == "B1"]
    hits_b2 = [x for x in hits if _pick_strategy(x) == "B2"]
    hits_c = [x for x in hits if _pick_strategy(x) == "C"]

    hits_g.sort(key=_priority)
    hits_s.sort(key=_priority_s)
    hits_a.sort(key=_priority)
    hits_b1.sort(key=_priority)
    hits_b2.sort(key=_priority)
    hits_c.sort(key=_priority)

    complete_hits = [x for x in hits if _grade_core(x) == "COMPLETE"]
    a_grade_hits = [x for x in hits if _grade_core(x) == "A"]
    b_grade_hits = [x for x in hits if _grade_core(x) == "B"]

    total = (
        min(len(hits_g), 5) + min(len(hits_s), 5) + min(len(hits_a), 5) + min(len(hits_b1), 5) +
        min(len(hits_b2), 5) + min(len(hits_c), 5)
    )

    
    if 0 < mins_left <= 180:
        time_text = f"마감까지 {mins_left}분"
    elif mins_left <= 0:
        time_text = "마감직전/마감후"
    else:
        time_text = "정규 종가배팅 시간 아님/테스트"
    header = (
        f"📌 종가배팅 전략별 선별 TOP {total} ({TODAY_STR})\n"
        f"🧩 버전 {CLOSING_BET_SCANNER_VERSION}\n"
        f"⏰ {time_text}\n"
        f"전체 후보 {len(hits)}개 | 출력 {total}개\n"
        f"모랄레스갭(G) {min(len(hits_g), 5)}개 | 고점재응축(S) 출력 {min(len(hits_s), 5)}개 · 전체 S2 {len(hits_s2)}/S1 {len(hits_s1)} | 돌파형(A) {min(len(hits_a), 5)}개 | "
        f"ENV엄격형(B1) {min(len(hits_b1), 5)}개 | BB확장형(B2) {min(len(hits_b2), 5)}개 | 역매공파(C) {min(len(hits_c), 5)}개\n"
        f"전체 후보 등급: 완전체 {len(complete_hits)}개 | A급 {len(a_grade_hits)}개 | B급 {len(b_grade_hits)}개"
    )

    def _is_s1_good(h: dict) -> bool:
        return (
            _pick_strategy(h) == "S"
            and str(h.get('s_type', '')) != 'S2'
            and _safe_float(h.get('rr', 0), 0.0) >= 0.70
            and _safe_float(h.get('amount_b', 0), 0.0) >= 100.0
            and _safe_float(h.get('close_loc_pct', 0), 0.0) >= 70.0
        )

    def _is_a_strong(h: dict) -> bool:
        return (
            _pick_strategy(h) == "A"
            and _grade_core(h) in ("COMPLETE", "A")
            and _safe_float(h.get('vol_ratio', h.get('volume_ratio', 0)), 0.0) >= 2.0
            and _safe_float(h.get('amount_b', 0), 0.0) >= 100.0
        )

    def _priority_practical(h: dict):
        mode = _pick_strategy(h)
        s_type = str(h.get('s_type', '') or '')
        if mode == 'S' and s_type == 'S2':
            group_rank = 0
        elif mode == 'S' and _is_s1_good(h):
            group_rank = 1
        elif mode == 'A' and _is_a_strong(h):
            group_rank = 2
        else:
            group_rank = 9
        return (
            group_rank,
            -_safe_float(h.get('rule35_pnl', h.get('rr', 0)), 0.0),
            -_safe_score(h),
            -_safe_float(h.get('amount_b', 0), 0.0),
            -_safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0),
        )

    def _brief_practical_line(h: dict, idx: int) -> str:
        code = str(h.get('code', '') or '').strip()
        name = str(h.get('name', '') or code).strip()
        if name == code or (name.isdigit() and len(name) == 6):
            name = '종목명확인필요'
        mode = _pick_strategy(h)
        if mode == 'S' and str(h.get('s_type', '')) == 'S2':
            tag = 'S2 실행형'
            reason = '고점권 재응축 후 거래량 재점화'
        elif mode == 'S':
            tag = 'S1 우수관찰'
            reason = '구조는 좋고 RR/거래대금 양호, 다음날 재점화 확인'
        elif mode == 'A':
            tag = 'A 강한돌파'
            reason = '+3~+5% 익절형 돌파 후보'
        else:
            tag = mode
            reason = '조건 확인 필요'
        close = _safe_float(h.get('close', 0), 0.0)
        amount_b = _safe_float(h.get('amount_b', 0), 0.0)
        volr = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
        rr = _safe_float(h.get('rr', 0), 0.0)
        score = _safe_float(h.get('score', 0), 0.0)
        stop = _safe_float(h.get('stoploss', 0), 0.0)
        return (
            f"{idx}) {tag} | {name}({code}) | {int(close):,}원 | 점수 {score:.1f} | "
            f"거래량비 {volr:.2f} | 거래대금 {amount_b:.1f}억 | RR {rr:.2f}\n"
            f"   핵심: {reason}\n"
            f"   대응: +3% 1차익절 / +5% 추가익절 / 손절 {int(stop):,}원 이탈 관리"
        )

    def _build_practical_block():
        pool = [h for h in hits if ((_pick_strategy(h) == 'S' and str(h.get('s_type','')) == 'S2') or _is_s1_good(h) or _is_a_strong(h))]
        pool = sorted(pool, key=_priority_practical)[:7]
        block = ['[🎯 실전 매수 우선 후보 — v3.3 핵심군]']
        if not pool:
            block.append('해당 종목 없음 — 오늘은 S2/S1우수/A강한돌파 기준을 동시에 만족한 실전 우선 후보가 없습니다.')
        else:
            block.append('기준: S2 실행형 > S1 우수관찰 > A 강한돌파 순서. B1/B2와 C완화형은 실전 매수 후보가 아니라 관찰/진단 섹션으로 분리합니다.')
            for i, h in enumerate(pool, 1):
                block.append(_brief_practical_line(h, i))
                block.append('')
        return '\n'.join(block).rstrip()

    sections = [header, _build_practical_block()]

    def _build_block(title: str, items: list, tag: str):
        block = [f"[{title}]"]
        if not items:
            if tag == "G":
                block.append("해당 종목 없음 — 오늘은 갭상승+Vol50 1.5배+갭지지+박스/신고가 돌파를 동시에 만족한 엄격형 후보가 없습니다.")
            elif tag == "S":
                block.append("해당 종목 없음 — 오늘은 강한 상승 이력+고점권 재응축+종가고점마감을 동시에 만족한 2차 슈팅 후보가 없습니다.")
            else:
                block.append("해당 종목 없음")
            return "\n".join(block)

        for idx, hit in enumerate(items[:5], 1):
            try:
                entry = _format_hit(hit, idx, mins_left)
            except Exception as e:
                log_error(f"_format_hit 오류 [{tag}/{hit.get('code','')}]: {e}")
                entry = ""
            log_info(f"[FORMAT-{tag}] code={hit.get('code')} | len={len(entry)}")
            if entry:
                block.append(entry)
                block.append("")
        return "\n".join(block).rstrip()

    sections.append(_build_block("🟢 모랄레스갭(G) TOP5", hits_g, "G"))
    sections.append(_build_block("🚀 고점재응축(S) TOP5", hits_s, "S"))
    sections.append(_build_block("돌파형(A) TOP5", hits_a, "A"))
    sections.append(_build_block("👀 ENV엄격형(B1) 관찰 TOP5", hits_b1, "B1"))
    sections.append(_build_block("👀 BB확장형(B2) 관찰 TOP5", hits_b2, "B2"))
    sections.append(_build_block("🏆 역매공파(C) TOP5", hits_c, "C"))

    chunks = []
    current = ""
    for sec in sections:
        add = sec if not current else "\n\n" + sec
        if len(current) + len(add) > 3500:
            if current.strip():
                chunks.append(current.strip())
            current = sec
        else:
            current += add
    if current.strip():
        chunks.append(current.strip())

    for i, chunk in enumerate(chunks, 1):
        log_info(f"텔레그램 전송 {i}/{len(chunks)} | 길이={len(chunk)}")
        send_telegram_photo(chunk, [])

    log_info("✅ 텔레그램 전송 완료")

def run_closing_bet_scan(force: bool = False) -> list:
    log_info(f"✅ BOOTCHECK: {CLOSING_BET_SCANNER_VERSION}")
    now = _now_kst()
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
    _refresh_marcap_map()
    _refresh_index_map()

    if SCAN_UNIVERSE == 'amount_top400':
        codes, names = _load_amount_top_universe(TOP_N)
        codes = [_normalize_code(c) for c in codes]
        if not codes:
            log_error("⚠️ amount_top400 유니버스 로드 실패")
            return []
        allowed_codes = [c for c in codes if _is_universe_allowed(c)]
        if allowed_codes:
            name_map = {_normalize_code(c): n for c, n in zip(codes, names)}
            source_codes = allowed_codes
            names = [name_map.get(c, c) for c in source_codes]
        else:
            source_codes = codes
    else:
        codes = _load_universe(SCAN_UNIVERSE)
        codes = [_normalize_code(c) for c in codes]
        codes = sorted(set(codes))
        if not codes:
            log_error("⚠️ 유니버스 로드 실패")
            return []

        try:
            listing_for_names = _get_krx_listing()
            fdr_name_map = dict(zip(listing_for_names['Code'], listing_for_names['Name'])) if not listing_for_names.empty else {}
        except Exception:
            fdr_name_map = {}

        try:
            from pykrx import stock as _pk
            name_map = {}
            for c in codes[:1000]:
                try:
                    nm = str(_pk.get_market_ticker_name(c) or '').strip()
                    name_map[c] = nm if nm and nm != c else fdr_name_map.get(c, c)
                except Exception:
                    name_map[c] = fdr_name_map.get(c, c)
        except Exception:
            name_map = {c: fdr_name_map.get(c, c) for c in codes}

        log_info(f"유니버스 종목수: {len(codes)}")
        log_info(f"유니버스 샘플: {codes[:10]}")
        log_info(f"INDEX_MAP 수: {len(INDEX_MAP)}")
        log_info(f"MARCAP_MAP 수: {len(MARCAP_MAP)}")

        allowed_codes = [c for c in codes if _is_universe_allowed(c)]
        log_info(f"유니버스 통과 종목수: {len(allowed_codes)}")
        log_info(f"유니버스 통과 샘플: {allowed_codes[:10]}")

        if len(allowed_codes) == 0:
            log_error("⚠️ 유니버스 통과 종목이 0개입니다. INDEX_MAP / MARCAP_MAP / 코드정규화 문제 가능성이 큽니다.")

        source_codes = allowed_codes if len(allowed_codes) > 0 else codes
        names = [name_map.get(c, c) for c in source_codes]

    log_info(f"대상: {len(source_codes)}개 ({SCAN_UNIVERSE})")

    hits = []
    ex = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = {
        ex.submit(_check_closing_bet, code, name): (code, name)
        for code, name in zip(source_codes, names)
    }
    processed_futures = set()
    done = 0
    failed = 0

    def _consume_future(future):
        nonlocal done, failed
        if future in processed_futures:
            return
        processed_futures.add(future)
        done += 1
        try:
            result = future.result()
            if result:
                hits.append(result)
        except Exception as e:
            failed += 1
            code, name = futures.get(future, ('', ''))
            log_debug(f"스캔 future 실패 [{code}/{name}]: {e}")

        if done % 100 == 0:
            log_info(f"진행: {done}/{len(source_codes)} | 후보: {len(hits)}개")

    try:
        for future in as_completed(futures, timeout=SCAN_FUTURES_TIMEOUT):
            _consume_future(future)
    except FuturesTimeoutError as e:
        # 핵심 수정: 일부 종목이 늦어도 전체 스캐너를 실패시키지 않고,
        # 이미 완료된 후보만으로 텔레그램/로그 저장을 계속 진행한다.
        log_error(f"⚠️ 일부 종목 스캔 타임아웃: {e} → 완료된 후보만 사용해 계속 진행")
        for future in list(futures.keys()):
            if future.done() and future not in processed_futures:
                _consume_future(future)
        unfinished = [f for f in futures if not f.done()]
        if unfinished:
            log_error(f"⚠️ 미완료 종목 {len(unfinished)}개는 이번 회차에서 제외")
            for future in unfinished:
                future.cancel()
    finally:
        ex.shutdown(wait=False, cancel_futures=True)

    if failed:
        log_info(f"스캔 중 개별 실패: {failed}건")
    log_info(f"스캔 처리 완료: {done}/{len(source_codes)} | 후보: {len(hits)}개 | cache={_load_df.cache_info()}")

    hits_g = [h for h in hits if h.get('mode') == 'G']
    hits_s = [h for h in hits if h.get('mode') == 'S']
    hits_a = [h for h in hits if h.get('mode') == 'A']
    hits_b1 = [h for h in hits if h.get('mode') == 'B1']
    hits_b2 = [h for h in hits if h.get('mode') == 'B2']
    hits_c = [h for h in hits if h.get('mode') == 'C']

    def _sort_hit_list(items):
        items.sort(
            key=lambda x: (
                _safe_float(x.get('score', 0), 0.0),
                _safe_float(x.get('amount_b', 0), 0.0),
                _safe_float(x.get('vol_ratio', x.get('volume_ratio', 0)), 0.0),
            ),
            reverse=True,
        )

    _sort_hit_list(hits_g)
    hits_s.sort(
        key=lambda x: (
            0 if str(x.get('s_type', '')) == 'S2' else 1,
            -_safe_float(x.get('score', 0), 0.0),
            -_safe_float(x.get('today_vol_ratio', x.get('vol_ratio', 0)), 0.0),
            -_safe_float(x.get('rr', 0), 0.0),
            -_safe_float(x.get('amount_b', 0), 0.0),
        )
    )
    _sort_hit_list(hits_a)
    _sort_hit_list(hits_b1)
    _sort_hit_list(hits_b2)
    _sort_hit_list(hits_c)

    # 기존 버그 방지: C전략과 신규 G/S전략이 최종 합산에서 빠지지 않도록 모두 포함
    hits = hits_g + hits_s + hits_a + hits_b1 + hits_b2 + hits_c

    log_info(f"\n종가배팅 후보: {len(hits)}개")
    log_info(
        f"모랄레스갭(G): {len(hits_g)}개 | 고점재응축(S): {len(hits_s)}개(S2 {sum(1 for h in hits_s if str(h.get('s_type','')) == 'S2')}/S1 {sum(1 for h in hits_s if str(h.get('s_type','')) != 'S2')}) | 돌파형(A): {len(hits_a)}개 | "
        f"ENV엄격형(B1): {len(hits_b1)}개 | BB확장형(B2): {len(hits_b2)}개 | 역매공파(C): {len(hits_c)}개"
    )
    log_info(f"완전체: {sum(1 for h in hits if '완전체' in h.get('grade', ''))}개")
    log_info(f"✅A급: {sum(1 for h in hits if 'A급' in h.get('grade', ''))}개")
    log_info(f"B급: {sum(1 for h in hits if h.get('grade') == 'B급')}개")

    if len(hits) == 0:
        log_info("후보 0개 진단 시작")
        log_info(f"- 전체 유니버스: {len(codes) if 'codes' in locals() else 0}")
        log_info(f"- 유니버스 통과: {len(allowed_codes) if 'allowed_codes' in locals() else 0}")
        log_info(f"- INDEX_MAP 수: {len(INDEX_MAP)}")
        log_info(f"- MARCAP_MAP 수: {len(MARCAP_MAP)}")

    log_info(
        f"[전략진단] G: {STRATEGY_DIAG['G_hit']}/{STRATEGY_DIAG['G_try']} | "
        f"S: {STRATEGY_DIAG['S_hit']}/{STRATEGY_DIAG['S_try']} | "
        f"A: {STRATEGY_DIAG['A_hit']}/{STRATEGY_DIAG['A_try']} | "
        f"B1: {STRATEGY_DIAG['B1_hit']}/{STRATEGY_DIAG['B1_try']} | "
        f"B2: {STRATEGY_DIAG['B2_hit']}/{STRATEGY_DIAG['B2_try']} | "
        f"C: {STRATEGY_DIAG['C_hit']}/{STRATEGY_DIAG['C_try']}"
    )
    log_info(f"[탈락진단] {STRATEGY_FAIL}")

    _send_results(hits, mins_left)
    _append_hits_to_validation_log(hits, now)
    _save_estimated_flow_snapshots(hits, now)
    return hits


def _now_kst():
    return datetime.now(KST)


def _is_closing_time(force: bool = False) -> bool:
    if force:
        return True
    now = _now_kst()
    hhmm = now.hour * 100 + now.minute
    start_hhmm = SCAN_START_HOUR * 100 + SCAN_START_MIN
    end_hhmm = SCAN_END_HOUR * 100 + SCAN_END_MIN
    return start_hhmm <= hhmm <= end_hhmm


def _time_to_close() -> int:
    now = _now_kst()
    target = now.replace(hour=SCAN_END_HOUR, minute=SCAN_END_MIN, second=0, microsecond=0)
    return max(0, int((target - now).total_seconds() // 60))



# =============================================================
# 6개월 성과검증 / 과거 재현 백테스트
# =============================================================
def _prepare_price_df(df: pd.DataFrame) -> pd.DataFrame:
    """FDR/pykrx/Naver 일봉을 스캐너 지표 형식으로 표준화. Date 컬럼은 보존한다."""
    if df is None or df.empty:
        return pd.DataFrame()
    try:
        out = df.copy()
        if 'Date' not in out.columns:
            out.insert(0, 'Date', pd.to_datetime(out.index, errors='coerce'))

        rename_map = {}
        for c in out.columns:
            raw = str(c).strip()
            cl = raw.lower()
            if cl == 'open' or raw == '시가':
                rename_map[c] = 'Open'
            elif cl == 'high' or raw == '고가':
                rename_map[c] = 'High'
            elif cl == 'low' or raw == '저가':
                rename_map[c] = 'Low'
            elif cl == 'close' or raw == '종가':
                rename_map[c] = 'Close'
            elif cl == 'volume' or raw == '거래량':
                rename_map[c] = 'Volume'
            elif cl == 'amount' or raw == '거래대금':
                rename_map[c] = 'Amount'
        if rename_map:
            out = out.rename(columns=rename_map)

        required = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        for col in required:
            if col not in out.columns:
                return pd.DataFrame()
        out['Date'] = pd.to_datetime(out['Date'], errors='coerce')
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            out[col] = pd.to_numeric(out[col], errors='coerce')
        out = out.dropna(subset=['Date', 'Open', 'High', 'Low', 'Close']).copy()
        out['Volume'] = pd.to_numeric(out['Volume'], errors='coerce').fillna(0)
        if 'Amount' not in out.columns:
            out['Amount'] = out['Close'] * out['Volume']
        else:
            out['Amount'] = pd.to_numeric(out['Amount'], errors='coerce').fillna(out['Close'] * out['Volume'])
        out = out.sort_values('Date').drop_duplicates(subset=['Date'], keep='last').reset_index(drop=True)

        out['MA5'] = out['Close'].rolling(5).mean()
        out['MA10'] = out['Close'].rolling(10).mean()
        out['MA20'] = out['Close'].rolling(20).mean()
        out['MA50'] = out['Close'].rolling(50).mean()
        out['MA112'] = out['Close'].rolling(112).mean()
        out['MA224'] = out['Close'].rolling(224).mean()
        out['VMA20'] = out['Volume'].rolling(20).mean()
        out['VMA50'] = out['Volume'].rolling(50).mean()
        tr1 = out['High'] - out['Low']
        tr2 = (out['High'] - out['Close'].shift(1)).abs()
        tr3 = (out['Low'] - out['Close'].shift(1)).abs()
        out['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        out['ATR'] = out['TR'].rolling(14).mean()
        out['RSI'] = _calc_rsi(out['Close'], 14)
        direction = np.sign(out['Close'].diff().fillna(0))
        out['OBV'] = (direction * out['Volume']).cumsum()
        return out
    except Exception as e:
        log_debug(f"_prepare_price_df 실패: {e}")
        return pd.DataFrame()


BACKTEST_LOAD_ERROR_SAMPLES = []
BACKTEST_LOAD_ERROR_LOCK = threading.Lock()


def _remember_backtest_load_error(msg: str, limit: int = 8):
    """백테스트 데이터 로딩 실패 원인을 진단 요약에 남기기 위한 샘플 저장."""
    try:
        with BACKTEST_LOAD_ERROR_LOCK:
            if len(BACKTEST_LOAD_ERROR_SAMPLES) < limit:
                BACKTEST_LOAD_ERROR_SAMPLES.append(str(msg)[:500])
    except Exception:
        pass


def _read_fdr_ohlcv(code: str, start_ymd: str, end_ymd: str) -> pd.DataFrame:
    raw = fdr.DataReader(code, start_ymd, end_ymd)
    return _prepare_price_df(raw)


def _read_pykrx_ohlcv(code: str, start_ymd: str, end_ymd: str) -> pd.DataFrame:
    if pykrx_stock is None:
        return pd.DataFrame()
    start_key = pd.Timestamp(start_ymd).strftime('%Y%m%d')
    end_key = pd.Timestamp(end_ymd).strftime('%Y%m%d')
    raw = pykrx_stock.get_market_ohlcv_by_date(start_key, end_key, code)
    return _prepare_price_df(raw)


def _read_naver_ohlcv(code: str, start_ymd: str, end_ymd: str, max_pages: int = 60) -> pd.DataFrame:
    """Naver Finance 일봉 보조 로더. FDR/pykrx가 막힐 때 최후 fallback."""
    try:
        code = _normalize_code(code)
        frames = []
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': f'https://finance.naver.com/item/sise_day.naver?code={code}',
        }
        for page in range(1, int(max_pages) + 1):
            url = f'https://finance.naver.com/item/sise_day.naver?code={code}&page={page}'
            res = requests.get(url, headers=headers, timeout=8)
            res.encoding = 'euc-kr'
            tables = pd.read_html(res.text)
            if not tables:
                continue
            t = tables[0].dropna(how='all')
            if t.empty or '날짜' not in t.columns:
                continue
            frames.append(t)
            # 백테스트 시작 이전까지 내려왔으면 중단
            oldest = pd.to_datetime(t['날짜'], errors='coerce').min()
            if pd.notna(oldest) and oldest <= pd.Timestamp(start_ymd):
                break
        if not frames:
            return pd.DataFrame()
        raw = pd.concat(frames, ignore_index=True)
        raw = raw.rename(columns={'날짜': 'Date', '시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close', '거래량': 'Volume'})
        out = _prepare_price_df(raw)
        if out.empty:
            return out
        return out[(out['Date'] >= pd.Timestamp(start_ymd)) & (out['Date'] <= pd.Timestamp(end_ymd))].copy().reset_index(drop=True)
    except Exception as e:
        _remember_backtest_load_error(f"NAVER {code}: {type(e).__name__}: {e}")
        return pd.DataFrame()


def _load_df_backtest(code: str, start_date: str, end_date: str, warmup_days: int = 360) -> pd.DataFrame:
    """백테스트용 데이터 로더.

    v3.0: FinanceDataReader가 빈 값을 반환하거나 네트워크/소스 변경으로 실패할 때
    pykrx → Naver Finance 순서로 자동 fallback한다.
    """
    code = _normalize_code(code)
    try:
        start_ts = pd.Timestamp(start_date) - pd.Timedelta(days=warmup_days)
        # 성과평가용 미래 봉 확보. 실제 미래 데이터가 없으면 가능한 범위만 반환된다.
        end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=35)
        start_ymd = start_ts.strftime('%Y-%m-%d')
        end_ymd = end_ts.strftime('%Y-%m-%d')
        source_pref = os.environ.get('CLOSING_BET_BACKTEST_DATA_SOURCE', 'auto').strip().lower()
        errors = []

        source_order = ['fdr', 'pykrx', 'naver']
        if source_pref in ('fdr', 'pykrx', 'naver'):
            source_order = [source_pref]
        elif source_pref == 'nofdr':
            source_order = ['pykrx', 'naver']

        for source in source_order:
            try:
                if source == 'fdr':
                    df = _read_fdr_ohlcv(code, start_ymd, end_ymd)
                elif source == 'pykrx':
                    df = _read_pykrx_ohlcv(code, start_ymd, end_ymd)
                else:
                    df = _read_naver_ohlcv(code, start_ymd, end_ymd)
                if df is not None and not df.empty and len(df) >= 90:
                    df['data_source'] = source
                    return df
                errors.append(f"{source}:empty_or_short({0 if df is None else len(df)})")
            except Exception as e:
                errors.append(f"{source}:{type(e).__name__}:{e}")

        _remember_backtest_load_error(f"{code} 데이터 로드 실패 | " + " | ".join(errors))
        return pd.DataFrame()
    except Exception as e:
        _remember_backtest_load_error(f"{code} loader fatal: {type(e).__name__}: {e}")
        log_debug(f"_load_df_backtest 실패 [{code}]: {e}")
        return pd.DataFrame()


def _bt_grade_from_score(score: int, complete_min: int = 6, a_min: int = 5) -> str:
    if score >= complete_min:
        return '완전체'
    if score >= a_min:
        return '✅A급'
    return 'B급'


def _bt_common_payload(code: str, name: str, mode: str, mode_label: str, grade: str, score: float, row: pd.Series, hist: pd.DataFrame, idx_label: str, marcap: float, passed: list) -> dict:
    info = _base_info(row, hist)
    is_mcap_or = marcap >= MCAP_OR_MIN
    band_rec = _get_band_recommendation(
        code=code,
        df=hist,
        row=row,
        index_label=idx_label,
        is_top_mcap=(code in TOP_MCAP_SET),
        is_mcap_or=is_mcap_or,
    )
    return {
        **info,
        'signal_date': pd.Timestamp(row.get('Date')).strftime('%Y-%m-%d') if not pd.isna(row.get('Date')) else '',
        'code': code,
        'name': name,
        'mode': mode,
        'strategy': mode,
        'mode_label': mode_label,
        'grade': grade,
        'score': score,
        'index_label': idx_label,
        'marcap': marcap,
        'is_mcap_or': int(is_mcap_or),
        'is_top_mcap': int(code in TOP_MCAP_SET),
        'close': info['_close'],
        'recommended_band': band_rec.get('recommended_band', ''),
        'support_band': band_rec.get('support_band', ''),
        'volatility_type': band_rec.get('volatility_type', ''),
        'universe_tag': band_rec.get('universe_tag', ''),
        'band_comment': band_rec.get('comment', ''),
        'band_recommend_reason': band_rec.get('reason', ''),
        'passed': passed,
    }


def _check_backtest_strategies_on_df(code: str, name: str, hist: pd.DataFrame) -> list:
    """특정 과거일(hist 마지막 봉)을 기준으로 G/A/B1/B2/C 전략 신호를 모두 계산한다."""
    hits = []
    try:
        if hist is None or len(hist) < 80:
            return hits
        code = _normalize_code(code)
        row = hist.iloc[-1]
        info = _base_info(row, hist)
        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        is_mcap_or = marcap >= MCAP_OR_MIN
        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            return hits
        if info['_close'] < MIN_PRICE:
            return hits

        # G — 모랄레스 갭 돌파형
        if len(hist) >= max(130, GAP_HIGH_LOOKBACK + 5):
            try:
                prev = hist.iloc[-2]
                past = hist.iloc[:-1]
                today_open = info['_open']
                today_high = info['_high']
                today_low = info['_low']
                today_close = info['_close']
                prev_close = _safe_float(prev.get('Close', 0), 0.0)
                if info['amount_b'] >= MIN_AMOUNT / 1e8 and today_open > 0 and prev_close > 0:
                    gap_pct = (today_open / prev_close - 1.0) * 100.0
                    vol50 = _safe_float(past['Volume'].tail(50).mean(), 0.0)
                    vol50_ratio = info['_vol'] / vol50 if vol50 > 0 else 0.0
                    gap_ok = GAP_MIN_PCT <= gap_pct <= GAP_MAX_PCT
                    volume_ok = vol50_ratio >= GAP_VOL50_MULT
                    gap_unfilled = today_low >= prev_close * (1.0 + GAP_LOW_KEEP_PCT / 100.0)
                    close_support = today_close >= today_open * GAP_CLOSE_OPEN_KEEP
                    close_strength = today_close >= (today_high + today_low) / 2.0 if today_high > today_low else today_close >= today_open
                    box_high_60 = _safe_float(past['High'].tail(GAP_BOX_LOOKBACK).max(), 0.0)
                    high_120 = _safe_float(past['High'].tail(GAP_HIGH_LOOKBACK).max(), 0.0)
                    box_breakout = bool(box_high_60 > 0 and today_close >= box_high_60 * 1.002)
                    new_high_breakout = bool(high_120 > 0 and today_close >= high_120 * 1.002)
                    ma20 = _safe_float(row.get('MA20', 0), 0.0)
                    disparity20 = today_close / ma20 * 100.0 if ma20 > 0 else 999.0
                    close_20ago = _safe_float(hist['Close'].iloc[-21], 0.0) if len(hist) >= 21 else 0.0
                    runup20 = (today_close / close_20ago - 1.0) * 100.0 if close_20ago > 0 else 999.0
                    candle_range = today_high - today_low
                    upper_wick_ratio = ((today_high - max(today_open, today_close)) / candle_range) if candle_range > 0 else 0.0
                    not_climax = disparity20 <= GAP_DISPARITY20_MAX and runup20 <= GAP_RUNUP20_MAX and upper_wick_ratio <= GAP_UPPER_WICK_MAX
                    if gap_ok and volume_ok and gap_unfilled and close_support and close_strength and (box_breakout or new_high_breakout) and not_climax:
                        score = 0
                        passed = []
                        score += 15; passed.append(f'①갭{gap_pct:+.1f}%')
                        score += 25; passed.append(f'②Vol50 {vol50_ratio:.1f}배')
                        score += 15; passed.append('③갭미메움')
                        score += 10; passed.append('④시가지지')
                        score += 10; passed.append('⑤종가강도')
                        if box_breakout:
                            score += 15; passed.append('⑥60일박스돌파')
                        if new_high_breakout:
                            score += 20; passed.append('⑦120일신고가')
                        score += 10; passed.append('⑧클라이맥스제외')
                        score = min(score, 100)
                        if score >= 85:
                            grade = '완전체' if score >= 95 else ('✅A급' if score >= 88 else 'B급')
                            h = _bt_common_payload(code, name, 'G', '모랄레스갭', grade, score, row, hist, idx_label, marcap, passed)
                            trail = _select_morales_trailing_ma(hist, marcap=marcap, idx_label=idx_label)
                            stoploss = round(today_low)
                            risk = max(today_close - stoploss, 0.0)
                            target1 = round(today_close + risk * 2.0) if risk > 0 else round(today_close * 1.05)
                            h.update({
                                'band_type': 'GAP',
                                'gap_pct': round(gap_pct, 1),
                                'vol50_ratio': round(vol50_ratio, 2),
                                'vol_ratio': round(vol50_ratio, 2),
                                'disparity20': round(disparity20, 1),
                                'runup20': round(runup20, 1),
                                'wick_pct': round(upper_wick_ratio * 100, 1),
                                'trail_ma': trail.get('trail_ma', ''),
                                'sell_rule': trail.get('sell_rule', ''),
                                'stoploss': stoploss,
                                'target1': target1,
                                'rr': round(((target1 - today_close) / risk), 2) if risk > 0 else 0.0,
                            })
                            hits.append(h)
            except Exception as e:
                log_debug(f"G 백테스트 오류 [{code}/{name}]: {e}")


        # S — 고점권 재응축 2차 슈팅형
        try:
            if info['amount_b'] >= MIN_AMOUNT / 1e8:
                sig = _evaluate_high_reaccum_signal(hist)
                if sig.get('pass'):
                    h = _bt_common_payload(code, name, 'S', '고점재응축', sig.get('grade', 'B급'), sig.get('score', 0), row, hist, idx_label, marcap, sig.get('passed', []))
                    h.update({
                        'band_type': 'HIGH_REACCUM',
                        'band_reason': '고점권 재응축+종가고점마감',
                        's_type': sig.get('s_type', ''),
                        's_type_label': sig.get('s_type_label', ''),
                        'execution_verdict': sig.get('execution_verdict', ''),
                        'band_pct_text': f"120일상승:{sig.get('runup120',0):+.1f}% | 고점근접:{sig.get('near_high120',0):.1f}% | 종가위치:{sig.get('close_loc_pct',0):.0f}%",
                        'runup120': sig.get('runup120', 0),
                        'near_high120': sig.get('near_high120', 0),
                        'pullback_from_high': sig.get('pullback_from_high', 0),
                        'close_loc_pct': sig.get('close_loc_pct', 0),
                        'upper_wick_range_pct': sig.get('upper_wick_range_pct', 0),
                        'vma5_20_ratio': sig.get('vma5_20_ratio', 0),
                        'today_vol_ratio': sig.get('today_vol_ratio', 0),
                        'obv_alive': sig.get('obv_alive', 0),
                        'rsi': sig.get('rsi', 0),
                        'disparity20': sig.get('disparity20', 0),
                        'runup20': sig.get('runup20', 0),
                        'stoploss': sig.get('stoploss', 0),
                        'target1': sig.get('target1', 0),
                        'target2': sig.get('target2', 0),
                        'rr': sig.get('rr', 0),
                        'stop_logic': sig.get('stop_logic', ''),
                        'initial_stop_rule': sig.get('initial_stop_rule', ''),
                        'high_close_rule': sig.get('high_close_rule', ''),
                    })
                    hits.append(h)
        except Exception as e:
            log_debug(f"S 백테스트 오류 [{code}/{name}]: {e}")

        # A — 돌파형
        try:
            if info['amount_b'] >= MIN_AMOUNT / 1e8:
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
                if score >= 4:
                    hits.append(_bt_common_payload(code, name, 'A', '돌파형', _bt_grade_from_score(score), score, row, hist, idx_label, marcap, passed))
        except Exception as e:
            log_debug(f"A 백테스트 오류 [{code}/{name}]: {e}")

        # B1 — ENV 엄격형
        try:
            if info['amount_b'] >= MIN_AMOUNT / 1e8:
                env = _check_envelope_bottom(row, hist)
                if env.get('env20_near') and env.get('env40_near'):
                    rsi = float(row.get('RSI', 50) or 50)
                    close = info['_close']; open_p = info['_open']; high = info['_high']; low = info['_low']
                    body_bot = min(close, open_p); body_top = max(close, open_p); body_size = max(body_top - body_bot, 1)
                    lower_wick = body_bot - low; upper_wick_len = high - body_top
                    close_to_high = (close / high * 100) if high > 0 else 0
                    obv_ma5 = hist['OBV'].rolling(5).mean(); obv_ma10 = hist['OBV'].rolling(10).mean()
                    obv_rising = float(obv_ma5.iloc[-1]) > float(obv_ma10.iloc[-1])
                    recent5 = hist.tail(5); vma10_val = float(hist['Volume'].rolling(10).mean().iloc[-1])
                    maejip_5d = int(((recent5['Volume'] > vma10_val) & (recent5['Close'] > recent5['Open'])).sum())
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
                    if score >= 4:
                        h = _bt_common_payload(code, name, 'B1', 'ENV엄격형', _bt_grade_from_score(score), score, row, hist, idx_label, marcap, passed)
                        env20_ma = float(_calc_envelope(hist, 20, 10)['ma'].iloc[-1])
                        h.update({
                            'band_type': 'ENV',
                            'env20_pct': env.get('env20_pct', 0),
                            'env40_pct': env.get('env40_pct', 0),
                            'rsi': round(rsi, 1),
                            'obv_rising': int(bool(obv_rising)),
                            'maejip_5d': maejip_5d,
                            'lower_wick_pct': round(lower_wick / body_size * 100, 1),
                            'upper_wick_pct': round(upper_wick_len / body_size * 100, 1),
                            'target1': round(env20_ma),
                        })
                        hits.append(h)
        except Exception as e:
            log_debug(f"B1 백테스트 오류 [{code}/{name}]: {e}")

        # B2 — BB 확장형
        try:
            if info['amount_b'] >= MIN_AMOUNT / 1e8:
                bb = _check_bb_bottom(row, hist)
                if bb.get('bb40_near'):
                    rsi = float(row.get('RSI', 50) or 50)
                    close = info['_close']; open_p = info['_open']; high = info['_high']; low = info['_low']
                    body_bot = min(close, open_p); body_top = max(close, open_p); body_size = max(body_top - body_bot, 1)
                    lower_wick = body_bot - low; upper_wick_len = high - body_top
                    close_to_high = (close / high * 100) if high > 0 else 0
                    obv_ma5 = hist['OBV'].rolling(5).mean(); obv_ma10 = hist['OBV'].rolling(10).mean()
                    obv_rising = float(obv_ma5.iloc[-1]) > float(obv_ma10.iloc[-1])
                    recent5 = hist.tail(5); vma10_val = float(hist['Volume'].rolling(10).mean().iloc[-1])
                    maejip_5d = int(((recent5['Volume'] > vma10_val) & (recent5['Close'] > recent5['Open'])).sum())
                    band_meta = _choose_lower_band_type(code, hist, row)
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
                    if score >= 4:
                        h = _bt_common_payload(code, name, 'B2', 'BB확장형', _bt_grade_from_score(score), score, row, hist, idx_label, marcap, passed)
                        h.update({
                            'band_type': 'BB',
                            'bb40_pct': bb.get('bb40_pct', 0),
                            'bb40_width': bb.get('bb40_width', 0),
                            'atr_pct': band_meta.get('atr_pct', 0),
                            'amount20_b': band_meta.get('amount20_b', 0),
                            'rsi': round(rsi, 1),
                            'obv_rising': int(bool(obv_rising)),
                            'maejip_5d': maejip_5d,
                            'lower_wick_pct': round(lower_wick / body_size * 100, 1),
                            'upper_wick_pct': round(upper_wick_len / body_size * 100, 1),
                            'target1': bb.get('mid40', 0),
                        })
                        hits.append(h)
        except Exception as e:
            log_debug(f"B2 백테스트 오류 [{code}/{name}]: {e}")

        # C — 역매공파
        try:
            if len(hist) >= 250 and info['amount_b'] >= MIN_AMOUNT / 1e8:
                recent_60 = hist.iloc[-60:]
                ma112_past = hist['MA112'].iloc[-60]
                ma224_past = hist['MA224'].iloc[-60]
                close_past = hist['Close'].iloc[-60]
                is_reverse = (ma112_past < ma224_past) and (close_past < ma112_past)
                spike_vol = recent_60['Volume'] > (recent_60['VMA20'] * 2.0)
                upper_tail = (recent_60['High'] - recent_60[['Open', 'Close']].max(axis=1)) / recent_60['Close'] > 0.03
                is_accumulation = bool((spike_vol & upper_tail).any())
                min_60 = recent_60['Low'].min()
                min_20 = hist.iloc[-20:]['Low'].min()
                is_concrete = min_20 >= (min_60 * 0.95)
                ma112_now = float(row.get('MA112', 0) or 0)
                ma224_now = float(row.get('MA224', 0) or 0)
                is_breakout = (info['_close'] >= ma112_now) or (info['_close'] >= ma224_now)

                # v3.3: 실시간 C전략이 나오는 데 백테스트에서 0건만 나오는 문제를 점검하기 위한 완화형 연결.
                # 엄격형은 기존 조건을 유지하고, 완화형은 과거 역배열/장기선 아래 구간이 있었고
                # 현재 MA5/MA20 회복 + 장기선 재도전 + 매집/OBV/거래량 흔적이 있으면 C 후보로 기록한다.
                past120 = hist.iloc[-120:] if len(hist) >= 120 else hist
                try:
                    past_bearish_count = int(((past120['Close'] < past120['MA112']) | (past120['MA112'] < past120['MA224'])).sum())
                except Exception:
                    past_bearish_count = 0
                past_bearish = past_bearish_count >= max(8, int(len(past120) * 0.10))
                ma5_now = float(row.get('MA5', 0) or 0)
                ma20_now = float(row.get('MA20', 0) or 0)
                ma5_recovery = bool(ma5_now > 0 and ma20_now > 0 and ma5_now >= ma20_now and info['_close'] >= ma112_now * 0.985)
                try:
                    obv_rising_c = float(hist['OBV'].tail(5).mean()) >= float(hist['OBV'].tail(20).mean())
                except Exception:
                    obv_rising_c = False
                vol_reaccum_c = bool(info.get('_vma20', 0) and info['_vol'] >= info['_vma20'] * 1.2)

                is_yma_strict = bool(is_reverse and is_accumulation and is_concrete and is_breakout)
                is_yma_relaxed = bool(past_bearish and is_concrete and (is_breakout or ma5_recovery) and (is_accumulation or obv_rising_c or vol_reaccum_c))

                if is_yma_strict or is_yma_relaxed:
                    c_grade = '완전체' if is_yma_strict else '✅A급'
                    c_score = 7 if is_yma_strict else 5
                    c_passed = ['역배열바닥', '매집흔적', '공구리', '장기선돌파'] if is_yma_strict else ['과거역배열', 'MA5/20회복', '장기선재도전', '매집/OBV흔적']
                    h = _bt_common_payload(code, name, 'C', '역매공파', c_grade, c_score, row, hist, idx_label, marcap, c_passed)
                    h.update({
                        'band_type': 'YMGP',
                        'c_type': 'strict' if is_yma_strict else 'relaxed',
                        'c_type_label': 'C 엄격형' if is_yma_strict else 'C 완화형(진단용)',
                        'kki_pattern': '바닥탈출대시세형',
                        'kki_habit': '매집 완료 후 장기 저항 돌파' if is_yma_strict else '과거 역배열 이후 장기선 재도전',
                        'kki_comment': '역매공파 타점 포착. 스윙 관점 유효.' if is_yma_strict else '완화형 C 백테스트 후보. 실전 후보가 아니라 C 조건 점검/비교용입니다.',
                    })
                    hits.append(h)
        except Exception as e:
            log_debug(f"C 백테스트 오류 [{code}/{name}]: {e}")
    except Exception as e:
        log_debug(f"_check_backtest_strategies_on_df 오류 [{code}/{name}]: {e}")
    return hits


def _evaluate_backtest_hit(hit: dict, df: pd.DataFrame, signal_idx: int, hold_days: int = 5) -> dict:
    """
    백테스트 평가.
    기존 고가/종가 성과에 더해 v3.1부터 실전형 3/5 익절·손절 시뮬레이션을 추가하고, v3.3부터 튜닝 리포트/실전추천 조합을 함께 계산한다.

    실전형 3/5 규칙(보수 모델):
    - 종가 진입 후 다음 거래일부터 평가
    - +3% 도달 시 50% 익절
    - +5% 도달 시 남은 50% 추가 익절
    - 손절선 터치 시 남은 물량 정리
    - +3 이후 +5/손절이 없으면 남은 물량은 hold_days 마지막 종가로 평가
    - 같은 날 목표가와 손절가가 모두 닿으면 보수적으로 손절을 먼저 본다.
    """
    entry = _safe_float(hit.get('close', 0), 0.0)
    future = df.iloc[signal_idx + 1: signal_idx + 1 + hold_days].copy()
    if entry <= 0 or future.empty:
        return {}
    next_bar = future.iloc[0]
    last_bar = future.iloc[-1]
    max_high = _safe_float(future['High'].max(), 0.0)
    min_low = _safe_float(future['Low'].min(), 0.0)
    stoploss = _safe_float(hit.get('stoploss', 0), 0.0)
    target1 = _safe_float(hit.get('target1', 0), 0.0)

    def _ret(price):
        return round((price / entry - 1.0) * 100.0, 2) if entry > 0 and price > 0 else np.nan

    # 3일 성과는 가능한 경우만 따로 계산
    future3 = df.iloc[signal_idx + 1: signal_idx + 4].copy()
    max_high_3 = _safe_float(future3['High'].max(), 0.0) if not future3.empty else np.nan
    close_3 = _safe_float(future3.iloc[-1]['Close'], 0.0) if not future3.empty else np.nan

    target3_price = entry * 1.03
    target5_price = entry * 1.05

    # 목표/손절 선행 여부: 일봉 OHLC만 있으므로 같은 날 충돌은 보수적으로 손절 우선
    hit3_before_stop = 0
    hit5_before_stop = 0
    stop_before_3 = 0
    first_event = '기간종료'
    first_event_date = ''
    for _, bar in future.iterrows():
        bdate = pd.Timestamp(bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(bar.get('Date')) else ''
        low = _safe_float(bar.get('Low', 0), 0.0)
        high = _safe_float(bar.get('High', 0), 0.0)
        if stoploss > 0 and low <= stoploss:
            stop_before_3 = 1
            first_event = '손절선행'
            first_event_date = bdate
            break
        if high >= target5_price:
            hit3_before_stop = 1
            hit5_before_stop = 1
            first_event = '+5선행'
            first_event_date = bdate
            break
        if high >= target3_price:
            hit3_before_stop = 1
            first_event = '+3선행'
            first_event_date = bdate
            break

    # 실전형 3/5 익절 시뮬레이션
    # same-day stop/target 충돌은 보수적으로 손절 먼저 처리
    pos_remain = 1.0
    realized_ret = 0.0
    hit3_rule = 0
    hit5_rule = 0
    stop_rule = 0
    exit_rule = '기간종료'
    exit_date = ''
    active = True

    for _, bar in future.iterrows():
        bdate = pd.Timestamp(bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(bar.get('Date')) else ''
        low = _safe_float(bar.get('Low', 0), 0.0)
        high = _safe_float(bar.get('High', 0), 0.0)

        if stoploss > 0 and low <= stoploss:
            realized_ret += pos_remain * _ret(stoploss)
            pos_remain = 0.0
            stop_rule = 1
            exit_rule = '손절'
            exit_date = bdate
            active = False
            break

        if hit3_rule == 0 and high >= target3_price:
            realized_ret += 0.5 * 3.0
            pos_remain -= 0.5
            hit3_rule = 1
            exit_rule = '+3절반익절'
            exit_date = bdate

        if hit3_rule == 1 and pos_remain > 0 and high >= target5_price:
            realized_ret += pos_remain * 5.0
            pos_remain = 0.0
            hit5_rule = 1
            exit_rule = '+5추가익절'
            exit_date = bdate
            active = False
            break

    if active and pos_remain > 0:
        close_ret = _ret(_safe_float(last_bar.get('Close', 0)))
        realized_ret += pos_remain * (0.0 if pd.isna(close_ret) else close_ret)
        if exit_rule == '기간종료':
            exit_date = pd.Timestamp(last_bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(last_bar.get('Date')) else ''

    return {
        'eval_start_date': pd.Timestamp(next_bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(next_bar.get('Date')) else '',
        'eval_end_date': pd.Timestamp(last_bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(last_bar.get('Date')) else '',
        'hold_days': int(len(future)),
        'next_open': _safe_float(next_bar.get('Open', 0)),
        'next_high': _safe_float(next_bar.get('High', 0)),
        'next_low': _safe_float(next_bar.get('Low', 0)),
        'next_close': _safe_float(next_bar.get('Close', 0)),
        'ret_next_open': _ret(_safe_float(next_bar.get('Open', 0))),
        'ret_next_high': _ret(_safe_float(next_bar.get('High', 0))),
        'ret_next_low': _ret(_safe_float(next_bar.get('Low', 0))),
        'ret_next_close': _ret(_safe_float(next_bar.get('Close', 0))),
        'ret_max_high_3d': _ret(max_high_3) if not pd.isna(max_high_3) else np.nan,
        'ret_close_3d': _ret(close_3) if not pd.isna(close_3) else np.nan,
        'ret_max_high_hd': _ret(max_high),
        'ret_min_low_hd': _ret(min_low),
        'ret_close_hd': _ret(_safe_float(last_bar.get('Close', 0))),
        'hit_plus2_hd': int(_ret(max_high) >= 2.0) if not pd.isna(_ret(max_high)) else 0,
        'hit_plus3_hd': int(_ret(max_high) >= 3.0) if not pd.isna(_ret(max_high)) else 0,
        'hit_plus5_hd': int(_ret(max_high) >= 5.0) if not pd.isna(_ret(max_high)) else 0,
        'close_win_hd': int(_ret(_safe_float(last_bar.get('Close', 0))) > 0) if not pd.isna(_ret(_safe_float(last_bar.get('Close', 0)))) else 0,
        'stoploss_hit_hd': int(min_low <= stoploss) if stoploss > 0 else np.nan,
        'target1_hit_hd': int(max_high >= target1) if target1 > 0 else np.nan,
        # v3.1: 선행/실전형 규칙
        'target3_price': round(target3_price),
        'target5_price': round(target5_price),
        'hit3_before_stop': int(hit3_before_stop),
        'hit5_before_stop': int(hit5_before_stop),
        'stop_before_3': int(stop_before_3),
        'first_event': first_event,
        'first_event_date': first_event_date,
        'rule35_pnl': round(realized_ret, 2),
        'rule35_win': int(realized_ret > 0),
        'rule35_hit3': int(hit3_rule),
        'rule35_hit5': int(hit5_rule),
        'rule35_stop': int(stop_rule),
        'rule35_exit': exit_rule,
        'rule35_exit_date': exit_date,
    }

def _backtest_sort_key(row: pd.Series):
    grade = str(row.get('grade', ''))
    g_rank = 0 if '완전체' in grade else (1 if 'A급' in grade else 2)
    return (g_rank, -_safe_float(row.get('score', 0)), -_safe_float(row.get('amount_b', 0)), -_safe_float(row.get('vol_ratio', 0)))


def _select_backtest_top(df: pd.DataFrame, top_per_strategy: int = 5, all_candidates: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work['_grade_rank'] = work['grade'].astype(str).map(lambda g: 0 if '완전체' in g else (1 if 'A급' in g else 2))
    work['_score_sort'] = pd.to_numeric(work.get('score', 0), errors='coerce').fillna(0)
    work['_amount_sort'] = pd.to_numeric(work.get('amount_b', 0), errors='coerce').fillna(0)
    work['_vol_sort'] = pd.to_numeric(work.get('vol_ratio', 0), errors='coerce').fillna(0)
    # v3.3: C는 엄격형을 우선하고, 완화형은 진단용 후보로 뒤에 둔다.
    if 'c_type' in work.columns:
        work['_c_type_rank'] = work['c_type'].astype(str).map(lambda x: 0 if x == 'strict' else (1 if x == 'relaxed' else 0))
    else:
        work['_c_type_rank'] = 0
    work = work.sort_values(['signal_date', 'mode', '_c_type_rank', '_grade_rank', '_score_sort', '_amount_sort', '_vol_sort'], ascending=[True, True, True, True, False, False, False])
    if not all_candidates:
        work = work.groupby(['signal_date', 'mode'], as_index=False, group_keys=False).head(int(top_per_strategy))
    work['selected_rank'] = work.groupby(['signal_date', 'mode']).cumcount() + 1
    return work.drop(columns=[c for c in ['_grade_rank', '_score_sort', '_amount_sort', '_vol_sort', '_c_type_rank'] if c in work.columns])


def _format_backtest_stat_block(sub: pd.DataFrame, label: str, hold_days: int) -> str:
    """백테스트 요약 1줄 포맷."""
    cnt = len(sub) if sub is not None else 0
    if cnt == 0:
        return f"- {label}: 0건"
    base = (
        f"- {label}: {cnt}건 | 다음날종가승률 {sub['ret_next_close'].gt(0).mean()*100:.1f}% | "
        f"{hold_days}일종가승률 {sub['close_win_hd'].fillna(0).mean()*100:.1f}% | "
        f"+2도달 {sub['hit_plus2_hd'].fillna(0).mean()*100:.1f}% | +3도달 {sub['hit_plus3_hd'].fillna(0).mean()*100:.1f}% | +5도달 {sub['hit_plus5_hd'].fillna(0).mean()*100:.1f}% | "
        f"평균최대상승 {sub['ret_max_high_hd'].mean():.2f}% | 평균종가수익 {sub['ret_close_hd'].mean():.2f}% | 평균최대하락 {sub['ret_min_low_hd'].mean():.2f}% | "
        f"손절터치 {sub['stoploss_hit_hd'].fillna(0).mean()*100:.1f}%"
    )
    return base


def _format_backtest_trade_rule_block(sub: pd.DataFrame, label: str) -> str:
    """v3.1 실전형 3/5 익절·손절 시뮬레이션 요약."""
    cnt = len(sub) if sub is not None else 0
    if cnt == 0:
        return f"- {label}: 0건"
    if 'rule35_pnl' not in sub.columns:
        return f"- {label}: 3/5 익절 시뮬레이션 데이터 없음"
    pnl = pd.to_numeric(sub.get('rule35_pnl'), errors='coerce')
    return (
        f"- {label}: {cnt}건 | 3/5규칙 평균수익 {pnl.mean():.2f}% | "
        f"승률 {pnl.gt(0).mean()*100:.1f}% | "
        f"+3선행 {sub.get('hit3_before_stop', pd.Series(dtype=float)).fillna(0).mean()*100:.1f}% | "
        f"+5선행 {sub.get('hit5_before_stop', pd.Series(dtype=float)).fillna(0).mean()*100:.1f}% | "
        f"손절선행 {sub.get('stop_before_3', pd.Series(dtype=float)).fillna(0).mean()*100:.1f}%"
    )

def _week_label_from_date(v) -> str:
    try:
        ts = pd.Timestamp(v)
        iso = ts.isocalendar()
        return f"{int(iso.year)}-W{int(iso.week):02d}"
    except Exception:
        return "unknown-week"


def _build_backtest_diag_text(diag: dict | None) -> str:
    """백테스트가 0건일 때 원인을 좁히기 위한 진단 텍스트."""
    if not diag:
        return ""
    lines = []
    lines.append("[백테스트 진단]")
    lines.append(f"- 유니버스 후보: {diag.get('universe_codes', 0)}개")
    lines.append(f"- 백테스트 대상: {diag.get('source_codes', 0)}개")
    lines.append(f"- 처리 완료 종목: {diag.get('codes_done', 0)}개")
    lines.append(f"- 데이터 로드 성공: {diag.get('data_loaded', 0)}개")
    lines.append(f"- 데이터 없음/부족: {diag.get('no_data', 0)}개")
    lines.append(f"- 검사한 일봉 수: {diag.get('bars_checked', 0)}개")
    lines.append(f"- 평가 생성 신호: {diag.get('rows', 0)}건")
    if diag.get('timeout'):
        lines.append("- 상태: 일부 타임아웃 발생 → 완료분만 집계")
    strat = diag.get('strategy_counts', {}) or {}
    if strat:
        order = ['G', 'S', 'A', 'B1', 'B2', 'C']
        part = []
        for m in order:
            part.append(f"{m}:{strat.get(m, 0)}")
        if part:
            lines.append("- 전략별 원신호: " + " / ".join(part))
    if diag.get('requested_period') and diag.get('actual_period') and diag.get('requested_period') != diag.get('actual_period'):
        lines.append(f"- 요청기간: {diag.get('requested_period')}")
        lines.append(f"- 실제검증기간: {diag.get('actual_period')}")
        lines.append("- 보유평가 미래봉 확보를 위해 주 단위 기본값은 평가 가능한 과거 주간으로 자동 보정됩니다.")
    if diag.get('data_sources'):
        ds = diag.get('data_sources') or {}
        lines.append("- 데이터소스: " + " / ".join(f"{k}:{v}" for k, v in ds.items()))
    if diag.get('load_error_samples'):
        lines.append("- 데이터 로드 오류 샘플:")
        for item in diag.get('load_error_samples', [])[:5]:
            lines.append(f"  · {item}")
    if diag.get('sample_error'):
        lines.append(f"- 샘플 오류: {diag.get('sample_error')}")
    return "\n".join(lines)



def _bt_mask_s2(df: pd.DataFrame):
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return (df['mode'].astype(str).eq('S')) & (df.get('s_type', '').astype(str).eq('S2'))


def _bt_mask_s1_good(df: pd.DataFrame):
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    rr = pd.to_numeric(df.get('rr', 0), errors='coerce').fillna(0)
    amount = pd.to_numeric(df.get('amount_b', 0), errors='coerce').fillna(0)
    close_loc = pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
    return (df['mode'].astype(str).eq('S')) & (df.get('s_type', '').astype(str).eq('S1')) & (rr >= 0.70) & (amount >= 100.0) & (close_loc >= 70.0)


def _bt_mask_a_strong(df: pd.DataFrame):
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    volr = pd.to_numeric(df.get('vol_ratio', 0), errors='coerce').fillna(0)
    amount = pd.to_numeric(df.get('amount_b', 0), errors='coerce').fillna(0)
    grade = df.get('grade', pd.Series('', index=df.index)).astype(str)
    good_grade = grade.str.contains('완전체|A급|A', regex=True, na=False)
    return df['mode'].astype(str).eq('A') & (volr >= 2.0) & (amount >= 100.0) & good_grade


def _format_practical_combo_report(df: pd.DataFrame) -> str:
    """v3.3: 백테스트 기반 실전 추천 조합 요약. 핵심군은 S2/S1우수/A강한만 포함하고 C 완화형은 진단용으로 분리한다."""
    lines = ["[실전 추천 조합 — v3.3 핵심군 튜닝 관점]"]
    if df is None or df.empty:
        lines.append("- 데이터 없음")
        return "\n".join(lines)
    try:
        s2 = df[_bt_mask_s2(df)]
        s1_good = df[_bt_mask_s1_good(df)]
        a_strong = df[_bt_mask_a_strong(df)]
        core = df[_bt_mask_s2(df) | _bt_mask_s1_good(df) | _bt_mask_a_strong(df)]
        b_observe = df[df['mode'].astype(str).isin(['B1', 'B2'])]
        g = df[df['mode'].astype(str).eq('G')]
        c_all = df[df['mode'].astype(str).eq('C')]
        if 'c_type' in c_all.columns:
            c_strict = c_all[c_all['c_type'].astype(str).eq('strict')]
            c_relaxed = c_all[c_all['c_type'].astype(str).eq('relaxed')]
        else:
            c_strict = c_all
            c_relaxed = c_all.iloc[0:0]
        lines.append(_format_backtest_trade_rule_block(core, '튜닝 핵심군(S2+S1우수+A강한)'))
        lines.append(_format_backtest_trade_rule_block(s2, 'S2 실행형'))
        lines.append(_format_backtest_trade_rule_block(s1_good, 'S1 우수관찰'))
        lines.append(_format_backtest_trade_rule_block(a_strong, 'A 강한돌파'))
        lines.append(_format_backtest_trade_rule_block(b_observe, 'B1/B2 관찰군'))
        if len(g) > 0:
            lines.append(_format_backtest_trade_rule_block(g, 'G 모랄레스갭'))
        if len(c_strict) > 0:
            lines.append(_format_backtest_trade_rule_block(c_strict, 'C 엄격형(스윙참고)'))
        if len(c_relaxed) > 0:
            lines.append(_format_backtest_trade_rule_block(c_relaxed, 'C 완화형(진단용)'))
        lines.append("- 해석: 실전 매수 우선 후보는 핵심군(S2+S1우수+A강한)입니다. B1/B2와 C완화형은 손절선행·조건 점검용으로 분리해 해석합니다.")
    except Exception as e:
        lines.append(f"- 조합 리포트 생성 오류: {e}")
    return "\n".join(lines)


def _bucket_series(values: pd.Series, bins: list, labels: list) -> pd.Series:
    try:
        return pd.cut(pd.to_numeric(values, errors='coerce'), bins=bins, labels=labels, include_lowest=True)
    except Exception:
        return pd.Series(['확인불가'] * len(values), index=values.index)


def _format_bucket_table(df: pd.DataFrame, bucket_col: str, label: str) -> list:
    lines = []
    if df is None or df.empty or bucket_col not in df.columns:
        lines.append(f"- {label}: 데이터 없음")
        return lines
    for bucket, sub in df.groupby(bucket_col, dropna=False):
        if len(sub) == 0:
            continue
        bucket_name = str(bucket)
        try:
            pnl = pd.to_numeric(sub.get('rule35_pnl', np.nan), errors='coerce').mean()
            win = pd.to_numeric(sub.get('rule35_win', np.nan), errors='coerce').fillna(0).mean() * 100
            p3 = pd.to_numeric(sub.get('rule35_first_plus3', np.nan), errors='coerce').fillna(0).mean() * 100
            sl = pd.to_numeric(sub.get('rule35_first_stop', np.nan), errors='coerce').fillna(0).mean() * 100
            lines.append(f"- {label} {bucket_name}: {len(sub)}건 | 3/5평균 {pnl:.2f}% | 승률 {win:.1f}% | +3선행 {p3:.1f}% | 손절선행 {sl:.1f}%")
        except Exception:
            lines.append(f"- {label} {bucket_name}: {len(sub)}건")
    return lines


def _format_tuning_report(df: pd.DataFrame) -> str:
    """v3.3: S/A/B 조건별 튜닝 리포트. 과최적화를 막기 위해 조건별 성과만 비교한다."""
    lines = ["[튜닝 리포트 — 조건별 성과]"]
    if df is None or df.empty:
        lines.append("- 데이터 없음")
        return "\n".join(lines)
    try:
        s_df = df[df['mode'].astype(str).eq('S')].copy()
        if not s_df.empty:
            s_df['RR구간'] = _bucket_series(s_df.get('rr', 0), [-999, 0.7, 1.0, 1.5, 999], ['RR<0.7', '0.7~1.0', '1.0~1.5', '1.5+'])
            s_df['거래량구간'] = _bucket_series(s_df.get('vol_ratio', 0), [-999, 1.0, 1.2, 1.5, 999], ['<1.0', '1.0~1.2', '1.2~1.5', '1.5+'])
            s_df['종가위치구간'] = _bucket_series(s_df.get('close_loc_pct', 0), [-999, 70, 85, 101], ['<70%', '70~85%', '85%+'])
            lines.append("- S 고점재응축: RR/거래량/종가위치별")
            lines += _format_bucket_table(s_df, 'RR구간', 'S RR')
            lines += _format_bucket_table(s_df, '거래량구간', 'S 거래량비')
            lines += _format_bucket_table(s_df, '종가위치구간', 'S 종가위치')
        a_df = df[df['mode'].astype(str).eq('A')].copy()
        if not a_df.empty:
            a_df['거래량구간'] = _bucket_series(a_df.get('vol_ratio', 0), [-999, 1.5, 2.0, 3.0, 999], ['<1.5', '1.5~2.0', '2.0~3.0', '3.0+'])
            a_df['거래대금구간'] = _bucket_series(a_df.get('amount_b', 0), [-999, 100, 300, 1000, 999999], ['<100억', '100~300억', '300~1000억', '1000억+'])
            lines.append("- A 돌파형: 거래량/거래대금별")
            lines += _format_bucket_table(a_df, '거래량구간', 'A 거래량비')
            lines += _format_bucket_table(a_df, '거래대금구간', 'A 거래대금')
        b_df = df[df['mode'].astype(str).isin(['B1', 'B2'])].copy()
        if not b_df.empty:
            lines.append("- B1/B2 하단형: 현재 백테스트상 관찰형 여부")
            lines.append(_format_backtest_trade_rule_block(b_df, 'B1/B2 통합'))
            lines.append("  → 손절선행이 높으면 즉시매수보다 다음날 양봉/전일고가 돌파 확인형으로 유지하는 편이 안전합니다.")
    except Exception as e:
        lines.append(f"- 튜닝 리포트 생성 오류: {e}")
    return "\n".join(lines)

def _build_backtest_summary(
    raw_df: pd.DataFrame,
    selected_df: pd.DataFrame,
    start_date: str,
    end_date: str,
    hold_days: int,
    top_per_strategy: int,
    all_candidates: bool,
    weekly_breakdown: bool = False,
    diag: dict | None = None,
) -> str:
    diag_text = _build_backtest_diag_text(diag)
    if selected_df is None or selected_df.empty:
        lines = [
            "🧪 종가배팅 과거 성과검증",
            f"버전: {CLOSING_BET_SCANNER_VERSION}",
            f"기간: {start_date} ~ {end_date} | 보유평가: 다음 {hold_days}거래일",
            "선별 후보가 없습니다.",
            "",
            diag_text,
            "",
            "[0건일 때 우선 확인]",
            "- 백테스트 대상이 0개면 지수/시총 유니버스 로딩 문제입니다.",
            "- 데이터 로드 성공이 0개면 FDR/pykrx/Naver 데이터 소스 또는 네트워크 문제입니다.",
            "- 검사한 일봉 수는 있는데 신호가 0건이면 전략 필터가 과하게 엄격한 상태입니다.",
            "- 최근 1주는 아직 다음 5거래일 데이터가 없어 성과평가가 불가능할 수 있습니다. v3.0은 주 단위 기본값을 평가 가능한 과거 주간으로 자동 보정합니다.",
        ]
        return "\n".join([x for x in lines if x is not None])

    df = selected_df.copy()
    if 'signal_week' not in df.columns and 'signal_date' in df.columns:
        df['signal_week'] = df['signal_date'].map(_week_label_from_date)

    lines = []
    lines.append("🧪 종가배팅 과거 성과검증")
    lines.append(f"버전: {CLOSING_BET_SCANNER_VERSION}")
    lines.append(f"기간: {start_date} ~ {end_date} | 보유평가: 다음 {hold_days}거래일")
    lines.append(f"원신호 {len(raw_df)}건 | 최종검증 {len(df)}건 | 선택방식: {'전체후보' if all_candidates else f'날짜별 전략별 TOP{top_per_strategy}'}")
    lines.append("")

    lines.append("[전체]")
    lines.append(_format_backtest_stat_block(df, '전체', hold_days))
    lines.append("")

    lines.append("[실전형 3/5 익절·손절]")
    lines.append(_format_backtest_trade_rule_block(df, '전체'))
    try:
        core_mask = _bt_mask_s2(df) | _bt_mask_s1_good(df) | _bt_mask_a_strong(df)
        core_df = df[core_mask]
        observe_mask = ((df['mode'].astype(str).eq('S')) & (~_bt_mask_s1_good(df)) & (~_bt_mask_s2(df))) | (df['mode'].astype(str).isin(['B1', 'B2']))
        observe_df = df[observe_mask]
        lines.append(_format_backtest_trade_rule_block(core_df, '튜닝 핵심군(S2+S1우수+A강한)'))
        lines.append(_format_backtest_trade_rule_block(observe_df, '관찰/확인군(S1일반+B1+B2)'))
    except Exception:
        pass
    lines.append("")

    try:
        lines.append(_format_practical_combo_report(df))
        lines.append("")
        lines.append(_format_tuning_report(df))
        lines.append("")
    except Exception as e:
        lines.append(f"[튜닝 리포트 오류] {e}")
        lines.append("")

    lines.append("[전략별]")
    mode_order = ['G', 'S', 'A', 'B1', 'B2', 'C']
    for mode in mode_order:
        sub = df[df['mode'] == mode]
        label = {
            'G': '모랄레스갭(G)',
            'S': '고점재응축(S)',
            'A': '돌파형(A)',
            'B1': 'ENV엄격형(B1)',
            'B2': 'BB확장형(B2)',
            'C': '역매공파(C)',
        }.get(mode, mode)
        lines.append(_format_backtest_stat_block(sub, label, hold_days))
        if not sub.empty and 'rule35_pnl' in sub.columns:
            lines.append('  ' + _format_backtest_trade_rule_block(sub, label).lstrip('- '))

    s_df = df[df['mode'] == 'S'] if 'mode' in df.columns else pd.DataFrame()
    if not s_df.empty and 's_type' in s_df.columns:
        lines.append("")
        lines.append("[S전략 세부]")
        for st in ['S2', 'S1']:
            sub = s_df[s_df['s_type'].astype(str) == st]
            label = 'S2 실행형' if st == 'S2' else 'S1 관찰형'
            lines.append(_format_backtest_stat_block(sub, label, hold_days))
            if not sub.empty and 'rule35_pnl' in sub.columns:
                lines.append('  ' + _format_backtest_trade_rule_block(sub, label).lstrip('- '))

    c_df = df[df['mode'] == 'C'] if 'mode' in df.columns else pd.DataFrame()
    if not c_df.empty:
        lines.append("")
        lines.append("[C전략 세부 — 엄격형/완화형 분리]")
        if 'c_type' in c_df.columns:
            for ct, label in [('strict', 'C 엄격형'), ('relaxed', 'C 완화형(진단용)')]:
                sub = c_df[c_df['c_type'].astype(str) == ct]
                lines.append(_format_backtest_stat_block(sub, label, hold_days))
                if not sub.empty and 'rule35_pnl' in sub.columns:
                    lines.append('  ' + _format_backtest_trade_rule_block(sub, label).lstrip('- '))
        else:
            lines.append(_format_backtest_stat_block(c_df, 'C 전체', hold_days))
        lines.append("- 해석: C 완화형은 실전 신호가 아니라 백테스트 진단용입니다. C 엄격형만 스윙 후보로 따로 검증하는 편이 안전합니다.")

    lines.append("")
    lines.append("[등급별]")
    for grade, sub in df.groupby('grade', dropna=False):
        lines.append(_format_backtest_stat_block(sub, str(grade), hold_days))

    if weekly_breakdown:
        lines.append("")
        lines.append("[주차별]")
        for week, sub in df.groupby('signal_week', dropna=False):
            cnt = len(sub)
            if cnt == 0:
                continue
            lines.append(
                f"- {week}: {cnt}건 | +3도달 {sub['hit_plus3_hd'].fillna(0).mean()*100:.1f}% | "
                f"+5도달 {sub['hit_plus5_hd'].fillna(0).mean()*100:.1f}% | "
                f"평균최대상승 {sub['ret_max_high_hd'].mean():.2f}% | 평균종가수익 {sub['ret_close_hd'].mean():.2f}%"
            )

    if diag_text:
        lines.append("")
        lines.append(diag_text)

    lines.append("")
    lines.append("[주의]")
    lines.append("- 이 검증은 일봉 종가 기준 재현입니다. 실제 장중 체결가, 슬리피지, 수수료, 호가 공백은 반영하지 않았습니다.")
    lines.append("- 기본값은 날짜별 전략별 TOP5만 검증하므로, 실전 텔레그램 선별 결과에 가깝게 보는 용도입니다.")
    lines.append("- v3.3의 3/5규칙은 보수 모델입니다. 같은 날 손절가와 목표가가 모두 닿으면 손절을 먼저 본 것으로 계산합니다.")
    lines.append("- v3.3의 튜닝 리포트는 최근 백테스트 결과를 조건별 성과로 비교하는 참고용이며, 최소 4주~12주 이상 반복 검증이 필요합니다.")
    lines.append("- C 완화형은 실전 추천 후보가 아니라 C조건 점검용 진단 후보입니다.")
    return "\n".join(lines)


def run_closing_bet_backtest(
    months: int = 6,
    weeks: int = 0,
    start_date: str = '',
    end_date: str = '',
    hold_days: int = 5,
    top_per_strategy: int = 5,
    all_candidates: bool = False,
    max_workers: int = None,
    weekly_breakdown: bool = False,
    debug: bool = False,
) -> tuple[str, str, str]:
    """과거 기간 동안 날짜별 종가배팅 신호를 재현하고 성과를 CSV/TXT로 저장한다.

    v2.9 추가:
    - --backtest-weeks N : 최근 N주만 빠르게 검증
    - --backtest-weekly : 주차별 성과 요약 포함
    - --backtest-debug : 0건 원인 추적용 진단 저장
    """
    log_info(f"✅ BOOTCHECK: {CLOSING_BET_SCANNER_VERSION}")
    _ensure_log_dir()
    now = _now_kst()
    hold_days = max(1, int(hold_days or 5))

    requested_end_date = end_date or now.strftime('%Y-%m-%d')
    requested_start_date = start_date

    # 주 단위 성과검증은 '신호 이후 다음 hold_days 거래일'이 필요하다.
    # 오늘 막 끝난 최근 1주는 아직 미래 봉이 없으므로 기본값에서는 평가 가능한 과거 주간으로 자동 보정한다.
    auto_evaluable = (
        int(weeks or 0) > 0
        and not end_date
        and os.environ.get('CLOSING_BET_BACKTEST_AUTO_EVALUABLE_END', '1').strip() != '0'
    )
    if auto_evaluable:
        shift_days = max(7, hold_days * 2)
        end_date = (pd.Timestamp(requested_end_date) - pd.Timedelta(days=shift_days)).strftime('%Y-%m-%d')
    else:
        end_date = requested_end_date

    if not start_date:
        if int(weeks or 0) > 0:
            start_date = (pd.Timestamp(end_date) - pd.Timedelta(days=7 * int(weeks))).strftime('%Y-%m-%d')
        else:
            start_date = (pd.Timestamp(end_date) - pd.DateOffset(months=int(months or 6))).strftime('%Y-%m-%d')

    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    top_per_strategy = max(1, int(top_per_strategy or 5))
    workers = int(max_workers or os.environ.get('CLOSING_BET_BACKTEST_WORKERS', max(4, min(MAX_WORKERS, 8))))
    weekly_breakdown = bool(weekly_breakdown or int(weeks or 0) > 0)

    log_info(f"\n{'=' * 60}")
    unit_txt = f"weeks={weeks}" if int(weeks or 0) > 0 else f"months={months}"
    log_info(f"종가배팅 백테스트 시작: {start_date} ~ {end_date} | {unit_txt} | hold={hold_days} | top_per_strategy={top_per_strategy}")
    if requested_end_date != end_date:
        log_info(f"주 단위 백테스트 평가 가능 기간 자동보정: 요청 종료 {requested_end_date} → 실제 종료 {end_date}")
    log_info(f"{'=' * 60}")

    _refresh_top_mcap_set(TOP_N)
    _refresh_marcap_map()
    _refresh_index_map()

    codes = _load_universe(SCAN_UNIVERSE)
    codes = sorted(set(_normalize_code(c) for c in codes if str(c).strip()))
    listing_for_names = _get_krx_listing()
    fdr_name_map = dict(zip(listing_for_names['Code'], listing_for_names['Name'])) if not listing_for_names.empty else {}
    source_codes = [c for c in codes if _is_universe_allowed(c)]
    names = [fdr_name_map.get(c, c) for c in source_codes]
    log_info(f"백테스트 대상: {len(source_codes)}개 ({SCAN_UNIVERSE})")

    raw_rows = []
    skipped = 0
    diag = {
        'universe_codes': len(codes),
        'source_codes': len(source_codes),
        'codes_done': 0,
        'data_loaded': 0,
        'no_data': 0,
        'bars_checked': 0,
        'rows': 0,
        'timeout': 0,
        'strategy_counts': {},
        'sample_error': '',
        'load_error_samples': [],
        'data_sources': {},
        'requested_period': f"{requested_start_date or '(auto)'} ~ {requested_end_date}",
        'actual_period': f"{start_date} ~ {end_date}",
    }

    def _merge_stat(st: dict):
        if not st:
            return
        diag['codes_done'] += int(st.get('codes_done', 0))
        diag['data_loaded'] += int(st.get('data_loaded', 0))
        diag['no_data'] += int(st.get('no_data', 0))
        diag['bars_checked'] += int(st.get('bars_checked', 0))
        diag['rows'] += int(st.get('rows', 0))
        for k, v in (st.get('strategy_counts', {}) or {}).items():
            diag['strategy_counts'][k] = diag['strategy_counts'].get(k, 0) + int(v or 0)
        for k, v in (st.get('data_sources', {}) or {}).items():
            diag['data_sources'][k] = diag['data_sources'].get(k, 0) + int(v or 0)
        if not diag.get('sample_error') and st.get('sample_error'):
            diag['sample_error'] = st.get('sample_error')

    def _run_one(code_name):
        code, name = code_name
        rows = []
        st = {
            'codes_done': 1,
            'data_loaded': 0,
            'no_data': 0,
            'bars_checked': 0,
            'rows': 0,
            'strategy_counts': {},
            'sample_error': '',
            'data_sources': {},
        }
        try:
            df = _load_df_backtest(code, start_date, end_date, warmup_days=380)
            if df is None or df.empty or len(df) < 90:
                st['no_data'] = 1
                return rows, 1, st
            st['data_loaded'] = 1
            try:
                src = str(df.get('data_source', pd.Series(['unknown'])).iloc[0]) if 'data_source' in df.columns else 'unknown'
                st.setdefault('data_sources', {})[src] = st.setdefault('data_sources', {}).get(src, 0) + 1
            except Exception:
                pass
            date_mask = (df['Date'] >= start_ts) & (df['Date'] <= end_ts)
            idx_list = df.index[date_mask].tolist()
            st['bars_checked'] = len(idx_list)
            for i in idx_list:
                # 미래 평가 봉이 없으면 제외
                if i + 1 >= len(df):
                    continue
                if i + hold_days >= len(df):
                    continue
                # 지표 warmup 부족 구간 제외
                if i < 80:
                    continue
                hist = df.iloc[:i + 1].copy().reset_index(drop=True)
                hits = _check_backtest_strategies_on_df(code, name, hist)
                if not hits:
                    continue
                for h in hits:
                    eval_rec = _evaluate_backtest_hit(h, df, i, hold_days=hold_days)
                    if not eval_rec:
                        continue
                    rec = {**h, **eval_rec}
                    if isinstance(rec.get('passed'), (list, tuple)):
                        rec['passed'] = ' · '.join(str(x) for x in rec.get('passed') if str(x).strip())
                    rec['signal_week'] = _week_label_from_date(rec.get('signal_date', ''))
                    rows.append(rec)
                    mode = str(rec.get('mode', ''))
                    st['strategy_counts'][mode] = st['strategy_counts'].get(mode, 0) + 1
            st['rows'] = len(rows)
            return rows, 0, st
        except Exception as e:
            st['sample_error'] = f"{code}/{name}: {e}"
            return rows, 1, st

    ex = ThreadPoolExecutor(max_workers=workers)
    futures = {ex.submit(_run_one, cn): cn for cn in zip(source_codes, names)}
    done = 0
    processed = set()
    try:
        for future in as_completed(futures, timeout=max(600, SCAN_FUTURES_TIMEOUT * 3)):
            processed.add(future)
            done += 1
            try:
                rows, skip, st = future.result()
                raw_rows.extend(rows)
                skipped += skip
                _merge_stat(st)
            except Exception as e:
                skipped += 1
                code, name = futures.get(future, ('', ''))
                if not diag.get('sample_error'):
                    diag['sample_error'] = f"{code}/{name}: {e}"
                log_debug(f"백테스트 개별 실패 [{code}/{name}]: {e}")
            if done % 100 == 0:
                log_info(f"백테스트 진행: {done}/{len(source_codes)} | 원신호 {len(raw_rows)}건 | 검사봉 {diag.get('bars_checked', 0)}개")
    except FuturesTimeoutError as e:
        diag['timeout'] = 1
        log_error(f"⚠️ 백테스트 일부 타임아웃: {e} → 완료분만 저장")
        for future in list(futures):
            if future in processed:
                continue
            if future.done():
                try:
                    rows, skip, st = future.result()
                    raw_rows.extend(rows)
                    skipped += skip
                    _merge_stat(st)
                except Exception as ex2:
                    skipped += 1
                    if not diag.get('sample_error'):
                        code, name = futures.get(future, ('', ''))
                        diag['sample_error'] = f"{code}/{name}: {ex2}"
            else:
                future.cancel()
    finally:
        ex.shutdown(wait=False, cancel_futures=True)

    try:
        diag['load_error_samples'] = list(BACKTEST_LOAD_ERROR_SAMPLES[:8])
    except Exception:
        pass
    raw_df = pd.DataFrame(raw_rows)
    stamp = now.strftime('%Y%m%d_%H%M%S')
    raw_path = LOG_DIR / f"closing_bet_backtest_raw_{stamp}.csv"
    selected_path = LOG_DIR / f"closing_bet_backtest_selected_{stamp}.csv"

    if raw_df.empty:
        report = _build_backtest_summary(
            raw_df, raw_df, start_date, end_date, hold_days, top_per_strategy,
            all_candidates, weekly_breakdown=weekly_breakdown, diag=diag,
        )
        try:
            BACKTEST_SUMMARY_TXT.write_text(report, encoding='utf-8')
            BACKTEST_DEBUG_TXT.write_text(_build_backtest_diag_text(diag), encoding='utf-8')
        except Exception as e:
            log_error(f"⚠️ 백테스트 0건 요약 저장 실패: {e}")
        return report, str(selected_path), str(raw_path)

    selected_df = _select_backtest_top(raw_df, top_per_strategy=top_per_strategy, all_candidates=all_candidates)
    try:
        raw_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
        selected_df.to_csv(selected_path, index=False, encoding='utf-8-sig')
    except Exception as e:
        log_error(f"⚠️ 백테스트 CSV 저장 실패: {e}")
    report = _build_backtest_summary(
        raw_df, selected_df, start_date, end_date, hold_days, top_per_strategy,
        all_candidates, weekly_breakdown=weekly_breakdown, diag=diag if debug else None,
    )
    try:
        BACKTEST_SUMMARY_TXT.write_text(report, encoding='utf-8')
        if debug:
            BACKTEST_DEBUG_TXT.write_text(_build_backtest_diag_text(diag), encoding='utf-8')
    except Exception as e:
        log_error(f"⚠️ 백테스트 요약 저장 실패: {e}")

    log_info(f"✅ 백테스트 완료 | 원신호 {len(raw_df)}건 | 최종검증 {len(selected_df)}건 | 스킵 {skipped}개")
    log_info(f"CSV(raw): {raw_path}")
    log_info(f"CSV(selected): {selected_path}")
    if debug:
        log_info("\n" + _build_backtest_diag_text(diag))
    return report, str(selected_path), str(raw_path)


# =============================================================
# 엔트리포인트
# =============================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='종가배팅 타점 스캐너')
    parser.add_argument('--force', action='store_true', help='시간 무관 강제 실행')
    parser.add_argument('--eval-pending', action='store_true', help='미평가 후보를 다음날 성과로 평가')
    parser.add_argument('--summary', action='store_true', help='검증 요약 출력')
    parser.add_argument('--send-summary', action='store_true', help='검증 요약을 텔레그램으로 전송')
    parser.add_argument('--backtest-months', type=int, default=0, help='과거 n개월 성과검증 실행. 예: --backtest-months 6')
    parser.add_argument('--backtest-weeks', type=int, default=0, help='과거 n주 성과검증 실행. 예: --backtest-weeks 1')
    parser.add_argument('--backtest-start', type=str, default='', help='백테스트 시작일 YYYY-MM-DD')
    parser.add_argument('--backtest-end', type=str, default='', help='백테스트 종료일 YYYY-MM-DD')
    parser.add_argument('--backtest-hold-days', type=int, default=5, help='성과 평가 보유 거래일 수')
    parser.add_argument('--backtest-top-per-strategy', type=int, default=5, help='날짜별 전략별 상위 n개만 검증')
    parser.add_argument('--backtest-all-candidates', action='store_true', help='TOP 제한 없이 모든 후보 검증')
    parser.add_argument('--backtest-weekly', action='store_true', help='백테스트 결과에 주차별 성과 요약 포함')
    parser.add_argument('--backtest-debug', action='store_true', help='백테스트 0건/진단 로그를 함께 출력')
    parser.add_argument('--send-backtest-summary', action='store_true', help='백테스트 요약을 텔레그램으로 전송')
    args = parser.parse_args()

    now = _now_kst()
    log_info(f"✅ BOOTCHECK: {CLOSING_BET_SCANNER_VERSION}")
    log_info(f"종가배팅 스캐너 시작: {now.strftime('%H:%M')} (force={args.force})")
    log_info(f"TELEGRAM_TOKEN: {'✅' if TELEGRAM_TOKEN else '❌ 없음'}")
    log_info(f"CHAT_ID_LIST: {'✅ ' + str(CHAT_ID_LIST) if CHAT_ID_LIST else '❌ 없음'}")
    log_info(f"SCAN_UNIVERSE: {SCAN_UNIVERSE}")
    log_info(f"시간 체크: {'✅ 통과' if _is_closing_time(args.force) else '❌ 시간 외'}")

    if args.backtest_months or args.backtest_weeks or args.backtest_start or args.backtest_end:
        report, selected_csv, raw_csv = run_closing_bet_backtest(
            months=args.backtest_months or 6,
            weeks=args.backtest_weeks or 0,
            start_date=args.backtest_start,
            end_date=args.backtest_end,
            hold_days=args.backtest_hold_days,
            top_per_strategy=args.backtest_top_per_strategy,
            all_candidates=args.backtest_all_candidates,
            weekly_breakdown=args.backtest_weekly or bool(args.backtest_weeks),
            debug=args.backtest_debug,
        )
        log_info("\n" + report)
        log_info(f"백테스트 선택 CSV: {selected_csv}")
        log_info(f"백테스트 원신호 CSV: {raw_csv}")
        if (args.send_backtest_summary or args.send_summary) and TELEGRAM_TOKEN:
            send_telegram_chunks(report, max_len=3500)
        sys.exit(0)

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
